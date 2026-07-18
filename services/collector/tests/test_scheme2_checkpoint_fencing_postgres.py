"""Opt-in PostgreSQL acceptance for Scheme 2 member checkpoint fencing.

The database must be disposable and already contain migrations 001..045.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from collector import parquet_bootstrap_import as importer
from collector.storage import scheme2_postgres as scheme2_storage
from collector.storage.scheme2_postgres import (
    LostScheme2MemberLease,
    PostgresScheme2KlineWriter,
    PostgresScheme2MemberCheckpointStore,
    Scheme2SourceMember,
)
from trading_protocol import Bar, SymbolInfo


TEST_DATABASE_URL = os.getenv("SCHEME2_CHECKPOINT_FENCING_TEST_DATABASE_URL", "")


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set SCHEME2_CHECKPOINT_FENCING_TEST_DATABASE_URL for a disposable migrated database",
)
def test_scheme2_member_checkpoint_lease_and_page_transaction_fencing(monkeypatch) -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        setup = await asyncpg.connect(TEST_DATABASE_URL)
        suffix = uuid4().hex[:10].upper()
        code = f"S2{suffix}"[:12]
        symbol = f"{code}.TS"
        root = f"D:/scheme2-fencing/{suffix}"
        generation_id = uuid4()
        symbol_id = await setup.fetchval(
            """insert into symbols(code, exchange, name, asset_type, market, is_active)
               values($1, 'TS', $1, 'stock', 'test', true) returning id""",
            code,
        )
        await setup.execute(
            """insert into kline_scope_catalog_generations(
                   generation_id, status, expected_scope_count, symbol_ids,
                   timeframes, completed_at, base_control_revision
               ) values($1, 'complete', 1, array[$2]::integer[], array[5]::integer[],
                        clock_timestamp(), 0)""",
            generation_id,
            symbol_id,
        )
        await setup.execute(
            """insert into kline_scope_catalog(
                   generation_id, symbol_id, timeframe, state, bounds_complete
               ) values($1, $2, 5, 'unknown', false)""",
            generation_id,
            symbol_id,
        )
        await setup.execute(
            """update kline_scope_catalog_control
                  set active_generation_id=$1, revision=revision+1
                where control_key='active'""",
            generation_id,
        )
        try:
            member = Scheme2SourceMember(
                root_path=root,
                source_profile="parquet_5f",
                zip_path=f"{root}/2024.zip",
                member_path="20240102.parquet",
                member_crc32=123,
                member_size_bytes=456,
            )
            async with PostgresScheme2MemberCheckpointStore(TEST_DATABASE_URL) as store:
                async with PostgresScheme2KlineWriter(TEST_DATABASE_URL) as writer:
                    assert await store.ensure_member_checkpoints([member]) == 1
                    task_a = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-a",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    assert await store.reset_running() == 0
                    await store.ensure_member_checkpoints([member], reset=True)
                    live = await setup.fetchrow(
                        """select status, claim_token, lease_version, imported_rows
                             from scheme2_source_member_checkpoints where id=$1""",
                        task_a["id"],
                    )
                    assert tuple(live.values()) == (
                        "running", task_a["claim_token"], task_a["lease_version"], 0,
                    )

                    before_identity_mismatch = await _snapshot(
                        setup, checkpoint_id=task_a["id"], symbol_id=symbol_id,
                    )
                    wrong_identity = dict(task_a, member_crc32=124)
                    with pytest.raises(ValueError, match="identity mismatch"):
                        await writer.commit_member_batch(
                            task=wrong_identity,
                            expected_imported_rows=0,
                            symbols=[_symbol(symbol, code)],
                            bars=[_bar(symbol, 1)],
                            lease_seconds=30,
                        )
                    assert await _snapshot(
                        setup, checkpoint_id=task_a["id"], symbol_id=symbol_id,
                    ) == before_identity_mismatch

                    first = _bar(symbol, 1)
                    assert await writer.commit_member_batch(
                        task=task_a,
                        expected_imported_rows=0,
                        symbols=[_symbol(symbol, code)],
                        bars=[first],
                        lease_seconds=30,
                    ) == 1

                    await setup.execute(
                        """update scheme2_source_member_checkpoints
                              set lease_until=clock_timestamp()-interval '1 second'
                            where id=$1""",
                        task_a["id"],
                    )
                    task_b = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-b",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    assert task_b["lease_version"] > task_a["lease_version"]
                    assert task_b["claim_token"] != task_a["claim_token"]
                    assert task_b["attempts"] == 1
                    assert task_b["imported_rows"] == 1

                    second = _bar(symbol, 2)
                    assert await writer.commit_member_batch(
                        task=task_b,
                        expected_imported_rows=1,
                        symbols=[_symbol(symbol, code)],
                        bars=[second],
                        lease_seconds=30,
                    ) == 1
                    before_stale = await _snapshot(
                        setup, checkpoint_id=task_b["id"], symbol_id=symbol_id,
                    )

                    stale = _bar(symbol, 3)
                    with pytest.raises(LostScheme2MemberLease):
                        await writer.commit_member_batch(
                            task=task_a,
                            expected_imported_rows=1,
                            symbols=[_symbol(symbol, code)],
                            bars=[stale],
                            lease_seconds=30,
                        )
                    assert not await store.heartbeat(
                        checkpoint_id=task_a["id"],
                        claim_token=task_a["claim_token"],
                        lease_version=task_a["lease_version"],
                        lease_seconds=30,
                    )
                    assert not await store.record_member_success(
                        checkpoint_id=task_a["id"],
                        claim_token=task_a["claim_token"],
                        lease_version=task_a["lease_version"],
                        expected_imported_rows=1,
                    )
                    assert not await store.record_member_failure(
                        checkpoint_id=task_a["id"],
                        claim_token=task_a["claim_token"],
                        lease_version=task_a["lease_version"],
                        expected_imported_rows=1,
                        error="late-a",
                    )
                    assert not await store.yield_member(
                        checkpoint_id=task_a["id"],
                        claim_token=task_a["claim_token"],
                        lease_version=task_a["lease_version"],
                        expected_imported_rows=1,
                    )
                    assert await _snapshot(
                        setup, checkpoint_id=task_b["id"], symbol_id=symbol_id,
                    ) == before_stale

                    assert await store.record_member_success(
                        checkpoint_id=task_b["id"],
                        claim_token=task_b["claim_token"],
                        lease_version=task_b["lease_version"],
                        expected_imported_rows=2,
                    )

                    locked_member = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=f"{root}/locked.zip",
                        member_path="locked.parquet",
                        member_crc32=456,
                        member_size_bytes=789,
                    )
                    await store.ensure_member_checkpoints([locked_member])
                    locked_task = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-lock-a",
                        lease_seconds=1,
                        max_attempts=3,
                    ))[0]
                    locker = await asyncpg.connect(TEST_DATABASE_URL)
                    lock_tx = locker.transaction()
                    await lock_tx.start()
                    try:
                        await locker.fetchrow(
                            "select id from scheme2_source_member_checkpoints where id=$1 for update",
                            locked_task["id"],
                        )
                        await asyncio.sleep(1.1)
                        assert await asyncio.wait_for(
                            store.claim_member_checkpoints(
                                limit=1,
                                worker_id="worker-lock-b",
                                lease_seconds=30,
                                max_attempts=3,
                            ),
                            timeout=0.5,
                        ) == []
                    finally:
                        await lock_tx.rollback()
                        await locker.close()
                    lock_reclaimed = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-lock-b",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    assert lock_reclaimed["id"] == locked_task["id"]
                    assert lock_reclaimed["attempts"] == 1
                    assert await store.yield_member(
                        checkpoint_id=lock_reclaimed["id"],
                        claim_token=lock_reclaimed["claim_token"],
                        lease_version=lock_reclaimed["lease_version"],
                        expected_imported_rows=0,
                    )
                    lock_after_yield = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-lock-c",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    assert lock_after_yield["id"] == locked_task["id"]
                    assert lock_after_yield["imported_rows"] == 0
                    assert await store.record_member_success(
                        checkpoint_id=lock_after_yield["id"],
                        claim_token=lock_after_yield["claim_token"],
                        lease_version=lock_after_yield["lease_version"],
                        expected_imported_rows=0,
                    )

                    concurrent_member = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=f"{root}/concurrent.zip",
                        member_path="concurrent.parquet",
                        member_crc32=147,
                        member_size_bytes=258,
                    )
                    await store.ensure_member_checkpoints([concurrent_member])
                    concurrent_claims = await asyncio.gather(
                        store.claim_member_checkpoints(
                            limit=1,
                            worker_id="worker-concurrent-a",
                            lease_seconds=30,
                            max_attempts=3,
                        ),
                        store.claim_member_checkpoints(
                            limit=1,
                            worker_id="worker-concurrent-b",
                            lease_seconds=30,
                            max_attempts=3,
                        ),
                    )
                    claimed = [task for tasks in concurrent_claims for task in tasks]
                    assert len(claimed) == 1
                    assert await store.record_member_success(
                        checkpoint_id=claimed[0]["id"],
                        claim_token=claimed[0]["claim_token"],
                        lease_version=claimed[0]["lease_version"],
                        expected_imported_rows=0,
                    )

                    resume_member = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=f"{root}/resume.zip",
                        member_path="resume.parquet",
                        member_crc32=321,
                        member_size_bytes=654,
                    )
                    await store.ensure_member_checkpoints([resume_member])
                    resume_a = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-resume-a",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    source_rows = [{"index": index} for index in (5, 6, 7)]

                    def resume_rows(_zip_path, _member_path, *, batch_size, **_kwargs):
                        return iter(
                            source_rows[offset:offset + batch_size]
                            for offset in range(0, len(source_rows), batch_size)
                        )

                    def parse_resume_rows(rows):
                        return importer.ParsedParquetBatch(
                            symbols={symbol: _symbol(symbol, code)},
                            bars=[_bar(symbol, int(row["index"])) for row in rows],
                        )

                    monkeypatch.setattr(importer, "_iter_member_rows", resume_rows)
                    monkeypatch.setattr(importer, "parse_parquet_rows", parse_resume_rows)
                    before_resume_count = await setup.fetchval(
                        "select count(*) from klines where symbol_id=$1", symbol_id,
                    )
                    assert await importer.process_member_task(
                        writer=writer,
                        checkpoint_store=store,
                        task=resume_a,
                        batch_size=2,
                        lease_seconds=30,
                        max_batches_per_member=1,
                    ) == {"bars": 2}
                    assert await setup.fetchval(
                        "select imported_rows from scheme2_source_member_checkpoints where id=$1",
                        resume_a["id"],
                    ) == 2
                    resume_b = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-resume-b",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    assert resume_b["id"] == resume_a["id"]
                    assert await importer.process_member_task(
                        writer=writer,
                        checkpoint_store=store,
                        task=resume_b,
                        batch_size=1,
                        lease_seconds=30,
                    ) == {"bars": 1}
                    resume_done = await setup.fetchrow(
                        "select status, imported_rows from scheme2_source_member_checkpoints where id=$1",
                        resume_b["id"],
                    )
                    assert tuple(resume_done.values()) == ("success", 3)
                    assert await setup.fetchval(
                        "select count(*) from klines where symbol_id=$1", symbol_id,
                    ) == before_resume_count + 3

                    mid_member = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=f"{root}/2025.zip",
                        member_path="20250102.parquet",
                        member_crc32=789,
                        member_size_bytes=654,
                    )
                    await store.ensure_member_checkpoints([mid_member])
                    mid = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-mid",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    before_mid = await _snapshot(
                        setup, checkpoint_id=mid["id"], symbol_id=symbol_id,
                    )
                    await setup.execute(
                        """update scheme2_source_member_checkpoints
                              set lease_until=clock_timestamp()-interval '1 second'
                            where id=$1""",
                        mid["id"],
                    )
                    with pytest.raises(LostScheme2MemberLease):
                        await writer.commit_member_batch(
                            task=mid,
                            expected_imported_rows=0,
                            symbols=[_symbol(symbol, code)],
                            bars=[_bar(symbol, 4)],
                            lease_seconds=30,
                        )
                    expired_before_lock = await _snapshot(
                        setup, checkpoint_id=mid["id"], symbol_id=symbol_id,
                    )
                    assert expired_before_lock[1:] == before_mid[1:]

                    mid_reclaimed = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-mid-reclaimed",
                        lease_seconds=30,
                        max_attempts=3,
                    ))[0]
                    assert mid_reclaimed["id"] == mid["id"]
                    original_write = writer._write_5f_batch

                    async def expire_after_writes(conn, *, symbol_rows, bar_rows):
                        await original_write(
                            conn, symbol_rows=symbol_rows, bar_rows=bar_rows,
                        )
                        await conn.execute(
                            """update scheme2_source_member_checkpoints
                                  set lease_until=clock_timestamp()-interval '1 second'
                                where id=$1""",
                            mid_reclaimed["id"],
                        )

                    writer._write_5f_batch = expire_after_writes
                    try:
                        assert await writer.commit_member_batch(
                            task=mid_reclaimed,
                            expected_imported_rows=0,
                            symbols=[_symbol(symbol, code)],
                            bars=[_bar(symbol, 4)],
                            lease_seconds=30,
                        ) == 1
                    finally:
                        writer._write_5f_batch = original_write
                    mid_after_slow_commit = await setup.fetchrow(
                        """select imported_rows, lease_until > clock_timestamp() as lease_live
                             from scheme2_source_member_checkpoints where id=$1""",
                        mid_reclaimed["id"],
                    )
                    assert tuple(mid_after_slow_commit.values()) == (1, True)
                    assert await store.record_member_success(
                        checkpoint_id=mid_reclaimed["id"],
                        claim_token=mid_reclaimed["claim_token"],
                        lease_version=mid_reclaimed["lease_version"],
                        expected_imported_rows=1,
                    )

                    max_member = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=f"{root}/max.zip",
                        member_path="max.parquet",
                        member_crc32=999,
                        member_size_bytes=111,
                    )
                    await store.ensure_member_checkpoints([max_member])
                    max_task = (await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-max-a",
                        lease_seconds=30,
                        max_attempts=1,
                    ))[0]
                    await setup.execute(
                        """update scheme2_source_member_checkpoints
                              set lease_until=clock_timestamp()-interval '1 second'
                            where id=$1""",
                        max_task["id"],
                    )
                    assert await store.claim_member_checkpoints(
                        limit=1,
                        worker_id="worker-max-b",
                        lease_seconds=30,
                        max_attempts=1,
                    ) == []
                    assert await setup.fetchval(
                        """select status from scheme2_source_member_checkpoints
                            where id=$1""",
                        max_task["id"],
                    ) == "dead_letter"

                    direct_path = f"{root}/symbols/direct.parquet"
                    direct_a = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=direct_path,
                        member_path="",
                        member_crc32=None,
                        member_size_bytes=4096,
                        content_sha256="a" * 64,
                    )
                    direct_b = Scheme2SourceMember(
                        root_path=root,
                        source_profile="parquet_5f",
                        zip_path=direct_path,
                        member_path="",
                        member_crc32=None,
                        member_size_bytes=4096,
                        content_sha256="b" * 64,
                    )
                    assert await store.ensure_member_checkpoints([direct_a]) == 1
                    assert await store.ensure_member_checkpoints([direct_b]) == 1
                    assert await setup.fetchval(
                        """select count(*)
                             from scheme2_source_member_checkpoints
                            where root_path=$1 and zip_path=$2 and member_path=''""",
                        root,
                        direct_path,
                    ) == 2
        finally:
            await setup.execute(
                "update kline_scope_catalog_control set active_generation_id=null, revision=revision+1 where control_key='active'"
            )
            await setup.execute(
                "delete from scheme2_source_member_checkpoints where root_path=$1", root
            )
            await setup.execute(
                "delete from scheme2_ingest_watermarks where symbol_id=$1", symbol_id
            )
            await setup.execute(
                "delete from kline_source_coverage where symbol_id=$1", symbol_id
            )
            await setup.execute("delete from klines where symbol_id=$1", symbol_id)
            await setup.execute(
                "delete from kline_scope_catalog where generation_id=$1", generation_id
            )
            await setup.execute(
                "delete from kline_scope_catalog_generations where generation_id=$1",
                generation_id,
            )
            await setup.execute("delete from symbols where id=$1", symbol_id)
            await setup.close()

    asyncio.run(scenario())


async def _snapshot(conn, *, checkpoint_id: int, symbol_id: int) -> tuple:
    checkpoint = await conn.fetchrow(
        """select status, imported_rows, worker_id, claim_token, lease_version,
                  attempts, max_attempts
             from scheme2_source_member_checkpoints where id=$1""",
        checkpoint_id,
    )
    klines = await conn.fetch(
        """select timeframe, ts, close_x1000, source
             from klines where symbol_id=$1 order by timeframe, ts""",
        symbol_id,
    )
    coverage = await conn.fetch(
        """select timeframe, source, covered_until
             from kline_source_coverage where symbol_id=$1 order by timeframe, source""",
        symbol_id,
    )
    catalog = await conn.fetch(
        """select generation_id, timeframe, state, bounds_complete, min_ts, max_ts
             from kline_scope_catalog where symbol_id=$1
             order by generation_id, timeframe""",
        symbol_id,
    )
    watermarks = await conn.fetch(
        """select timeframe, last_bar_end, source
             from scheme2_ingest_watermarks where symbol_id=$1 order by timeframe""",
        symbol_id,
    )
    return (
        tuple(checkpoint.values()),
        tuple(tuple(row.values()) for row in klines),
        tuple(tuple(row.values()) for row in coverage),
        tuple(tuple(row.values()) for row in catalog),
        tuple(tuple(row.values()) for row in watermarks),
    )


def _symbol(symbol: str, code: str) -> SymbolInfo:
    return SymbolInfo(
        symbol=symbol,
        code=code,
        exchange="TS",
        name=code,
        asset_type="stock",
        market="test",
    )


def _bar(symbol: str, index: int) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="5f",
        ts=datetime(2026, 1, 2, 1, 30, tzinfo=UTC)
        + timedelta(minutes=index * 5),
        open=10 + index,
        high=11 + index,
        low=9 + index,
        close=10.5 + index,
        volume=100 + index,
        source="parquet_5f",
    )
