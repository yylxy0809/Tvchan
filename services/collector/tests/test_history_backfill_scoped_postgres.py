"""Opt-in disposable PostgreSQL acceptance for scoped PyTDX tail runs."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from collector.market_fill import symbol_info_from_symbol
from collector.storage.backfill_postgres import PostgresBackfillTaskStore
from collector.storage.postgres import LostBackfillLease, PostgresKlineWriter
from trading_protocol import Bar


TEST_DATABASE_URL = os.getenv("HISTORY_BACKFILL_SCOPED_TEST_DATABASE_URL", "")


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set HISTORY_BACKFILL_SCOPED_TEST_DATABASE_URL for a disposable migrated database",
)
def test_scoped_run_is_durable_and_legacy_worker_cannot_claim_or_mutate_it() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        setup = await asyncpg.connect(TEST_DATABASE_URL)
        code = f"{uuid4().int % 1_000_000:06d}"
        symbol = f"{code}.SZ"
        symbol_id = await setup.fetchval(
            """
            insert into symbols(code, exchange, name, asset_type, market, is_active)
            values($1, 'SZ', $1, 'stock', 'test', true)
            returning id
            """,
            code,
        )
        generation_id = uuid4()
        run_id = uuid4()
        other_run_id = uuid4()
        run_identity = uuid4().hex * 2
        manifest_sha256 = uuid4().hex * 2
        cutoff = datetime(2026, 7, 10, 7, tzinfo=UTC)
        try:
            await setup.execute(
                """
                insert into kline_scope_catalog_generations(
                    generation_id, status, expected_scope_count, symbol_ids,
                    timeframes, completed_at
                ) values($1, 'complete', 1, $2, array[5], clock_timestamp())
                """,
                generation_id,
                [symbol_id],
            )
            await setup.execute(
                """
                insert into kline_scope_catalog(
                    generation_id, symbol_id, timeframe, state, bounds_complete,
                    min_ts, max_ts
                ) values($1,$2,5,'present',true,$3,$3)
                """,
                generation_id,
                symbol_id,
                cutoff,
            )
            await setup.execute(
                """
                update kline_scope_catalog_control
                set active_generation_id=$1, revision=revision+1
                where control_key='active'
                """,
                generation_id,
            )
            legacy_id = await setup.fetchval(
                """
                insert into historical_backfill_tasks(
                    symbol_id,timeframe,provider,page_size
                ) values($1,5,'pytdx',10) returning id
                """,
                symbol_id,
            )

            async with PostgresBackfillTaskStore(TEST_DATABASE_URL) as store:
                assert await store.ensure_tasks(
                    symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                    provider="pytdx", page_size=10,
                ) == [legacy_id]
                scoped_ids = await store.ensure_scoped_run_tasks(
                    run_id=run_id,
                    run_identity=run_identity,
                    manifest_sha256=manifest_sha256,
                    symbols=[symbol_info_from_symbol(symbol)],
                    timeframes=["5f"],
                    stop_at={"5f": cutoff},
                    expected_through={"5f": cutoff.replace(day=17)},
                    freshness_contract_sha256="a" * 64,
                    provider="pytdx",
                    page_size=10,
                    endpoint="127.0.0.1:7709",
                    source_policy="primary_failover",
                )
                assert len(scoped_ids) == 1
                assert scoped_ids[0] != legacy_id
                resumed_ids = await store.ensure_scoped_run_tasks(
                    run_id=run_id, run_identity=run_identity,
                    manifest_sha256=manifest_sha256,
                    symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                    stop_at={"5f": cutoff}, provider="pytdx", page_size=10,
                    expected_through={"5f": cutoff.replace(day=17)},
                    freshness_contract_sha256="a" * 64,
                    endpoint="127.0.0.1:7709", source_policy="primary_failover",
                )
                assert resumed_ids == scoped_ids
                with pytest.raises(RuntimeError, match="stop-at differs"):
                    await store.ensure_scoped_run_tasks(
                        run_id=other_run_id, run_identity=uuid4().hex * 2,
                        manifest_sha256=manifest_sha256,
                        symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                        stop_at={"5f": cutoff.replace(day=9)},
                        expected_through={"5f": cutoff.replace(day=17)},
                        freshness_contract_sha256="a" * 64,
                        provider="pytdx", page_size=10,
                        endpoint="127.0.0.1:7709", source_policy="primary_failover",
                    )
                with pytest.raises(asyncpg.PostgresError) as blocked_insert:
                    await setup.execute(
                        """
                        insert into historical_backfill_tasks(
                            run_id, stop_at, symbol_id, timeframe, provider, page_size
                        ) values($1,$2,$3,30,'pytdx',10)
                        """,
                        run_id,
                        cutoff,
                        symbol_id,
                    )
                assert blocked_insert.value.sqlstate == "55000"

                with pytest.raises(asyncpg.PostgresError) as blocked_promotion:
                    await setup.execute(
                        """
                        update historical_backfill_tasks
                        set run_id=$1, stop_at=$2
                        where id=$3
                        """,
                        run_id,
                        cutoff,
                        legacy_id,
                    )
                assert blocked_promotion.value.sqlstate == "55000"

                async with setup.transaction():
                    await setup.execute(
                        "select set_config('tvchan.history_backfill_scoped_run_id',$1,true)",
                        str(run_id),
                    )
                    with pytest.raises(asyncpg.PostgresError) as immutable_promotion:
                        await setup.execute(
                            """
                            update historical_backfill_tasks
                            set run_id=$1, stop_at=$2
                            where id=$3
                            """,
                            run_id,
                            cutoff,
                            legacy_id,
                        )
                assert immutable_promotion.value.sqlstate == "55000"

                for column, value in (
                    ("stop_at", cutoff.replace(day=9)),
                    ("symbol_id", symbol_id + 1),
                    ("timeframe", 30),
                    ("provider", "other"),
                    ("page_size", 11),
                ):
                    async with setup.transaction():
                        await setup.execute(
                            "select set_config('tvchan.history_backfill_scoped_run_id',$1,true)",
                            str(run_id),
                        )
                        with pytest.raises(asyncpg.PostgresError) as immutable_identity:
                            await setup.execute(
                                f"update historical_backfill_tasks set {column}=$1 where id=$2",
                                value,
                                scoped_ids[0],
                            )
                    assert immutable_identity.value.sqlstate == "55000"

                with pytest.raises(RuntimeError, match="durable run identity mismatch"):
                    await store.ensure_scoped_run_tasks(
                        run_id=run_id, run_identity=run_identity,
                        manifest_sha256=manifest_sha256,
                        symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                        stop_at={"5f": cutoff}, provider="pytdx", page_size=11,
                        expected_through={"5f": cutoff.replace(day=17)},
                        freshness_contract_sha256="a" * 64,
                        endpoint="127.0.0.1:7709", source_policy="primary_failover",
                    )

                legacy = await store.claim_tasks(
                    provider="pytdx", limit=10, worker_id="legacy",
                    lease_seconds=30, max_attempts=3, task_ids=[legacy_id],
                )
                assert [row["id"] for row in legacy] == [legacy_id]

                with pytest.raises(asyncpg.PostgresError) as blocked:
                    await setup.execute(
                        """
                        update historical_backfill_tasks
                        set status='running', worker_id='old-binary',
                            claim_token='old', lease_until=clock_timestamp()+interval '1 minute',
                            lease_heartbeat_at=clock_timestamp()
                        where id=$1
                        """,
                        scoped_ids[0],
                    )
                assert blocked.value.sqlstate == "55000"

                scoped = await store.claim_tasks(
                    provider="pytdx", limit=1, worker_id="scoped",
                    lease_seconds=30, max_attempts=3, task_ids=scoped_ids,
                    run_id=run_id,
                )
                assert len(scoped) == 1
                task = scoped[0]
                assert task["run_id"] == run_id
                assert task["stop_at"] == cutoff
                assert not await store.heartbeat(
                    task_id=task["id"], claim_token=task["claim_token"],
                    lease_version=task["lease_version"], lease_seconds=30,
                    run_id=other_run_id, stop_at=cutoff,
                )
                assert not await store.yield_task(
                    task_id=task["id"], claim_token=task["claim_token"],
                    lease_version=task["lease_version"], run_id=other_run_id,
                    stop_at=cutoff,
                )
                assert await store.heartbeat(
                    task_id=task["id"], claim_token=task["claim_token"],
                    lease_version=task["lease_version"], lease_seconds=30,
                    run_id=run_id, stop_at=cutoff,
                )
                assert await store.yield_task(
                    task_id=task["id"], claim_token=task["claim_token"],
                    lease_version=task["lease_version"], run_id=run_id,
                    stop_at=cutoff,
                )
                competing = await asyncio.gather(
                    store.claim_tasks(
                        provider="pytdx", limit=1, worker_id="scoped-recovered-a",
                        lease_seconds=30, max_attempts=3, task_ids=scoped_ids,
                        run_id=run_id,
                    ),
                    store.claim_tasks(
                        provider="pytdx", limit=1, worker_id="scoped-recovered-b",
                        lease_seconds=30, max_attempts=3, task_ids=scoped_ids,
                        run_id=run_id,
                    ),
                )
                reclaimed = [row for rows in competing for row in rows]
                assert len(reclaimed) == 1
                assert reclaimed[0]["claim_token"] != task["claim_token"]
                assert reclaimed[0]["lease_version"] > task["lease_version"]
                task = reclaimed[0]

                bar = Bar(
                    symbol=symbol, timeframe="5f",
                    ts=datetime(2026, 7, 17, 7, tzinfo=UTC),
                    open=1, high=1, low=1, close=1, volume=1, source="pytdx",
                )
                async with PostgresKlineWriter(TEST_DATABASE_URL) as writer:
                    wrong = dict(task, run_id=other_run_id)
                    with pytest.raises(LostBackfillLease):
                        await writer.commit_history_backfill_page(
                            task=wrong, expected_offset=0, next_offset=1,
                            bars=[bar], oldest_ts=bar.ts, newest_ts=bar.ts,
                            exhausted=True, lease_seconds=30,
                            provider_newest_ts=cutoff.replace(day=17),
                        )
                    boundary_bar = Bar(
                        symbol=symbol, timeframe="5f", ts=cutoff,
                        open=1, high=1, low=1, close=1, volume=1, source="pytdx",
                    )
                    with pytest.raises(ValueError, match="crosses scoped stop_at"):
                        await writer.commit_history_backfill_page(
                            task=task, expected_offset=0, next_offset=1,
                            bars=[boundary_bar], oldest_ts=cutoff, newest_ts=cutoff,
                            exhausted=True, lease_seconds=30,
                            provider_newest_ts=cutoff.replace(day=17),
                        )
                    with pytest.raises(ValueError, match="expected-through is unproven"):
                        await writer.commit_history_backfill_page(
                            task=task, expected_offset=0, next_offset=0,
                            bars=[], oldest_ts=None, newest_ts=None,
                            exhausted=True, lease_seconds=30,
                        )
                    future_bar = Bar(
                        symbol=symbol, timeframe="5f", ts=cutoff.replace(day=18),
                        open=1, high=1, low=1, close=1, volume=1, source="pytdx",
                    )
                    with pytest.raises(ValueError, match="exceeds expected-through"):
                        await writer.commit_history_backfill_page(
                            task=task, expected_offset=0, next_offset=1,
                            bars=[future_bar], oldest_ts=future_bar.ts,
                            newest_ts=future_bar.ts, exhausted=False, lease_seconds=30,
                            provider_newest_ts=future_bar.ts,
                        )
                    assert await setup.fetchval(
                        "select count(*) from klines where symbol_id=$1", symbol_id
                    ) == 0
                    assert await writer.commit_history_backfill_page(
                        task=task, expected_offset=0, next_offset=1,
                        bars=[bar], oldest_ts=bar.ts, newest_ts=bar.ts,
                        exhausted=False, lease_seconds=30,
                        provider_newest_ts=cutoff.replace(day=17),
                    ) == 1
                    assert await store.ensure_scoped_run_tasks(
                        run_id=run_id, run_identity=run_identity,
                        manifest_sha256=manifest_sha256,
                        symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                        stop_at={"5f": cutoff},
                        expected_through={"5f": cutoff.replace(day=17)},
                        freshness_contract_sha256="a" * 64,
                        provider="pytdx", page_size=10,
                        endpoint="127.0.0.1:7709", source_policy="primary_failover",
                    ) == scoped_ids
                    assert await writer.commit_history_backfill_page(
                        task=task, expected_offset=1, next_offset=1,
                        bars=[], oldest_ts=None, newest_ts=None,
                        exhausted=True, lease_seconds=30,
                        provider_newest_ts=cutoff.replace(day=17),
                    ) == 0
                assert await setup.fetchval(
                    "select status from historical_backfill_tasks where id=$1",
                    task["id"],
                ) == "success"
                assert await store.ensure_scoped_run_tasks(
                    run_id=run_id, run_identity=run_identity,
                    manifest_sha256=manifest_sha256,
                    symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                    stop_at={"5f": cutoff},
                    expected_through={"5f": cutoff.replace(day=17)},
                    freshness_contract_sha256="a" * 64,
                    provider="pytdx", page_size=10,
                    endpoint="127.0.0.1:7709", source_policy="primary_failover",
                ) == scoped_ids
                await setup.execute(
                    """update kline_scope_catalog set max_ts=$1
                       where generation_id=$2 and symbol_id=$3 and timeframe=5""",
                    cutoff.replace(day=16), generation_id, symbol_id,
                )
                with pytest.raises(RuntimeError, match="catalog progress mismatch"):
                    await store.ensure_scoped_run_tasks(
                        run_id=run_id, run_identity=run_identity,
                        manifest_sha256=manifest_sha256,
                        symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                        stop_at={"5f": cutoff},
                        expected_through={"5f": cutoff.replace(day=17)},
                        freshness_contract_sha256="a" * 64,
                        provider="pytdx", page_size=10,
                        endpoint="127.0.0.1:7709", source_policy="primary_failover",
                    )
        finally:
            await setup.close()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set HISTORY_BACKFILL_SCOPED_TEST_DATABASE_URL for a disposable migrated database",
)
def test_scoped_run_rejects_inactive_or_non_authoritative_scope_before_task_insert() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        connection = await asyncpg.connect(TEST_DATABASE_URL)
        code = f"{uuid4().int % 1_000_000:06d}"
        symbol = f"{code}.SH"
        symbol_id = await connection.fetchval(
            """
            insert into symbols(code, exchange, name, asset_type, market, is_active)
            values($1, 'SH', $1, 'stock', 'test', false) returning id
            """,
            code,
        )
        generation_id = uuid4()
        run_id = uuid4()
        await connection.execute(
            """
            insert into kline_scope_catalog_generations(
                generation_id,status,expected_scope_count,symbol_ids,timeframes,completed_at
            ) values($1,'complete',1,$2,array[5],clock_timestamp())
            """,
            generation_id,
            [symbol_id],
        )
        await connection.execute(
            """
            insert into kline_scope_catalog(
                generation_id,symbol_id,timeframe,state,bounds_complete
            ) values($1,$2,5,'empty',true)
            """,
            generation_id,
            symbol_id,
        )
        await connection.execute(
            """update kline_scope_catalog_control
               set active_generation_id=$1, revision=revision+1
               where control_key='active'""",
            generation_id,
        )
        try:
            async with PostgresBackfillTaskStore(TEST_DATABASE_URL) as store:
                with pytest.raises(RuntimeError, match="inactive, unknown"):
                    await store.ensure_scoped_run_tasks(
                        run_id=run_id, run_identity=uuid4().hex * 2,
                        manifest_sha256=uuid4().hex * 2,
                        symbols=[symbol_info_from_symbol(symbol)], timeframes=["5f"],
                        stop_at={"5f": datetime(2026, 7, 10, 7, tzinfo=UTC)},
                        expected_through={"5f": datetime(2026, 7, 17, 7, tzinfo=UTC)},
                        freshness_contract_sha256="a" * 64,
                        provider="pytdx", page_size=10,
                        endpoint="127.0.0.1:7709", source_policy="primary_failover",
                    )
            assert await connection.fetchval(
                "select count(*) from historical_backfill_scoped_runs where run_id=$1",
                run_id,
            ) == 0
            assert await connection.fetchval(
                "select count(*) from historical_backfill_tasks where run_id=$1", run_id
            ) == 0
        finally:
            await connection.close()

    asyncio.run(scenario())
