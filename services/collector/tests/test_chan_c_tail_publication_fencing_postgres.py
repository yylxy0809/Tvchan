"""Opt-in PostgreSQL acceptance for Module C tail publication fencing.

The test database must be disposable and must already contain all migrations through 050.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from collector.storage.chan_c_stream_postgres import (
    PostgresChanCStreamStore,
)
from collector.storage.chan_postgres import (
    PostgresChanWriter,
    StaleChanHeadError,
    StaleTailTaskLeaseError,
)
from collector.storage.postgres import PostgresKlineWriter
from trading_protocol import Bar, MODULE_C_CONFIG_HASH


TEST_DATABASE_URL = os.getenv("MODULE_C_EXECUTION_TEST_DATABASE_URL", "")


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return None


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set MODULE_C_EXECUTION_TEST_DATABASE_URL for a disposable migrated PostgreSQL database",
)
def test_earlier_canonical_revision_advances_independent_input_version() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        connection = await asyncpg.connect(TEST_DATABASE_URL)
        transaction = connection.transaction()
        await transaction.start()
        try:
            suffix = uuid4().hex[:10].upper()
            code = f"W{suffix}"
            symbol = f"{code}.TS"
            await connection.execute(
                "insert into symbols (code,exchange,name,market,is_active) "
                "values ($1,'TS',$1,'A_SHARE',true)",
                code,
            )
            earlier = datetime(2026, 7, 3, 6, 55, tzinfo=UTC)
            latest = datetime(2026, 7, 3, 7, 0, tzinfo=UTC)
            writer = PostgresKlineWriter(TEST_DATABASE_URL)
            await writer._upsert_bars_rows(
                connection,
                [
                    (
                        symbol, 5, earlier, 10000, 11000, 9000, 10500,
                        100, 1000, True, 0, 2,
                    ),
                    (
                        symbol, 5, latest, 10000, 11000, 9000, 10500,
                        100, 1000, True, 0, 2,
                    ),
                ],
            )
            before = await connection.fetchrow(
                "select last_bar_end,change_version from scheme2_ingest_watermarks "
                "where symbol_id=(select id from symbols where code=$1 and exchange='TS') "
                "and timeframe=5",
                code,
            )
            await writer._upsert_bars_rows(
                connection,
                [
                    (
                        symbol, 5, earlier, 10000, 11000, 9000, 10600,
                        101, 1000, True, 0, 2,
                    )
                ],
            )
            after = await connection.fetchrow(
                "select last_bar_end,change_version from scheme2_ingest_watermarks "
                "where symbol_id=(select id from symbols where code=$1 and exchange='TS') "
                "and timeframe=5",
                code,
            )
            assert before["last_bar_end"] == after["last_bar_end"] == latest
            assert int(after["change_version"]) == int(before["change_version"]) + 1
        finally:
            await transaction.rollback()
            await connection.close()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set MODULE_C_EXECUTION_TEST_DATABASE_URL for a disposable migrated PostgreSQL database",
)
def test_same_scope_writers_serialize_versions_and_leave_tail_dirty() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        control = await asyncpg.connect(TEST_DATABASE_URL)
        writer_a_connection = await asyncpg.connect(TEST_DATABASE_URL)
        writer_b_connection = await asyncpg.connect(TEST_DATABASE_URL)
        suffix = uuid4().hex[:10].upper()
        code = f"L{suffix}"
        symbol = f"{code}.TS"
        symbol_id = await control.fetchval(
            "insert into symbols (code,exchange,name,market,is_active) "
            "values ($1,'TS',$1,'A_SHARE',true) returning id",
            code,
        )
        earlier = datetime(2026, 7, 3, 6, 55, tzinfo=UTC)
        latest = datetime(2026, 7, 3, 7, 0, tzinfo=UTC)
        writer = PostgresKlineWriter(TEST_DATABASE_URL)
        try:
            async with control.transaction():
                await writer._upsert_bars_rows(control, [
                    (symbol, 5, earlier, 10000, 11000, 9000, 10500, 100, 1000, True, 0, 2),
                    (symbol, 5, latest, 10000, 11000, 9000, 10500, 100, 1000, True, 0, 2),
                ])
            initial_version = int(await control.fetchval(
                "select change_version from scheme2_ingest_watermarks "
                "where symbol_id=$1 and timeframe=5",
                symbol_id,
            ))
            baseline_run_id = await control.fetchval(
                "insert into chan_c_runs "
                "(symbol_id,chan_level,mode,input_signature,config_hash,bar_from,bar_until,"
                "bar_count,status,finished_at,snapshot_version,computed_at,run_kind,run_group_id) "
                "values ($1,5,0,$2,$3,$4,$5,2,'success',now(),$2,now(),'online','online') "
                "returning id",
                symbol_id, f"scope-lock-{suffix}", MODULE_C_CONFIG_HASH, earlier, latest,
            )
            await control.execute(
                "insert into scheme2_chan_c_published_heads "
                "(symbol_id,chan_level,mode,base_timeframe,base_from_bar_end,base_to_bar_end,"
                "bar_count,snapshot_version,status,run_id,published_at,config_hash,consumed_input_version) "
                "values ($1,5,'confirmed',5,$2,$3,2,$4,'published',$5,now(),$6,$7)",
                symbol_id, earlier, latest, f"scope-lock-{suffix}", baseline_run_id,
                MODULE_C_CONFIG_HASH, initial_version,
            )

            transaction_a = writer_a_connection.transaction()
            transaction_b = writer_b_connection.transaction()
            await transaction_a.start()
            await writer._upsert_bars_rows(writer_a_connection, [
                (symbol, 5, earlier, 10000, 11000, 9000, 10600, 101, 1000, True, 0, 2)
            ])
            assert await writer_a_connection.fetchval(
                "select change_version from scheme2_ingest_watermarks "
                "where symbol_id=$1 and timeframe=5",
                symbol_id,
            ) == initial_version + 1

            await transaction_b.start()
            write_b = asyncio.create_task(writer._upsert_bars_rows(writer_b_connection, [
                (symbol, 5, latest, 10000, 11000, 9000, 10700, 102, 1000, True, 0, 2)
            ]))
            wait_event_type = None
            for _ in range(50):
                wait_event_type = await control.fetchval(
                    "select wait_event_type from pg_stat_activity where pid=$1",
                    writer_b_connection.get_server_pid(),
                )
                if wait_event_type == "Lock":
                    break
                await asyncio.sleep(0.02)
            assert wait_event_type == "Lock"
            assert await control.fetchval(
                "select change_version from scheme2_ingest_watermarks "
                "where symbol_id=$1 and timeframe=5",
                symbol_id,
            ) == initial_version

            await transaction_a.commit()
            await asyncio.wait_for(write_b, timeout=5)
            assert await writer_b_connection.fetchval(
                "select change_version from scheme2_ingest_watermarks "
                "where symbol_id=$1 and timeframe=5",
                symbol_id,
            ) == initial_version + 2
            await transaction_b.commit()

            store = PostgresChanCStreamStore(TEST_DATABASE_URL)
            store._pool = _Pool(control)
            assert await store.ensure_tail_tasks_for_stale_heads(
                levels=["5f"], modes=["confirmed"], limit=10, symbols=[symbol]
            ) == 1
            assert await control.fetchval(
                "select target_input_version from scheme2_chan_c_tail_tasks "
                "where symbol_id=$1 and chan_level=5 and mode='confirmed'",
                symbol_id,
            ) == initial_version + 2
        finally:
            for connection in (writer_a_connection, writer_b_connection):
                if connection.is_in_transaction():
                    await connection.execute("rollback")
            await control.execute("delete from scheme2_chan_c_tail_tasks where symbol_id=$1", symbol_id)
            await control.execute("delete from scheme2_chan_c_published_heads where symbol_id=$1", symbol_id)
            await control.execute("delete from chan_c_runs where symbol_id=$1", symbol_id)
            await control.execute("delete from scheme2_ingest_watermarks where symbol_id=$1", symbol_id)
            await control.execute("delete from kline_scope_catalog where symbol_id=$1", symbol_id)
            await control.execute("delete from klines where symbol_id=$1", symbol_id)
            await control.execute("delete from symbols where id=$1", symbol_id)
            await writer_a_connection.close()
            await writer_b_connection.close()
            await control.close()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set MODULE_C_EXECUTION_TEST_DATABASE_URL for a disposable migrated PostgreSQL database",
)
def test_expired_normalized_tail_claim_fences_stale_publication() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        connection = await asyncpg.connect(TEST_DATABASE_URL)
        suffix = uuid4().hex[:10].upper()
        code = f"T{suffix}"
        symbol = f"{code}.TS"
        symbol_id: int | None = None

        async def cleanup() -> None:
            if symbol_id is None:
                return
            async with connection.transaction():
                await connection.execute(
                    "delete from chan_structure_lifecycle_events where head_history_id in "
                    "(select id from chan_c_head_history where symbol_id=$1)",
                    symbol_id,
                )
                await connection.execute(
                    "delete from chan_c_head_outbox where head_history_id in "
                    "(select id from chan_c_head_history where symbol_id=$1)",
                    symbol_id,
                )
                await connection.execute(
                    "delete from chan_structure_lifecycle_current where fingerprint in "
                    "(select fingerprint from chan_structure_identity where symbol_id=$1)",
                    symbol_id,
                )
                await connection.execute(
                    "delete from chan_structure_lifecycle_events where fingerprint in "
                    "(select fingerprint from chan_structure_identity where symbol_id=$1)",
                    symbol_id,
                )
                await connection.execute(
                    "delete from chan_structure_identity where symbol_id=$1", symbol_id
                )
                await connection.execute(
                    "delete from chan_c_head_history where symbol_id=$1", symbol_id
                )
                await connection.execute(
                    "delete from scheme2_chan_c_tail_tasks where symbol_id=$1", symbol_id
                )
                await connection.execute(
                    "delete from scheme2_ingest_watermarks where symbol_id=$1", symbol_id
                )
                await connection.execute(
                    "delete from scheme2_chan_c_recompute_watermarks where symbol_id=$1",
                    symbol_id,
                )
                await connection.execute(
                    "delete from scheme2_chan_c_published_heads where symbol_id=$1",
                    symbol_id,
                )
                for table in (
                    "chan_c_strokes",
                    "chan_c_segments",
                    "chan_c_centers",
                    "chan_c_signals",
                ):
                    await connection.execute(
                        f"delete from {table} where symbol_id=$1", symbol_id
                    )
                await connection.execute(
                    "delete from chan_c_runs where symbol_id=$1", symbol_id
                )
                await connection.execute("delete from klines where symbol_id=$1", symbol_id)
                await connection.execute("delete from symbols where id=$1", symbol_id)

        try:
            symbol_id = await connection.fetchval(
                "insert into symbols (code,exchange,name,market,is_active) "
                "values ($1,'TS',$1,'A_SHARE',true) returning id",
                code,
            )
            target_bar_end = datetime(2026, 7, 3, 7, tzinfo=UTC)
            publication_bar_end = datetime(2026, 7, 2, 7, tzinfo=UTC)
            head_bar_end = datetime(2026, 6, 26, 7, tzinfo=UTC)
            bar_from = head_bar_end - timedelta(days=7)

            await connection.executemany(
                "insert into klines "
                "(symbol_id,timeframe,ts,open_x1000,high_x1000,low_x1000,close_x1000,"
                "volume,amount_x100,is_complete,revision,source) "
                "values ($1,10080,$2,10000,11000,9000,10500,100,1000,true,0,2)",
                [(symbol_id, head_bar_end), (symbol_id, target_bar_end)],
            )
            await connection.execute(
                "insert into scheme2_ingest_watermarks "
                "(symbol_id,timeframe,last_bar_end,source) values ($1,10080,$2,'test')",
                symbol_id,
                target_bar_end,
            )
            baseline_run_id = await connection.fetchval(
                "insert into chan_c_runs "
                "(symbol_id,chan_level,mode,input_signature,config_hash,bar_from,bar_until,"
                "bar_count,status,finished_at,snapshot_version,computed_at,run_kind,run_group_id) "
                "values ($1,10080,0,$2,$3,$4,$5,1,'success',now(),$6,now(),'online','online') "
                "returning id",
                symbol_id,
                f"tail-fence-baseline-{suffix}",
                MODULE_C_CONFIG_HASH,
                bar_from,
                head_bar_end,
                f"tail-fence-baseline-{suffix}",
            )
            await connection.execute(
                "insert into scheme2_chan_c_published_heads "
                "(symbol_id,chan_level,mode,base_timeframe,base_from_bar_end,base_to_bar_end,"
                "bar_count,snapshot_version,status,run_id,published_at,config_hash) "
                "values ($1,10080,'confirmed',10080,$2,$3,1,$4,'published',$5,now(),$6)",
                symbol_id,
                bar_from,
                head_bar_end,
                f"tail-fence-head-{suffix}",
                baseline_run_id,
                MODULE_C_CONFIG_HASH,
            )
            task_id = await connection.fetchval(
                "insert into scheme2_chan_c_tail_tasks "
                "(symbol_id,chan_level,mode,base_timeframe,status,priority,queue_name,"
                "schedule_interval_seconds,next_run_at,pending_since,shard_bucket,anchor_bar_end,"
                "target_bar_end,expected_head_run_id,expected_head_base_to_bar_end) "
                "values ($1,10080,'confirmed',10080,'pending',10,'chan_c_1w',604800,"
                "now()-interval '1 second',now(),0,$2,$3,$4,$2) returning id",
                symbol_id,
                head_bar_end,
                target_bar_end,
                baseline_run_id,
            )

            pool = _Pool(connection)
            store = PostgresChanCStreamStore(TEST_DATABASE_URL)
            store._pool = pool
            writer = PostgresChanWriter(
                TEST_DATABASE_URL,
                tail_config_hash=MODULE_C_CONFIG_HASH,
                native_base_timeframe=True,
                publication_profile="online",
                publication_source="stream",
                run_kind="online",
                run_group_id="online",
                worker_id="tail-fence-test",
            )
            writer._pool = pool

            claim_a = (
                await store.claim_tail_tasks(
                    limit=1,
                    worker_id="worker-a",
                    lease_seconds=600,
                    symbols=[symbol],
                )
            )[0]
            active_before = await connection.fetchrow(
                "select status,claim_token,lease_version,claimed_target_bar_end "
                "from scheme2_chan_c_tail_tasks where id=$1",
                task_id,
            )
            assert await store.normalize_higher_timeframe_targets(
                levels=["1w"], modes=["confirmed"], symbols=[symbol]
            ) == 0
            active_after = await connection.fetchrow(
                "select status,claim_token,lease_version,claimed_target_bar_end "
                "from scheme2_chan_c_tail_tasks where id=$1",
                task_id,
            )
            assert tuple(active_after) == tuple(active_before)

            await connection.execute(
                "update scheme2_chan_c_tail_tasks "
                "set lease_until=now()-interval '1 second' where id=$1",
                task_id,
            )
            assert await store.normalize_higher_timeframe_targets(
                levels=["1w"], modes=["confirmed"], symbols=[symbol]
            ) == 1
            claim_b = (
                await store.claim_tail_tasks(
                    limit=1,
                    worker_id="worker-b",
                    lease_seconds=600,
                    symbols=[symbol],
                )
            )[0]
            assert claim_b["id"] == claim_a["id"] == task_id
            assert claim_b["claim_token"] != claim_a["claim_token"]
            assert int(claim_b["lease_version"]) > int(claim_a["lease_version"])

            async def publication_counts() -> tuple[int, int, int]:
                return tuple(
                    await connection.fetchrow(
                        "select "
                        "(select count(*)::int from chan_c_runs where symbol_id=$1 and status='success'),"
                        "(select count(*)::int from chan_c_head_history where symbol_id=$1),"
                        "(select count(*)::int from chan_c_head_outbox outbox join chan_c_head_history history "
                        "on history.id=outbox.head_history_id where history.symbol_id=$1)",
                        symbol_id,
                    )
                )

            before_stale = await publication_counts()
            head_before_stale = tuple(
                await connection.fetchrow(
                    "select run_id,base_to_bar_end,snapshot_version "
                    "from scheme2_chan_c_published_heads where symbol_id=$1 "
                    "and chan_level=10080 and mode='confirmed' and base_timeframe=10080",
                    symbol_id,
                )
            )
            with pytest.raises(StaleTailTaskLeaseError, match="lease fence failed"):
                await writer.replace_incremental_analysis(
                    symbol=symbol,
                    level="1w",
                    modes=["confirmed"],
                    anchor_bar_end=head_bar_end,
                    bar_until=publication_bar_end,
                    response={
                        "snapshot_version": f"tail-fence-a-{suffix}",
                        "strokes": [],
                        "segments": [],
                        "centers": [],
                        "signals": [],
                    },
                    expected_head_run_id=int(claim_a["expected_head_run_id"]),
                    expected_head_base_to_bar_end=claim_a[
                        "expected_head_base_to_bar_end"
                    ],
                    publication_task_id=int(claim_a["id"]),
                    publication_claim_token=str(claim_a["claim_token"]),
                    publication_lease_version=int(claim_a["lease_version"]),
                    publication_target_bar_end=claim_a["claimed_target_bar_end"],
                    expected_input_version=int(claim_a["claimed_input_version"]),
                )
            assert await publication_counts() == before_stale
            assert tuple(
                await connection.fetchrow(
                    "select run_id,base_to_bar_end,snapshot_version "
                    "from scheme2_chan_c_published_heads where symbol_id=$1 "
                    "and chan_level=10080 and mode='confirmed' and base_timeframe=10080",
                    symbol_id,
                )
            ) == head_before_stale
            assert tuple(
                await connection.fetchrow(
                    "select status,worker_id,claim_token,lease_version "
                    "from scheme2_chan_c_tail_tasks where id=$1",
                    task_id,
                )
            ) == (
                "running",
                "worker-b",
                claim_b["claim_token"],
                claim_b["lease_version"],
            )
            assert await connection.fetchval(
                "select count(*)::int from scheme2_chan_c_recompute_watermarks "
                "where symbol_id=$1",
                symbol_id,
            ) == 0

            await connection.execute(
                "update scheme2_chan_c_tail_tasks "
                "set lease_until=clock_timestamp()+interval '1 second' where id=$1",
                task_id,
            )
            lock_tx = connection.transaction()
            await lock_tx.start()
            lock_released = False
            waiting_connection = await asyncpg.connect(TEST_DATABASE_URL)
            try:
                await connection.fetchrow(
                    "select id from scheme2_chan_c_tail_tasks where id=$1 for update",
                    task_id,
                )
                waiting_writer = PostgresChanWriter(
                    TEST_DATABASE_URL,
                    tail_config_hash=MODULE_C_CONFIG_HASH,
                    native_base_timeframe=True,
                    publication_profile="online",
                    publication_source="stream",
                    run_kind="online",
                    run_group_id="online",
                    worker_id="tail-fence-wait-test",
                )
                waiting_writer._pool = _Pool(waiting_connection)
                waiting_publish = asyncio.create_task(
                    waiting_writer.replace_incremental_analysis(
                        symbol=symbol,
                        level="1w",
                        modes=["confirmed"],
                        anchor_bar_end=head_bar_end,
                        bar_until=publication_bar_end,
                        response={
                            "snapshot_version": f"tail-fence-wait-{suffix}",
                            "strokes": [],
                            "segments": [],
                            "centers": [],
                            "signals": [],
                        },
                        expected_head_run_id=int(claim_b["expected_head_run_id"]),
                        expected_head_base_to_bar_end=claim_b[
                            "expected_head_base_to_bar_end"
                        ],
                        publication_task_id=int(claim_b["id"]),
                        publication_claim_token=str(claim_b["claim_token"]),
                        publication_lease_version=int(claim_b["lease_version"]),
                        publication_target_bar_end=claim_b[
                            "claimed_target_bar_end"
                        ],
                        expected_input_version=int(claim_b["claimed_input_version"]),
                    )
                )
                await asyncio.sleep(1.25)
                await lock_tx.commit()
                lock_released = True
                with pytest.raises(StaleTailTaskLeaseError, match="lease fence failed"):
                    await asyncio.wait_for(waiting_publish, timeout=5)
            finally:
                if not lock_released:
                    await lock_tx.rollback()
                await waiting_connection.close()
            assert await publication_counts() == before_stale
            assert await connection.fetchval(
                "select count(*)::int from scheme2_chan_c_recompute_watermarks "
                "where symbol_id=$1",
                symbol_id,
            ) == 0
            await connection.execute(
                "update scheme2_chan_c_tail_tasks "
                "set lease_until=clock_timestamp()+interval '10 minutes' where id=$1",
                task_id,
            )

            published = await writer.replace_incremental_analysis(
                symbol=symbol,
                level="1w",
                modes=["confirmed"],
                anchor_bar_end=head_bar_end,
                bar_until=publication_bar_end,
                response={
                    "snapshot_version": f"tail-fence-b-{suffix}",
                    "strokes": [],
                    "segments": [],
                    "centers": [],
                    "signals": [],
                },
                expected_head_run_id=int(claim_b["expected_head_run_id"]),
                expected_head_base_to_bar_end=claim_b[
                    "expected_head_base_to_bar_end"
                ],
                publication_task_id=int(claim_b["id"]),
                publication_claim_token=str(claim_b["claim_token"]),
                publication_lease_version=int(claim_b["lease_version"]),
                publication_target_bar_end=claim_b["claimed_target_bar_end"],
                expected_input_version=int(claim_b["claimed_input_version"]),
            )
            after_success = await publication_counts()
            assert after_success == (
                before_stale[0] + 1,
                before_stale[1] + 1,
                before_stale[2] + 1,
            )
            assert tuple(
                await connection.fetchrow(
                    "select run_id,base_to_bar_end,snapshot_version "
                    "from scheme2_chan_c_published_heads where symbol_id=$1 "
                    "and chan_level=10080 and mode='confirmed' and base_timeframe=10080",
                    symbol_id,
                )
            ) == (
                published["run_id"],
                publication_bar_end,
                f"tail-fence-b-{suffix}",
            )
            assert tuple(
                await connection.fetchrow(
                    "select status,claim_token,last_success_bar_end "
                    "from scheme2_chan_c_tail_tasks where id=$1",
                    task_id,
                )
            ) == ("success", None, publication_bar_end)
            counts_after_success = await publication_counts()
            assert await store.ensure_tail_tasks_for_stale_heads(
                levels=["1w"],
                modes=["confirmed"],
                limit=10,
                symbols=[symbol],
            ) == 0
            assert await store.normalize_higher_timeframe_targets(
                levels=["1w"], modes=["confirmed"], symbols=[symbol]
            ) == 0
            assert await store.claim_tail_tasks(
                limit=1,
                worker_id="same-period-recheck",
                lease_seconds=60,
                symbols=[symbol],
            ) == []
            assert await publication_counts() == counts_after_success

            # A claims canonical rev2, B commits rev3 before A publishes. A must
            # roll back completely; the durable task then reclaims rev3 and wins.
            await connection.execute(
                "update klines set close_x1000=10600,revision=revision+1,"
                "updated_at=clock_timestamp() where symbol_id=$1 and timeframe=10080 and ts=$2",
                symbol_id,
                head_bar_end,
            )
            await connection.execute(
                "update scheme2_ingest_watermarks set change_version=change_version+1,"
                "updated_at=clock_timestamp() where symbol_id=$1 and timeframe=10080",
                symbol_id,
            )
            assert await store.ensure_tail_tasks_for_stale_heads(
                levels=["1w"], modes=["confirmed"], limit=10, symbols=[symbol]
            ) == 1
            claim_rev2 = (
                await store.claim_tail_tasks(
                    limit=1, worker_id="rev2-worker", lease_seconds=600, symbols=[symbol]
                )
            )[0]

            before_rev2_publish = await publication_counts()
            race_transaction = connection.transaction()
            await race_transaction.start()
            race_committed = False
            race_connection = await asyncpg.connect(TEST_DATABASE_URL)
            try:
                await connection.fetchrow(
                    "select id from scheme2_chan_c_tail_tasks where id=$1 for update",
                    task_id,
                )
                race_writer = PostgresChanWriter(
                    TEST_DATABASE_URL, tail_config_hash=MODULE_C_CONFIG_HASH,
                    native_base_timeframe=True, publication_profile="online",
                    publication_source="stream", run_kind="online",
                    run_group_id="online", worker_id="rev2-race-worker",
                )
                race_writer._pool = _Pool(race_connection)
                rev2_publish = asyncio.create_task(race_writer.replace_incremental_analysis(
                    symbol=symbol, level="1w", modes=["confirmed"],
                    anchor_bar_end=claim_rev2["anchor_bar_end"], bar_until=publication_bar_end,
                    response={"snapshot_version": f"rev2-{suffix}", "strokes": [],
                              "segments": [], "centers": [], "signals": []},
                    expected_head_run_id=int(claim_rev2["expected_head_run_id"]),
                    expected_head_base_to_bar_end=claim_rev2["expected_head_base_to_bar_end"],
                    publication_task_id=int(claim_rev2["id"]),
                    publication_claim_token=str(claim_rev2["claim_token"]),
                    publication_lease_version=int(claim_rev2["lease_version"]),
                    publication_target_bar_end=claim_rev2["claimed_target_bar_end"],
                    expected_input_version=int(claim_rev2["claimed_input_version"]),
                ))
                wait_event_type = None
                for _ in range(50):
                    wait_event_type = await connection.fetchval(
                        "select wait_event_type from pg_stat_activity where pid=$1",
                        race_connection.get_server_pid(),
                    )
                    if wait_event_type == "Lock":
                        break
                    await asyncio.sleep(0.02)
                assert wait_event_type == "Lock"
                await connection.execute(
                    "update klines set close_x1000=10700,revision=revision+1,"
                    "updated_at=clock_timestamp() where symbol_id=$1 and timeframe=10080 and ts=$2",
                    symbol_id, head_bar_end,
                )
                await connection.execute(
                    "update scheme2_ingest_watermarks set change_version=change_version+1,"
                    "updated_at=clock_timestamp() where symbol_id=$1 and timeframe=10080",
                    symbol_id,
                )
                await race_transaction.commit()
                race_committed = True
                with pytest.raises(StaleChanHeadError, match="Stale Chan input") as stale:
                    await asyncio.wait_for(rev2_publish, timeout=5)
            finally:
                if not race_committed:
                    await race_transaction.rollback()
                await race_connection.close()
            assert await publication_counts() == before_rev2_publish
            assert await store.complete_tail_task(
                task_id=int(claim_rev2["id"]),
                claim_token=str(claim_rev2["claim_token"]),
                error=str(stale.value),
            )
            claim_rev3 = (
                await store.claim_tail_tasks(
                    limit=1, worker_id="rev3-worker", lease_seconds=600, symbols=[symbol]
                )
            )[0]
            assert int(claim_rev3["claimed_input_version"]) > int(
                claim_rev2["claimed_input_version"]
            )
            rev3 = await writer.replace_incremental_analysis(
                symbol=symbol, level="1w", modes=["confirmed"],
                anchor_bar_end=claim_rev3["anchor_bar_end"], bar_until=publication_bar_end,
                response={"snapshot_version": f"rev3-{suffix}", "strokes": [],
                          "segments": [], "centers": [], "signals": []},
                expected_head_run_id=int(claim_rev3["expected_head_run_id"]),
                expected_head_base_to_bar_end=claim_rev3["expected_head_base_to_bar_end"],
                publication_task_id=int(claim_rev3["id"]),
                publication_claim_token=str(claim_rev3["claim_token"]),
                publication_lease_version=int(claim_rev3["lease_version"]),
                publication_target_bar_end=claim_rev3["claimed_target_bar_end"],
                expected_input_version=int(claim_rev3["claimed_input_version"]),
            )
            assert await connection.fetchval(
                "select consumed_input_version from scheme2_chan_c_published_heads "
                "where symbol_id=$1 and chan_level=10080 and mode='confirmed'",
                symbol_id,
            ) == claim_rev3["claimed_input_version"]
            assert rev3["snapshot_version"] == f"rev3-{suffix}"

            next_period_target = datetime(2026, 7, 10, 7, tzinfo=UTC)
            await connection.execute(
                "insert into klines "
                "(symbol_id,timeframe,ts,open_x1000,high_x1000,low_x1000,close_x1000,"
                "volume,amount_x100,is_complete,revision,source) "
                "values ($1,10080,$2,10000,11000,9000,10500,100,1000,true,0,2)",
                symbol_id,
                next_period_target,
            )
            await connection.execute(
                "update scheme2_ingest_watermarks set last_bar_end=$2 where symbol_id=$1 "
                "and timeframe=10080",
                symbol_id,
                next_period_target,
            )
            assert await store.ensure_tail_tasks_for_stale_heads(
                levels=["1w"],
                modes=["confirmed"],
                limit=10,
                symbols=[symbol],
            ) == 1
            next_claim = await store.claim_tail_tasks(
                limit=1,
                worker_id="next-period-worker",
                lease_seconds=60,
                symbols=[symbol],
            )
            assert len(next_claim) == 1
            assert next_claim[0]["claimed_target_bar_end"] == next_period_target
        finally:
            try:
                await cleanup()
            finally:
                await connection.close()

    asyncio.run(scenario())
