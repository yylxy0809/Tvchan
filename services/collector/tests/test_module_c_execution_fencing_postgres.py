"""Opt-in PostgreSQL acceptance for Module C execution fences.

The test database must be disposable and must already contain migrations 001..043.
"""

from __future__ import annotations

import asyncio
import os
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from collector.chan_module_c_recompute import claim_recompute_task
from collector.module_c_batch_control import activate_batch
from collector.storage.chan_postgres import PostgresChanWriter, StaleChanHeadError
from trading_protocol import MODULE_C_CONFIG_HASH


TEST_DATABASE_URL = os.getenv("MODULE_C_EXECUTION_TEST_DATABASE_URL", "")


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set MODULE_C_EXECUTION_TEST_DATABASE_URL for a disposable migrated PostgreSQL database",
)
def test_publication_and_terminal_state_serialize_without_partial_writes(monkeypatch) -> None:
    asyncpg = pytest.importorskip("asyncpg")

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

    class _PausingConnection:
        def __init__(self, connection, locked, release):
            self.connection = connection
            self.locked = locked
            self.release = release

        def transaction(self, *args, **kwargs):
            return self.connection.transaction(*args, **kwargs)

        async def fetchval(self, query, *args):
            return await self.connection.fetchval(query, *args)

        async def fetchrow(self, query, *args):
            row = await self.connection.fetchrow(query, *args)
            if "from chan_c_full_recompute_tasks" in query.lower():
                self.locked.set()
                await self.release.wait()
            return row

        async def execute(self, query, *args):
            return await self.connection.execute(query, *args)

        async def executemany(self, query, args):
            return await self.connection.executemany(query, args)

        async def copy_records_to_table(self, *args, **kwargs):
            return await self.connection.copy_records_to_table(*args, **kwargs)

    class _ParentCasMissConnection:
        def __init__(self, connection):
            self.connection = connection

        def transaction(self, *args, **kwargs):
            return self.connection.transaction(*args, **kwargs)

        async def fetchrow(self, query, *args):
            return await self.connection.fetchrow(query, *args)

        async def execute(self, query, *args):
            if "update chan_c_batches" in query.lower():
                return "UPDATE 0"
            return await self.connection.execute(query, *args)

    async def seed(
        connection,
        *,
        parent_status="running",
        child_status="running",
        task_status="running",
    ):
        suffix = uuid4().hex[:12]
        symbol = f"{suffix[:6].upper()}.TS"
        symbol_id = await connection.fetchval(
            "insert into symbols (code,exchange,name,market,is_active) "
            "values ($1,'TS',$1,'A_SHARE',true) returning id",
            suffix[:6].upper(),
        )
        build_id = uuid4()
        await connection.execute(
            "insert into module_c_eligibility_builds "
            "(build_id,manifest_version,config_hash,active_universe_hash,manifest_hash,"
            "active_symbols,disposition_rows,parameters,summary) "
            "values ($1,$2,$3,'u','m',0,0,'{}','{}')",
            build_id,
            f"execution-fence-{suffix}",
            MODULE_C_CONFIG_HASH,
        )
        sealed_at = datetime.now(UTC) if parent_status == "sealed" else None
        batch_id = await connection.fetchval(
            "insert into chan_c_batches "
            "(batch_key,publication_namespace,profile_id,run_group_id,batch_kind,status,"
            "code_commit,image_digest,vendor_manifest_sha256,effective_config,config_hash,"
            "eligible_manifest_sha256,sealed_at) "
            "values ($1,'canonical','module-c-v4',$2,'canary',$3,'commit','image',$4,"
            "'{}',$5,$4,$6) returning id",
            f"execution-fence-{suffix}",
            f"run-{suffix}",
            parent_status,
            "a" * 64,
            MODULE_C_CONFIG_HASH,
            sealed_at,
        )
        await connection.execute(
            "insert into chan_c_full_recompute_batches "
            "(batch_id,eligibility_build_id,run_group_id,config_hash,publication_namespace,"
            "profile_id,shard_count,status,active_symbols,disposition_rows,started_at) "
            "values ($1,$2,$3,$4,'canonical','module-c-v4',1,$5,1,1,now())",
            batch_id,
            build_id,
            f"run-{suffix}",
            MODULE_C_CONFIG_HASH,
            child_status,
        )
        cutoff = datetime(2026, 7, 3, 7, tzinfo=UTC)
        token = uuid4().hex
        await connection.execute(
            "insert into chan_c_full_recompute_tasks "
            "(batch_id,symbol_id,symbol,chan_level,eligible,target_bar_until,shard_bucket,"
            "status,attempts,worker_id,claim_token,lease_version,lease_until,expected_heads) "
            "values ($1,$2,$3,5,true,$4,0,$5,$6,$7,$8,$9,$10,'{}')",
            batch_id,
            symbol_id,
            symbol,
            cutoff,
            task_status,
            1 if task_status == "running" else 0,
            "worker" if task_status == "running" else None,
            token if task_status == "running" else None,
            1 if task_status == "running" else 0,
            datetime.now(UTC) + timedelta(minutes=10) if task_status == "running" else None,
        )
        return {
            "batch_id": batch_id,
            "symbol_id": symbol_id,
            "symbol": symbol,
            "run_group_id": f"run-{suffix}",
            "cutoff": cutoff,
            "claim_token": token,
        }

    def writer(pool, seeded):
        value = PostgresChanWriter(
            TEST_DATABASE_URL,
            run_config_hash=MODULE_C_CONFIG_HASH,
            native_base_timeframe=True,
            publication_profile="baseline",
            publication_source="full_recompute",
            run_kind="full_recompute",
            batch_id=seeded["batch_id"],
            publication_namespace="canonical",
            profile_id="module-c-v4",
            run_group_id=seeded["run_group_id"],
            worker_id="worker",
        )
        value._pool = pool
        return value

    async def publish(value, seeded):
        return await value.replace_analysis(
            symbol=seeded["symbol"],
            level="5f",
            modes=["confirmed", "predictive"],
            bar_from=seeded["cutoff"] - timedelta(minutes=5),
            bar_until=seeded["cutoff"],
            bar_count=1,
            response={
                "snapshot_version": f"execution-fence-{seeded['batch_id']}",
                "strokes": [],
                "segments": [],
                "centers": [],
                "signals": [],
            },
            full_recompute_task={
                "batch_id": seeded["batch_id"],
                "symbol_id": seeded["symbol_id"],
                "chan_level": 5,
                "claim_token": seeded["claim_token"],
                "lease_version": 1,
                "target_bar_until": seeded["cutoff"],
                "expected_heads": {},
            },
        )

    async def scenario() -> None:
        async def no_revalidation(*_args, **_kwargs):
            return None

        monkeypatch.setattr(
            "collector.module_c_batch_control.revalidate_strict_v2_build",
            no_revalidation,
        )
        monkeypatch.setattr(
            "collector.module_c_batch_control.validate_pristine_task_manifest",
            no_revalidation,
        )
        monkeypatch.setattr(
            "collector.module_c_batch_control.validate_activation_identity",
            lambda _batch: None,
        )
        setup = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            planned = await seed(
                setup,
                parent_status="planned",
                child_status="pending",
                task_status="pending",
            )
            assert await claim_recompute_task(
                kline_writer=SimpleNamespace(_pool=_Pool(setup)),
                batch_id=planned["batch_id"],
                worker_id="worker",
                shard_index=0,
                shard_count=1,
                lease_seconds=60,
                max_attempts=3,
            ) is None
            assert await setup.fetchval(
                "select status from chan_c_full_recompute_tasks "
                "where batch_id=$1 and symbol_id=$2 and chan_level=5",
                planned["batch_id"],
                planned["symbol_id"],
            ) == "pending"

            assert await activate_batch(
                setup, Namespace(batch_id=planned["batch_id"])
            ) == {"batch_id": planned["batch_id"], "status": "running"}
            assert tuple(await setup.fetchrow(
                "select parent.status,child.status from chan_c_batches parent "
                "join chan_c_full_recompute_batches child on child.batch_id=parent.id "
                "where parent.id=$1",
                planned["batch_id"],
            )) == ("running", "running")

            rollback = await seed(
                setup,
                parent_status="planned",
                child_status="pending",
                task_status="pending",
            )
            with pytest.raises(RuntimeError, match="atomically activate"):
                await activate_batch(
                    _ParentCasMissConnection(setup),
                    Namespace(batch_id=rollback["batch_id"]),
                )
            assert tuple(await setup.fetchrow(
                "select parent.status,child.status from chan_c_batches parent "
                "join chan_c_full_recompute_batches child on child.batch_id=parent.id "
                "where parent.id=$1",
                rollback["batch_id"],
            )) == ("planned", "pending")

            terminal = await seed(setup, parent_status="sealed")
            terminal_writer = writer(_Pool(setup), terminal)
            before = await setup.fetchval(
                "select count(*) from chan_c_runs where batch_id=$1", terminal["batch_id"]
            )
            with pytest.raises(StaleChanHeadError, match="batch status fence failed"):
                await publish(terminal_writer, terminal)
            assert await setup.fetchval(
                "select count(*) from chan_c_runs where batch_id=$1", terminal["batch_id"]
            ) == before

            active = await seed(setup)
        finally:
            await setup.close()

        publish_connection = await asyncpg.connect(TEST_DATABASE_URL)
        terminal_connection = await asyncpg.connect(TEST_DATABASE_URL)
        locked = asyncio.Event()
        release = asyncio.Event()
        try:
            paused = _PausingConnection(publish_connection, locked, release)
            publish_task = asyncio.create_task(publish(writer(_Pool(paused), active), active))
            await asyncio.wait_for(locked.wait(), timeout=5)

            async with terminal_connection.transaction():
                await terminal_connection.execute("set local lock_timeout='250ms'")
                with pytest.raises(asyncpg.LockNotAvailableError):
                    await terminal_connection.execute(
                        "update chan_c_batches set status='sealed',sealed_at=now() "
                        "where id=$1 and status='running'",
                        active["batch_id"],
                    )

            release.set()
            await asyncio.wait_for(publish_task, timeout=10)
            assert await terminal_connection.fetchval(
                "select status from chan_c_full_recompute_tasks "
                "where batch_id=$1 and symbol_id=$2 and chan_level=5",
                active["batch_id"],
                active["symbol_id"],
            ) == "completed"
            assert await terminal_connection.fetchval(
                "select count(*) from chan_c_runs where batch_id=$1", active["batch_id"]
            ) == 1
            assert await terminal_connection.fetchval(
                "select count(*) from scheme2_chan_c_published_heads where batch_id=$1",
                active["batch_id"],
            ) == 2
        finally:
            release.set()
            await publish_connection.close()
            await terminal_connection.close()

    asyncio.run(scenario())
