from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from collector.storage.chan_postgres import (
    MODULE_C_CHAN_TABLES,
    PostgresChanWriter,
    StaleChanHeadError,
)


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeConn:
    def __init__(self) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.copy_calls: list[tuple[str, list[str], list[tuple[object, ...]]]] = []
        self._fetchval_results = iter([1, 99])

    async def fetchval(self, query: str, *args):
        self.fetchval_calls.append((query, args))
        return next(self._fetchval_results)

    async def execute(self, query: str, *args):
        self.execute_calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        return []

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        return None

    async def executemany(self, query: str, rows: list[tuple[object, ...]]):
        self.executemany_calls.append((query, rows))
        return None

    async def copy_records_to_table(self, table: str, *, records, columns):
        self.copy_calls.append((table, list(columns), list(records)))
        return None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()


class FakeAcquire:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.conn)


def test_replace_analysis_defaults_to_module_c_tables() -> (
    None
):
    async def scenario() -> None:
        conn = FakeConn()
        writer = PostgresChanWriter("postgresql://unused")
        writer._pool = FakePool(conn)

        response = {
            "snapshot_version": "snapshot-write-001",
            "strokes": [
                {
                    "id": "stroke-1",
                    "mode": "confirmed",
                    "start": {"time": 1_717_300_600, "price": 10.12},
                    "end": {"time": 1_717_304_200, "price": 10.56},
                    "begin_base_ts": 1_717_300_600,
                    "end_base_ts": 1_717_304_200,
                    "begin_base_seq": 1001,
                    "end_base_seq": 1012,
                    "direction": "up",
                    "confirmed": True,
                }
            ],
            "segments": [],
            "centers": [
                {
                    "id": "center-1",
                    "mode": "predictive",
                    "start_time": 1_717_300_600,
                    "end_time": 1_717_304_200,
                    "begin_base_ts": 1_717_300_600,
                    "end_base_ts": 1_717_304_200,
                    "begin_base_seq": 2001,
                    "end_base_seq": 2009,
                    "low": 10.0,
                    "high": 10.8,
                    "confirmed": False,
                }
            ],
            "signals": [
                {
                    "id": "signal-1",
                    "mode": "confirmed",
                    "time": 1_717_304_200,
                    "price": 10.66,
                    "base_ts": 1_717_304_200,
                    "base_seq": 3007,
                    "signal_type": "2buy",
                    "confirmed": True,
                }
            ],
        }

        counts = await writer.replace_analysis(
            symbol="000001.SZ",
            level="30f",
            modes=["confirmed", "predictive"],
            bar_from=datetime.fromtimestamp(1_717_300_600, tz=UTC),
            bar_until=datetime.fromtimestamp(1_717_304_200, tz=UTC),
            bar_count=300,
            response=response,
        )

        assert counts == {"strokes": 1, "segments": 0, "centers": 1, "signals": 1}

        run_insert_query, run_insert_args = conn.fetchval_calls[1]
        assert "insert into chan_c_runs" in run_insert_query
        assert "snapshot_version" in run_insert_query
        assert run_insert_args[7] == "snapshot-write-001"
        assert run_insert_args[8] == "online"

        stroke_insert = next(
            call for call in conn.copy_calls if call[0] == "chan_c_strokes"
        )
        stroke_row = stroke_insert[2][0]
        assert int(stroke_row[11].timestamp()) == 1_717_300_600
        assert int(stroke_row[12].timestamp()) == 1_717_304_200
        assert stroke_row[13] == 1001
        assert stroke_row[14] == 1012

        center_insert = next(
            call for call in conn.copy_calls if call[0] == "chan_c_centers"
        )
        center_row = center_insert[2][0]
        assert int(center_row[10].timestamp()) == 1_717_300_600
        assert int(center_row[11].timestamp()) == 1_717_304_200
        assert center_row[12] == 2001
        assert center_row[13] == 2009

        signal_insert = next(
            call for call in conn.copy_calls if call[0] == "chan_c_signals"
        )
        signal_row = signal_insert[2][0]
        assert int(signal_row[8].timestamp()) == 1_717_304_200
        assert signal_row[9] == 3007

        published_upserts = [
            args
            for query, args in conn.execute_calls
            if "insert into scheme2_chan_c_published_heads" in query
        ]
        assert len(published_upserts) == 2
        assert all(row[9] == "published" for row in published_upserts)

        assert any(
            "update chan_c_runs" in query and args[0] == 99
            for query, args in conn.execute_calls
        )
        outbox_writes = [
            (query, args)
            for query, args in conn.execute_calls
            if "insert into chan_c_head_outbox" in query
        ]
        assert len(outbox_writes) == 2
        assert all(args[8] == 99 for _query, args in outbox_writes)

    asyncio.run(scenario())


def test_module_c_writer_uses_module_c_tables_and_native_base_timeframe() -> None:
    async def scenario() -> None:
        conn = FakeConn()
        writer = PostgresChanWriter(
            "postgresql://unused",
            tables=MODULE_C_CHAN_TABLES,
            run_config_hash="module-c:chan.py-native-levels-v2-bi-strict-false",
            native_base_timeframe=True,
        )
        writer._pool = FakePool(conn)

        response = {
            "snapshot_version": "module-c-snapshot-001",
            "strokes": [
                {
                    "id": "c-stroke-1",
                    "mode": "confirmed",
                    "start": {"time": 1_717_300_600, "price": 10.12},
                    "end": {"time": 1_717_304_200, "price": 10.56},
                    "begin_base_ts": 1_717_300_600,
                    "end_base_ts": 1_717_304_200,
                    "direction": "up",
                    "confirmed": True,
                }
            ],
            "segments": [],
            "centers": [],
            "signals": [],
        }

        counts = await writer.replace_analysis(
            symbol="000001.SZ",
            level="30f",
            modes=["confirmed"],
            bar_from=datetime.fromtimestamp(1_717_300_600, tz=UTC),
            bar_until=datetime.fromtimestamp(1_717_304_200, tz=UTC),
            bar_count=10,
            response=response,
        )

        assert counts == {"strokes": 1, "segments": 0, "centers": 0, "signals": 0}
        run_insert_query, run_insert_args = conn.fetchval_calls[1]
        assert "insert into chan_c_runs" in run_insert_query
        assert run_insert_args[3] == "module-c:chan.py-native-levels-v2-bi-strict-false"
        published_upserts = [
            args
            for query, args in conn.execute_calls
            if "insert into scheme2_chan_c_published_heads" in query
        ]
        assert len(published_upserts) == 1
        assert published_upserts[-1][3] == 30
        assert any(
            table == "chan_c_strokes" for table, _columns, _rows in conn.copy_calls
        )

    asyncio.run(scenario())


def test_publish_heads_cas_returns_only_the_winning_committed_identity() -> None:
    class CasConn:
        def __init__(self, result: str) -> None:
            self.result = result

        async def execute(self, *_args):
            return self.result

    async def scenario() -> None:
        writer = PostgresChanWriter("postgresql://unused")
        kwargs = {
            "symbol_id": 1,
            "level_code": 5,
            "modes": ["confirmed"],
            "base_timeframe_code": 5,
            "bar_from": datetime.fromtimestamp(100, UTC),
            "bar_until": datetime.fromtimestamp(200, UTC),
            "bar_count": 20,
            "snapshot_version": "committed-v99",
            "run_id": 99,
            "expected_run_id": 98,
        }
        committed = await writer._publish_heads_cas(CasConn("UPDATE 1"), **kwargs)
        assert committed == {"run_id": 99, "snapshot_version": "committed-v99"}

        with pytest.raises(StaleChanHeadError, match="CAS failed"):
            await writer._publish_heads_cas(CasConn("UPDATE 0"), **kwargs)

    asyncio.run(scenario())


def test_full_publish_does_not_emit_outbox_when_head_upsert_loses() -> None:
    class Conn:
        def __init__(self) -> None:
            self.queries = []
        async def fetchrow(self, *_args):
            return {"run_id": 7, "base_to_bar_end": datetime.fromtimestamp(200, UTC)}
        async def execute(self, query, *_args):
            self.queries.append(query)
            if "insert into scheme2_chan_c_published_heads" in query:
                return "INSERT 0 0"
            return "OK"

    async def scenario() -> None:
        conn = Conn()
        writer = PostgresChanWriter("postgresql://unused")
        await writer._upsert_published_heads(
            conn, symbol_id=1, level_code=5, modes=["confirmed"],
            base_timeframe_code=5, bar_from=datetime.fromtimestamp(100, UTC),
            bar_until=datetime.fromtimestamp(200, UTC), bar_count=20,
            snapshot_version="same", run_id=8, status="published", last_error=None,
        )
        assert not any("insert into chan_c_head_outbox" in query for query in conn.queries)

    asyncio.run(scenario())


def test_full_publish_rejects_a_head_changed_after_task_initialization() -> None:
    class Conn:
        async def fetchrow(self, *_args):
            return {"run_id": 9, "base_to_bar_end": datetime.fromtimestamp(200, UTC)}

    async def scenario() -> None:
        writer = PostgresChanWriter("postgresql://unused")
        with pytest.raises(StaleChanHeadError, match="head changed"):
            await writer._upsert_published_heads(
                Conn(), symbol_id=1, level_code=5, modes=["confirmed"],
                base_timeframe_code=5, bar_from=datetime.fromtimestamp(100, UTC),
                bar_until=datetime.fromtimestamp(200, UTC), bar_count=20,
                snapshot_version="new", run_id=10, status="published", last_error=None,
                expected_heads={"confirmed": 8}, publication_claim_token="task-token",
            )

    asyncio.run(scenario())


def test_full_recompute_write_and_task_completion_share_the_fenced_transaction() -> None:
    class FullConn(FakeConn):
        async def execute(self, query: str, *args):
            self.execute_calls.append((query, args))
            if "set status = 'completed'" in query:
                return "UPDATE 1"
            return "OK"

    async def scenario() -> None:
        conn = FullConn()
        conn._fetchval_results = iter([1, 1, 99])
        writer = PostgresChanWriter(
            "postgresql://unused", batch_id=7, run_group_id="batch-7",
            native_base_timeframe=True,
        )
        writer._pool = FakePool(conn)
        task = {
            "batch_id": 7, "symbol_id": 1, "chan_level": 5,
            "claim_token": "claim-1", "lease_version": 2,
            "expected_heads": {},
        }

        await writer.replace_analysis(
            symbol="000001.SZ", level="5f", modes=["confirmed", "predictive"],
            bar_from=datetime.fromtimestamp(100, UTC),
            bar_until=datetime.fromtimestamp(200, UTC), bar_count=20,
            response={
                "snapshot_version": "batch-7-snapshot",
                "strokes": [], "segments": [], "centers": [], "signals": [],
            },
            full_recompute_task=task,
        )

        fence_query, fence_args = conn.fetchval_calls[1]
        assert "from chan_c_full_recompute_tasks" in fence_query
        assert "from chan_c_full_recompute_batches batch" in fence_query
        assert "join chan_c_batches parent" in fence_query
        assert "batch.status in ('pending', 'running')" in fence_query
        assert "parent.status in ('planned', 'running')" in fence_query
        assert "for share of parent, batch" in fence_query
        assert "for update of task" in fence_query
        assert fence_args == (7, 1, 5, "claim-1", 2)
        run_query, _run_args = conn.fetchval_calls[2]
        assert "on conflict (batch_id, run_identity)" in run_query
        completion = next(
            (query, args) for query, args in conn.execute_calls
            if "set status = 'completed'" in query
        )
        assert completion[1][:5] == (7, 1, 5, "claim-1", 2)

    asyncio.run(scenario())


def test_full_recompute_publication_rechecks_batch_status_before_any_write() -> None:
    async def scenario() -> None:
        conn = FakeConn()
        conn._fetchval_results = iter([1, None])
        writer = PostgresChanWriter(
            "postgresql://unused", batch_id=7, run_group_id="batch-7",
            native_base_timeframe=True,
        )
        writer._pool = FakePool(conn)
        task = {
            "batch_id": 7, "symbol_id": 1, "chan_level": 5,
            "claim_token": "claim-1", "lease_version": 2,
            "expected_heads": {},
        }

        with pytest.raises(StaleChanHeadError, match="batch status fence failed"):
            await writer.replace_analysis(
                symbol="000001.SZ", level="5f", modes=["confirmed", "predictive"],
                bar_from=datetime.fromtimestamp(100, UTC),
                bar_until=datetime.fromtimestamp(200, UTC), bar_count=20,
                response={
                    "snapshot_version": "batch-7-snapshot",
                    "strokes": [], "segments": [], "centers": [], "signals": [],
                },
                full_recompute_task=task,
            )

        fence_query, _fence_args = conn.fetchval_calls[1]
        assert "from chan_c_full_recompute_batches batch" in fence_query
        assert "join chan_c_batches parent" in fence_query
        assert "batch.status in ('pending', 'running')" in fence_query
        assert "parent.status in ('planned', 'running')" in fence_query
        assert "for share of parent, batch" in fence_query
        assert "for update of task" in fence_query
        assert conn.execute_calls == []
        assert conn.copy_calls == []

    asyncio.run(scenario())
