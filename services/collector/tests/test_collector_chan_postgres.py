from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from collector.historical_replay import ReplayContract
from collector.storage.chan_postgres import (
    MODULE_C_CHAN_TABLES,
    PostgresChanWriter,
    StaleChanHeadError,
    StaleTailTaskLeaseError,
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
        lowered = query.lower()
        contract = ReplayContract(
            config_hash="module-c-v4",
            source_batch_id=6,
            eligible_universe_snapshot_id="eligibility:6",
            canonical_gate_snapshot_id="canonical:6",
            cutoff_time=datetime(2026, 7, 3, 7, tzinfo=UTC),
        )
        if "from chan_c_batches" in lowered:
            return {
                "id": 9,
                "status": "running",
                "batch_kind": "historical_replay",
                "publication_namespace": "historical-replay",
                "profile_id": "module-c-historical-replay-v1",
                "run_group_id": "historical-replay",
                "config_hash": contract.config_hash,
                "effective_config": {},
                "audit_references": [],
            }
        if "from chan_c_historical_replay_batches" in lowered:
            return {
                "batch_id": 9,
                "status": "running",
                "source_batch_id": 6,
                "contract_version": contract.contract_version,
                "contract_hash": contract.digest(),
                "contract": contract.payload(),
                "eligible_universe_snapshot_id": contract.eligible_universe_snapshot_id,
                "canonical_gate_snapshot_id": contract.canonical_gate_snapshot_id,
                "cutoff_policy": contract.cutoff_policy,
            }
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


def test_tail_publication_lock_requires_exact_live_task_identity() -> None:
    class TailConn:
        def __init__(self, row, checked_at) -> None:
            self.row = row
            self.checked_at = checked_at
            self.calls = []

        async def fetchrow(self, query, *args):
            self.calls.append((query, args))
            return self.row

        async def fetchval(self, query, *args):
            self.calls.append((query, args))
            return self.checked_at

    async def scenario() -> None:
        writer = PostgresChanWriter("postgresql://unused")
        checked_at = datetime.fromtimestamp(150, UTC)
        anchor = datetime.fromtimestamp(100, UTC)
        target = datetime.fromtimestamp(200, UTC)
        task_row = {
            "id": 17,
            "symbol_id": 1,
            "chan_level": 5,
            "mode": "confirmed",
            "base_timeframe": 5,
            "status": "running",
            "claim_token": "claim-7",
            "lease_version": 7,
            "lease_until": datetime.fromtimestamp(250, UTC),
            "anchor_bar_end": anchor,
            "claimed_target_bar_end": target,
            "target_bar_end": target,
            "expected_head_run_id": 11,
            "expected_head_base_to_bar_end": anchor,
        }
        conn = TailConn(task_row, checked_at)

        await writer._lock_tail_publication_task(
            conn,
            task_id=17,
            claim_token="claim-7",
            lease_version=7,
            symbol_id=1,
            level_code=5,
            mode="confirmed",
            base_timeframe_code=5,
            anchor_bar_end=anchor,
            claimed_target_bar_end=target,
            expected_head_run_id=11,
            expected_head_base_to_bar_end=anchor,
        )

        query, args = conn.calls[0]
        lowered = query.lower()
        assert "from scheme2_chan_c_tail_tasks" in lowered
        assert "lease_until" in lowered
        assert "claim_token" in lowered
        assert "expected_head_run_id" in lowered
        assert "join symbols symbol" in lowered
        assert "and symbol.is_active" in lowered
        assert "for update of task" in lowered
        assert "for share of symbol" in lowered
        assert args == (17,)
        assert conn.calls[1] == ("select clock_timestamp()", ())

        with pytest.raises(StaleTailTaskLeaseError, match="task_id=17"):
            await writer._lock_tail_publication_task(
                TailConn({**task_row, "lease_until": checked_at}, checked_at),
                task_id=17,
                claim_token="stale",
                lease_version=6,
                symbol_id=1,
                level_code=5,
                mode="confirmed",
                base_timeframe_code=5,
                anchor_bar_end=anchor,
                claimed_target_bar_end=target,
                expected_head_run_id=11,
                expected_head_base_to_bar_end=anchor,
            )

    asyncio.run(scenario())


def test_tail_publication_rejects_target_drift_before_pool_acquire() -> None:
    class NoAcquirePool:
        def acquire(self):
            raise AssertionError("target drift must fail before pool acquire")

    writer = PostgresChanWriter("postgresql://unused")
    writer._pool = NoAcquirePool()
    target = datetime(2026, 7, 3, 6, 55, tzinfo=UTC)

    with pytest.raises(StaleTailTaskLeaseError, match="target mismatch"):
        asyncio.run(
            writer.replace_incremental_analysis(
                symbol="000001.SZ",
                level="5f",
                modes=["confirmed"],
                anchor_bar_end=datetime(2026, 7, 3, 6, 50, tzinfo=UTC),
                bar_until=datetime(2026, 7, 3, 7, 0, tzinfo=UTC),
                response={"snapshot_version": "v1"},
                publication_task_id=17,
                publication_claim_token="claim-7",
                publication_lease_version=7,
                publication_target_bar_end=target,
                expected_head_run_id=11,
                expected_head_base_to_bar_end=datetime(
                    2026, 7, 3, 6, 50, tzinfo=UTC
                ),
            )
        )


def test_tail_publication_accepts_same_week_canonical_bar_label() -> None:
    class PoolReached(RuntimeError):
        pass

    class MarkerPool:
        def acquire(self):
            raise PoolReached("logical target accepted")

    writer = PostgresChanWriter("postgresql://unused")
    writer._pool = MarkerPool()
    claimed_monday = datetime(2026, 7, 6, 7, tzinfo=UTC)
    canonical_friday = datetime(2026, 7, 10, 7, tzinfo=UTC)

    with pytest.raises(PoolReached, match="logical target accepted"):
        asyncio.run(
            writer.replace_incremental_analysis(
                symbol="000001.SZ",
                level="1w",
                modes=["confirmed"],
                anchor_bar_end=datetime(2026, 6, 29, 7, tzinfo=UTC),
                bar_until=canonical_friday,
                response={"snapshot_version": "v1"},
                publication_task_id=17,
                publication_claim_token="claim-7",
                publication_lease_version=7,
                publication_target_bar_end=claimed_monday,
                expected_head_run_id=11,
                expected_head_base_to_bar_end=datetime(
                    2026, 7, 3, 7, tzinfo=UTC
                ),
            )
        )


def test_tail_completion_compares_new_work_to_published_endpoint() -> None:
    class Conn:
        def __init__(self):
            self.call = None

        async def execute(self, query, *args):
            self.call = (query, args)
            return "UPDATE 1"

    async def scenario() -> None:
        conn = Conn()
        writer = PostgresChanWriter("postgresql://unused")
        published_endpoint = datetime(2026, 7, 10, 7, tzinfo=UTC)

        await writer._complete_tail_publication_task(
            conn,
            task_id=17,
            claim_token="claim-7",
            lease_version=7,
            bar_until=published_endpoint,
        )

        query, args = conn.call
        assert "completion.has_new_period" in query
        assert "date_trunc('week', target_bar_end" in query
        assert "date_trunc('month', target_bar_end" in query
        assert "else target_bar_end > $4" in query
        assert "target_bar_end > claimed_target_bar_end" not in query
        assert args == (17, "claim-7", 7, published_endpoint)

    asyncio.run(scenario())


class FullRecomputeConn(FakeConn):
    def __init__(self, *, task_overrides=None, parent_overrides=None, child_overrides=None) -> None:
        super().__init__()
        self.task_overrides = task_overrides or {}
        self.parent_overrides = parent_overrides or {}
        self.child_overrides = child_overrides or {}

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        lowered = query.lower()
        if "from chan_c_batches" in lowered:
            return {
                "id": 7,
                "status": "running",
                "batch_kind": "canary",
                "run_group_id": "batch-7",
                "config_hash": "module-c-v4",
                "publication_namespace": "canonical",
                "profile_id": "module-c-v4",
                **self.parent_overrides,
            }
        if "from chan_c_full_recompute_batches" in lowered:
            return {
                "batch_id": 7,
                "status": "running",
                "run_group_id": "batch-7",
                "config_hash": "module-c-v4",
                "publication_namespace": "canonical",
                "profile_id": "module-c-v4",
                **self.child_overrides,
            }
        if "from chan_c_full_recompute_tasks" in lowered:
            return {
                "batch_id": 7,
                "symbol_id": 1,
                "chan_level": 5,
                "status": "running",
                "claim_token": "claim-1",
                "lease_version": 2,
                "target_bar_until": datetime.fromtimestamp(200, UTC),
                "expected_heads": {},
                **self.task_overrides,
            }
        return None


def _full_recompute_writer(conn: FakeConn) -> PostgresChanWriter:
    writer = PostgresChanWriter(
        "postgresql://unused",
        batch_id=7,
        run_group_id="batch-7",
        run_config_hash="module-c-v4",
        native_base_timeframe=True,
        publication_profile="baseline",
        publication_source="full_recompute",
        run_kind="full_recompute",
        publication_namespace="canonical",
        profile_id="module-c-v4",
    )
    writer._pool = FakePool(conn)
    return writer


def test_inactive_symbol_publication_fails_before_any_durable_write() -> None:
    async def scenario() -> None:
        conn = FakeConn()
        conn._fetchval_results = iter([None])
        writer = PostgresChanWriter("postgresql://unused")
        writer._pool = FakePool(conn)

        with pytest.raises(StaleChanHeadError, match="unknown or inactive symbol"):
            await writer.replace_analysis(
                symbol="000001.SZ",
                level="5f",
                modes=["confirmed"],
                bar_from=datetime.fromtimestamp(100, UTC),
                bar_until=datetime.fromtimestamp(200, UTC),
                bar_count=20,
                response={"snapshot_version": "inactive", "strokes": [], "segments": [], "centers": [], "signals": []},
            )

        assert conn.execute_calls == []
        assert conn.copy_calls == []

    asyncio.run(scenario())


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
    class FullConn(FullRecomputeConn):
        async def execute(self, query: str, *args):
            self.execute_calls.append((query, args))
            if "set status = 'completed'" in query:
                return "UPDATE 1"
            return "OK"

    async def scenario() -> None:
        conn = FullConn()
        conn._fetchval_results = iter([1, 99])
        writer = _full_recompute_writer(conn)
        task = {
            "batch_id": 7, "symbol_id": 1, "chan_level": 5,
            "claim_token": "claim-1", "lease_version": 2,
            "target_bar_until": datetime.fromtimestamp(200, UTC), "expected_heads": {},
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

        assert len(conn.fetchrow_calls) >= 3
        parent_query, parent_args = conn.fetchrow_calls[0]
        child_query, child_args = conn.fetchrow_calls[1]
        task_query, task_args = conn.fetchrow_calls[2]
        assert "from chan_c_batches" in parent_query
        assert "status = 'running'" in parent_query
        assert "for share" in parent_query
        assert parent_args == (7,)
        assert "from chan_c_full_recompute_batches" in child_query
        assert "status = 'running'" in child_query
        assert "for share" in child_query
        assert child_args == (7,)
        assert "from chan_c_full_recompute_tasks" in task_query
        assert "for update" in task_query
        assert task_args == (7, 1, 5, "claim-1", 2)
        run_query, _run_args = conn.fetchval_calls[1]
        assert "on conflict (batch_id, run_identity)" in run_query
        completion = next(
            (query, args) for query, args in conn.execute_calls
            if "set status = 'completed'" in query
        )
        assert completion[1][:5] == (7, 1, 5, "claim-1", 2)

    asyncio.run(scenario())


def test_full_recompute_publication_rechecks_batch_status_before_any_write() -> None:
    async def scenario() -> None:
        conn = FullRecomputeConn(child_overrides={"status": "pending"})
        conn._fetchval_results = iter([1])
        writer = _full_recompute_writer(conn)
        task = {
            "batch_id": 7, "symbol_id": 1, "chan_level": 5,
            "claim_token": "claim-1", "lease_version": 2,
            "target_bar_until": datetime.fromtimestamp(200, UTC), "expected_heads": {},
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

        assert len(conn.fetchrow_calls) == 2
        assert "from chan_c_batches" in conn.fetchrow_calls[0][0].lower()
        assert "from chan_c_full_recompute_batches" in conn.fetchrow_calls[1][0].lower()
        assert conn.execute_calls == []
        assert conn.copy_calls == []

    asyncio.run(scenario())


def test_full_recompute_writer_requires_matching_task_before_pool_acquire() -> None:
    class NoAcquirePool:
        def acquire(self):
            raise AssertionError("invalid full-recompute input must fail before acquiring a connection")

    writer = PostgresChanWriter(
        "postgresql://unused",
        batch_id=7,
        run_group_id="batch-7",
        run_config_hash="module-c-v4",
        native_base_timeframe=True,
        publication_profile="baseline",
        publication_source="full_recompute",
        run_kind="full_recompute",
        publication_namespace="canonical",
        profile_id="module-c-v4",
    )
    writer._pool = NoAcquirePool()
    kwargs = {
        "symbol": "000001.SZ", "level": "5f", "modes": ["confirmed"],
        "bar_from": datetime.fromtimestamp(100, UTC),
        "bar_until": datetime.fromtimestamp(200, UTC), "bar_count": 20,
        "response": {"snapshot_version": "v1"},
    }

    with pytest.raises(ValueError, match="requires a fenced task"):
        asyncio.run(writer.replace_analysis(**kwargs))
    with pytest.raises(ValueError, match="does not match writer batch"):
        asyncio.run(writer.replace_analysis(
            **kwargs,
            full_recompute_task={"batch_id": 8},
        ))


@pytest.mark.parametrize(
    "override",
    [
        {"run_kind": "online"},
        {"publication_profile": "online"},
        {"publication_source": "collector"},
        {"native_base_timeframe": False},
        {"batch_id": None},
        {"run_group_id": None},
        {"run_config_hash": ""},
        {"publication_namespace": None},
        {"profile_id": None},
    ],
)
def test_full_recompute_task_rejects_incomplete_writer_before_pool_acquire(override) -> None:
    class NoAcquirePool:
        def acquire(self):
            raise AssertionError("invalid full-recompute writer must fail before pool acquire")

    writer_args = {
        "batch_id": 7,
        "run_group_id": "batch-7",
        "run_config_hash": "module-c-v4",
        "native_base_timeframe": True,
        "publication_profile": "baseline",
        "publication_source": "full_recompute",
        "run_kind": "full_recompute",
        "publication_namespace": "canonical",
        "profile_id": "module-c-v4",
        **override,
    }
    writer = PostgresChanWriter("postgresql://unused", **writer_args)
    writer._pool = NoAcquirePool()

    with pytest.raises(ValueError, match="exact writer configuration"):
        asyncio.run(writer.replace_analysis(
            symbol="000001.SZ",
            level="5f",
            modes=["confirmed"],
            bar_from=datetime.fromtimestamp(100, UTC),
            bar_until=datetime.fromtimestamp(200, UTC),
            bar_count=20,
            response={"snapshot_version": "v1"},
            full_recompute_task={"batch_id": 7},
        ))


@pytest.mark.parametrize(
    ("connection", "task_override", "message"),
    [
        (
            FullRecomputeConn(parent_overrides={"run_group_id": "wrong"}),
            {},
            "parent identity",
        ),
        (
            FullRecomputeConn(child_overrides={"profile_id": "wrong"}),
            {},
            "child identity",
        ),
        (
            FullRecomputeConn(task_overrides={
                "target_bar_until": datetime.fromtimestamp(201, UTC),
            }),
            {},
            "target_bar_until",
        ),
        (
            FullRecomputeConn(task_overrides={"expected_heads": {"confirmed": 9}}),
            {"expected_heads": {"confirmed": 8}},
            "expected_heads",
        ),
    ],
)
def test_full_recompute_fence_rejects_identity_cutoff_or_head_drift_before_writes(
    connection, task_override, message,
) -> None:
    async def scenario() -> None:
        connection._fetchval_results = iter([1])
        writer = _full_recompute_writer(connection)
        task = {
            "batch_id": 7, "symbol_id": 1, "chan_level": 5,
            "claim_token": "claim-1", "lease_version": 2,
            "target_bar_until": datetime.fromtimestamp(200, UTC), "expected_heads": {},
            **task_override,
        }
        with pytest.raises(StaleChanHeadError, match=message):
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
        assert connection.execute_calls == []
        assert connection.copy_calls == []

    asyncio.run(scenario())


def test_historical_replay_publication_rechecks_parent_and_child_status_before_any_write() -> None:
    async def scenario() -> None:
        conn = FakeConn()
        conn._fetchval_results = iter([1, None])
        writer = PostgresChanWriter(
            "postgresql://unused",
            batch_id=9,
            run_group_id="historical-replay",
            native_base_timeframe=True,
            publication_profile="historical_replay",
            publication_source="historical_replay",
            run_kind="historical_replay",
            publication_namespace="historical-replay",
            profile_id="module-c-historical-replay-v1",
        )
        writer._pool = FakePool(conn)
        task = {
            "id": 10,
            "batch_id": 9,
            "symbol_id": 1,
            "chan_level": 5,
            "claim_token": "claim-1",
            "lease_version": 2,
            "replay_identity": "a" * 64,
            "contract_version": "historical-replay-v1",
        }

        with pytest.raises(StaleChanHeadError, match="batch status fence failed"):
            await writer.replace_analysis(
                symbol="000001.SZ",
                level="5f",
                modes=["confirmed", "predictive"],
                bar_from=datetime.fromtimestamp(100, UTC),
                bar_until=datetime.fromtimestamp(200, UTC),
                bar_count=20,
                response={
                    "snapshot_version": "historical-replay-snapshot",
                    "strokes": [],
                    "segments": [],
                    "centers": [],
                    "signals": [],
                },
                historical_replay_task=task,
            )

        fence_query, fence_args = conn.fetchval_calls[1]
        lowered = fence_query.lower()
        assert "for update of task" in lowered
        assert fence_args == (10, 9, 1, 5, "claim-1", 2)
        assert "from chan_c_batches" in conn.fetchrow_calls[0][0].lower()
        assert "for share" in conn.fetchrow_calls[0][0].lower()
        assert "from chan_c_historical_replay_batches" in conn.fetchrow_calls[1][0].lower()
        assert "for share" in conn.fetchrow_calls[1][0].lower()
        assert conn.execute_calls == []
        assert conn.copy_calls == []

    asyncio.run(scenario())
