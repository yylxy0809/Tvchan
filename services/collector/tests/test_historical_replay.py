from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from collector.historical_replay import (
    InactiveReplayBatchError,
    ReplayBatchNotFinalizableError,
    ReplayContract,
    build_initial_cutoff_grid,
    build_intraday_cutoff_grid,
    stable_replay_identity,
    visible_bars_at_cutoff,
    claim_replay_task,
    ensure_replay_batch,
    fail_replay_task,
    finalize_replay_batch,
    heartbeat_replay_task,
)
from collector.historical_replay_worker import (
    assert_no_future_output,
    load_scope_bars,
    parse_args,
    seed_canary_tasks,
    seed_full_high_level_tasks,
    seed_intraday_strategy_tasks,
)


def _contract(cutoff: datetime | str = "2026-07-10T07:00:00+00:00") -> ReplayContract:
    return ReplayContract(
        config_hash="module-c:test",
        source_batch_id=6,
        eligible_universe_snapshot_id="eligibility:6",
        canonical_gate_snapshot_id="canonical:6",
        cutoff_time=cutoff,
    )


def test_contract_identity_is_stable_and_normalizes_equivalent_offsets() -> None:
    utc = _contract("2026-07-10T07:00:00+00:00")
    china = _contract("2026-07-10T15:00:00+08:00")
    assert utc.digest() == china.digest()
    assert stable_replay_identity(
        utc, symbol="000001.sz", level="1d", mode="confirmed,predictive", cutoff_time=utc.cutoff_time
    ) == stable_replay_identity(
        china, symbol="000001.SZ", level="1d", mode="confirmed,predictive", cutoff_time=china.cutoff_time
    )


def test_contract_rejects_naive_datetime_and_non_replay_provenance() -> None:
    with pytest.raises(ValueError, match="Naive"):
        _contract(datetime(2026, 7, 10, 7))
    with pytest.raises(ValueError, match="cannot be relabeled"):
        ReplayContract(
            config_hash="x", source_batch_id=6, eligible_universe_snapshot_id="e",
            canonical_gate_snapshot_id="c", cutoff_time=datetime.now(UTC), provenance="baseline",
        )


def test_initial_grid_uses_completed_native_bars_and_excludes_open_week_month() -> None:
    bars = [
        {"level": "1d", "ts": "2026-07-10T07:00:00+00:00", "complete": True},
        {"level": "1w", "ts": "2026-07-10T07:00:00+00:00", "complete": True},
        {"level": "1w", "ts": "2026-07-17T07:00:00+00:00", "complete": True},
        {"level": "1m", "ts": "2026-06-30T07:00:00+00:00", "complete": True},
        {"level": "1m", "ts": "2026-07-31T07:00:00+00:00", "complete": True},
        {"level": "1d", "ts": "2026-07-13T07:00:00+00:00", "complete": False},
        {"level": "5f", "ts": "2026-07-10T01:35:00+00:00", "complete": True},
    ]
    grid = build_initial_cutoff_grid(bars, as_of_time="2026-07-14T08:00:00+00:00")
    assert {(row["level"], row["cutoff_time"]) for row in grid} == {
        ("1d", "2026-07-10T07:00:00+00:00"),
        ("1w", "2026-07-10T07:00:00+00:00"),
        ("1m", "2026-06-30T07:00:00+00:00"),
    }


def test_intraday_grid_is_limited_to_predeclared_windows_and_handles_sparse_bars() -> None:
    bars = [
        {"level": "30f", "ts": "2026-07-10T01:30:00+00:00", "complete": True},
        {"level": "5f", "ts": "2026-07-10T01:35:00+00:00", "complete": True},
        {"level": "5f", "ts": "2026-07-10T01:40:00+00:00", "complete": False},
        {"level": "30f", "ts": "2026-07-10T08:00:00+00:00", "complete": True},
    ]
    grid = build_intraday_cutoff_grid(
        bars,
        windows=[{"window_id": "setup-1", "start_time": "2026-07-10T01:30:00Z", "end_time": "2026-07-10T02:00:00Z"}],
    )
    assert [(row["level"], row["cutoff_time"]) for row in grid] == [
        ("30f", "2026-07-10T01:30:00+00:00"),
        ("5f", "2026-07-10T01:35:00+00:00"),
    ]


def test_visible_bar_query_never_reads_after_cutoff() -> None:
    bars = [
        {"ts": "2026-07-10T01:30:00+00:00", "complete": True},
        {"ts": "2026-07-10T01:35:00+00:00", "complete": True},
        {"ts": "2026-07-10T01:40:00+00:00", "complete": True},
    ]
    visible = visible_bars_at_cutoff(bars, cutoff_time="2026-07-10T01:35:00+00:00")
    assert visible == bars[:2]


def test_replay_output_rejects_future_structure_base_time() -> None:
    cutoff = datetime(2026, 7, 10, 7, tzinfo=UTC)
    assert_no_future_output(
        {"strokes": [{"end_base_ts": int(cutoff.timestamp())}], "segments": [], "centers": [], "signals": []},
        cutoff_time=cutoff,
    )
    with pytest.raises(RuntimeError, match="future_data_leak"):
        assert_no_future_output(
            {"signals": [{"base_ts": int(cutoff.timestamp()) + 1}]}, cutoff_time=cutoff
        )


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return None


class _Transaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_args):
        return None


class _Connection:
    def __init__(self, row=None, execute_result="UPDATE 1", *, contract=None):
        self.row = row
        self.execute_result = execute_result
        self.calls = []
        self.contract = contract or _contract()

    def transaction(self):
        return _Transaction()

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
        lowered = query.lower()
        if "from chan_c_batches" in lowered:
            return _parent_row(self.contract)
        if "from chan_c_historical_replay_batches" in lowered:
            return _child_row(self.contract)
        return self.row

    async def execute(self, query, *args):
        self.calls.append(("execute", query, args))
        return self.execute_result


class _Writer:
    def __init__(self, connection):
        self.connection = connection
        self._pool = self

    def acquire(self):
        return _Acquire(self.connection)


def _parent_row(contract: ReplayContract, *, status: str = "running") -> dict:
    return {
        "id": 9,
        "status": status,
        "batch_kind": "historical_replay",
        "publication_namespace": "historical-replay",
        "profile_id": "module-c-historical-replay-v1",
        "run_group_id": "historical-replay-test",
        "config_hash": contract.config_hash,
        "effective_config": {
            "replay_contract_version": contract.contract_version,
            "source_batch_id": contract.source_batch_id,
        },
        "audit_references": [
            {"type": "source_batch", "batch_id": contract.source_batch_id}
        ],
    }


def _child_row(contract: ReplayContract, *, status: str = "running") -> dict:
    return {
        "batch_id": 9,
        "status": status,
        "source_batch_id": contract.source_batch_id,
        "contract_version": contract.contract_version,
        "contract_hash": contract.digest(),
        "contract": contract.payload(),
        "eligible_universe_snapshot_id": contract.eligible_universe_snapshot_id,
        "canonical_gate_snapshot_id": contract.canonical_gate_snapshot_id,
        "cutoff_policy": contract.cutoff_policy,
    }


class _SeederConnection:
    def __init__(self, *, parent_status="running", child_status="planned", contract=None):
        self.contract = contract or _contract()
        self.parent_status = parent_status
        self.child_status = child_status
        self.calls = []

    def transaction(self):
        self.calls.append(("transaction", "enter", ()))
        return _Transaction()

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
        lowered = query.lower()
        if "from chan_c_batches" in lowered:
            return _parent_row(self.contract, status=self.parent_status)
        if "from chan_c_historical_replay_batches" in lowered:
            return _child_row(self.contract, status=self.child_status)
        if "select coalesce(" in lowered:
            return {"windows": [], "tasks": []}
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def fetch(self, query, *args):
        self.calls.append(("fetch", query, args))
        return []

    async def fetchval(self, query, *args):
        self.calls.append(("fetchval", query, args))
        return 0

    async def executemany(self, query, rows):
        self.calls.append(("executemany", query, tuple(rows)))


@pytest.mark.parametrize(
    "seeder,kwargs",
    [
        (
            seed_canary_tasks,
            {"source_batch_id": 6, "canary_source_batch_id": 5, "cutoffs_per_scope": 2},
        ),
        (seed_full_high_level_tasks, {"source_batch_id": 6, "cutoffs_per_scope": 2}),
    ],
)
def test_initial_replay_seeders_lock_parent_then_child_and_require_planned(seeder, kwargs) -> None:
    contract = _contract()
    connection = _SeederConnection(contract=contract)
    result = asyncio.run(seeder(connection, batch_id=9, contract=contract, **kwargs))
    assert result == 0
    lock_calls = [call for call in connection.calls if call[0] == "fetchrow"][:2]
    assert "from chan_c_batches" in lock_calls[0][1].lower()
    assert "for share" in lock_calls[0][1].lower()
    assert "from chan_c_historical_replay_batches" in lock_calls[1][1].lower()
    assert "for share" in lock_calls[1][1].lower()

    terminal = _SeederConnection(contract=contract, child_status="sealed")
    with pytest.raises(InactiveReplayBatchError, match="sealed"):
        asyncio.run(seeder(terminal, batch_id=9, contract=contract, **kwargs))
    assert not any(call[0] in {"fetch", "executemany", "fetchval"} for call in terminal.calls)


def test_intraday_replay_seeder_requires_running_and_rejects_sealed_before_planning() -> None:
    contract = _contract()
    running = _SeederConnection(contract=contract, child_status="running")
    result = asyncio.run(
        seed_intraday_strategy_tasks(
            running, batch_id=9, source_batch_id=6, contract=contract
        )
    )
    assert result["planned_task_count"] == 0

    for status in ("planned", "completed", "failed", "sealed", "stopped"):
        connection = _SeederConnection(contract=contract, child_status=status)
        with pytest.raises(InactiveReplayBatchError, match=status):
            asyncio.run(
                seed_intraday_strategy_tasks(
                    connection, batch_id=9, source_batch_id=6, contract=contract
                )
            )
        assert not any(call[0] in {"executemany", "fetchval"} for call in connection.calls)


def test_replay_seeder_rejects_contract_mismatch_before_planning() -> None:
    expected = _contract()
    actual = _contract("2026-07-09T07:00:00+00:00")
    connection = _SeederConnection(contract=actual)
    with pytest.raises(InactiveReplayBatchError, match="contract mismatch"):
        asyncio.run(
            seed_full_high_level_tasks(
                connection,
                batch_id=9,
                source_batch_id=6,
                contract=expected,
                cutoffs_per_scope=2,
            )
        )
    assert not any(call[0] in {"fetch", "executemany", "fetchval"} for call in connection.calls)


def test_replay_batch_resume_reuses_and_validates_same_contract() -> None:
    contract = _contract()
    row = {
        "source_batch_id": 6,
        "contract_version": contract.contract_version,
        "contract_hash": contract.digest(),
        "eligible_universe_snapshot_id": contract.eligible_universe_snapshot_id,
        "canonical_gate_snapshot_id": contract.canonical_gate_snapshot_id,
        "cutoff_policy": contract.cutoff_policy,
    }
    class Conn(_Connection):
        async def fetchrow(self, query, *args):
            self.calls.append(("fetchrow", query, args))
            if "from chan_c_batches" in query.lower():
                return _parent_row(contract)
            return {**_child_row(contract), **row, "status": "running"}

    connection = Conn()
    asyncio.run(ensure_replay_batch(kline_writer=_Writer(connection), batch_id=7, contract=contract))
    insert = next(call for call in connection.calls if call[0] == "execute")
    assert "on conflict (batch_id) do nothing" in insert[1].lower()
    assert "for update" in connection.calls[0][1].lower()
    assert "for update" in connection.calls[1][1].lower()


@pytest.mark.parametrize("status", ["sealed", "failed", "aborted"])
def test_replay_batch_init_rejects_terminal_parent_before_writes(status: str) -> None:
    class Conn(_Connection):
        async def fetchrow(self, query, *args):
            self.calls.append(("fetchrow", query, args))
            assert "from chan_c_batches" in query.lower()
            return _parent_row(_contract(), status=status)

    connection = Conn()
    with pytest.raises(InactiveReplayBatchError, match=status):
        asyncio.run(ensure_replay_batch(kline_writer=_Writer(connection), batch_id=7, contract=_contract()))
    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.parametrize("status", ["completed", "sealed", "failed", "stopped"])
def test_replay_batch_init_rejects_terminal_child_before_writes(status: str) -> None:
    class Conn(_Connection):
        async def fetchrow(self, query, *args):
            self.calls.append(("fetchrow", query, args))
            if "from chan_c_batches" in query.lower():
                return _parent_row(_contract())
            return _child_row(_contract(), status=status)

    connection = Conn()
    with pytest.raises(InactiveReplayBatchError, match=status):
        asyncio.run(ensure_replay_batch(kline_writer=_Writer(connection), batch_id=7, contract=_contract()))
    assert not any(call[0] == "execute" for call in connection.calls)


def test_replay_claim_uses_skip_locked_lease_and_fencing_version() -> None:
    row = {"id": 10, "claim_token": "new", "lease_version": 2}
    connection = _Connection(row=row)
    claimed = asyncio.run(claim_replay_task(
        kline_writer=_Writer(connection), batch_id=7, worker_id="worker-1", lease_seconds=30
    ))
    assert claimed == row
    lock_queries = [call[1].lower() for call in connection.calls[:2]]
    assert "from chan_c_batches" in lock_queries[0] and "for share" in lock_queries[0]
    assert "from chan_c_historical_replay_batches" in lock_queries[1] and "for share" in lock_queries[1]
    query = next(call[1].lower() for call in connection.calls if "for update of task skip locked" in call[1].lower())
    assert "for update of task skip locked" in query
    assert "lease_version = task.lease_version + 1" in query
    assert "lease_until <= now()" in query


def test_replay_claim_keeps_worker_on_its_symbol_shard() -> None:
    connection = _Connection(row=None)
    asyncio.run(claim_replay_task(
        kline_writer=_Writer(connection), batch_id=9, worker_id="worker-2",
        shard_index=2, shard_count=4,
    ))
    task_call = next(call for call in connection.calls if "mod(task.symbol_id, $5)" in call[1].lower())
    query = task_call[1].lower()
    assert "mod(task.symbol_id, $5) = $6" in query
    assert "order by task.symbol_id, task.chan_level, task.cutoff_time" in query
    assert task_call[2][-2:] == (4, 2)


def test_replay_reuses_bars_for_adjacent_cutoffs_in_same_scope() -> None:
    class KlineWriter:
        def __init__(self) -> None:
            self.calls = 0

        async def get_bars(self, symbol: str, level: str):
            self.calls += 1
            return [symbol, level]

    writer = KlineWriter()
    cache = {}
    first = asyncio.run(load_scope_bars(writer, symbol="000001.SZ", level="1d", bars_cache=cache))
    second = asyncio.run(load_scope_bars(writer, symbol="000001.SZ", level="1d", bars_cache=cache))
    assert first is second
    assert writer.calls == 1


def test_replay_heartbeat_and_failure_are_fenced_and_structured() -> None:
    task = {"id": 10, "batch_id": 9, "claim_token": "token", "lease_version": 2}
    connection = _Connection()
    assert asyncio.run(heartbeat_replay_task(kline_writer=_Writer(connection), task=task, lease_seconds=30))
    heartbeat_query = next(call[1].lower() for call in connection.calls if "set lease_until" in call[1].lower())
    assert "lease_version = $3" in heartbeat_query
    assert asyncio.run(fail_replay_task(kline_writer=_Writer(connection), task=task, error=RuntimeError("boom")))
    failure_call = next(call for call in connection.calls if "set status = 'failed'" in call[1].lower())
    failure_query = failure_call[1].lower()
    assert "lease_version = $3" in failure_query
    failure_args = failure_call[2]
    assert json.loads(failure_args[4])["error_type"] == "RuntimeError"


def _ready_counts(**overrides):
    counts = {
        "total_tasks": 2,
        "completed_tasks": 1,
        "excluded_tasks": 1,
        "pending_tasks": 0,
        "running_tasks": 0,
        "failed_tasks": 0,
        "invalid_task_states": 0,
        "missing_run_ids": 0,
        "invalid_task_contracts": 0,
        "invalid_runs": 0,
        "missing_heads": 0,
        "unexpected_heads": 0,
        "mismatched_heads": 0,
        "missing_history": 0,
        "invalid_history": 0,
        "unexpected_history": 0,
        "missing_outbox": 0,
        "blocking_outbox": 0,
        "invalid_outbox_payload": 0,
        "invalid_lifecycle_events": 0,
    }
    counts.update(overrides)
    return counts


class _FinalizerConnection:
    def __init__(self, *, parent_status="running", child_status="running", counts=None):
        self.parent_status = parent_status
        self.child_status = child_status
        self.counts = counts or _ready_counts()
        self.calls = []

    def transaction(self):
        return _Transaction()

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
        lowered = query.lower()
        if "from chan_c_batches" in lowered:
            return _parent_row(_contract(), status=self.parent_status)
        if "from chan_c_historical_replay_batches" in lowered and "count(" not in lowered:
            return _child_row(_contract(), status=self.child_status)
        if "with event_state" in lowered:
            return {"count": 0, "samples": []}
        if "with missing as" in lowered:
            return {"count": 0, "samples": []}
        return self.counts

    async def fetch(self, query, *args):
        self.calls.append(("fetch", query, args))
        return []

    async def execute(self, query, *args):
        self.calls.append(("execute", query, args))
        return "UPDATE 1"


def _finalize(connection, **kwargs):
    return finalize_replay_batch(
        connection,
        batch_id=9,
        sealed_by="device-b-finalizer",
        expected_contract_hash=_contract().digest(),
        **kwargs,
    )


def test_finalize_repairs_sealed_parent_running_child_without_touching_parent() -> None:
    connection = _FinalizerConnection(parent_status="sealed", child_status="running")
    result = asyncio.run(_finalize(connection))

    assert result["ready"] is True
    assert result["parent_status_after"] == "sealed"
    assert result["child_status_after"] == "sealed"
    assert result["repaired_parent_already_sealed"] is True
    execute_calls = [call for call in connection.calls if call[0] == "execute"]
    assert len(execute_calls) == 1
    assert "update chan_c_historical_replay_batches" in execute_calls[0][1].lower()


def test_finalize_seals_parent_and_child_in_one_transaction() -> None:
    connection = _FinalizerConnection(parent_status="running", child_status="completed")
    result = asyncio.run(_finalize(connection))

    assert result["parent_status_after"] == "sealed"
    assert result["child_status_after"] == "sealed"
    execute_calls = [call for call in connection.calls if call[0] == "execute"]
    assert len(execute_calls) == 2
    assert "update chan_c_historical_replay_batches" in execute_calls[0][1].lower()
    assert "update chan_c_batches" in execute_calls[1][1].lower()
    assert execute_calls[1][2] == (9, "device-b-finalizer", "running")
    lock_calls = [call for call in connection.calls if call[0] == "fetchrow"][:2]
    assert "from chan_c_batches" in lock_calls[0][1].lower()
    assert "for update" in lock_calls[0][1].lower()
    assert "from chan_c_historical_replay_batches" in lock_calls[1][1].lower()
    assert "for update" in lock_calls[1][1].lower()


def test_finalize_is_idempotent_for_already_sealed_pair() -> None:
    connection = _FinalizerConnection(parent_status="sealed", child_status="sealed")
    result = asyncio.run(_finalize(connection))
    assert result["ready"] is True
    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.parametrize(
    ("overrides", "blocker"),
    [
        ({"total_tasks": 0, "completed_tasks": 0, "excluded_tasks": 0}, "empty_batch"),
        ({"pending_tasks": 1}, "pending_tasks"),
        ({"running_tasks": 1}, "running_tasks"),
        ({"failed_tasks": 1}, "failed_tasks"),
        ({"invalid_task_states": 1}, "invalid_task_states"),
        ({"missing_run_ids": 1}, "missing_run_ids"),
        ({"invalid_task_contracts": 1}, "invalid_task_contracts"),
        ({"invalid_runs": 1}, "invalid_runs"),
        ({"missing_heads": 1}, "missing_heads"),
        ({"unexpected_heads": 1}, "unexpected_heads"),
        ({"mismatched_heads": 1}, "mismatched_heads"),
        ({"missing_history": 1}, "missing_history"),
        ({"invalid_history": 1}, "invalid_history"),
        ({"unexpected_history": 1}, "unexpected_history"),
        ({"missing_outbox": 1}, "missing_outbox"),
        ({"blocking_outbox": 1}, "blocking_outbox"),
        ({"invalid_outbox_payload": 1}, "invalid_outbox_payload"),
        ({"invalid_lifecycle_events": 1}, "invalid_lifecycle_events"),
    ],
)
def test_finalize_fails_closed_without_mutating_or_replaying(overrides, blocker) -> None:
    connection = _FinalizerConnection(counts=_ready_counts(**overrides))
    with pytest.raises(ReplayBatchNotFinalizableError, match=blocker):
        asyncio.run(_finalize(connection))
    assert not any(call[0] == "execute" for call in connection.calls)


def test_finalize_dry_run_uses_shared_locks_and_performs_no_writes() -> None:
    connection = _FinalizerConnection()
    result = asyncio.run(
        _finalize(connection, dry_run=True)
    )
    assert result["dry_run"] is True
    assert result["parent_status_after"] == "running"
    assert result["child_status_after"] == "running"
    assert result["would_parent_status"] == "sealed"
    assert result["would_child_status"] == "sealed"
    assert not any(call[0] == "execute" for call in connection.calls)
    lock_calls = [call for call in connection.calls if call[0] == "fetchrow"][:2]
    assert all("for share" in call[1].lower() for call in lock_calls)


def test_finalize_dry_run_reports_blockers_without_raising_or_writing() -> None:
    connection = _FinalizerConnection(counts=_ready_counts(running_tasks=1))
    result = asyncio.run(
        _finalize(connection, dry_run=True)
    )
    assert result["ready"] is False
    assert "running_tasks" in result["blockers"]
    assert not any(call[0] == "execute" for call in connection.calls)


def test_finalize_verifies_run_head_history_outbox_and_lifecycle_identity() -> None:
    connection = _FinalizerConnection()
    asyncio.run(_finalize(connection, dry_run=True))
    query = next(
        call[1].lower()
        for call in connection.calls
        if call[0] == "fetchrow" and "history_evidence" in call[1].lower()
    )
    for required in (
        "run.bar_until = task.cutoff_time",
        "run.cutoff_bar_end = task.cutoff_time",
        "run.run_kind = 'historical_replay'",
        "run.run_identity = task.replay_identity",
        "history.publication_profile",
        "lag(actual.run_id)",
        "lag(actual.cutoff_time)",
        "history.old_run_id as history_old_run_id",
        "history.old_base_to_bar_end as history_old_cutoff_time",
        "history_old_run_id is distinct from expected_old_run_id",
        "history_old_cutoff_time is distinct from expected_old_cutoff_time",
        "chan_c_head_outbox",
        "outbox_status <> 'completed'",
        "invalid_outbox_payload",
        "payload->>'old_run_id'",
        "payload->>'old_base_to_bar_end'",
        "payload->>'new_base_to_bar_end'",
        "payload->>'published_at'",
        "event.effective_time <> evidence.cutoff_time",
        "event.observed_time is distinct from evidence.history_published_at",
    ):
        assert required in query


def test_finalize_requires_global_lifecycle_reconciliation_pass(monkeypatch) -> None:
    async def failed_reconciliation(_conn):
        return {
            "decision": "FAIL",
            "blockers": ["outbox_not_drained"],
            "outbox": {"blocking_count": 1},
            "projection_replay": {"mismatch_count": 0},
            "published_head_history": {"missing_count": 0},
        }

    monkeypatch.setattr(
        "collector.historical_replay.build_reconciliation", failed_reconciliation
    )
    connection = _FinalizerConnection()
    with pytest.raises(ReplayBatchNotFinalizableError, match="lifecycle_reconciliation"):
        asyncio.run(_finalize(connection))
    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.parametrize(
    ("parent_status", "child_status"),
    [("failed", "running"), ("aborted", "stopped"), ("sealed", "failed")],
)
def test_finalize_rejects_incompatible_terminal_states(parent_status, child_status) -> None:
    connection = _FinalizerConnection(parent_status=parent_status, child_status=child_status)
    with pytest.raises(ReplayBatchNotFinalizableError, match="batch_status"):
        asyncio.run(_finalize(connection))
    assert not any(call[0] == "execute" for call in connection.calls)


def test_finalize_rejects_contract_hash_mismatch() -> None:
    connection = _FinalizerConnection()
    with pytest.raises(ReplayBatchNotFinalizableError, match="contract_hash"):
        asyncio.run(
            finalize_replay_batch(
                connection,
                batch_id=9,
                sealed_by="device-b-finalizer",
                expected_contract_hash="b" * 64,
            )
        )
    assert not any(call[0] == "execute" for call in connection.calls)


def test_finalize_cli_requires_explicit_actor_and_supports_dry_run() -> None:
    args = parse_args(
        [
            "finalize",
            "--database-url",
            "postgresql://unused",
            "--batch-id",
            "9",
            "--sealed-by",
            "device-b-finalizer",
            "--expected-contract-hash",
            "a" * 64,
            "--dry-run",
        ]
    )
    assert args.action == "finalize"
    assert args.batch_id == 9
    assert args.sealed_by == "device-b-finalizer"
    assert args.dry_run is True

    with pytest.raises(SystemExit):
        parse_args(
            [
                "finalize",
                "--database-url",
                "postgresql://unused",
                "--batch-id",
                "9",
            ]
        )
    with pytest.raises(SystemExit):
        parse_args(
            [
                "finalize",
                "--database-url",
                "postgresql://unused",
                "--batch-id",
                "9",
                "--sealed-by",
                "device-b-finalizer",
            ]
        )
