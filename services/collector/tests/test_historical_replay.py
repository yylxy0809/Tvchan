from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from collector.historical_replay import (
    ReplayContract,
    build_initial_cutoff_grid,
    build_intraday_cutoff_grid,
    stable_replay_identity,
    visible_bars_at_cutoff,
    claim_replay_task,
    ensure_replay_batch,
    fail_replay_task,
    heartbeat_replay_task,
)
from collector.historical_replay_worker import assert_no_future_output, load_scope_bars


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
    def __init__(self, row=None, execute_result="UPDATE 1"):
        self.row = row
        self.execute_result = execute_result
        self.calls = []

    def transaction(self):
        return _Transaction()

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
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
    connection = _Connection(row=row)
    asyncio.run(ensure_replay_batch(kline_writer=_Writer(connection), batch_id=7, contract=contract))
    insert = connection.calls[0]
    assert "on conflict (batch_id) do nothing" in insert[1].lower()


def test_replay_claim_uses_skip_locked_lease_and_fencing_version() -> None:
    row = {"id": 10, "claim_token": "new", "lease_version": 2}
    connection = _Connection(row=row)
    claimed = asyncio.run(claim_replay_task(
        kline_writer=_Writer(connection), batch_id=7, worker_id="worker-1", lease_seconds=30
    ))
    assert claimed == row
    query = connection.calls[0][1].lower()
    assert "for update skip locked" in query
    assert "lease_version = task.lease_version + 1" in query
    assert "lease_until <= now()" in query


def test_replay_claim_keeps_worker_on_its_symbol_shard() -> None:
    connection = _Connection(row=None)
    asyncio.run(claim_replay_task(
        kline_writer=_Writer(connection), batch_id=9, worker_id="worker-2",
        shard_index=2, shard_count=4,
    ))
    query = connection.calls[0][1].lower()
    assert "mod(symbol_id, $5) = $6" in query
    assert "order by symbol_id, chan_level, cutoff_time" in query
    assert connection.calls[0][2][-2:] == (4, 2)


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
    task = {"id": 10, "claim_token": "token", "lease_version": 2}
    connection = _Connection()
    assert asyncio.run(heartbeat_replay_task(kline_writer=_Writer(connection), task=task, lease_seconds=30))
    assert "lease_version = $3" in connection.calls[0][1]
    assert asyncio.run(fail_replay_task(kline_writer=_Writer(connection), task=task, error=RuntimeError("boom")))
    failure_args = connection.calls[1][2]
    assert json.loads(failure_args[4])["error_type"] == "RuntimeError"
