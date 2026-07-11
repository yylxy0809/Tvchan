from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from app.domain.enums import LEVEL_TO_DB
from app.repositories.module_c_repo import ModuleCRepository


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def fetch(self, _query: str, symbol_id: int, chan_level: int, mode: int, run_kind: str | None, run_group_id: str | None):
        rows = [
            row
            for row in self._rows
            if row["symbol_id"] == symbol_id
            and row["chan_level"] == chan_level
            and row["mode"] == mode
            and row["status"] == "success"
            and (run_kind is None or row["run_kind"] == run_kind)
            and (run_group_id is None or row["run_group_id"] == run_group_id)
        ]
        rows.sort(key=lambda row: (row["bar_until"], row["computed_at"], row["id"]))
        return rows


class _Acquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    def __init__(self, rows: list[dict]) -> None:
        self._conn = _FakeConn(rows)

    def acquire(self) -> _Acquire:
        return _Acquire(self._conn)


def test_historical_run_lookup_prefers_exact_mode_and_group():
    rows = [
        {
            "id": 10,
            "symbol_id": 1,
            "chan_level": LEVEL_TO_DB["1d"],
            "mode": 2,
            "run_kind": "historical_backfill",
            "run_group_id": "research_daily_close",
            "status": "success",
            "snapshot_version": "exact",
            "bar_from": _dt("2025-01-01T00:00:00"),
            "bar_until": _dt("2025-02-01T00:00:00"),
            "computed_at": _dt("2025-02-01T00:01:00"),
        },
        {
            "id": 11,
            "symbol_id": 1,
            "chan_level": LEVEL_TO_DB["1d"],
            "mode": 2,
            "run_kind": "historical_backfill",
            "run_group_id": "phase_1_9_benchmark_after_c3",
            "status": "success",
            "snapshot_version": "wrong-group",
            "bar_from": _dt("2025-01-01T00:00:00"),
            "bar_until": _dt("2025-02-01T00:00:00"),
            "computed_at": _dt("2025-02-01T00:02:00"),
        },
    ]
    repo = ModuleCRepository(_FakePool(rows))

    lookup = asyncio.run(
        repo.get_historical_run_lookup(
            1,
            "1d",
            "predictive",
            _dt("2025-02-01T00:00:00"),
            run_kind="historical_backfill",
            run_group_id="research_daily_close",
            allow_legacy_mode_fallback=False,
        )
    )

    assert lookup.selected is not None
    assert lookup.selected.run_id == 10
    assert lookup.run_count == 1


def test_legacy_mode_zero_does_not_override_predictive_match():
    rows = [
        {
            "id": 20,
            "symbol_id": 1,
            "chan_level": LEVEL_TO_DB["1d"],
            "mode": 2,
            "run_kind": "historical_backfill",
            "run_group_id": "research_daily_close",
            "status": "success",
            "snapshot_version": "predictive",
            "bar_from": _dt("2025-01-01T00:00:00"),
            "bar_until": _dt("2025-02-01T00:00:00"),
            "computed_at": _dt("2025-02-01T00:01:00"),
        },
        {
            "id": 21,
            "symbol_id": 1,
            "chan_level": LEVEL_TO_DB["1d"],
            "mode": 0,
            "run_kind": "historical_backfill",
            "run_group_id": "research_daily_close",
            "status": "success",
            "snapshot_version": "legacy",
            "bar_from": _dt("2025-01-01T00:00:00"),
            "bar_until": _dt("2025-02-01T00:00:00"),
            "computed_at": _dt("2025-02-01T00:02:00"),
        },
    ]
    repo = ModuleCRepository(_FakePool(rows))

    lookup = asyncio.run(
        repo.get_historical_run_lookup(
            1,
            "1d",
            "predictive",
            _dt("2025-02-01T00:00:00"),
            run_kind="historical_backfill",
            run_group_id="research_daily_close",
            allow_legacy_mode_fallback=True,
        )
    )

    assert lookup.selected is not None
    assert lookup.selected.run_id == 20
