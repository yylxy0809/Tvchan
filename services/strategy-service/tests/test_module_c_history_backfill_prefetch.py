from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from app.engine.module_c_history_backfill import HistoricalBackfillWriter


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, *_args, **_kwargs):
        return self._rows


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, rows):
        self._conn = _FakeConn(rows)

    def acquire(self):
        return _AcquireCtx(self._conn)


def test_prefetch_existing_cutoffs_maps_db_levels_back_to_level_names():
    rows = [
        {"chan_level": 5, "bar_until": datetime(2025, 9, 18, 7, 0, tzinfo=UTC)},
        {"chan_level": 30, "bar_until": datetime(2025, 9, 18, 7, 0, tzinfo=UTC)},
    ]
    writer = HistoricalBackfillWriter(_FakePool(rows))

    payload = asyncio.run(
        writer.prefetch_existing_cutoffs(
            symbol_id=1,
            levels=("5f", "30f"),
            mode="predictive",
            run_group_id="phase_1_15_targeted_entry_window_intraday",
        )
    )

    assert payload["5f"] == {datetime(2025, 9, 18, 7, 0, tzinfo=UTC)}
    assert payload["30f"] == {datetime(2025, 9, 18, 7, 0, tzinfo=UTC)}
