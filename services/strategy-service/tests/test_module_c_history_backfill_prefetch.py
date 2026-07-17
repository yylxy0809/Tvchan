from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from app.engine.module_c_history_backfill import HistoricalBackfillWriter
from trading_protocol import MODULE_C_CONFIG_HASH


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.fetch_args = None
        self.fetchval_args = None

    async def fetch(self, _query, *args, **_kwargs):
        self.fetch_args = args
        return self._rows

    async def fetchval(self, _query, *args):
        self.fetchval_args = args
        return None


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
    assert writer.pool._conn.fetch_args[-1] == MODULE_C_CONFIG_HASH


def test_run_exists_does_not_treat_an_older_semantic_run_as_resumable():
    writer = HistoricalBackfillWriter(_FakePool([]))

    exists = asyncio.run(writer.run_exists(
        symbol_id=1,
        level="5f",
        mode="predictive",
        cutoff_time=datetime(2025, 9, 18, 7, 0, tzinfo=UTC),
        run_group_id="research_daily_close",
    ))

    assert exists is False
    assert writer.pool._conn.fetchval_args[-1] == MODULE_C_CONFIG_HASH
