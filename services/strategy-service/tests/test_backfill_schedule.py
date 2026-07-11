from __future__ import annotations

from datetime import UTC, datetime

from app.engine.module_c_history_backfill import build_snapshot_schedule
from app.repositories.kline_repo import KlineBar


def _bar(ts: str) -> KlineBar:
    value = datetime.fromisoformat(ts).replace(tzinfo=UTC)
    return KlineBar(ts=value, open=1.0, high=1.0, low=1.0, close=1.0, volume=1)


def test_research_daily_close_uses_latest_intraday_bar_per_day():
    bars_by_level = {
        "5f": [
            _bar("2026-01-02T06:35:00"),
            _bar("2026-01-02T07:00:00"),
            _bar("2026-01-03T06:35:00"),
            _bar("2026-01-03T07:00:00"),
        ],
        "30f": [_bar("2026-01-02T07:00:00"), _bar("2026-01-03T07:00:00")],
        "1d": [_bar("2026-01-02T07:00:00"), _bar("2026-01-03T07:00:00")],
        "1w": [_bar("2026-01-02T07:00:00")],
        "1m": [_bar("2026-01-02T07:00:00")],
    }

    schedule = build_snapshot_schedule(
        profile="research_daily_close",
        bars_by_level=bars_by_level,
        backtest_start=datetime(2026, 1, 2, tzinfo=UTC),
        end_time=datetime(2026, 1, 3, 23, 59, tzinfo=UTC),
    )

    assert schedule["5f"] == [
        datetime(2026, 1, 2, 7, 0, tzinfo=UTC),
        datetime(2026, 1, 3, 7, 0, tzinfo=UTC),
    ]
    assert schedule["30f"] == [
        datetime(2026, 1, 2, 7, 0, tzinfo=UTC),
        datetime(2026, 1, 3, 7, 0, tzinfo=UTC),
    ]


def test_strategy_30f_uses_30f_cutoffs_for_5f_snapshots():
    bars_by_level = {
        "5f": [_bar("2026-01-02T06:35:00"), _bar("2026-01-02T06:40:00"), _bar("2026-01-02T07:00:00")],
        "30f": [_bar("2026-01-02T06:30:00"), _bar("2026-01-02T07:00:00")],
        "1d": [_bar("2026-01-02T07:00:00")],
        "1w": [],
        "1m": [],
    }

    schedule = build_snapshot_schedule(
        profile="strategy_30f",
        bars_by_level=bars_by_level,
        backtest_start=datetime(2026, 1, 2, tzinfo=UTC),
        end_time=datetime(2026, 1, 2, 23, 59, tzinfo=UTC),
    )

    assert schedule["5f"] == [
        datetime(2026, 1, 2, 6, 30, tzinfo=UTC),
        datetime(2026, 1, 2, 7, 0, tzinfo=UTC),
    ]
