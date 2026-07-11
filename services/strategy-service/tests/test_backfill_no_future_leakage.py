from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.engine.module_c_history_backfill import assert_no_future_leakage, build_cutoff_windows, slice_bars_for_cutoff
from app.repositories.kline_repo import KlineBar


def _bar(ts: str) -> KlineBar:
    value = datetime.fromisoformat(ts).replace(tzinfo=UTC)
    return KlineBar(ts=value, open=1.0, high=1.0, low=1.0, close=1.0, volume=1)


def test_slice_bars_for_cutoff_excludes_future_bar():
    bars = [
        _bar("2026-01-02T06:30:00"),
        _bar("2026-01-02T07:00:00"),
        _bar("2026-01-02T07:30:00"),
    ]

    sliced = slice_bars_for_cutoff(bars, datetime(2026, 1, 2, 7, 0, tzinfo=UTC))

    assert [bar.ts for bar in sliced] == [
        datetime(2026, 1, 2, 6, 30, tzinfo=UTC),
        datetime(2026, 1, 2, 7, 0, tzinfo=UTC),
    ]


def test_no_future_leakage_guard_raises():
    bars = [_bar("2026-01-02T07:30:00")]

    with pytest.raises(ValueError, match="future bar leakage"):
        assert_no_future_leakage(bars, datetime(2026, 1, 2, 7, 0, tzinfo=UTC))


def test_build_cutoff_windows_cursor_matches_legacy_slice():
    bars = [
        _bar("2026-01-02T06:30:00"),
        _bar("2026-01-02T06:35:00"),
        _bar("2026-01-02T07:00:00"),
    ]
    cutoffs = [
        datetime(2026, 1, 2, 6, 30, tzinfo=UTC),
        datetime(2026, 1, 2, 6, 35, tzinfo=UTC),
        datetime(2026, 1, 2, 7, 0, tzinfo=UTC),
    ]

    cursor_windows = build_cutoff_windows(bars, cutoffs, use_cursor=True)
    legacy_windows = build_cutoff_windows(bars, cutoffs, use_cursor=False)

    assert [[bar.ts for bar in window] for _, window in cursor_windows] == [
        [bar.ts for bar in window] for _, window in legacy_windows
    ]
