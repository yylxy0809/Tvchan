from __future__ import annotations

from datetime import datetime, timezone

from app.analyzers.fractal_detector import latest_bottom_fractal_time, latest_top_fractal_time
from app.repositories.kline_repo import KlineBar


def _bar(index: int, high: float, low: float) -> KlineBar:
    return KlineBar(
        ts=datetime(2026, 1, 1 + index, tzinfo=timezone.utc),
        open=(high + low) / 2,
        high=high,
        low=low,
        close=(high + low) / 2,
        volume=100,
    )


def test_latest_bottom_fractal_time_uses_third_bar_confirmation():
    bars = [
        _bar(0, 10, 8),
        _bar(1, 9, 7),
        _bar(2, 11, 8),
    ]
    assert latest_bottom_fractal_time(bars) == bars[2].ts


def test_latest_top_fractal_time_uses_third_bar_confirmation():
    bars = [
        _bar(0, 10, 8),
        _bar(1, 12, 9),
        _bar(2, 11, 8),
    ]
    assert latest_top_fractal_time(bars) == bars[2].ts
