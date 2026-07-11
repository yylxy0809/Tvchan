from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.analyzers.strength_evaluator import evaluate_daily_first_up_strength
from app.domain.models import ChanCenter, ChanStroke
from app.repositories.kline_repo import KlineBar


def test_strength_evaluator_scores_break_above_center():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    prev = ChanStroke(0, "1d", "predictive", "down", start, start + timedelta(days=2), 10.0, 8.0, start, start + timedelta(days=2), True)
    up = ChanStroke(1, "1d", "predictive", "up", start + timedelta(days=3), start + timedelta(days=5), 8.2, 11.5, start + timedelta(days=3), start + timedelta(days=5), True)
    bars = [
        KlineBar(start + timedelta(days=index), 9.0, 10.0 + index * 0.1, 8.0, 9.0 + index * 0.2, 100 + index * 10)
        for index in range(7)
    ]
    result = evaluate_daily_first_up_strength(
        previous_down_stroke=prev,
        first_up_stroke=up,
        daily_bars=bars,
        daily_center_low=9.0,
        daily_center_high=10.5,
        sub_segments=[],
        sub_centers=[],
    )
    assert result["location_state"] == "BREAK_ABOVE_CENTER"
    assert result["strength_score"] >= 30.0
