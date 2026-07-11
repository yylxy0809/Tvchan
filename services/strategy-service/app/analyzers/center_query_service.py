from __future__ import annotations

from app.domain.models import ChanCenter, ChanStroke


def find_last_relevant_daily_center(
    daily_centers: list[ChanCenter],
    *,
    daily_b1_time,
    weekly_context_start_time,
) -> tuple[str | None, float | None, float | None]:
    candidates = [
        center
        for center in daily_centers
        if center.end_time <= daily_b1_time and center.start_time >= weekly_context_start_time
    ]
    if candidates:
        center = candidates[-1]
        return "CENTER", center.low, center.high
    return None, None, None


def fallback_segment_overlap(daily_segments: list[ChanStroke], *, daily_b1_time):
    candidates = [segment for segment in daily_segments if segment.end_time <= daily_b1_time]
    if len(candidates) < 2:
        return None, None
    left = candidates[-2]
    right = candidates[-1]
    low = max(min(left.start_price, left.end_price), min(right.start_price, right.end_price))
    high = min(max(left.start_price, left.end_price), max(right.start_price, right.end_price))
    if low > high:
        return None, None
    return low, high
