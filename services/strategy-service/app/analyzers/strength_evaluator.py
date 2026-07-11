from __future__ import annotations

from app.domain.models import ChanCenter, ChanStroke
from app.repositories.kline_repo import KlineBar, compute_macd


def evaluate_daily_first_up_strength(
    *,
    previous_down_stroke: ChanStroke,
    first_up_stroke: ChanStroke,
    daily_bars: list[KlineBar],
    daily_center_low: float | None,
    daily_center_high: float | None,
    sub_segments: list[ChanStroke],
    sub_centers: list[ChanCenter],
) -> dict:
    complexity_down = _complexity(previous_down_stroke, sub_segments, sub_centers)
    complexity_up = _complexity(first_up_stroke, sub_segments, sub_centers)
    efficiency_down = _efficiency(previous_down_stroke, complexity_down, daily_bars)
    efficiency_up = _efficiency(first_up_stroke, complexity_up, daily_bars)
    efficiency_ratio = _safe_ratio(efficiency_up, efficiency_down)

    if efficiency_ratio >= 1.2:
        structure_score = 40.0
    elif efficiency_ratio >= 1.0:
        structure_score = 30.0
    elif efficiency_ratio >= 0.8:
        structure_score = 20.0
    else:
        structure_score = 0.0

    location_state = "NO_ZONE"
    location_score = 0.0
    if daily_center_low is not None and daily_center_high is not None:
        end_price = first_up_stroke.end_price
        if end_price > daily_center_high:
            location_state = "BREAK_ABOVE_CENTER"
            location_score = 30.0
        elif end_price > daily_center_low:
            location_state = "ENTER_CENTER"
            location_score = 15.0
        else:
            location_state = "BELOW_CENTER"

    momentum = _momentum_features(previous_down_stroke, first_up_stroke, daily_bars)
    momentum_score = 0.0
    if momentum["macd_area_ratio"] >= 1.2:
        momentum_score += 10.0
    if momentum["macd_peak_ratio"] >= 1.2:
        momentum_score += 7.0
    if momentum["price_speed_ratio"] >= 1.0:
        momentum_score += 8.0
    if momentum["volume_ratio"] >= 1.0:
        momentum_score += 5.0

    return {
        "structure_score": structure_score,
        "location_score": location_score,
        "momentum_score": momentum_score,
        "strength_score": structure_score + location_score + momentum_score,
        "efficiency_ratio": efficiency_ratio,
        "location_state": location_state,
        **momentum,
    }


def _complexity(stroke: ChanStroke, segments: list[ChanStroke], centers: list[ChanCenter]) -> int:
    center_count = sum(1 for center in centers if center.start_time >= stroke.start_time and center.end_time <= stroke.end_time)
    segment_count = sum(1 for segment in segments if segment.start_time >= stroke.start_time and segment.end_time <= stroke.end_time)
    if center_count >= 2:
        return 4
    if center_count == 1:
        return 3
    if segment_count >= 2:
        return 2
    return 1


def _efficiency(stroke: ChanStroke, complexity: int, daily_bars: list[KlineBar]) -> float:
    duration = len([bar for bar in daily_bars if stroke.start_time <= bar.ts <= stroke.end_time]) or 1
    price_amp = abs(stroke.end_price - stroke.start_price)
    return price_amp / max(complexity, 1) / duration


def _momentum_features(previous_down_stroke: ChanStroke, first_up_stroke: ChanStroke, daily_bars: list[KlineBar]) -> dict:
    down_bars = [bar for bar in daily_bars if previous_down_stroke.start_time <= bar.ts <= previous_down_stroke.end_time]
    up_bars = [bar for bar in daily_bars if first_up_stroke.start_time <= bar.ts <= first_up_stroke.end_time]
    down_macd = compute_macd(down_bars)
    up_macd = compute_macd(up_bars)
    down_area = sum(abs(item["histogram"]) for item in down_macd) or 0.0
    up_area = sum(abs(item["histogram"]) for item in up_macd) or 0.0
    down_peak = max((abs(item["histogram"]) for item in down_macd), default=0.0)
    up_peak = max((abs(item["histogram"]) for item in up_macd), default=0.0)
    down_speed = abs(previous_down_stroke.end_price - previous_down_stroke.start_price) / max(len(down_bars), 1)
    up_speed = abs(first_up_stroke.end_price - first_up_stroke.start_price) / max(len(up_bars), 1)
    down_volume = sum(bar.volume for bar in down_bars)
    up_volume = sum(bar.volume for bar in up_bars)
    return {
        "macd_area_ratio": _safe_ratio(up_area, down_area),
        "macd_peak_ratio": _safe_ratio(up_peak, down_peak),
        "price_speed_ratio": _safe_ratio(up_speed, down_speed),
        "volume_ratio": _safe_ratio(float(up_volume), float(down_volume)),
    }


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0 if numerator <= 0 else 999.0
    return numerator / denominator
