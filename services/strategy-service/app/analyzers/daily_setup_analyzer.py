from __future__ import annotations

from app.analyzers.center_query_service import fallback_segment_overlap, find_last_relevant_daily_center
from app.analyzers.strength_evaluator import evaluate_daily_first_up_strength
from app.domain.models import DailySetup
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


class DailySetupAnalyzer:
    def __init__(self, module_c_repo: ModuleCRepository, kline_repo: KlineRepository) -> None:
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo

    async def evaluate(self, symbol_id: int, weekly_context, as_of_time, params) -> DailySetup | None:
        signals = await self.module_c_repo.get_signals(symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        daily_b1_candidates = [
            signal for signal in signals
            if signal.side == "buy"
            and signal.bsp_type == "1"
            and signal.point_time >= weekly_context.weekly_b1.point_time
        ]
        if not daily_b1_candidates:
            return None
        daily_b1 = daily_b1_candidates[-1]
        daily_b2_candidates = [
            signal for signal in signals
            if signal.side == "buy"
            and signal.bsp_type == "2"
            and signal.point_time > daily_b1.point_time
            and signal.price > daily_b1.price
        ]
        daily_b2s_candidates = [
            signal for signal in signals
            if signal.side == "buy"
            and signal.bsp_type == "2s"
            and signal.point_time > daily_b1.point_time
            and signal.price > daily_b1.price
        ]
        strokes = await self.module_c_repo.get_strokes(symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        segments_30f = await self.module_c_repo.get_segments(symbol_id, "30f", mode="predictive", as_of_time=as_of_time)
        centers_30f = await self.module_c_repo.get_centers(symbol_id, "30f", mode="predictive", as_of_time=as_of_time)
        centers_1d = await self.module_c_repo.get_centers(symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        segments_1d = await self.module_c_repo.get_segments(symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        previous_down = _latest_stroke(strokes, direction="down", before=daily_b1.point_time)
        first_up = _first_stroke(strokes, direction="up", after=daily_b1.point_time)
        if previous_down is None or first_up is None:
            return None
        center_type, center_low, center_high = find_last_relevant_daily_center(
            centers_1d,
            daily_b1_time=daily_b1.point_time,
            weekly_context_start_time=weekly_context.weekly_b1.point_time,
        )
        if center_type is None:
            overlap_low, overlap_high = fallback_segment_overlap(segments_1d, daily_b1_time=daily_b1.point_time)
            if overlap_low is not None and overlap_high is not None:
                center_type = "SEGMENT_OVERLAP"
                center_low = overlap_low
                center_high = overlap_high

        daily_bars = await self.kline_repo.get_klines(symbol_id, "1d", start=previous_down.start_time, end=as_of_time)
        strength = evaluate_daily_first_up_strength(
            previous_down_stroke=previous_down,
            first_up_stroke=first_up,
            daily_bars=daily_bars,
            daily_center_low=center_low,
            daily_center_high=center_high,
            sub_segments=segments_30f,
            sub_centers=centers_30f,
        )
        if strength["strength_score"] < params.strength_threshold:
            return None
        return DailySetup(
            daily_b1=daily_b1,
            daily_b2=daily_b2_candidates[-1] if daily_b2_candidates else None,
            daily_b2s=daily_b2s_candidates[-1] if daily_b2s_candidates else None,
            previous_down_stroke=previous_down,
            first_up_stroke=first_up,
            center_low=center_low,
            center_high=center_high,
            center_type=center_type,
            structure_score=strength["structure_score"],
            location_score=strength["location_score"],
            momentum_score=strength["momentum_score"],
            strength_score=strength["strength_score"],
            features=strength,
        )


def _latest_stroke(strokes, *, direction: str, before):
    candidates = [stroke for stroke in strokes if stroke.direction == direction and stroke.end_time <= before]
    return candidates[-1] if candidates else None


def _first_stroke(strokes, *, direction: str, after):
    candidates = [stroke for stroke in strokes if stroke.direction == direction and stroke.start_time >= after]
    return candidates[0] if candidates else None
