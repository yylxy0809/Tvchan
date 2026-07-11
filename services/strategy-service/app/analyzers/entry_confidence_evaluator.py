from __future__ import annotations

from app.analyzers.fractal_detector import latest_bottom_fractal_time
from app.domain.models import EntryEvaluation
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


class EntryConfidenceEvaluator:
    def __init__(self, module_c_repo: ModuleCRepository, kline_repo: KlineRepository) -> None:
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo

    async def evaluate(self, symbol_id: int, daily_setup, as_of_time, params) -> EntryEvaluation:
        daily_floor_time = daily_setup.daily_b2.point_time if daily_setup.daily_b2 else daily_setup.daily_b1.point_time
        signals_30f = await self.module_c_repo.get_signals(
            symbol_id,
            "30f",
            mode="predictive",
            as_of_time=as_of_time,
            start=daily_floor_time,
        )
        thirty_b1 = next(
            (
                signal for signal in reversed(signals_30f)
                if signal.side == "buy" and signal.bsp_type == "1"
            ),
            None,
        )

        daily_bars = await self.kline_repo.get_klines(symbol_id, "1d", end=as_of_time)
        daily_bottom_time = latest_bottom_fractal_time(daily_bars, after=daily_floor_time)

        five_b2_confirm = None
        if thirty_b1 is not None:
            signals_5f = await self.module_c_repo.get_signals(
                symbol_id,
                "5f",
                mode="predictive",
                as_of_time=as_of_time,
                start=thirty_b1.point_time,
            )
            five_b2_confirm = next(
                (
                    signal for signal in reversed(signals_5f)
                    if signal.side == "buy" and signal.bsp_type in {"2", "2s"}
                ),
                None,
            )

        confidence_score = 0.0
        reasons: dict[str, object] = {
            "thirty_b1_time": thirty_b1.point_time.isoformat() if thirty_b1 else None,
            "daily_bottom_time": daily_bottom_time.isoformat() if daily_bottom_time else None,
            "five_b2_confirm_time": five_b2_confirm.point_time.isoformat() if five_b2_confirm else None,
        }
        if thirty_b1 is not None:
            confidence_score += params.confidence_weight("30F_B1")
        if daily_bottom_time is not None:
            confidence_score += params.confidence_weight("DAILY_BOTTOM_FRACTAL")
        if five_b2_confirm is not None:
            confidence_score += params.confidence_weight("5F_B2_CONFIRM_30F_B1")
        entry_level = "30f" if thirty_b1 is not None else ("5f" if (five_b2_confirm is not None or daily_bottom_time is not None) else None)

        return EntryEvaluation(
            confidence_score=confidence_score,
            has_30f_b1=thirty_b1 is not None,
            thirty_b1=thirty_b1,
            five_b2_confirm=five_b2_confirm,
            daily_bottom_time=daily_bottom_time,
            entry_level=entry_level,
            reasons=reasons,
        )
