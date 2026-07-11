from __future__ import annotations

from app.domain.models import WeeklyContext
from app.repositories.kline_repo import KlineRepository, compute_macd
from app.repositories.module_c_repo import ModuleCRepository


class WeeklyContextAnalyzer:
    def __init__(self, module_c_repo: ModuleCRepository, kline_repo: KlineRepository) -> None:
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo

    async def find_active_context(self, symbol_id: int, as_of_time, params) -> WeeklyContext | None:
        signals = await self.module_c_repo.get_signals(symbol_id, "1w", mode="predictive", as_of_time=as_of_time)
        buy_signals = [signal for signal in signals if signal.side == "buy"]
        b1_candidates = [signal for signal in buy_signals if signal.bsp_type == "1"]
        b2_candidates = [signal for signal in buy_signals if signal.bsp_type == "2"]
        if not b1_candidates or not b2_candidates:
            return None
        weekly_b2 = b2_candidates[-1]
        previous_b1 = [signal for signal in b1_candidates if signal.point_time < weekly_b2.point_time]
        if not previous_b1:
            return None
        weekly_b1 = previous_b1[-1]
        if weekly_b2.price <= weekly_b1.price:
            return None

        weekly_bars = await self.kline_repo.get_klines(symbol_id, "1w", end=as_of_time)
        if not weekly_bars:
            return None
        macd = compute_macd(weekly_bars)
        macd_row = next((item for item in reversed(macd) if item["ts"] <= weekly_b2.point_time), None)
        if macd_row is None or macd_row["dif"] <= 0:
            return None
        latest_close = weekly_bars[-1].close
        is_active = latest_close >= weekly_b1.price
        if not is_active:
            return None
        return WeeklyContext(
            weekly_b1=weekly_b1,
            weekly_b2=weekly_b2,
            weekly_bsp_type=str(weekly_b2.bsp_type or ""),
            context_mode="explicit_prior_b1",
            context_score=100.0,
            anchor_time=weekly_b1.point_time,
            anchor_source="prior_weekly_b1",
            stop_reference_price=weekly_b1.price,
            stop_reference_source="weekly_b1_price",
            prior_weekly_b1_found=True,
            same_bar_with_b1=False,
            same_price_with_b1=False,
            dif=float(macd_row["dif"]),
            dea=float(macd_row["dea"]),
            latest_close=latest_close,
            is_active=True,
        )
