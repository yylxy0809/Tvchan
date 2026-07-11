from __future__ import annotations

from app.analyzers.fractal_detector import latest_top_fractal_time
from app.domain.enums import ExitReason
from app.domain.models import ExitDecision, Trade
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


class ExitSignalEvaluator:
    def __init__(self, module_c_repo: ModuleCRepository, kline_repo: KlineRepository) -> None:
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo

    async def evaluate(self, trade: Trade, as_of_time, daily_setup) -> ExitDecision:
        daily_close = await self.kline_repo.get_close_at_or_before(trade.symbol.symbol_id, "1d", as_of_time)
        if daily_close is not None and daily_close[1] < trade.stop_price:
            return ExitDecision(
                should_exit=True,
                reason=ExitReason.DAILY_B1_BROKEN.value,
                signal_time=daily_close[0],
                execution_timeframe="1d",
            )

        if daily_setup.daily_b2 is None and daily_setup.daily_b2s is None:
            return ExitDecision(should_exit=False)

        signals_30f = await self.module_c_repo.get_signals(
            trade.symbol.symbol_id,
            "30f",
            mode="predictive",
            as_of_time=as_of_time,
            start=trade.entry_time,
        )
        thirty_s1 = next(
            (
                signal for signal in reversed(signals_30f)
                if signal.side == "sell" and signal.bsp_type == "1"
            ),
            None,
        )
        if thirty_s1 is not None:
            return ExitDecision(
                should_exit=True,
                reason=ExitReason.F30_S1.value,
                signal_time=thirty_s1.point_time,
                execution_timeframe="30f",
            )

        daily_bars = await self.kline_repo.get_klines(trade.symbol.symbol_id, "1d", end=as_of_time)
        daily_top = latest_top_fractal_time(daily_bars, after=trade.entry_time)
        if daily_top is not None:
            return ExitDecision(
                should_exit=True,
                reason=ExitReason.DAILY_TOP_FRACTAL.value,
                signal_time=daily_top,
                execution_timeframe="1d",
            )

        weekly_bars = await self.kline_repo.get_klines(trade.symbol.symbol_id, "1w", end=as_of_time)
        weekly_top = latest_top_fractal_time(weekly_bars, after=trade.entry_time)
        if weekly_top is not None:
            return ExitDecision(
                should_exit=True,
                reason=ExitReason.WEEKLY_TOP_FRACTAL.value,
                signal_time=weekly_top,
                execution_timeframe="1d",
            )
        return ExitDecision(should_exit=False)
