from __future__ import annotations

from datetime import datetime

from app.analyzers.exit_signal_evaluator import ExitSignalEvaluator
from app.config.strategy_params import StrategyParams
from app.domain.enums import BacktestMode, ScanStatus
from app.domain.models import Trade
from app.engine.strategy_runner import StrategyRunner
from app.repositories.kline_repo import KlineBar, KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


class ReplayEngine:
    def __init__(
        self,
        runner: StrategyRunner,
        module_c_repo: ModuleCRepository,
        kline_repo: KlineRepository,
    ) -> None:
        self.runner = runner
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo
        self.exit_evaluator = ExitSignalEvaluator(module_c_repo, kline_repo)

    async def backtest_symbol(
        self,
        symbol,
        *,
        params: StrategyParams,
        start_time: datetime,
        end_time: datetime,
        mode: BacktestMode,
    ) -> list[Trade]:
        await self.kline_repo.prime_symbol_cache(symbol.symbol_id, start_time=start_time, end_time=end_time)
        await self.module_c_repo.prime_symbol_cache(symbol.symbol_id)
        try:
            bars_30f = await self.kline_repo.get_klines(symbol.symbol_id, "30f", start=start_time, end=end_time)
            trades: list[Trade] = []
            position: Trade | None = None

            for index, bar in enumerate(bars_30f):
                as_of_time = bar.ts
                if position is None:
                    result = await self.runner.evaluate_symbol(symbol, as_of_time=as_of_time, params=params)
                    if result is None:
                        continue
                    if mode == BacktestMode.EXPLORATORY_STATIC:
                        should_open = result.status in {ScanStatus.WATCH, ScanStatus.TRIGGER}
                    else:
                        should_open = result.status == ScanStatus.TRIGGER
                    if not should_open:
                        continue
                    next_bar = bars_30f[index + 1] if index + 1 < len(bars_30f) else None
                    if next_bar is None:
                        continue
                    position = Trade(
                        symbol=symbol,
                        entry_time=next_bar.ts,
                        entry_price=next_bar.open,
                        entry_reason=result.status.value,
                        entry_confidence=result.entry.confidence_score,
                        entry_level=result.entry.entry_level or "30f",
                        daily_b1_price=result.daily_setup.daily_b1.price,
                        stop_price=result.daily_setup.daily_b1.price,
                        features={
                            "weekly_b1": result.weekly_context.weekly_b1.point_time.isoformat() if result.weekly_context.weekly_b1 else None,
                            "weekly_b2": result.weekly_context.weekly_b2.point_time.isoformat(),
                            "weekly_context_mode": result.weekly_context.context_mode,
                            "weekly_bsp_type": result.weekly_context.weekly_bsp_type,
                            "stop_reference_source": result.weekly_context.stop_reference_source,
                            "daily_b1": result.daily_setup.daily_b1.point_time.isoformat(),
                            "daily_b2": result.daily_setup.daily_b2.point_time.isoformat() if result.daily_setup.daily_b2 else None,
                            "strength_score": result.daily_setup.strength_score,
                            "confidence_score": result.entry.confidence_score,
                            "entry_level": result.entry.entry_level,
                        },
                    )
                    continue

                position.holding_bars += 1
                position.holding_days = max(position.holding_days, (bar.ts.date() - position.entry_time.date()).days)
                mfe = (bar.high - position.entry_price) / position.entry_price
                mae = (bar.low - position.entry_price) / position.entry_price
                position.max_favorable_pct = max(position.max_favorable_pct, mfe)
                position.max_adverse_pct = min(position.max_adverse_pct, mae)
                result = await self.runner.evaluate_symbol(symbol, as_of_time=as_of_time, params=params)
                if result is None:
                    continue
                exit_decision = await self.exit_evaluator.evaluate(position, as_of_time, result.daily_setup)
                if not exit_decision.should_exit:
                    continue
                execution = await self.kline_repo.get_next_open(
                    symbol.symbol_id,
                    exit_decision.execution_timeframe or "30f",
                    exit_decision.signal_time or as_of_time,
                )
                if execution is None:
                    position.exit_time = bar.ts
                    position.exit_price = bar.close
                else:
                    position.exit_time = execution[0]
                    position.exit_price = execution[1]
                position.exit_reason = exit_decision.reason
                trades.append(position)
                position = None

            if position is not None and bars_30f:
                last_bar: KlineBar = bars_30f[-1]
                position.exit_time = last_bar.ts
                position.exit_price = last_bar.close
                position.exit_reason = "FORCED_END"
                trades.append(position)
            return trades
        finally:
            self.kline_repo.release_symbol_cache(symbol.symbol_id)
            self.module_c_repo.release_symbol_cache(symbol.symbol_id)
