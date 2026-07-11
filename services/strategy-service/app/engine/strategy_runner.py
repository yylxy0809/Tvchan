from __future__ import annotations

from app.analyzers.daily_setup_analyzer import DailySetupAnalyzer
from app.analyzers.entry_confidence_evaluator import EntryConfidenceEvaluator
from app.analyzers.weekly_context_analyzer import WeeklyContextAnalyzer
from app.config.strategy_params import StrategyParams
from app.domain.enums import ScanStatus
from app.domain.models import ScanResult, SymbolInfo
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


class StrategyRunner:
    def __init__(self, module_c_repo: ModuleCRepository, kline_repo: KlineRepository) -> None:
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo
        self.weekly_context_analyzer = WeeklyContextAnalyzer(module_c_repo, kline_repo)
        self.daily_setup_analyzer = DailySetupAnalyzer(module_c_repo, kline_repo)
        self.entry_confidence_evaluator = EntryConfidenceEvaluator(module_c_repo, kline_repo)
        self.diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)

    async def evaluate_symbol(
        self,
        symbol: SymbolInfo,
        *,
        as_of_time,
        params: StrategyParams,
    ) -> ScanResult | None:
        diagnosis = await self.diagnoser.diagnose_symbol(symbol, as_of_time=as_of_time, params=params)
        return diagnosis.result
