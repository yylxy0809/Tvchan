from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path
from time import perf_counter

from app.backtest.metrics import compute_metrics
from app.backtest.report_writer import write_report
from app.backtest.replay_engine import ReplayEngine
from app.config.strategy_execution import (
    require_diagnostic_strategy as require_diagnostic_backtest_strategy,
)
from app.config.strategy_params import (
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
)
from app.db import create_pool
from app.domain.enums import BacktestMode, MarketCapPolicy
from app.engine.coverage_audit import build_coverage_report
from app.engine.diagnostic_reporting import top_failure_gates
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.engine.strategy_runner import StrategyRunner
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository
from app.repositories.strategy_repo import StrategyRepository


def _strategy_meta(strategy_code: str) -> tuple[str, str]:
    if strategy_code == STRICT_EXPLICIT_B1_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Strict", "Diagnostic baseline with explicit prior weekly B1"
    if strategy_code == DIAG_TRUST_B2_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Diag Trust B2", "Diagnostic mode that trusts weekly B2 without explicit prior B1"
    if strategy_code == DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Diag Trust B2/B2s", "Diagnostic mode that trusts weekly B2 or B2s without explicit prior B1"
    if strategy_code == DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Diag Same-Bar B1/B2s", "Diagnostic mode for same-bar weekly B1 and B2s semantics"
    if strategy_code == SANITY_LOOSE_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Sanity Loose", "Loose diagnostic variant from phase 1.2"
    return "Weekly-Daily B2 Resonance", "Module C weekly B2 plus daily setup plus 30F/5F entry confirmation"


async def _run(args) -> int:
    params = require_diagnostic_backtest_strategy(args.strategy)
    started_at = perf_counter()
    worker_concurrency = max(1, args.concurrency)
    pool = await create_pool(max_size=max(4, worker_concurrency + 2))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        strategy_repo = StrategyRepository(pool)
        if args.market_cap_policy:
            params = params.with_overrides(market_cap_policy=args.market_cap_policy)
        strategy_name, description = _strategy_meta(params.strategy_code)
        await strategy_repo.ensure_default_definition(
            strategy_code=params.strategy_code,
            version=params.strategy_version,
            strategy_name=strategy_name,
            description=description,
            rule_spec_json=params.raw,
        )
        runner = StrategyRunner(module_c_repo, kline_repo)
        diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)
        engine = ReplayEngine(runner, module_c_repo, kline_repo)
        requested_symbols = list(args.symbols or [])
        if args.symbol:
            requested_symbols.append(args.symbol)
        all_symbols = await module_c_repo.list_active_symbols(limit=args.limit, symbols=requested_symbols or None)
        mode = BacktestMode(args.mode)
        start_time = datetime.fromisoformat(args.start)
        end_time = datetime.fromisoformat(args.end)
        symbols = await module_c_repo.filter_symbols_with_weekly_context(
            all_symbols,
            weekly_b2_types=params.weekly_b2_types,
            end_time=end_time,
        )
        run_id = await strategy_repo.create_backtest_run(
            strategy_code=params.strategy_code,
            strategy_version=params.strategy_version,
            run_name=args.run_name or f"{params.strategy_code}-{args.mode}",
            run_mode=args.mode,
            start_time=start_time,
            end_time=end_time,
            rule_spec_json=params.raw,
            data_source_json={"namespace": "c", "mode": args.mode},
            notes=None,
        )

        async def backtest_one(symbol):
            symbol_started = perf_counter()
            symbol_trades = await engine.backtest_symbol(
                symbol,
                params=params,
                start_time=start_time,
                end_time=end_time,
                mode=mode,
            )
            return symbol_trades, perf_counter() - symbol_started

        trades = []
        symbol_durations = []
        for index in range(0, len(symbols), worker_concurrency):
            batch = symbols[index : index + worker_concurrency]
            batch_results = await asyncio.gather(*(backtest_one(symbol) for symbol in batch))
            for symbol_trades, elapsed in batch_results:
                trades.extend(symbol_trades)
                symbol_durations.append(elapsed)
        metrics = compute_metrics(trades)
        coverage_started = perf_counter()
        coverage_report = await build_coverage_report(pool, as_of_time=end_time, params=params)
        coverage_elapsed = perf_counter() - coverage_started

        async def diagnose_one(symbol):
            return await diagnoser.diagnose_symbol(symbol, as_of_time=end_time, params=params)

        diagnosis_started = perf_counter()
        diagnoses = []
        for index in range(0, len(symbols), worker_concurrency):
            batch = symbols[index : index + worker_concurrency]
            diagnoses.extend(await asyncio.gather(*(diagnose_one(symbol) for symbol in batch)))
        diagnosis_elapsed = perf_counter() - diagnosis_started
        eligibility = coverage_report["scan_eligibility"]
        eligible_symbols = eligibility["eligible_symbols_require"]
        if params.market_cap_policy == MarketCapPolicy.WARN_ALLOW_MISSING:
            eligible_symbols = eligibility["eligible_symbols_warn_allow_missing"]
        elif params.market_cap_policy == MarketCapPolicy.IGNORE:
            eligible_symbols = eligibility["eligible_symbols_ignore"]
        await strategy_repo.insert_trades(run_id, params.strategy_code, params.strategy_version, trades)
        await strategy_repo.finalize_backtest_run(run_id, metrics, len(all_symbols))
        market_cap_coverage = coverage_report["market_cap_coverage"]
        write_report(
            Path(args.output_dir),
            trades,
            metrics,
            {
                "strategy_code": params.strategy_code,
                "strategy_version": params.strategy_version,
                "run_mode": args.mode,
                "total_symbols": len(all_symbols),
                "replayed_symbols": len(symbols),
                "eligible_symbols": eligible_symbols,
                "market_cap_policy": params.market_cap_policy.value,
                "market_cap_rule_enabled": params.market_cap_min > 0,
                "market_cap_filter_applied": params.market_cap_policy != MarketCapPolicy.IGNORE,
                "market_cap_hard_filter_effective": (
                    params.market_cap_policy == MarketCapPolicy.REQUIRE
                    and market_cap_coverage["active_with_market_cap_ratio"] > 0
                ),
                "market_cap_missing_allowed": params.allow_missing_market_cap,
                "market_cap_data_coverage_ratio": market_cap_coverage["active_with_market_cap_ratio"],
                "data_coverage_summary": coverage_report,
                "top_failure_gates": top_failure_gates(diagnoses),
                "first_seen_time_source": params.first_seen_time_source,
                "backtest_perf": {
                    "elapsed_seconds": round(perf_counter() - started_at, 3),
                    "coverage_elapsed_seconds": round(coverage_elapsed, 3),
                    "diagnosis_elapsed_seconds": round(diagnosis_elapsed, 3),
                    "symbol_backtest_elapsed_seconds_total": round(sum(symbol_durations), 3),
                    "worker_concurrency": worker_concurrency,
                    "symbol_backtest_elapsed_seconds_p95": round(
                        sorted(symbol_durations)[min(len(symbol_durations) - 1, int(len(symbol_durations) * 0.95))]
                        if symbol_durations
                        else 0.0,
                        3,
                    ),
                },
            },
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="weekly_daily_b2_resonance_v1")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--mode", choices=[mode.value for mode in BacktestMode], default=BacktestMode.EVENT_REPLAY.value)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--symbol")
    parser.add_argument("--market-cap-policy", choices=[item.value for item in MarketCapPolicy])
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--run-name")
    parser.add_argument("--output-dir", default="outputs/strategy-backtest")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
