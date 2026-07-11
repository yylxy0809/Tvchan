from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.config.strategy_params import PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE, StrategyParams
from app.db import create_pool
from app.engine.module_c_history_audit import (
    build_effective_backtest_window,
    build_historical_run_lookup_audit,
    build_module_c_history_coverage,
    render_effective_backtest_window_markdown,
    render_historical_run_lookup_audit_markdown,
    render_module_c_history_coverage_markdown,
    render_phase_1_5_summary_markdown,
    write_module_c_history_coverage_csv,
)
from app.engine.phase_1_4 import build_weekly_context_compare, run_historical_backtest, write_phase_1_4_outputs
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args) -> int:
    start_time = datetime.fromisoformat(args.start)
    end_time = datetime.fromisoformat(args.end)
    levels = tuple(item.strip() for item in args.levels.split(",") if item.strip())
    requested_symbols = list(args.symbols or [])
    pool = await create_pool(max_size=max(6, args.concurrency + 2))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        coverage = await build_module_c_history_coverage(
            pool,
            start_time=start_time,
            end_time=end_time,
            levels=levels,
            mode=args.mode,
            symbols=requested_symbols or None,
            limit=args.limit,
        )
        lookup_audit = await build_historical_run_lookup_audit(
            module_c_repo,
            levels=levels,
            mode=args.mode,
        )
        effective_window = build_effective_backtest_window(coverage)
        symbols = await module_c_repo.list_active_symbols(limit=args.limit or None, symbols=requested_symbols or None)
        as_of_time = datetime.fromisoformat(args.as_of or args.end)
        weekly_context_compare = await build_weekly_context_compare(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            as_of_time=as_of_time,
            concurrency=max(1, args.concurrency),
        )
        params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
        historical_backtest = await run_historical_backtest(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            params=params,
            start_time=start_time,
            end_time=end_time,
            concurrency=max(1, args.concurrency),
        )
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "module_c_history_coverage.json").write_text(
            json.dumps(coverage, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "module_c_history_coverage.md").write_text(
            render_module_c_history_coverage_markdown(coverage),
            encoding="utf-8",
        )
        write_module_c_history_coverage_csv(coverage, str(output_dir / "module_c_history_coverage.csv"))
        (output_dir / "historical_run_lookup_audit.json").write_text(
            json.dumps(lookup_audit, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "historical_run_lookup_audit.md").write_text(
            render_historical_run_lookup_audit_markdown(lookup_audit),
            encoding="utf-8",
        )
        (output_dir / "effective_backtest_window.json").write_text(
            json.dumps(effective_window, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "effective_backtest_window.md").write_text(
            render_effective_backtest_window_markdown(effective_window),
            encoding="utf-8",
        )
        write_phase_1_4_outputs(
            output_dir,
            weekly_context_compare=weekly_context_compare,
            historical_backtest=historical_backtest,
            start_time=start_time,
            end_time=end_time,
        )
        (output_dir / "replay_data_audit.json").write_text(
            json.dumps(historical_backtest.replay_audit, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "replay_data_audit.md").write_text(
            "\n".join(
                [
                    "# Replay Data Audit",
                    "",
                    f"- Replayed symbols: `{historical_backtest.replay_audit['replayed_symbols']}`",
                    f"- Total replay steps: `{historical_backtest.replay_audit['total_replay_steps']}`",
                    f"- Total symbol time points: `{historical_backtest.replay_audit['total_symbol_time_points']}`",
                    f"- Future leakage detected: `{historical_backtest.replay_audit['future_leakage_detected']}`",
                    f"- Future leakage violation count: `{historical_backtest.replay_audit['future_leakage_violation_count']}`",
                    f"- Backtest elapsed seconds: `{historical_backtest.replay_audit['backtest_elapsed_seconds']}`",
                    f"- Symbol elapsed p50: `{historical_backtest.replay_audit['symbol_backtest_elapsed_seconds_p50']}`",
                    f"- Symbol elapsed p95: `{historical_backtest.replay_audit['symbol_backtest_elapsed_seconds_p95']}`",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        (output_dir / "phase_1_5_summary.md").write_text(
            render_phase_1_5_summary_markdown(
                coverage=coverage,
                lookup_audit=lookup_audit,
                effective_window=effective_window,
                replay_audit=historical_backtest.replay_audit,
            ),
            encoding="utf-8",
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--as-of")
    parser.add_argument("--levels", default="5f,30f,1d,1w,1m")
    parser.add_argument("--mode", choices=("predictive", "confirmed"), default="predictive")
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--output-dir", default="outputs/phase-1-5")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
