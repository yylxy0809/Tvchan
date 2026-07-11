from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.config.strategy_params import (
    PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
    StrategyParams,
)
from app.db import create_pool
from app.engine.diagnostic_reporting import diagnosis_to_dict, render_trace_markdown
from app.engine.module_c_history_audit import (
    build_effective_backtest_window,
    build_module_c_history_coverage,
    render_effective_backtest_window_markdown,
    render_module_c_history_coverage_markdown,
    write_module_c_history_coverage_csv,
)
from app.engine.module_c_history_backfill import (
    DEFAULT_LEVELS,
    DEFAULT_PROFILE,
    build_backfill_dry_run,
    preload_symbol_bars,
    render_backfill_plan_markdown,
    render_backfill_summary_markdown,
    run_historical_backfill,
    write_failed_symbols_jsonl,
    write_runs_manifest_csv,
)
from app.engine.phase_1_4 import (
    _render_backtest_report_markdown,
    _render_historical_gate_waterfall_markdown,
    _render_replay_audit_markdown,
    _render_trade_analysis_markdown,
    run_historical_backtest,
)
from app.engine.phase_1_7 import (
    DEFAULT_PHASE_1_7_SYMBOLS,
    PHASE_1_7_WEEKLY_CONTEXT_STRATEGIES,
    build_performance_scale_estimate,
    build_preflight_audit,
    enrich_dry_run_estimates,
    render_performance_scale_estimate_markdown,
    render_phase_1_7_summary_markdown,
    render_phase_1_7_task_checklist_report,
    render_preflight_audit_markdown,
    serialize_backfill_summary,
    write_json,
)
from app.engine.weekly_signal_distribution import (
    build_weekly_signal_distribution,
    render_weekly_signal_distribution_markdown,
)
from app.engine.weekly_context_audit import build_weekly_context_audit
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository
from app.engine.strategy_diagnoser import StrategyDiagnoser


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    levels = tuple(item.strip() for item in args.levels.split(",") if item.strip())
    requested_symbols = args.symbols.split(",") if args.symbols else DEFAULT_PHASE_1_7_SYMBOLS
    warmup_start = datetime.fromisoformat(args.warmup_start)
    backtest_start = datetime.fromisoformat(args.backtest_start)
    end_time = datetime.fromisoformat(args.end)

    pool = await create_pool(max_size=max(8, args.max_workers + 4))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        symbols = await module_c_repo.list_active_symbols(symbols=requested_symbols)

        preflight = await build_preflight_audit(
            pool,
            symbols=symbols,
            levels=levels,
            profile=args.profile,
            mode=args.mode,
            warmup_start=warmup_start,
            backtest_start=backtest_start,
            end_time=end_time,
        )
        write_json(output_dir / "preflight_audit.json", preflight)
        (output_dir / "preflight_audit.md").write_text(render_preflight_audit_markdown(preflight), encoding="utf-8")

        bars_by_symbol = await preload_symbol_bars(
            kline_repo=kline_repo,
            symbols=symbols,
            levels=levels,
            warmup_start=warmup_start,
            end_time=end_time,
        )
        dry_run = enrich_dry_run_estimates(
            build_backfill_dry_run(
                symbols=symbols,
                bars_by_symbol=bars_by_symbol,
                profile=args.profile,
                warmup_start=warmup_start,
                backtest_start=backtest_start,
                end_time=end_time,
                levels=levels,
                mode=args.mode,
            ),
            bars_by_symbol=bars_by_symbol,
            symbols=symbols,
            levels=levels,
        )
        write_json(output_dir / "backfill_10_symbols_dry_run.json", dry_run)
        (output_dir / "backfill_10_symbols_plan.md").write_text(render_backfill_plan_markdown(dry_run), encoding="utf-8")

        summary = await run_historical_backfill(
            pool=pool,
            symbols=symbols,
            bars_by_symbol=bars_by_symbol,
            profile=args.profile,
            warmup_start=warmup_start,
            backtest_start=backtest_start,
            end_time=end_time,
            levels=levels,
            mode=args.mode,
            max_workers=args.max_workers,
            resume=args.resume,
        )
        write_json(output_dir / "backfill_10_symbols_summary.json", serialize_backfill_summary(summary))
        (output_dir / "backfill_10_symbols_summary.md").write_text(render_backfill_summary_markdown(summary), encoding="utf-8")
        write_json(
            output_dir / "backfill_perf.json",
            {
                "elapsed_seconds": summary["elapsed_seconds"],
                "symbol_elapsed_seconds_p50": summary["symbol_elapsed_seconds_p50"],
                "symbol_elapsed_seconds_p95": summary["symbol_elapsed_seconds_p95"],
                "written_runs": summary["written_runs"],
                "skipped_existing_runs": summary.get("skipped_existing_runs", 0),
                "failed_runs": summary["failed_runs"],
            },
        )
        write_failed_symbols_jsonl(output_dir / "backfill_failed_symbols.jsonl", summary["failures"])
        write_runs_manifest_csv(output_dir / "backfilled_runs_manifest.csv", summary["results"])

        coverage = await build_module_c_history_coverage(
            pool,
            start_time=backtest_start,
            end_time=end_time,
            levels=levels,
            mode=args.mode,
            symbols=[symbol.symbol for symbol in symbols],
            limit=0,
        )
        write_json(output_dir / "backfill_10_symbols_coverage.json", coverage)
        (output_dir / "backfill_10_symbols_coverage.md").write_text(
            render_module_c_history_coverage_markdown(coverage),
            encoding="utf-8",
        )
        write_module_c_history_coverage_csv(coverage, str(output_dir / "backfill_10_symbols_coverage.csv"))
        effective_window = build_effective_backtest_window(coverage)
        write_json(output_dir / "effective_backtest_window_after_10_symbols.json", effective_window)
        (output_dir / "effective_backtest_window_after_10_symbols.md").write_text(
            render_effective_backtest_window_markdown(effective_window),
            encoding="utf-8",
        )

        replay_start = datetime.fromisoformat(effective_window["strict_global_effective_start"]) if effective_window.get("strict_global_window_valid") else backtest_start
        replay_end = datetime.fromisoformat(effective_window["strict_global_effective_end"]) if effective_window.get("strict_global_window_valid") else end_time

        compare_rows: list[dict] = []
        compare_backtests: dict[str, object] = {}
        for strategy_code in PHASE_1_7_WEEKLY_CONTEXT_STRATEGIES:
            params = StrategyParams.from_strategy_code(strategy_code)
            result = await run_historical_backtest(
                module_c_repo=module_c_repo,
                kline_repo=kline_repo,
                symbols=symbols,
                params=params,
                start_time=replay_start,
                end_time=replay_end,
                concurrency=max(1, args.max_workers),
            )
            gate_rows = result.gate_waterfall["rows"]
            replay_audit = dict(result.replay_audit)
            replay_audit["gate_waterfall_rows"] = gate_rows
            compare_backtests[strategy_code] = {
                "replay_audit": replay_audit,
                "gate_waterfall": result.gate_waterfall,
                "trade_analysis": result.trade_analysis,
                "metrics": result.metrics,
                "trades": result.trades,
            }
            compare_rows.append(
                {
                    "strategy_code": strategy_code,
                    "weekly_context_mode": params.weekly_context_mode_normalized,
                    "replayed_symbols": replay_audit["replayed_symbols"],
                    "total_replay_steps": replay_audit["total_replay_steps"],
                    "future_leakage_detected": replay_audit["future_leakage_detected"],
                    "trade_count": result.metrics["total_trades"],
                    "module_c_all_runs_available_pass_rate": _gate_rate(gate_rows, "module_c_all_runs_available"),
                    "backtest_elapsed_seconds": replay_audit["backtest_elapsed_seconds"],
                    "top_failure_gates": _top_failed_gates(gate_rows, limit=5),
                }
            )

        compare_payload = {
            "start_time": replay_start.isoformat(),
            "end_time": replay_end.isoformat(),
            "symbol_count": len(symbols),
            "rows": compare_rows,
        }
        main_code = PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE
        main_backtest = compare_backtests[main_code]
        main_replay_audit = main_backtest["replay_audit"]
        main_gate_waterfall = main_backtest["gate_waterfall"]
        main_trade_analysis = main_backtest["trade_analysis"]
        main_metrics = main_backtest["metrics"]

        write_json(output_dir / "weekly_context_compare_after_10_symbols.json", compare_payload)
        (output_dir / "weekly_context_compare_after_10_symbols.md").write_text(
            _render_weekly_context_compare_after_10_symbols(compare_payload),
            encoding="utf-8",
        )
        write_json(output_dir / "replay_after_10_symbols_audit.json", main_replay_audit)
        (output_dir / "replay_after_10_symbols_audit.md").write_text(
            _render_replay_audit_markdown(main_replay_audit),
            encoding="utf-8",
        )
        write_json(output_dir / "gate_waterfall_after_10_symbols.json", main_gate_waterfall)
        (output_dir / "gate_waterfall_after_10_symbols.md").write_text(
            _render_historical_gate_waterfall_markdown(main_gate_waterfall),
            encoding="utf-8",
        )
        (output_dir / "backtest_report_after_10_symbols.md").write_text(
            _render_backtest_report_markdown(
                main_metrics,
                main_replay_audit,
                main_trade_analysis,
                start_time=replay_start,
                end_time=replay_end,
            ),
            encoding="utf-8",
        )
        (output_dir / "trade_analysis_after_10_symbols.md").write_text(
            _render_trade_analysis_after_10_symbols(main_trade_analysis, main_replay_audit, main_gate_waterfall),
            encoding="utf-8",
        )

        diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)
        trace_params = StrategyParams.from_strategy_code(main_code)
        symbol_effective = {
            item["symbol"]: item
            for item in coverage["summary"].get("effective_symbol_windows", [])
        }
        diagnoses = []
        for symbol in symbols:
            symbol_window = symbol_effective.get(symbol.symbol)
            as_of_time = replay_end
            if symbol_window and symbol_window.get("effective_end"):
                candidate = datetime.fromisoformat(symbol_window["effective_end"])
                if candidate < as_of_time:
                    as_of_time = candidate
            diagnosis = await diagnoser.diagnose_symbol(symbol, as_of_time=as_of_time, params=trace_params)
            diagnoses.append(diagnosis)
            trace_dir = traces_dir / symbol.symbol
            trace_dir.mkdir(parents=True, exist_ok=True)
            write_json(trace_dir / "trace.json", diagnosis_to_dict(diagnosis))
            (trace_dir / "trace.md").write_text(render_trace_markdown(diagnosis), encoding="utf-8")
        write_json(traces_dir / "diagnoses.json", [diagnosis_to_dict(item) for item in diagnoses])

        weekly_context_audit = await build_weekly_context_audit(pool, as_of_time=replay_end)
        write_json(output_dir / "weekly_context_audit_after_10_symbols.json", weekly_context_audit)
        weekly_signal_distribution = await build_weekly_signal_distribution(pool, as_of_time=replay_end)
        write_json(output_dir / "weekly_signal_distribution_after_10_symbols.json", weekly_signal_distribution)
        (output_dir / "weekly_signal_distribution_after_10_symbols.md").write_text(
            render_weekly_signal_distribution_markdown(weekly_signal_distribution),
            encoding="utf-8",
        )

        performance = build_performance_scale_estimate(
            dry_run=dry_run,
            backfill_summary=summary,
            effective_window=effective_window,
            replay_audit=main_replay_audit,
        )
        write_json(output_dir / "performance_scale_estimate.json", performance)
        (output_dir / "performance_scale_estimate.md").write_text(
            render_performance_scale_estimate_markdown(performance),
            encoding="utf-8",
        )

        deliverables = [
            "preflight_audit.md",
            "preflight_audit.json",
            "backfill_10_symbols_plan.md",
            "backfill_10_symbols_dry_run.json",
            "backfill_10_symbols_summary.md",
            "backfill_10_symbols_summary.json",
            "backfill_10_symbols_coverage.md",
            "backfill_10_symbols_coverage.json",
            "effective_backtest_window_after_10_symbols.md",
            "replay_after_10_symbols_audit.md",
            "replay_after_10_symbols_audit.json",
            "gate_waterfall_after_10_symbols.md",
            "gate_waterfall_after_10_symbols.json",
            "weekly_context_compare_after_10_symbols.md",
            "weekly_context_compare_after_10_symbols.json",
            "backtest_report_after_10_symbols.md",
            "trade_analysis_after_10_symbols.md",
            "backfill_perf.json",
            "backfill_failed_symbols.jsonl",
            "backfilled_runs_manifest.csv",
            "performance_scale_estimate.md",
            "performance_scale_estimate.json",
            "traces/",
        ]
        (output_dir / "phase_1_7_summary.md").write_text(
            render_phase_1_7_summary_markdown(
                preflight=preflight,
                dry_run=dry_run,
                backfill_summary=summary,
                coverage=coverage,
                effective_window=effective_window,
                replay_audit=main_replay_audit,
                performance=performance,
                strategy_30f_executed=False,
            ),
            encoding="utf-8",
        )
        (output_dir / "phase_1_7_task_checklist_report.md").write_text(
            render_phase_1_7_task_checklist_report(
                preflight_done=True,
                dry_run_done=True,
                backfill_summary=summary,
                effective_window=effective_window,
                replay_audit=main_replay_audit,
                coverage=coverage,
                deliverables=deliverables,
            ),
            encoding="utf-8",
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--symbols")
    parser.add_argument("--warmup-start", default="2024-01-01T00:00:00+00:00")
    parser.add_argument("--backtest-start", default="2025-01-01T00:00:00+00:00")
    parser.add_argument("--end", default="2026-07-01T00:00:00+00:00")
    parser.add_argument("--mode", choices=("predictive", "confirmed"), default="predictive")
    parser.add_argument("--levels", default=",".join(DEFAULT_LEVELS))
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--output-dir",
        default="services/strategy-service/outputs/phase-1-7-10-symbols",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


def _gate_rate(rows: list[dict], gate_name: str) -> float:
    for row in rows:
        if row["gate"] == gate_name:
            return float(row.get("pass_rate_from_total") or 0.0)
    return 0.0


def _top_failed_gates(rows: list[dict], *, limit: int) -> list[dict]:
    failed = [row for row in rows if row["failed"] > 0]
    failed.sort(key=lambda item: item["failed"], reverse=True)
    return [{"gate": row["gate"], "failed": row["failed"]} for row in failed[:limit]]


def _render_weekly_context_compare_after_10_symbols(payload: dict[str, object]) -> str:
    lines = [
        "# Weekly Context Compare After 10 Symbols",
        "",
        f"- Start: `{payload['start_time']}`",
        f"- End: `{payload['end_time']}`",
        f"- Symbol count: `{payload['symbol_count']}`",
        "",
        "| Mode | Replay Symbols | Replay Steps | Trades | All Module C Runs Pass Rate | Backtest Seconds |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["rows"]:
        lines.append(
            f"| `{row['weekly_context_mode']}` | {row['replayed_symbols']} | {row['total_replay_steps']} | "
            f"{row['trade_count']} | {row['module_c_all_runs_available_pass_rate']:.4f} | {row['backtest_elapsed_seconds']} |"
        )
    lines.append("")
    for row in payload["rows"]:
        lines.append(f"## {row['weekly_context_mode']}")
        lines.append("")
        lines.append(f"- Strategy: `{row['strategy_code']}`")
        lines.append(f"- Top failure gates: `{json.dumps(row['top_failure_gates'], ensure_ascii=False)}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def _render_trade_analysis_after_10_symbols(
    trade_analysis: dict[str, object],
    replay_audit: dict[str, object],
    gate_waterfall: dict[str, object],
) -> str:
    lines = [
        _render_trade_analysis_markdown(trade_analysis).rstrip(),
        "",
        "## No-Trade Explanation",
        "",
    ]
    if int(replay_audit.get("total_replay_steps") or 0) == 0:
        lines.append("- replay steps are zero, so this phase did not reach a meaningful strategy evaluation window.")
    elif int(trade_analysis.get("success_samples") is not None) and int((replay_audit.get("future_leakage_violation_count") or 0)) > 0:
        lines.append("- replay had future-leakage violations and cannot be treated as a trustworthy no-trade result.")
    else:
        module_c_rate = _gate_rate(gate_waterfall["rows"], "module_c_all_runs_available")
        lines.append(f"- module_c_all_runs_available pass rate: `{module_c_rate:.4f}`")
        if module_c_rate >= 0.95:
            lines.append("- no-trade outcomes should be interpreted as strategy gates, not missing Module C historical runs.")
        else:
            lines.append("- trade scarcity may still be contaminated by Module C run availability gaps.")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
