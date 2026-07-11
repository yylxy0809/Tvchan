from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.phase_1_8 import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PHASE_1_7_SYMBOLS,
    build_backfill_performance_profile,
    build_daily_setup_audit,
    build_daily_setup_compare,
    build_daily_signal_distribution,
    build_performance_scale_estimate_after_optimization,
    build_trace_plan,
    load_phase_1_7_inputs,
    materialize_traces,
    render_backfill_performance_profile_markdown,
    render_daily_setup_audit_markdown,
    render_daily_setup_compare_markdown,
    render_daily_signal_distribution_markdown,
    render_performance_scale_estimate_after_optimization_markdown,
    render_phase_1_8_summary_markdown,
    render_phase_1_8_task_checklist_report,
)
from app.engine.module_c_history_backfill import preload_symbol_bars, build_backfill_dry_run
from app.engine.phase_1_7 import write_json
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    phase_1_7_inputs = load_phase_1_7_inputs()
    effective_window = phase_1_7_inputs["effective_window"]
    start_time = datetime.fromisoformat(effective_window["strict_global_effective_start"])
    end_time = datetime.fromisoformat(effective_window["strict_global_effective_end"])

    pool = await create_pool(max_size=max(8, args.max_workers + 4))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        requested_symbols = args.symbols.split(",") if args.symbols else DEFAULT_PHASE_1_7_SYMBOLS
        symbols = await module_c_repo.list_active_symbols(symbols=requested_symbols)

        daily_distribution = await build_daily_signal_distribution(
            pool,
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            profile="research_daily_close",
            mode="predictive",
        )
        write_json(output_dir / "daily_signal_distribution_after_10_symbols.json", daily_distribution)
        (output_dir / "daily_signal_distribution_after_10_symbols.md").write_text(
            render_daily_signal_distribution_markdown(daily_distribution),
            encoding="utf-8",
        )

        daily_audit = await build_daily_setup_audit(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            concurrency=args.max_workers,
        )
        write_json(output_dir / "daily_setup_audit.json", daily_audit)
        (output_dir / "daily_setup_audit.md").write_text(
            render_daily_setup_audit_markdown(daily_audit),
            encoding="utf-8",
        )
        with (output_dir / "daily_setup_failure_samples.jsonl").open("w", encoding="utf-8") as handle:
            for row in daily_audit["rows"]:
                if not row["strict_daily_setup_found"]:
                    handle.write(__import__("json").dumps(row, ensure_ascii=False) + "\n")

        compare_payload = await build_daily_setup_compare(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            concurrency=args.max_workers,
        )
        write_json(output_dir / "daily_setup_compare.json", compare_payload["summary"])
        (output_dir / "daily_setup_compare.md").write_text(
            render_daily_setup_compare_markdown(compare_payload),
            encoding="utf-8",
        )
        gate_waterfall_daily_modes = {
            mode_name: backtest.gate_waterfall for mode_name, backtest in compare_payload["backtests"].items()
        }
        write_json(output_dir / "gate_waterfall_daily_modes.json", gate_waterfall_daily_modes)
        from app.engine.phase_1_8 import _render_gate_waterfall_daily_modes
        (output_dir / "gate_waterfall_daily_modes.md").write_text(
            _render_gate_waterfall_daily_modes(compare_payload),
            encoding="utf-8",
        )
        replay_after_daily_setup_audit = {
            mode_name: backtest.replay_audit for mode_name, backtest in compare_payload["backtests"].items()
        }
        write_json(output_dir / "replay_after_daily_setup_audit.json", replay_after_daily_setup_audit)
        (output_dir / "replay_after_daily_setup_audit.md").write_text(
            __import__("json").dumps(replay_after_daily_setup_audit, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        summary_rows = compare_payload["summary"]["rows"]
        trade_report_lines = [
            "# Backtest Report Daily Setup Modes",
            "",
            "| Mode | Trades | Entry Watch | Entry Trigger | 30F B1 | Confidence40 | Confidence70 | Confidence100 | Future Leakage |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
        trade_analysis_lines = [
            "# Trade Analysis Daily Setup Modes",
            "",
        ]
        for row in summary_rows:
            trade_report_lines.append(
                f"| `{row['daily_setup_mode']}` | {row['trades']} | {row['entry_watch_count']} | {row['entry_trigger_count']} | "
                f"{row['thirty_f_b1_count']} | {row['entry_confidence_40']} | {row['entry_confidence_70']} | "
                f"{row['entry_confidence_100']} | `{row['future_leakage_detected']}` |"
            )
            trade_analysis_lines.append(f"## `{row['daily_setup_mode']}`")
            trade_analysis_lines.append("")
            trade_analysis_lines.append(f"- top_failure_gates samples: `{row['top_failure_gates']}`")
            trade_analysis_lines.append("")
        (output_dir / "backtest_report_daily_setup_modes.md").write_text("\n".join(trade_report_lines) + "\n", encoding="utf-8")
        (output_dir / "trade_analysis_daily_setup_modes.md").write_text("\n".join(trade_analysis_lines) + "\n", encoding="utf-8")

        trace_plan = build_trace_plan(daily_audit["rows"])
        trace_rows = await materialize_traces(
            output_dir=output_dir,
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            trace_plan=trace_plan,
        )
        with (output_dir / "weekly_context_daily_trace_samples.jsonl").open("w", encoding="utf-8") as handle:
            for row in trace_rows:
                handle.write(__import__("json").dumps(row, ensure_ascii=False) + "\n")

        bars_by_symbol = await preload_symbol_bars(
            kline_repo=kline_repo,
            symbols=symbols,
            levels=("5f", "30f", "1d", "1w", "1m"),
            warmup_start=start_time,
            end_time=end_time,
        )
        research_dry_run = build_backfill_dry_run(
            symbols=symbols,
            bars_by_symbol=bars_by_symbol,
            profile="research_daily_close",
            warmup_start=start_time,
            backtest_start=start_time,
            end_time=end_time,
            levels=("5f", "30f", "1d", "1w", "1m"),
            mode="predictive",
        )
        strategy_30f_dry_run = build_backfill_dry_run(
            symbols=symbols,
            bars_by_symbol=bars_by_symbol,
            profile="strategy_30f",
            warmup_start=start_time,
            backtest_start=start_time,
            end_time=end_time,
            levels=("5f", "30f", "1d", "1w", "1m"),
            mode="predictive",
        )
        performance_profile = build_backfill_performance_profile(
            backfill_summary=phase_1_7_inputs["backfill_summary"],
            backfill_perf=phase_1_7_inputs["backfill_perf"],
            research_dry_run=research_dry_run,
            strategy_30f_dry_run=strategy_30f_dry_run,
        )
        write_json(output_dir / "backfill_performance_profile.json", performance_profile)
        (output_dir / "backfill_performance_profile.md").write_text(
            render_backfill_performance_profile_markdown(performance_profile),
            encoding="utf-8",
        )
        performance_estimate = build_performance_scale_estimate_after_optimization(
            performance_profile=performance_profile
        )
        write_json(output_dir / "performance_scale_estimate_after_optimization.json", performance_estimate)
        (output_dir / "performance_scale_estimate_after_optimization.md").write_text(
            render_performance_scale_estimate_after_optimization_markdown(performance_estimate),
            encoding="utf-8",
        )

        outputs = [
            "phase_1_8_summary.md",
            "phase_1_8_task_checklist_report.md",
            "daily_signal_distribution_after_10_symbols.md/json",
            "daily_setup_audit.md/json",
            "daily_setup_compare.md/json",
            "daily_setup_failure_samples.jsonl",
            "weekly_context_daily_trace_samples.jsonl",
            "gate_waterfall_daily_modes.md/json",
            "backfill_performance_profile.md/json",
            "performance_scale_estimate_after_optimization.md/json",
            "replay_after_daily_setup_audit.md/json",
            "backtest_report_daily_setup_modes.md",
            "trade_analysis_daily_setup_modes.md",
        ]
        (output_dir / "phase_1_8_summary.md").write_text(
            render_phase_1_8_summary_markdown(
                phase_1_7_inputs=phase_1_7_inputs,
                daily_distribution=daily_distribution,
                daily_audit=daily_audit,
                compare_payload=compare_payload,
                performance_estimate=performance_estimate,
                trace_rows=trace_rows,
            ),
            encoding="utf-8",
        )
        (output_dir / "phase_1_8_task_checklist_report.md").write_text(
            render_phase_1_8_task_checklist_report(
                daily_distribution=daily_distribution,
                daily_audit=daily_audit,
                compare_payload=compare_payload,
                trace_rows=trace_rows,
                performance_estimate=performance_estimate,
                outputs=outputs,
            ),
            encoding="utf-8",
        )
    finally:
        await pool.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.8 daily setup audit.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbols", default=",".join(DEFAULT_PHASE_1_7_SYMBOLS))
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
