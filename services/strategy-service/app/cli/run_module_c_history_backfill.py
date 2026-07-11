from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.db import create_pool
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
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    levels = tuple(item.strip() for item in args.levels.split(",") if item.strip())
    pool = await create_pool(max_size=max(4, args.max_workers + 2))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        symbols = await module_c_repo.list_active_symbols(symbols=args.symbols.split(",") if args.symbols else None)
        warmup_start = datetime.fromisoformat(args.warmup_start)
        backtest_start = datetime.fromisoformat(args.backtest_start)
        end_time = datetime.fromisoformat(args.end)
        bars_by_symbol = await preload_symbol_bars(
            kline_repo=kline_repo,
            symbols=symbols,
            levels=levels,
            warmup_start=warmup_start,
            end_time=end_time,
        )
        dry_run = build_backfill_dry_run(
            symbols=symbols,
            bars_by_symbol=bars_by_symbol,
            profile=args.profile,
            warmup_start=warmup_start,
            backtest_start=backtest_start,
            end_time=end_time,
            levels=levels,
            mode=args.mode,
        )
        (output_dir / "module_c_backfill_dry_run.json").write_text(
            json.dumps(dry_run, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "module_c_backfill_plan.md").write_text(
            render_backfill_plan_markdown(dry_run),
            encoding="utf-8",
        )
        if args.dry_run:
            return 0

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
        (output_dir / "module_c_backfill_summary.json").write_text(
            json.dumps(
                {
                    **summary,
                    "results": [
                        {
                            "symbol": result.symbol,
                            "level": result.level,
                            "cutoff_time": result.cutoff_time.isoformat(),
                            "bar_count": result.bar_count,
                            "run_id": result.run_id,
                            "snapshot_version": result.snapshot_version,
                            "strokes": result.strokes,
                            "segments": result.segments,
                            "centers": result.centers,
                            "signals": result.signals,
                            "elapsed_seconds": result.elapsed_seconds,
                        }
                        for result in summary["results"]
                    ],
                    "failures": [
                        {
                            "symbol": failure.symbol,
                            "level": failure.level,
                            "cutoff_time": failure.cutoff_time.isoformat() if failure.cutoff_time else None,
                            "error": failure.error,
                        }
                        for failure in summary["failures"]
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (output_dir / "module_c_backfill_summary.md").write_text(
            render_backfill_summary_markdown(summary),
            encoding="utf-8",
        )
        (output_dir / "backfill_perf.json").write_text(
            json.dumps(
                {
                    "elapsed_seconds": summary["elapsed_seconds"],
                    "symbol_elapsed_seconds_p50": summary["symbol_elapsed_seconds_p50"],
                    "symbol_elapsed_seconds_p95": summary["symbol_elapsed_seconds_p95"],
                    "written_runs": summary["written_runs"],
                    "failed_runs": summary["failed_runs"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        write_failed_symbols_jsonl(output_dir / "backfill_failed_symbols.jsonl", summary["failures"])
        write_runs_manifest_csv(output_dir / "backfilled_runs_manifest.csv", summary["results"])
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--symbols")
    parser.add_argument("--warmup-start", required=True)
    parser.add_argument("--backtest-start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--mode", choices=("predictive", "confirmed"), default="predictive")
    parser.add_argument("--levels", default=",".join(DEFAULT_LEVELS))
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output", default="services/strategy-service/outputs/phase-1-6/backfill")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
