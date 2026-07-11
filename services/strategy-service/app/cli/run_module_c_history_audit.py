from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.module_c_history_audit import (
    DEFAULT_LEVELS,
    build_effective_backtest_window,
    build_historical_run_lookup_audit,
    build_module_c_history_coverage,
    render_effective_backtest_window_markdown,
    render_historical_run_lookup_audit_markdown,
    render_module_c_history_coverage_markdown,
    write_module_c_history_coverage_csv,
)
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args) -> int:
    levels = tuple(item.strip() for item in args.levels.split(",") if item.strip())
    pool = await create_pool()
    try:
        module_c_repo = ModuleCRepository(pool)
        coverage = await build_module_c_history_coverage(
            pool,
            start_time=datetime.fromisoformat(args.start),
            end_time=datetime.fromisoformat(args.end),
            levels=levels or DEFAULT_LEVELS,
            mode=args.mode,
            symbols=list(args.symbols or []),
            limit=args.limit,
        )
        lookup_audit = await build_historical_run_lookup_audit(
            module_c_repo,
            symbols=list(args.lookup_symbols or []),
            as_of_times=[datetime.fromisoformat(value) for value in args.lookup_times] if args.lookup_times else None,
            levels=levels or DEFAULT_LEVELS,
            mode=args.mode,
        )
        effective_window = build_effective_backtest_window(coverage)
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
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", "--from", dest="start", required=True)
    parser.add_argument("--end", "--to", dest="end", required=True)
    parser.add_argument("--levels", default="5f,30f,1d,1w,1m")
    parser.add_argument("--mode", choices=("predictive", "confirmed"), default="predictive")
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--lookup-symbols", nargs="*")
    parser.add_argument("--lookup-times", nargs="*")
    parser.add_argument("--output", "--output-dir", dest="output_dir", default="outputs/module-c-history-audit")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
