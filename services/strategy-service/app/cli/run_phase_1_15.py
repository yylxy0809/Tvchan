from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.db import create_pool
from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_13 import DEFAULT_OUTPUT_DIR as PHASE_1_13_OUTPUT_DIR
from app.engine.phase_1_14 import DEFAULT_OUTPUT_DIR as PHASE_1_14_OUTPUT_DIR
from app.engine.phase_1_15 import DEFAULT_OUTPUT_DIR, DEFAULT_TARGETED_RUN_GROUP, run_phase_1_15
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pool = await create_pool(max_size=max(8, args.max_workers + 4))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        requested_symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]
        await run_phase_1_15(
            task=args.task,
            pool=pool,
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            output_dir=output_dir,
            phase_1_12_output_dir=Path(args.phase_1_12_output_dir),
            phase_1_13_output_dir=Path(args.phase_1_13_output_dir),
            phase_1_14_output_dir=Path(args.phase_1_14_output_dir),
            symbols=requested_symbols,
            run_group_id=args.run_group,
            max_workers=args.max_workers,
            resume=not args.no_resume,
        )
    finally:
        await pool.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.15 sample-lineage / price-fractal / 5F root-cause / targeted intraday micro-backfill / V4 replay.")
    parser.add_argument("--task", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--phase-1-12-output-dir", default=str(PHASE_1_12_OUTPUT_DIR))
    parser.add_argument("--phase-1-13-output-dir", default=str(PHASE_1_13_OUTPUT_DIR))
    parser.add_argument("--phase-1-14-output-dir", default=str(PHASE_1_14_OUTPUT_DIR))
    parser.add_argument("--symbols", default="000001.SZ,000651.SZ")
    parser.add_argument("--run-group", default=DEFAULT_TARGETED_RUN_GROUP)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
