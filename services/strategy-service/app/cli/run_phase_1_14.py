from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.db import create_pool
from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_13 import DEFAULT_OUTPUT_DIR as PHASE_1_13_OUTPUT_DIR
from app.engine.phase_1_14 import DEFAULT_OUTPUT_DIR, run_phase_1_14
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS, write_json
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pool = await create_pool(max_size=max(8, args.max_workers + 4))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        requested_symbols = args.symbols.split(",") if args.symbols else DEFAULT_PHASE_1_7_SYMBOLS
        summary = await run_phase_1_14(
            pool=pool,
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            output_dir=output_dir,
            phase_1_12_output_dir=Path(args.phase_1_12_output_dir),
            phase_1_13_output_dir=Path(args.phase_1_13_output_dir),
            symbols=requested_symbols,
        )
        write_json(output_dir / "phase_1_14_run_summary.json", summary)
    finally:
        await pool.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.14 30F price validity and daily bottom fractal diagnostics.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--phase-1-12-output-dir", default=str(PHASE_1_12_OUTPUT_DIR))
    parser.add_argument("--phase-1-13-output-dir", default=str(PHASE_1_13_OUTPUT_DIR))
    parser.add_argument("--symbols", default=",".join(DEFAULT_PHASE_1_7_SYMBOLS))
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
