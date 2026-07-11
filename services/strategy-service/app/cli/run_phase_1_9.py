from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.db import create_pool
from app.engine.phase_1_9 import DEFAULT_OUTPUT_DIR, run_phase_1_9
from app.engine.phase_1_7 import write_json
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pool = await create_pool(max_size=max(8, args.max_workers + 4))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        summary = await run_phase_1_9(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            output_dir=output_dir,
            max_workers=args.max_workers,
        )
        write_json(output_dir / "phase_1_9_summary.json", summary)
    finally:
        await pool.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.9 daily setup semantics and backfill optimization audit.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
