from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.engine.phase_1_16 import DEFAULT_OUTPUT_DIR as PHASE_1_16_OUTPUT_DIR
from app.engine.phase_1_17 import DEFAULT_OUTPUT_DIR, DEFAULT_RUN_GROUP_ID, run_phase_1_17


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.17 Entry Trigger window diagnostics and Micro-backfill V2.")
    parser.add_argument("--task", default="all")
    parser.add_argument("--input-dir", "--phase-1-16-output-dir", dest="phase_1_16_output_dir", default=str(PHASE_1_16_OUTPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-group-id", default=DEFAULT_RUN_GROUP_ID)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args()
    asyncio.run(
        run_phase_1_17(
            task=args.task,
            output_dir=Path(args.output_dir),
            phase_1_16_output_dir=Path(args.phase_1_16_output_dir),
            run_group_id=args.run_group_id,
            max_workers=args.max_workers,
            resume=not args.no_resume,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
