from __future__ import annotations

import argparse
from pathlib import Path

from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_13 import DEFAULT_OUTPUT_DIR as PHASE_1_13_OUTPUT_DIR
from app.engine.phase_1_14 import DEFAULT_OUTPUT_DIR as PHASE_1_14_OUTPUT_DIR
from app.engine.phase_1_15 import DEFAULT_OUTPUT_DIR as PHASE_1_15_OUTPUT_DIR
from app.engine.phase_1_16 import DEFAULT_OUTPUT_DIR, run_phase_1_16


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.16 30F window/price V2 + Entry Trigger V5 diagnostics.")
    parser.add_argument("--task", default="all")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--phase-1-12-output-dir", default=str(PHASE_1_12_OUTPUT_DIR))
    parser.add_argument("--phase-1-13-output-dir", default=str(PHASE_1_13_OUTPUT_DIR))
    parser.add_argument("--phase-1-14-output-dir", default=str(PHASE_1_14_OUTPUT_DIR))
    parser.add_argument("--phase-1-15-output-dir", default=str(PHASE_1_15_OUTPUT_DIR))
    args = parser.parse_args()
    run_phase_1_16(
        task=args.task,
        output_dir=Path(args.output_dir),
        phase_1_12_output_dir=Path(args.phase_1_12_output_dir),
        phase_1_13_output_dir=Path(args.phase_1_13_output_dir),
        phase_1_14_output_dir=Path(args.phase_1_14_output_dir),
        phase_1_15_output_dir=Path(args.phase_1_15_output_dir),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
