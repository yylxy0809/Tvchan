from __future__ import annotations

import argparse
from pathlib import Path

from app.engine.phase_1_18 import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PHASE_1_11_OUTPUT_DIR,
    DEFAULT_PHASE_1_12_OUTPUT_DIR,
    DEFAULT_PHASE_1_16_OUTPUT_DIR,
    DEFAULT_PHASE_1_17_OUTPUT_DIR,
    run_phase_1_18,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 1.18 staleness policy and candidate universe diagnostics.")
    parser.add_argument("--task", default="all")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--input-phase-1-11", default=str(DEFAULT_PHASE_1_11_OUTPUT_DIR))
    parser.add_argument("--input-phase-1-12", default=str(DEFAULT_PHASE_1_12_OUTPUT_DIR))
    parser.add_argument("--input-phase-1-16", default=str(DEFAULT_PHASE_1_16_OUTPUT_DIR))
    parser.add_argument("--input-phase-1-17", default=str(DEFAULT_PHASE_1_17_OUTPUT_DIR))
    args = parser.parse_args()
    run_phase_1_18(
        task=args.task,
        output_dir=Path(args.output_dir),
        phase_1_11_output_dir=Path(args.input_phase_1_11),
        phase_1_12_output_dir=Path(args.input_phase_1_12),
        phase_1_16_output_dir=Path(args.input_phase_1_16),
        phase_1_17_output_dir=Path(args.input_phase_1_17),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
