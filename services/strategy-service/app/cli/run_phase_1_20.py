from __future__ import annotations

import argparse
from pathlib import Path

from app.engine.phase_1_20 import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PHASE_1_18_OUTPUT_DIR,
    DEFAULT_PHASE_1_19_OUTPUT_DIR,
    run_phase_1_20,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 1.20 30F refresh visibility diagnostics.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--input-phase-1-18", type=Path, default=DEFAULT_PHASE_1_18_OUTPUT_DIR)
    parser.add_argument("--input-phase-1-19", type=Path, default=DEFAULT_PHASE_1_19_OUTPUT_DIR)
    args = parser.parse_args()
    run_phase_1_20(
        output_dir=args.output_dir,
        phase_1_18_output_dir=args.input_phase_1_18,
        phase_1_19_output_dir=args.input_phase_1_19,
    )


if __name__ == "__main__":
    main()
