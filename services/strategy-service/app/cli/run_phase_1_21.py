from __future__ import annotations

import argparse
from app.engine.time_utils import utc_time
from pathlib import Path

from app.engine.phase_1_21 import DEFAULT_OUTPUT_DIR, run_phase_1_21_sync


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 1.21 read-only intraday grid and signal lifecycle audit.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--reconstruction-start", type=utc_time)
    parser.add_argument("--reconstruction-end", type=utc_time)
    args = parser.parse_args()
    run_phase_1_21_sync(output_dir=args.output_dir, reconstruction_start=args.reconstruction_start, reconstruction_end=args.reconstruction_end)


if __name__ == "__main__":
    main()
