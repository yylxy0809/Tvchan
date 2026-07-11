from __future__ import annotations

import argparse
from pathlib import Path

from app.engine.phase_1_20r import DEFAULT_OUTPUT_DIR, run_phase_1_20r_sync


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 1.20R database truth reconciliation.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    run_phase_1_20r_sync(output_dir=args.output_dir)


if __name__ == "__main__":
    main()
