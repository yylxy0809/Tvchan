from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.weekly_signal_distribution import (
    build_weekly_signal_distribution,
    render_weekly_signal_distribution_markdown,
)


async def _run(args) -> int:
    pool = await create_pool()
    try:
        report = await build_weekly_signal_distribution(pool, as_of_time=datetime.fromisoformat(args.as_of))
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "weekly_signal_distribution.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "weekly_signal_distribution.md").write_text(
            render_weekly_signal_distribution_markdown(report),
            encoding="utf-8",
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", "--output-dir", dest="output_dir", default="outputs/weekly-signal-distribution")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
