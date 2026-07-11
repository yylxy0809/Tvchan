from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.weekly_context_audit import build_weekly_context_audit, write_weekly_context_audit


async def _run(args) -> int:
    pool = await create_pool()
    try:
        payload = await build_weekly_context_audit(pool, as_of_time=datetime.fromisoformat(args.as_of))
        write_weekly_context_audit(Path(args.output_dir), payload)
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", "--output-dir", dest="output_dir", default="outputs/weekly-context-audit")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
