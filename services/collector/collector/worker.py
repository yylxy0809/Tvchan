from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

from collector.worker_registry import list_workers, run_worker


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a registered collector worker")
    parser.add_argument("worker", nargs="?", help="Worker name, for example: market-fill")
    parser.add_argument("worker_args", nargs=argparse.REMAINDER)
    parser.add_argument("--list", action="store_true", help="List registered workers and exit")
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list:
        for spec in list_workers():
            print(f"{spec.name}\t{spec.module}\t{spec.description}")
        return 0
    if not args.worker:
        available = ", ".join(spec.name for spec in list_workers())
        raise SystemExit(f"Missing worker name. Available workers: {available}")
    return await run_worker(args.worker, args.worker_args)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
