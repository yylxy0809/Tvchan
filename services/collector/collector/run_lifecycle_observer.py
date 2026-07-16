from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg

from collector.lifecycle_observer import LifecycleObserver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Module C head lifecycle outbox")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--observer-name", default=os.getenv("CHAN_LIFECYCLE_OBSERVER", "chan-lifecycle-v1"))
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


async def run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise ValueError("DATABASE_URL or --database-url is required")
    observer = LifecycleObserver(observer_name=args.observer_name)
    processed = 0
    pool = await asyncpg.create_pool(args.database_url, min_size=1, max_size=max(2, args.batch_size))
    try:
        while True:
            async with pool.acquire() as conn:
                claimed_count = await observer.process_next(
                    conn, lease_seconds=max(1, args.lease_seconds)
                )
                processed += claimed_count
            if args.once:
                return processed
            if not claimed_count:
                await asyncio.sleep(max(0.1, args.poll_seconds))
    finally:
        await pool.close()


def main() -> int:
    return asyncio.run(run(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
