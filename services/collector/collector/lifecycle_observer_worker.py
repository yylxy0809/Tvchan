from __future__ import annotations

import argparse
import asyncio
import json
import os

import asyncpg

from collector.lifecycle_observer import LifecycleObserver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Module C lifecycle publication outbox")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"), required=os.getenv("DATABASE_URL") is None)
    parser.add_argument("--observer-name", default="chan-lifecycle-v1")
    parser.add_argument("--max-items", type=int, default=0, help="0 consumes until the outbox is empty")
    parser.add_argument("--lease-seconds", type=int, default=300)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    observer = LifecycleObserver(observer_name=args.observer_name)
    connection = await asyncpg.connect(args.database_url)
    processed = 0
    try:
        while args.max_items <= 0 or processed < args.max_items:
            if not await observer.process_next(connection, lease_seconds=args.lease_seconds):
                break
            processed += 1
        await observer.rebuild_current_projection(connection)
    finally:
        await connection.close()
    print(json.dumps({"processed": processed, "observer_name": args.observer_name}, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
