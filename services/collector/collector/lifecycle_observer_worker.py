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
    parser.add_argument(
        "--max-attempts", type=int,
        default=int(os.getenv("LIFECYCLE_MAX_ATTEMPTS", "5")),
    )
    parser.add_argument(
        "--retry-delay-seconds", type=int,
        default=int(os.getenv("LIFECYCLE_RETRY_DELAY_SECONDS", "30")),
    )
    parser.add_argument("--loop", action="store_true", default=os.getenv("LIFECYCLE_LOOP") == "1")
    parser.add_argument(
        "--poll-interval", type=float,
        default=float(os.getenv("LIFECYCLE_POLL_INTERVAL", "5")),
    )
    parser.add_argument(
        "--rebuild-current", action="store_true",
        help="Explicitly rebuild the disposable projection after consumption",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    observer = LifecycleObserver(observer_name=args.observer_name)
    connection = await asyncpg.connect(args.database_url)
    processed = 0
    try:
        while args.max_items <= 0 or processed < args.max_items:
            consumed = await observer.process_next(
                connection,
                lease_seconds=args.lease_seconds,
                max_attempts=args.max_attempts,
                retry_delay_seconds=args.retry_delay_seconds,
            )
            if consumed:
                processed += consumed
                continue
            if not args.loop:
                break
            await asyncio.sleep(max(0.1, args.poll_interval))
        if args.rebuild_current:
            await observer.rebuild_current_projection(connection)
        status_rows = await connection.fetch(
            "select status, count(*)::bigint as count from chan_c_head_outbox group by status order by status"
        )
        statuses = {str(row["status"]): int(row["count"]) for row in status_rows}
    finally:
        await connection.close()
    print(json.dumps(
        {"processed": processed, "observer_name": args.observer_name, "statuses": statuses},
        sort_keys=True,
    ))


if __name__ == "__main__":
    asyncio.run(main())
