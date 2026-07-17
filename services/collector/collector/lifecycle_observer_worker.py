from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import asyncpg

from collector.lifecycle_observer import LifecycleObserver


TRY_RUNTIME_LOCK_SQL = "select pg_try_advisory_lock(hashtext($1))"
UNLOCK_RUNTIME_SQL = "select pg_advisory_unlock(hashtext($1))"


class DuplicateLifecycleObserver(RuntimeError):
    pass


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Module C lifecycle publication outbox")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument(
        "--observer-name",
        default=os.getenv("CHAN_LIFECYCLE_OBSERVER", "chan-lifecycle-v1"),
    )
    parser.add_argument("--max-items", type=int, default=0, help="0 consumes until the outbox is empty")
    parser.add_argument(
        "--lease-seconds", type=int,
        default=os.getenv("LIFECYCLE_LEASE_SECONDS", "300"),
    )
    parser.add_argument(
        "--max-attempts", type=int,
        default=os.getenv("LIFECYCLE_MAX_ATTEMPTS", "5"),
    )
    parser.add_argument(
        "--retry-delay-seconds", type=int,
        default=os.getenv("LIFECYCLE_RETRY_DELAY_SECONDS", "30"),
    )
    loop_group = parser.add_mutually_exclusive_group()
    loop_group.add_argument(
        "--loop", action="store_true", dest="loop",
        default=os.getenv("LIFECYCLE_LOOP") == "1",
    )
    loop_group.add_argument("--once", action="store_false", dest="loop", help=argparse.SUPPRESS)
    parser.add_argument(
        "--poll-interval", "--poll-seconds", type=float,
        default=os.getenv("LIFECYCLE_POLL_INTERVAL", "5"),
    )
    parser.add_argument("--batch-size", type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument(
        "--rebuild-current", action="store_true",
        help="Explicitly rebuild the disposable projection after consumption",
    )
    args = parser.parse_args(argv)
    if not str(args.database_url or "").strip():
        parser.error("DATABASE_URL or --database-url is required")
    if not str(args.observer_name or "").strip():
        parser.error("CHAN_LIFECYCLE_OBSERVER or --observer-name must not be empty")
    if args.max_items < 0:
        parser.error("--max-items must be zero or greater")
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")
    if args.lease_seconds <= 0:
        parser.error("--lease-seconds must be greater than zero")
    if args.max_attempts <= 0:
        parser.error("--max-attempts must be greater than zero")
    if args.retry_delay_seconds <= 0:
        parser.error("--retry-delay-seconds must be greater than zero")
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be greater than zero")
    args.database_url = str(args.database_url).strip()
    args.observer_name = str(args.observer_name).strip()
    return args


async def _wait_for_stop(stop_event: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except TimeoutError:
        pass


async def run_observer(
    args: argparse.Namespace,
    *,
    stop_event: asyncio.Event | None = None,
    connect: Callable[[str], Awaitable[Any]] = asyncpg.connect,
    observer_factory: Callable[..., LifecycleObserver] = LifecycleObserver,
    emit_status: bool = True,
) -> int:
    stop_event = stop_event or asyncio.Event()
    observer = observer_factory(observer_name=args.observer_name)
    connection = await connect(args.database_url)
    runtime_lock_name = f"{args.observer_name}:runtime"
    lock_acquired = False
    processed = 0
    statuses: dict[str, int] = {}
    try:
        lock_acquired = bool(await connection.fetchval(TRY_RUNTIME_LOCK_SQL, runtime_lock_name))
        if not lock_acquired:
            raise DuplicateLifecycleObserver(
                f"lifecycle observer {args.observer_name!r} is already running"
            )
        while not stop_event.is_set() and (args.max_items <= 0 or processed < args.max_items):
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
            await _wait_for_stop(stop_event, args.poll_interval)
        if args.rebuild_current:
            await observer.rebuild_current_projection(connection)
        status_rows = await connection.fetch(
            "select status, count(*)::bigint as count from chan_c_head_outbox group by status order by status"
        )
        statuses = {str(row["status"]): int(row["count"]) for row in status_rows}
    finally:
        try:
            if lock_acquired:
                await connection.fetchval(UNLOCK_RUNTIME_SQL, runtime_lock_name)
        finally:
            await connection.close()
    if emit_status:
        print(json.dumps(
            {"processed": processed, "observer_name": args.observer_name, "statuses": statuses},
            sort_keys=True,
        ))
    return processed


def _install_signal_handlers(stop_event: asyncio.Event) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    previous: dict[signal.Signals, Any] = {}

    def request_stop(_signum: int, _frame: object) -> None:
        loop.call_soon_threadsafe(stop_event.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)

    def restore() -> None:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    return restore


async def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    stop_event = asyncio.Event()
    restore_signals = _install_signal_handlers(stop_event)
    try:
        try:
            await run_observer(args, stop_event=stop_event)
        except DuplicateLifecycleObserver as exc:
            print(str(exc), file=sys.stderr)
            return 2
        return 0
    finally:
        restore_signals()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
