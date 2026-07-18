from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import uuid
from datetime import datetime
from typing import Any

from collector.market_fill import (
    DEFAULT_TIMEFRAMES,
    create_provider,
    parse_timeframes,
    select_symbols,
)
from collector.storage.backfill_postgres import PostgresBackfillTaskStore
from collector.storage.postgres import LostBackfillLease, PostgresKlineWriter
from trading_protocol import Bar
from trading_protocol.timeframes import TIMEFRAMES

DB_TO_TIMEFRAME = {value.minutes: code for code, value in TIMEFRAMES.items()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recoverable historical K-line backfill worker")
    parser.add_argument("--provider", default=os.getenv("HISTORY_BACKFILL_PROVIDER", "pytdx"), choices=["seed", "pytdx"])
    parser.add_argument("--symbols", default=os.getenv("HISTORY_BACKFILL_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_SYMBOL_LIMIT", "1")),
        help="Maximum symbols when --symbols is omitted. Use 0 for all provider symbols.",
    )
    parser.add_argument("--timeframes", default=os.getenv("HISTORY_BACKFILL_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("HISTORY_BACKFILL_PAGE_SIZE", "800")))
    parser.add_argument("--task-limit", type=int, default=int(os.getenv("HISTORY_BACKFILL_TASK_LIMIT", "3")))
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_LEASE_SECONDS", "300")),
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_MAX_ATTEMPTS", "5")),
    )
    parser.add_argument(
        "--worker-id",
        default=os.getenv("HISTORY_BACKFILL_WORKER_ID"),
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_CONCURRENCY", "1")),
        help="Maximum claimed backfill tasks to process concurrently.",
    )
    parser.add_argument(
        "--max-pages-per-task",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_MAX_PAGES_PER_TASK", "1")),
        help="Pages to fetch for each claimed task. Use 0 to continue until the provider is exhausted.",
    )
    parser.add_argument("--sleep", type=float, default=float(os.getenv("HISTORY_BACKFILL_SLEEP", "0.25")))
    parser.add_argument("--loop", action="store_true", default=os.getenv("HISTORY_BACKFILL_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("HISTORY_BACKFILL_LOOP_INTERVAL", "30")),
    )
    parser.add_argument("--reset", action="store_true", default=os.getenv("HISTORY_BACKFILL_RESET") == "1")
    parser.add_argument(
        "--reset-running",
        action="store_true",
        default=os.getenv("HISTORY_BACKFILL_RESET_RUNNING") == "1",
    )
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("HISTORY_BACKFILL_DRY_RUN") == "1")
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
    parser.add_argument("--source-policy", default=os.getenv("HISTORY_BACKFILL_SOURCE_POLICY", "primary_failover"))
    parser.add_argument("--http-timeout", type=float, default=float(os.getenv("HISTORY_BACKFILL_HTTP_TIMEOUT", "5")))
    parser.add_argument("--pool-timeout", type=float, default=float(os.getenv("HISTORY_BACKFILL_POOL_TIMEOUT", "8")))
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    args = parser.parse_args()
    if args.lease_seconds <= 0:
        parser.error("--lease-seconds must be greater than zero")
    if args.max_attempts <= 0:
        parser.error("--max-attempts must be greater than zero")
    args.worker_id = str(
        args.worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    ).strip()
    if not args.worker_id or len(args.worker_id) > 160:
        parser.error("--worker-id must contain between 1 and 160 characters")
    return args


async def main() -> None:
    args = parse_args()
    while True:
        await run_once(args)
        if not args.loop:
            return
        await asyncio.sleep(args.loop_interval)


async def run_once(args: argparse.Namespace) -> None:
    provider = create_provider(args)
    timeframes = parse_timeframes(args.timeframes)
    symbols = await select_symbols(provider, args.symbols, args.symbol_limit)
    emit(
        "history_pass_started",
        provider=provider.name,
        symbols=len(symbols),
        timeframes=timeframes,
        page_size=args.page_size,
        task_limit=args.task_limit,
        concurrency=max(1, args.concurrency),
        max_pages_per_task=args.max_pages_per_task,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        for symbol in symbols:
            for timeframe in timeframes:
                emit("history_dry_task", symbol=symbol.symbol, timeframe=timeframe)
        emit("history_pass_finished", tasks=0, pages=0, bars=0)
        return

    async with PostgresKlineWriter(args.database_url) as kline_writer:
        async with PostgresBackfillTaskStore(args.database_url) as task_store:
            await kline_writer.upsert_symbols(symbols)
            ensured = await task_store.ensure_tasks(
                symbols=symbols,
                timeframes=timeframes,
                provider=provider.name,
                page_size=args.page_size,
                reset=args.reset,
            )
            reset_count = 0
            if args.reset_running:
                reset_count = await task_store.reset_running(provider=provider.name)

            tasks = await task_store.claim_tasks(
                provider=provider.name,
                limit=args.task_limit,
                worker_id=args.worker_id,
                lease_seconds=args.lease_seconds,
                max_attempts=args.max_attempts,
            )
            emit(
                "history_tasks_claimed",
                ensured=ensured,
                reset_running=reset_count,
                tasks=len(tasks),
                concurrency=max(1, args.concurrency),
            )

            result = await process_tasks_concurrently(
                provider_factory=lambda: create_provider(args),
                kline_writer=kline_writer,
                task_store=task_store,
                tasks=tasks,
                concurrency=max(1, args.concurrency),
                max_pages_per_task=args.max_pages_per_task,
                sleep=args.sleep,
                lease_seconds=args.lease_seconds,
            )
            emit(
                "history_pass_finished",
                tasks=len(tasks),
                pages=result["pages"],
                bars=result["bars"],
            )


async def process_tasks_concurrently(
    *,
    provider_factory,
    kline_writer: PostgresKlineWriter,
    task_store: PostgresBackfillTaskStore,
    tasks: list[dict[str, Any]],
    concurrency: int,
    max_pages_per_task: int,
    sleep: float,
    lease_seconds: int,
) -> dict[str, int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_task(task: dict[str, Any]) -> dict[str, int]:
        async with semaphore:
            return await process_task(
                provider=provider_factory(),
                kline_writer=kline_writer,
                task_store=task_store,
                task=task,
                max_pages_per_task=max_pages_per_task,
                sleep=sleep,
                lease_seconds=lease_seconds,
            )

    results = await asyncio.gather(*(run_task(task) for task in tasks))
    return {
        "pages": sum(item["pages"] for item in results),
        "bars": sum(item["bars"] for item in results),
    }


async def process_task(
    *,
    provider,
    kline_writer: PostgresKlineWriter,
    task_store: PostgresBackfillTaskStore,
    task: dict[str, Any],
    max_pages_per_task: int,
    sleep: float,
    lease_seconds: int,
) -> dict[str, int]:
    symbol = str(task["symbol"])
    timeframe = DB_TO_TIMEFRAME[int(task["timeframe"])]
    page_size = int(task["page_size"])
    offset = int(task["next_offset"])
    pages = 0
    total_bars = 0
    exhausted = False
    lease_lost = asyncio.Event()
    stop_heartbeat = asyncio.Event()

    async def maintain_lease() -> None:
        interval = max(0.1, lease_seconds / 3)
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=interval)
                return
            except TimeoutError:
                try:
                    renewed = await task_store.heartbeat(
                        task_id=int(task["id"]),
                        claim_token=str(task["claim_token"]),
                        lease_version=int(task["lease_version"]),
                        lease_seconds=lease_seconds,
                    )
                except Exception:
                    renewed = False
                if not renewed:
                    lease_lost.set()
                    return

    heartbeat_task = asyncio.create_task(maintain_lease())

    try:
        while max_pages_per_task <= 0 or pages < max_pages_per_task:
            if lease_lost.is_set():
                raise LostBackfillLease(f"historical backfill task lease lost: {task['id']}")
            bars, raw_rows_read = await get_provider_page_with_raw_count(
                provider,
                symbol=symbol,
                timeframe=timeframe,
                offset=offset,
                limit=page_size,
            )
            if lease_lost.is_set():
                raise LostBackfillLease(f"historical backfill task lease lost: {task['id']}")
            bars_read = len(bars)
            exhausted = raw_rows_read < page_size
            next_offset = offset + raw_rows_read
            oldest_ts = min((bar.ts for bar in bars), default=None)
            newest_ts = max((bar.ts for bar in bars), default=None)
            bars_written = await kline_writer.commit_history_backfill_page(
                task=task,
                expected_offset=offset,
                next_offset=next_offset,
                bars=bars,
                oldest_ts=oldest_ts,
                newest_ts=newest_ts,
                exhausted=exhausted,
                lease_seconds=lease_seconds,
            )
            emit(
                "history_page_written",
                symbol=symbol,
                timeframe=timeframe,
                offset=offset,
                next_offset=next_offset,
                bars_read=bars_read,
                raw_rows_read=raw_rows_read,
                bars_written=bars_written,
                exhausted=exhausted,
            )
            pages += 1
            total_bars += bars_written
            offset = next_offset
            if exhausted:
                break
            await sleep_between_requests(sleep)

        if not exhausted:
            stop_heartbeat.set()
            await heartbeat_task
            if not await task_store.yield_task(
                task_id=int(task["id"]),
                claim_token=str(task["claim_token"]),
                lease_version=int(task["lease_version"]),
            ):
                raise LostBackfillLease(
                    f"historical backfill task lease lost before yield: {task['id']}"
                )

    except LostBackfillLease as exc:
        emit(
            "history_task_lease_lost",
            symbol=symbol,
            timeframe=timeframe,
            offset=offset,
            error=str(exc)[:500],
        )
    except Exception as exc:
        recorded = await task_store.record_failure(
            task_id=int(task["id"]),
            claim_token=str(task["claim_token"]),
            lease_version=int(task["lease_version"]),
            error=str(exc),
        )
        emit(
            "history_task_failed" if recorded else "history_task_lease_lost",
            symbol=symbol,
            timeframe=timeframe,
            offset=offset,
            error=str(exc)[:500],
        )
    finally:
        stop_heartbeat.set()
        if not heartbeat_task.done():
            await heartbeat_task
    return {"pages": pages, "bars": total_bars}


async def get_provider_page(
    provider,
    *,
    symbol: str,
    timeframe: str,
    offset: int,
    limit: int,
) -> list[Bar]:
    bars, _raw_count = await get_provider_page_with_raw_count(
        provider,
        symbol=symbol,
        timeframe=timeframe,
        offset=offset,
        limit=limit,
    )
    return bars


async def get_provider_page_with_raw_count(
    provider,
    *,
    symbol: str,
    timeframe: str,
    offset: int,
    limit: int,
) -> tuple[list[Bar], int]:
    if hasattr(provider, "get_bars_page_with_raw_count"):
        return await provider.get_bars_page_with_raw_count(
            symbol,
            timeframe,
            offset=offset,
            limit=limit,
        )
    if hasattr(provider, "get_bars_page"):
        bars = await provider.get_bars_page(
            symbol,
            timeframe,
            offset=offset,
            limit=limit,
        )
        return bars, len(bars)
    bars = await provider.get_bars(symbol, timeframe, limit=offset + limit)
    page = bars[offset : offset + limit]
    return page, len(page)


async def sleep_between_requests(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
