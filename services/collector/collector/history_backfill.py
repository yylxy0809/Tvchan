from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from typing import Any

from collector.market_fill import (
    DEFAULT_CHAN_LEVELS,
    DEFAULT_MODES,
    DEFAULT_TIMEFRAMES,
    analyze_chan,
    create_provider,
    filter_chan_response_level,
    parse_csv,
    parse_timeframes,
    select_symbols,
)
from collector.storage.backfill_postgres import PostgresBackfillTaskStore
from collector.storage.chan_postgres import PostgresChanWriter
from collector.storage.postgres import PostgresKlineWriter
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
    parser.add_argument(
        "--recompute-chan-on-success",
        action="store_true",
        default=os.getenv("HISTORY_BACKFILL_RECOMPUTE_CHAN_ON_SUCCESS") == "1",
    )
    parser.add_argument("--chan-levels", default=os.getenv("HISTORY_BACKFILL_CHAN_LEVELS", DEFAULT_CHAN_LEVELS))
    parser.add_argument("--modes", default=os.getenv("HISTORY_BACKFILL_MODES", DEFAULT_MODES))
    parser.add_argument("--chan-service-url", default=os.getenv("CHAN_SERVICE_URL", "http://127.0.0.1:8002"))
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    return parser.parse_args()


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
    chan_levels = parse_timeframes(args.chan_levels)
    modes = parse_csv(args.modes)
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

            tasks = await task_store.claim_tasks(provider=provider.name, limit=args.task_limit)
            emit(
                "history_tasks_claimed",
                ensured=ensured,
                reset_running=reset_count,
                tasks=len(tasks),
                concurrency=max(1, args.concurrency),
            )

            chan_writer = None
            if args.recompute_chan_on_success:
                chan_writer = PostgresChanWriter(args.database_url)
                await chan_writer.__aenter__()
            try:
                result = await process_tasks_concurrently(
                    provider_factory=lambda: create_provider(args),
                    kline_writer=kline_writer,
                    task_store=task_store,
                    chan_writer=chan_writer,
                    tasks=tasks,
                    concurrency=max(1, args.concurrency),
                    max_pages_per_task=args.max_pages_per_task,
                    sleep=args.sleep,
                    chan_levels=chan_levels,
                    modes=modes,
                    chan_service_url=args.chan_service_url,
                )
                emit(
                    "history_pass_finished",
                    tasks=len(tasks),
                    pages=result["pages"],
                    bars=result["bars"],
                )
            finally:
                if chan_writer is not None:
                    await chan_writer.__aexit__(None, None, None)


async def process_tasks_concurrently(
    *,
    provider_factory,
    kline_writer: PostgresKlineWriter,
    task_store: PostgresBackfillTaskStore,
    chan_writer: PostgresChanWriter | None,
    tasks: list[dict[str, Any]],
    concurrency: int,
    max_pages_per_task: int,
    sleep: float,
    chan_levels: list[str],
    modes: list[str],
    chan_service_url: str,
) -> dict[str, int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_task(task: dict[str, Any]) -> dict[str, int]:
        async with semaphore:
            return await process_task(
                provider=provider_factory(),
                kline_writer=kline_writer,
                task_store=task_store,
                chan_writer=chan_writer,
                task=task,
                max_pages_per_task=max_pages_per_task,
                sleep=sleep,
                chan_levels=chan_levels,
                modes=modes,
                chan_service_url=chan_service_url,
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
    chan_writer: PostgresChanWriter | None,
    task: dict[str, Any],
    max_pages_per_task: int,
    sleep: float,
    chan_levels: list[str],
    modes: list[str],
    chan_service_url: str,
) -> dict[str, int]:
    symbol = str(task["symbol"])
    timeframe = DB_TO_TIMEFRAME[int(task["timeframe"])]
    page_size = int(task["page_size"])
    offset = int(task["next_offset"])
    pages = 0
    total_bars = 0
    exhausted = False

    try:
        while max_pages_per_task <= 0 or pages < max_pages_per_task:
            bars = await get_provider_page(
                provider,
                symbol=symbol,
                timeframe=timeframe,
                offset=offset,
                limit=page_size,
            )
            bars_written = await kline_writer.upsert_bars(bars)
            bars_read = len(bars)
            exhausted = bars_read < page_size
            next_offset = offset + bars_read
            oldest_ts = min((bar.ts for bar in bars), default=None)
            newest_ts = max((bar.ts for bar in bars), default=None)
            await task_store.record_page_success(
                task_id=int(task["id"]),
                next_offset=next_offset,
                bars_read=bars_read,
                bars_written=bars_written,
                oldest_ts=oldest_ts,
                newest_ts=newest_ts,
                exhausted=exhausted,
            )
            emit(
                "history_page_written",
                symbol=symbol,
                timeframe=timeframe,
                offset=offset,
                next_offset=next_offset,
                bars_read=bars_read,
                bars_written=bars_written,
                exhausted=exhausted,
            )
            pages += 1
            total_bars += bars_written
            offset = next_offset
            if exhausted:
                break
            await sleep_between_requests(sleep)

        if exhausted and chan_writer is not None and timeframe == "5f":
            await recompute_chan(
                kline_writer=kline_writer,
                chan_writer=chan_writer,
                chan_service_url=chan_service_url,
                symbol=symbol,
                base_timeframe=timeframe,
                levels=chan_levels,
                modes=modes,
            )
    except Exception as exc:
        await task_store.record_failure(task_id=int(task["id"]), error=str(exc))
        emit(
            "history_task_failed",
            symbol=symbol,
            timeframe=timeframe,
            offset=offset,
            error=str(exc)[:500],
        )
    return {"pages": pages, "bars": total_bars}


async def get_provider_page(
    provider,
    *,
    symbol: str,
    timeframe: str,
    offset: int,
    limit: int,
) -> list[Bar]:
    if hasattr(provider, "get_bars_page"):
        return await provider.get_bars_page(
            symbol,
            timeframe,
            offset=offset,
            limit=limit,
        )
    bars = await provider.get_bars(symbol, timeframe, limit=offset + limit)
    return bars[offset : offset + limit]


async def recompute_chan(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    chan_service_url: str,
    symbol: str,
    base_timeframe: str,
    levels: list[str],
    modes: list[str],
) -> None:
    bars = await kline_writer.get_bars(symbol, base_timeframe)
    if not bars:
        return
    response = await analyze_chan(
        chan_service_url,
        symbol,
        base_timeframe,
        modes,
        bars,
        chan_levels=levels,
    )
    for level in levels:
        counts = await chan_writer.replace_analysis(
            symbol=symbol,
            level=level,
            modes=modes,
            bar_from=bars[0].ts,
            bar_until=bars[-1].ts,
            bar_count=len(bars),
            response=filter_chan_response_level(response, level),
        )
        emit(
            "history_chan_recomputed",
            symbol=symbol,
            base_timeframe=base_timeframe,
            level=level,
            engine=response.get("engine"),
            input_bars=len(bars),
            **counts,
        )


async def sleep_between_requests(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
