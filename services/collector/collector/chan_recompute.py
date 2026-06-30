from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
from functools import lru_cache
from pathlib import Path
from datetime import datetime
from typing import Any

from collector.market_fill import (
    DEFAULT_CHAN_LEVELS,
    DEFAULT_MODES,
    bar_to_chan_payload,
    filter_chan_response_level,
    normalize_symbol,
    parse_csv,
    parse_timeframes,
)
from collector.storage.chan_postgres import PostgresChanWriter
from collector.storage.chan_recompute_postgres import PostgresChanRecomputeTaskStore
from collector.storage.postgres import PostgresKlineWriter
from trading_protocol.timeframes import TIMEFRAMES

DB_TO_TIMEFRAME = {value.minutes: code for code, value in TIMEFRAMES.items()}
DEFAULT_BASE_TIMEFRAME = "5f"
DEFAULT_PAGE_SIZE = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queued full-history Chan recompute worker")
    parser.add_argument("--symbols", default=os.getenv("CHAN_RECOMPUTE_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("CHAN_RECOMPUTE_SYMBOL_LIMIT", "10")),
        help="Maximum database symbols when --symbols is omitted. Use 0 for all symbols with bars.",
    )
    parser.add_argument("--chan-levels", default=os.getenv("CHAN_RECOMPUTE_LEVELS", DEFAULT_CHAN_LEVELS))
    parser.add_argument("--base-timeframe", default=os.getenv("CHAN_RECOMPUTE_BASE_TIMEFRAME", DEFAULT_BASE_TIMEFRAME))
    parser.add_argument("--modes", default=os.getenv("CHAN_RECOMPUTE_MODES", DEFAULT_MODES))
    parser.add_argument("--task-limit", type=int, default=int(os.getenv("CHAN_RECOMPUTE_TASK_LIMIT", "3")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("CHAN_RECOMPUTE_CONCURRENCY", "1")))
    parser.add_argument("--sleep", type=float, default=float(os.getenv("CHAN_RECOMPUTE_SLEEP", "0.1")))
    parser.add_argument(
        "--chan-timeout",
        type=float,
        default=float(os.getenv("CHAN_ANALYZE_TIMEOUT", "120")),
        help="Deprecated: retained for backward compatibility.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=int(os.getenv("CHAN_RECOMPUTE_PAGE_SIZE", str(DEFAULT_PAGE_SIZE))),
        help="Number of canonical 5f bars to read per database chunk.",
    )
    parser.add_argument(
        "--db-pool-min-size",
        type=int,
        default=int(os.getenv("CHAN_RECOMPUTE_DB_POOL_MIN_SIZE", "1")),
        help="Minimum asyncpg pool size per recompute worker database pool.",
    )
    parser.add_argument(
        "--db-pool-max-size",
        type=int,
        default=int(os.getenv("CHAN_RECOMPUTE_DB_POOL_MAX_SIZE", "1")),
        help="Maximum asyncpg pool size per recompute worker database pool.",
    )
    parser.add_argument("--chan-py-path", default=os.getenv("CHAN_PY_PATH"))
    parser.add_argument("--loop", action="store_true", default=os.getenv("CHAN_RECOMPUTE_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("CHAN_RECOMPUTE_LOOP_INTERVAL", "30")),
    )
    parser.add_argument(
        "--skip-ensure",
        action="store_true",
        default=os.getenv("CHAN_RECOMPUTE_SKIP_ENSURE") == "1",
        help="Skip symbol discovery and task upsert; only claim existing queued tasks.",
    )
    parser.add_argument("--reset", action="store_true", default=os.getenv("CHAN_RECOMPUTE_RESET") == "1")
    parser.add_argument(
        "--reset-running",
        action="store_true",
        default=os.getenv("CHAN_RECOMPUTE_RESET_RUNNING") == "1",
    )
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("CHAN_RECOMPUTE_DRY_RUN") == "1")
    parser.add_argument("--chan-service-url", default=os.getenv("CHAN_SERVICE_URL", "http://127.0.0.1:8002"))
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
    levels = parse_timeframes(args.chan_levels)
    base_timeframe = parse_timeframes(args.base_timeframe)[0]
    modes = parse_csv(args.modes)
    db_pool_min_size = max(1, args.db_pool_min_size)
    db_pool_max_size = max(db_pool_min_size, args.db_pool_max_size)
    async with PostgresChanRecomputeTaskStore(
        args.database_url,
        pool_min_size=db_pool_min_size,
        pool_max_size=db_pool_max_size,
    ) as task_store:
        symbols = (
            []
            if args.skip_ensure
            else await resolve_symbols(
                task_store=task_store,
                symbols_arg=args.symbols,
                levels=[base_timeframe],
                symbol_limit=args.symbol_limit,
            )
        )
        emit(
            "chan_recompute_pass_started",
            symbols=None if args.skip_ensure else len(symbols),
            base_timeframe=base_timeframe,
            levels=levels,
            modes=modes,
            task_limit=args.task_limit,
            concurrency=max(1, args.concurrency),
            dry_run=args.dry_run,
            skip_ensure=args.skip_ensure,
        )

        if args.dry_run:
            for symbol in symbols:
                emit("chan_recompute_dry_task", symbol=symbol, base_timeframe=base_timeframe, levels=levels)
            emit("chan_recompute_pass_finished", tasks=0, runs=0)
            return

        ensured = (
            0
            if args.skip_ensure
            else await task_store.ensure_tasks(
                symbols=symbols,
                levels=[base_timeframe],
                modes=modes,
                reset=args.reset,
            )
        )
        reset_count = 0
        if args.reset_running:
            reset_count = await task_store.reset_running()
        tasks = await task_store.claim_tasks(limit=args.task_limit)
        emit(
            "chan_recompute_tasks_claimed",
            ensured=ensured,
            reset_running=reset_count,
            tasks=len(tasks),
            concurrency=max(1, args.concurrency),
        )

        async with PostgresKlineWriter(
            args.database_url,
            pool_min_size=db_pool_min_size,
            pool_max_size=db_pool_max_size,
        ) as kline_writer:
            async with PostgresChanWriter(
                args.database_url,
                pool_min_size=db_pool_min_size,
                pool_max_size=db_pool_max_size,
            ) as chan_writer:
                result = await process_tasks_concurrently(
                    kline_writer=kline_writer,
                    chan_writer=chan_writer,
                    task_store=task_store,
                    tasks=tasks,
                    analysis_levels=levels,
                    concurrency=max(1, args.concurrency),
                    sleep=args.sleep,
                    chan_py_path=args.chan_py_path,
                    page_size=max(1, args.page_size),
                )
        emit(
            "chan_recompute_pass_finished",
            tasks=len(tasks),
            runs=result["runs"],
        )


async def resolve_symbols(
    *,
    task_store: PostgresChanRecomputeTaskStore,
    symbols_arg: str | None,
    levels: list[str],
    symbol_limit: int,
) -> list[str]:
    if symbols_arg:
        return sorted({normalize_symbol(value) for value in parse_csv(symbols_arg)})
    return await task_store.list_symbols_with_bars(levels=levels, limit=symbol_limit)


async def process_tasks_concurrently(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    task_store: PostgresChanRecomputeTaskStore,
    tasks: list[dict[str, Any]],
    analysis_levels: list[str],
    concurrency: int,
    sleep: float,
    chan_py_path: str | None,
    page_size: int,
) -> dict[str, int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_task(task: dict[str, Any]) -> dict[str, int]:
        async with semaphore:
            return await process_task(
                kline_writer=kline_writer,
                chan_writer=chan_writer,
                task_store=task_store,
                task=task,
                analysis_levels=analysis_levels,
                sleep=sleep,
                chan_py_path=chan_py_path,
                page_size=page_size,
            )

    results = await asyncio.gather(*(run_task(task) for task in tasks))
    return {"runs": sum(item["runs"] for item in results)}


async def process_task(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    task_store: PostgresChanRecomputeTaskStore,
    task: dict[str, Any],
    analysis_levels: list[str],
    sleep: float,
    chan_py_path: str | None,
    page_size: int,
) -> dict[str, int]:
    symbol = str(task["symbol"])
    base_timeframe = DB_TO_TIMEFRAME[int(task["chan_level"])]
    modes = parse_csv(str(task["modes"]))
    try:
        bars, response = await compute_chan_overlay_chunked(
            kline_writer=kline_writer,
            symbol=symbol,
            base_timeframe=base_timeframe,
            analysis_levels=analysis_levels,
            modes=modes,
            chan_py_path=chan_py_path,
            page_size=page_size,
        )
        validate_chan_response(
            response=response,
            symbol=symbol,
            analysis_levels=analysis_levels,
            bar_from=bars[0].ts,
            bar_until=bars[-1].ts,
            bar_count=len(bars),
        )
        aggregate_counts = {"strokes": 0, "segments": 0, "centers": 0, "signals": 0}
        for level in analysis_levels:
            level_response = filter_chan_response_level(response, level)
            counts = await chan_writer.replace_analysis(
                symbol=symbol,
                level=level,
                modes=modes,
                bar_from=bars[0].ts,
                bar_until=bars[-1].ts,
                bar_count=len(bars),
                response=level_response,
            )
            for key in aggregate_counts:
                aggregate_counts[key] += counts.get(key, 0)
        await task_store.record_success(
            task_id=int(task["id"]),
            bar_from=bars[0].ts,
            bar_until=bars[-1].ts,
            bar_count=len(bars),
            counts=aggregate_counts,
        )
        emit(
            "chan_recompute_written",
            symbol=symbol,
            base_timeframe=base_timeframe,
            levels=analysis_levels,
            engine=response.get("engine"),
            input_bars=len(bars),
            **aggregate_counts,
        )
        await sleep_between_requests(sleep)
        return {"runs": 1}
    except Exception as exc:
        await task_store.record_failure(task_id=int(task["id"]), error=str(exc))
        emit(
            "chan_recompute_failed",
            symbol=symbol,
            base_timeframe=base_timeframe,
            levels=analysis_levels,
            error=str(exc)[:500],
        )
        return {"runs": 0}


async def sleep_between_requests(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


async def compute_chan_overlay_chunked(
    *,
    kline_writer: PostgresKlineWriter,
    symbol: str,
    base_timeframe: str,
    analysis_levels: list[str],
    modes: list[str],
    chan_py_path: str | None,
    page_size: int,
) -> tuple[list[Any], dict[str, Any]]:
    build_overlay = _load_chan_overlay_builder()
    all_bars = await kline_writer.get_bars(symbol, base_timeframe)
    if not all_bars:
        raise RuntimeError(f"No stored K-lines for {symbol} {base_timeframe}")
    response = await asyncio.to_thread(
        build_overlay,
        {
            "symbol": symbol,
            "timeframe": base_timeframe,
            "chan_levels": analysis_levels,
            "modes": modes,
            "bars": [bar_to_chan_payload(bar) for bar in all_bars],
            "chan_py_path": chan_py_path,
        },
    )
    return all_bars, response


def validate_chan_response(
    *,
    response: dict[str, Any],
    symbol: str,
    analysis_levels: list[str],
    bar_from: datetime,
    bar_until: datetime,
    bar_count: int,
) -> None:
    engine = str(response.get("engine") or "")
    formal_engines = {"module-b:chan.py"}
    if engine not in formal_engines:
        raise RuntimeError(f"Rejected non-formal Chan engine for {symbol}: {engine or 'unknown'}")
    levels = set(analysis_levels)
    for key in ("strokes", "segments", "centers", "signals", "channels"):
        if not isinstance(response.get(key), list):
            raise RuntimeError(f"Invalid Chan response for {symbol}: {key} is not a list")
    for level in levels:
        if not any(item.get("level") == level for item in response["strokes"]):
            raise RuntimeError(f"Invalid Chan response for {symbol}: missing {level} strokes")
    total_strokes = len(response["strokes"])
    total_segments = len(response["segments"])
    total_centers = len(response["centers"])
    max_line_count = max(1000, int(bar_count * 0.25))
    if total_strokes > max_line_count:
        raise RuntimeError(
            f"Invalid Chan response for {symbol}: too many strokes {total_strokes}/{bar_count}"
        )
    if total_segments > total_strokes or total_centers > total_strokes:
        raise RuntimeError(
            f"Invalid Chan response for {symbol}: inconsistent counts "
            f"strokes={total_strokes}, segments={total_segments}, centers={total_centers}"
        )
    range_start = int(bar_from.timestamp())
    range_end = int(bar_until.timestamp())
    for part in ("strokes", "segments"):
        for index, item in enumerate(response[part]):
            _validate_level(item, levels, symbol, part, index)
            start_time = _required_nested_int(item, ["start", "time"], symbol, part, index)
            end_time = _required_nested_int(item, ["end", "time"], symbol, part, index)
            begin_base_ts = int(item.get("begin_base_ts") or start_time)
            end_base_ts = int(item.get("end_base_ts") or end_time)
            _validate_ts_range(start_time, range_start, range_end, symbol, part, index, "start.time")
            _validate_ts_range(end_time, range_start, range_end, symbol, part, index, "end.time")
            _validate_ts_range(begin_base_ts, range_start, range_end, symbol, part, index, "begin_base_ts")
            _validate_ts_range(end_base_ts, range_start, range_end, symbol, part, index, "end_base_ts")
            if begin_base_ts > end_base_ts:
                raise RuntimeError(f"Invalid Chan response for {symbol}: {part}[{index}] reversed base range")
    for index, item in enumerate(response["centers"]):
        _validate_level(item, levels, symbol, "centers", index)
        start_time = _required_int(item, "start_time", symbol, "centers", index)
        end_time = _required_int(item, "end_time", symbol, "centers", index)
        begin_base_ts = int(item.get("begin_base_ts") or start_time)
        end_base_ts = int(item.get("end_base_ts") or end_time)
        _validate_ts_range(start_time, range_start, range_end, symbol, "centers", index, "start_time")
        _validate_ts_range(end_time, range_start, range_end, symbol, "centers", index, "end_time")
        _validate_ts_range(begin_base_ts, range_start, range_end, symbol, "centers", index, "begin_base_ts")
        _validate_ts_range(end_base_ts, range_start, range_end, symbol, "centers", index, "end_base_ts")
        if begin_base_ts > end_base_ts:
            raise RuntimeError(f"Invalid Chan response for {symbol}: centers[{index}] reversed base range")
        if float(item.get("low", 0)) > float(item.get("high", 0)):
            raise RuntimeError(f"Invalid Chan response for {symbol}: centers[{index}] low > high")
    for index, item in enumerate(response["signals"]):
        _validate_level(item, levels, symbol, "signals", index)
        signal_time = _required_int(item, "time", symbol, "signals", index)
        base_ts = int(item.get("base_ts") or signal_time)
        _validate_ts_range(signal_time, range_start, range_end, symbol, "signals", index, "time")
        _validate_ts_range(base_ts, range_start, range_end, symbol, "signals", index, "base_ts")


def _validate_level(item: dict[str, Any], levels: set[str], symbol: str, part: str, index: int) -> None:
    level = item.get("level")
    if level not in levels:
        raise RuntimeError(f"Invalid Chan response for {symbol}: {part}[{index}] invalid level {level!r}")


def _required_nested_int(
    item: dict[str, Any],
    path: list[str],
    symbol: str,
    part: str,
    index: int,
) -> int:
    value: Any = item
    for key in path:
        if not isinstance(value, dict) or key not in value:
            raise RuntimeError(f"Invalid Chan response for {symbol}: {part}[{index}] missing {'.'.join(path)}")
        value = value[key]
    return int(value)


def _required_int(item: dict[str, Any], key: str, symbol: str, part: str, index: int) -> int:
    if key not in item:
        raise RuntimeError(f"Invalid Chan response for {symbol}: {part}[{index}] missing {key}")
    return int(item[key])


def _validate_ts_range(
    value: int,
    range_start: int,
    range_end: int,
    symbol: str,
    part: str,
    index: int,
    field: str,
) -> None:
    if value < range_start or value > range_end:
        raise RuntimeError(
            f"Invalid Chan response for {symbol}: {part}[{index}].{field} out of input range"
        )


@lru_cache(maxsize=1)
def _load_chan_overlay_builder():
    adapter_path = (
        Path(__file__).resolve().parents[3]
        / "services"
        / "chan-service"
        / "chan_service"
        / "vendor_chan_adapter.py"
    )
    spec = importlib.util.spec_from_file_location("collector_vendor_chan_adapter", adapter_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load Chan adapter module: {adapter_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.build_overlay


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
