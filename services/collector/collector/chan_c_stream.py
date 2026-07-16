from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Sequence

from collector.chan_module_c_recompute import (
    MODULE_C_CONFIG_HASH,
    compute_module_c_overlay,
    validate_module_c_response,
)
from collector.market_fill import (
    DEFAULT_MODES,
    filter_chan_response_level,
    normalize_symbol,
    parse_csv,
    parse_timeframes,
)
from collector.realtime_publisher import publish_chan_head_update
from collector.storage.chan_c_stream_postgres import PostgresChanCStreamStore
from collector.storage.chan_postgres import MODULE_C_CHAN_TABLES, PostgresChanWriter
from collector.storage.postgres import PostgresKlineWriter
from trading_protocol.timeframes import TIMEFRAMES

DB_TO_TIMEFRAME = {value.minutes: code for code, value in TIMEFRAMES.items()}
DEFAULT_MODULE_C_CHAN_LEVELS = "5f,30f,1d,1w,1m"
DEFAULT_TAIL_BAR_LIMIT = 2000
DEFAULT_CONTEXT_BARS = 64


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Module C native-timeframe streaming Chan tail worker"
    )
    parser.add_argument("--symbols", default=os.getenv("CHAN_C_STREAM_SYMBOLS"))
    parser.add_argument(
        "--chan-levels",
        default=os.getenv("CHAN_C_STREAM_LEVELS", DEFAULT_MODULE_C_CHAN_LEVELS),
    )
    parser.add_argument(
        "--modes", default=os.getenv("CHAN_C_STREAM_MODES", DEFAULT_MODES)
    )
    parser.add_argument(
        "--task-limit",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_TASK_LIMIT", "100")),
    )
    parser.add_argument(
        "--discovery-limit",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_DISCOVERY_LIMIT", "500")),
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_CONCURRENCY", "1")),
    )
    parser.add_argument(
        "--tail-bar-limit",
        type=int,
        default=int(
            os.getenv("CHAN_C_STREAM_TAIL_BAR_LIMIT", str(DEFAULT_TAIL_BAR_LIMIT))
        ),
    )
    parser.add_argument(
        "--context-bars",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_CONTEXT_BARS", str(DEFAULT_CONTEXT_BARS))),
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_LEASE_SECONDS", "600")),
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_SHARD_INDEX", "0")),
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_SHARD_COUNT", "1")),
    )
    parser.add_argument("--worker-id", default=os.getenv("CHAN_C_STREAM_WORKER_ID"))
    parser.add_argument(
        "--loop", action="store_true", default=os.getenv("CHAN_C_STREAM_LOOP") == "1"
    )
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("CHAN_C_STREAM_LOOP_INTERVAL", "5")),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.getenv("CHAN_C_STREAM_DRY_RUN") == "1",
    )
    parser.add_argument("--chan-py-path", default=os.getenv("CHAN_PY_PATH"))
    parser.add_argument(
        "--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    )
    parser.add_argument(
        "--skip-publish",
        action="store_true",
        default=os.getenv("CHAN_C_STREAM_SKIP_PUBLISH") == "1",
    )
    parser.add_argument(
        "--db-pool-min-size",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_DB_POOL_MIN_SIZE", "1")),
    )
    parser.add_argument(
        "--db-pool-max-size",
        type=int,
        default=int(os.getenv("CHAN_C_STREAM_DB_POOL_MAX_SIZE", "4")),
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    while True:
        await run_once(args)
        if not args.loop:
            return
        await asyncio.sleep(args.loop_interval)


async def run_once(args: argparse.Namespace) -> None:
    levels = parse_timeframes(args.chan_levels)
    modes = parse_csv(args.modes)
    symbols = (
        sorted({normalize_symbol(value) for value in parse_csv(args.symbols)})
        if args.symbols
        else None
    )
    worker_id = (
        args.worker_id
        or f"chan-c-stream-{os.getpid()}-{args.shard_index}of{args.shard_count}"
    )
    emit(
        "chan_c_stream_pass_started",
        worker_id=worker_id,
        symbols=None if symbols is None else len(symbols),
        levels=levels,
        modes=modes,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        emit("chan_c_stream_pass_finished", discovered=0, tasks=0, runs=0)
        return

    db_pool_min_size = max(1, args.db_pool_min_size)
    db_pool_max_size = max(db_pool_min_size, args.db_pool_max_size)
    async with PostgresKlineWriter(
        args.database_url,
        pool_min_size=db_pool_min_size,
        pool_max_size=db_pool_max_size,
    ) as kline_writer:
        async with PostgresChanWriter(
            args.database_url,
            pool_min_size=db_pool_min_size,
            pool_max_size=db_pool_max_size,
            tables=MODULE_C_CHAN_TABLES,
            run_config_hash=MODULE_C_CONFIG_HASH,
            tail_config_hash=MODULE_C_CONFIG_HASH,
            native_base_timeframe=True,
            publication_profile="online",
            publication_source="stream",
            run_kind="online",
            run_group_id="online",
            worker_id=worker_id,
        ) as chan_writer:
            async with PostgresChanCStreamStore(
                args.database_url,
                pool_min_size=db_pool_min_size,
                pool_max_size=db_pool_max_size,
            ) as task_store:
                normalized = await task_store.normalize_higher_timeframe_targets(
                    levels=levels,
                    modes=modes,
                    shard_index=max(0, args.shard_index),
                    shard_count=max(1, args.shard_count),
                    symbols=symbols,
                )
                if normalized:
                    emit(
                        "chan_c_stream_higher_timeframes_normalized",
                        worker_id=worker_id,
                        updated=normalized,
                    )
                discovered = await task_store.ensure_tail_tasks_for_stale_heads(
                    levels=levels,
                    modes=modes,
                    limit=max(1, args.discovery_limit),
                    shard_index=max(0, args.shard_index),
                    shard_count=max(1, args.shard_count),
                    symbols=symbols,
                )
                tasks = await task_store.claim_tail_tasks(
                    limit=max(1, args.task_limit),
                    worker_id=worker_id,
                    lease_seconds=max(1, args.lease_seconds),
                    shard_index=max(0, args.shard_index),
                    shard_count=max(1, args.shard_count),
                    symbols=symbols,
                )
                runs = await process_tail_tasks(
                    kline_writer=kline_writer,
                    chan_writer=chan_writer,
                    task_store=task_store,
                    tasks=tasks,
                    chan_py_path=args.chan_py_path,
                    tail_bar_limit=max(1, args.tail_bar_limit),
                    context_bars=max(0, args.context_bars),
                    concurrency=max(1, args.concurrency),
                    redis_url=None if args.skip_publish else args.redis_url,
                )
    emit(
        "chan_c_stream_pass_finished",
        discovered=discovered,
        tasks=len(tasks),
        runs=runs,
    )


async def process_tail_tasks(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    task_store: PostgresChanCStreamStore,
    tasks: list[dict[str, Any]],
    chan_py_path: str | None,
    tail_bar_limit: int,
    context_bars: int,
    concurrency: int,
    redis_url: str | None,
) -> int:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        grouped[str(task["symbol"])].append(task)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_symbol(symbol: str, jobs: list[dict[str, Any]]) -> int:
        async with semaphore:
            try:
                runs = await process_symbol_tail(
                    kline_writer=kline_writer,
                    chan_writer=chan_writer,
                    symbol=symbol,
                    jobs=jobs,
                    chan_py_path=chan_py_path,
                    tail_bar_limit=tail_bar_limit,
                    context_bars=context_bars,
                    redis_url=redis_url,
                )
                bar_until = max(
                    (
                        job.get("last_bar_end")
                        for job in jobs
                        if job.get("last_bar_end")
                    ),
                    default=None,
                )
                for job in jobs:
                    await task_store.complete_tail_task(
                        task_id=int(job["id"]),
                        claim_token=str(job["claim_token"]),
                        bar_until=bar_until,
                    )
                return runs
            except Exception as exc:
                for job in jobs:
                    await task_store.complete_tail_task(
                        task_id=int(job["id"]),
                        claim_token=str(job["claim_token"]),
                        error=str(exc),
                    )
                emit("chan_c_stream_failed", symbol=symbol, error=str(exc)[:500])
                return 0

    results = await asyncio.gather(
        *(run_symbol(symbol, jobs) for symbol, jobs in grouped.items())
    )
    return sum(results)


async def process_symbol_tail(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    symbol: str,
    jobs: list[dict[str, Any]],
    chan_py_path: str | None,
    tail_bar_limit: int,
    context_bars: int,
    redis_url: str | None,
) -> int:
    runs = 0
    for level, level_jobs in group_jobs_by_level(jobs).items():
        anchors = [
            job["anchor_bar_end"] for job in level_jobs if job.get("anchor_bar_end")
        ]
        if not anchors:
            emit(
                "chan_c_stream_skipped",
                symbol=symbol,
                level=level,
                reason="missing_anchor",
            )
            continue
        tail_start = min(anchors)
        query_after_ts = tail_start - timedelta(
            minutes=TIMEFRAMES[level].minutes * context_bars
        )
        bars = await kline_writer.get_bars_chunk(
            symbol,
            level,
            after_ts=query_after_ts,
            limit=tail_bar_limit,
        )
        if not bars:
            emit(
                "chan_c_stream_skipped",
                symbol=symbol,
                level=level,
                reason="no_tail_bars",
            )
            continue
        latest_required = max(
            job.get("last_bar_end") or job["anchor_bar_end"] for job in level_jobs
        )
        if bars[-1].ts < latest_required:
            raise RuntimeError(
                "tail_bar_limit_exhausted: "
                f"{symbol} {level} requires bars through {latest_required.isoformat()}, "
                f"but only loaded through {bars[-1].ts.isoformat()} with limit {tail_bar_limit}"
            )

        modes = sorted({str(job["mode"]) for job in level_jobs})
        response = await compute_module_c_overlay(
            symbol=symbol,
            levels=[level],
            modes=modes,
            bars_by_level={level: bars},
            chan_py_path=chan_py_path,
        )
        validate_module_c_response(
            response=response,
            symbol=symbol,
            levels=[level],
            bars_by_level={level: bars},
        )
        level_response = filter_chan_response_level(response, level)
        for mode, mode_jobs in group_jobs_by_mode(level_jobs).items():
            mode_anchors = [
                job["anchor_bar_end"] for job in mode_jobs if job.get("anchor_bar_end")
            ]
            if not mode_anchors:
                emit(
                    "chan_c_stream_skipped",
                    symbol=symbol,
                    level=level,
                    mode=mode,
                    reason="missing_mode_anchor",
                )
                continue
            anchor = min(mode_anchors)
            counts = await chan_writer.replace_incremental_analysis(
                symbol=symbol,
                level=level,
                modes=[mode],
                anchor_bar_end=anchor,
                bar_until=bars[-1].ts,
                response=level_response,
                expected_head_run_id=first_present(mode_jobs, "expected_head_run_id"),
                expected_head_base_to_bar_end=first_present(
                    mode_jobs, "expected_head_base_to_bar_end"
                ),
                publication_claim_token=first_present(mode_jobs, "claim_token"),
            )
            emit(
                "chan_c_stream_published",
                symbol=symbol,
                level=level,
                mode=mode,
                anchor_bar_end=anchor.isoformat(),
                bar_until=bars[-1].ts.isoformat(),
                input_bars=len(bars),
                **counts,
            )
            if redis_url:
                await publish_chan_head_update(
                    redis_url=redis_url,
                    symbol=symbol,
                    level=level,
                    modes=[mode],
                    bar_until=bars[-1].ts,
                    run_id=int(counts["run_id"]),
                    snapshot_version=str(counts["snapshot_version"]),
                )
            runs += 1
    return runs


def first_present(items: list[dict[str, Any]], key: str) -> Any:
    for item in items:
        value = item.get(key)
        if value is not None:
            return value
    return None


def group_jobs_by_level(jobs: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for job in jobs:
        grouped[DB_TO_TIMEFRAME[int(job["chan_level"])]].append(job)
    return dict(grouped)


def group_jobs_by_mode(jobs: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for job in jobs:
        grouped[str(job["mode"])].append(job)
    return dict(grouped)


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
