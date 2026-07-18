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
from trading_protocol import kline_logical_key
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
            raise RuntimeError(f"missing_tail_anchor: {symbol} {level}")
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
            raise RuntimeError(f"no_tail_bars: {symbol} {level}")
        latest_required = max(
            job.get("last_bar_end") or job["anchor_bar_end"] for job in level_jobs
        )
        if kline_logical_key(level, bars[-1].ts) < kline_logical_key(
            level, latest_required
        ):
            raise RuntimeError(
                "tail_bar_limit_exhausted: "
                f"{symbol} {level} requires bars through {latest_required.isoformat()}, "
                f"but only loaded through {bars[-1].ts.isoformat()} with limit {tail_bar_limit}"
            )

        for claimed_target, target_jobs in group_jobs_by_claimed_target(
            level_jobs
        ).items():
            target_bars, publication_bar_until = bars_through_claimed_period(
                level=level,
                bars=bars,
                claimed_target=claimed_target,
                symbol=symbol,
            )
            modes = sorted({str(job["mode"]) for job in target_jobs})
            response = await compute_module_c_overlay(
                symbol=symbol,
                levels=[level],
                modes=modes,
                bars_by_level={level: target_bars},
                chan_py_path=chan_py_path,
            )
            validate_module_c_response(
                response=response,
                symbol=symbol,
                levels=[level],
                bars_by_level={level: target_bars},
            )
            level_response = filter_chan_response_level(response, level)
            for mode, mode_jobs in group_jobs_by_mode(target_jobs).items():
                if len(mode_jobs) != 1:
                    raise RuntimeError(
                        "duplicate_tail_task_identity: "
                        f"{symbol} {level} {mode} {claimed_target.isoformat()}"
                    )
                job = mode_jobs[0]
                anchor = job.get("anchor_bar_end")
                if anchor is None:
                    emit(
                        "chan_c_stream_skipped",
                        symbol=symbol,
                        level=level,
                        mode=mode,
                        reason="missing_mode_anchor",
                    )
                    raise RuntimeError(
                        f"missing_tail_anchor: {symbol} {level} {mode}"
                    )
                counts = await chan_writer.replace_incremental_analysis(
                    symbol=symbol,
                    level=level,
                    modes=[mode],
                    anchor_bar_end=anchor,
                    bar_until=publication_bar_until,
                    response=level_response,
                    publication_task_id=int(job["id"]),
                    publication_claim_token=str(job["claim_token"]),
                    publication_lease_version=int(job["lease_version"]),
                    publication_target_bar_end=claimed_target,
                    expected_head_run_id=job.get("expected_head_run_id"),
                    expected_head_base_to_bar_end=job.get(
                        "expected_head_base_to_bar_end"
                    ),
                )
                emit(
                    "chan_c_stream_published",
                    symbol=symbol,
                    level=level,
                    mode=mode,
                    anchor_bar_end=anchor.isoformat(),
                    bar_until=publication_bar_until.isoformat(),
                    input_bars=len(target_bars),
                    **counts,
                )
                if redis_url:
                    await publish_chan_head_update(
                        redis_url=redis_url,
                        symbol=symbol,
                        level=level,
                        modes=[mode],
                        bar_until=publication_bar_until,
                        run_id=int(counts["run_id"]),
                        snapshot_version=str(counts["snapshot_version"]),
                    )
                runs += 1
    return runs


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


def group_jobs_by_claimed_target(
    jobs: list[dict[str, Any]],
) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for job in jobs:
        claimed_target = job.get("claimed_target_bar_end")
        if not isinstance(claimed_target, datetime):
            raise RuntimeError("Tail task is missing its frozen claimed target")
        grouped[claimed_target].append(job)
    return dict(grouped)


def bars_through_claimed_period(
    *,
    level: str,
    bars: list[Any],
    claimed_target: datetime,
    symbol: str,
) -> tuple[list[Any], datetime]:
    claimed_key = kline_logical_key(level, claimed_target)
    target_bars = [
        bar for bar in bars if kline_logical_key(level, bar.ts) <= claimed_key
    ]
    if (
        not target_bars
        or kline_logical_key(level, target_bars[-1].ts) != claimed_key
    ):
        raise RuntimeError(
            "tail_claim_target_missing: "
            f"{symbol} {level} requires claimed period {claimed_key[1].isoformat()}"
        )
    return target_bars, target_bars[-1].ts


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
