from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import math
import os
import uuid
from functools import lru_cache
from pathlib import Path
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from collector.market_fill import (
    DEFAULT_MODES,
    bar_to_chan_payload,
    filter_chan_response_level,
    normalize_symbol,
    parse_csv,
    parse_timeframes,
)
from collector.storage.chan_postgres import MODULE_C_CHAN_TABLES, PostgresChanWriter
from collector.storage.postgres import PostgresKlineWriter, timeframe_to_db_code
from trading_protocol import Bar, MODULE_C_CONFIG_HASH

DEFAULT_MODULE_C_CHAN_LEVELS = "5f,30f,1d,1w,1m"
CN_TZ = ZoneInfo("Asia/Shanghai")


class InactiveRecomputeBatchError(RuntimeError):
    """The durable recompute batch is no longer allowed to execute."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Module C native-timeframe Chan recompute worker")
    parser.add_argument("--symbols", default=os.getenv("CHAN_MODULE_C_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=(
            int(os.environ["CHAN_MODULE_C_SYMBOL_LIMIT"])
            if "CHAN_MODULE_C_SYMBOL_LIMIT" in os.environ
            else None
        ),
        help="Maximum database symbols when --symbols is omitted. Use 0 for all symbols with native bars.",
    )
    parser.add_argument("--chan-levels", default=os.getenv("CHAN_MODULE_C_LEVELS", DEFAULT_MODULE_C_CHAN_LEVELS))
    parser.add_argument("--modes", default=os.getenv("CHAN_MODULE_C_MODES", DEFAULT_MODES))
    parser.add_argument("--run-group-id", default=os.getenv("CHAN_MODULE_C_RUN_GROUP_ID"))
    parser.add_argument("--batch-id", type=int, default=os.getenv("CHAN_MODULE_C_BATCH_ID"))
    parser.add_argument("--eligibility-build-id", default=os.getenv("CHAN_MODULE_C_ELIGIBILITY_BUILD_ID"))
    parser.add_argument("--publication-namespace", default=os.getenv("CHAN_MODULE_C_PUBLICATION_NAMESPACE", "production"))
    parser.add_argument("--profile-id", default=os.getenv("CHAN_MODULE_C_PROFILE_ID", "module-c-native-5lvl"))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("CHAN_MODULE_C_CONCURRENCY", "1")))
    parser.add_argument("--shard-index", type=int, default=int(os.getenv("CHAN_MODULE_C_SHARD_INDEX", "0")))
    parser.add_argument("--shard-count", type=int, default=int(os.getenv("CHAN_MODULE_C_SHARD_COUNT", "1")))
    parser.add_argument("--worker-id", default=os.getenv("CHAN_MODULE_C_WORKER_ID"))
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.getenv("CHAN_MODULE_C_LEASE_SECONDS", "900")),
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.getenv("CHAN_MODULE_C_MAX_ATTEMPTS", "3")),
    )
    parser.add_argument("--sleep", type=float, default=float(os.getenv("CHAN_MODULE_C_SLEEP", "0.1")))
    parser.add_argument(
        "--db-pool-min-size",
        type=int,
        default=int(os.getenv("CHAN_MODULE_C_DB_POOL_MIN_SIZE", "1")),
    )
    parser.add_argument(
        "--db-pool-max-size",
        type=int,
        default=int(os.getenv("CHAN_MODULE_C_DB_POOL_MAX_SIZE", "1")),
    )
    parser.add_argument("--chan-py-path", default=os.getenv("CHAN_PY_PATH"))
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("CHAN_MODULE_C_DRY_RUN") == "1")
    parser.add_argument(
        "--skip-completed",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("CHAN_MODULE_C_SKIP_COMPLETED", "1") != "0",
        help="Skip symbols whose Module C published heads already cover all requested levels and modes.",
    )
    parser.add_argument(
        "--prepare-native-bars",
        action="store_true",
        default=os.getenv("CHAN_MODULE_C_PREPARE_NATIVE_BARS") == "1",
        help="Pre-aggregate missing/stale 30f and 1d bars from stored 5f bars before Module C calculation. Do not use for native five-level runs.",
    )
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
    levels = parse_timeframes(args.chan_levels)
    modes = parse_csv(args.modes)
    db_pool_min_size = max(1, args.db_pool_min_size)
    db_pool_max_size = max(db_pool_min_size, args.db_pool_max_size)
    if not args.dry_run and not args.run_group_id:
        raise ValueError("--run-group-id is required for a non-dry Module C recompute")
    if not args.dry_run and not args.batch_id:
        raise ValueError("--batch-id is required for a non-dry Module C recompute")
    if not args.dry_run and not args.eligibility_build_id:
        raise ValueError("--eligibility-build-id is required for a non-dry Module C recompute")
    if not args.dry_run and (args.symbols or args.symbol_limit not in (None, 0)):
        raise ValueError(
            "--symbols and positive --symbol-limit are dry-run only; production scope must come from the frozen eligibility build"
        )
    if not args.dry_run and set(levels) != set(parse_timeframes(DEFAULT_MODULE_C_CHAN_LEVELS)):
        raise ValueError("A production Module C batch must use the native five-level contract")
    if not args.dry_run and args.prepare_native_bars:
        raise ValueError("--prepare-native-bars is forbidden for a production Module C batch")
    if not args.dry_run and args.concurrency != 1:
        raise ValueError("A durable B4 worker must use --concurrency=1; scale with static shards")
    if not args.dry_run and set(modes) != {"confirmed", "predictive"}:
        raise ValueError("A production Module C batch must publish confirmed and predictive modes")
    run_group_id = args.run_group_id or f"dry-run-{uuid.uuid4()}"

    async with PostgresKlineWriter(
        args.database_url,
        pool_min_size=db_pool_min_size,
        pool_max_size=db_pool_max_size,
    ) as kline_writer:
        shard_count = max(1, args.shard_count)
        shard_index = args.shard_index
        if shard_index < 0 or shard_index >= shard_count:
            raise ValueError(f"--shard-index must be in [0, {shard_count - 1}]")
        if args.dry_run:
            symbols = await resolve_symbols(
                kline_writer=kline_writer,
                symbols_arg=args.symbols,
                levels=levels,
                symbol_limit=10 if args.symbol_limit is None else args.symbol_limit,
            )
            if shard_count > 1:
                symbols = [symbol for index, symbol in enumerate(symbols) if index % shard_count == shard_index]
            emit(
                "chan_module_c_pass_started",
                symbols=len(symbols),
                levels=levels,
                modes=modes,
                concurrency=max(1, args.concurrency),
                shard_index=shard_index,
                shard_count=shard_count,
                dry_run=True,
            )
            for symbol in symbols:
                emit("chan_module_c_dry_symbol", symbol=symbol, levels=levels)
            emit("chan_module_c_pass_finished", symbols=len(symbols), runs=0)
            return

        worker_id = args.worker_id or f"module-c-b4-s{shard_index}-{uuid.uuid4().hex[:12]}"
        await ensure_recompute_batch(
            kline_writer=kline_writer,
            batch_id=args.batch_id,
            eligibility_build_id=args.eligibility_build_id,
            run_group_id=run_group_id,
            config_hash=MODULE_C_CONFIG_HASH,
            publication_namespace=args.publication_namespace,
            profile_id=args.profile_id,
            shard_count=shard_count,
            levels=levels,
        )
        emit(
            "chan_module_c_pass_started",
            batch_id=args.batch_id,
            eligibility_build_id=args.eligibility_build_id,
            levels=levels,
            modes=modes,
            concurrency=1,
            shard_index=shard_index,
            shard_count=shard_count,
            worker_id=worker_id,
            dry_run=False,
        )

        async with PostgresChanWriter(
            args.database_url,
            pool_min_size=db_pool_min_size,
            pool_max_size=db_pool_max_size,
            tables=MODULE_C_CHAN_TABLES,
            run_config_hash=MODULE_C_CONFIG_HASH,
            native_base_timeframe=True,
            publication_profile="baseline",
            publication_source="full_recompute",
            run_kind="full_recompute",
            batch_id=args.batch_id,
            publication_namespace=args.publication_namespace,
            profile_id=args.profile_id,
            run_group_id=run_group_id,
            worker_id=worker_id,
        ) as chan_writer:
            result = await process_claimed_tasks(
                kline_writer=kline_writer,
                chan_writer=chan_writer,
                batch_id=args.batch_id,
                modes=modes,
                worker_id=worker_id,
                shard_index=shard_index,
                shard_count=shard_count,
                lease_seconds=max(30, args.lease_seconds),
                max_attempts=max(1, args.max_attempts),
                sleep=max(0.0, args.sleep),
                chan_py_path=args.chan_py_path,
            )
        emit(
            "chan_module_c_pass_finished",
            batch_id=args.batch_id,
            runs=result["runs"],
            failed=result["failed"],
        )
        if result["failed"]:
            raise RuntimeError(
                f"Module C batch {args.batch_id} shard {shard_index} has "
                f"{result['failed']} exhausted failed tasks"
            )


async def resolve_symbols(
    *,
    kline_writer: PostgresKlineWriter,
    symbols_arg: str | None,
    levels: list[str],
    symbol_limit: int,
) -> list[str]:
    if symbols_arg:
        return sorted({normalize_symbol(value) for value in parse_csv(symbols_arg)})

    assert kline_writer._pool is not None
    level_codes = [timeframe_to_db_code(level) for level in levels]
    limit_value = None if symbol_limit <= 0 else symbol_limit
    async with kline_writer._pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select s.code || '.' || s.exchange as symbol
            from symbols s
            where s.is_active = true
              and not exists (
                  select 1
                  from unnest($1::int[]) tf(timeframe)
                  where not exists (
                      select 1
                      from scheme2_ingest_watermarks wm
                      where wm.symbol_id = s.id
                        and wm.timeframe = tf.timeframe
                  )
              )
            order by s.code, s.exchange
            limit coalesce($2::int, 2147483647)
            """,
            level_codes,
            limit_value,
        )
    return [str(row["symbol"]) for row in rows]


async def filter_completed_symbols(
    *,
    kline_writer: PostgresKlineWriter,
    symbols: list[str],
    levels: list[str],
    modes: list[str],
) -> list[str]:
    if not symbols:
        return []

    assert kline_writer._pool is not None
    level_codes = [timeframe_to_db_code(level) for level in levels]
    async with kline_writer._pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select s.code || '.' || s.exchange as symbol,
                   head.chan_level,
                   head.base_timeframe,
                   head.mode,
                   head.status as head_status,
                   run.status as run_status,
                   run.config_hash
            from symbols s
            left join scheme2_chan_c_published_heads head
              on head.symbol_id = s.id
             and head.status = 'published'
             and head.chan_level = any($2::int[])
             and head.mode = any($3::text[])
            left join chan_c_runs run
              on run.id = head.run_id
            where s.is_active = true
              and (s.code || '.' || s.exchange) = any($1::text[])
            order by s.code, s.exchange
            """,
            symbols,
            level_codes,
            modes,
        )
    rows_by_symbol: dict[str, list[Mapping[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in rows:
        rows_by_symbol.setdefault(str(row["symbol"]), []).append(row)
    return [
        symbol
        for symbol in symbols
        if not is_module_c_complete(rows_by_symbol.get(symbol, []), levels=level_codes, modes=modes)
    ]


def is_module_c_complete(
    rows: Iterable[Mapping[str, Any]],
    *,
    levels: Iterable[int],
    modes: Iterable[str],
) -> bool:
    expected = {(int(level), str(mode)) for level in levels for mode in modes}
    complete = {
        (int(row["chan_level"]), str(row["mode"]))
        for row in rows
        if row.get("head_status") == "published"
        and row.get("run_status") == "success"
        and row.get("config_hash") == MODULE_C_CONFIG_HASH
        and row.get("base_timeframe") == row.get("chan_level")
    }
    return expected <= complete


DB_LEVEL_NAMES = {
    5: "5f",
    30: "30f",
    1440: "1d",
    10080: "1w",
    43200: "1m",
}


async def ensure_recompute_batch(
    *,
    kline_writer: PostgresKlineWriter,
    batch_id: int,
    eligibility_build_id: str,
    run_group_id: str,
    config_hash: str,
    publication_namespace: str,
    profile_id: str,
    shard_count: int,
    levels: list[str],
) -> None:
    """Create one immutable batch manifest and its level-specific tasks."""
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        async with conn.transaction():
            await ensure_recompute_batch_on_connection(
                conn=conn,
                batch_id=batch_id,
                eligibility_build_id=eligibility_build_id,
                run_group_id=run_group_id,
                config_hash=config_hash,
                publication_namespace=publication_namespace,
                profile_id=profile_id,
                shard_count=shard_count,
                levels=levels,
                allow_create=False,
            )


async def ensure_recompute_batch_on_connection(
    *,
    conn: Any,
    batch_id: int,
    eligibility_build_id: str,
    run_group_id: str,
    config_hash: str,
    publication_namespace: str,
    profile_id: str,
    shard_count: int,
    levels: list[str],
    allow_create: bool,
) -> None:
    """Create a recompute child and tasks inside the caller's transaction."""
    level_codes = [timeframe_to_db_code(level) for level in levels]
    parent = await conn.fetchrow(
        """
        select parent.status, parent.batch_kind, parent.run_group_id,
               parent.config_hash, parent.publication_namespace, parent.profile_id,
               parent.eligible_manifest_sha256, build.manifest_hash
          from chan_c_batches parent
          join module_c_eligibility_builds build on build.build_id=$2::uuid
         where parent.id=$1
         for update of parent
        """,
        batch_id,
        eligibility_build_id,
    )
    if parent is None:
        raise InactiveRecomputeBatchError(f"Unknown parent batch: {batch_id}")
    parent_status = str(parent["status"])
    required_parent_status = "planned" if allow_create else "running"
    if parent_status != required_parent_status:
        raise InactiveRecomputeBatchError(
            f"Parent batch {batch_id} is not executable: {parent_status}"
        )
    expected_parent = {
        "run_group_id": run_group_id,
        "config_hash": config_hash,
        "publication_namespace": publication_namespace,
        "profile_id": profile_id,
        "eligible_manifest_sha256": parent["manifest_hash"],
    }
    actual_parent = {key: parent[key] for key in expected_parent}
    if parent["batch_kind"] not in {"canary", "baseline"} or actual_parent != expected_parent:
        raise RuntimeError(f"Parent batch {batch_id} identity mismatch: {actual_parent!r}")

    batch = await conn.fetchrow(
        """
        select eligibility_build_id::text as eligibility_build_id,
               run_group_id, config_hash, publication_namespace,
               profile_id, shard_count, status
          from chan_c_full_recompute_batches
         where batch_id = $1
         for update
        """,
        batch_id,
    )
    required_batch_status = "pending" if allow_create else "running"
    if batch is not None and str(batch["status"]) != required_batch_status:
        raise InactiveRecomputeBatchError(
            f"Full-recompute batch {batch_id} is not executable: {batch['status']}"
        )

    if batch is None and not allow_create:
        raise RuntimeError(
            f"Full-recompute batch {batch_id} was not prepared by module-c-batch-control"
        )
    if allow_create:
        await conn.execute(
        """
        insert into chan_c_full_recompute_batches (
            batch_id, eligibility_build_id, run_group_id, config_hash,
            publication_namespace, profile_id, shard_count,
            active_symbols, disposition_rows
        )
        select $1, build_id, $3, $4::varchar, $5, $6, $7,
               active_symbols, count(*)
          from module_c_eligibility_builds build
          join module_c_eligibility eligibility using (build_id)
         where build.build_id = $2::uuid
           and build.config_hash = $4::text
           and eligibility.timeframe = any($8::int[])
         group by build_id, active_symbols
        on conflict (batch_id) do nothing
        """,
            batch_id, eligibility_build_id, run_group_id, config_hash,
            publication_namespace, profile_id, shard_count, level_codes,
        )
    if batch is None:
        batch = await conn.fetchrow(
            """
            select eligibility_build_id::text as eligibility_build_id,
                   run_group_id, config_hash, publication_namespace,
                   profile_id, shard_count, status
              from chan_c_full_recompute_batches
             where batch_id = $1
             for update
            """,
            batch_id,
        )
    if batch is None:
        raise RuntimeError(f"Unknown eligibility build: {eligibility_build_id}")
    if str(batch["status"]) != required_batch_status:
        raise InactiveRecomputeBatchError(
            f"Full-recompute batch {batch_id} is not executable: {batch['status']}"
        )
    expected = {
        "eligibility_build_id": str(eligibility_build_id),
        "run_group_id": run_group_id,
        "config_hash": config_hash,
        "publication_namespace": publication_namespace,
        "profile_id": profile_id,
        "shard_count": shard_count,
    }
    actual = {key: batch[key] for key in expected}
    if actual != expected:
        raise RuntimeError(f"Batch {batch_id} manifest mismatch: {actual!r}")

    if allow_create:
        await conn.execute(
        """
        insert into chan_c_full_recompute_tasks (
            batch_id, symbol_id, symbol, chan_level, eligible,
            exclusion_reasons, target_bar_until, shard_bucket, status,
            expected_heads
        )
        select $1, eligibility.symbol_id, eligibility.symbol,
               eligibility.timeframe, eligibility.eligible,
               eligibility.reasons, eligibility.covered_until,
               mod((hashtextextended(eligibility.symbol, 0) & 2147483647)::integer, 1024)::smallint,
               case when eligibility.eligible then 'pending' else 'excluded' end,
               coalesce((
                   select jsonb_object_agg(head.mode, head.run_id)
                     from scheme2_chan_c_published_heads head
                    where head.symbol_id = eligibility.symbol_id
                      and head.chan_level = eligibility.timeframe
                      and head.base_timeframe = eligibility.timeframe
                      and head.status = 'published'
               ), '{}'::jsonb)
          from module_c_eligibility eligibility
         where eligibility.build_id = $2::uuid
           and eligibility.timeframe = any($3::int[])
        on conflict (batch_id, symbol_id, chan_level) do nothing
        """,
            batch_id, eligibility_build_id, level_codes,
        )
    task_count = await conn.fetchval(
        "select count(*) from chan_c_full_recompute_tasks where batch_id = $1", batch_id
    )
    disposition_rows = await conn.fetchval(
        "select disposition_rows from chan_c_full_recompute_batches where batch_id = $1",
        batch_id,
    )
    if int(task_count or 0) != int(disposition_rows or 0):
        raise RuntimeError(
            f"Batch {batch_id} task manifest is incomplete: "
            f"tasks={task_count} expected={disposition_rows}"
        )


async def claim_recompute_task(
    *,
    kline_writer: PostgresKlineWriter,
    batch_id: int,
    worker_id: str,
    shard_index: int,
    shard_count: int,
    lease_seconds: int,
    max_attempts: int,
) -> dict[str, Any] | None:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            with executable_batch as materialized (
                select batch.batch_id
                  from chan_c_full_recompute_batches batch
                  join chan_c_batches parent
                    on parent.id = batch.batch_id
                 where batch.batch_id = $1
                   and batch.status = 'running'
                   and parent.status = 'running'
                 for share of batch, parent
            ), candidate as (
                select task.batch_id, task.symbol_id, task.chan_level
                  from chan_c_full_recompute_tasks task
                  join executable_batch batch
                    on batch.batch_id = task.batch_id
                 where task.batch_id = $1
                   and task.eligible
                   and task.attempts < $6
                   and mod(task.shard_bucket::integer, $5) = $4
                   and (
                        task.status in ('pending', 'failed')
                        or (task.status = 'running' and task.lease_until <= now())
                   )
                 order by task.attempts, task.symbol_id, task.chan_level
                 for update of task skip locked
                 limit 1
            )
            update chan_c_full_recompute_tasks task
               set status = 'running',
                   attempts = task.attempts + 1,
                   worker_id = $2,
                   lease_version = task.lease_version + 1,
                   claim_token = md5(task.batch_id::text || ':' || task.symbol_id::text || ':' ||
                                     task.chan_level::text || ':' || (task.lease_version + 1)::text || ':' ||
                                     clock_timestamp()::text || ':' || random()::text),
                   lease_until = now() + ($3::integer * interval '1 second'),
                   lease_heartbeat_at = now(),
                   started_at = coalesce(task.started_at, now()),
                   last_error = null,
                   updated_at = now()
              from candidate
             where task.batch_id = candidate.batch_id
               and task.symbol_id = candidate.symbol_id
               and task.chan_level = candidate.chan_level
            returning task.*
            """,
            batch_id,
            worker_id,
            lease_seconds,
            shard_index,
            shard_count,
            max_attempts,
        )
    return dict(row) if row is not None else None


async def heartbeat_recompute_task(
    *,
    kline_writer: PostgresKlineWriter,
    task: Mapping[str, Any],
    lease_seconds: int,
) -> bool:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        result = await conn.execute(
            """
            with executable_batch as materialized (
                select batch.batch_id
                  from chan_c_full_recompute_batches batch
                  join chan_c_batches parent on parent.id = batch.batch_id
                 where batch.batch_id = $1
                   and batch.status = 'running'
                   and parent.status = 'running'
                 for share of batch, parent
            )
            update chan_c_full_recompute_tasks task
               set lease_until = now() + ($6::integer * interval '1 second'),
                   lease_heartbeat_at = now(), updated_at = now()
              from executable_batch batch
             where task.batch_id = batch.batch_id
               and task.symbol_id = $2 and task.chan_level = $3
               and task.status = 'running' and task.claim_token = $4
               and task.lease_version = $5 and task.lease_until > now()
            """,
            task["batch_id"],
            task["symbol_id"],
            task["chan_level"],
            task["claim_token"],
            task["lease_version"],
            lease_seconds,
        )
    return result.endswith(" 1")


async def fail_recompute_task(
    *,
    kline_writer: PostgresKlineWriter,
    task: Mapping[str, Any],
    error: str,
) -> bool:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        result = await conn.execute(
            """
            with executable_batch as materialized (
                select batch.batch_id
                  from chan_c_full_recompute_batches batch
                  join chan_c_batches parent on parent.id = batch.batch_id
                 where batch.batch_id = $1
                   and batch.status = 'running'
                   and parent.status = 'running'
                 for share of batch, parent
            )
            update chan_c_full_recompute_tasks task
               set status = 'failed', last_error = $6,
                   worker_id = null, claim_token = null, lease_until = null,
                   lease_heartbeat_at = null, updated_at = now()
              from executable_batch batch
             where task.batch_id = batch.batch_id
               and task.symbol_id = $2 and task.chan_level = $3
               and task.status = 'running' and task.claim_token = $4
               and task.lease_version = $5
            """,
            task["batch_id"],
            task["symbol_id"],
            task["chan_level"],
            task["claim_token"],
            task["lease_version"],
            error[:2000],
        )
    return result.endswith(" 1")


async def process_claimed_tasks(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    batch_id: int,
    modes: list[str],
    worker_id: str,
    shard_index: int,
    shard_count: int,
    lease_seconds: int,
    max_attempts: int,
    sleep: float,
    chan_py_path: str | None,
) -> dict[str, int]:
    runs = 0
    failures_observed = 0
    while True:
        task = await claim_recompute_task(
            kline_writer=kline_writer,
            batch_id=batch_id,
            worker_id=worker_id,
            shard_index=shard_index,
            shard_count=shard_count,
            lease_seconds=lease_seconds,
            max_attempts=max_attempts,
        )
        if task is None:
            break
        try:
            await process_claimed_task(
                kline_writer=kline_writer,
                chan_writer=chan_writer,
                task=task,
                modes=modes,
                lease_seconds=lease_seconds,
                chan_py_path=chan_py_path,
            )
            runs += 1
        except Exception as exc:
            failures_observed += 1
            await fail_recompute_task(kline_writer=kline_writer, task=task, error=str(exc))
            emit(
                "chan_module_c_task_failed",
                batch_id=batch_id,
                symbol=task["symbol"],
                level=DB_LEVEL_NAMES[int(task["chan_level"])],
                attempt=task["attempts"],
                error=str(exc)[:500],
            )
        if sleep > 0:
            await asyncio.sleep(sleep)
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        await conn.execute(
            """
            with executable_batch as materialized (
                select batch.batch_id
                  from chan_c_full_recompute_batches batch
                  join chan_c_batches parent on parent.id = batch.batch_id
                 where batch.batch_id = $1
                   and batch.status = 'running'
                   and parent.status = 'running'
                 for share of batch, parent
            )
            update chan_c_full_recompute_batches batch
               set status = case
                       when exists (
                           select 1 from chan_c_full_recompute_tasks task
                            where task.batch_id = batch.batch_id and task.status = 'failed'
                       ) then 'failed'
                       else 'completed'
                   end,
                   finished_at = now(), updated_at = now()
              from executable_batch executable
             where batch.batch_id = executable.batch_id
               and batch.status = 'running'
               and not exists (
                   select 1 from chan_c_full_recompute_tasks task
                    where task.batch_id = batch.batch_id
                      and task.status in ('pending', 'running')
               )
            """,
            batch_id,
        )
        failed = await conn.fetchval(
            """
            select count(*)
              from chan_c_full_recompute_tasks
             where batch_id = $1 and eligible and status = 'failed'
               and attempts >= $4 and mod(shard_bucket::integer, $3) = $2
            """,
            batch_id,
            shard_index,
            shard_count,
            max_attempts,
        )
    return {
        "runs": runs,
        "failed": int(failed or 0),
        "failures_observed": failures_observed,
    }


async def process_claimed_task(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    task: Mapping[str, Any],
    modes: list[str],
    lease_seconds: int,
    chan_py_path: str | None,
) -> None:
    level = DB_LEVEL_NAMES[int(task["chan_level"])]
    target = task["target_bar_until"]
    bars = [bar for bar in await kline_writer.get_bars(str(task["symbol"]), level) if bar.ts <= target]
    if not bars or bars[-1].ts != target:
        raise RuntimeError(
            f"Frozen cutoff unavailable for {task['symbol']} {level}: "
            f"expected={target!s} actual={bars[-1].ts if bars else None!s}"
        )

    stop_heartbeat = asyncio.Event()
    lease_lost = asyncio.Event()

    async def heartbeat_loop() -> None:
        interval = max(1.0, lease_seconds / 3)
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=interval)
            except TimeoutError:
                if not await heartbeat_recompute_task(
                    kline_writer=kline_writer, task=task, lease_seconds=lease_seconds
                ):
                    lease_lost.set()
                    return

    heartbeat = asyncio.create_task(heartbeat_loop())
    try:
        response = await compute_module_c_overlay(
            symbol=str(task["symbol"]),
            levels=[level],
            modes=modes,
            bars_by_level={level: bars},
            chan_py_path=chan_py_path,
        )
    finally:
        stop_heartbeat.set()
        await heartbeat
    if lease_lost.is_set() or not await heartbeat_recompute_task(
        kline_writer=kline_writer, task=task, lease_seconds=lease_seconds
    ):
        raise RuntimeError("Full-recompute task lease was lost before publication")

    validate_module_c_response(
        response=response,
        symbol=str(task["symbol"]),
        levels=[level],
        bars_by_level={level: bars},
    )
    counts = await chan_writer.replace_analysis(
        symbol=str(task["symbol"]),
        level=level,
        modes=modes,
        bar_from=bars[0].ts,
        bar_until=bars[-1].ts,
        bar_count=len(bars),
        response=filter_chan_response_level(response, level),
        full_recompute_task=task,
    )
    emit(
        "chan_module_c_task_completed",
        batch_id=task["batch_id"],
        symbol=task["symbol"],
        level=level,
        bars=len(bars),
        **counts,
    )


async def process_symbols_concurrently(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    symbols: list[str],
    levels: list[str],
    modes: list[str],
    concurrency: int,
    sleep: float,
    chan_py_path: str | None,
    prepare_native_bars: bool,
) -> dict[str, int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_symbol(symbol: str) -> dict[str, int]:
        async with semaphore:
            return await process_symbol(
                kline_writer=kline_writer,
                chan_writer=chan_writer,
                symbol=symbol,
                levels=levels,
                modes=modes,
                sleep=sleep,
                chan_py_path=chan_py_path,
                prepare_native_bars=prepare_native_bars,
            )

    results = await asyncio.gather(*(run_symbol(symbol) for symbol in symbols))
    return {
        "runs": sum(item["runs"] for item in results),
        "failed": sum(item["failed"] for item in results),
    }


async def process_symbol(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    symbol: str,
    levels: list[str],
    modes: list[str],
    sleep: float,
    chan_py_path: str | None,
    prepare_native_bars: bool,
) -> dict[str, int]:
    try:
        if prepare_native_bars:
            prepared = await prepare_native_bars_from_5f(
                kline_writer=kline_writer,
                symbol=symbol,
                levels=levels,
            )
            if prepared:
                emit("chan_module_c_native_bars_prepared", symbol=symbol, **prepared)
        bars_by_level = {
            level: await kline_writer.get_bars(symbol, level)
            for level in levels
        }
        missing = [level for level, bars in bars_by_level.items() if not bars]
        if missing:
            raise RuntimeError(f"Missing native K-lines for {symbol}: {', '.join(missing)}")

        response = await compute_module_c_overlay(
            symbol=symbol,
            levels=levels,
            modes=modes,
            bars_by_level=bars_by_level,
            chan_py_path=chan_py_path,
        )
        validate_module_c_response(
            response=response,
            symbol=symbol,
            levels=levels,
            bars_by_level=bars_by_level,
        )

        aggregate_counts = {"strokes": 0, "segments": 0, "centers": 0, "signals": 0}
        for level in levels:
            level_bars = bars_by_level[level]
            level_response = filter_chan_response_level(response, level)
            counts = await chan_writer.replace_analysis(
                symbol=symbol,
                level=level,
                modes=modes,
                bar_from=level_bars[0].ts,
                bar_until=level_bars[-1].ts,
                bar_count=len(level_bars),
                response=level_response,
            )
            for key in aggregate_counts:
                aggregate_counts[key] += counts.get(key, 0)

        emit(
            "chan_module_c_written",
            symbol=symbol,
            levels=levels,
            engine=response.get("engine"),
            bars_by_level={level: len(bars_by_level[level]) for level in levels},
            **aggregate_counts,
        )
        if sleep > 0:
            await asyncio.sleep(sleep)
        return {"runs": 1, "failed": 0}
    except Exception as exc:
        emit("chan_module_c_failed", symbol=symbol, levels=levels, error=str(exc)[:500])
        return {"runs": 0, "failed": 1}


async def prepare_native_bars_from_5f(
    *,
    kline_writer: PostgresKlineWriter,
    symbol: str,
    levels: list[str],
) -> dict[str, int]:
    targets = [level for level in levels if level in {"30f", "1d"}]
    if not targets:
        return {}
    source_bars = await kline_writer.get_bars(symbol, "5f")
    if not source_bars:
        raise RuntimeError(f"No stored 5f K-lines for native aggregation: {symbol}")
    result: dict[str, int] = {}
    for target in targets:
        aggregated = aggregate_from_5f(symbol=symbol, source_bars=source_bars, target_timeframe=target)
        if aggregated:
            result[target] = await kline_writer.upsert_bars(aggregated)
    return result


def aggregate_from_5f(
    *,
    symbol: str,
    source_bars: list[Bar],
    target_timeframe: str,
) -> list[Bar]:
    groups: dict[datetime, list[Bar]] = {}
    for bar in source_bars:
        target_end = _target_bar_end(bar.ts, target_timeframe)
        if target_end is None:
            continue
        groups.setdefault(target_end, []).append(bar)
    return [
        _aggregate_group(symbol=symbol, target_timeframe=target_timeframe, end_ts=end_ts, bars=groups[end_ts])
        for end_ts in sorted(groups)
        if groups[end_ts]
    ]


def _target_bar_end(ts: datetime, target_timeframe: str) -> datetime | None:
    local = ts.astimezone(CN_TZ)
    if target_timeframe == "1d":
        return local.replace(hour=15, minute=0, second=0, microsecond=0).astimezone(UTC)
    if target_timeframe != "30f":
        return None

    minute_of_day = local.hour * 60 + local.minute
    for session_start, session_end in ((9 * 60 + 30, 11 * 60 + 30), (13 * 60, 15 * 60)):
        if session_start < minute_of_day <= session_end:
            bucket_end = session_start + int(math.ceil((minute_of_day - session_start) / 30.0) * 30)
            if bucket_end > session_end:
                return None
            return local.replace(
                hour=bucket_end // 60,
                minute=bucket_end % 60,
                second=0,
                microsecond=0,
            ).astimezone(UTC)
    return None


def _aggregate_group(
    *,
    symbol: str,
    target_timeframe: str,
    end_ts: datetime,
    bars: list[Bar],
) -> Bar:
    ordered = sorted(bars, key=lambda item: item.ts)
    amount_values = [bar.amount for bar in ordered if bar.amount is not None]
    amount = sum(amount_values) if len(amount_values) == len(ordered) else None
    return Bar(
        symbol=symbol,
        timeframe=target_timeframe,
        ts=end_ts,
        open=ordered[0].open,
        high=max(bar.high for bar in ordered),
        low=min(bar.low for bar in ordered),
        close=ordered[-1].close,
        volume=sum(int(bar.volume or 0) for bar in ordered),
        amount=amount,
        complete=all(bar.complete for bar in ordered),
        revision=max(int(bar.revision or 0) for bar in ordered),
        source="derived_5f",
    )


async def compute_module_c_overlay(
    *,
    symbol: str,
    levels: list[str],
    modes: list[str],
    bars_by_level: dict[str, list[Any]],
    chan_py_path: str | None,
) -> dict[str, Any]:
    build_overlay = _load_module_c_overlay_builder()
    return await asyncio.to_thread(
        build_overlay,
        {
            "symbol": symbol,
            "timeframe": levels[0],
            "chan_levels": levels,
            "modes": modes,
            "bars_by_level": {
                level: [bar_to_chan_payload(bar) for bar in bars]
                for level, bars in bars_by_level.items()
            },
            "chan_py_path": chan_py_path,
        },
    )


def validate_module_c_response(
    *,
    response: dict[str, Any],
    symbol: str,
    levels: list[str],
    bars_by_level: dict[str, list[Any]],
) -> None:
    if response.get("engine") != "module-c:chan.py-native-levels":
        raise RuntimeError(f"Rejected non-module-C Chan engine for {symbol}: {response.get('engine') or 'unknown'}")
    for key in ("strokes", "segments", "centers", "signals", "channels"):
        if not isinstance(response.get(key), list):
            raise RuntimeError(f"Invalid Module C Chan response for {symbol}: {key} is not a list")
    for level in levels:
        bars = bars_by_level.get(level) or []
        if not bars:
            raise RuntimeError(f"Invalid Module C Chan response for {symbol}: missing {level} bars")
        range_start = int(bars[0].ts.timestamp())
        range_end = int(bars[-1].ts.timestamp())
        for part in ("strokes", "segments"):
            for index, item in enumerate(item for item in response[part] if item.get("level") == level):
                start_time = int(item["start"]["time"])
                end_time = int(item["end"]["time"])
                begin_base_ts = int(item.get("begin_base_ts") or start_time)
                end_base_ts = int(item.get("end_base_ts") or end_time)
                _validate_ts_range(begin_base_ts, range_start, range_end, symbol, level, part, index)
                _validate_ts_range(end_base_ts, range_start, range_end, symbol, level, part, index)
                if begin_base_ts > end_base_ts:
                    raise RuntimeError(f"Invalid Module C Chan response for {symbol} {level}: reversed {part}[{index}]")
        for index, item in enumerate(item for item in response["centers"] if item.get("level") == level):
            begin_base_ts = int(item.get("begin_base_ts") or item["start_time"])
            end_base_ts = int(item.get("end_base_ts") or item["end_time"])
            _validate_ts_range(begin_base_ts, range_start, range_end, symbol, level, "centers", index)
            _validate_ts_range(end_base_ts, range_start, range_end, symbol, level, "centers", index)
            if float(item["low"]) > float(item["high"]):
                raise RuntimeError(f"Invalid Module C Chan response for {symbol} {level}: center low > high")
        for index, item in enumerate(item for item in response["signals"] if item.get("level") == level):
            base_ts = int(item.get("base_ts") or item["time"])
            _validate_ts_range(base_ts, range_start, range_end, symbol, level, "signals", index)


def _validate_ts_range(
    value: int,
    range_start: int,
    range_end: int,
    symbol: str,
    level: str,
    part: str,
    index: int,
) -> None:
    if value < range_start or value > range_end:
        raise RuntimeError(
            f"Invalid Module C Chan response for {symbol} {level}: {part}[{index}] out of native bar range"
        )


@lru_cache(maxsize=1)
def _load_module_c_overlay_builder():
    adapter_path = Path(__file__).with_name("module_c_adapter.py")
    spec = importlib.util.spec_from_file_location("collector_module_c_adapter", adapter_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load Module C Chan adapter: {adapter_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.build_overlay


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
