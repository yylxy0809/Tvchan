from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from collector.chan_module_c_recompute import compute_module_c_overlay
from collector.historical_replay import (
    REPLAY_CONTRACT_VERSION,
    ReplayContract,
    claim_replay_task,
    ensure_replay_batch,
    fail_replay_task,
    heartbeat_replay_task,
    stable_replay_identity,
)
from collector.market_fill import filter_chan_response_level
from collector.storage.chan_postgres import MODULE_C_CHAN_TABLES, PostgresChanWriter
from collector.storage.postgres import PostgresKlineWriter, timeframe_to_db_code
from trading_protocol import MODULE_C_CONFIG_HASH


LEVEL_NAMES = {5: "5f", 30: "30f", 1440: "1d", 10080: "1w", 43200: "1m"}
DEFAULT_OUTPUT_DIR = Path("outputs/device-b-historical-replay-20260714")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Durable official Module C historical replay")
    parser.add_argument("action", choices=("prepare-canary", "prepare-full", "prepare-intraday", "work", "report"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--batch-id", type=int)
    parser.add_argument("--source-batch-id", type=int, default=6)
    parser.add_argument("--canary-source-batch-id", type=int, default=5)
    parser.add_argument("--batch-key", default="historical-replay-canary-20260714-v1")
    parser.add_argument("--run-group-id", default="historical-replay-canary-20260714-v1")
    parser.add_argument("--code-commit", default=os.getenv("GIT_COMMIT", "unknown"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--cutoffs-per-scope", type=int, default=2)
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--lease-seconds", type=int, default=900)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int)
    parser.add_argument("--task-limit", type=int, default=0)
    parser.add_argument("--chan-py-path", default=os.getenv("CHAN_PY_PATH"))
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    if args.action in {"prepare-intraday", "work", "report"} and not args.batch_id:
        parser.error("--batch-id is required for prepare-intraday, work and report")
    if args.cutoffs_per_scope < 1:
        parser.error("--cutoffs-per-scope must be positive")
    if (args.shard_index is None) != (args.shard_count is None):
        parser.error("--shard-index and --shard-count must be provided together")
    if args.shard_count is not None and not 0 <= args.shard_index < args.shard_count:
        parser.error("--shard-index must be between zero and --shard-count minus one")
    return args


async def create_parent_batch(
    conn: asyncpg.Connection,
    *,
    source_batch_id: int,
    batch_key: str,
    run_group_id: str,
    code_commit: str,
) -> int:
    batch_id = await conn.fetchval(
        """
        insert into chan_c_batches (
            batch_key, publication_namespace, profile_id, run_group_id, batch_kind,
            status, code_commit, image_digest, vendor_manifest_sha256, effective_config,
            config_hash, eligible_manifest_uri, eligible_manifest_sha256,
            input_watermark, audit_references, notes
        )
        select $2, 'historical-replay', 'module-c-historical-replay-v1', $3,
               'historical_replay', 'planned', $4, source.image_digest,
               source.vendor_manifest_sha256,
               source.effective_config || jsonb_build_object(
                   'replay_contract_version', $5::text,
                   'source_batch_id', $1::bigint,
                   'worker_limit', 2,
                   'concurrency_per_worker', 1
               ),
               source.config_hash, source.eligible_manifest_uri,
               source.eligible_manifest_sha256, source.input_watermark,
               source.audit_references || jsonb_build_array(
                   jsonb_build_object('type', 'source_batch', 'batch_id', $1::bigint)
               ),
               'Official historical replay canary; isolated from baseline and online heads.'
          from chan_c_batches source
         where source.id = $1 and source.status = 'sealed'
        on conflict (batch_key) do update
            set code_commit = excluded.code_commit
          where chan_c_batches.status = 'planned'
        returning id
        """,
        source_batch_id,
        batch_key,
        run_group_id,
        code_commit,
        REPLAY_CONTRACT_VERSION,
    )
    if batch_id is None:
        raise RuntimeError(f"Sealed source batch {source_batch_id} was not found")
    return int(batch_id)


async def load_contract(conn: asyncpg.Connection, *, source_batch_id: int) -> ReplayContract:
    row = await conn.fetchrow(
        """
        select source.config_hash,
               recompute.eligibility_build_id::text as eligibility_snapshot,
               eligibility.manifest_hash as canonical_snapshot,
               max(task.target_bar_until) as cutoff_time
          from chan_c_batches source
          join chan_c_full_recompute_batches recompute on recompute.batch_id = source.id
          join module_c_eligibility_builds eligibility on eligibility.build_id = recompute.eligibility_build_id
          join chan_c_full_recompute_tasks task on task.batch_id = source.id and task.eligible
         where source.id = $1 and source.status = 'sealed'
         group by source.config_hash, recompute.eligibility_build_id, eligibility.manifest_hash
        """,
        source_batch_id,
    )
    if row is None or row["cutoff_time"] is None:
        raise RuntimeError(f"Source batch {source_batch_id} lacks a frozen eligible snapshot")
    return ReplayContract(
        config_hash=str(row["config_hash"]),
        source_batch_id=source_batch_id,
        eligible_universe_snapshot_id=str(row["eligibility_snapshot"]),
        canonical_gate_snapshot_id=str(row["canonical_snapshot"]),
        cutoff_time=row["cutoff_time"],
    )


async def seed_canary_tasks(
    conn: asyncpg.Connection,
    *,
    batch_id: int,
    source_batch_id: int,
    canary_source_batch_id: int,
    contract: ReplayContract,
    cutoffs_per_scope: int,
) -> int:
    rows = await conn.fetch(
        """
        with canary as (
            select distinct symbol_id, symbol
              from chan_c_full_recompute_tasks
             where batch_id = $2
        ), source as (
            select eligibility_build_id from chan_c_full_recompute_batches where batch_id = $1
        )
        select canary.symbol_id, canary.symbol, eligibility.timeframe as chan_level,
               eligibility.eligible, eligibility.reasons as exclusion_reasons,
               eligibility.covered_until,
               cutoff.ts as cutoff_time
          from canary
          cross join source
          join module_c_eligibility eligibility
            on eligibility.build_id = source.eligibility_build_id
           and eligibility.symbol_id = canary.symbol_id
          left join lateral (
              select k.ts
                from klines k
               where k.symbol_id = canary.symbol_id
                 and k.timeframe = eligibility.timeframe
                 and k.is_complete
                 and k.ts <= eligibility.covered_until
               order by k.ts desc
               limit $3
          ) cutoff on eligibility.eligible
         order by canary.symbol, eligibility.timeframe, cutoff.ts
        """,
        source_batch_id,
        canary_source_batch_id,
        cutoffs_per_scope,
    )
    manifests: list[tuple[Any, ...]] = []
    for row in rows:
        level = LEVEL_NAMES[int(row["chan_level"])]
        cutoff = row["cutoff_time"] or row["covered_until"] or contract.cutoff_time
        eligible = bool(row["eligible"] and row["cutoff_time"] is not None)
        reasons = list(row["exclusion_reasons"] or [])
        if row["eligible"] and row["cutoff_time"] is None:
            reasons.append("no_complete_native_bar_at_or_before_frozen_cutoff")
        identity = stable_replay_identity(
            contract,
            symbol=str(row["symbol"]),
            level=level,
            mode="confirmed,predictive",
            cutoff_time=cutoff,
        )
        manifests.append(
            (
                batch_id, int(row["symbol_id"]), str(row["symbol"]), int(row["chan_level"]),
                "confirmed,predictive", cutoff, contract.contract_version, identity, eligible,
                reasons, "pending" if eligible else "excluded",
            )
        )
    await conn.executemany(
        """
        insert into chan_c_historical_replay_tasks (
            batch_id, symbol_id, symbol, chan_level, mode, cutoff_time,
            contract_version, replay_identity, eligible, exclusion_reasons, status
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        on conflict (batch_id, symbol_id, chan_level, mode, cutoff_time, contract_version) do nothing
        """,
        manifests,
    )
    return int(await conn.fetchval("select count(*) from chan_c_historical_replay_tasks where batch_id=$1", batch_id))


async def seed_full_high_level_tasks(
    conn: asyncpg.Connection,
    *,
    batch_id: int,
    source_batch_id: int,
    contract: ReplayContract,
    cutoffs_per_scope: int,
) -> int:
    rows = await conn.fetch(
        """
        with source as (
            select eligibility_build_id from chan_c_full_recompute_batches where batch_id = $1
        )
        select eligibility.symbol_id, eligibility.symbol, eligibility.timeframe as chan_level,
               eligibility.eligible, eligibility.reasons as exclusion_reasons,
               eligibility.covered_until, cutoff.ts as cutoff_time
          from source
          join module_c_eligibility eligibility on eligibility.build_id = source.eligibility_build_id
          left join lateral (
              select k.ts
                from klines k
               where k.symbol_id = eligibility.symbol_id
                 and k.timeframe = eligibility.timeframe
                 and k.is_complete and k.ts <= eligibility.covered_until
               order by k.ts desc
               limit $2
          ) cutoff on eligibility.eligible and eligibility.timeframe in (1440, 10080, 43200)
         where not eligibility.eligible or eligibility.timeframe in (1440, 10080, 43200)
         order by eligibility.symbol, eligibility.timeframe, cutoff.ts
        """,
        source_batch_id,
        cutoffs_per_scope,
    )
    manifests: list[tuple[Any, ...]] = []
    for row in rows:
        level = LEVEL_NAMES[int(row["chan_level"])]
        cutoff = row["cutoff_time"] or row["covered_until"] or contract.cutoff_time
        eligible = bool(row["eligible"] and row["cutoff_time"] is not None)
        reasons = list(row["exclusion_reasons"] or [])
        if row["eligible"] and row["cutoff_time"] is None:
            reasons.append("no_complete_native_bar_at_or_before_frozen_cutoff")
        manifests.append((
            batch_id, int(row["symbol_id"]), str(row["symbol"]), int(row["chan_level"]),
            "confirmed,predictive", cutoff, contract.contract_version,
            stable_replay_identity(
                contract, symbol=str(row["symbol"]), level=level,
                mode="confirmed,predictive", cutoff_time=cutoff,
            ),
            eligible, reasons, "pending" if eligible else "excluded",
        ))
    await conn.executemany(
        """
        insert into chan_c_historical_replay_tasks (
            batch_id, symbol_id, symbol, chan_level, mode, cutoff_time,
            contract_version, replay_identity, eligible, exclusion_reasons, status
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        on conflict (batch_id, symbol_id, chan_level, mode, cutoff_time, contract_version) do nothing
        """,
        manifests,
    )
    return int(await conn.fetchval("select count(*) from chan_c_historical_replay_tasks where batch_id=$1", batch_id))


async def seed_intraday_strategy_tasks(
    conn: asyncpg.Connection,
    *,
    batch_id: int,
    source_batch_id: int,
    contract: ReplayContract,
) -> dict[str, Any]:
    """Expand native 5F/30F bars only inside causal official strategy windows."""
    row = await conn.fetchrow(
        """
        with eligible_levels as materialized (
            select eligibility.symbol_id, eligibility.symbol,
                   eligibility.timeframe, eligibility.covered_until
              from module_c_eligibility eligibility
             where eligibility.build_id = (
                       select eligibility_build_id
                         from chan_c_full_recompute_batches
                        where batch_id = $1
                   )
               and eligibility.timeframe in (5, 30)
               and eligibility.eligible
        ), eligible_symbols as materialized (
            select symbol_id
              from eligible_levels
             group by symbol_id
            having count(distinct timeframe) = 2
        ), signals as materialized (
            select identity.fingerprint, identity.symbol_id, identity.chan_level,
                   identity.bsp_type, identity.point_time, identity.price_x1000,
                   event.effective_time
              from chan_structure_lifecycle_events event
              join chan_structure_identity identity using (fingerprint)
              join eligible_symbols eligible on eligible.symbol_id = identity.symbol_id
             where event.event_type = 'first_seen'
               and event.provenance->>'publication_profile' = 'historical_replay'
               and identity.structure_type = 'signal'
               and identity.side_or_direction = 'buy'
               and identity.chan_level in (1440, 10080)
               and event.effective_time <= $2
        ), windows as materialized (
            select daily.fingerprint as daily_setup_fingerprint,
                   daily.symbol_id,
                   daily.effective_time as start_time,
                   least(daily.effective_time + interval '5 days', $2::timestamptz) as end_time
              from signals daily
             where daily.chan_level = 1440
               and daily.bsp_type in ('2', '2s')
               and exists (
                   select 1 from signals daily_b1
                    where daily_b1.symbol_id = daily.symbol_id
                      and daily_b1.chan_level = 1440
                      and daily_b1.bsp_type = '1'
                      and daily_b1.point_time < daily.point_time
                      and daily_b1.price_x1000 < daily.price_x1000
                      and daily_b1.effective_time <= daily.effective_time
               )
               and exists (
                   select 1 from signals weekly_b2
                    where weekly_b2.symbol_id = daily.symbol_id
                      and weekly_b2.chan_level = 10080
                      and weekly_b2.bsp_type = '2'
                      and weekly_b2.effective_time <= daily.effective_time
                      and exists (
                          select 1 from signals weekly_b1
                           where weekly_b1.symbol_id = weekly_b2.symbol_id
                             and weekly_b1.chan_level = 10080
                             and weekly_b1.bsp_type = '1'
                             and weekly_b1.point_time < weekly_b2.point_time
                             and weekly_b1.price_x1000 < weekly_b2.price_x1000
                             and weekly_b1.effective_time <= weekly_b2.effective_time
                      )
               )
        ), tasks as materialized (
            select distinct level.symbol_id, level.symbol,
                   level.timeframe as chan_level, kline.ts as cutoff_time
              from eligible_levels level
              join windows strategy_window on strategy_window.symbol_id = level.symbol_id
              join klines kline
                on kline.symbol_id = level.symbol_id
               and kline.timeframe = level.timeframe
               and kline.is_complete
               and kline.ts >= strategy_window.start_time
               and kline.ts <= strategy_window.end_time
               and kline.ts <= level.covered_until
        )
        select coalesce(
                   (select jsonb_agg(to_jsonb(strategy_window) order by strategy_window.symbol_id, strategy_window.start_time, strategy_window.daily_setup_fingerprint)
                      from windows strategy_window),
                   '[]'::jsonb
               ) as windows,
               coalesce(
                   (select jsonb_agg(to_jsonb(task) order by task.symbol_id, task.chan_level, task.cutoff_time)
                      from tasks task),
                   '[]'::jsonb
               ) as tasks
        """,
        source_batch_id,
        contract.cutoff_time,
    )
    windows = row["windows"]
    tasks = row["tasks"]
    if isinstance(windows, str):
        windows = json.loads(windows)
    if isinstance(tasks, str):
        tasks = json.loads(tasks)
    manifests = []
    for task in tasks:
        cutoff = datetime.fromisoformat(str(task["cutoff_time"]).replace("Z", "+00:00"))
        level = LEVEL_NAMES[int(task["chan_level"])]
        manifests.append((
            batch_id, int(task["symbol_id"]), str(task["symbol"]), int(task["chan_level"]),
            "confirmed,predictive", cutoff, contract.contract_version,
            stable_replay_identity(
                contract, symbol=str(task["symbol"]), level=level,
                mode="confirmed,predictive", cutoff_time=cutoff,
            ),
            True, [], "pending",
        ))
    if manifests:
        await conn.executemany(
            """
            insert into chan_c_historical_replay_tasks (
                batch_id, symbol_id, symbol, chan_level, mode, cutoff_time,
                contract_version, replay_identity, eligible, exclusion_reasons, status
            ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            on conflict (batch_id, symbol_id, chan_level, mode, cutoff_time, contract_version) do nothing
            """,
            manifests,
        )
    planned = int(await conn.fetchval(
        "select count(*) from chan_c_historical_replay_tasks where batch_id=$1 and chan_level in (5,30) and eligible",
        batch_id,
    ))
    return {
        "contract": "weekly_daily_b2_official_v1 causal 5-day forward window",
        "episode_count": len(windows),
        "symbol_count": len({int(window["symbol_id"]) for window in windows}),
        "planned_task_count": planned,
        "windows": windows,
    }


def assert_no_future_output(response: Mapping[str, Any], *, cutoff_time: datetime) -> None:
    cutoff_epoch = int(cutoff_time.timestamp())
    checks = (
        ("strokes", ("end_base_ts",), ("end", "time")),
        ("segments", ("end_base_ts",), ("end", "time")),
        ("centers", ("end_base_ts", "end_time"), None),
        ("signals", ("base_ts", "time"), None),
    )
    for object_type, direct_keys, nested in checks:
        for item in response.get(object_type, []):
            value = next((item.get(key) for key in direct_keys if item.get(key) is not None), None)
            if value is None and nested is not None:
                value = (item.get(nested[0]) or {}).get(nested[1])
            if value is not None and int(value) > cutoff_epoch:
                raise RuntimeError(f"future_data_leak:{object_type}:{value}>{cutoff_epoch}")


async def load_scope_bars(
    kline_writer: PostgresKlineWriter,
    *,
    symbol: str,
    level: str,
    bars_cache: dict[tuple[str, str], list[Any]] | None,
) -> list[Any]:
    cache_key = (symbol, level)
    all_bars = bars_cache.get(cache_key) if bars_cache is not None else None
    if all_bars is None:
        all_bars = await kline_writer.get_bars(symbol, level)
        if bars_cache is not None:
            bars_cache.clear()
            bars_cache[cache_key] = all_bars
    return all_bars


async def process_task(
    *,
    kline_writer: PostgresKlineWriter,
    chan_writer: PostgresChanWriter,
    task: Mapping[str, Any],
    lease_seconds: int,
    chan_py_path: str | None,
    bars_cache: dict[tuple[str, str], list[Any]] | None = None,
) -> None:
    level = LEVEL_NAMES[int(task["chan_level"])]
    cutoff = task["cutoff_time"].astimezone(UTC)
    all_bars = await load_scope_bars(
        kline_writer, symbol=str(task["symbol"]), level=level, bars_cache=bars_cache,
    )
    bars = [
        bar for bar in all_bars
        if bar.complete and bar.ts <= cutoff
    ]
    if not bars:
        raise RuntimeError("No complete native bars at or before replay cutoff")
    if bars[-1].ts != cutoff:
        raise RuntimeError(f"Replay cutoff is not a visible native bar end: {bars[-1].ts} != {cutoff}")
    response = await compute_module_c_overlay(
        symbol=str(task["symbol"]), levels=[level], modes=["confirmed", "predictive"],
        bars_by_level={level: bars}, chan_py_path=chan_py_path,
    )
    response = filter_chan_response_level(response, level)
    response["snapshot_version"] = f"historical-replay:{task['replay_identity']}"
    assert_no_future_output(response, cutoff_time=cutoff)
    if not await heartbeat_replay_task(kline_writer=kline_writer, task=task, lease_seconds=lease_seconds):
        raise RuntimeError("Historical replay lease was lost before publication")
    await chan_writer.replace_analysis(
        symbol=str(task["symbol"]), level=level, modes=["confirmed", "predictive"],
        bar_from=bars[0].ts, bar_until=cutoff, bar_count=len(bars), response=response,
        historical_replay_task=dict(task),
    )


async def work(args: argparse.Namespace) -> None:
    worker_id = args.worker_id or f"historical-replay-{uuid.uuid4().hex[:12]}"
    lookup = await asyncpg.connect(args.database_url)
    try:
        run_group = await lookup.fetchval("select run_group_id from chan_c_batches where id=$1", args.batch_id)
    finally:
        await lookup.close()
    if not run_group:
        raise RuntimeError(f"Unknown replay batch {args.batch_id}")
    processed = 0
    bars_cache: dict[tuple[str, str], list[Any]] = {}
    async with PostgresKlineWriter(args.database_url, pool_min_size=1, pool_max_size=1) as kline_writer:
        async with PostgresChanWriter(
            args.database_url, pool_min_size=1, pool_max_size=1, tables=MODULE_C_CHAN_TABLES,
            run_config_hash=MODULE_C_CONFIG_HASH, native_base_timeframe=True,
            publication_profile="historical_replay", publication_source="historical_replay",
            run_kind="historical_replay", batch_id=args.batch_id,
            publication_namespace="historical-replay", profile_id="module-c-historical-replay-v1",
            run_group_id=str(run_group), worker_id=worker_id,
        ) as chan_writer:
            while args.task_limit <= 0 or processed < args.task_limit:
                task = await claim_replay_task(
                    kline_writer=kline_writer, batch_id=args.batch_id, worker_id=worker_id,
                    lease_seconds=args.lease_seconds, max_attempts=args.max_attempts,
                    shard_index=args.shard_index, shard_count=args.shard_count,
                )
                if task is None:
                    break
                try:
                    await process_task(
                        kline_writer=kline_writer, chan_writer=chan_writer, task=task,
                        lease_seconds=args.lease_seconds, chan_py_path=args.chan_py_path,
                        bars_cache=bars_cache,
                    )
                except Exception as error:
                    await fail_replay_task(kline_writer=kline_writer, task=task, error=error)
                processed += 1
    print(json.dumps({"batch_id": args.batch_id, "worker_id": worker_id, "processed": processed}, sort_keys=True))


async def build_report(conn: asyncpg.Connection, *, batch_id: int) -> dict[str, Any]:
    status_rows = await conn.fetch(
        """
        select chan_level, status, count(*)::int count,
               coalesce(sum(bar_count),0)::bigint bars,
               coalesce(sum(stroke_count),0)::bigint strokes,
               coalesce(sum(segment_count),0)::bigint segments,
               coalesce(sum(center_count),0)::bigint centers,
               coalesce(sum(signal_count),0)::bigint signals
          from chan_c_historical_replay_tasks where batch_id=$1
         group by chan_level,status order by chan_level,status
        """,
        batch_id,
    )
    failures = await conn.fetch(
        """select id,symbol,chan_level,cutoff_time,attempts,last_error,failure
             from chan_c_historical_replay_tasks where batch_id=$1 and status='failed' order by id""",
        batch_id,
    )
    heads = int(await conn.fetchval("select count(*) from chan_c_historical_replay_heads where batch_id=$1", batch_id))
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "batch_id": batch_id,
        "status_by_level": [dict(row) for row in status_rows],
        "historical_heads": heads,
        "failure_count": len(failures),
        "failures": [dict(row) for row in failures],
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False, dir=path.parent)
    temporary = Path(handle.name)
    try:
        with handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def main(args: argparse.Namespace) -> None:
    if args.action == "prepare-intraday":
        conn = await asyncpg.connect(args.database_url)
        try:
            contract = await load_contract(conn, source_batch_id=args.source_batch_id)
            plan = await seed_intraday_strategy_tasks(
                conn,
                batch_id=args.batch_id,
                source_batch_id=args.source_batch_id,
                contract=contract,
            )
            write_json(args.output_dir / "strategy_forward_windows.json", plan)
            print(json.dumps({key: plan[key] for key in ("episode_count", "symbol_count", "planned_task_count")}, sort_keys=True))
        finally:
            await conn.close()
        return
    if args.action in {"prepare-canary", "prepare-full"}:
        conn = await asyncpg.connect(args.database_url)
        try:
            contract = await load_contract(conn, source_batch_id=args.source_batch_id)
            full = args.action == "prepare-full"
            batch_key = args.batch_key if not full or args.batch_key != "historical-replay-canary-20260714-v1" else "historical-replay-full-20260714-v1"
            run_group_id = args.run_group_id if not full or args.run_group_id != "historical-replay-canary-20260714-v1" else "historical-replay-full-20260714-v1"
            batch_id = await create_parent_batch(
                conn, source_batch_id=args.source_batch_id, batch_key=batch_key,
                run_group_id=run_group_id, code_commit=args.code_commit,
            )
            writer = type("Writer", (), {"_pool": type("Pool", (), {"acquire": lambda _self: _ConnectionAcquire(conn)})()})()
            await ensure_replay_batch(kline_writer=writer, batch_id=batch_id, contract=contract)
            if full:
                task_count = await seed_full_high_level_tasks(
                    conn, batch_id=batch_id, source_batch_id=args.source_batch_id,
                    contract=contract, cutoffs_per_scope=args.cutoffs_per_scope,
                )
            else:
                task_count = await seed_canary_tasks(
                    conn, batch_id=batch_id, source_batch_id=args.source_batch_id,
                    canary_source_batch_id=args.canary_source_batch_id, contract=contract,
                    cutoffs_per_scope=args.cutoffs_per_scope,
                )
            write_json(args.output_dir / "replay_contract.json", {**contract.payload(), "contract_hash": contract.digest(), "batch_id": batch_id})
            summary = await build_report(conn, batch_id=batch_id)
            summary.update({
                "scope": "full-market-high-level" if full else "20-symbol-canary",
                "task_count": task_count,
                "cutoffs_per_eligible_scope": args.cutoffs_per_scope,
                "deferred_levels": ["5f", "30f"] if full else [],
                "deferred_reason": "awaiting_causal_official_strategy_forward_windows" if full else None,
            })
            write_json(args.output_dir / "cutoff_grid_summary.json", summary)
            print(json.dumps({"batch_id": batch_id, "task_count": task_count}, sort_keys=True))
        finally:
            await conn.close()
        return
    if args.action == "work":
        await work(args)
        return
    conn = await asyncpg.connect(args.database_url)
    try:
        report = await build_report(conn, batch_id=args.batch_id)
        write_json(args.output_dir / "replay_batch_manifest.json", report)
        with (args.output_dir / "replay_failures.jsonl").open("w", encoding="utf-8") as handle:
            for failure in report["failures"]:
                handle.write(json.dumps(failure, ensure_ascii=False, default=str) + "\n")
        print(json.dumps({key: report[key] for key in ("batch_id", "historical_heads", "failure_count")}, sort_keys=True))
    finally:
        await conn.close()


class _ConnectionAcquire:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def __aenter__(self) -> asyncpg.Connection:
        return self.connection

    async def __aexit__(self, *_args: Any) -> None:
        return None


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
