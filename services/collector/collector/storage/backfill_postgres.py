from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
import json
from typing import Any
from uuid import UUID

from collector.storage.postgres import timeframe_to_db_code
from trading_protocol import SymbolInfo


async def _set_scoped_session_fence(conn, run_id: UUID | None) -> None:
    if run_id is not None:
        await conn.execute(
            "select set_config('tvchan.history_backfill_scoped_run_id', $1, true)",
            str(run_id),
        )


class PostgresBackfillTaskStore:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pool = None

    async def __aenter__(self) -> "PostgresBackfillTaskStore":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for historical backfill tasks."
            ) from exc
        self._pool = await asyncpg.create_pool(self.database_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def ensure_tasks(
        self,
        *,
        symbols: Iterable[SymbolInfo],
        timeframes: Iterable[str],
        provider: str,
        page_size: int,
        reset: bool = False,
    ) -> list[int]:
        assert self._pool is not None
        rows = [
            (symbol.symbol, timeframe_to_db_code(timeframe), provider, page_size)
            for symbol in symbols
            for timeframe in timeframes
        ]
        if not rows:
            return []
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if reset:
                    await conn.executemany(
                        """
                        insert into historical_backfill_tasks (
                        symbol_id,
                        timeframe,
                        provider,
                        page_size,
                        next_offset,
                        status,
                        pages_done,
                        bars_read,
                        bars_written,
                        oldest_ts,
                        newest_ts,
                        started_at,
                        last_run_at,
                        finished_at,
                        last_error
                    )
                    select
                        s.id,
                        $2,
                        $3,
                        $4,
                        0,
                        'pending',
                        0,
                        0,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                    from symbols s
                    where (s.code || '.' || s.exchange) = $1
                    on conflict (symbol_id, timeframe, provider) where run_id is null do update
                    set page_size = excluded.page_size,
                        next_offset = 0,
                        status = 'pending',
                        worker_id = null,
                        claim_token = null,
                        lease_until = null,
                        lease_heartbeat_at = null,
                        attempts = 0,
                        pages_done = 0,
                        bars_read = 0,
                        bars_written = 0,
                        oldest_ts = null,
                        newest_ts = null,
                        started_at = null,
                        last_run_at = null,
                        finished_at = null,
                        last_error = null,
                        updated_at = now()
                    where historical_backfill_tasks.status <> 'running'
                       or historical_backfill_tasks.lease_until <= clock_timestamp()
                        """,
                        rows,
                    )
                else:
                    await conn.executemany(
                        """
                        insert into historical_backfill_tasks (
                        symbol_id,
                        timeframe,
                        provider,
                        page_size
                    )
                    select s.id, $2, $3, $4
                    from symbols s
                    where (s.code || '.' || s.exchange) = $1
                    on conflict (symbol_id, timeframe, provider) where run_id is null do update
                    set page_size = excluded.page_size,
                        updated_at = now()
                    where historical_backfill_tasks.status in ('pending', 'failed')
                        """,
                        rows,
                    )
                task_ids = await conn.fetch(
                    """
                    select task.id
                    from historical_backfill_tasks task
                    join symbols symbol on symbol.id = task.symbol_id
                    where (symbol.code || '.' || symbol.exchange) = any($1::text[])
                      and task.timeframe = any($2::integer[])
                      and task.provider = $3
                      and task.run_id is null
                    order by task.id
                    """,
                    sorted({row[0] for row in rows}),
                    sorted({row[1] for row in rows}),
                    provider,
                )
        return [int(row["id"]) for row in task_ids]

    async def ensure_scoped_run_tasks(
        self,
        *,
        run_id: UUID,
        run_identity: str,
        manifest_sha256: str,
        symbols: Iterable[SymbolInfo],
        timeframes: Iterable[str],
        stop_at: dict[str, datetime],
        provider: str,
        page_size: int,
        endpoint: str,
        source_policy: str,
    ) -> list[int]:
        """Create or resume one immutable scoped run from active canonical DB scopes."""
        assert self._pool is not None
        symbol_names = [symbol.symbol for symbol in symbols]
        timeframe_names = list(timeframes)
        timeframe_codes = [timeframe_to_db_code(value) for value in timeframe_names]
        expected_tasks = len(symbol_names) * len(timeframe_codes)
        if not symbol_names or not timeframe_codes or expected_tasks <= 0:
            raise ValueError("scoped backfill requires symbols and timeframes")
        stop_at_json = {
            timeframe: stop_at[timeframe].isoformat()
            for timeframe in sorted(timeframe_names)
        }
        stop_at_payload = json.dumps(stop_at_json, sort_keys=True, separators=(",", ":"))

        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="serializable"):
                control = await conn.fetchrow(
                    """
                    select control.active_generation_id, control.revision
                    from kline_scope_catalog_control control
                    join kline_scope_catalog_generations generation
                      on generation.generation_id = control.active_generation_id
                     and generation.status = 'complete'
                    where control.control_key = 'active'
                    for share of control, generation
                    """
                )
                if control is None:
                    raise RuntimeError("active complete K-line scope catalog is unavailable")
                authoritative = await conn.fetch(
                    """
                    select symbol.id, symbol.code || '.' || symbol.exchange as symbol,
                           catalog.timeframe
                    from symbols symbol
                    join kline_scope_catalog catalog
                      on catalog.generation_id = $1
                     and catalog.symbol_id = symbol.id
                     and catalog.timeframe = any($3::integer[])
                     and catalog.bounds_complete
                     and catalog.state in ('present', 'empty')
                    where (symbol.code || '.' || symbol.exchange) = any($2::text[])
                      and symbol.is_active
                    order by symbol.id, catalog.timeframe
                    """,
                    control["active_generation_id"],
                    symbol_names,
                    timeframe_codes,
                )
                actual_scopes = {
                    (str(row["symbol"]), int(row["timeframe"])) for row in authoritative
                }
                expected_scopes = {
                    (symbol, timeframe)
                    for symbol in symbol_names
                    for timeframe in timeframe_codes
                }
                if actual_scopes != expected_scopes:
                    missing = sorted(expected_scopes - actual_scopes)
                    raise RuntimeError(
                        "scoped backfill contains inactive, unknown, or non-authoritative "
                        f"scopes: {missing[:10]}"
                    )

                await conn.execute(
                    """
                    insert into historical_backfill_scoped_runs (
                        run_id, run_identity, provider, manifest_sha256, page_size,
                        endpoint, source_policy, catalog_generation_id,
                        catalog_revision, symbol_count, timeframes, stop_at, task_count
                    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
                    on conflict (run_id) do nothing
                    """,
                    run_id, run_identity, provider, manifest_sha256, page_size,
                    endpoint, source_policy, control["active_generation_id"],
                    int(control["revision"]), len(symbol_names), timeframe_codes,
                    stop_at_payload, expected_tasks,
                )
                durable_run = await conn.fetchrow(
                    """
                    select run_identity, provider, manifest_sha256, page_size, endpoint,
                           source_policy, catalog_generation_id, catalog_revision,
                           symbol_count, timeframes, stop_at, task_count
                    from historical_backfill_scoped_runs
                    where run_id = $1
                    for share
                    """,
                    run_id,
                )
                expected_run = (
                    run_identity, provider, manifest_sha256, page_size, endpoint,
                    source_policy, control["active_generation_id"],
                    int(control["revision"]), len(symbol_names), timeframe_codes,
                    stop_at_json, expected_tasks,
                )
                actual_run = (
                    str(durable_run["run_identity"]), str(durable_run["provider"]),
                    str(durable_run["manifest_sha256"]), int(durable_run["page_size"]),
                    str(durable_run["endpoint"]), str(durable_run["source_policy"]),
                    durable_run["catalog_generation_id"],
                    int(durable_run["catalog_revision"]),
                    int(durable_run["symbol_count"]), list(durable_run["timeframes"]),
                    json.loads(str(durable_run["stop_at"])),
                    int(durable_run["task_count"]),
                )
                if actual_run != expected_run:
                    raise RuntimeError("scoped backfill durable run identity mismatch")

                for timeframe, timeframe_code in zip(timeframe_names, timeframe_codes):
                    await conn.execute(
                        """
                        insert into historical_backfill_tasks (
                            run_id, stop_at, symbol_id, timeframe, provider, page_size
                        )
                        select $1, $2, symbol.id, $3, $4, $5
                        from symbols symbol
                        where (symbol.code || '.' || symbol.exchange) = any($6::text[])
                          and symbol.is_active
                        on conflict (run_id, symbol_id, timeframe, provider)
                            where run_id is not null do nothing
                        """,
                        run_id, stop_at[timeframe], timeframe_code, provider, page_size,
                        symbol_names,
                    )
                rows = await conn.fetch(
                    """
                    select task.id
                    from historical_backfill_tasks task
                    where task.run_id = $1
                    order by task.id
                    """,
                    run_id,
                )
                task_ids = [int(row["id"]) for row in rows]
                if len(task_ids) != expected_tasks:
                    raise RuntimeError(
                        "scoped backfill task cardinality mismatch: "
                        f"expected {expected_tasks}, got {len(task_ids)}"
                    )
                return task_ids

    async def reset_running(
        self,
        *,
        provider: str,
        task_ids: list[int] | None = None,
        run_id: UUID | None = None,
    ) -> int:
        """Release only expired owners; a live lease is never operator-reset."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _set_scoped_session_fence(conn, run_id)
                result = await conn.execute(
                """
                update historical_backfill_tasks
                set status = case
                        when attempts + 1 >= max_attempts then 'dead_letter'
                        else 'pending'
                    end,
                    attempts = attempts + 1,
                    worker_id = null,
                    claim_token = null,
                    lease_until = null,
                    lease_heartbeat_at = null,
                    last_error = case
                        when attempts + 1 >= max_attempts
                            then coalesce(last_error, 'maximum attempts exhausted')
                        else null
                    end,
                    finished_at = case
                        when attempts + 1 >= max_attempts then clock_timestamp()
                        else finished_at
                    end,
                    updated_at = clock_timestamp()
                where provider = $1
                  and run_id is not distinct from $2::uuid
                  and ($3::bigint[] is null or id = any($3::bigint[]))
                  and status = 'running'
                  and lease_until <= clock_timestamp()
                """,
                    provider,
                    run_id,
                    task_ids,
                )
        return int(result.split()[-1])

    async def claim_tasks(
        self,
        *,
        provider: str,
        limit: int,
        worker_id: str,
        lease_seconds: int,
        max_attempts: int,
        task_ids: list[int] | None = None,
        run_id: UUID | None = None,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _set_scoped_session_fence(conn, run_id)
                rows = await conn.fetch(
                """
                with exhausted as (
                    update historical_backfill_tasks
                       set status = 'dead_letter',
                           worker_id = null,
                           claim_token = null,
                           lease_until = null,
                           lease_heartbeat_at = null,
                           attempts = case
                               when status = 'running' then attempts + 1
                               else attempts
                           end,
                           max_attempts = $5,
                           last_error = coalesce(last_error, 'maximum attempts exhausted'),
                           finished_at = clock_timestamp(),
                           updated_at = clock_timestamp()
                     where provider = $1
                       and run_id is not distinct from $6::uuid
                       and ($7::bigint[] is null or id = any($7::bigint[]))
                       and (
                           (status in ('pending', 'failed')
                            and attempts >= least(max_attempts, $5))
                           or
                           (status = 'running'
                            and lease_until <= clock_timestamp()
                            and attempts + 1 >= least(max_attempts, $5))
                       )
                    returning id
                ), next_tasks as (
                    select id
                    from historical_backfill_tasks
                    where provider = $1
                      and run_id is not distinct from $6::uuid
                      and ($7::bigint[] is null or id = any($7::bigint[]))
                      and (
                          (status in ('pending', 'failed')
                           and attempts < least(max_attempts, $5))
                          or
                          (status = 'running'
                           and lease_until <= clock_timestamp()
                           and attempts + 1 < least(max_attempts, $5))
                      )
                    order by priority, updated_at, id
                    limit $2
                    for update skip locked
                )
                update historical_backfill_tasks task
                set status = 'running',
                    worker_id = $3,
                    lease_version = task.lease_version + 1,
                    claim_token = md5(task.id::text || ':' ||
                                      (task.lease_version + 1)::text || ':' ||
                                      clock_timestamp()::text || ':' || random()::text),
                    lease_until = clock_timestamp() + ($4::integer * interval '1 second'),
                    lease_heartbeat_at = clock_timestamp(),
                    attempts = case
                        when task.status = 'running' then task.attempts + 1
                        else task.attempts
                    end,
                    max_attempts = $5,
                    started_at = coalesce(task.started_at, clock_timestamp()),
                    last_run_at = clock_timestamp(),
                    last_error = null,
                    finished_at = null,
                    updated_at = clock_timestamp()
                from next_tasks, symbols s
                where task.id = next_tasks.id
                  and s.id = task.symbol_id
                returning
                    task.id,
                    task.next_offset,
                    task.page_size,
                    task.timeframe,
                    task.provider,
                    task.worker_id,
                    task.claim_token,
                    task.lease_version,
                    task.lease_until,
                    task.attempts,
                    task.max_attempts,
                    task.run_id,
                    task.stop_at,
                    s.code || '.' || s.exchange as symbol
                """,
                    provider,
                    max(1, limit),
                    worker_id,
                    max(1, lease_seconds),
                    max(1, max_attempts),
                    run_id,
                    task_ids,
                )
        return [dict(row) for row in rows]

    async def heartbeat(
        self,
        *,
        task_id: int,
        claim_token: str,
        lease_version: int,
        lease_seconds: int,
        run_id: UUID | None = None,
        stop_at: datetime | None = None,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _set_scoped_session_fence(conn, run_id)
                row = await conn.fetchrow(
                """
                update historical_backfill_tasks
                   set lease_until = clock_timestamp() + ($4::integer * interval '1 second'),
                       lease_heartbeat_at = clock_timestamp(),
                       updated_at = clock_timestamp()
                 where id = $1
                   and status = 'running'
                   and claim_token = $2
                   and lease_version = $3
                   and lease_until > clock_timestamp()
                   and run_id is not distinct from $5::uuid
                   and stop_at is not distinct from $6::timestamptz
                returning id
                """,
                    task_id,
                    claim_token,
                    lease_version,
                    max(1, lease_seconds),
                    run_id,
                    stop_at,
                )
        return row is not None

    async def yield_task(
        self,
        *,
        task_id: int,
        claim_token: str,
        lease_version: int,
        run_id: UUID | None = None,
        stop_at: datetime | None = None,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _set_scoped_session_fence(conn, run_id)
                row = await conn.fetchrow(
                """
                update historical_backfill_tasks
                   set status = 'pending',
                       worker_id = null,
                       claim_token = null,
                       lease_until = null,
                       lease_heartbeat_at = null,
                       updated_at = clock_timestamp()
                 where id = $1
                   and status = 'running'
                   and claim_token = $2
                   and lease_version = $3
                   and lease_until > clock_timestamp()
                   and run_id is not distinct from $4::uuid
                   and stop_at is not distinct from $5::timestamptz
                returning id
                """,
                    task_id,
                    claim_token,
                    lease_version,
                    run_id,
                    stop_at,
                )
        return row is not None

    async def record_failure(
        self,
        *,
        task_id: int,
        claim_token: str,
        lease_version: int,
        error: str,
        run_id: UUID | None = None,
        stop_at: datetime | None = None,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _set_scoped_session_fence(conn, run_id)
                row = await conn.fetchrow(
                """
                update historical_backfill_tasks
                set status = case
                        when attempts + 1 >= max_attempts then 'dead_letter'
                        else 'failed'
                    end,
                    attempts = attempts + 1,
                    worker_id = null,
                    claim_token = null,
                    lease_until = null,
                    lease_heartbeat_at = null,
                    last_error = $2,
                    last_run_at = clock_timestamp(),
                    finished_at = case
                        when attempts + 1 >= max_attempts then clock_timestamp()
                        else finished_at
                    end,
                    updated_at = clock_timestamp()
                where id = $1
                  and status = 'running'
                  and claim_token = $3
                  and lease_version = $4
                  and lease_until > clock_timestamp()
                  and run_id is not distinct from $5::uuid
                  and stop_at is not distinct from $6::timestamptz
                returning id
                """,
                    task_id,
                    error[:2000],
                    claim_token,
                    lease_version,
                    run_id,
                    stop_at,
                )
        return row is not None
