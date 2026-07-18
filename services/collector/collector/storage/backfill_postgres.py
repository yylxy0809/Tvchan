from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from collector.storage.postgres import timeframe_to_db_code
from trading_protocol import SymbolInfo


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
    ) -> int:
        assert self._pool is not None
        rows = [
            (symbol.symbol, timeframe_to_db_code(timeframe), provider, page_size)
            for symbol in symbols
            for timeframe in timeframes
        ]
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
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
                    on conflict (symbol_id, timeframe, provider) do update
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
                    on conflict (symbol_id, timeframe, provider) do update
                    set page_size = excluded.page_size,
                        updated_at = now()
                    where historical_backfill_tasks.status in ('pending', 'failed')
                    """,
                    rows,
                )
        return len(rows)

    async def reset_running(self, *, provider: str) -> int:
        """Release only expired owners; a live lease is never operator-reset."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
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
                  and status = 'running'
                  and lease_until <= clock_timestamp()
                """,
                provider,
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
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
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
                    s.code || '.' || s.exchange as symbol
                """,
                provider,
                max(1, limit),
                worker_id,
                max(1, lease_seconds),
                max(1, max_attempts),
            )
        return [dict(row) for row in rows]

    async def heartbeat(
        self,
        *,
        task_id: int,
        claim_token: str,
        lease_version: int,
        lease_seconds: int,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
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
                returning id
                """,
                task_id,
                claim_token,
                lease_version,
                max(1, lease_seconds),
            )
        return row is not None

    async def yield_task(
        self,
        *,
        task_id: int,
        claim_token: str,
        lease_version: int,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
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
                returning id
                """,
                task_id,
                claim_token,
                lease_version,
            )
        return row is not None

    async def record_failure(
        self,
        *,
        task_id: int,
        claim_token: str,
        lease_version: int,
        error: str,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
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
                returning id
                """,
                task_id,
                error[:2000],
                claim_token,
                lease_version,
            )
        return row is not None
