from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
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
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                update historical_backfill_tasks
                set status = 'pending',
                    last_error = null,
                    updated_at = now()
                where provider = $1
                  and status = 'running'
                """,
                provider,
            )
        return int(result.split()[-1])

    async def claim_tasks(self, *, provider: str, limit: int) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with next_tasks as (
                    select id
                    from historical_backfill_tasks
                    where provider = $1
                      and status in ('pending', 'failed')
                    order by priority, updated_at, id
                    limit $2
                    for update skip locked
                )
                update historical_backfill_tasks task
                set status = 'running',
                    started_at = coalesce(task.started_at, now()),
                    last_run_at = now(),
                    last_error = null,
                    updated_at = now()
                from next_tasks, symbols s
                where task.id = next_tasks.id
                  and s.id = task.symbol_id
                returning
                    task.id,
                    task.next_offset,
                    task.page_size,
                    task.timeframe,
                    task.provider,
                    s.code || '.' || s.exchange as symbol
                """,
                provider,
                limit,
            )
        return [dict(row) for row in rows]

    async def record_page_success(
        self,
        *,
        task_id: int,
        next_offset: int,
        bars_read: int,
        bars_written: int,
        oldest_ts: datetime | None,
        newest_ts: datetime | None,
        exhausted: bool,
    ) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update historical_backfill_tasks
                set next_offset = $2,
                    status = case when $7 then 'success' else 'pending' end,
                    pages_done = pages_done + 1,
                    bars_read = bars_read + $3,
                    bars_written = bars_written + $4,
                    oldest_ts = case
                        when oldest_ts is null then $5::timestamptz
                        when $5::timestamptz is null then oldest_ts
                        when $5::timestamptz < oldest_ts then $5::timestamptz
                        else oldest_ts
                    end,
                    newest_ts = case
                        when newest_ts is null then $6::timestamptz
                        when $6::timestamptz is null then newest_ts
                        when $6::timestamptz > newest_ts then $6::timestamptz
                        else newest_ts
                    end,
                    finished_at = case when $7 then now() else finished_at end,
                    last_run_at = now(),
                    last_error = null,
                    updated_at = now()
                where id = $1
                """,
                task_id,
                next_offset,
                bars_read,
                bars_written,
                oldest_ts,
                newest_ts,
                exhausted,
            )

    async def record_failure(self, *, task_id: int, error: str) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update historical_backfill_tasks
                set status = 'failed',
                    last_error = $2,
                    last_run_at = now(),
                    updated_at = now()
                where id = $1
                """,
                task_id,
                error[:2000],
            )
