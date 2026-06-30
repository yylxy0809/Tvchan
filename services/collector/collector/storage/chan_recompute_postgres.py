from __future__ import annotations

from datetime import datetime
from typing import Any

from collector.storage.postgres import timeframe_to_db_code


class PostgresChanRecomputeTaskStore:
    def __init__(
        self,
        database_url: str,
        *,
        pool_min_size: int | None = None,
        pool_max_size: int | None = None,
    ) -> None:
        self.database_url = database_url
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self._pool = None

    async def __aenter__(self) -> "PostgresChanRecomputeTaskStore":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for Chan recompute tasks."
            ) from exc
        kwargs = {}
        if self.pool_min_size is not None:
            kwargs["min_size"] = self.pool_min_size
        if self.pool_max_size is not None:
            kwargs["max_size"] = self.pool_max_size
        self._pool = await asyncpg.create_pool(self.database_url, **kwargs)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def list_symbols_with_bars(self, *, levels: list[str], limit: int) -> list[str]:
        assert self._pool is not None
        level_codes = [timeframe_to_db_code(level) for level in levels]
        limit_value = None if limit <= 0 else limit
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select s.code || '.' || s.exchange as symbol
                from symbols s
                join scheme2_ingest_watermarks w on w.symbol_id = s.id
                where s.is_active = true
                  and w.timeframe = any($1::int[])
                order by s.code, s.exchange
                limit coalesce($2::int, 2147483647)
                """,
                level_codes,
                limit_value,
            )
        return [str(row["symbol"]) for row in rows]

    async def ensure_tasks(
        self,
        *,
        symbols: list[str],
        levels: list[str],
        modes: list[str],
        config_hash: str = "module-b:chan.py-5f-v1",
        reset: bool = False,
    ) -> int:
        assert self._pool is not None
        modes_value = ",".join(modes)
        rows = [
            (symbol, timeframe_to_db_code(level), modes_value, config_hash)
            for symbol in symbols
            for level in levels
        ]
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            if reset:
                await conn.executemany(
                    """
                    insert into chan_recompute_tasks (
                        symbol_id,
                        chan_level,
                        modes,
                        config_hash,
                        status,
                        attempts,
                        last_bar_from,
                        last_bar_until,
                        last_bar_count,
                        strokes_count,
                        segments_count,
                        centers_count,
                        signals_count,
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
                        'pending',
                        0,
                        null,
                        null,
                        null,
                        0,
                        0,
                        0,
                        0,
                        null,
                        null,
                        null,
                        null
                    from symbols s
                    where (s.code || '.' || s.exchange) = $1
                    on conflict (symbol_id, chan_level, modes, config_hash) do update
                    set status = 'pending',
                        attempts = 0,
                        last_bar_from = null,
                        last_bar_until = null,
                        last_bar_count = null,
                        strokes_count = 0,
                        segments_count = 0,
                        centers_count = 0,
                        signals_count = 0,
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
                    insert into chan_recompute_tasks (
                        symbol_id,
                        chan_level,
                        modes,
                        config_hash,
                        status
                    )
                    select s.id, $2, $3, $4, 'pending'
                    from symbols s
                    where (s.code || '.' || s.exchange) = $1
                    on conflict (symbol_id, chan_level, modes, config_hash) do nothing
                    """,
                    rows,
                )
                await conn.executemany(
                    """
                    with symbol_row as (
                        select id
                        from symbols
                        where (code || '.' || exchange) = $1
                    ),
                    bar_stats as (
                        select
                            min(k.ts) as bar_from,
                            max(k.ts) as bar_until,
                            count(*)::int as bar_count
                        from klines k, symbol_row s
                        where k.symbol_id = s.id
                          and k.timeframe = $2
                    )
                    update chan_recompute_tasks task
                    set status = 'pending',
                        updated_at = now()
                    from symbol_row s, bar_stats stats
                    where task.symbol_id = s.id
                      and task.chan_level = $2
                      and task.modes = $3
                      and task.config_hash = $4
                      and task.status <> 'running'
                      and (
                          task.status <> 'success'
                          or task.last_bar_count is distinct from stats.bar_count
                          or task.last_bar_from is distinct from stats.bar_from
                          or task.last_bar_until is distinct from stats.bar_until
                      )
                    """,
                    rows,
                )
        return len(rows)

    async def reset_running(self) -> int:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                update chan_recompute_tasks
                set status = 'pending',
                    last_error = null,
                    updated_at = now()
                where status = 'running'
                """
            )
        return int(result.split()[-1])

    async def claim_tasks(self, *, limit: int) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with next_tasks as (
                    select id
                    from chan_recompute_tasks
                    where status in ('pending', 'failed')
                    order by priority, updated_at, id
                    limit $1
                    for update skip locked
                )
                update chan_recompute_tasks task
                set status = 'running',
                    attempts = task.attempts + 1,
                    started_at = coalesce(task.started_at, now()),
                    last_run_at = now(),
                    last_error = null,
                    updated_at = now()
                from next_tasks, symbols s
                where task.id = next_tasks.id
                  and s.id = task.symbol_id
                returning
                    task.id,
                    task.chan_level,
                    task.modes,
                    task.config_hash,
                    s.code || '.' || s.exchange as symbol
                """,
                limit,
            )
        return [dict(row) for row in rows]

    async def record_success(
        self,
        *,
        task_id: int,
        bar_from: datetime,
        bar_until: datetime,
        bar_count: int,
        counts: dict[str, int],
    ) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update chan_recompute_tasks
                set status = 'success',
                    last_bar_from = $2,
                    last_bar_until = $3,
                    last_bar_count = $4,
                    strokes_count = $5,
                    segments_count = $6,
                    centers_count = $7,
                    signals_count = $8,
                    finished_at = now(),
                    last_run_at = now(),
                    last_error = null,
                    updated_at = now()
                where id = $1
                """,
                task_id,
                bar_from,
                bar_until,
                bar_count,
                counts.get("strokes", 0),
                counts.get("segments", 0),
                counts.get("centers", 0),
                counts.get("signals", 0),
            )

    async def record_failure(self, *, task_id: int, error: str) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update chan_recompute_tasks
                set status = 'failed',
                    last_error = $2,
                    last_run_at = now(),
                    updated_at = now()
                where id = $1
                """,
                task_id,
                error[:2000],
            )
