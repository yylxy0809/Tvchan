from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from collector.storage.postgres import timeframe_to_db_code


@dataclass(frozen=True)
class TdxCsvArchiveTask:
    zip_path: str
    timeframe: str
    zip_size: int
    zip_mtime: datetime


class PostgresTdxCsvImportTaskStore:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pool = None

    async def __aenter__(self) -> "PostgresTdxCsvImportTaskStore":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required for TDX CSV import tasks.") from exc
        self._pool = await asyncpg.create_pool(self.database_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def ensure_tasks(self, archives: list[TdxCsvArchiveTask], *, reset: bool = False) -> int:
        assert self._pool is not None
        rows = [
            (
                archive.zip_path,
                timeframe_to_db_code(archive.timeframe),
                archive.zip_size,
                archive.zip_mtime,
            )
            for archive in archives
        ]
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                insert into tdx_csv_import_tasks (
                    zip_path,
                    timeframe,
                    zip_size,
                    zip_mtime,
                    status
                )
                values ($1, $2, $3, $4, 'pending')
                on conflict (zip_path, timeframe) do update
                set zip_size = excluded.zip_size,
                    zip_mtime = excluded.zip_mtime,
                    status = case
                        when $5::boolean then 'pending'
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then 'pending'
                        else tdx_csv_import_tasks.status
                    end,
                    attempts = case
                        when $5::boolean then 0
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then 0
                        else tdx_csv_import_tasks.attempts
                    end,
                    entries_total = case
                        when $5::boolean then null
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then null
                        else tdx_csv_import_tasks.entries_total
                    end,
                    entries_done = case
                        when $5::boolean then 0
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then 0
                        else tdx_csv_import_tasks.entries_done
                    end,
                    last_entry_index = case
                        when $5::boolean then -1
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then -1
                        else tdx_csv_import_tasks.last_entry_index
                    end,
                    last_entry_name = case
                        when $5::boolean then null
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then null
                        else tdx_csv_import_tasks.last_entry_name
                    end,
                    bars_read = case
                        when $5::boolean then 0
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then 0
                        else tdx_csv_import_tasks.bars_read
                    end,
                    bars_written = case
                        when $5::boolean then 0
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then 0
                        else tdx_csv_import_tasks.bars_written
                    end,
                    symbols_seen = case
                        when $5::boolean then 0
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then 0
                        else tdx_csv_import_tasks.symbols_seen
                    end,
                    started_at = case
                        when $5::boolean then null
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then null
                        else tdx_csv_import_tasks.started_at
                    end,
                    last_run_at = case
                        when $5::boolean then null
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then null
                        else tdx_csv_import_tasks.last_run_at
                    end,
                    finished_at = case
                        when $5::boolean then null
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then null
                        else tdx_csv_import_tasks.finished_at
                    end,
                    last_error = case
                        when $5::boolean then null
                        when tdx_csv_import_tasks.zip_size is distinct from excluded.zip_size
                          or tdx_csv_import_tasks.zip_mtime is distinct from excluded.zip_mtime
                        then null
                        else tdx_csv_import_tasks.last_error
                    end,
                    updated_at = now()
                """,
                [(row[0], row[1], row[2], row[3], reset) for row in rows],
            )
        return len(rows)

    async def reset_running(self) -> int:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                update tdx_csv_import_tasks
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
                    from tdx_csv_import_tasks
                    where status in ('pending', 'failed')
                    order by priority, updated_at, id
                    limit $1
                    for update skip locked
                )
                update tdx_csv_import_tasks task
                set status = 'running',
                    attempts = task.attempts + 1,
                    started_at = coalesce(task.started_at, now()),
                    last_run_at = now(),
                    last_error = null,
                    updated_at = now()
                from next_tasks
                where task.id = next_tasks.id
                returning
                    task.id,
                    task.zip_path,
                    task.timeframe,
                    task.entries_total,
                    task.entries_done,
                    task.last_entry_index,
                    task.last_entry_name
                """,
                limit,
            )
        return [dict(row) for row in rows]

    async def record_progress(
        self,
        *,
        task_id: int,
        entries_total: int,
        entries_done_delta: int,
        last_entry_index: int,
        last_entry_name: str | None,
        bars_read_delta: int,
        bars_written_delta: int,
        symbols_seen_delta: int,
    ) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update tdx_csv_import_tasks
                set entries_total = $2,
                    entries_done = entries_done + $3,
                    last_entry_index = $4,
                    last_entry_name = $5,
                    bars_read = bars_read + $6,
                    bars_written = bars_written + $7,
                    symbols_seen = symbols_seen + $8,
                    last_run_at = now(),
                    updated_at = now()
                where id = $1
                """,
                task_id,
                entries_total,
                entries_done_delta,
                last_entry_index,
                last_entry_name,
                bars_read_delta,
                bars_written_delta,
                symbols_seen_delta,
            )

    async def record_success(self, *, task_id: int, entries_total: int) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update tdx_csv_import_tasks
                set status = 'success',
                    entries_total = $2,
                    entries_done = $2,
                    last_entry_index = case when $2 > 0 then $2 - 1 else -1 end,
                    finished_at = now(),
                    last_run_at = now(),
                    last_error = null,
                    updated_at = now()
                where id = $1
                """,
                task_id,
                entries_total,
            )

    async def record_paused(self, *, task_id: int, entries_total: int) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update tdx_csv_import_tasks
                set status = 'pending',
                    entries_total = $2,
                    last_run_at = now(),
                    updated_at = now()
                where id = $1
                """,
                task_id,
                entries_total,
            )

    async def record_failure(self, *, task_id: int, error: str) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update tdx_csv_import_tasks
                set status = 'failed',
                    last_error = $2,
                    last_run_at = now(),
                    updated_at = now()
                where id = $1
                """,
                task_id,
                error[:2000],
            )
