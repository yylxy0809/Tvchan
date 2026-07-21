from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from collector.storage.postgres import timeframe_to_db_code


def queue_name_for_chan_c(level: str) -> str:
    return f"chan_c_{level}"


def schedule_interval_seconds(level: str) -> int:
    code = timeframe_to_db_code(level)
    if code == 5:
        return 300
    if code == 30:
        return 1800
    if code == 1440:
        return 7200
    return max(300, code * 60)


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
HIGHER_TIMEFRAME_CODES = {10080, 43200}


def closed_period_cutoffs_utc(now: datetime | None = None) -> tuple[datetime, datetime]:
    current = now.astimezone(SHANGHAI_TZ) if now is not None else datetime.now(SHANGHAI_TZ)
    local_midnight = current.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start_local = local_midnight - timedelta(days=current.weekday())
    month_start_local = local_midnight.replace(day=1)
    return week_start_local.astimezone(timezone.utc), month_start_local.astimezone(timezone.utc)


class PostgresChanCStreamStore:
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

    async def __aenter__(self) -> "PostgresChanCStreamStore":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required for Module C stream tasks.") from exc
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

    async def ensure_tail_tasks_for_stale_heads(
        self,
        *,
        levels: list[str],
        modes: list[str],
        limit: int,
        shard_index: int = 0,
        shard_count: int = 1,
        symbols: list[str] | None = None,
    ) -> int:
        assert self._pool is not None
        level_codes = [timeframe_to_db_code(level) for level in levels]
        week_cutoff_utc, month_cutoff_utc = closed_period_cutoffs_utc()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with candidate_jobs as (
                    select
                        head.symbol_id,
                        head.chan_level,
                        head.mode,
                        head.base_timeframe,
                        coalesce(latest_stroke.anchor_bar_end, head.base_to_bar_end) as anchor_bar_end,
                        case
                            when head.chan_level in (10080, 43200) then closed_bar.target_bar_end
                            else ingest.last_bar_end
                        end as target_bar_end,
                        head.run_id as expected_head_run_id,
                        head.base_to_bar_end as expected_head_base_to_bar_end
                    from scheme2_chan_c_published_heads head
                    join symbols s on s.id = head.symbol_id
                    join scheme2_ingest_watermarks ingest
                      on ingest.symbol_id = head.symbol_id
                     and ingest.timeframe = head.base_timeframe
                    left join lateral (
                        select coalesce(end_base_ts, end_ts) as anchor_bar_end
                        from chan_c_strokes stroke
                        where stroke.run_id = head.run_id
                          and stroke.mode = case head.mode
                              when 'confirmed' then 1::smallint
                              when 'predictive' then 2::smallint
                              else 0::smallint
                          end
                          and stroke.is_confirmed = true
                        order by coalesce(end_base_ts, end_ts) desc, seq desc, id desc
                        limit 1
                    ) latest_stroke on true
                    left join lateral (
                        select k.ts as target_bar_end
                        from klines k
                        where k.symbol_id = head.symbol_id
                          and k.timeframe = head.base_timeframe
                          and k.source = any(array[2,3,4,5,6,7,8,9]::smallint[])
                          and (
                              (head.chan_level = 10080 and k.ts < $6::timestamptz)
                              or (head.chan_level = 43200 and k.ts < $7::timestamptz)
                          )
                        order by k.ts desc
                        limit 1
                    ) closed_bar on true
                    where s.is_active = true
                      and head.status = 'published'
                      and head.run_id is not null
                      and head.base_to_bar_end is not null
                      and head.base_timeframe = head.chan_level
                      and head.chan_level = any($1::int[])
                      and head.mode = any($2::text[])
                      and case
                          when head.chan_level = 10080 then
                              date_trunc('week', closed_bar.target_bar_end at time zone 'Asia/Shanghai')
                              > date_trunc('week', head.base_to_bar_end at time zone 'Asia/Shanghai')
                          when head.chan_level = 43200 then
                              date_trunc('month', closed_bar.target_bar_end at time zone 'Asia/Shanghai')
                              > date_trunc('month', head.base_to_bar_end at time zone 'Asia/Shanghai')
                          else (
                              coalesce(ingest.last_bar_end, '-infinity'::timestamptz)
                                  > head.base_to_bar_end
                              or (
                                  ingest.last_bar_end = head.base_to_bar_end
                                  and ingest.updated_at > head.updated_at
                              )
                          )
                      end
                      and (
                          $5::text[] is null
                          or (s.code || '.' || s.exchange) = any($5::text[])
                      )
                      and (
                          $4::int <= 1
                          or mod(abs(hashtext(s.code || '.' || s.exchange)::bigint), $4::int) = $3::int
                      )
                    order by head.base_to_bar_end, s.code, head.chan_level, head.mode
                    limit $8
                ),
                upserted as (
                    insert into scheme2_chan_c_tail_tasks (
                        symbol_id,
                        chan_level,
                        mode,
                        base_timeframe,
                        status,
                        priority,
                        queue_name,
                        schedule_interval_seconds,
                        next_run_at,
                        pending_since,
                        shard_bucket,
                        anchor_bar_end,
                        target_bar_end,
                        expected_head_run_id,
                        expected_head_base_to_bar_end
                    )
                    select
                        symbol_id,
                        chan_level,
                        mode,
                        base_timeframe,
                        'pending',
                        case chan_level
                            when 5 then 30
                            else 10
                        end,
                        'chan_c_' || case chan_level
                            when 5 then '5f'
                            when 30 then '30f'
                            when 1440 then '1d'
                            when 10080 then '1w'
                            when 43200 then '1m'
                            else chan_level::text
                        end,
                        case chan_level
                            when 5 then 300
                            when 30 then 1800
                            when 1440 then 7200
                            else greatest(300, chan_level * 60)
                        end,
                        now(),
                        now(),
                        mod(abs(hashtext((select code || '.' || exchange from symbols where id = candidate_jobs.symbol_id))::bigint), 1024)::smallint,
                        anchor_bar_end,
                        target_bar_end,
                        expected_head_run_id,
                        expected_head_base_to_bar_end
                    from candidate_jobs
                    on conflict (symbol_id, chan_level, mode, base_timeframe)
                    do update
                    set status = case
                            when scheme2_chan_c_tail_tasks.status = 'running'
                             and scheme2_chan_c_tail_tasks.lease_until > now()
                                then scheme2_chan_c_tail_tasks.status
                            else 'pending'
                        end,
                        pending_since = now(),
                        next_run_at = now(),
                        backoff_until = null,
                        priority = excluded.priority,
                        queue_name = excluded.queue_name,
                        schedule_interval_seconds = excluded.schedule_interval_seconds,
                        shard_bucket = excluded.shard_bucket,
                        anchor_bar_end = excluded.anchor_bar_end,
                        target_bar_end = greatest(
                            coalesce(scheme2_chan_c_tail_tasks.target_bar_end, '-infinity'::timestamptz),
                            excluded.target_bar_end
                        ),
                        expected_head_run_id = excluded.expected_head_run_id,
                        expected_head_base_to_bar_end = excluded.expected_head_base_to_bar_end,
                        updated_at = now()
                    returning id
                )
                select count(*)::int as count from upserted
                """,
                level_codes,
                modes,
                max(0, shard_index),
                max(1, shard_count),
                symbols or None,
                week_cutoff_utc,
                month_cutoff_utc,
                max(1, limit),
            )
        return int(rows[0]["count"]) if rows else 0

    async def normalize_higher_timeframe_targets(
        self,
        *,
        levels: list[str],
        modes: list[str],
        shard_index: int = 0,
        shard_count: int = 1,
        symbols: list[str] | None = None,
    ) -> int:
        assert self._pool is not None
        level_codes = [timeframe_to_db_code(level) for level in levels if timeframe_to_db_code(level) in HIGHER_TIMEFRAME_CODES]
        if not level_codes:
            return 0
        week_cutoff_utc, month_cutoff_utc = closed_period_cutoffs_utc()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with normalized as (
                    select
                        task.id,
                        head.base_to_bar_end,
                        closed_bar.target_bar_end as normalized_target_bar_end,
                        case
                            when task.chan_level = 10080 then
                                date_trunc('week', closed_bar.target_bar_end at time zone 'Asia/Shanghai')
                                > date_trunc('week', head.base_to_bar_end at time zone 'Asia/Shanghai')
                            when task.chan_level = 43200 then
                                date_trunc('month', closed_bar.target_bar_end at time zone 'Asia/Shanghai')
                                > date_trunc('month', head.base_to_bar_end at time zone 'Asia/Shanghai')
                            else false
                        end as has_new_period
                    from scheme2_chan_c_tail_tasks task
                    join symbols s on s.id = task.symbol_id
                    join scheme2_chan_c_published_heads head
                      on head.symbol_id = task.symbol_id
                     and head.chan_level = task.chan_level
                     and head.mode = task.mode
                     and head.base_timeframe = task.base_timeframe
                     and head.status = 'published'
                    left join lateral (
                        select k.ts as target_bar_end
                        from klines k
                        where k.symbol_id = task.symbol_id
                          and k.timeframe = task.base_timeframe
                          and k.source = any(array[2,3,4,5,6,7,8,9]::smallint[])
                          and (
                              (task.chan_level = 10080 and k.ts < $6::timestamptz)
                              or (task.chan_level = 43200 and k.ts < $7::timestamptz)
                          )
                        order by k.ts desc
                        limit 1
                    ) closed_bar on true
                    where task.chan_level = any($1::int[])
                      and task.mode = any($2::text[])
                      and s.is_active = true
                      and (
                          $5::text[] is null
                          or (s.code || '.' || s.exchange) = any($5::text[])
                      )
                      and (
                          $4::int <= 1
                          or mod(coalesce(task.shard_bucket, mod(abs(hashtext(s.code || '.' || s.exchange)::bigint), 1024))::int, $4::int) = $3::int
                      )
                ),
                updated as (
                    update scheme2_chan_c_tail_tasks task
                    set target_bar_end = normalized.normalized_target_bar_end,
                        status = case
                            when normalized.has_new_period then 'pending'
                            else 'success'
                        end,
                        next_run_at = case
                            when normalized.has_new_period then now()
                            else now() + (task.schedule_interval_seconds * interval '1 second')
                        end,
                        pending_since = case
                            when normalized.has_new_period then now()
                            else task.pending_since
                        end,
                        backoff_until = null,
                        claimed_target_bar_end = null,
                        lease_until = null,
                        lease_heartbeat_at = null,
                        worker_id = null,
                        claim_token = null,
                        last_error = null,
                        updated_at = now()
                    from normalized
                    where task.id = normalized.id
                      and not (
                          task.status = 'running'
                          and coalesce(task.lease_until, '-infinity'::timestamptz) > now()
                      )
                      and (
                          task.status = 'running'
                          or coalesce(task.target_bar_end, '-infinity'::timestamptz) is distinct from coalesce(normalized.normalized_target_bar_end, '-infinity'::timestamptz)
                          or (
                              not normalized.has_new_period
                              and task.status <> 'success'
                          )
                          or (
                              normalized.has_new_period
                              and task.status = 'success'
                          )
                      )
                    returning task.id
                )
                select count(*)::int as count from updated
                """,
                level_codes,
                modes,
                max(0, shard_index),
                max(1, shard_count),
                symbols or None,
                week_cutoff_utc,
                month_cutoff_utc,
            )
        return int(rows[0]["count"]) if rows else 0

    async def claim_tail_tasks(
        self,
        *,
        limit: int,
        worker_id: str,
        lease_seconds: int,
        shard_index: int = 0,
        shard_count: int = 1,
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with next_tasks as (
                    select task.id
                    from scheme2_chan_c_tail_tasks task
                    join symbols s on s.id = task.symbol_id
                    join scheme2_chan_c_published_heads head
                      on head.symbol_id = task.symbol_id
                     and head.chan_level = task.chan_level
                     and head.mode = task.mode
                     and head.base_timeframe = task.base_timeframe
                     and head.status = 'published'
                    where (
                          task.status in ('pending', 'failed', 'success')
                          or (
                              task.status = 'running'
                              and coalesce(task.lease_until, '-infinity'::timestamptz) <= now()
                          )
                      )
                      and s.is_active = true
                      and case
                          when task.chan_level = 10080 then
                              date_trunc('week', task.target_bar_end at time zone 'Asia/Shanghai')
                              > date_trunc('week', head.base_to_bar_end at time zone 'Asia/Shanghai')
                          when task.chan_level = 43200 then
                              date_trunc('month', task.target_bar_end at time zone 'Asia/Shanghai')
                              > date_trunc('month', head.base_to_bar_end at time zone 'Asia/Shanghai')
                          else (
                              task.target_bar_end > head.base_to_bar_end
                              or (
                                  task.target_bar_end = head.base_to_bar_end
                                  and task.updated_at > head.updated_at
                              )
                          )
                      end
                      and task.next_run_at <= now()
                      and coalesce(task.backoff_until, '-infinity'::timestamptz) <= now()
                      and (
                          $6::text[] is null
                          or (s.code || '.' || s.exchange) = any($6::text[])
                      )
                      and (
                          $5::int <= 1
                          or mod(coalesce(task.shard_bucket, mod(abs(hashtext(s.code || '.' || s.exchange)::bigint), 1024))::int, $5::int) = $4::int
                      )
                    order by task.priority desc, task.next_run_at, task.pending_since, task.updated_at, task.id
                    limit $1
                    for update skip locked
                )
                update scheme2_chan_c_tail_tasks task
                set status = 'running',
                    attempts = task.attempts + 1,
                    worker_id = $2,
                    lease_version = task.lease_version + 1,
                    claim_token = md5(task.id::text || ':' || (task.lease_version + 1)::text || ':' || clock_timestamp()::text),
                    lease_until = now() + ($3::int * interval '1 second'),
                    lease_heartbeat_at = now(),
                    claimed_target_bar_end = task.target_bar_end,
                    expected_head_run_id = coalesce(
                        (
                            select head.run_id
                            from scheme2_chan_c_published_heads head
                            where head.symbol_id = task.symbol_id
                              and head.chan_level = task.chan_level
                              and head.mode = task.mode
                              and head.base_timeframe = task.base_timeframe
                              and head.status = 'published'
                            limit 1
                        ),
                        task.expected_head_run_id
                    ),
                    expected_head_base_to_bar_end = coalesce(
                        (
                            select head.base_to_bar_end
                            from scheme2_chan_c_published_heads head
                            where head.symbol_id = task.symbol_id
                              and head.chan_level = task.chan_level
                              and head.mode = task.mode
                              and head.base_timeframe = task.base_timeframe
                              and head.status = 'published'
                            limit 1
                        ),
                        task.expected_head_base_to_bar_end
                    ),
                    last_error = null,
                    updated_at = now()
                from next_tasks, symbols s
                where task.id = next_tasks.id
                  and s.id = task.symbol_id
                returning
                    task.id,
                    s.code || '.' || s.exchange as symbol,
                    task.chan_level,
                    task.mode,
                    task.base_timeframe,
                    task.anchor_bar_end,
                    task.target_bar_end as last_bar_end,
                    task.expected_head_run_id,
                    task.expected_head_base_to_bar_end,
                    task.claim_token,
                    task.lease_version,
                    task.claimed_target_bar_end
                """,
                max(1, limit),
                worker_id,
                max(1, lease_seconds),
                max(0, shard_index),
                max(1, shard_count),
                symbols or None,
            )
        return [dict(row) for row in rows]

    async def complete_tail_task(
        self,
        *,
        task_id: int,
        claim_token: str,
        bar_until: datetime | None = None,
        error: str | None = None,
    ) -> bool:
        assert self._pool is not None
        status = "failed" if error else "success"
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                update scheme2_chan_c_tail_tasks
                set status = case
                        when $3::varchar = 'failed' and coalesce($5, '') like 'Refusing to publish non-advancing Chan tail%' then 'success'
                        when $3::varchar = 'failed' and coalesce($5, '') like 'Stale Chan head%' then 'pending'
                        when $3::varchar = 'failed' then 'failed'
                        when target_bar_end > coalesce(claimed_target_bar_end, '-infinity'::timestamptz) then 'pending'
                        else 'success'
                    end,
                    last_success_bar_end = case when $3::varchar = 'success' then coalesce($4, last_success_bar_end) else last_success_bar_end end,
                    last_error = $5,
                    consecutive_failures = case
                        when $3::varchar = 'success' then 0
                        when coalesce($5, '') like 'Refusing to publish non-advancing Chan tail%' then 0
                        when coalesce($5, '') like 'Stale Chan head%' then consecutive_failures
                        else consecutive_failures + 1
                    end,
                    backoff_until = case
                        when $3::varchar = 'success' then null
                        when coalesce($5, '') like 'Refusing to publish non-advancing Chan tail%' then null
                        when coalesce($5, '') like 'Stale Chan head%' then null
                        else now() + (
                            least(900, 30 * power(2, least(consecutive_failures, 5)))::int
                            * interval '1 second'
                        )
                    end,
                    next_run_at = case
                        when $3::varchar = 'failed' and coalesce($5, '') like 'Refusing to publish non-advancing Chan tail%'
                            then now() + (schedule_interval_seconds * interval '1 second')
                        when $3::varchar = 'failed' and coalesce($5, '') like 'Stale Chan head%' then now()
                        when $3::varchar = 'failed' then next_run_at
                        when target_bar_end > coalesce(claimed_target_bar_end, '-infinity'::timestamptz) then now()
                        else now() + (schedule_interval_seconds * interval '1 second')
                    end,
                    pending_since = case
                        when $3::varchar = 'success'
                         and target_bar_end > coalesce(claimed_target_bar_end, '-infinity'::timestamptz)
                            then now()
                        else pending_since
                    end,
                    lease_until = null,
                    lease_heartbeat_at = null,
                    worker_id = null,
                    claim_token = null,
                    claimed_target_bar_end = null,
                    updated_at = now()
                where id = $1
                  and status = 'running'
                  and claim_token = $2
                """,
                task_id,
                claim_token,
                status,
                bar_until,
                None if error is None else error[:2000],
            )
        return result.endswith(" 1")
