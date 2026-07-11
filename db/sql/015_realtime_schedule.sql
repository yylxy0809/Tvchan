alter table if exists scheme2_market_fetch_tasks
    add column if not exists queue_name varchar(32) not null default 'fetch_5f',
    add column if not exists next_run_at timestamptz not null default now(),
    add column if not exists deadline_at timestamptz,
    add column if not exists backoff_until timestamptz,
    add column if not exists target_bar_end timestamptz,
    add column if not exists claimed_target_bar_end timestamptz,
    add column if not exists pending_since timestamptz not null default now(),
    add column if not exists shard_bucket smallint,
    add column if not exists consecutive_failures integer not null default 0,
    add column if not exists lease_heartbeat_at timestamptz,
    add column if not exists schedule_interval_seconds integer not null default 300;

alter table if exists scheme2_chan_tail_tasks
    add column if not exists queue_name varchar(32) not null default 'chan_5f',
    add column if not exists next_run_at timestamptz not null default now(),
    add column if not exists deadline_at timestamptz,
    add column if not exists backoff_until timestamptz,
    add column if not exists claimed_target_bar_end timestamptz,
    add column if not exists pending_since timestamptz not null default now(),
    add column if not exists shard_bucket smallint,
    add column if not exists consecutive_failures integer not null default 0,
    add column if not exists lease_heartbeat_at timestamptz,
    add column if not exists schedule_interval_seconds integer not null default 300;

update scheme2_market_fetch_tasks task
set
    queue_name = 'fetch_' || case task.timeframe when 5 then '5f' when 15 then '15f' when 30 then '30f' when 60 then '1h' when 1440 then '1d' else task.timeframe::text end,
    schedule_interval_seconds = case task.timeframe
        when 5 then 300
        when 30 then 1800
        when 1440 then 7200
        else greatest(300, task.timeframe * 60)
    end,
    shard_bucket = mod(abs(hashtext(s.code || '.' || s.exchange)::bigint), 1024)::smallint,
    pending_since = coalesce(task.pending_since, task.updated_at, now())
from symbols s
where s.id = task.symbol_id
  and task.shard_bucket is null;

update scheme2_chan_tail_tasks task
set
    queue_name = 'chan_' || case task.chan_level when 5 then '5f' when 30 then '30f' when 1440 then '1d' else task.chan_level::text end,
    schedule_interval_seconds = case task.chan_level
        when 5 then 300
        when 30 then 1800
        when 1440 then 7200
        else greatest(300, task.chan_level * 60)
    end,
    shard_bucket = mod(abs(hashtext(s.code || '.' || s.exchange)::bigint), 1024)::smallint,
    pending_since = coalesce(task.pending_since, task.updated_at, now())
from symbols s
where s.id = task.symbol_id
  and task.shard_bucket is null;

create index if not exists idx_scheme2_market_fetch_tasks_due
on scheme2_market_fetch_tasks (next_run_at, backoff_until, priority, pending_since, id)
where status in ('pending', 'failed', 'success', 'running');

create index if not exists idx_scheme2_market_fetch_tasks_shard_due
on scheme2_market_fetch_tasks (shard_bucket, next_run_at, priority, pending_since, id);

create index if not exists idx_scheme2_chan_tail_tasks_due
on scheme2_chan_tail_tasks (next_run_at, backoff_until, priority, pending_since, id)
where status in ('pending', 'failed', 'success', 'running');

create index if not exists idx_scheme2_chan_tail_tasks_shard_due
on scheme2_chan_tail_tasks (shard_bucket, next_run_at, priority, pending_since, id);

comment on column scheme2_market_fetch_tasks.next_run_at is
'Earliest time this fetch task may be claimed. Workers use this instead of polling every symbol every pass.';

comment on column scheme2_chan_tail_tasks.claimed_target_bar_end is
'Target bar end seen at claim time. Completion keeps the task pending if a newer target arrived while this worker was running.';
