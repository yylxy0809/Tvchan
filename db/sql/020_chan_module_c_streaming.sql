alter table if exists chan_c_runs
    add column if not exists run_kind varchar(24) not null default 'full',
    add column if not exists parent_run_id bigint references chan_c_runs(id),
    add column if not exists expected_head_run_id bigint,
    add column if not exists run_group_id varchar(64),
    add column if not exists anchor_bar_end timestamptz,
    add column if not exists cutoff_bar_end timestamptz;

create index if not exists idx_chan_c_runs_parent_tail
on chan_c_runs (parent_run_id, run_kind, bar_until desc)
where parent_run_id is not null;

create index if not exists idx_chan_c_signals_base_range
on chan_c_signals (run_id, base_ts);

create table if not exists scheme2_chan_c_tail_tasks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null,
    status varchar(16) not null default 'pending',
    priority integer not null default 100,
    queue_name varchar(64),
    schedule_interval_seconds integer not null default 300,
    next_run_at timestamptz not null default now(),
    deadline_at timestamptz,
    backoff_until timestamptz,
    pending_since timestamptz,
    shard_bucket smallint,
    attempts integer not null default 0,
    consecutive_failures integer not null default 0,
    worker_id varchar(128),
    claim_token varchar(64),
    lease_version bigint not null default 0,
    lease_until timestamptz,
    lease_heartbeat_at timestamptz,
    anchor_bar_end timestamptz,
    target_bar_end timestamptz,
    claimed_target_bar_end timestamptz,
    expected_head_run_id bigint,
    expected_head_base_to_bar_end timestamptz,
    last_success_bar_end timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists uq_scheme2_chan_c_tail_task_scope
on scheme2_chan_c_tail_tasks (symbol_id, chan_level, mode, base_timeframe);

create index if not exists idx_scheme2_chan_c_tail_tasks_due
on scheme2_chan_c_tail_tasks (next_run_at, backoff_until, priority, pending_since, id)
where status in ('pending', 'failed', 'success');

create index if not exists idx_scheme2_chan_c_tail_tasks_shard_due
on scheme2_chan_c_tail_tasks (shard_bucket, next_run_at, priority, pending_since, id);

comment on table scheme2_chan_c_tail_tasks is
'Module C native-timeframe Chan tail task lease table. One task advances one symbol/level/mode using that level native K-line timeframe.';
