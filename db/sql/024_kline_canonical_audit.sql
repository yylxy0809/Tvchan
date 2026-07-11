-- Durable state for the bounded canonical K-line audit worker.
-- No index is added to the klines hypertable: existing (symbol_id,timeframe,ts)
-- access paths support the worker's per-symbol bounded reads.

create table if not exists kline_audit_runs (
    audit_run_id uuid primary key,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    status text not null check (status in ('running', 'completed', 'failed')),
    apply_mode boolean not null default false,
    parameters jsonb not null default '{}'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    failure text
);

create table if not exists kline_audit_checkpoints (
    audit_run_id uuid not null references kline_audit_runs(audit_run_id) on delete cascade,
    symbol_id integer not null references symbols(id) on delete cascade,
    timeframe integer not null,
    shard_start timestamptz not null,
    shard_end timestamptz not null,
    status text not null check (status in ('completed', 'failed')),
    rows_scanned bigint not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now(),
    primary key (audit_run_id, symbol_id, timeframe, shard_start, shard_end)
);

create index if not exists kline_audit_checkpoints_resume_idx
    on kline_audit_checkpoints (audit_run_id, status, symbol_id, timeframe, shard_start);

create table if not exists kline_audit_quarantine (
    id bigint generated always as identity primary key,
    audit_run_id uuid not null references kline_audit_runs(audit_run_id) on delete restrict,
    quarantined_at timestamptz not null default now(),
    reason text not null,
    conflict_details jsonb not null default '{}'::jsonb,
    symbol_id integer not null,
    timeframe integer not null,
    ts timestamptz not null,
    open_x1000 integer not null,
    high_x1000 integer not null,
    low_x1000 integer not null,
    close_x1000 integer not null,
    volume bigint not null,
    amount_x100 bigint,
    is_complete boolean not null,
    revision integer not null,
    source smallint not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (audit_run_id, symbol_id, timeframe, ts, source, revision, updated_at)
);

create index if not exists kline_audit_quarantine_run_idx
    on kline_audit_quarantine (audit_run_id, symbol_id, timeframe, ts);
