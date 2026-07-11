-- Raw source import failures are intentionally kept separate from
-- kline_audit_quarantine: these rows may not satisfy the canonical klines
-- schema or symbol foreign-key constraints.
create table if not exists kline_import_runs (
    import_run_id uuid primary key,
    source_name text not null,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    status text not null check (status in ('running', 'completed', 'failed')),
    parameters jsonb not null default '{}'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    failure text
);

create table if not exists kline_import_checkpoints (
    import_run_id uuid not null references kline_import_runs(import_run_id) on delete cascade,
    source_ref text not null,
    source_checksum text not null,
    status text not null check (status in ('pending', 'running', 'completed', 'failed')),
    accepted_rows bigint not null default 0,
    quarantined_rows bigint not null default 0,
    last_source_row bigint,
    error_message text,
    updated_at timestamptz not null default now(),
    completed_at timestamptz,
    primary key (import_run_id, source_ref, source_checksum)
);

create index if not exists kline_import_checkpoints_resume_idx
    on kline_import_checkpoints (import_run_id, status, updated_at, source_ref);

create table if not exists kline_import_quarantine (
    id bigint generated always as identity primary key,
    import_run_id uuid not null references kline_import_runs(import_run_id) on delete restrict,
    source_name text not null,
    source_ref text not null,
    source_row bigint not null check (source_row >= 0),
    symbol_text text,
    timeframe text,
    raw_ts text,
    reason text not null,
    raw_payload jsonb not null,
    detected_at timestamptz not null default now(),
    unique (source_name, source_ref, source_row, reason)
);

create index if not exists kline_import_quarantine_run_idx
    on kline_import_quarantine (import_run_id, source_name, timeframe, detected_at);
