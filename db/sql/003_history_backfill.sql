create table if not exists historical_backfill_tasks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    timeframe integer not null,
    provider varchar(32) not null,
    page_size integer not null default 800,
    next_offset integer not null default 0,
    status varchar(16) not null default 'pending',
    priority integer not null default 100,
    pages_done integer not null default 0,
    bars_read bigint not null default 0,
    bars_written bigint not null default 0,
    oldest_ts timestamptz,
    newest_ts timestamptz,
    started_at timestamptz,
    last_run_at timestamptz,
    finished_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (symbol_id, timeframe, provider)
);

create index if not exists idx_historical_backfill_tasks_status
on historical_backfill_tasks (provider, status, priority, updated_at, id);

create index if not exists idx_historical_backfill_tasks_symbol_timeframe
on historical_backfill_tasks (symbol_id, timeframe, provider);
