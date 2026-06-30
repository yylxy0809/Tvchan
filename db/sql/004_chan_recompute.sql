create table if not exists chan_recompute_tasks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    modes varchar(64) not null default 'confirmed,predictive',
    config_hash varchar(64) not null default 'module-b:chan.py-default',
    status varchar(16) not null default 'pending',
    priority integer not null default 100,
    attempts integer not null default 0,
    last_bar_from timestamptz,
    last_bar_until timestamptz,
    last_bar_count integer,
    strokes_count integer not null default 0,
    segments_count integer not null default 0,
    centers_count integer not null default 0,
    signals_count integer not null default 0,
    started_at timestamptz,
    last_run_at timestamptz,
    finished_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (symbol_id, chan_level, modes, config_hash)
);

create index if not exists idx_chan_recompute_tasks_status
on chan_recompute_tasks (status, priority, updated_at, id);

create index if not exists idx_chan_recompute_tasks_symbol_level
on chan_recompute_tasks (symbol_id, chan_level, modes, config_hash);
