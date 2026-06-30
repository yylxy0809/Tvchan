create table if not exists tdx_csv_import_tasks (
    id bigint generated always as identity primary key,
    zip_path text not null,
    timeframe integer not null,
    zip_size bigint not null,
    zip_mtime timestamptz not null,
    status varchar(16) not null default 'pending',
    priority integer not null default 100,
    attempts integer not null default 0,
    entries_total integer,
    entries_done integer not null default 0,
    last_entry_index integer not null default -1,
    last_entry_name text,
    bars_read bigint not null default 0,
    bars_written bigint not null default 0,
    symbols_seen integer not null default 0,
    started_at timestamptz,
    last_run_at timestamptz,
    finished_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (zip_path, timeframe)
);

create index if not exists idx_tdx_csv_import_tasks_status
on tdx_csv_import_tasks (status, priority, updated_at, id);

create index if not exists idx_tdx_csv_import_tasks_timeframe
on tdx_csv_import_tasks (timeframe, status);
