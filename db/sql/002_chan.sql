create table if not exists chan_runs (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    input_signature varchar(128) not null,
    config_hash varchar(64) not null,
    bar_from timestamptz,
    bar_until timestamptz not null,
    bar_count integer,
    status varchar(16) not null,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    error_message text
);

alter table chan_runs
add column if not exists bar_from timestamptz;

alter table chan_runs
add column if not exists bar_count integer;

create index if not exists idx_chan_runs_lookup
on chan_runs (symbol_id, chan_level, status, bar_from, bar_until desc);

create table if not exists chan_strokes (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_runs(id),
    seq integer not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    start_price_x1000 integer not null,
    end_price_x1000 integer not null,
    direction smallint not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    extra jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists chan_segments (like chan_strokes including all);

create table if not exists chan_centers (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_runs(id),
    seq integer not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    low_x1000 integer not null,
    high_x1000 integer not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    extra jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists chan_signals (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_runs(id),
    ts timestamptz not null,
    price_x1000 integer not null,
    signal_type varchar(32) not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    extra jsonb,
    created_at timestamptz not null default now()
);
