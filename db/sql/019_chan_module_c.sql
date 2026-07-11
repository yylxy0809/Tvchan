create table if not exists chan_c_runs (
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
    error_message text,
    snapshot_version varchar(255),
    computed_at timestamptz
);

comment on table chan_c_runs is
'Module C Chan calculation runs. Module C calculates each Chan level from its native K-line timeframe, not from recursive 5f structures.';

create index if not exists idx_chan_c_runs_lookup
on chan_c_runs (symbol_id, chan_level, status, bar_from, bar_until desc);

create index if not exists idx_chan_c_runs_snapshot_version
on chan_c_runs (symbol_id, chan_level, status, snapshot_version);

create table if not exists chan_c_strokes (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_c_runs(id),
    seq integer not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    start_price_x1000 integer not null,
    end_price_x1000 integer not null,
    direction smallint not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    begin_base_ts timestamptz,
    end_base_ts timestamptz,
    begin_base_seq integer,
    end_base_seq integer,
    extra jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists chan_c_segments (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_c_runs(id),
    seq integer not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    start_price_x1000 integer not null,
    end_price_x1000 integer not null,
    direction smallint not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    begin_base_ts timestamptz,
    end_base_ts timestamptz,
    begin_base_seq integer,
    end_base_seq integer,
    extra jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_chan_c_strokes_base_range
on chan_c_strokes (run_id, begin_base_ts, end_base_ts);

create index if not exists idx_chan_c_segments_base_range
on chan_c_segments (run_id, begin_base_ts, end_base_ts);

create table if not exists chan_c_centers (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_c_runs(id),
    seq integer not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    low_x1000 integer not null,
    high_x1000 integer not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    begin_base_ts timestamptz,
    end_base_ts timestamptz,
    begin_base_seq integer,
    end_base_seq integer,
    extra jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_chan_c_centers_base_range
on chan_c_centers (run_id, begin_base_ts, end_base_ts);

create table if not exists chan_c_signals (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode smallint not null,
    run_id bigint references chan_c_runs(id),
    ts timestamptz not null,
    price_x1000 integer not null,
    signal_type varchar(32) not null,
    is_confirmed boolean not null,
    revision integer not null default 0,
    base_ts timestamptz,
    base_seq integer,
    extra jsonb,
    created_at timestamptz not null default now()
);

create table if not exists scheme2_chan_c_published_heads (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null,
    base_from_bar_end timestamptz,
    base_to_bar_end timestamptz,
    bar_count integer,
    snapshot_version varchar(255) not null,
    status varchar(16) not null default 'published',
    run_id bigint references chan_c_runs(id),
    published_at timestamptz,
    updated_at timestamptz not null default now(),
    last_error text
);

comment on table scheme2_chan_c_published_heads is
'Published Module C Chan heads. base_timeframe equals the native K-line timeframe used for that Chan level.';

create unique index if not exists uq_scheme2_chan_c_published_head_scope
on scheme2_chan_c_published_heads (symbol_id, chan_level, mode, base_timeframe);

create index if not exists idx_scheme2_chan_c_published_head_status
on scheme2_chan_c_published_heads (status, published_at desc, symbol_id, chan_level);

create index if not exists idx_scheme2_chan_c_published_head_snapshot
on scheme2_chan_c_published_heads (snapshot_version, symbol_id, chan_level);

create table if not exists scheme2_chan_c_recompute_watermarks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null,
    dirty_from_bar_end timestamptz,
    last_computed_bar_end timestamptz,
    updated_at timestamptz not null default now(),
    dirty_reason text,
    last_error text
);

create unique index if not exists uq_scheme2_chan_c_recompute_watermark_scope
on scheme2_chan_c_recompute_watermarks (symbol_id, chan_level, mode, base_timeframe);

create index if not exists idx_scheme2_chan_c_recompute_watermark_dirty
on scheme2_chan_c_recompute_watermarks (dirty_from_bar_end, updated_at desc, symbol_id, chan_level)
where dirty_from_bar_end is not null;

create index if not exists idx_scheme2_chan_c_recompute_watermark_computed
on scheme2_chan_c_recompute_watermarks (last_computed_bar_end desc, symbol_id, chan_level);
