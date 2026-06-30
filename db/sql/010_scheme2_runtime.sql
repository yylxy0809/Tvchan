create table if not exists scheme2_source_member_checkpoints (
    id bigint generated always as identity primary key,
    root_path text not null,
    source_profile varchar(64) not null,
    zip_path text not null,
    member_path text not null,
    member_crc32 bigint,
    member_size_bytes bigint,
    timeframe integer not null default 5,
    status varchar(16) not null default 'pending',
    imported_rows bigint not null default 0,
    error_message text,
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

comment on table scheme2_source_member_checkpoints is
'Scheme 2 source-import member checkpoints. Tracks archive/member level import state only and does not change klines or chan_* primary data contracts.';

comment on column scheme2_source_member_checkpoints.timeframe is
'Canonical timeframe code for the imported member. Scheme 2 runtime currently expects 5 for canonical 5f history import.';

comment on column scheme2_source_member_checkpoints.status is
'Suggested values: pending, running, success, failed, skipped.';

create unique index if not exists uq_scheme2_source_member_checkpoint_identity
on scheme2_source_member_checkpoints (
    root_path,
    source_profile,
    zip_path,
    member_path,
    coalesce(member_crc32, -1),
    coalesce(member_size_bytes, -1)
);

create index if not exists idx_scheme2_source_member_checkpoint_status
on scheme2_source_member_checkpoints (status, updated_at desc, id desc);

create table if not exists scheme2_ingest_watermarks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    timeframe integer not null default 5,
    last_bar_end timestamptz,
    source varchar(32) not null default 'unknown',
    updated_at timestamptz not null default now(),
    note text
);

comment on table scheme2_ingest_watermarks is
'Scheme 2 ingest watermarks. Records how far canonical bar ingestion has been committed per symbol/timeframe. State only; does not replace klines.';

comment on column scheme2_ingest_watermarks.last_bar_end is
'Latest committed canonical bar_end for the symbol/timeframe watermark.';

create unique index if not exists uq_scheme2_ingest_watermark_symbol_timeframe
on scheme2_ingest_watermarks (symbol_id, timeframe);

create index if not exists idx_scheme2_ingest_watermark_updated_at
on scheme2_ingest_watermarks (timeframe, updated_at desc, symbol_id);

create table if not exists scheme2_chan_published_heads (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null default 5,
    base_from_bar_end timestamptz,
    base_to_bar_end timestamptz,
    bar_count integer,
    snapshot_version varchar(255) not null,
    status varchar(16) not null default 'published',
    run_id bigint references chan_runs(id),
    published_at timestamptz,
    updated_at timestamptz not null default now(),
    last_error text
);

comment on table scheme2_chan_published_heads is
'Scheme 2 published Chan heads. Stores the currently published snapshot metadata per symbol/level/mode/base_timeframe. State only; main structures remain in chan_* tables.';

comment on column scheme2_chan_published_heads.status is
'Suggested values: staged, published, superseded, failed.';

comment on column scheme2_chan_published_heads.base_from_bar_end is
'Inclusive lower bound of the canonical 5f base-bar range used to build the published snapshot.';

comment on column scheme2_chan_published_heads.base_to_bar_end is
'Inclusive upper bound of the canonical 5f base-bar range used to build the published snapshot.';

create unique index if not exists uq_scheme2_chan_published_head_scope
on scheme2_chan_published_heads (symbol_id, chan_level, mode, base_timeframe);

create index if not exists idx_scheme2_chan_published_head_status
on scheme2_chan_published_heads (status, published_at desc, symbol_id, chan_level);

create index if not exists idx_scheme2_chan_published_head_snapshot
on scheme2_chan_published_heads (snapshot_version, symbol_id, chan_level);

create table if not exists scheme2_chan_recompute_watermarks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null default 5,
    dirty_from_bar_end timestamptz,
    last_computed_bar_end timestamptz,
    updated_at timestamptz not null default now(),
    dirty_reason text,
    last_error text
);

comment on table scheme2_chan_recompute_watermarks is
'Scheme 2 incremental Chan recompute state. Tracks the earliest dirty canonical bar_end and the last fully computed bar_end per symbol/level/mode/base_timeframe.';

comment on column scheme2_chan_recompute_watermarks.dirty_from_bar_end is
'Earliest canonical 5f bar_end that must be recomputed before a new head can be published.';

comment on column scheme2_chan_recompute_watermarks.last_computed_bar_end is
'Latest canonical 5f bar_end whose Chan result has been fully computed and persisted.';

create unique index if not exists uq_scheme2_chan_recompute_watermark_scope
on scheme2_chan_recompute_watermarks (symbol_id, chan_level, mode, base_timeframe);

create index if not exists idx_scheme2_chan_recompute_watermark_dirty
on scheme2_chan_recompute_watermarks (dirty_from_bar_end, updated_at desc, symbol_id, chan_level)
where dirty_from_bar_end is not null;

create index if not exists idx_scheme2_chan_recompute_watermark_computed
on scheme2_chan_recompute_watermarks (last_computed_bar_end desc, symbol_id, chan_level);

comment on column klines.source is
'K-line source priority marker: 1=seed, 2=pytdx, 3=tdx_csv, 4=parquet_5f, 0=unknown.';
