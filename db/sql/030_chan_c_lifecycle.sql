-- Module C publication history and lifecycle evidence.  These tables are
-- append-only except for the explicitly rebuildable current projection.

alter table if exists scheme2_chan_c_published_heads
    add column if not exists config_hash varchar(128);

create table if not exists chan_c_head_history (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null,
    config_hash varchar(128) not null,
    publication_profile varchar(32) not null check (publication_profile in ('baseline', 'online', 'historical_replay')),
    run_group_id varchar(128),
    old_run_id bigint references chan_c_runs(id),
    new_run_id bigint not null references chan_c_runs(id),
    old_base_to_bar_end timestamptz,
    new_base_to_bar_end timestamptz not null,
    snapshot_version varchar(255) not null,
    worker_id varchar(128),
    claim_token varchar(128),
    source varchar(64) not null,
    provenance jsonb not null default '{}'::jsonb,
    published_at timestamptz not null default clock_timestamp(),
    unique (symbol_id, chan_level, mode, base_timeframe, new_run_id)
);

create index if not exists idx_chan_c_head_history_scope_time
    on chan_c_head_history (symbol_id, chan_level, mode, base_timeframe, published_at, id);

create table if not exists chan_c_head_outbox (
    id bigint generated always as identity primary key,
    head_history_id bigint not null unique references chan_c_head_history(id) on delete restrict,
    status varchar(16) not null default 'pending' check (status in ('pending', 'processing', 'completed')),
    lease_version integer not null default 0,
    lease_token varchar(128),
    lease_until timestamptz,
    attempts integer not null default 0,
    payload jsonb not null,
    processed_at timestamptz,
    created_at timestamptz not null default clock_timestamp(),
    updated_at timestamptz not null default clock_timestamp()
);

create index if not exists idx_chan_c_head_outbox_due
    on chan_c_head_outbox (status, lease_until, id);

create table if not exists chan_structure_identity (
    fingerprint varchar(64) primary key,
    identity_version smallint not null,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    structure_type varchar(16) not null check (structure_type in ('stroke', 'segment', 'center', 'signal')),
    side_or_direction varchar(32),
    bsp_type varchar(32),
    point_time timestamptz not null,
    end_time timestamptz,
    price_x1000 integer,
    start_price_x1000 integer,
    end_price_x1000 integer,
    low_x1000 integer,
    high_x1000 integer,
    config_hash varchar(128) not null,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default clock_timestamp()
);

create index if not exists idx_chan_structure_identity_scope_time
    on chan_structure_identity (symbol_id, chan_level, structure_type, point_time);

create table if not exists chan_structure_lifecycle_events (
    id bigint generated always as identity primary key,
    fingerprint varchar(64) not null references chan_structure_identity(fingerprint) on delete restrict,
    head_history_id bigint not null references chan_c_head_history(id) on delete restrict,
    event_type varchar(32) not null check (event_type in ('baseline_observed', 'first_seen', 'confirmed', 'disappeared', 'reappeared')),
    effective_time timestamptz not null,
    point_time timestamptz not null,
    previous_mode varchar(32),
    current_mode varchar(32),
    run_id bigint references chan_c_runs(id),
    provenance jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default clock_timestamp(),
    unique (fingerprint, event_type, head_history_id)
);

create index if not exists idx_chan_structure_lifecycle_events_fingerprint_time
    on chan_structure_lifecycle_events (fingerprint, effective_time, id);

create table if not exists chan_structure_lifecycle_current (
    fingerprint varchar(64) primary key references chan_structure_identity(fingerprint) on delete restrict,
    point_time timestamptz not null,
    first_seen_time timestamptz,
    confirm_time timestamptz,
    disappear_time timestamptz,
    current_status varchar(32) not null,
    current_mode varchar(32),
    first_seen_run_id bigint references chan_c_runs(id),
    confirmed_run_id bigint references chan_c_runs(id),
    last_seen_run_id bigint references chan_c_runs(id),
    provenance jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default clock_timestamp()
);

create table if not exists chan_lifecycle_observer_watermarks (
    observer_name varchar(128) primary key,
    last_outbox_id bigint not null default 0,
    updated_at timestamptz not null default clock_timestamp()
);
