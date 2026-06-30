create table if not exists chan_level_state_snapshots (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null default 5,
    snapshot_version varchar(255) not null,
    run_id bigint not null references chan_runs(id),
    asof_base_ts timestamptz,
    source_bar_until timestamptz,
    bar_count integer,
    latest_stroke_seq integer,
    latest_stroke_direction smallint,
    latest_stroke_confirmed boolean,
    latest_stroke_begin_base_ts timestamptz,
    latest_stroke_end_base_ts timestamptz,
    latest_segment_seq integer,
    latest_segment_direction smallint,
    latest_segment_confirmed boolean,
    latest_segment_begin_base_ts timestamptz,
    latest_segment_end_base_ts timestamptz,
    has_active_center boolean not null default false,
    active_center_seq integer,
    center_low_x1000 integer,
    center_high_x1000 integer,
    center_count integer not null default 0,
    structure_state varchar(32) not null default 'unknown',
    structure_direction smallint,
    last_signal_type varchar(32),
    last_signal_side varchar(16),
    last_signal_bsp_type varchar(32),
    last_signal_base_ts timestamptz,
    is_complete boolean not null default false,
    warnings jsonb not null default '{}'::jsonb,
    definition_version varchar(64) not null,
    computed_at timestamptz not null default now()
);

comment on table chan_level_state_snapshots is
'Query-ready Chan level state. Derived from published chan_* structures; source structures remain the authoritative drawing data.';

comment on column chan_level_state_snapshots.structure_state is
'chan-state-v1: no_center when no center exists in the current segment, consolidation when one/overlapping center exists, trend when the latest two centers are non-overlapping in the same direction.';

create unique index if not exists uq_chan_level_state_scope
on chan_level_state_snapshots (symbol_id, chan_level, mode, base_timeframe);

create index if not exists idx_chan_level_state_screen
on chan_level_state_snapshots (
    chan_level,
    mode,
    structure_state,
    structure_direction,
    latest_segment_direction,
    latest_stroke_direction
);

create index if not exists idx_chan_level_state_signal
on chan_level_state_snapshots (
    chan_level,
    mode,
    last_signal_type,
    last_signal_bsp_type,
    last_signal_side
);

create table if not exists chan_cross_level_states (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    snapshot_version varchar(255) not null,
    mode varchar(32) not null,
    parent_level integer not null,
    parent_structure_type varchar(16) not null,
    parent_seq integer not null,
    parent_run_id bigint not null references chan_runs(id),
    parent_direction smallint,
    parent_begin_base_ts timestamptz,
    parent_end_base_ts timestamptz,
    child_level integer not null,
    child_run_id bigint not null references chan_runs(id),
    child_stroke_count integer not null default 0,
    child_segment_count integer not null default 0,
    child_center_count integer not null default 0,
    child_latest_stroke_direction smallint,
    child_latest_segment_direction smallint,
    child_last_signal_type varchar(32),
    is_current boolean not null default true,
    definition_version varchar(64) not null,
    computed_at timestamptz not null default now()
);

comment on table chan_cross_level_states is
'Current cross-level nesting summary. Used by Chan natural-language screening such as parent stroke contains child strokes/segments.';

create unique index if not exists uq_chan_cross_level_current_scope
on chan_cross_level_states (
    symbol_id,
    snapshot_version,
    mode,
    parent_level,
    parent_structure_type,
    parent_seq,
    child_level,
    definition_version
);

create index if not exists idx_chan_cross_level_screen
on chan_cross_level_states (
    parent_level,
    child_level,
    mode,
    parent_direction,
    child_latest_stroke_direction,
    child_latest_segment_direction
);
