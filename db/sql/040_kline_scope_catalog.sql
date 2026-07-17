-- Generation-fenced existence and boundary metadata for canonical K-line scopes.
-- This migration intentionally adds no index or trigger to the klines hypertable.

create table if not exists kline_scope_catalog_generations (
    generation_id uuid primary key,
    status varchar(16) not null default 'building'
        check (status in ('building', 'complete', 'failed', 'superseded')),
    expected_scope_count bigint not null check (expected_scope_count > 0),
    symbol_ids integer[] not null check (cardinality(symbol_ids) > 0),
    timeframes integer[] not null check (cardinality(timeframes) > 0),
    created_at timestamptz not null default clock_timestamp(),
    completed_at timestamptz,
    failed_at timestamptz,
    failure text,
    check (expected_scope_count = cardinality(symbol_ids)::bigint * cardinality(timeframes)::bigint),
    check (
        (status = 'building' and completed_at is null and failed_at is null)
        or (status in ('complete', 'superseded') and completed_at is not null and failed_at is null)
        or (status = 'failed' and completed_at is null and failed_at is not null)
    )
);

create table if not exists kline_scope_catalog (
    generation_id uuid not null
        references kline_scope_catalog_generations(generation_id) on delete restrict,
    symbol_id integer not null references symbols(id) on delete restrict,
    timeframe integer not null check (timeframe in (5, 15, 30, 60, 1440, 10080, 43200)),
    state varchar(16) not null default 'unknown'
        check (state in ('unknown', 'present', 'empty')),
    bounds_complete boolean not null default false,
    min_ts timestamptz,
    max_ts timestamptz,
    updated_at timestamptz not null default clock_timestamp(),
    primary key (generation_id, symbol_id, timeframe),
    check (
        (state = 'unknown' and not bounds_complete and min_ts is null and max_ts is null)
        or (state = 'present' and (
            (bounds_complete and min_ts is not null and max_ts is not null and min_ts <= max_ts)
            or (not bounds_complete and (min_ts is null or max_ts is null or min_ts <= max_ts))
        ))
        or (state = 'empty' and bounds_complete and min_ts is null and max_ts is null)
    )
);

create table if not exists kline_scope_catalog_control (
    control_key varchar(16) primary key check (control_key = 'active'),
    active_generation_id uuid
        references kline_scope_catalog_generations(generation_id) on delete restrict,
    updated_at timestamptz not null default clock_timestamp()
);

insert into kline_scope_catalog_control(control_key, active_generation_id)
values ('active', null)
on conflict (control_key) do nothing;

create or replace view active_kline_scope_catalog as
select catalog.generation_id,
       catalog.symbol_id,
       catalog.timeframe,
       catalog.state,
       catalog.bounds_complete,
       catalog.min_ts,
       catalog.max_ts,
       catalog.updated_at
  from kline_scope_catalog_control control
  join kline_scope_catalog_generations generation
    on generation.generation_id = control.active_generation_id
   and generation.status = 'complete'
  join kline_scope_catalog catalog
    on catalog.generation_id = generation.generation_id
 where control.control_key = 'active'
   and catalog.bounds_complete
   and catalog.state in ('present', 'empty');

comment on view active_kline_scope_catalog is
'Only the atomically activated complete generation. Building and failed generations are never exposed.';

revoke all on table kline_scope_catalog_generations from public;
revoke all on table kline_scope_catalog from public;
revoke all on table kline_scope_catalog_control from public;
revoke all on table active_kline_scope_catalog from public;
