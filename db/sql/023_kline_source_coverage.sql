-- Immutable imported-history boundary used to prefer pytdx only for newer bars.
create table if not exists kline_source_coverage (
    symbol_id bigint not null references symbols(id) on delete cascade,
    timeframe integer not null,
    source smallint not null check (source in (4, 9)),
    covered_until timestamptz not null,
    updated_at timestamptz not null default now(),
    primary key (symbol_id, timeframe, source)
);

create index if not exists kline_source_coverage_scope_idx
    on kline_source_coverage (symbol_id, timeframe, covered_until desc);
