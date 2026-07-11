-- Compact read model for chart viewport requests at weekly and monthly levels.
-- Canonical klines remain authoritative; this table is filled explicitly by
-- the chart-period backfill and never replaces source data.

create table if not exists chart_period_bars (
    symbol_id bigint not null references symbols(id) on delete cascade,
    timeframe integer not null check (timeframe in (10080, 43200)),
    ts timestamptz not null,
    open_x1000 integer not null,
    high_x1000 integer not null,
    low_x1000 integer not null,
    close_x1000 integer not null,
    volume bigint not null,
    amount_x100 bigint,
    is_complete boolean not null,
    revision integer not null default 0,
    refreshed_at timestamptz not null default now(),
    primary key (symbol_id, timeframe, ts)
);

comment on table chart_period_bars is
    'Read-only chart projection of canonical weekly/monthly bars; klines remains authoritative.';
