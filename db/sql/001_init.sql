create extension if not exists timescaledb;

create table if not exists symbols (
    id integer generated always as identity primary key,
    code varchar(16) not null,
    exchange varchar(8) not null,
    name varchar(64) not null,
    asset_type varchar(16) not null default 'stock',
    market varchar(16) not null default 'A_SHARE',
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_symbols_name on symbols (name);
create index if not exists idx_symbols_exchange_code on symbols (exchange, code);
create unique index if not exists uq_symbols_exchange_code on symbols (exchange, code);

create table if not exists klines (
    symbol_id integer not null references symbols(id),
    timeframe integer not null,
    ts timestamptz not null,
    open_x1000 integer not null,
    high_x1000 integer not null,
    low_x1000 integer not null,
    close_x1000 integer not null,
    volume bigint not null default 0,
    amount_x100 bigint,
    is_complete boolean not null default true,
    revision integer not null default 0,
    source smallint not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol_id, timeframe, ts)
);

comment on column klines.timeframe is
'Minutes-based code: 5=5f, 15=15f, 30=30f, 60=1h, 1440=1d, 10080=1w, 43200=1m/monthly. 1m means month, not 1-minute.';

comment on column klines.source is
'Data source code: 1=seed deterministic sample, 2=pytdx real quote server, 3=tdx_csv local zipped CSV, 0=unknown.';

select create_hypertable('klines', 'ts', if_not_exists => true);

create index if not exists idx_klines_symbol_timeframe_ts
on klines (symbol_id, timeframe, ts desc);

create index if not exists idx_klines_symbol_timeframe_source_ts
on klines (symbol_id, timeframe, source, ts desc);

insert into symbols (code, exchange, name)
values
    ('000001', 'SZ', '平安银行'),
    ('000002', 'SZ', '万科A'),
    ('000063', 'SZ', '中兴通讯'),
    ('000333', 'SZ', '美的集团'),
    ('000651', 'SZ', '格力电器'),
    ('600000', 'SH', '浦发银行'),
    ('600519', 'SH', '贵州茅台'),
    ('600887', 'SH', '伊利股份'),
    ('601318', 'SH', '中国平安'),
    ('601398', 'SH', '工商银行')
on conflict (exchange, code) do update
set name = excluded.name,
    updated_at = now();

-- Compression policy is intentionally not enabled in Phase 1.
-- After the Phase 2 30-symbol storage test, enable Timescale compression/columnstore
-- with segmenting by symbol_id,timeframe and ordering by ts.
