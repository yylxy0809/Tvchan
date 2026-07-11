do $$
begin
    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = 'strategy_backtest_runs'
    ) and not exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = 'strategy_backtest_runs'
          and column_name = 'strategy_code'
    ) then
        if exists (
            select 1
            from information_schema.tables
            where table_schema = 'public'
              and table_name = 'strategy_backtest_runs_legacy'
        ) then
            execute 'drop table strategy_backtest_runs_legacy';
        end if;
        execute 'alter table strategy_backtest_runs rename to strategy_backtest_runs_legacy';
    end if;
end
$$;

create table if not exists symbol_fundamentals (
    symbol_id integer primary key references symbols(id),
    market_cap_x100 bigint,
    pe_ttm_x100 integer,
    pb_x100 integer,
    source varchar(32),
    as_of_date date,
    extra jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists strategy_definitions (
    id bigint generated always as identity primary key,
    strategy_code text not null,
    version text not null,
    strategy_name text not null,
    description text,
    rule_spec_json jsonb not null,
    enabled boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (strategy_code, version)
);

create table if not exists strategy_signal_events (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    strategy_code text not null,
    strategy_version text not null,
    event_type text not null,
    status text not null,
    source_namespace text not null default 'c',
    source_level text,
    source_signal_type text,
    source_signal_side text,
    point_time timestamptz,
    first_seen_time timestamptz,
    confirm_time timestamptz,
    disappear_time timestamptz,
    price_x1000 bigint,
    source_run_id bigint,
    source_snapshot_version varchar(255),
    source_head_id bigint,
    confidence_score numeric(8,4),
    strength_score numeric(8,4),
    features_json jsonb,
    reason_json jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_strategy_events_symbol_time
on strategy_signal_events(symbol_id, first_seen_time);

create index if not exists idx_strategy_events_strategy_status
on strategy_signal_events(strategy_code, strategy_version, status);

create index if not exists idx_strategy_events_type_time
on strategy_signal_events(event_type, first_seen_time);

create table if not exists strategy_contexts (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    strategy_code text not null,
    strategy_version text not null,
    context_type text not null,
    status text not null,
    start_time timestamptz not null,
    end_time timestamptz,
    weekly_b1_signal_id bigint,
    weekly_b2_signal_id bigint,
    daily_b1_signal_id bigint,
    daily_b2_signal_id bigint,
    weekly_b1_price_x1000 bigint,
    weekly_b2_price_x1000 bigint,
    daily_b1_price_x1000 bigint,
    daily_b2_price_x1000 bigint,
    source_run_id bigint,
    source_snapshot_version varchar(255),
    features_json jsonb,
    reason_json jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_strategy_contexts_symbol_time
on strategy_contexts(symbol_id, start_time desc);

create index if not exists idx_strategy_contexts_strategy_status
on strategy_contexts(strategy_code, strategy_version, status);

create table if not exists strategy_backtest_runs (
    id bigint generated always as identity primary key,
    strategy_code text not null,
    strategy_version text not null,
    run_name text,
    run_mode text not null default 'offline',
    start_time timestamptz not null,
    end_time timestamptz not null,
    rule_spec_json jsonb not null,
    data_source_json jsonb,
    notes text,
    total_symbols integer,
    total_trades integer,
    win_rate numeric(10,6),
    avg_return numeric(10,6),
    profit_factor numeric(10,6),
    max_drawdown numeric(10,6),
    avg_holding_bars numeric(10,2),
    created_at timestamptz not null default now()
);

create table if not exists strategy_backtest_trades (
    id bigint generated always as identity primary key,
    backtest_run_id bigint not null references strategy_backtest_runs(id),
    symbol_id integer not null references symbols(id),
    strategy_code text not null,
    strategy_version text not null,
    entry_time timestamptz not null,
    entry_price_x1000 bigint not null,
    entry_level text,
    entry_reason text,
    entry_confidence_score numeric(8,4),
    exit_time timestamptz,
    exit_price_x1000 bigint,
    exit_reason text,
    daily_b1_price_x1000 bigint,
    stop_price_x1000 bigint,
    return_pct numeric(12,6),
    max_favorable_pct numeric(12,6),
    max_adverse_pct numeric(12,6),
    holding_bars integer,
    holding_days integer,
    features_json jsonb,
    event_trace_json jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_strategy_backtest_trades_run_symbol
on strategy_backtest_trades(backtest_run_id, symbol_id);
