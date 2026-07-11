create table if not exists scheme2_market_fetch_tasks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    timeframe integer not null,
    status varchar(16) not null default 'pending',
    priority integer not null default 100,
    attempts integer not null default 0,
    worker_id varchar(128),
    claim_token varchar(64),
    lease_version bigint not null default 0,
    lease_until timestamptz,
    last_bar_end timestamptz,
    last_success_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (symbol_id, timeframe)
);

create index if not exists idx_scheme2_market_fetch_tasks_claim
on scheme2_market_fetch_tasks (status, lease_until, priority, updated_at, id);

create table if not exists scheme2_market_fetch_attempts (
    attempt_id varchar(64) primary key,
    symbol_id integer not null references symbols(id),
    timeframe integer not null,
    source varchar(32),
    policy varchar(32) not null,
    winning_source varchar(32),
    status varchar(16) not null,
    latency_ms integer not null default 0,
    quality_flags jsonb not null default '{}'::jsonb,
    error_message text,
    observed_at timestamptz not null default now()
);

create index if not exists idx_scheme2_market_fetch_attempts_symbol_time
on scheme2_market_fetch_attempts (symbol_id, timeframe, observed_at desc);

create table if not exists scheme2_market_candidate_bars (
    id bigint generated always as identity primary key,
    attempt_id varchar(64) not null references scheme2_market_fetch_attempts(attempt_id) on delete cascade,
    symbol_id integer not null references symbols(id),
    timeframe integer not null,
    ts timestamptz not null,
    source varchar(32) not null,
    open_x1000 integer not null,
    high_x1000 integer not null,
    low_x1000 integer not null,
    close_x1000 integer not null,
    volume bigint not null default 0,
    amount_x100 bigint,
    reason varchar(64) not null,
    quality_flags jsonb not null default '{}'::jsonb,
    observed_at timestamptz not null default now()
);

create index if not exists idx_scheme2_market_candidate_bars_attempt
on scheme2_market_candidate_bars (attempt_id, source, ts);

create table if not exists scheme2_chan_tail_tasks (
    id bigint generated always as identity primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode varchar(32) not null,
    base_timeframe integer not null default 5,
    status varchar(16) not null default 'pending',
    priority integer not null default 100,
    attempts integer not null default 0,
    worker_id varchar(128),
    claim_token varchar(64),
    lease_version bigint not null default 0,
    lease_until timestamptz,
    anchor_bar_end timestamptz not null,
    target_bar_end timestamptz not null,
    expected_head_run_id bigint,
    expected_head_base_to_bar_end timestamptz,
    last_success_bar_end timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (symbol_id, chan_level, mode, base_timeframe)
);

create index if not exists idx_scheme2_chan_tail_tasks_claim
on scheme2_chan_tail_tasks (status, lease_until, priority, updated_at, id);

comment on table scheme2_market_fetch_tasks is
'Realtime market fetch task lease table. claim_token/lease_version fence old workers from writing back after lease expiry.';

comment on table scheme2_market_candidate_bars is
'Bounded abnormal candidate bars retained only for source conflict, fallback, quality failure, or hedged loser diagnostics.';

comment on table scheme2_chan_tail_tasks is
'Realtime Chan tail publish task lease table. The published head remains unchanged until a claimed worker atomically publishes a newer run.';

comment on column klines.source is
'K-line source priority marker: 1=seed, 2=pytdx, 3=tdx_csv, 4=parquet_5f, 5=mootdx, 6=tencent, 7=baidu, 0=unknown.';
