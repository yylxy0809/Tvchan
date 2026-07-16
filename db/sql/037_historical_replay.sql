-- Durable historical replay execution, isolated from baseline and online heads.

create table if not exists chan_c_historical_replay_batches (
    batch_id bigint primary key references chan_c_batches(id) on delete restrict,
    source_batch_id bigint not null references chan_c_batches(id) on delete restrict,
    contract_version varchar(64) not null,
    contract_hash varchar(64) not null check (contract_hash ~ '^[0-9a-f]{64}$'),
    contract jsonb not null check (jsonb_typeof(contract) = 'object'),
    eligible_universe_snapshot_id text not null,
    canonical_gate_snapshot_id text not null,
    cutoff_policy varchar(128) not null,
    status varchar(16) not null default 'planned'
        check (status in ('planned', 'running', 'completed', 'failed', 'sealed', 'stopped')),
    worker_limit smallint not null default 2 check (worker_limit between 1 and 4),
    concurrency_per_worker smallint not null default 1 check (concurrency_per_worker = 1),
    rss_limit_mb integer not null default 1200 check (rss_limit_mb between 256 and 1200),
    max_active_queries smallint not null default 2 check (max_active_queries between 1 and 2),
    created_at timestamptz not null default clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz not null default clock_timestamp(),
    unique (contract_hash, source_batch_id)
);

create table if not exists chan_c_historical_replay_tasks (
    id bigint generated always as identity primary key,
    batch_id bigint not null references chan_c_historical_replay_batches(batch_id) on delete restrict,
    symbol_id integer not null references symbols(id) on delete restrict,
    symbol text not null,
    chan_level integer not null check (chan_level in (5, 30, 1440, 10080, 43200)),
    mode varchar(64) not null check (mode <> ''),
    cutoff_time timestamptz not null,
    contract_version varchar(64) not null,
    replay_identity varchar(64) not null check (replay_identity ~ '^[0-9a-f]{64}$'),
    eligible boolean not null,
    exclusion_reasons text[] not null default '{}',
    status varchar(16) not null default 'pending'
        check (status in ('pending', 'running', 'completed', 'failed', 'excluded')),
    attempts integer not null default 0 check (attempts >= 0),
    worker_id varchar(128),
    claim_token varchar(64),
    lease_version bigint not null default 0,
    lease_until timestamptz,
    lease_heartbeat_at timestamptz,
    run_id bigint references chan_c_runs(id) on delete restrict,
    bar_count integer,
    stroke_count integer,
    segment_count integer,
    center_count integer,
    signal_count integer,
    last_error text,
    failure jsonb,
    created_at timestamptz not null default clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz not null default clock_timestamp(),
    unique (batch_id, symbol_id, chan_level, mode, cutoff_time, contract_version),
    unique (batch_id, replay_identity),
    check ((eligible and cardinality(exclusion_reasons) = 0)
        or (not eligible and cardinality(exclusion_reasons) > 0 and status = 'excluded'))
);

create index if not exists idx_chan_c_historical_replay_tasks_claim
    on chan_c_historical_replay_tasks (batch_id, status, lease_until, attempts, cutoff_time, symbol_id, chan_level);

create table if not exists chan_c_historical_replay_heads (
    batch_id bigint not null references chan_c_historical_replay_batches(batch_id) on delete restrict,
    task_id bigint not null references chan_c_historical_replay_tasks(id) on delete restrict,
    symbol_id integer not null references symbols(id) on delete restrict,
    chan_level integer not null,
    mode varchar(32) not null,
    cutoff_time timestamptz not null,
    run_id bigint not null references chan_c_runs(id) on delete restrict,
    config_hash varchar(128) not null,
    contract_version varchar(64) not null,
    replay_identity varchar(64) not null,
    published_at timestamptz not null default clock_timestamp(),
    primary key (batch_id, symbol_id, chan_level, mode, cutoff_time),
    unique (batch_id, task_id, mode),
    unique (batch_id, replay_identity, mode)
);

create index if not exists idx_chan_c_historical_replay_heads_scope
    on chan_c_historical_replay_heads (batch_id, symbol_id, chan_level, mode, cutoff_time);

comment on table chan_c_historical_replay_heads is
'Immutable official replay heads by cutoff. These do not overwrite baseline or online current heads.';
