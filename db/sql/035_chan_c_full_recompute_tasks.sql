-- Durable, fenced execution state for a Module C full-recompute batch.
-- The provenance columns were already used by the writer but were missing
-- from the repository migration chain.

create table if not exists chan_c_batches (
    id bigint generated always as identity primary key,
    batch_key varchar(128) not null unique,
    publication_namespace varchar(64) not null check (publication_namespace <> ''),
    profile_id varchar(64) not null check (profile_id <> ''),
    run_group_id varchar(64) not null check (run_group_id <> ''),
    batch_kind varchar(32) not null default 'baseline'
        check (batch_kind in ('baseline', 'canary', 'tail', 'historical_replay', 'diagnostic')),
    status varchar(16) not null default 'planned'
        check (status in ('planned', 'running', 'sealed', 'failed', 'aborted')),
    code_commit varchar(64) not null,
    image_digest varchar(128) not null,
    vendor_manifest_sha256 varchar(64) not null
        check (vendor_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    effective_config jsonb not null check (jsonb_typeof(effective_config) = 'object'),
    config_hash varchar(128) not null,
    eligible_manifest_uri text,
    eligible_manifest_sha256 varchar(64) not null
        check (eligible_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    input_watermark jsonb not null default '{}'::jsonb
        check (jsonb_typeof(input_watermark) = 'object'),
    audit_references jsonb not null default '[]'::jsonb
        check (jsonb_typeof(audit_references) = 'array'),
    created_at timestamptz not null default clock_timestamp(),
    sealed_at timestamptz,
    sealed_by varchar(128),
    aborted_at timestamptz,
    abort_reason text,
    notes text,
    unique (publication_namespace, profile_id, run_group_id),
    check ((status = 'sealed') = (sealed_at is not null)),
    check (status <> 'aborted' or aborted_at is not null)
);

alter table if exists chan_c_runs
    add column if not exists batch_id bigint,
    add column if not exists publication_namespace varchar(64),
    add column if not exists profile_id varchar(64),
    add column if not exists base_timeframe integer,
    add column if not exists run_identity varchar(128),
    add column if not exists provenance jsonb not null default '{}'::jsonb;

alter table if exists scheme2_chan_c_published_heads
    add column if not exists batch_id bigint,
    add column if not exists publication_namespace varchar(64),
    add column if not exists profile_id varchar(64),
    add column if not exists run_group_id varchar(64),
    add column if not exists publication_event_id uuid;

do $$
begin
    if not exists (select 1 from pg_constraint where conname = 'chan_c_runs_batch_id_fkey') then
        alter table chan_c_runs add constraint chan_c_runs_batch_id_fkey
            foreign key (batch_id) references chan_c_batches(id);
    end if;
    if not exists (select 1 from pg_constraint where conname = 'scheme2_chan_c_published_heads_batch_id_fkey') then
        alter table scheme2_chan_c_published_heads
            add constraint scheme2_chan_c_published_heads_batch_id_fkey
            foreign key (batch_id) references chan_c_batches(id);
    end if;
end
$$;

create unique index if not exists uq_chan_c_runs_scope_key
    on chan_c_runs (id, symbol_id, chan_level, base_timeframe);

create index if not exists idx_chan_c_runs_batch_scope
    on chan_c_runs (batch_id, symbol_id, chan_level, base_timeframe, status, bar_until desc)
    where batch_id is not null;

create unique index if not exists uq_chan_c_runs_batch_identity
    on chan_c_runs (batch_id, run_identity)
    where batch_id is not null and run_identity is not null;

create index if not exists idx_scheme2_chan_c_heads_batch_scope
    on scheme2_chan_c_published_heads
       (batch_id, publication_namespace, profile_id, symbol_id, chan_level, mode)
    where batch_id is not null;

create table if not exists chan_c_full_recompute_batches (
    batch_id bigint primary key references chan_c_batches(id) on delete restrict,
    eligibility_build_id uuid not null references module_c_eligibility_builds(build_id) on delete restrict,
    run_group_id varchar(64) not null,
    config_hash varchar(128) not null,
    publication_namespace varchar(64) not null,
    profile_id varchar(64) not null,
    shard_count integer not null check (shard_count > 0),
    status varchar(16) not null default 'pending'
        check (status in ('pending', 'running', 'completed', 'failed', 'stopped')),
    active_symbols integer not null check (active_symbols >= 0),
    disposition_rows integer not null check (disposition_rows >= 0),
    created_at timestamptz not null default clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz not null default clock_timestamp()
);

create table if not exists chan_c_full_recompute_tasks (
    batch_id bigint not null references chan_c_full_recompute_batches(batch_id) on delete restrict,
    symbol_id integer not null references symbols(id) on delete restrict,
    symbol text not null,
    chan_level integer not null check (chan_level in (5, 30, 1440, 10080, 43200)),
    eligible boolean not null,
    exclusion_reasons text[] not null default '{}',
    target_bar_until timestamptz,
    shard_bucket smallint not null check (shard_bucket between 0 and 1023),
    status varchar(16) not null
        check (status in ('pending', 'running', 'completed', 'failed', 'excluded')),
    attempts integer not null default 0 check (attempts >= 0),
    worker_id varchar(128),
    claim_token varchar(64),
    lease_version bigint not null default 0,
    lease_until timestamptz,
    lease_heartbeat_at timestamptz,
    expected_heads jsonb not null default '{}'::jsonb,
    run_id bigint references chan_c_runs(id) on delete restrict,
    bar_count integer,
    stroke_count integer,
    segment_count integer,
    center_count integer,
    signal_count integer,
    last_error text,
    created_at timestamptz not null default clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz not null default clock_timestamp(),
    primary key (batch_id, symbol_id, chan_level),
    check ((eligible and cardinality(exclusion_reasons) = 0 and target_bar_until is not null)
        or (not eligible and cardinality(exclusion_reasons) > 0 and status = 'excluded'))
);

create index if not exists idx_chan_c_full_recompute_tasks_claim
    on chan_c_full_recompute_tasks
       (batch_id, shard_bucket, status, lease_until, attempts, symbol_id, chan_level);

create index if not exists idx_chan_c_full_recompute_tasks_progress
    on chan_c_full_recompute_tasks (batch_id, status, chan_level, updated_at desc);

comment on table chan_c_full_recompute_batches is
'Frozen Module C full-recompute manifest bound to one eligibility build and configuration.';

comment on table chan_c_full_recompute_tasks is
'Level-specific full-recompute tasks. claim_token and lease_version fence stale workers from publishing or completing work.';
