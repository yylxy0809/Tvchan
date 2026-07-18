-- Immutable, explicitly scoped PyTDX tail-fill runs.
-- Legacy provider-wide tasks remain isolated with run_id IS NULL.

begin;

create table if not exists historical_backfill_scoped_runs (
    run_id uuid primary key,
    run_identity char(64) not null unique,
    provider varchar(32) not null check (provider = 'pytdx'),
    manifest_sha256 char(64) not null,
    page_size integer not null check (page_size > 0),
    endpoint varchar(320) not null,
    source_policy varchar(64) not null,
    catalog_generation_id uuid not null
        references kline_scope_catalog_generations(generation_id) on delete restrict,
    catalog_revision bigint not null check (catalog_revision >= 0),
    symbol_count integer not null check (symbol_count > 0),
    timeframes integer[] not null check (cardinality(timeframes) > 0),
    stop_at jsonb not null,
    task_count integer not null check (task_count > 0),
    created_at timestamptz not null default clock_timestamp(),
    check (run_identity ~ '^[0-9a-f]{64}$'),
    check (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

alter table historical_backfill_tasks
    add column if not exists run_id uuid
        references historical_backfill_scoped_runs(run_id) on delete restrict,
    add column if not exists stop_at timestamptz;

do $$
declare
    constraint_name text;
begin
    select conname
      into constraint_name
      from pg_constraint
     where conrelid = 'historical_backfill_tasks'::regclass
       and contype = 'u'
       and pg_get_constraintdef(oid) = 'UNIQUE (symbol_id, timeframe, provider)';
    if constraint_name is not null then
        execute format(
            'alter table historical_backfill_tasks drop constraint %I',
            constraint_name
        );
    end if;
end
$$;

create unique index if not exists uq_historical_backfill_legacy_scope
    on historical_backfill_tasks (symbol_id, timeframe, provider)
    where run_id is null;

create unique index if not exists uq_historical_backfill_scoped_run_scope
    on historical_backfill_tasks (run_id, symbol_id, timeframe, provider)
    where run_id is not null;

do $$
begin
    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'historical_backfill_tasks'::regclass
           and conname = 'ck_historical_backfill_scoped_identity'
    ) then
        alter table historical_backfill_tasks
            add constraint ck_historical_backfill_scoped_identity
            check (
                (run_id is null and stop_at is null)
                or (run_id is not null and stop_at is not null)
            );
    end if;
end
$$;

create index if not exists idx_historical_backfill_scoped_claimable
    on historical_backfill_tasks (run_id, status, priority, updated_at, lease_until, id)
    where run_id is not null and status in ('pending', 'failed', 'running');

create or replace function enforce_historical_backfill_scoped_session_fence()
returns trigger
language plpgsql
as $$
declare
    expected_run_id text;
begin
    expected_run_id := nullif(
        current_setting('tvchan.history_backfill_scoped_run_id', true), ''
    );
    if old.run_id is not null and expected_run_id is distinct from old.run_id::text then
        raise exception using
            errcode = '55000',
            message = 'scoped historical backfill mutation requires exact session run fence';
    end if;
    if tg_op = 'UPDATE'
       and new.run_id is distinct from old.run_id then
        raise exception using
            errcode = '55000',
            message = 'scoped historical backfill run identity is immutable';
    end if;
    return case when tg_op = 'DELETE' then old else new end;
end
$$;

revoke all on function enforce_historical_backfill_scoped_session_fence() from public;

drop trigger if exists trg_historical_backfill_scoped_session_fence
    on historical_backfill_tasks;

create trigger trg_historical_backfill_scoped_session_fence
before update or delete on historical_backfill_tasks
for each row
when (old.run_id is not null)
execute function enforce_historical_backfill_scoped_session_fence();

revoke all on table historical_backfill_scoped_runs from public;

commit;
