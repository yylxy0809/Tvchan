-- Permanently exclude symbols that cannot satisfy the native five-level data
-- contract. Historical K-lines and prior runs remain immutable; runtime code
-- already consumes symbols through is_active=true.

begin;

create table if not exists symbol_data_availability_exclusion_runs (
    exclusion_run_id uuid primary key,
    canonical_audit_run_id uuid not null
        references kline_audit_runs(audit_run_id) on delete restrict,
    audit_evidence_sha256 varchar(64) not null
        check (audit_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    audit_active_universe_sha256 varchar(64) not null
        check (audit_active_universe_sha256 ~ '^[0-9a-f]{64}$'),
    manifest_sha256 varchar(64) not null
        check (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    required_timeframes integer[] not null
        check (required_timeframes = array[5,30,1440,10080,43200]),
    excluded_symbols integer not null check (excluded_symbols > 0),
    justification text not null
        check (length(btrim(justification)) between 1 and 1000),
    created_at timestamptz not null default clock_timestamp(),
    unique (canonical_audit_run_id, audit_evidence_sha256, manifest_sha256)
);

create table if not exists symbol_data_availability_exclusions (
    exclusion_run_id uuid not null
        references symbol_data_availability_exclusion_runs(exclusion_run_id)
        on delete restrict,
    symbol_id integer primary key references symbols(id) on delete restrict,
    symbol text not null,
    unavailable_timeframes integer[] not null
        check (
            cardinality(unavailable_timeframes) > 0
            and unavailable_timeframes <@ array[5,30,1440,10080,43200]
        ),
    reason text not null default 'required_canonical_data_unavailable'
        check (reason = 'required_canonical_data_unavailable'),
    created_at timestamptz not null default clock_timestamp()
);

create index if not exists symbol_data_availability_exclusions_run_idx
    on symbol_data_availability_exclusions(exclusion_run_id, symbol_id);

create or replace function reject_symbol_data_availability_exclusion_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception '% is append-only', tg_table_name;
end;
$$;

drop trigger if exists symbol_data_availability_exclusion_runs_append_only
    on symbol_data_availability_exclusion_runs;
create trigger symbol_data_availability_exclusion_runs_append_only
before update or delete on symbol_data_availability_exclusion_runs
for each row execute function reject_symbol_data_availability_exclusion_mutation();

drop trigger if exists symbol_data_availability_exclusions_append_only
    on symbol_data_availability_exclusions;
create trigger symbol_data_availability_exclusions_append_only
before update or delete on symbol_data_availability_exclusions
for each row execute function reject_symbol_data_availability_exclusion_mutation();

-- Provider/master refreshes may continue to see an excluded listing. Preserve
-- the permanent project exclusion instead of silently reactivating it.
create or replace function preserve_symbol_data_availability_exclusion()
returns trigger
language plpgsql
as $$
begin
    if new.is_active and exists (
        select 1
          from symbol_data_availability_exclusions exclusion
         where exclusion.symbol_id = new.id
    ) then
        new.is_active := false;
    end if;
    return new;
end;
$$;

drop trigger if exists preserve_symbol_data_availability_exclusion_on_symbol
    on symbols;
create trigger preserve_symbol_data_availability_exclusion_on_symbol
before update of is_active on symbols
for each row execute function preserve_symbol_data_availability_exclusion();

revoke all on function reject_symbol_data_availability_exclusion_mutation()
    from public;
revoke all on function preserve_symbol_data_availability_exclusion()
    from public;

commit;
