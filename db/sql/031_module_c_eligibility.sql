-- Versioned, append-only disposition of every active symbol at every Module C level.
-- A build is inserted only after all rows have been evaluated by the collector.

create table if not exists module_c_eligibility_builds (
    build_id uuid primary key,
    manifest_version text not null unique,
    created_at timestamptz not null default now(),
    config_hash text not null,
    active_universe_hash text not null,
    manifest_hash text not null,
    active_symbols integer not null check (active_symbols >= 0),
    disposition_rows integer not null check (disposition_rows = active_symbols * 5),
    parameters jsonb not null default '{}'::jsonb,
    summary jsonb not null
);

create table if not exists module_c_eligibility (
    build_id uuid not null references module_c_eligibility_builds(build_id) on delete restrict,
    symbol_id integer not null references symbols(id) on delete restrict,
    symbol text not null,
    timeframe integer not null check (timeframe in (5, 30, 1440, 10080, 43200)),
    eligible boolean not null,
    reasons text[] not null,
    covered_until timestamptz,
    unresolved_rows bigint not null default 0 check (unresolved_rows >= 0),
    primary key (build_id, symbol_id, timeframe),
    check ((eligible and cardinality(reasons) = 0) or
           (not eligible and cardinality(reasons) > 0))
);

create index if not exists module_c_eligibility_lookup_idx
    on module_c_eligibility (build_id, timeframe, eligible, symbol_id);

create or replace function reject_module_c_eligibility_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception '% is append-only', tg_table_name;
end;
$$;

drop trigger if exists module_c_eligibility_builds_append_only
    on module_c_eligibility_builds;
create trigger module_c_eligibility_builds_append_only
before update or delete on module_c_eligibility_builds
for each row execute function reject_module_c_eligibility_mutation();

drop trigger if exists module_c_eligibility_append_only
    on module_c_eligibility;
create trigger module_c_eligibility_append_only
before update or delete on module_c_eligibility
for each row execute function reject_module_c_eligibility_mutation();
