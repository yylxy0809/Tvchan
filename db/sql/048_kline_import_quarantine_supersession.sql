-- Append-only evidence that an exact historical import-quarantine group was
-- superseded by a later, read-only canonical audit of the same symbol/scope.
-- The original quarantine rows are retained.  Counts and max ID fence the
-- snapshot so newly inserted quarantine rows fail closed automatically.

begin;

create table if not exists kline_import_quarantine_supersessions (
    supersession_id uuid primary key,
    source_import_run_id uuid not null
        references kline_import_runs(import_run_id) on delete restrict,
    reason text not null
        check (reason in ('ambiguous_volume_unit', 'missing_source_file')),
    symbol_id integer not null references symbols(id) on delete restrict,
    symbol text not null,
    timeframe text not null check (timeframe in ('5f', '30f', '1d', '1w', '1m')),
    quarantine_rows bigint not null check (quarantine_rows > 0),
    max_quarantine_id bigint not null check (max_quarantine_id > 0),
    canonical_audit_run_id uuid not null
        references kline_audit_runs(audit_run_id) on delete restrict,
    audit_evidence_sha256 varchar(64) not null
        check (audit_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    resolution_kind text not null default 'superseded_by_canonical_audit'
        check (resolution_kind = 'superseded_by_canonical_audit'),
    justification text not null
        check (length(btrim(justification)) between 1 and 1000),
    created_at timestamptz not null default now(),
    unique (
        source_import_run_id, reason, symbol_id, timeframe,
        quarantine_rows, max_quarantine_id, canonical_audit_run_id
    )
);

create index if not exists kline_import_quarantine_supersessions_lookup_idx
    on kline_import_quarantine_supersessions (
        canonical_audit_run_id, audit_evidence_sha256,
        source_import_run_id, reason, symbol, timeframe
    );

create or replace function reject_kline_import_quarantine_supersession_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception '% is append-only', tg_table_name;
end;
$$;

drop trigger if exists kline_import_quarantine_supersessions_append_only
    on kline_import_quarantine_supersessions;
create trigger kline_import_quarantine_supersessions_append_only
before update or delete on kline_import_quarantine_supersessions
for each row execute function reject_kline_import_quarantine_supersession_mutation();

-- Snapshot bounds are meaningful only while the source evidence itself cannot
-- be rewritten.  Importers already use insert/on-conflict-do-nothing.
drop trigger if exists kline_import_quarantine_append_only
    on kline_import_quarantine;
create trigger kline_import_quarantine_append_only
before update or delete on kline_import_quarantine
for each row execute function reject_kline_import_quarantine_supersession_mutation();

commit;
