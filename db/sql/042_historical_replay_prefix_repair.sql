-- Durable, narrowly-scoped audit evidence for an exceptional historical replay
-- prefix repair.  Canonical run/head/task/K-line data is deliberately outside
-- this schema.

begin;

create table if not exists chan_c_historical_replay_prefix_repairs (
    repair_id uuid primary key,
    batch_id bigint not null references chan_c_batches(id) on delete restrict,
    contract_version varchar(96) not null,
    replay_contract_hash varchar(64) not null
        check (replay_contract_hash ~ '^[0-9a-f]{64}$'),
    manifest_sha256 varchar(64) not null
        check (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    manifest jsonb not null,
    before_event_set_sha256 varchar(64) not null
        check (before_event_set_sha256 ~ '^[0-9a-f]{64}$'),
    before_event_identity_sha256 varchar(64) not null
        check (before_event_identity_sha256 ~ '^[0-9a-f]{64}$'),
    target_event_set_sha256 varchar(64) not null
        check (target_event_set_sha256 ~ '^[0-9a-f]{64}$'),
    status varchar(24) not null
        check (status in ('applied', 'verified', 'rolled_back')),
    applied_by varchar(128) not null,
    applied_at timestamptz not null default clock_timestamp(),
    verified_by varchar(128),
    verified_at timestamptz,
    verification jsonb,
    rolled_back_by varchar(128),
    rolled_back_at timestamptz,
    rollback_verification jsonb,
    check (
        (status = 'applied'
         and verified_by is null and verified_at is null and verification is null
         and rolled_back_by is null and rolled_back_at is null
         and rollback_verification is null)
        or
        (status = 'verified'
         and verified_by is not null and verified_at is not null
         and verification is not null
         and rolled_back_by is null and rolled_back_at is null
         and rollback_verification is null)
        or
        (status = 'rolled_back'
         and rolled_back_by is not null and rolled_back_at is not null
         and rollback_verification is not null)
    ),
    unique (batch_id, manifest_sha256)
);

create table if not exists chan_c_historical_replay_prefix_repair_snapshots (
    repair_id uuid not null
        references chan_c_historical_replay_prefix_repairs(repair_id) on delete restrict,
    history_id bigint not null,
    outbox_id bigint not null,
    history_before jsonb not null,
    outbox_before jsonb not null,
    events_before jsonb not null,
    target_history jsonb not null,
    target_payload jsonb not null,
    target_event_set_sha256 varchar(64) not null
        check (target_event_set_sha256 ~ '^[0-9a-f]{64}$'),
    primary key (repair_id, history_id),
    unique (repair_id, outbox_id)
);

create or replace function reject_historical_replay_prefix_snapshot_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception 'historical replay prefix repair snapshots are append-only';
end
$$;

revoke all on function reject_historical_replay_prefix_snapshot_mutation() from public;

drop trigger if exists trg_historical_replay_prefix_snapshot_append_only
    on chan_c_historical_replay_prefix_repair_snapshots;

create trigger trg_historical_replay_prefix_snapshot_append_only
before update or delete on chan_c_historical_replay_prefix_repair_snapshots
for each row execute function reject_historical_replay_prefix_snapshot_mutation();

create or replace function guard_historical_replay_prefix_repair_header()
returns trigger
language plpgsql
as $$
begin
    if new.repair_id is distinct from old.repair_id
       or new.batch_id is distinct from old.batch_id
       or new.contract_version is distinct from old.contract_version
       or new.replay_contract_hash is distinct from old.replay_contract_hash
       or new.manifest_sha256 is distinct from old.manifest_sha256
       or new.manifest is distinct from old.manifest
       or new.before_event_set_sha256 is distinct from old.before_event_set_sha256
       or new.before_event_identity_sha256 is distinct from old.before_event_identity_sha256
       or new.target_event_set_sha256 is distinct from old.target_event_set_sha256
       or new.applied_by is distinct from old.applied_by
       or new.applied_at is distinct from old.applied_at then
        raise exception 'historical replay prefix repair core audit fields are immutable';
    end if;

    if old.status = 'applied' and new.status = 'verified' then
        if new.verified_by is null or new.verified_at is null or new.verification is null
           or new.rolled_back_by is not null or new.rolled_back_at is not null
           or new.rollback_verification is not null then
            raise exception 'invalid applied-to-verified repair evidence';
        end if;
    elsif old.status in ('applied', 'verified') and new.status = 'rolled_back' then
        if new.rolled_back_by is null or new.rolled_back_at is null
           or new.rollback_verification is null then
            raise exception 'invalid repair rollback evidence';
        end if;
        if old.status = 'applied'
           and (new.verified_by is not null or new.verified_at is not null
                or new.verification is not null) then
            raise exception 'applied repair cannot invent verification during rollback';
        end if;
        if old.status = 'verified'
           and (new.verified_by is distinct from old.verified_by
                or new.verified_at is distinct from old.verified_at
                or new.verification is distinct from old.verification) then
            raise exception 'verified repair evidence is immutable during rollback';
        end if;
    else
        raise exception 'invalid historical replay prefix repair state transition: % -> %',
            old.status, new.status;
    end if;
    return new;
end
$$;

revoke all on function guard_historical_replay_prefix_repair_header() from public;

drop trigger if exists trg_historical_replay_prefix_repair_header_guard
    on chan_c_historical_replay_prefix_repairs;

create trigger trg_historical_replay_prefix_repair_header_guard
before update on chan_c_historical_replay_prefix_repairs
for each row execute function guard_historical_replay_prefix_repair_header();

commit;
