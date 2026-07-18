-- Durable ownership and retry fencing for Scheme 2 parquet member checkpoints.
-- Legacy running rows have no provable owner, so first application fails closed.

begin;

do $$
begin
    -- A legacy worker can keep writing Kline pages after its old checkpoint is
    -- reset.  On the first application only, lock out claimers and fail closed
    -- until operators quiesce every pre-045 importer and resolve running rows.
    if not exists (
        select 1
          from pg_attribute
         where attrelid = 'scheme2_source_member_checkpoints'::regclass
           and attname = 'lease_version'
           and not attisdropped
    ) then
        lock table scheme2_source_member_checkpoints in access exclusive mode;
        if exists (
            select 1
              from scheme2_source_member_checkpoints
             where status = 'running'
        ) then
            raise exception using
                errcode = '55006',
                message = 'migration 045 requires all legacy Scheme 2 parquet workers to be stopped and running checkpoints resolved';
        end if;
    end if;
end
$$;

alter table scheme2_source_member_checkpoints
    add column if not exists content_sha256 varchar(64),
    add column if not exists worker_id varchar(160),
    add column if not exists claim_token varchar(64),
    add column if not exists lease_version bigint not null default 0,
    add column if not exists lease_until timestamptz,
    add column if not exists lease_heartbeat_at timestamptz,
    add column if not exists attempts integer not null default 0,
    add column if not exists max_attempts integer not null default 5;

do $$
begin
    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'scheme2_source_member_checkpoints'::regclass
           and conname = 'ck_scheme2_member_checkpoint_attempts'
    ) then
        alter table scheme2_source_member_checkpoints
            add constraint ck_scheme2_member_checkpoint_attempts
            check (
                lease_version >= 0
                and imported_rows >= 0
                and attempts >= 0
                and max_attempts > 0
            );
    end if;

    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'scheme2_source_member_checkpoints'::regclass
           and conname = 'ck_scheme2_member_checkpoint_content_sha256'
    ) then
        alter table scheme2_source_member_checkpoints
            add constraint ck_scheme2_member_checkpoint_content_sha256
            check (
                content_sha256 is null
                or content_sha256 ~ '^[0-9a-f]{64}$'
            );
    end if;

    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'scheme2_source_member_checkpoints'::regclass
           and conname = 'ck_scheme2_member_checkpoint_lease_state'
    ) then
        alter table scheme2_source_member_checkpoints
            add constraint ck_scheme2_member_checkpoint_lease_state
            check (
                (status = 'running'
                 and worker_id is not null
                 and claim_token is not null
                 and lease_until is not null
                 and lease_heartbeat_at is not null)
                or
                (status <> 'running'
                 and worker_id is null
                 and claim_token is null
                 and lease_until is null
                 and lease_heartbeat_at is null)
            );
    end if;
end
$$;

create unique index if not exists uq_scheme2_source_member_checkpoint_identity_v2
on scheme2_source_member_checkpoints (
    root_path,
    source_profile,
    zip_path,
    member_path,
    coalesce(member_crc32, -1),
    coalesce(member_size_bytes, -1),
    coalesce(content_sha256, '')
);

drop index if exists uq_scheme2_source_member_checkpoint_identity;

create index if not exists idx_scheme2_member_checkpoint_claimable
    on scheme2_source_member_checkpoints (
        source_profile,
        timeframe,
        status,
        updated_at,
        lease_until,
        id
    )
    where status in ('pending', 'failed', 'running');

commit;
