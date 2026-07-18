-- Durable ownership and retry fencing for historical K-line backfill tasks.
-- Legacy running rows have no provable owner, so make them safely reclaimable.

begin;

alter table historical_backfill_tasks
    add column if not exists worker_id varchar(160),
    add column if not exists claim_token varchar(64),
    add column if not exists lease_version bigint not null default 0,
    add column if not exists lease_until timestamptz,
    add column if not exists lease_heartbeat_at timestamptz,
    add column if not exists attempts integer not null default 0,
    add column if not exists max_attempts integer not null default 5;

update historical_backfill_tasks
   set status = 'pending',
       worker_id = null,
       claim_token = null,
       lease_until = null,
       lease_heartbeat_at = null,
       last_error = coalesce(last_error, 'migration 044 recovered legacy running task'),
       updated_at = clock_timestamp()
 where status = 'running'
   and (worker_id is null or claim_token is null or lease_until is null);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'historical_backfill_tasks'::regclass
           and conname = 'ck_historical_backfill_attempts'
    ) then
        alter table historical_backfill_tasks
            add constraint ck_historical_backfill_attempts
            check (attempts >= 0 and max_attempts > 0);
    end if;
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'historical_backfill_tasks'::regclass
           and conname = 'ck_historical_backfill_lease_state'
    ) then
        alter table historical_backfill_tasks
            add constraint ck_historical_backfill_lease_state
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

create index if not exists idx_historical_backfill_claimable
    on historical_backfill_tasks (provider, status, priority, updated_at, lease_until, id)
    where status in ('pending', 'failed', 'running');

commit;
