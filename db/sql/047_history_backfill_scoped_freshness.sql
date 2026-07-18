-- Bind scoped PyTDX tail runs to an authoritative upper freshness watermark.
-- Existing scoped runs remain durable audit evidence but cannot be claimed without it.

begin;

alter table historical_backfill_scoped_runs
    add column if not exists expected_through jsonb,
    add column if not exists freshness_contract_sha256 char(64);

alter table historical_backfill_tasks
    add column if not exists expected_through timestamptz,
    add column if not exists provider_newest_ts timestamptz;

do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conrelid = 'historical_backfill_scoped_runs'::regclass
          and conname = 'ck_historical_backfill_scoped_freshness_sha'
    ) then
        alter table historical_backfill_scoped_runs
            add constraint ck_historical_backfill_scoped_freshness_sha
            check (
                freshness_contract_sha256 is null
                or freshness_contract_sha256 ~ '^[0-9a-f]{64}$'
            );
    end if;
end
$$;

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
    if tg_op = 'INSERT' then
        if new.run_id is not null
           and expected_run_id is distinct from new.run_id::text then
            raise exception using
                errcode = '55000',
                message = 'scoped historical backfill insert requires exact session run fence';
        end if;
        if new.run_id is not null and new.expected_through is null then
            raise exception using
                errcode = '55000',
                message = 'scoped historical backfill insert requires expected-through evidence';
        end if;
        return new;
    end if;
    if tg_op = 'DELETE' then
        if old.run_id is not null
           and expected_run_id is distinct from old.run_id::text then
            raise exception using
                errcode = '55000',
                message = 'scoped historical backfill delete requires exact session run fence';
        end if;
        return old;
    end if;
    if (old.run_id is not null or new.run_id is not null)
       and expected_run_id is distinct from coalesce(old.run_id, new.run_id)::text then
        raise exception using
            errcode = '55000',
            message = 'scoped historical backfill mutation requires exact session run fence';
    end if;
    if new.run_id is not null
       and new.expected_through is null
       and old.status is distinct from new.status then
        raise exception using
            errcode = '55000',
            message = 'legacy scoped historical backfill run cannot change task status';
    end if;
    if old.run_id is distinct from new.run_id
       or (old.run_id is not null and (
           old.stop_at is distinct from new.stop_at
           or old.expected_through is distinct from new.expected_through
           or old.symbol_id is distinct from new.symbol_id
           or old.timeframe is distinct from new.timeframe
           or old.provider is distinct from new.provider
           or old.page_size is distinct from new.page_size
       )) then
        raise exception using
            errcode = '55000',
            message = 'scoped historical backfill task identity is immutable';
    end if;
    return new;
end
$$;

revoke all on function enforce_historical_backfill_scoped_session_fence() from public;

commit;
