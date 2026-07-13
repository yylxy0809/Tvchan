-- Retry and dead-letter metadata for the durable Module C lifecycle outbox.
-- This migration is additive and deliberately leaves migration 035 untouched.

alter table if exists chan_c_head_outbox
    add column if not exists next_attempt_at timestamptz,
    add column if not exists last_error text,
    add column if not exists failed_at timestamptz,
    add column if not exists dead_lettered_at timestamptz;

do $$
declare
    constraint_name text;
begin
    for constraint_name in
        select con.conname
          from pg_constraint con
          join pg_class rel on rel.oid = con.conrelid
          join pg_attribute attr
            on attr.attrelid = rel.oid
           and attr.attname = 'status'
         where rel.relname = 'chan_c_head_outbox'
           and con.contype = 'c'
           and attr.attnum = any(con.conkey)
    loop
        execute format(
            'alter table chan_c_head_outbox drop constraint %I',
            constraint_name
        );
    end loop;
end
$$;

alter table if exists chan_c_head_outbox
    add constraint ck_chan_c_head_outbox_status
    check (status in ('pending', 'processing', 'failed', 'completed', 'dead_letter'));

create index if not exists idx_chan_c_head_outbox_retry_due
    on chan_c_head_outbox (status, next_attempt_at, lease_until, id)
    where status in ('pending', 'processing', 'failed');
