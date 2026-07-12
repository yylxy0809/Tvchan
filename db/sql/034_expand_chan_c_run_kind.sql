-- Extend the legacy Module C run-kind contract for the lifecycle workers.
-- This is a pure superset of values already accepted by production.

do $$
declare
    definition text;
begin
    select pg_get_constraintdef(oid)
      into definition
      from pg_constraint
     where conrelid = 'chan_c_runs'::regclass
       and conname = 'chk_chan_c_runs_run_kind';

    if definition is null
       or definition not like '%full_recompute%'
       or definition not like '%online%' then
        alter table chan_c_runs
            drop constraint if exists chk_chan_c_runs_run_kind;
        alter table chan_c_runs
            add constraint chk_chan_c_runs_run_kind
            check (run_kind in (
                'full', 'tail', 'baseline', 'canary', 'historical_replay',
                'diagnostic', 'full_recompute', 'online'
            )) not valid;
    end if;
end
$$;

alter table chan_c_runs
    validate constraint chk_chan_c_runs_run_kind;
