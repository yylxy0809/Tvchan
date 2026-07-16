-- One immutable replay contract may have separate canary and full-market batches.
do $$
declare
    constraint_name text;
begin
    for constraint_name in
        select conname
          from pg_constraint
         where conrelid = 'chan_c_historical_replay_batches'::regclass
           and contype = 'u'
           and pg_get_constraintdef(oid) like '%contract_hash, source_batch_id%'
    loop
        execute format(
            'alter table chan_c_historical_replay_batches drop constraint %I',
            constraint_name
        );
    end loop;
end
$$;
