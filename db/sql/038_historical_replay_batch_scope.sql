-- One immutable replay contract may have separate canary and full-market batches.
alter table if exists chan_c_historical_replay_batches
    drop constraint if exists chan_c_historical_replay_batches_contract_hash_source_batch_id_key;
