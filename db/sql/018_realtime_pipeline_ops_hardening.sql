alter table if exists scheme2_market_fetch_attempts
    add column if not exists source varchar(32);

create index if not exists idx_scheme2_market_fetch_attempts_source_time
on scheme2_market_fetch_attempts (source, observed_at desc);
