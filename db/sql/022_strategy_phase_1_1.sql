create table if not exists scheme2_chan_c_published_head_history (
    id bigserial primary key,
    symbol_id integer not null references symbols(id),
    chan_level integer not null,
    mode text not null,
    base_timeframe integer not null,
    old_run_id bigint,
    new_run_id bigint not null,
    old_base_to_bar_end timestamptz,
    new_base_to_bar_end timestamptz,
    snapshot_version bigint,
    observed_at timestamptz not null default now(),
    source text not null default 'strategy_observer'
);

create index if not exists idx_chan_c_head_history_symbol_level_mode
on scheme2_chan_c_published_head_history(symbol_id, chan_level, mode, observed_at desc);

create index if not exists idx_chan_c_head_history_new_run
on scheme2_chan_c_published_head_history(new_run_id);
