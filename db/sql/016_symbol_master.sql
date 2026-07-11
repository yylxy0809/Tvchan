create index if not exists idx_symbols_active_a_share
on symbols (exchange, code)
where is_active = true
  and asset_type = 'stock'
  and market = 'A_SHARE';

create index if not exists idx_scheme2_market_fetch_tasks_symbol_status
on scheme2_market_fetch_tasks (symbol_id, status, next_run_at);

create index if not exists idx_scheme2_chan_tail_tasks_symbol_status
on scheme2_chan_tail_tasks (symbol_id, status, next_run_at);

comment on index idx_symbols_active_a_share is
'Current tradable A-share symbol pool used by APIs and realtime workers.';
