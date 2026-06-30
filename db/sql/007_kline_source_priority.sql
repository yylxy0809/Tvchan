create index if not exists idx_klines_symbol_timeframe_source_ts
on klines (symbol_id, timeframe, source, ts desc);
