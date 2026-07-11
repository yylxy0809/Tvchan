create index concurrently if not exists idx_chan_strokes_scope_delete
on chan_strokes (symbol_id, chan_level, mode);

create index concurrently if not exists idx_chan_segments_scope_delete
on chan_segments (symbol_id, chan_level, mode);

create index concurrently if not exists idx_chan_centers_scope_delete
on chan_centers (symbol_id, chan_level, mode);

create index concurrently if not exists idx_chan_signals_scope_delete
on chan_signals (symbol_id, chan_level, mode);

create index concurrently if not exists idx_chan_runs_scope_mode
on chan_runs (symbol_id, chan_level, mode, status, bar_until desc);

comment on index idx_chan_strokes_scope_delete is
'Supports replace/delete by symbol, Chan level, and mode during full recompute.';
