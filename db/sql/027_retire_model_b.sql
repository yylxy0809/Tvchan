-- Model B retirement. Module C tables and the vendored chan.py core remain.
-- These relations contain only the retired Module B runtime state.

drop table if exists scheme2_chan_tail_tasks;
drop table if exists scheme2_chan_recompute_watermarks;
drop table if exists scheme2_chan_published_heads;
drop table if exists chan_recompute_tasks;
drop table if exists chan_cross_level_states;
drop table if exists chan_level_state_snapshots;
drop table if exists chan_signals;
drop table if exists chan_centers;
drop table if exists chan_segments;
drop table if exists chan_strokes;
drop table if exists chan_runs;
