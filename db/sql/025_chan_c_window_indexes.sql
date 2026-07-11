-- This migration must run outside a transaction: CREATE INDEX CONCURRENTLY is
-- required for the potentially large Module C signal table.
--
-- Existing line/center indexes use raw timestamps. The overlay overlap query
-- uses tstzrange(...) && tstzrange(...), so these composite GiST indexes match
-- both the equality scope and the range predicate. btree_gist supplies GiST
-- operator classes for run_id and mode.
-- No DROP statements are included; each name is idempotent and safe to retain.
create extension if not exists btree_gist;
create index concurrently if not exists idx_chan_c_strokes_window_range_gist
on chan_c_strokes using gist (run_id, mode, (tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')));
create index concurrently if not exists idx_chan_c_strokes_window_predecessor
on chan_c_strokes (run_id, mode, (coalesce(end_base_ts, end_ts)) desc, seq desc, id desc);
create index concurrently if not exists idx_chan_c_strokes_window_successor
on chan_c_strokes (run_id, mode, (coalesce(begin_base_ts, start_ts)), seq, id);

create index concurrently if not exists idx_chan_c_segments_window_range_gist
on chan_c_segments using gist (run_id, mode, (tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')));
create index concurrently if not exists idx_chan_c_segments_window_predecessor
on chan_c_segments (run_id, mode, (coalesce(end_base_ts, end_ts)) desc, seq desc, id desc);
create index concurrently if not exists idx_chan_c_segments_window_successor
on chan_c_segments (run_id, mode, (coalesce(begin_base_ts, start_ts)), seq, id);

create index concurrently if not exists idx_chan_c_centers_window_range_gist
on chan_c_centers using gist (run_id, mode, (tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')));

-- Signals had no equivalent index, while the authoritative window read uses
-- run_id + mode + coalesce(base_ts, ts) point containment.
create index concurrently if not exists idx_chan_c_signals_window_lookup
on chan_c_signals (run_id, mode, (coalesce(base_ts, ts)));
--
-- Run this read-only check against a staging copy before introducing an index:
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT id FROM chan_c_signals
-- WHERE run_id = :run_id AND mode = :mode
--   AND coalesce(base_ts, ts) between :window_start and :window_end;
--
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT id FROM chan_c_strokes
-- WHERE run_id = :run_id AND mode = :mode
--   AND tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')
--       && tstzrange(:window_start, :window_end, '[]')
-- ORDER BY coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), seq, id
-- LIMIT :cap;
--
-- Verify the existing line-table indexes separately before adding any more
-- indexes; do not introduce speculative replacements.
