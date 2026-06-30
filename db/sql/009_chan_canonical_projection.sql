alter table chan_runs
add column if not exists snapshot_version varchar(255);

alter table chan_runs
add column if not exists computed_at timestamptz;

update chan_runs
set computed_at = coalesce(computed_at, finished_at, started_at)
where computed_at is null;

create index if not exists idx_chan_runs_snapshot_version
on chan_runs (symbol_id, chan_level, status, snapshot_version);

alter table chan_strokes
add column if not exists begin_base_ts timestamptz;

alter table chan_strokes
add column if not exists end_base_ts timestamptz;

alter table chan_strokes
add column if not exists begin_base_seq integer;

alter table chan_strokes
add column if not exists end_base_seq integer;

update chan_strokes
set begin_base_ts = coalesce(begin_base_ts, start_ts),
    end_base_ts = coalesce(end_base_ts, end_ts)
where begin_base_ts is null
   or end_base_ts is null;

create index if not exists idx_chan_strokes_run_base_ts
on chan_strokes (run_id, begin_base_ts, end_base_ts);

alter table chan_segments
add column if not exists begin_base_ts timestamptz;

alter table chan_segments
add column if not exists end_base_ts timestamptz;

alter table chan_segments
add column if not exists begin_base_seq integer;

alter table chan_segments
add column if not exists end_base_seq integer;

update chan_segments
set begin_base_ts = coalesce(begin_base_ts, start_ts),
    end_base_ts = coalesce(end_base_ts, end_ts)
where begin_base_ts is null
   or end_base_ts is null;

create index if not exists idx_chan_segments_run_base_ts
on chan_segments (run_id, begin_base_ts, end_base_ts);

alter table chan_centers
add column if not exists begin_base_ts timestamptz;

alter table chan_centers
add column if not exists end_base_ts timestamptz;

alter table chan_centers
add column if not exists begin_base_seq integer;

alter table chan_centers
add column if not exists end_base_seq integer;

update chan_centers
set begin_base_ts = coalesce(begin_base_ts, start_ts),
    end_base_ts = coalesce(end_base_ts, end_ts)
where begin_base_ts is null
   or end_base_ts is null;

create index if not exists idx_chan_centers_run_base_ts
on chan_centers (run_id, begin_base_ts, end_base_ts);

alter table chan_signals
add column if not exists base_ts timestamptz;

alter table chan_signals
add column if not exists base_seq integer;

update chan_signals
set base_ts = coalesce(base_ts, ts)
where base_ts is null;

create index if not exists idx_chan_signals_run_base_ts
on chan_signals (run_id, base_ts);
