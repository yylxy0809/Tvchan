# Head History Observer

## Purpose

`run_head_history_observer.py` is a forward-only observer for `scheme2_chan_c_published_heads`.

It is intended to accumulate future `first_seen_time` evidence after deployment. It is not a mechanism to reconstruct exact historical `first_seen_time` for old strategy backtests.

## Table

The observer writes to `scheme2_chan_c_published_head_history`.

Each inserted row records:

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `old_run_id`
- `new_run_id`
- `old_base_to_bar_end`
- `new_base_to_bar_end`
- `snapshot_version`
- `source`
- `observed_at`

The row represents a detected transition from the last observed published head to the current published head.

## CLI

Run once:

```powershell
python -m app.cli.run_head_history_observer --source strategy_observer --output outputs/head-history-observer-summary.json
```

Arguments:

- `--source`: free-form source tag stored with each observation
- `--output`: JSON summary path

## Idempotency

The observer is effectively idempotent across repeated runs against unchanged published heads.

It inserts only when one of these changes relative to the latest observed row for the same scope:

- `new_run_id`
- `new_base_to_bar_end`

If no published head changed, the run writes `inserted = 0`.

## Deployment Guidance

Recommended deployment pattern:

1. Start the observer as a periodic task after module C publish is stable.
2. Run it on a short cadence, for example every 1-5 minutes during trading hours.
3. Keep it separate from historical replay and offline backtest jobs.

This task is lightweight and only compares the current published head view against the latest observed history row.

## Relationship To first_seen_time

Current Phase 1.2 event replay uses an approximate historical source:

- `chan_c_runs_event_replay_approx`

The observer prepares a future path toward more realistic first-seen timestamps:

1. module C publishes a new head
2. observer records the change time in `scheme2_chan_c_published_head_history`
3. later strategy engines can read these observations as a forward-built `first_seen_time` source

## Important Limitation

This observer does not backfill exact historical first-seen times for past heads.

Any historical replay performed before a long-running observer history exists remains an approximation and should be described as such in reports.
