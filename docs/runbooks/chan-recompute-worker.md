# Chan Recompute Worker

This worker recomputes Chan analysis from the stored 5-minute K-line history,
then recursively derives the `5f`, `30f`, and `1d` Chan trend levels and replaces
the precomputed Chan rows in PostgreSQL.

Use it after historical backfill expands older 5-minute K-lines. This keeps
TradingView loading K-lines and Chan drawings from the same historical source:
the frontend may request a limited visible range, but the stored Chan result is
calculated from the full local 5-minute history and then filtered to the
requested window.

## Apply Schema

```powershell
powershell -ExecutionPolicy Bypass -File scripts\apply-db-migrations.ps1
```

This creates:

- `chan_recompute_tasks`

The task key is:

```text
symbol + base_timeframe + modes + config_hash
```

Important task fields:

- `status`: `pending`, `running`, `success`, or `failed`
- `last_bar_from`, `last_bar_until`, `last_bar_count`
- `strokes_count`, `segments_count`, `centers_count`, `signals_count`
- `attempts`, `last_error`

Existing successful tasks are only re-queued when the stored K-line count or
time window changes. Use `-Reset` when you intentionally want to force a full
recompute even if the local K-line history is unchanged.

`chan_level` in the task table is the base timeframe for recomputation. It
should normally be `5f`. The `-ChanLevels` option controls which recursive Chan
trend levels are written from that 5-minute base sequence.

## Safe Pilot

Start chan-service first:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-service.ps1
```

Dry-run one symbol:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-recompute-worker.ps1 `
  -Symbols 000001.SZ `
  -BaseTimeframe 5f `
  -ChanLevels '5f,30f,1d' `
  -DryRun
```

Run one 5-minute recompute:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-recompute-worker.ps1 `
  -Symbols 000001.SZ `
  -BaseTimeframe 5f `
  -ChanLevels '5f' `
  -TaskLimit 1 `
  -Concurrency 1 `
  -ChanTimeout 120 `
  -ResetRunning
```

## Background Loop

After the pilot is stable:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-recompute-worker.ps1 `
  -SymbolLimit 10 `
  -BaseTimeframe 5f `
  -ChanLevels '5f,30f,1d' `
  -TaskLimit 10 `
  -Concurrency 2 `
  -ChanTimeout 180 `
  -Loop `
  -LoopInterval 60 `
  -ResetRunning
```

Use `-SymbolLimit 0` only after storage, CPU, and chan-service latency are
stable. Full-market recompute should normally run after a historical backfill
batch, not on every realtime tick.

## Inspect Progress

```powershell
docker exec tv_local_timescaledb psql `
  -U trader `
  -d tradingview_local `
  -c "select s.code||'.'||s.exchange as symbol, task.chan_level, task.status, task.last_bar_count, task.strokes_count, task.segments_count, task.centers_count, task.signals_count, task.last_error from chan_recompute_tasks task join symbols s on s.id=task.symbol_id order by task.updated_at desc limit 20;"
```

## API Expectation

After a task succeeds, this API should read precomputed Chan rows:

```powershell
Invoke-RestMethod `
  -Uri 'http://127.0.0.1:8001/api/v1/chan/overlay?symbol=000001.SZ&timeframe=5f&limit=300' `
  -Headers @{Authorization='Bearer dev-local-token'} |
  ConvertTo-Json -Depth 8
```

Expected engine:

```text
database:chan-precomputed
```
