# Historical Backfill Worker

This worker fills older K-lines page by page and records progress in
`historical_backfill_tasks`.

Use it for slow, resumable history expansion. Keep `market_fill` for latest-bar
refresh and realtime Redis publishing. Use the Chan recompute worker after
history batches to rebuild precomputed Chan rows from the complete stored
5-minute K-line history.

## Apply Schema

```powershell
powershell -ExecutionPolicy Bypass -File scripts\apply-db-migrations.ps1
```

This creates:

- `historical_backfill_tasks`

The task key is:

```text
symbol + timeframe + provider
```

Important task fields:

- `next_offset`: next pytdx page offset to fetch
- `status`: `pending`, `running`, `success`, or `failed`
- `pages_done`, `bars_read`, `bars_written`
- `oldest_ts`, `newest_ts`

## Concurrency Model

The worker uses task-level concurrency:

- `-TaskLimit` controls how many pending tasks are claimed in one pass.
- `-Concurrency` controls how many claimed tasks run at the same time.
- Each concurrent task creates its own provider instance, so pytdx connection
  state is not shared across tasks.
- K-line writes and task updates use async PostgreSQL pools.

Start conservatively. On a Windows desktop or NAS, use `-Concurrency 2` first,
then try `3` or `4` if the TDX server and database remain stable.

## Safe Pilot

PowerShell can parse values like `1d` as numbers if they are not quoted. Quote
timeframe lists.

Dry-run one symbol:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-history-backfill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Timeframes '5f,30f,1d' `
  -DryRun
```

Fetch one 5-minute page with a fixed TDX server:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-history-backfill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Timeframes '5f' `
  -PageSize 800 `
  -TaskLimit 1 `
  -Concurrency 1 `
  -MaxPagesPerTask 1 `
  -TdxHost 124.70.199.56 `
  -TdxRetries 1 `
  -TdxTimeout 10 `
  -ResetRunning
```

Run the same command again to continue from the stored `next_offset`.

## Background Loop

After the pilot is stable:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-history-backfill-worker.ps1 `
  -Provider pytdx `
  -SymbolLimit 10 `
  -Timeframes '5f,15f,30f,1h,1d,1w,1m' `
  -PageSize 800 `
  -TaskLimit 10 `
  -Concurrency 2 `
  -MaxPagesPerTask 2 `
  -TdxHost 124.70.199.56 `
  -TdxRetries 1 `
  -TdxTimeout 10 `
  -Loop `
  -LoopInterval 30 `
  -ResetRunning
```

Use `-SymbolLimit 0` only when the machine and network are ready for the full
A-share universe.

## Chan Recompute

Historical backfill only extends stored K-lines by default.

The preferred routine is:

1. Run historical backfill to expand older K-lines.
2. Run `start-chan-recompute-worker.ps1` for the same symbol or batch. It reads
   the full stored `5f` bars, recursively derives the `5f`, `30f`, and `1d`
   Chan trend levels, then replaces the precomputed rows used by the TradingView
   overlay API.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-recompute-worker.ps1 `
  -Symbols 000001.SZ `
  -BaseTimeframe 5f `
  -ChanLevels '5f,30f,1d' `
  -TaskLimit 3 `
  -Concurrency 1 `
  -ResetRunning
```

For a task that has reached the provider's historical end, the history worker can
also recompute Chan immediately:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-history-backfill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Timeframes '5f' `
  -RecomputeChanOnSuccess
```

That inline mode is useful for one-off pilots. For batches and full-market
maintenance, prefer the dedicated worker documented in
`docs/runbooks/chan-recompute-worker.md`.

## Inspect Progress

```powershell
docker exec tv_local_timescaledb psql `
  -U trader `
  -d tradingview_local `
  -c "select s.code||'.'||s.exchange as symbol, task.timeframe, task.status, task.next_offset, task.pages_done, task.bars_read, task.bars_written, task.oldest_ts, task.newest_ts from historical_backfill_tasks task join symbols s on s.id=task.symbol_id order by task.updated_at desc limit 20;"
```
