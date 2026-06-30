# Market Fill Worker

The market fill worker can run alongside local development to keep K-line data
fresh and precompute Chan structures for `5f`, `30f`, and `1d`.

## Safe Pilot

Run one pytdx symbol once:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Limit 300 `
  -Sleep 0.25
```

Run without writing:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -DryRun
```

## Background Loop

Start a bounded loop for the first ten provider symbols:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -SymbolLimit 10 `
  -Limit 300 `
  -Sleep 0.25 `
  -Loop `
  -LoopInterval 60
```

The default period list is `5f,15f,30f,1h,1d,1w,1m`. Chan precompute uses the
stored `5f` sequence as the only base input and recursively derives the `5f`,
`30f`, and `1d` Chan trend levels.

`-Limit` controls how many recent bars are fetched from the quote provider in
this pass. It does not control how many bars are sent to `chan.py`. After the
new bars are written, Chan precompute reloads all stored bars for the symbol and
base timeframe from PostgreSQL and calculates one continuous historical Chan
chain from the earliest stored 5-minute K-line to the latest stored 5-minute
K-line. The `30f` and `1d` Chan levels are not calculated from 30-minute or
daily period K-lines.

The worker also publishes the latest bar of every fetched timeframe to Redis
unless `-SkipPublish` is supplied. This is what drives live WebSocket updates in
Phase 3.

Use `-SymbolLimit 0` only after the pilot is stable; it asks the provider for
the full A-share universe and can take a long time on weak networks.

## Useful Modes

Refresh K-lines and push realtime events, but skip Chan recomputation:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -SymbolLimit 10 `
  -Limit 300 `
  -SkipChan
```

Refresh K-lines and Chan rows without notifying WebSocket clients:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Limit 300 `
  -SkipPublish
```

## Database Notes

Apply both schema files before writing Chan data:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\apply-db-migrations.ps1
```

`klines` is upserted. Chan detail tables are replaced per symbol/level/mode on
each run, while `chan_runs` keeps historical run records. The active detail
rows therefore represent the latest complete Chan chain over the stored K-line
history, not a sliced response window.

## API Cache Hit

`GET /api/v1/chan/overlay` prefers precomputed Chan rows when all requested
levels have a successful run that covers the requested K-line window:

- `bar_from <= first requested bar`
- `bar_until >= last requested bar`

The API then returns Chan objects whose time span intersects the requested
K-line window. A stroke or segment can start before the requested window and
still be returned if it enters the visible range. Buy/sell points are returned
when the point timestamp is inside the requested window.

When this hits, the response engine is:

```text
database:chan-precomputed
```

Probe locally:

```powershell
Invoke-WebRequest `
  -UseBasicParsing `
  'http://127.0.0.1:8001/api/v1/chan/overlay?symbol=000001.SZ&timeframe=5f&limit=300' `
  -Headers @{Authorization='Bearer dev-local-token'} |
  Select-Object -ExpandProperty Content
```

If no matching precomputed run exists, the API falls back to the configured
chan-service and returns `chan-service:chan.py`.
