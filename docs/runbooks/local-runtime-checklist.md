# Local Runtime Checklist

## Start infrastructure

```powershell
docker compose -f deploy/docker-compose.dev.yml up -d
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected containers:

- `tv_local_timescaledb`
- `tv_local_redis`

For a Docker/NAS-style backend deployment that also containers API,
chan-service, and collectors, see `docs/runbooks/backend-docker-nas.md`.

## Start API in database mode

```powershell
.\scripts\start-api-db.ps1
```

The script sets:

- `USE_SEED_DATA=false`
- `DATABASE_URL=postgresql://trader:trader@127.0.0.1:5432/tradingview_local`
- `REDIS_URL=redis://127.0.0.1:6379/0`
- `CHAN_SERVICE_URL=http://127.0.0.1:8002`
- `PYTHONPATH=<repo>\libs\protocol\python`

Check the running API:

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8001/api/v1/health | ConvertTo-Json -Depth 5
```

Important source labels:

- `data_source=seed`: API is using in-memory deterministic sample bars.
- `data_source=database:seed`: API is reading from PostgreSQL, but the rows were inserted by the seed provider.
- `data_source=database:pytdx`: API is reading real pytdx-origin rows from PostgreSQL.
- `data_source=database:empty`: API is connected to PostgreSQL, but no K-line rows exist yet.

## Why the chart may not show real market data

TradingView renders whatever bars the API returns. If the health endpoint shows
`database:seed`, the chart is correctly rendering database data, but those rows
are still sample seed bars. Real market data appears only after pytdx can reach a
quote server and the collector writes rows with source `pytdx`.

Probe pytdx quote-server connectivity:

```powershell
cd services/collector
$env:PYTHONPATH="<repo>\libs\protocol\python;<repo>\services\api"
python -m collector.backfill `
  --provider pytdx `
  --tdx-host 124.70.199.56 `
  --tdx-port 7709 `
  --tdx-timeout 10 `
  --tdx-retries 3 `
  --probe-servers
```

If every server reports `"ok": false`, the current machine or network cannot
reach the TDX quote-server port. In that state, pytdx backfill cannot produce
real market bars. Try another network, allow outbound TCP `7709`, or pass a
known-good server with `--tdx-host <host> --tdx-port 7709`.

The local Tongdaxin client reported a usable quote site on 2026-06-15:

- `124.70.199.56:7709`

On this machine the TCP port is reachable, but pytdx needs a longer handshake
timeout than the old 3-second default. Use `--tdx-timeout 10`.

## Start frontend against port 8001

```powershell
cd apps/web
$env:VITE_API_BASE_URL="http://127.0.0.1:8001"
$env:VITE_API_TOKEN="dev-local-token"
npm run dev
```

## Refresh market data and Chan cache

After PostgreSQL, Redis, API, and chan-service are running, fill one real symbol:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\apply-db-migrations.ps1
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Limit 300 `
  -Sleep 0.25
```

Expected API overlay engine after this pass:

```text
database:chan-precomputed
```

## Expand historical K-lines

Use the historical worker for resumable older K-line backfill:

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
  -ResetRunning
```

See `docs/runbooks/historical-backfill-worker.md` for full-market loop settings.

## Recompute Chan from stored history

After historical K-lines expand, rebuild the precomputed Chan rows from the full
stored 5-minute history. The `30f` and `1d` Chan levels are recursively derived
from that 5-minute base sequence; they are not calculated from 30-minute or
daily period K-lines.

If local zipped TDX CSV history is available, import it before recomputing Chan:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-tdx-csv-import-worker.ps1 `
  -Root 'D:\BaiduNetdiskDownload\tdx数据' `
  -Timeframes '5f' `
  -TaskLimit 1 `
  -MaxEntriesPerTask 50 `
  -ResetRunning
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-recompute-worker.ps1 `
  -Symbols 000001.SZ `
  -BaseTimeframe 5f `
  -ChanLevels '5f,30f,1d' `
  -TaskLimit 3 `
  -Concurrency 1 `
  -ChanTimeout 120 `
  -ResetRunning
```

The API should then return:

```text
database:chan-precomputed
```

See `docs/runbooks/chan-recompute-worker.md` for queued batch settings.
