# Backend Docker/NAS Runbook

This runbook is for deploying the backend side on a fixed Windows host or NAS
with Docker. It does not serve the frontend. Frontend devices only need network
access to the API port.

## Scope

The backend stack contains:

- TimescaleDB/PostgreSQL
- Redis
- one-shot database migration container
- FastAPI API gateway
- chan-service with mounted `chan.py`
- optional collector workers for realtime fill, historical backfill, Chan
  recompute, and local TDX CSV import

## Prepare Files

Copy the env template and edit secrets and host paths:

```powershell
Copy-Item deploy\backend.env.example deploy\backend.env
notepad deploy\backend.env
```

Required edits:

- `API_TOKEN`: use a long random value.
- `POSTGRES_PASSWORD`: use a real password.
- `API_BIND`: defaults to `0.0.0.0`, so frontend devices on the LAN can reach
  the API.
- `POSTGRES_BIND`, `REDIS_BIND`, and `CHAN_SERVICE_BIND`: default to
  `127.0.0.1` so database, Redis, and chan-service are not exposed to the LAN.
- `CORS_ORIGINS`: include the frontend device origins, for example
  `http://192.168.1.20:5173`.
- `CHAN_PY_HOST_PATH`: path to the extracted `chan.py-main` directory on the
  Docker host.
- `TDX_CSV_HOST_ROOT`: optional path to local zipped TDX CSV history.

On NAS, use Linux-style bind paths such as:

```text
CHAN_PY_HOST_PATH=/volume1/docker/tradingview/vendor/chan.py-main
TDX_CSV_HOST_ROOT=/volume1/market-data/tdx-data
```

## Start Core Backend

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml up -d --build
```

Expected always-on containers:

- `tv_backend_timescaledb`
- `tv_backend_redis`
- `tv_backend_db_migrate` exits with code 0
- `tv_backend_chan_service`
- `tv_backend_api`

Check health:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml ps
Invoke-RestMethod http://127.0.0.1:8001/api/v1/health | ConvertTo-Json -Depth 5
Invoke-RestMethod http://127.0.0.1:8002/health | ConvertTo-Json -Depth 5
```

For another device on the LAN, replace `127.0.0.1` with the backend host IP.

## Run Optional Workers

Realtime latest-bar fill:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile market-fill up -d --build
```

Historical pytdx backfill:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile history up -d --build
```

Full-history Chan recompute from stored 5-minute bars:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile chan-recompute up -d --build
```

Local zipped TDX CSV import:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile tdx-csv-import up -d --build
```

The market fill, history backfill, Chan recompute, and TDX CSV import workers
are long-lived loops when their `*_LOOP=1` env values are enabled. The CSV
worker defaults to `TDX_CSV_LOOP_INTERVAL=300`, so it sleeps between passes
instead of relying on Docker restart loops.

The `30f` and `1d` Chan levels are recursively derived from stored `5f` bars;
they are not calculated from 30-minute or daily period K-lines.

## Inspect Data

```powershell
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select count(*) from symbols;"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select timeframe, count(*) from klines group by timeframe order by timeframe;"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select chan_level, count(*) from chan_strokes group by chan_level order by chan_level;"
```

If the database user or database name changed, replace `trader` and
`tradingview_local` with the values in `deploy/backend.env`.

## Logs

```powershell
docker logs --tail 100 tv_backend_api
docker logs --tail 100 tv_backend_chan_service
docker logs --tail 100 tv_backend_market_fill_worker
docker logs --tail 100 tv_backend_history_backfill_worker
docker logs --tail 100 tv_backend_chan_recompute_worker
docker logs --tail 100 tv_backend_tdx_csv_import_worker
```

## Stop

Stop services without deleting data:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml down
```

Delete database and Redis volumes only when intentionally resetting the backend:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml down -v
```

## Current Readiness Notes

- Core Docker/NAS packaging is ready for a pilot deployment.
- The Advanced Charts frontend remains separate and should point
  `VITE_API_BASE_URL` at the backend host.
- `chan.py` is mounted from the host so the licensed/project source stays out of
  the image.
- The TDX CSV importer uses zip-level resume state in PostgreSQL. Keep
  `TDX_CSV_CONCURRENCY=1` for the first NAS import batch, then raise it only
  after disk and database write latency are stable.
- `TDX_CSV_HOST_ROOT` must point to a NAS path that contains the downloaded TDX
  zip folders, for example a shared folder copied from
  `D:\BaiduNetdiskDownload\tdx数据`.
- If a future `chan.py` configuration imports optional plotting or external data
  modules, add those Python packages to the chan-service image.
