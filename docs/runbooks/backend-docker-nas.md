# Backend Docker/NAS Runbook

This runbook describes the deployment-ready backend topology. The default
unattended runtime is the core stack plus the realtime pipeline fetch/Chan
workers. Legacy workers are kept only for manual maintenance windows.

## Default Topology

Always-on services:

- TimescaleDB/PostgreSQL
- Redis
- one-shot database migration container
- chan-service
- FastAPI API gateway
- web-gateway
- realtime-pipeline fetch worker
- realtime-pipeline Chan worker

Do not run `market-fill`, standalone `chan-tail-publisher`, or full
`chan-recompute` together with realtime pipeline tail publishing. They can write
overlapping K-line or Chan state and should be used only as explicit rollback or
batch maintenance tools.

## Prepare Env

Copy the template and fill secrets on the deployment host. Deployment packages
must not contain a filled `deploy/backend.env`.

```powershell
Copy-Item deploy\backend.env.template deploy\backend.env
notepad deploy\backend.env
```

Required fields:

- `API_TOKEN`, `ADMIN_API_TOKEN`, `POSTGRES_PASSWORD`
- `CORS_ORIGINS`
- `CHAN_PY_HOST_PATH`
- `COMPOSE_PROFILES=realtime-pipeline`
- optional `CLOUDFLARED_TOKEN`, Wencai, and LLM settings

On NAS, use Linux-style bind paths:

```text
CHAN_PY_HOST_PATH=/volume1/docker/tradingview/vendor/chan.py-main
TDX_CSV_HOST_ROOT=/volume1/market-data/tdx-data
```

## Start Core And Realtime Pipeline

With `COMPOSE_PROFILES=realtime-pipeline` in `deploy/backend.env`, this starts
core services plus the realtime fetch and Chan pipeline workers:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml up -d --build
```

Check containers:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml ps
```

Check API health:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/api/v1/health | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8001/api/v1/admin/ops/status -Headers @{ Authorization = "Bearer <ADMIN_API_TOKEN>" } | ConvertTo-Json -Depth 8
```

For LAN access, replace `127.0.0.1` with the backend host IP.

## Manual Maintenance Workers

Use these profiles only when realtime pipeline is stopped or when you have
explicitly accepted the overlap risk.

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile manual-market-fill up -d --build
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile batch-history up -d --build
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile batch-chan-recompute up -d --build
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile manual-chan-tail-publisher up -d --build
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml --profile batch-tdx-csv-import up -d --build
```

Rules:

- `manual-market-fill` is a rollback fetch path.
- `manual-chan-tail-publisher` is a rollback Chan tail path.
- `batch-chan-recompute` is for full recompute windows; stop realtime Chan
  workers first.
- `batch-history` and `batch-tdx-csv-import` are batch recovery/import paths.

## Progress SQL

Replace database/user values if they differ from `deploy/backend.env`.

```powershell
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select count(*) filter (where is_active) active_symbols from symbols where market='A_SHARE' and asset_type='stock';"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select timeframe, count(*) tasks, count(*) filter (where status='running') running, count(*) filter (where status='failed') failed, min(last_bar_end), max(last_bar_end) from scheme2_market_fetch_tasks group by timeframe order by timeframe;"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select chan_level, mode, count(*) tasks, count(*) filter (where status='running') running, count(*) filter (where status='failed') failed, min(last_success_bar_end), max(last_success_bar_end) from scheme2_chan_tail_tasks group by chan_level, mode order by chan_level, mode;"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select chan_level, mode, count(*) heads, min(base_to_bar_end), max(base_to_bar_end) from scheme2_chan_published_heads where status='published' group by chan_level, mode order by chan_level, mode;"
```

## Logs

```powershell
docker logs --tail 100 tv_backend_api
docker logs --tail 100 tv_backend_chan_service
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml logs --tail 100 realtime-pipeline-fetch-worker
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml logs --tail 100 realtime-pipeline-chan-worker
```

Manual worker logs:

```powershell
docker logs --tail 100 tv_backend_market_fill_worker
docker logs --tail 100 tv_backend_history_backfill_worker
docker logs --tail 100 tv_backend_chan_recompute_worker
docker logs --tail 100 tv_backend_chan_tail_publisher_worker
docker logs --tail 100 tv_backend_tdx_csv_import_worker
```

## Stop

Stop services without deleting data:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml down
```

Delete database and Redis volumes only for an intentional reset:

```powershell
docker compose --env-file deploy\backend.env -f deploy\docker-compose.backend.yml down -v
```

## Readiness Notes

- `5f` stored bars are the base input for `5f`, `30f`, and `1d` Chan levels.
- Redis is required for user-visible realtime push; if Redis is down, data can
  still land in PostgreSQL but frontend updates will fall back to polling.
- Keep source credentials and API keys only in deployment-host env files or the
  admin runtime config page.
