# Phase 2 pytdx Real Backfill

Date: 2026-06-15

## Verified Quote Site

The local Tongdaxin client reported:

- `124.70.199.56:7709`

Verification results on this machine:

- `Test-NetConnection 124.70.199.56 -Port 7709` succeeds.
- pytdx works with `--tdx-timeout 10`.
- The previous 3-second timeout was too short for this site.

## Backfill Command

```powershell
cd services/collector
$env:PYTHONPATH="<repo>\libs\protocol\python;<repo>\services\api"
$env:DATABASE_URL="postgresql://trader:trader@127.0.0.1:5432/tradingview_local"

python -m collector.backfill `
  --provider pytdx `
  --tdx-host 124.70.199.56 `
  --tdx-port 7709 `
  --tdx-timeout 10 `
  --tdx-retries 3 `
  --symbols "000001.SZ,601318.SH" `
  --timeframes "5f,15f,30f,1h,1d,1w,1m" `
  --limit 300 `
  --write-db `
  --replace-db
```

Verified output:

```json
{"provider":"pytdx","symbols":2,"deleted_bars":2596,"bars":3900,"timeframes":["5f","15f","30f","1h","1d","1w","1m"],"database":"written"}
```

## API Check

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8001/api/v1/bars?symbol=000001.SZ&timeframe=5f&limit=3" `
  -Headers @{ Authorization = "Bearer dev-local-token" } |
  ConvertTo-Json -Depth 5
```

Expected behavior:

- `000001.SZ` latest 5-minute bars should be around the real pytdx price level.
- `GET /api/v1/health` should report `database:seed,pytdx` while seed rows still exist for other symbols.
