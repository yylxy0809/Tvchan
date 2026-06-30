# Scheme 2 NAS Preflight

Use this checklist immediately before moving Scheme 2 to NAS. It keeps the
verification helper non-destructive and lists the real import commands
separately.

## Common Variables

Set these once per shell:

```powershell
$DataRoot = "D:\5f$([char]0x6570)$([char]0x636e)\5m_price"
$env:DATABASE_URL = 'postgresql://trader:password@127.0.0.1:5432/tradingview_local'
$ApiBaseUrl = 'http://127.0.0.1:8001'
$ApiToken = 'dev-local-token'
```

On NAS, change only `$DataRoot`, `$env:DATABASE_URL`, `$ApiBaseUrl`, and
`$ApiToken` for the target host.

## 1. Before Import

Goal: prove the source and runtime metadata are ready before writing data.

Run the read-only helper:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot `
  -DatabaseUrl $env:DATABASE_URL
```

Run the source audit:

```powershell
Push-Location services\collector
python -m collector.parquet_bootstrap_audit --root $DataRoot --sample-size 2
Pop-Location
```

Apply the runtime migration if it has not already been applied:

```powershell
psql $env:DATABASE_URL -X -v ON_ERROR_STOP=1 -f db\sql\010_scheme2_runtime.sql
```

Run the non-destructive parquet import dry-run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 `
  -Root $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -TaskLimit 5 `
  -Concurrency 1 `
  -BatchSize 50000 `
  -DryRun
```

Do not start the real import until source readability, year coverage, field
mapping, `bar_end` semantics, and runtime migration checks are either `PASS` or
explicitly waived in the evidence folder.

## 2. During Import

This is the real historical import command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 `
  -Root $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -TaskLimit 200 `
  -Concurrency 2 `
  -BatchSize 50000 `
  -Loop
```

Monitor with read-only checks:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DatabaseUrl $env:DATABASE_URL
```

If the worker stops unexpectedly, confirm no importer process is active before
using `--reset-running`. Never reset running checkpoints while a real worker is
still active.

## 3. After Import

Run the full read-only acceptance check:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot `
  -DatabaseUrl $env:DATABASE_URL
```

The evidence must include:

- `source = 4` `5f` row count and symbol coverage
- `scheme2_source_member_checkpoints` success, failed, running, and pending
  counts
- `scheme2_ingest_watermarks.last_bar_end` alignment with `max(klines.ts)`
- duplicate `(symbol_id, timeframe, ts)` query output
- symbol-day `5f` anomaly summary and exception list
- landed `bar_end` grid and session-label samples

Do not hand off to Chan bootstrap while any public-universe import gap,
watermark drift, duplicate bar, or unexplained timestamp issue remains.

## 4. Before Deployment

Confirm full Chan precompute and bundle readiness:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -ApiBaseUrl $ApiBaseUrl `
  -ApiToken $ApiToken
```

Deployment is blocked unless:

- runtime migration tables and columns exist
- `source = 4` canonical `5f` history is complete for the public universe
- `scheme2_chan_published_heads` covers `5f`, `30f`, and `1d`
- `scheme2_chan_recompute_watermarks` has no uncontrolled dirty ranges
- `/api/v3/chart/bundle` returns non-empty bars and three-level Chan data
- frontend source uses the canonical bundle path
- NAS pressure-test evidence meets the agreed latency and error thresholds
- rollback steps from `docs/runbooks/nas-safe-upgrade-with-existing-tdx-data.md`
  are confirmed

## 5. After Deployment

Goal: prove automatic resume and recompute continue from canonical `bar_end`
watermarks after NAS cutover.

Run the helper against the NAS API and database:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DatabaseUrl $env:DATABASE_URL `
  -ApiBaseUrl $ApiBaseUrl `
  -ApiToken $ApiToken
```

Check ingest resume state:

```powershell
psql $env:DATABASE_URL -X -P pager=off -c "
select
  source,
  timeframe,
  count(*) as symbols,
  min(last_bar_end) as min_last_bar_end,
  max(last_bar_end) as max_last_bar_end
from scheme2_ingest_watermarks
where timeframe = 5
group by source, timeframe
order by source, timeframe;"
```

Check recompute resume state:

```powershell
psql $env:DATABASE_URL -X -P pager=off -c "
select
  chan_level,
  mode,
  count(*) as rows,
  count(*) filter (where dirty_from_bar_end is not null) as dirty_rows,
  min(dirty_from_bar_end) as earliest_dirty_bar_end,
  max(last_computed_bar_end) as max_last_computed_bar_end
from scheme2_chan_recompute_watermarks
where base_timeframe = 5
group by chan_level, mode
order by chan_level, mode;"
```

For the first post-deploy trading window, record two observations at least
five minutes apart:

- `scheme2_ingest_watermarks.last_bar_end` advances only after landed `5f`
  bars are committed
- dirty ranges are created from the earliest impacted canonical `bar_end`
- dirty ranges clear after recompute catches up
- bundle `snapshot_version` and `snapshot_id` stay stable across identical
  quiet-window reads
- no API, worker, chan-service, database, or Redis restart loop appears on NAS

If automatic continuation stalls, preserve logs and watermarks before restarting
workers so the resume boundary can be audited.
