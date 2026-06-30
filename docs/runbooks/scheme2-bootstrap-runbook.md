# Scheme 2 Bootstrap Runbook

This runbook is for quickly validating the Scheme 2 historical import chain
around the parquet `5f` source. It does not change collector behavior.

## Scope

Use this runbook when validating:

- source inventory before import
- checkpoint progress during import
- landed `source = 4` `5f` bars after import
- ingest watermarks and `bar_end` semantics before Chan handoff

Do not use this runbook to change API, frontend, deployment, or collector code.

## Assumptions

- Source root: set `$DataRoot` to the local or NAS-mounted source path
- Source profile: `parquet_5f`
- Landed kline source code: `4`
- Canonical timeframe code: `5`
- Timestamp semantic: `bar_end`
- Normal full source day: `48` or `49` bars, depending on the source session
  template

When copying commands from a shell that cannot display Chinese paths reliably,
use this PowerShell variable and pass `$DataRoot` instead of typing the path:

```powershell
$DataRoot = "D:\5f$([char]0x6570)$([char]0x636e)\5m_price"
```

All PowerShell examples below use `$DataRoot`. If the source path differs on
NAS, change only this variable.

## Import-before Commands

Run these from the repository root unless noted.

```powershell
$env:DATABASE_URL = 'postgresql://trader:password@127.0.0.1:5432/tradingview_local'
```

```powershell
psql $env:DATABASE_URL -X -v ON_ERROR_STOP=1 -f db\sql\010_scheme2_runtime.sql
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot
```

```powershell
Push-Location services\collector
python -m collector.parquet_bootstrap_audit --root $DataRoot --sample-size 2
Pop-Location
```

Optional pilot:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 `
  -Root $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -TaskLimit 5 `
  -Concurrency 1 `
  -BatchSize 50000 `
  -DryRun
```

Before starting the real import, capture:

- zip/member inventory
- sampled required columns
- current checkpoint status counts
- current `source = 4` row count
- written note that `trade_time` is imported as `bar_end` without `+5m`

## Import-during Commands

Start the importer from the repository root. Use the wrapper script so
`PYTHONPATH` is set consistently for the collector, API models, and protocol
package:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 `
  -Root $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -TaskLimit 200 `
  -Concurrency 2 `
  -BatchSize 50000 `
  -Loop
```

Monitor progress from the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DatabaseUrl $env:DATABASE_URL
```

Focused checkpoint query:

```powershell
psql $env:DATABASE_URL -X -P pager=off -c "
select
  status,
  count(*) as members,
  coalesce(sum(imported_rows), 0) as imported_rows,
  min(updated_at) as oldest_update,
  max(updated_at) as newest_update
from scheme2_source_member_checkpoints
where source_profile = 'parquet_5f'
  and timeframe = 5
group by status
order by status;"
```

If a worker exits unexpectedly and no importer is active, clear stale running
members before resuming:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 `
  -Root $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -ResetRunning `
  -TaskLimit 200 `
  -Concurrency 2 `
  -BatchSize 50000 `
  -Loop
```

Do not reset running members while a real importer process is still active.

## Import-after Commands

Run the full local verification helper:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot `
  -DatabaseUrl $env:DATABASE_URL
```

If the API is available, add bundle checks:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  -ApiBaseUrl 'http://127.0.0.1:8001' `
  -ApiToken 'dev-local-token'
```

Save the output into an evidence folder:

```powershell
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
New-Item -ItemType Directory -Force -Path "work\verify\scheme2\$stamp" | Out-Null
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot $DataRoot `
  -DatabaseUrl $env:DATABASE_URL `
  *> "work\verify\scheme2\$stamp\verify-scheme2-local.txt"
```

## Acceptance Thresholds

| Check | PASS threshold |
| --- | --- |
| `source = 4` row count | greater than `0` and covers the agreed target universe |
| imported symbol coverage | `100%` of the public target universe |
| symbol watermarks | one `timeframe = 5` row per imported symbol |
| watermark alignment | `last_bar_end = max(klines.ts)` for every imported symbol |
| checkpoint failed count | `0` after import |
| checkpoint running count | `0` after import; during import, no row stale for `> 60` minutes |
| checkpoint pending count | `0` after full import, unless intentionally stopped |
| duplicate bars | `0` rows for `(symbol_id, timeframe, ts)` in the `source = 4` set |
| daily 5f anomaly ratio | `<= 0.1%`, or every anomaly has an exception |
| bar-end grid | `0` off-grid rows in Asia/Shanghai local time |
| bar-end labels | sampled full days include expected labels such as `09:30`, `11:30`, `13:05`, `15:00` |

## Handling Findings

Failed checkpoints:

- inspect `error_message`, `zip_path`, and `member_path`
- fix the source/dependency/schema issue
- rerun the importer; do not delete successful checkpoints

Stale running checkpoints:

- confirm no importer process is active
- resume with `--reset-running`
- keep the stale checkpoint query output in evidence

Missing symbols:

- classify as listing-date, suspended, excluded universe, missing source member,
  or import failure
- do not hand off to Chan until public-universe misses are resolved or waived

Watermark drift:

- compare `scheme2_ingest_watermarks.last_bar_end` with `max(klines.ts)` for
  `source = 4`
- repair metadata only after confirming landed bars are correct

Daily count anomalies:

- compare the symbol-day against the source parquet member
- expected exception categories are listing boundary, suspension, source gap,
  or approved exclusion
- anomaly ratio above `0.1%` is a no-go without explicit acceptance

Duplicate bars:

- stop cutover
- check whether the source has duplicate member coverage or database constraints
  were bypassed
- keep both the duplicate query and checkpoint identity query in evidence

Timestamp semantic issues:

- stop cutover if any landed row is off the 5-minute grid
- stop cutover if evidence suggests `trade_time` was shifted by `+5m`
- keep the source audit and landed timestamp sample together for review

## Handoff To Chan Bootstrap

Chan bootstrap can start only after:

- import coverage is complete for the target universe
- checkpoint failed/running counts are clean
- watermarks align to landed `source = 4` bars
- daily anomalies are either below threshold or fully explained
- `bar_end` semantics are confirmed in both source samples and landed rows

After Chan bootstrap, continue with
`docs/runbooks/scheme2-verification-gates.md` Gate 3 and later gates.

Before NAS cutover, complete
`docs/runbooks/scheme2-nas-preflight.md` for the deploy-before and
deploy-after automatic resume checks.
