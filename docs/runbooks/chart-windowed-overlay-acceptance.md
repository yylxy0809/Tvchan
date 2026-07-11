# Chart Windowed Overlay Acceptance

Run this only after Tasks 3, 4, and 5 are merged and reviewed. The harness is a GET-only acceptance client: it does not mutate the database, apply migrations, run Module C recompute, flush server caches, start or stop processes, or launch a browser.

## Rollout Sequence

1. Merge and review Tasks 3-5, then run their API/web tests.
2. Complete the bounded canonical K-line audit and repair for the controlled scope.
3. Recompute a controlled watchlist with the current Module C configuration hash. Verify complete successful heads and endpoint parity before switching those heads.
4. Run this Task 6 harness and the manual browser/realtime checklist against the controlled watchlist.
5. Start the full Module C rollout only after the database audit repair and controlled acceptance pass. Keep old heads until the new generation is verified.

## Migration And Opt-In Integration Test

Migration `025` contains `CREATE INDEX CONCURRENTLY`; run it with normal `psql` autocommit and never inside a transaction. The acceptance harness does not run either command.

```powershell
# Nontransactional: apply only migration 025 through psql autocommit.
.\scripts\apply-db-migrations.ps1 `
  -Only 025_chan_c_window_indexes.sql `
  -ContainerName tv_backend_timescaledb

# Dedicated disposable/staging database only. The test rolls back its fixture rows.
$env:CHAN_WINDOW_TEST_DATABASE_URL = 'postgresql://trader:REDACTED@127.0.0.1:5432/tradingview_window_test'
pytest services/api/tests/test_windowed_chan_postgres_integration.py -q
Remove-Item Env:CHAN_WINDOW_TEST_DATABASE_URL
```

Confirm plans with `EXPLAIN (ANALYZE, BUFFERS)` on staging before production. Never point the opt-in integration test at production.

## Harness Tests And Dry Run

The Task 6 tests require only local PowerShell/Pester and do not contact an API.

```powershell
Import-Module Pester -MinimumVersion 3.4.0
Invoke-Pester .\scripts\chart-windowed-overlay\verify-chart-windowed-overlay.Tests.ps1

# Parses configuration and records the complete planned matrix in memory.
# No GET, manager test, browser, server, DB, or evidence write occurs.
.\scripts\verify-chart-windowed-overlay.ps1 -WhatIf
```

## Acceptance Run

Defaults are sequential (`concurrency=1`), bounded to 300 bars, three warm repeats, and at most 100 matrix cells. The default matrix is four symbols x seven chart timeframes x two windows = 56 cells. It is never silently truncated. Increase `-MaxMatrixCells` explicitly if adding samples.

```powershell
$env:VITE_API_TOKEN = 'REDACTED'
.\scripts\verify-chart-windowed-overlay.ps1 `
  -ApiBaseUrl 'http://127.0.0.1:8001' `
  -WebUrl 'http://127.0.0.1:5173' `
  -Symbols '000001.SZ','600000.SH','430047.BJ','000017.SZ' `
  -SparseSymbol '000017.SZ' `
  -EqualPriceSymbol '000001.SZ' `
  -Timeframes '5f','15f','30f','1h','1d','1w','1m' `
  -WindowNames 'cold-first-observation','shifted-overlap' `
  -BarLimit 300 `
  -WarmSamples 3 `
  -RequestTimeoutSeconds 10 `
  -MaxMatrixCells 100 `
  -RunId 'controlled-watchlist-20260711'
```

`cold-first-observation` means the first harness observation of a unique bounded request. It is not proof of a cold database, HTTP, or server cache. The harness never flushes a cache and records `serverCacheFlushed=false`. `shifted-overlap` is reported separately and is not included in the first-observation distribution.

`-ManagerContractTests Auto` runs the focused local `chartDataManager.contract.test.ts` suite when the already-installed `tsx` executable is available. It does not install dependencies. Use `Run` to make absence a failure or `Skip` to record `PENDING`.

The source gate invokes the installed Node executable and project-local TypeScript compiler at `apps/web/node_modules/typescript/lib/typescript.js`. `-SourceScanTimeoutSeconds 10` bounds input transfer, compiler execution, and output capture. Missing Node/compiler/scanner files, process timeout, nonzero exit, oversized output, invalid scanner JSON, or TypeScript syntax diagnostics are explicit fail-closed uncertainty.

`RunId` is restricted to 1-128 ASCII characters matching `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. Paths are canonicalized and verified as descendants of `OutputRoot`. A same-RunId exclusive lock is held from preflight through evidence promotion; a concurrent run fails with exit code `2` instead of sharing a staging directory. `-NoDefaultSymbols -NoSupplementalSymbols` intentionally produces an empty matrix and is rejected.

## Bounded HTTP Reads

Each request uses `.NET HttpClient` with `ResponseHeadersRead`, a cancellation deadline, and bounded streaming. A declared `Content-Length` over the endpoint cap is rejected before reading the body. Chunked or inaccurate-length responses are stopped once the streamed byte cap would be crossed. JSON parsing happens only after the bounded download succeeds.

PowerShell 7 uses normal cancellation-token behavior. Windows PowerShell 5.1 uses the same `HttpClient` timeout, cancellation token, `ResponseHeadersRead`, and bounded stream loop as a fail-closed fallback. On .NET Framework, DNS cancellation can follow platform timing rather than stop at the exact deadline; this limitation is recorded in `effectiveConfig.resourceSafety.ps51Fallback`. A timeout is still a harness `FAIL`, never a latency sample.

## Automated Gates

- Every configured symbol x timeframe x window cell is present in the report, including unavailable sparse/suspended samples as `SKIP`.
- Empty effective symbol, timeframe, window, or Cartesian matrices fail before probing.
- Every bars and overlay response must pass HTTP and content validation before its latency can enter a distribution. Failed requests cannot become `0ms` passes.
- Request timeout and raw response byte caps are enforced before JSON parsing; timeout, oversize, transport, and invalid-JSON results fail closed.
- Bars are unique, ascending, bounded to `[from,to)`, and do not exceed the configured count or byte limits.
- Overlay levels exactly match: `5f/15f -> 5f,30f,1d`; `30f/1h -> 30f,1d`; `1d -> 1d,1w`; `1w -> 1w,1m`; `1m -> 1m`.
- Overlay IDs are stable/nonempty and unique within type/level/mode. Centers overlap the inclusive overlay window, signals are inside it, and each line type/level/mode has at most one predecessor and one successor.
- Default payload limits are bars 2 MiB, overlay 4 MiB, 6,000 overlay objects, and `12 x returned bars + 32` objects. All effective limits are recorded and can be overridden explicitly.
- Validated first-observation and warm p50/p95 are collected. Gates are bars first-observation p95 `<500ms`, warm p95 `<100ms`; overlay first-observation p95 `<800ms`, warm p95 `<150ms`.
- The no-bundle source gate uses a project-local TypeScript compiler `Program`, type checker, and AST, not PowerShell brace or token parsing. Required semantic roots are the datafeed and `ChartDataManager` bars-history paths, `ChartWorkspace` overlay lifecycle, `ChartDataManager.subscribeChanOverlay` plus websocket message dispatch, `ChanOverlayManager` request/resync/realtime paths, and `ChanRealtimeOverlayBridge` hydration/apply paths. A missing root fails closed.
- From each root, the scanner follows checker-resolved same-file method, function, callback, and simple callable-alias edges, including `Function.prototype.call`, `apply`, and `bind` chains. Dormant compatibility bundle loaders are allowed only while unreachable from that production graph; a required root that calls or aliases one is a finding. Duplicate class/function/module root identities and ambiguous callable symbols are uncertainty, never last-write-wins selection.
- Within each active scope, the gate propagates simple local `const`/`let` symbol aliases for forbidden loaders, network methods, and endpoint strings. Supported strings include literals, no-substitution templates, templates whose substitutions resolve to local constants, and literal concatenation; direct calls, alias chains, `.bundle`/computed bundle access, transport commands, and resolved endpoint aliases passed to `fetch`/request/client methods are findings.
- Analysis is intentionally bounded to 20,000 AST nodes per scope, 512 aliases, 512 reachable declarations per semantic area, 8 propagation passes, and expression depth 16. Syntax errors, missing compiler/semantic roots, nonconvergent chains, and unsupported dynamic/computed network or call aliases that may touch bundle traffic are recorded as uncertainty and fail closed; this is not represented as full interprocedural semantic analysis.
- Focused manager cache/coalescing/revisit tests are captured when locally runnable. Browser-level zero-HTTP revisit remains an explicit manual assertion.

HTTP errors and malformed content are `FAIL`. A missing configured symbol, empty data sample, or unavailable authoritative snapshot is `SKIP`. Unexecuted browser, realtime, optional manager, or source evidence is `PENDING`, never `PASS`.

Exit code `2` means at least one contract, source, payload, matrix, manager-test, or latency gate failed.

## Evidence Promotion And Recovery

The harness writes under `outputs/chart-windowed-overlay-verification` using this sequence:

1. Write `report.json` and `report.md` atomically inside `.<run-id>.staging`.
2. Hash both files and write `manifest.json` last.
3. Validate the staged manifest and hashes.
4. Rename the staging directory to `<run-id>` in one same-filesystem promotion.

The final run directory is therefore never exposed with only one report file. If execution is interrupted, rerun with the same `-RunId`; the staging directory is recovered and completed. If the final directory already has a valid manifest, rerun is idempotent and returns its existing result. A corrupt/incomplete final directory is rejected rather than overwritten.

Every dynamic Markdown field is emitted as fixed `<code>` text after HTML escaping and entity-encoding link/image/autolink punctuation, URL separators/protocol colons, pipes, backticks, and line breaks. Untrusted values therefore cannot create links, images, HTML, or remote-content references. JSON remains the authoritative machine-readable evidence.

The dedicated Pester suite executes the complete script in child Windows PowerShell 5.1 processes against a bounded raw local TCP mock. It covers invalid timestamps, timeout, declared oversize and chunked oversize streaming, traversal and concurrent RunIds, empty matrices, current semantic-root discovery, deleted old scopes, missing/renamed and duplicate class/function/module roots, dormant compatibility loaders, reachable bundle aliases, direct/aliased `call`/`apply`/`bind` chains, dynamic computed receivers, AST comments/literals/layout/destructuring, static templates, syntax failure, Markdown link/image/autolink escaping, and interrupted evidence recovery. These tests do not inspect implementation source strings.

## Manual Browser And Realtime Acceptance

Run these after the API report has no failures. Save network/console evidence with the promoted report.

- Perform five rapid drags. Confirm the 150ms debounce leaves at most one completed overlay paint and superseded drags produce zero completed overlay HTTP responses.
- Revisit an already covered bars and overlay window. Confirm zero HTTP requests, no flash-empty state, and no full redraw.
- Rapidly switch symbols and timeframes. Confirm no stale paint, no visible `AbortError` noise, and K-lines paint before overlay.
- Validate endpoint, center, and signal projection. For the configured equal-price sample, confirm the last matching chart bar is selected.
- Measure overlay merge/paint completion under 2 seconds in the browser.
- Force a WebSocket sequence gap and verify one active-window HTTP snapshot repairs it. Then force WebSocket loss and verify HTTP fallback preserves the last valid overlay.

The GET-only harness always labels these browser/realtime checks `PENDING`; it cannot satisfy them by inference from API timing.
