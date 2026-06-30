# Scheme 2 Verification Gates

This runbook defines the final go/no-go gates before a NAS deployment that
serves Scheme 2 data.

Scheme 2 deployment is considered ready only when all of these are true:

- canonical `5f` history is imported from 2010 to the agreed bootstrap cutoff
- full Chan precompute is published for `5f`, `30f`, and `1d`
- incremental resume state is clean or explicitly quarantined
- API bundle reads are stable and do not rely on legacy primary paths
- the frontend reads bundle data on the main path
- the target NAS survives the expected concurrent drag-and-switch workload

## Deployment assumptions

- Canonical market-data source is `5f`.
- Canonical time semantics are `bar_end`.
- `30f` and `1d` Chan levels are derived from stored canonical `5f` bars.
- Frontend primary HTTP read path is `/api/v3/chart/bundle`.
- Frontend primary WebSocket read path is `get_chart_bundle`.
- `scheme2_chan_published_heads` and `scheme2_chan_recompute_watermarks` are
  required for a full PASS. Missing tables mean the release is `BLOCKED`.

## Hard stop rules

- Do not deploy when any gate is `FAIL` or `BLOCKED`.
- Do not accept placeholder Chan engines such as `api-fake-overlay`.
- Do not cut over while uncontrolled dirty ranges exist.
- Do not run `docker compose ... down -v` on the active NAS deployment.
- Do not treat `/api/v1/chart/window` as the frontend primary path.

## Recommended evidence bundle

Capture one evidence folder per final check, for example:

```text
work/verify/scheme2/<YYYYMMDD-HHMMSS>/
```

Save:

- source inventory and header/schema samples
- SQL outputs for import and Chan coverage
- bundle smoke JSON samples
- frontend source grep output
- pressure-test latency summary
- host or container CPU, memory, disk, and restart observations
- completed NAS preflight notes from
  `docs/runbooks/scheme2-nas-preflight.md`

## Gate 1: Data Source Audit

Goal: prove the bootstrap source itself is usable before trusting any import
counts.

### What to inspect

- zip or parquet inventory under the agreed source root
- field names or schema columns
- year coverage from 2010 through the current year
- `bar_end` semantics on sampled full-session trading days
- per-symbol-day `5f` count anomalies

### Required checks

1. Inventory:
   - every required year from `2010` through the current year is present, or a
     missing year has an explicit source note
   - source files are discoverable for the agreed stock universe
2. Field mapping:
   - source columns can be mapped to:
     - symbol
     - exchange
     - `bar_end`
     - open
     - high
     - low
     - close
     - volume
     - optional amount
3. `bar_end` semantics:
   - the sampled session template must be written down from the actual source
     data before import
   - for the current parquet ZIP source, observed normal days include a
     `09:30` first timestamp, `11:30` morning close, `13:05` afternoon restart,
     and `15:00` final timestamp
   - these timestamps are accepted as source `bar_end` labels and must not be
     shifted by `+5m`
4. Per-day `5f` counts:
   - default expectation must be derived from the agreed source session template
   - for the current parquet ZIP source, a full normal trading day may contain
     `49` rows because the source includes a `09:30` timestamp
   - `0` is acceptable only for suspended or explicitly excluded symbol-days
   - any value outside the agreed normal-day set must be listed as an anomaly

### PASS criteria

- zero unmapped required fields
- zero unknown timestamp semantics
- all years covered or explicitly waived
- anomaly ratio under `0.1%` of sampled symbol-days, or every anomaly is
  documented with cause and disposition

### FAIL examples

- source headers cannot be mapped to `bar_end`
- sampled rows look like `bar_start` timestamps
- large year gaps exist in the archive root
- abnormal `5f` day counts appear without an exception list

## Gate 2: Import Completion

Goal: prove the bootstrap import wrote the expected canonical `5f` history into
PostgreSQL.

### What to inspect

- `klines` at timeframe code `5` and `source = 4`
- `scheme2_ingest_watermarks`
- `scheme2_source_member_checkpoints`
- symbol coverage, total row count, per-symbol min/max `ts`
- checkpoint success, failed, running, pending, and skipped counts
- day-count anomalies and unexplained gaps
- duplicate canonical bars
- `bar_end` timestamp semantics after landing

### Required checks

1. Universe denominator is frozen:
   - default recommendation: active A-share `stock` symbols in `symbols`
   - if delisted or paused symbols are included, the denominator must be named
     explicitly in the evidence folder
2. Coverage:
   - every target symbol has at least one canonical `5f` row where
     `klines.source = 4`
   - `source = 4` is the parquet historical bootstrap source
3. Range:
   - per symbol `min(ts)` reaches the expected lower bound:
     - the first available canonical source `5f` bar end for symbols listed before 2010
     - listing start for newer symbols
   - per symbol `max(ts)` reaches the agreed bootstrap cutoff
4. Watermark alignment:
   - `scheme2_ingest_watermarks.last_bar_end` matches the imported `5f`
     high-water mark per symbol
   - every imported symbol has one `scheme2_ingest_watermarks` row at
     `timeframe = 5`
   - watermark `source` is `parquet_5f` or an explicitly approved equivalent
5. Checkpoint state:
   - `scheme2_source_member_checkpoints` has rows for `source_profile =
     'parquet_5f'` and `timeframe = 5`
   - after the full import, `failed = 0`, `running = 0`, and `pending = 0`
     unless the run was intentionally stopped before completion
   - during import, `running` rows must keep advancing; any row unchanged for
     more than `60` minutes is stale and needs operator review
6. Gap and anomaly statistics:
   - day-count anomalies are enumerated
   - unexplained import gaps are zero before cutover
   - normal full trading days should have `48` or `49` source bars, depending
     on the agreed source template
   - anomaly ratio must stay at or below `0.1%` of landed symbol-days, or every
     anomaly must have a documented exception
7. Duplicate bars:
   - no duplicate rows may exist for the canonical key
     `(symbol_id, timeframe, ts)` in the `source = 4` import set
   - if the database primary key prevents duplicates, keep the zero-row query
     output as evidence
8. Landed `bar_end` semantics:
   - imported timestamps must stay on the 5-minute grid in Asia/Shanghai time
   - sampled full days should include source labels such as `09:30`, `11:30`,
     `13:05`, and `15:00`
   - the landed `ts` values must not be shifted by `+5m`

### PASS criteria

- `100%` of target symbols have canonical `source = 4` `5f` rows
- `100%` of target symbols have an ingest watermark
- `100%` of imported symbol watermarks align to `max(klines.ts)` for
  `source = 4`
- `0` failed checkpoints after the import
- `0` stale running checkpoints
- `0` duplicate canonical bars
- `0` off-grid timestamp rows
- zero unexplained missing-year, missing-symbol, or missing-day gaps
- the bootstrap cutoff is met for the public universe

### Post-import acceptance metrics

Record these values in the evidence folder:

| Metric | Threshold | If outside threshold |
| --- | --- | --- |
| `source = 4` `5f` rows | greater than `0`; covers the target universe | stop cutover; confirm import worker used `parquet_5f` and check source root |
| imported symbols / target symbols | `100%` for the public universe | classify missing symbols as listing-date, suspended, excluded, or source gap |
| watermark rows / imported symbols | `100%` | rerun watermark repair only after confirming landed bars are correct |
| watermark drift | `0` rows | compare `last_bar_end` to `max(ts)`; fix metadata before Chan handoff |
| checkpoint failed rows | `0` | inspect `error_message`, fix source or schema issue, rerun failed members |
| checkpoint running rows after import | `0` | if stale, rerun importer with `--reset-running` after confirming no worker is active |
| daily anomaly ratio | `<= 0.1%`, or every row has an exception | compare source member, classify cause, quarantine or repair before cutover |
| duplicate bars | `0` | stop; preserve query output and investigate source/member overlap or constraints |
| off-grid landed timestamps | `0` | stop; verify no timezone or `+5m` shift was introduced |

### SQL focus

Use SQL outputs for:

- distinct imported symbols at timeframe `5` and `source = 4`
- total `source = 4` `5f` row count
- source mix grouped by `timeframe, source`
- per-symbol `min(ts)`, `max(ts)`, and row counts
- checkpoint status counts and failed/running samples
- per-symbol-day `count(*)` anomaly summary
- ingest watermark alignment against `max(ts)`
- duplicate bar query grouped by `(symbol, timeframe, ts)`
- landed `bar_end` semantic query for 5-minute grid and session labels

## Gate 3: Chan Completion

Goal: prove full Chan precompute is published and clean before the first public
Scheme 2 cutover.

### What to inspect

- `scheme2_chan_published_heads`
- `scheme2_chan_recompute_watermarks`
- `chan_runs`
- published head coverage for `5f`, `30f`, and `1d`

### Required checks

1. Published heads:
   - every target symbol x Chan level x mode has one published head at
     `base_timeframe = 5`
2. Published range:
   - `base_from_bar_end` is at or before the symbol's imported first `5f`
   - `base_to_bar_end` is at or after the symbol's latest imported `5f`
3. Snapshot integrity:
   - `snapshot_version` resolves to a real run or real Chan detail rows
4. Dirty-range state:
   - preferred state is `dirty_from_bar_end is null`
   - controlled exceptions are allowed only when:
     - the symbol is quarantined from public release
     - the reason is allowlisted
     - the exception is named in the evidence folder
     - the total controlled dirty scope stays under `0.1%` of public symbol x
       level x mode combinations

### PASS criteria

- `100%` published head coverage for the public universe
- `100%` published heads in `published` state
- zero uncontrolled dirty ranges
- zero published heads whose `base_to_bar_end` lags the imported watermark for
  public symbols

### Suggested dirty reason allowlist

- `manual-waiver`
- `halted-symbol`
- `late-source-repair`

Anything else is a fail until explicitly approved.

## Gate 4: API Bundle Verification

Goal: prove the runtime serving path can return canonical bars plus three-level
Chan data with stable snapshot identity.

### What to inspect

- HTTP `GET /api/v3/chart/bundle`
- WebSocket `get_chart_bundle`
- repeated identical requests during a quiet window

### Required checks

Sample at least:

- `10` random symbols from the public universe
- `3` chart timeframes: `5f`, `30f`, `1d`
- `2` identical reads per sample during a quiet window

For each sample:

- HTTP returns `200`
- `bars` is non-empty
- `chan` is present
- `chan.levels` contains `5f`, `30f`, and `1d`
- `chan.base_timeframe == "5f"`
- top-level `bar_time_semantics == "bar_end"`
- top-level `base_timeframe == "5f"`
- `bars_by_level` includes all three analysis levels
- `snapshot_version` is stable across repeated identical reads
- `snapshot_id` is stable across repeated identical reads

### PASS criteria

- no `4xx` or `5xx` responses in the sample set
- no placeholder engine
- zero empty `bars` payloads for active public symbols
- zero empty three-level Chan payloads
- repeated identical requests remain stable when the dataset is not advancing

### Release recommendation

For Scheme 2 cutover, the preferred engine is a published-head or precomputed
engine. If the runtime still falls back to a live recompute engine for covered
windows, mark the release as `RISK ACCEPTANCE REQUIRED`.

## Gate 5: Frontend Read-Only Bundle Path

Goal: prove the frontend does not depend on the legacy chart-window route as its
primary path.

### What to inspect

- `apps/web/src/api/client.ts`
- `apps/web/src/api/chartDataManager.ts`
- `apps/web/src/App.tsx`
- network traffic from one manual browser session

### Required checks

1. Source-level checks:
   - main HTTP path reads `/api/v3/chart/bundle`
   - `getChartWindow()` delegates to bundle reads instead of directly calling
     `/api/v1/chart/window`
   - WebSocket reads use `get_chart_bundle`
   - no direct `/api/v1/chart/window` fetch exists under `apps/web/src`
2. Runtime spot check:
   - one browser drag / zoom / timeframe-switch session shows bundle reads as
     the main path
   - compatibility payloads such as `chart-window.v1` may still exist for
     server-side compatibility, but not as the frontend primary route

### PASS criteria

- zero direct legacy chart-window route reads in `apps/web/src`
- zero network captures where `/api/v1/chart/window` is the primary path
- bundle path remains usable when switching between `5f`, `30f`, and `1d`

## Gate 6: Pressure Test

Goal: prove the NAS can sustain the expected read workload before public cutover.

### Scenario

Run three tiers:

1. `5` concurrent users
2. `10` concurrent users
3. `20` concurrent users

Each virtual user repeats:

1. open one symbol at `5f`
2. drag or pan through `5` history windows
3. switch `5f -> 30f -> 1d -> 5f`
4. repeat for `5` minutes after warmup

### Observe

- HTTP latency for `/api/v3/chart/bundle`
- WebSocket response latency for `get_chart_bundle`
- API, chan-service, and database CPU
- API, chan-service, database, and Redis memory
- host or container disk throughput and queueing
- restart count or healthcheck flaps
- application error count

If the NAS cannot expose per-container disk latency, record host-level disk busy
percent, queue length, and read or write latency from the NAS monitoring panel.

### PASS thresholds

- `5` users: bundle `p95 <= 1.0s`
- `10` users: bundle `p95 <= 1.5s`
- `20` users: bundle `p95 <= 2.5s`
- error rate under `1%` at every tier
- zero container restarts
- zero sustained healthcheck failures
- no sustained API or chan-service CPU above `85%`
- no sustained database storage saturation that causes visible latency growth

If the target NAS is materially weaker than the current benchmark host, tighten
the user tier before public launch instead of silently accepting higher tail
latency.

## Final Go/No-Go rule

Release is `GO` only when:

- Gate 1 through Gate 6 are all `PASS`
- no gate is `BLOCKED`
- every exception is written down with owner and expiry
- rollback steps are confirmed from
  `docs/runbooks/nas-safe-upgrade-with-existing-tdx-data.md`

Release is `NO-GO` when any of these are true:

- source audit is incomplete
- import coverage is partial
- published heads are missing
- dirty ranges are uncontrolled
- bundle snapshots are unstable without active writes
- frontend primary path still depends on legacy window reads
- the NAS cannot hold the agreed concurrency tier

## Local helper

Use the local helper skeleton to collect a first-pass audit:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1
```

With no parameters, the helper should not fail the shell. It prints bootstrap
commands, static checks, and `BLOCKED` SQL/API gates that need live parameters.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 `
  -DataRoot '<path-to-zip-or-parquet-root>' `
  -ApiBaseUrl 'http://127.0.0.1:8001' `
  -ApiToken 'dev-local-token' `
  -DatabaseUrl 'postgresql://trader:password@127.0.0.1:5432/tradingview_local'
```

The helper does not require Docker to be running. It probes what is available,
prints missing prerequisites, and emits SQL or HTTP follow-up hints when a live
check cannot be completed in the current shell.

For the import-specific command sequence, thresholds, and handling advice, use
`docs/runbooks/scheme2-bootstrap-runbook.md`.

For the NAS cutover checklist that spans import-before, import-during,
import-after, deployment-before, and deployment-after automatic resume checks,
use `docs/runbooks/scheme2-nas-preflight.md`.
