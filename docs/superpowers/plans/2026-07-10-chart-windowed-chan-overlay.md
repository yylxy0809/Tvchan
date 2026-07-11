# Chart Windowed Chan Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make TradingView K-lines load on demand and render authoritative Module C Chan structures for only the current chart window and required analysis levels, without full bundle reloads.

**Architecture:** Module C remains the only authoritative Chan engine. The API returns bounded window overlays from published Module C runs; TradingView loads K-lines through `from/to/countBack`, paints bars first, and asynchronously merges stable-ID overlay snapshots/deltas. PineJS remains a renderer rather than a second Chan engine.

**Tech Stack:** FastAPI, asyncpg/PostgreSQL/TimescaleDB, React/TypeScript, TradingView Charting Library, PineJS custom indicators, WebSocket.

---

## Frozen Contracts

- Module C sets `bi_strict=False` and `bi_allow_sub_peak=False`; the configuration hash must change so old runs cannot be mistaken for new-semantic runs.
- Display levels are exact: `5f/15f -> 5f,30f,1d`; `30f/1h -> 30f,1d`; `1d -> 1d,1w`; `1w -> 1w,1m`; `1m -> 1m`.
- Main chart history uses `/api/v3/chart/bars`; main Chan history uses `/api/v3/chart/overlay`. `/bundle` remains compatibility-only.
- Overlay windows are inclusive in canonical bar-end time. Lines include intersecting rows plus one predecessor and successor per level/mode; centers include every overlap with original bounds; signals are window points only.
- Published heads must match symbol, level, requested mode, native `base_timeframe=chan_level`, successful run and full window coverage.
- Frontend merges by authoritative stable `id`; K-lines paint before overlay; stale requests cannot paint after symbol/timeframe changes.

## Task 0: Establish Canonical K-Line Data Truth

**Files:** collector provider/storage code, `db/sql/023_kline_source_coverage.sql`, `db/sql/024_kline_canonical_audit.sql`, a new audit/repair command with tests, and an operational report under `outputs/kline-canonical-audit`.

- [ ] Audit every active symbol and `5f/30f/1d/1w/1m` for exact timestamp duplicates, logical period duplicates, invalid A-share bar-end timestamps, source conflicts and OHLCV disagreement. Run bounded timeframe/date shards so the audit never launches an unbounded full-hypertable aggregate.
- [ ] Define one shared source priority and logical bar-key contract. Native imported history (`parquet_native`/approved parquet) wins its covered history, `pytdx` wins recent native bars, Tencent/Baidu/Mootdx are fallback-only, and derived bars cannot overwrite a valid native bar.
- [ ] Normalize provider timestamps before write. A daily bar is keyed to the trading-day close (`15:00 Asia/Shanghai`); intraday bars use canonical session bar ends; weekly/monthly bars use their canonical completed-period end.
- [ ] Add source-priority protection to K-line upsert so a lower-priority fallback cannot overwrite a higher-priority canonical row at the same logical timestamp.
- [ ] Implement dry-run-first repair: when a higher-priority logical bar exists, quarantine/delete the lower-priority duplicate; when only a fallback row exists, normalize it to the canonical timestamp without creating a duplicate. Record before/after counts, disagreements and affected symbols. Never delete the sole valid logical bar.
- [ ] Make API and Module C readers reject or deduplicate unresolved logical duplicates during rollout, so old dirty rows cannot reach charting or Chan calculation before cleanup completes.
- [ ] Execute the all-active-symbol audit only as bounded symbol/timeframe/date shards with concurrency `2`, `statement_timeout=20s`, `lock_timeout=1s`, and resumable checkpoints. Dry-run is the default; mutation requires explicit `--apply --audit-run-id`. Repair transactions contain at most 500 logical groups and quarantine every removed physical row before deletion.

**Acceptance:** `000001.SZ` has exactly one daily logical bar per trading date and the March-May fixture uses source-2 15:00 bars; all active-symbol audits report zero unresolved logical duplicates before Module C recomputation; exact OHLC disagreements remain quarantined for review rather than silently selected; audit shards use bounded time predicates and keep DB active audit queries within the configured concurrency.

## Task 1: Freeze Module C No-Sub-Peak Semantics

**Files:** `services/chan-service/chan_service/module_c_adapter.py`, `services/collector/collector/chan_module_c_recompute.py`, related chan-service/collector tests.

- [ ] Add failing tests proving Module C explicitly sets `bi_allow_sub_peak=False` and that the run configuration hash identifies the new semantic version.
- [ ] Add the smallest configuration change and bump `MODULE_C_CONFIG_HASH` to a new immutable value containing `bi-allow-sub-peak-false`.
- [ ] Add a static canonical source-2 daily fixture for `000001.SZ`: the 2026-03-23 low near `10.43` must not terminate at the earlier `11.32` sub-peak; the up-stroke endpoint is the later absolute high (`2026-04-30`, `11.60`). Select the stroke by endpoint identity, not list position.
- [ ] Run chan-service and collector focused/full tests. Do not publish or delete production heads during unit tests.

**Acceptance:** config is explicit; old hash is absent from newly created runs; A/B fixture distinguishes the old and new endpoint; no `chan.py` vendor source is modified.

## Task 2: Implement Windowed Module C Overlay Reads

**Files:** `services/api/app/routes/chan.py`, `services/api/app/repositories/chan_postgres.py`, `services/api/app/routes/chart.py`, `services/api/tests`, `db/sql/025_chan_c_window_indexes.sql`.

- [ ] Add failing level-mapping tests for all seven chart timeframes and make one shared backend mapping enforce the frozen contract.
- [ ] Stop loading every analysis timeframe plus unconditional `5f`; load only chart bars needed to establish the requested window and projection candidates needed by a higher-level endpoint.
- [ ] Select published heads by requested level/mode, native base timeframe, successful run and complete window coverage. Generate a deterministic composite version when levels publish independently.
- [ ] Query lines by inclusive range overlap; add one adjacent predecessor/successor; query centers by overlap and signals by point containment; preserve stable IDs and original geometry.
- [ ] Add B-tree/GiST indexes only where `EXPLAIN (ANALYZE, BUFFERS)` proves the existing indexes insufficient. Execute index creation non-transactionally when required.
- [ ] Preserve boundary context in compatibility bundle grouping, but keep `/bundle` out of the production frontend path.

**Acceptance:** exact levels; wrong mode/base timeframe/failed/uncovered heads are rejected; no full detail-table scan; warm 300-bar overlay p95 `<150ms`, cold p95 `<800ms`; output size is proportional to the window plus at most two continuity lines per level/mode/type.

## Task 3: Implement Demand-Driven K-Line Loading

**Files:** `apps/web/src/tradingview/datafeed.ts`, `apps/web/src/api/chartDataManager.ts`, new focused contract tests.

- [ ] Replace fixed 300-3000 bar prefetch constants with a range planner honoring TradingView `from/to/countBack`; retain at most a 25% directional guard band capped at 200 bars.
- [ ] Route datafeed history through `chartDataManager.getBars`; cache sorted bars and covered intervals by symbol/timeframe; request only uncovered ranges or a `countBack` deficit.
- [ ] Coalesce overlapping requests, deduplicate inflight work and cancel the underlying HTTP request only when no consumer remains.
- [ ] Return strictly ascending unique bars; treat `to` consistently as an exclusive request boundary; set `noData=true` only when the historical side is proven exhausted.
- [ ] Use A-share session/timezone metadata and preserve daily/weekly/monthly TradingView timestamp normalization.

**Acceptance:** first request contains no fixed large prefetch; revisiting a covered range causes zero HTTP calls; rapid symbol/timeframe switches never paint stale callbacks; cold bars p95 `<500ms`, warm cache p95 `<100ms`; response bars do not exceed `countBack + guard` except when the backend must bridge a trading-calendar gap.

## Task 4: Decouple and Incrementally Render Overlay

**Files:** `apps/web/src/components/ChartWorkspace.tsx`, `apps/web/src/api/chartDataManager.ts`, `apps/web/src/tradingview/chanStudy.ts`, `apps/web/src/tradingview/widget.ts`, contract tests.

- [ ] Paint bars immediately, then debounce overlay requests by 150ms using per-symbol/timeframe AbortController and generation fencing.
- [ ] Request `/overlay` with the exact display levels; never call `getChartWindow` or `/bundle` from normal history, drag, symbol-switch or timeframe-switch paths.
- [ ] Merge strokes/segments/centers/signals by stable ID. Retain overlapping geometry and one line predecessor/successor at the retained bar boundary; evict objects wholly outside retained history.
- [ ] Update PineJS state without removing/recreating the study solely because snapshot version or object count changes. Rebuild only affected level caches and reconcile fallback drawings by stable ID.
- [ ] Keep higher-level point projection deterministic: search the higher-level native K interval on the chart timeframe, match target extreme price, and choose the last equal-price bar.

**Acceptance:** K-lines appear without waiting for overlay; five rapid drag events result in at most one completed overlay paint after the last event; no flash-empty/full redraw; cached-window revisit performs no overlay HTTP request; endpoint, center and signal projection tests pass for every level mapping.

## Task 5: Realtime Delta, Rollout and End-to-End Verification

**Files:** `services/api/app/routes/chart_ws.py`, frontend realtime/cache code, API/web realtime tests and runbook.

- [ ] Define a versioned Chan event envelope with `kind`, `snapshot_version`, `base_version`, `sequence`, `range`, stable-ID `upserts` and `deletes`. A delta must not contain a renamed full overlay.
- [ ] Apply a delta only when `base_version` matches cached state; ignore duplicate/older sequence; fetch one active-range snapshot on a gap.
- [ ] Keep bar revisions and overlay versions separate; a new bar invalidates only the affected tail/range, not every symbol cache entry.
- [ ] Recompute a controlled watchlist sample with the new Module C hash, validate endpoint parity and switch only complete successful heads. Then run active-symbol recomputation as an operational rollout with progress and failure accounting.
- [ ] Record `bars.network/cache`, `overlay.network/merge/paint`, payload bytes, abort count and rendered object count. Run API, collector, chan-service and web tests plus desktop smoke tests.

**Acceptance:** no old-config head is served as new semantic data; snapshot gaps self-heal; Redis/WS loss falls back to HTTP without losing the last valid overlay; 5f chart displays `5f+30f+1d`; 30f displays `30f+1d`; day/week/month mappings are exact; switch and drag meet Task 2-4 latency targets; no duplicate K-line times, endpoint drift or full bundle requests appear in debug logs.

## Final Quality Gates

- [ ] Independent specification review confirms every frozen contract and metric.
- [ ] Independent code-quality review has no Critical or Important findings.
- [ ] `000001.SZ` endpoint regression, an SH symbol, a BJ symbol, sparse/suspended symbol and equal-price endpoint sample all pass.
- [ ] Production data mutation is limited to explicit migrations and controlled Module C recomputation; no valid old run is deleted until new heads are verified.
