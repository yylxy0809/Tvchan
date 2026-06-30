# Scheme 2 Bootstrap Team Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rebuild the data and serving pipeline around a 5f-only canonical market data source, complete the pre-deployment historical bootstrap from 2010 to present, compute and store three-level Chan snapshots before deployment, and switch production runtime to breakpoint-only collection plus incremental Chan updates.

**Architecture:** Historical parquet files under `D:\5f数据\5m_price` become the bootstrap source of truth for canonical 5f bars. All higher timeframe bars are derived from canonical 5f with A-share session-aware aggregation, and all Chan levels are derived recursively from canonical 5f. Production read paths serve a published snapshot head and never trigger full-history recomputation.

**Tech Stack:** Python 3.11, FastAPI, PostgreSQL/TimescaleDB, Redis, TradingView Advanced Charts, TypeScript/React/Vite, parquet bootstrap source.

---

## Fixed Assumptions

1. Canonical market data source is `5f` only.
2. Historical source is `D:\5f数据\5m_price`, with yearly zip archives and daily parquet members.
3. Source `trade_time` is treated as `Asia/Shanghai` bar end time and is not shifted.
4. Higher timeframe bars are served from 5f aggregation only.
5. Chan three-level output is always `5f / 30f / 1d`, all recursively derived from 5f.
6. `J:\stock_data.db` is an optional reconciliation source only, not a canonical runtime database.
7. Production runtime must not run full-history import or full-history Chan recompute loops.

## Command Structure

### Commander
- Main agent owns:
  - cross-agent sequencing
  - contract freeze
  - integration review
  - acceptance gate approval
  - conflict resolution

### Agent Groups
1. Data Bootstrap Agent
2. Backend Canonical/Runtime Agent
3. Frontend Projection Agent
4. Verification/Release Agent

## Shared Conflict Rules

1. No agent may change `trade_time` semantics unilaterally.
2. No agent may reintroduce high-timeframe canonical storage as a serving dependency.
3. No agent may change bundle payload shape without commander approval.
4. `snapshot_version` is a content ID, not a progress watermark.
5. Bootstrap workers and production workers are separate responsibilities and separate compose modes.
6. `chanStudy.ts` has single-owner policy during implementation.
7. Any file outside the assigned write scope requires commander reassignment first.

## Workstream 1: Data Bootstrap Agent

**Mission:** Build and validate the historical 5f bootstrap path from parquet into canonical PostgreSQL `klines(timeframe=5)`.

**Owned files/modules:**
- `services/collector/collector/*` for new parquet bootstrap worker only
- `services/collector/collector/storage/*` for bootstrap task/report storage only
- `db/sql/*` for bootstrap task/report migrations only
- `services/collector/tests/*` for bootstrap tests
- `docs/runbooks/*` for bootstrap runbooks

**Must not edit:**
- `services/api/**`
- `services/chan-service/**`
- `apps/web/**`
- aggregation logic for 15f+

### Task D1: Freeze import contract
**Deliverable:** one short contract note stating:
- only `5f` lands in canonical `klines`
- `trade_time = bar_end`
- no source-time shifting
- `J:` reconciliation is optional

**Acceptance:**
- commander signoff
- no open ambiguity on time semantics

### Task D2: Build parquet inventory audit
**Deliverable:** inventory report over all yearly zip files and all parquet members.

**Acceptance:**
- includes zip count, member count, per-member schema, row count, distinct symbols, min/max `trade_time`
- explicitly records current observed counts from source

### Task D3: Build time-semantic audit
**Deliverable:** report proving source times are treated as bar-end times.

**Acceptance:**
- explicit discussion of `09:30`, `11:30`, `13:05`, `15:00`
- no `+5m` or timezone compensation in importer path

### Task D4: Define task grain and resume key
**Deliverable:** bootstrap task model keyed at parquet-member granularity.

**Acceptance:**
- resume point is precise to `(zip, member, fingerprint)`
- restart does not redo already-committed members

### Task D5: Implement bulk import path design
**Deliverable:** importer path using parquet read -> staging -> bulk load/upsert.

**Acceptance:**
- does not use the current `executemany` hot path as the primary bootstrap path
- documents staging and final merge behavior

### Task D6: Run pilot import
**Deliverable:** small-sample import over at least one early and one recent trading day.

**Acceptance:**
- row counts match source
- min/max times match source
- OHLCV/amount map correctly
- no duplicate canonical key collisions

### Task D7: Run full bootstrap import
**Deliverable:** resumable import process over the full parquet history.

**Acceptance:**
- all imported rows are `timeframe=5`
- manifest-to-landed reconciliation succeeds
- restart can continue after interruption

### Task D8: Generate coverage and anomaly reports
**Deliverable:** reports for source coverage, loaded coverage, and anomalies.

**Acceptance:**
- anomaly categories include missing file, missing symbol-day, non-49-count symbol-day, non-monotonic time, duplicate timestamps
- output can feed later release gate checks

### Task D9: Emit Chan handoff manifest
**Deliverable:** symbol list and per-symbol `bar_from/bar_until/bar_count` handoff for Chan bootstrap.

**Acceptance:**
- downstream Chan bootstrap can consume it directly

### Task D10: Optional J-drive reconciliation
**Deliverable:** optional compare-only report against `J:\stock_data.db` when mounted.

**Acceptance:**
- absence of `J:` does not fail bootstrap
- compare path never writes back into canonical tables

## Workstream 2: Backend Canonical/Runtime Agent

**Mission:** Make 5f canonical bars and published Chan snapshots the only production serving truth.

**Owned files/modules:**
- `services/api/app/repositories/postgres.py`
- `services/api/app/routes/*.py`
- `services/api/app/services/*.py`
- `services/api/app/models.py`
- `services/collector/collector/chan_recompute.py`
- `services/collector/collector/storage/chan_*.py`
- `services/chan-service/**` only where required for snapshot/update protocol
- `deploy/*`
- `scripts/start-*.ps1`

**Must not edit:**
- parquet import worker owned by Workstream 1
- `apps/web/src/tradingview/chanStudy.ts`

### Task B1: Freeze backend invariants
**Deliverable:** ADR or design note for:
- 5f-only canonical bars
- published snapshot head
- `snapshot_version != watermark`

**Acceptance:**
- commander signoff

### Task B2: Complete A-share session-aware aggregation
**Deliverable:** 5f aggregation for `15f/30f/1h/1d/1w/1m`.

**Acceptance:**
- higher timeframe serving no longer depends on stored high-timeframe rows
- aggregation passes golden tests for session close boundaries

### Task B3: Add durable snapshot-head and watermark state
**Deliverable:** persistent metadata for published head, last complete 5f watermark, and dirty-from marker.

**Acceptance:**
- survives restart
- distinct concepts are not overloaded into `snapshot_version`

### Task B4: Make bundle API published-head-first
**Deliverable:** bundle and websocket routes read only published snapshots in prod.

**Acceptance:**
- no request-path full-history Chan calculation in prod mode
- response shape stays compatible with current frontend expectations

### Task B5: Build bootstrap Chan initialization path
**Deliverable:** one-time full-history three-level Chan bootstrap over canonical 5f.

**Acceptance:**
- each active symbol has published `5f/30f/1d` snapshot coverage
- initial frontend load no longer depends on live calculation

### Task B6: Add gap scanner
**Deliverable:** scanner that detects missing or dirty 5f ranges and emits repair tasks.

**Acceptance:**
- scans by A-share session expectations
- does not directly write Chan results

### Task B7: Add production tail collector
**Deliverable:** steady-state collector that fetches only after watermark and records dirty ranges.

**Acceptance:**
- does not pull all timeframes
- does not trigger full-history Chan recomputation

### Task B8: Add incremental Chan updater
**Deliverable:** updater that recomputes only minimal tail windows and atomically publishes a new head.

**Acceptance:**
- three levels update together
- confirmed and predictive records coexist without breaking ordering

### Task B9: Isolate bootstrap and prod runtime
**Deliverable:** separate compose/env/profile modes.

**Acceptance:**
- prod mode cannot accidentally launch full-history import or full-history Chan loops
- bootstrap mode can run to completion without serving as prod

## Workstream 3: Frontend Projection Agent

**Mission:** Keep the frontend on a single bundle contract and make all chart-timeframe rendering a projection over canonical 5f-bound Chan objects.

**Owned files/modules:**
- `apps/web/src/api/client.ts`
- `apps/web/src/api/chartDataManager.ts`
- `apps/web/src/api/marketData.ts`
- `apps/web/src/tradingview/datafeed.ts`
- `apps/web/src/tradingview/chanStudy.ts`
- `apps/web/src/tradingview/widget.ts`
- `apps/web/src/tradingview/time.ts`
- `apps/web/src/App.tsx`
- `apps/web/src/components/WatchlistPanel.tsx`
- `apps/web/src/tradingview/chanStudy.contract.test.ts`

**Must not edit:**
- backend schema or runtime compose files
- importer or bootstrap task tables

### Task F1: Freeze frontend canonical contract
**Deliverable:** one-page field contract for bundle consumption.

**Acceptance:**
- `chart_timeframe`, `base_timeframe`, `base_ts_semantics`, and time unit are explicit
- same-bar multi-signal representation is explicit

### Task F2: Fully close chart reads onto bundle
**Deliverable:** bundle is the only chart-data authority.

**Acceptance:**
- no split fetch path for bars vs overlay
- HTTP and WS shapes remain coherent

### Task F3: Split view timeframe from analysis timeframe
**Deliverable:** frontend logic treats displayed bars and fixed three-level Chan analysis as separate concerns.

**Acceptance:**
- timeframe switch changes bars and projection only
- fixed `5f/30f/1d` intelligence remains stable

### Task F4: Unify end-time plotting semantics
**Deliverable:** all study and fallback rendering use canonical end-time fields.

**Acceptance:**
- no duplicate time-mapping logic
- no legacy time-field drift on 30f/1d/1w/1m

### Task F5: Rebuild study raw cache on canonical 5f endpoints
**Deliverable:** view projection cache keyed from canonical 5f identities.

**Acceptance:**
- same snapshot projects deterministically across `5f/15f/30f/1h/1d/1w/1m`

### Task F6: Fix buy/sell point projection and visibility rules
**Deliverable:** support same-bar multiple signal variants and timeframe-based visibility.

**Acceptance:**
- `<1d` shows `1d + 30f + 5f`
- `>=1d` shows `1d + 30f`
- same-bar variants do not overwrite each other

### Task F7: Align realtime invalidation
**Deliverable:** 5f updates invalidate all affected cached views correctly.

**Acceptance:**
- switching timeframe does not leak stale overlay state
- realtime does not miss necessary redraws

### Task F8: Build projection regression matrix
**Deliverable:** tests and screenshots over the full timeframe set.

**Acceptance:**
- includes endpoint placement, center separation, signal visibility, and round-trip timeframe switching

## Workstream 4: Verification/Release Agent

**Mission:** Define and enforce the release gate from bootstrap completion to production cutover.

**Owned files/modules:**
- `services/api/tests/*`
- `services/collector/tests/*`
- `services/chan-service/tests/*`
- `docs/runbooks/*`
- dedicated verification scripts or SQL under a new verification folder if needed

**Must not edit:**
- core production logic unless commander reassigns

### Task V1: Freeze release gates
**Deliverable:** written gate list for bootstrap complete and prod cutover.

**Acceptance:**
- commander signoff
- gates reference measurable outputs, not vague “success”

### Task V2: Build coverage verification
**Deliverable:** queries/scripts proving canonical 5f loaded coverage.

**Acceptance:**
- actual `klines` data is checked
- not inferred from task success flags alone

### Task V3: Build gap verification
**Deliverable:** checks for missing or malformed 5f sequences.

**Acceptance:**
- exceptions are explicitly categorized, not silently ignored

### Task V4: Build Chan snapshot completeness verification
**Deliverable:** checks that `5f/30f/1d` snapshots are present and coherent per symbol.

**Acceptance:**
- bundle API returns precomputed data, not live fallback, for bootstrap-complete symbols

### Task V5: Build frontend regression checklist
**Deliverable:** screenshot matrix and manual checklist for all chart timeframes.

**Acceptance:**
- includes no center cross-connection, stable endpoint placement, and correct signal visibility rules

### Task V6: Build restart and recovery verification
**Deliverable:** interruption and restart test procedure for bootstrap and prod modes.

**Acceptance:**
- restart continues correctly
- prod restart does not trigger full-history loops

### Task V7: Build cutover rehearsal
**Deliverable:** procedure to switch from bootstrap-complete database to prod mode.

**Acceptance:**
- only tail collection and incremental Chan updates run post-cutover
- no hidden full-history workers remain active

## Integration Order

1. Data Bootstrap Agent runs D1-D6 first.
2. Backend Canonical/Runtime Agent runs B1-B4 in parallel after D1 is frozen.
3. Frontend Projection Agent starts only after B1 contract freeze and B4 draft payload are available.
4. Data Bootstrap Agent runs D7-D10 after backend accepts canonical 5f ingest contract.
5. Backend Canonical/Runtime Agent runs B5-B9 after D7 handoff manifest is available.
6. Verification/Release Agent starts V1-V4 as soon as D2 and B1 exist, then completes V5-V7 after frontend and runtime work land.

## Blockers Requiring Commander Decision

1. Universe scope:
   - import all `SH/SZ/BJ` from source
   - or restrict to a defined serving universe
2. Whether to keep stored higher-timeframe rows as transitional cache tables or retire them entirely from prod serving.
3. Whether parquet source gets a new `klines.source` code or is tracked only in separate bootstrap metadata tables.

## Minimum 95% Confidence Gate Before Execution

1. Historical parquet source coverage is verified and documented.
2. `bar_end` time semantics are frozen with no remaining ambiguity.
3. 5f-only canonical serving is accepted.
4. Published snapshot head and watermark semantics are accepted.
5. Bootstrap/prod isolation is accepted.
6. Frontend agrees to bundle-only reads and canonical projection.
7. Verification gates are accepted before any code execution starts.
