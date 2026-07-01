# 2026-06-30 Cleanup And Modularization Inventory

## Scope

This document records the agreed cleanup and modularization baseline before any source deletion or architectural rewrite.

Current baseline commit:

```text
ff6b51d chore: establish cleanup baseline
```

Tracked file counts at the baseline:

```text
all tracked files: 210
apps/web/src: 33
services/api + services/chan-service + services/collector: 89
```

No business code has been changed in this stage.

## Confirmed Assumptions

1. Local Git baseline is allowed and has been created.
2. `旧版方案/` stays protected until golden fixtures fully replace it.
3. Frontend chart data should converge to the canonical chart bundle contract.
4. `work/vendor/chan.py-main` and `apps/web/public/charting_library` are runtime dependencies and must stay protected.
5. Cleanup must proceed by evidence, not by filename guesswork.
6. Modularization must be incremental; every module extraction must keep the app runnable.

## Protected Paths

These paths are not deletion candidates in the first cleanup pass:

| Path | Reason |
| --- | --- |
| `apps/web/public/charting_library/**` | TradingView Advanced Charts runtime. |
| `work/vendor/chan.py-main/**` | Current module B Chan engine source. |
| `旧版方案/**` | Golden behavior reference until fixtures are extracted. |
| `db/sql/**` | Database schema and migrations. |
| `deploy/**` | Docker and NAS deployment source. Secrets are ignored separately. |
| `docs/contracts/**` | API and chart bundle contracts. |
| `docs/runbooks/**` | Operational runbooks still used for deployment and verification. |
| `libs/protocol/**` | Cross-service protocol library. |
| `services/**/tests/**` | Regression and contract protection. |
| `scripts/verify-scheme2-local.ps1` | End-to-end verification script. |

## Active Frontend Surface

Static entry:

```text
apps/web/index.html
apps/web/public/app-config.js
apps/web/src/main.tsx
apps/web/src/App.tsx
```

Current active frontend modules:

| Area | Files |
| --- | --- |
| Runtime config | `src/config.ts`, `src/vite-env.d.ts` |
| Auth and tokens | `src/auth/api.ts`, `src/app/sessionPersistence.ts`, `src/api/client.ts` |
| Chart data | `src/api/chartDataManager.ts`, `src/api/marketContracts.ts`, `src/api/marketData.ts`, `src/api/realtime.ts` |
| Watchlist and panels | `src/api/watchlistStore.ts`, `src/components/RightSidebar.tsx`, `src/components/WatchlistPanel.tsx`, `src/components/AlertsPanel.tsx`, `src/components/StockNewsPanel.tsx`, `src/components/StrongestTodayPanel.tsx` |
| Screeners | `src/api/wencaiScreener.ts`, `src/api/chanScreener.ts`, `src/components/ScreenerDock.tsx` |
| TradingView integration | `src/components/ChartWorkspace.tsx`, `src/app/chartPreferences.ts`, `src/tradingview/widget.ts`, `src/tradingview/datafeed.ts`, `src/tradingview/chanStudy.ts`, `src/tradingview/chanStudySettings.ts`, `src/tradingview/chanStyles.ts`, `src/tradingview/overlaySettings.ts`, `src/tradingview/time.ts`, `src/tradingview/debug.ts` |
| Styling | `src/styles.css` |
| Tests | `src/tradingview/chanStudy.contract.test.ts` |

Frontend files needing evidence before deletion:

| Candidate | Current assessment | Required proof before removal |
| --- | --- | --- |
| None currently. | The first confirmed orphan set has been removed. | Re-run import/reference scans before adding more candidates. |

Frontend files removed after evidence:

| Removed file | Evidence | Verification |
| --- | --- | --- |
| `src/components/ChanOverlayControls.tsx` | `rg -n "ChanOverlayControls" apps/web/src` returned only the file definition. | `npm run build`; `npm run test:contract`. |
| `src/components/StatusPanel.tsx` | `rg -n "StatusPanel" apps/web/src` returned only the file definition. | `npm run build`; `npm run test:contract`. |
| `src/api/history.ts` | `rg -n "history" apps/web/src` showed no importer or UI use of this module. | `npm run build`; `npm run test:contract`. |

Frontend canonical bundle status:

| Gap | Evidence | Target |
| --- | --- | --- |
| Chart K-line reads are bundle-routed. | `src/api/client.ts:getBars` is now a compatibility wrapper over `getChartBundle`; no frontend source references `/api/v1/bars`. | Keep the compatibility wrapper only for non-chart callers that need bar-only shape. |
| Chart manager uses bundle request semantics. | `src/api/chartDataManager.ts` sends WS `get_chart_bundle`; static scan finds no `subscribe_chan`, `get_bars`, or `get_chan` in `apps/web/src`. | Keep frontend chart reads on `/api/v3/chart/bundle` or WS `get_chart_bundle`. |
| `marketData.ts` is aligned through the bundle adapter. | It calls `getBars`, which resolves through `getChartBundle`. | Supporting quote/profile data can stay separate; chart K-line/Chan data should not use legacy bars/chan endpoints. |

## Active Backend Surface

FastAPI entry:

```text
services/api/app/main.py
```

Registered API routes:

| Prefix | Router | Status |
| --- | --- | --- |
| `/api/v1/health` | `routes/health.py` | Active. |
| `/api/v1/auth` | `routes/auth.py` | Active login/token flow. |
| `/api/v1/admin/tokens` | `routes/admin.py` | Active admin token management. |
| `/api/v1/symbols` | `routes/symbols.py` | Active symbol search. |
| `/api/v1/bars` | `routes/bars.py` | Legacy/supporting route; not final chart contract. |
| `/api/v1/chan/overlay` | `routes/chan.py` | Legacy/supporting route; not final chart contract. |
| `/api/v1/screener/chan` | `routes/screener.py` | Active Chan screener. |
| `/api/v1/chart/window` | `routes/chart.py` | Legacy chart window route. |
| `/api/v2/chart/bundle` | `routes/chart.py` | Transitional chart bundle route. |
| `/api/v3/chart/bundle` | `routes/chart.py` | Target canonical chart bundle route. |
| `/api/v1/history` | `routes/history.py` | Historical export path; keep until usage is decided. |
| `/ws/v1/realtime` | `routes/realtime.py` | Realtime quote/bar path. |
| `/ws/v2/chart` | `routes/chart_ws.py` | Target chart websocket path, but handler still supports legacy message types. |

Backend active modules:

| Area | Files |
| --- | --- |
| Config/security/db | `core/config.py`, `core/security.py`, `db.py` |
| Models/contracts | `models.py` |
| Repositories | `repositories/postgres.py`, `repositories/bars.py`, `repositories/chan_postgres.py`, `repositories/chan_screener.py`, `repositories/tokens.py` |
| Services | `services/chan_client.py`, `services/llm_client.py` |
| History export | `history/exports.py`, `routes/history.py` |
| Chart bundle | `routes/chart.py`, `routes/chart_ws.py` |
| Realtime | `routes/realtime.py`, `realtime/__init__.py` |

Chan service active surface:

| Area | Files |
| --- | --- |
| Service entry | `services/chan-service/chan_service/main.py` |
| Analyzer | `services/chan-service/chan_service/analyzer.py` |
| Module B adapter | `services/chan-service/chan_service/vendor_chan_adapter.py` |
| Models | `services/chan-service/chan_service/models.py` |
| Regression tests | `services/chan-service/tests/**` |

Backend files needing evidence before deletion:

| Candidate | Current assessment | Required proof before removal |
| --- | --- | --- |
| `services/collector/collector/backfill.py` | Keep for now. It is still the `backfill` compatibility alias and pytdx probe hint target. | Remove only after a replacement pytdx probe entrypoint exists and old runbooks/tests are updated. |

Backend and shared files removed after evidence:

| Removed item | Evidence | Verification |
| --- | --- | --- |
| `services/chan-service/chan_service/adapter_template.py` | `rg -n "adapter_template" services docs` returned only the audit document and the file itself. | `pytest services/chan-service/tests -q`. |
| `apps/web/src/api/realtime.ts:createRealtimeSocket` | `rg -n "createRealtimeSocket" apps/web/src` returned only the exported function; chart data uses `createChartSocket`. | `npm run build`; `npm run test:contract`. |
| `services/chan-service/chan_service/legacy_engine.py` | `rg -n "legacy_engine|analyze_with_legacy_engine" services apps libs deploy scripts` returned no active importer; `CHAN_ENGINE_MODE=module_b` is enforced in local, Docker, and tests. | `pytest services/chan-service/tests -q`. |
| `work/vendor/chanlun.py-main/**`, `work/vendor/czsc-v0.9.69/**`, and their zip files | Static scan showed only historical docs/audit references; current runtime mounts only `work/vendor/chan.py-main`. | Vendor directory now contains only `chan.py-main` and `chan.zip`. |

## Generated Or Local-Only Cleanup Candidates

These are ignored or should remain untracked. They can be deleted locally after active processes are stopped.

| Path | Action |
| --- | --- |
| `apps/web/node_modules/**` | Regenerate with `npm install`; do not commit. |
| `apps/web/dist/**` | Regenerate with `npm run build`; do not commit. |
| `apps/web/tsconfig.tsbuildinfo` | Delete freely; generated by TypeScript. |
| `.pytest_cache/**`, `**/__pycache__/**` | Delete freely. |
| `logs/**`, `outputs/**`, `tmp/**`, `tmp-*` | Delete after confirming no running service is writing to them. |
| `work/logs/**`, `work/runtime-logs/**` | Delete after active runtime is stopped. |
| `work/deploy/tv-backend-nas-package/**` | Regenerate with packaging script; archive only if it is a known deployment artifact. |

## Required Fixes Before Any NAS Package

| Issue | Risk | Fix |
| --- | --- | --- |
| `scripts/apply-db-migrations.ps1` enumerates sorted `db/sql/*.sql` and auto-detects `tv_backend_timescaledb` / `tv_local_timescaledb`. | Local DB can be missing auth/canonical/runtime/screener tables if migrations are skipped. | Fixed; keep this script as the local migration entrypoint. |
| `/ws/v2/chart` still supports legacy `get_bars`, `get_chan`, `subscribe_chan`. | External or old test clients may still depend on compatibility message types. | Keep compatibility for now; frontend source is already bundle-only. |
| Frontend chart manager legacy message names have been removed. | `rg -n "/api/v1/bars|/api/v1/chan/overlay|/api/v1/chart/window|subscribe_chan|get_bars|get_chan|createRealtimeSocket" apps/web/src` returns no matches. | Maintain this scan as a regression check before NAS packaging. |
| POC vendors and legacy engine were removed. | Confusion during future maintenance is reduced; static scan currently shows no active imports. | Keep `work/vendor/chan.py-main` protected as the only active Chan vendor. |

## Modularization Target

The project should become module-routed, not file-sprawled.

Frontend target modules:

```text
app shell
auth
chart
chan overlay study
watchlist
alerts
news
strongest today
wencai screener
chan screener
settings/theme
```

Backend target modules:

```text
core config/security/db
auth/admin tokens
symbols
market profile/supporting data
chart bundle v3
chart websocket v2
chan engine registry
chan screener state
collector workers
history export
```

Registry targets:

| Layer | Registry |
| --- | --- |
| Frontend | `src/features/featureRegistry.ts` centralizes sidebar feature definitions and bottom screener tab metadata. Status: partial; right sidebar is registry-driven, bottom dock renders tab id, title, and icon from the registry. |
| API routes | `services/api/app/routes/registry.py` centralizes router registration. Status: complete. |
| Workers | `services/collector/collector/worker_registry.py` and `collector.worker` provide a compatible unified worker entry. Status: complete; Docker worker commands and `scripts/start-chan-recompute-worker.ps1` use registry aliases. |
| Chan engine | `services/chan-service/chan_service/engine_registry.py` selects module B as the default Chan engine. Status: complete. |

## Ponytail Use Decision

`DietrichGebert/ponytail` is an agent-rule/plugin project focused on reducing unnecessary code and enforcing a minimal-change ladder. It is useful as a review discipline for this cleanup, but it should not be installed or added as a runtime dependency without a separate safety review because plugin lifecycle hooks are involved.

For this repository, use it as a principle first:

1. Reuse existing module boundaries before inventing new ones.
2. Delete or quarantine only with import/runtime evidence.
3. Prefer one small registry per layer over broad rewrites.
4. Keep every refactor paired with a runnable verification.

Reference: <https://github.com/DietrichGebert/ponytail>

## Execution Gates

### Gate 0: Baseline

Status: complete.

Verification:

```powershell
git status --short
git rev-parse --short HEAD
```

Expected:

```text
clean worktree
ff6b51d
```

### Gate 1: Inventory

Status: this document.

Verification:

```powershell
git diff -- docs/audits/2026-06-30-cleanup-modularization-inventory.md
```

Expected:

```text
documentation only
```

### Gate 2: Low-Risk Consistency Fix

First implementation candidate:

```text
scripts/apply-db-migrations.ps1
```

Verification:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/apply-db-migrations.ps1 -ContainerName <db-container>
```

Expected:

```text
001 through 011 are applied in sorted order.
```

### Gate 3: Frontend Bundle-Only Chart Path

Target:

```text
TradingView datafeed + chartDataManager + marketData chart reads use chart-bundle.v3.
```

Verification:

```powershell
rg -n "/api/v1/bars|/api/v1/chan/overlay|/api/v1/chart/window|subscribe_chan|get_bars|get_chan" apps/web/src
npm run build
npm run test:contract
```

Expected:

```text
No direct frontend chart read through legacy routes.
Build passes.
Chan study contract passes.
```

### Gate 4: Backend Registry Introduction

Target:

```text
Router registration moved from app.main into a small route registry without changing endpoint paths.
```

Verification:

```powershell
pytest services/api/tests
Invoke-RestMethod http://127.0.0.1:8001/api/v1/health
Invoke-RestMethod "http://127.0.0.1:8001/api/v3/chart/bundle?symbol=000001.SZ&timeframe=5f&limit=300"
```

Expected:

```text
All existing routes still reachable.
No endpoint path changes.
```

### Gate 5: Candidate Quarantine

Target:

```text
Move unused candidates into a quarantine folder or remove only after tests and runtime checks.
```

Verification:

```powershell
npm run build
npm run test:contract
pytest services/api/tests services/chan-service/tests services/collector/tests
powershell -ExecutionPolicy Bypass -File scripts/verify-scheme2-local.ps1
```

Expected:

```text
No active runtime regression.
No missing imports.
No missing Docker build context.
```

## Next Recommended Step

Do not delete broader legacy backend paths yet.

Proceed with the next compatibility review:

```text
Review legacy API routes and decide whether `collector.backfill` should be replaced by a smaller pytdx probe command.
```

Remove only after the local runtime checklist passes and no dynamic import, Docker build context, runbook dependency, or external client compatibility requirement remains.

## 2026-07-01 Runtime Modularity Progress

Implemented low-risk runtime configuration and user preference foundations without changing the chart rendering contract.

Completed:

- Added runtime feature configuration storage and API:
  - `db/sql/012_runtime_config.sql`
  - `GET /api/v1/config/features`
  - `PUT /api/v1/admin/runtime-config/{key}`
- Wired frontend right-sidebar and bottom screener dock feature lists through runtime config with default fallback.
- Added user settings storage and API:
  - `db/sql/013_user_settings.sql`
  - `GET /api/v1/user/settings`
  - `PUT /api/v1/user/settings/{bucket}`
  - buckets: `theme`, `watchlist`, `layout`, `indicatorSettings`
- Split `LoginPage` and `AdminConsole` out of `App.tsx`.
- Wired frontend chart theme and watchlist groups to server-side user settings with local fallback.
- Wired TradingView layout and Chan indicator settings to server-side user settings with local fallback.
- Split `ChartWorkspace` out of `App.tsx`.
- Split session persistence and chart preference helpers into `src/app/sessionPersistence.ts` and `src/app/chartPreferences.ts`.
- Added admin runtime feature switch UI for right-sidebar and screener dock features.
- Added frontend runtime feature config change events so admin feature switches refresh active sidebar and screener modules without a page reload.
- Updated `scripts/apply-db-migrations.ps1` with `-Only` support so single migrations can be applied while long-running workers are active.

Verified:

```powershell
python -m pytest services/api/tests -q
# 63 passed

cd apps/web
npm run build
npm run test:contract
# build passed; 6 contract tests passed

powershell -NoProfile -ExecutionPolicy Bypass -File scripts/apply-db-migrations.ps1 -Only 013_user_settings.sql
# migration applied
```

Runtime notes:

- Local Docker API was rebuilt and restarted after adding user settings routes.
- `tv_backend_timescaledb`, `tv_backend_api`, `tv_backend_chan_service`, `tv_backend_web_gateway`, and `tv_backend_redis` were healthy.
- 10 local `chan-recompute` workers were running after reboot recovery.
- After the 2026-07-01 power recovery, 10 local `chan-recompute` workers were restarted. Last checked waterline: `success=4056`, `pending=1706`, `running=10`, `failed=5`.

Next safe steps:

1. Continue extracting chart data orchestration out of `ChartWorkspace` only if a separate regression check is added for pan/zoom and Chan overlay persistence.
2. Add a focused browser smoke test for persisted layout, theme, watchlist, runtime feature switches, and Chan indicator settings before NAS packaging.
