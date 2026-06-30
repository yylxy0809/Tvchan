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
| Auth and tokens | `src/auth/api.ts`, `src/api/client.ts` |
| Chart data | `src/api/chartDataManager.ts`, `src/api/marketContracts.ts`, `src/api/marketData.ts`, `src/api/realtime.ts` |
| Watchlist and panels | `src/api/watchlistStore.ts`, `src/components/RightSidebar.tsx`, `src/components/WatchlistPanel.tsx`, `src/components/AlertsPanel.tsx`, `src/components/StockNewsPanel.tsx`, `src/components/StrongestTodayPanel.tsx` |
| Screeners | `src/api/wencaiScreener.ts`, `src/api/chanScreener.ts`, `src/components/ScreenerDock.tsx` |
| TradingView integration | `src/tradingview/widget.ts`, `src/tradingview/datafeed.ts`, `src/tradingview/chanStudy.ts`, `src/tradingview/chanStudySettings.ts`, `src/tradingview/chanStyles.ts`, `src/tradingview/overlaySettings.ts`, `src/tradingview/time.ts`, `src/tradingview/debug.ts` |
| Styling | `src/styles.css` |
| Tests | `src/tradingview/chanStudy.contract.test.ts` |

Frontend files needing evidence before deletion:

| Candidate | Current assessment | Required proof before removal |
| --- | --- | --- |
| `src/components/ChanOverlayControls.tsx` | Legacy left-panel control candidate. | No import/render path after UI cleanup, and chart setting controls still work. |
| `src/components/StatusPanel.tsx` | Legacy diagnostic panel candidate. | No production import/render path. |
| `src/api/history.ts` | Historical export candidate, likely not part of current chart UX. | No current UI or runbook path relies on it. |

Frontend canonical bundle gaps:

| Gap | Evidence | Target |
| --- | --- | --- |
| Direct `/api/v1/bars` client still exists. | `src/api/client.ts` defines `fetchBars`. | Chart K-line reads should use `/api/v3/chart/bundle` or WS `get_chart_bundle`. |
| Chart manager still emits `subscribe_chan`. | `src/api/chartDataManager.ts` contains `subscribe_chan` and `unsubscribe_chan`. | Replace with bundle/window subscription semantics. |
| `marketData.ts` must stay aligned with canonical bundle path. | It exists as a separate data module. | Supporting quote/profile data can stay separate; chart K-line/Chan data should not use legacy bars/chan endpoints. |

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
| `services/chan-service/chan_service/legacy_engine.py` | Module A legacy candidate. | `CHAN_ENGINE_MODE=module_b` is enforced in local, Docker, and tests; no fallback route uses this as accepted output. |
| `services/chan-service/chan_service/adapter_template.py` | Integration template candidate. | Docs no longer reference it as user-facing guidance. |
| `services/collector/collector/backfill.py` | Older backfill candidate. | Current workers use `history_backfill`, `parquet_bootstrap_import`, `pytdx_5f_spool`, or `tdx_csv_import`. |
| `work/vendor/chanlun.py-main/**` | Old module A/C POC vendor candidate. | Confirm no dynamic imports and no docs require it. |
| `work/vendor/czsc-v0.9.69/**` | Module C POC vendor candidate. | Confirm no dynamic imports and no docs require it. |

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
| `scripts/apply-db-migrations.ps1` only applies `001` through `007`, while schema files now include `008` through `011`. | Local DB can be missing auth/canonical/runtime/screener tables. | Update script to enumerate sorted `db/sql/*.sql` or explicitly include all migrations. |
| `/ws/v2/chart` still supports legacy `get_bars`, `get_chan`, `subscribe_chan`. | Frontend can accidentally keep old contracts alive. | Keep compatibility for now, but make frontend chart reads bundle-only before deleting old paths. |
| Frontend chart manager still contains legacy Chan subscription names. | Drag/zoom behavior can regress to mixed data paths. | Migrate to canonical bundle/window cache semantics. |
| POC vendors and legacy engine still exist. | Confusion during future maintenance. | Quarantine after module B regression fixtures pass. |

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
| Frontend | `src/features/featureRegistry.ts` for sidebar/bottom-dock panels and feature lifecycle. |
| API routes | `services/api/app/routes/registry.py` to centralize router registration. |
| Workers | `services/collector/collector/worker_registry.py` for runnable worker entries. |
| Chan engine | `services/chan-service/chan_service/engine_registry.py` with module B as default. |

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

Do not delete source files yet.

Proceed with Gate 2 first because it is isolated, low risk, and fixes a real deployment consistency issue:

```text
Update scripts/apply-db-migrations.ps1 to apply all sorted db/sql/*.sql files.
```

After Gate 2 passes, proceed to frontend bundle-only migration before any frontend source cleanup.
