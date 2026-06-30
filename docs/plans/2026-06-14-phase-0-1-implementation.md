# TradingView A Share Phase 0/1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the first runnable skeleton for Phase 0/1: FastAPI health/symbols/bars APIs, seedable K-line storage path, collector abstraction, React/Vite TradingView datafeed shell, and local dev deployment files.

**Architecture:** Use a Python-first modular backend with FastAPI for API/Datafeed endpoints, a collector package with a pytdx-ready provider interface, PostgreSQL/TimescaleDB SQL bootstrap scripts, and a React/Vite frontend. The first loop runs on seeded sample data when pytdx or PostgreSQL are unavailable, while keeping the code paths ready for the real database and provider.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic, asyncpg optional, pytest, React, Vite, TypeScript, TradingView Advanced Charts local assets, PostgreSQL + TimescaleDB, Redis.

---

### Task 1: Repository Skeleton

**Files:**
- Create: `README.md`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `deploy/docker-compose.dev.yml`

**Steps:**
1. Add top-level documentation with Phase 0/1 startup instructions.
2. Ignore generated environments, node modules, local TradingView licensed assets, and local database files.
3. Add environment variables for API, DB, Redis, token, and seed mode.
4. Add Docker Compose for TimescaleDB and Redis.

**Verification:**
- `Get-ChildItem` shows the expected top-level directories and files.

### Task 2: Shared Protocol Types

**Files:**
- Create: `libs/protocol/python/trading_protocol/__init__.py`
- Create: `libs/protocol/python/trading_protocol/timeframes.py`
- Create: `libs/protocol/python/trading_protocol/symbols.py`
- Create: `libs/protocol/python/trading_protocol/bars.py`

**Steps:**
1. Define canonical timeframe mapping for `5f`, `15f`, `30f`, `1h`, `1d`, `1w`, `1m`.
2. Define symbol and bar dataclasses/Pydantic-compatible models.
3. Keep types dependency-light so API and collector can both import them.

**Verification:**
- `python -m py_compile` succeeds for shared modules.

### Task 3: FastAPI API Service

**Files:**
- Create: `services/api/requirements.txt`
- Create: `services/api/pytest.ini`
- Create: `services/api/app/main.py`
- Create: `services/api/app/core/config.py`
- Create: `services/api/app/core/security.py`
- Create: `services/api/app/models.py`
- Create: `services/api/app/repositories/bars.py`
- Create: `services/api/app/routes/health.py`
- Create: `services/api/app/routes/symbols.py`
- Create: `services/api/app/routes/bars.py`
- Create: `services/api/tests/test_api.py`

**Steps:**
1. Implement settings and optional bearer token auth.
2. Implement `/api/v1/health`.
3. Implement `/api/v1/symbols` backed by seed data.
4. Implement `/api/v1/bars` backed by generated seed K-lines for Phase 1.
5. Add tests for health, auth, symbols, and bars.

**Verification:**
- `python -m pytest services/api/tests -q` passes when dependencies are installed.
- `uvicorn app.main:app --reload` starts from `services/api`.

### Task 4: Collector Package

**Files:**
- Create: `services/collector/requirements.txt`
- Create: `services/collector/collector/models.py`
- Create: `services/collector/collector/providers/base.py`
- Create: `services/collector/collector/providers/seed.py`
- Create: `services/collector/collector/providers/pytdx_provider.py`
- Create: `services/collector/collector/backfill.py`
- Create: `services/collector/tests/test_seed_provider.py`

**Steps:**
1. Define `MarketDataProvider`.
2. Implement deterministic `SeedProvider`.
3. Add `PytdxProvider` placeholder with import-time optional dependency handling.
4. Add a backfill CLI that prints JSON lines for now.

**Verification:**
- `python -m pytest services/collector/tests -q` passes when dependencies are installed.
- `python services/collector/collector/backfill.py --provider seed --symbols 000001.SZ --timeframes 5f,1d` emits bars.

### Task 5: Database Bootstrap

**Files:**
- Create: `db/sql/001_init.sql`
- Create: `scripts/measure_storage.ps1`

**Steps:**
1. Add `symbols` and `klines` schema.
2. Add Timescale hypertable creation guarded for extension availability.
3. Add seed symbol inserts for 10 sample stocks.
4. Add a storage measurement helper script.

**Verification:**
- SQL is syntactically readable and documented for `psql -f db/sql/001_init.sql`.

### Task 6: Frontend Vite App

**Files:**
- Create: `apps/web/package.json`
- Create: `apps/web/index.html`
- Create: `apps/web/tsconfig.json`
- Create: `apps/web/vite.config.ts`
- Create: `apps/web/src/main.tsx`
- Create: `apps/web/src/App.tsx`
- Create: `apps/web/src/styles.css`
- Create: `apps/web/src/api/client.ts`
- Create: `apps/web/src/tradingview/datafeed.ts`
- Create: `apps/web/src/tradingview/widget.ts`
- Create: `apps/web/src/components/StatusPanel.tsx`

**Steps:**
1. Build a restrained, operational terminal UI.
2. Load TradingView Advanced Charts if local `charting_library` assets exist.
3. Fall back to a simple table/chart placeholder when licensed assets are not copied yet.
4. Implement symbols and bars API calls with token support.
5. Implement Datafeed shell for `onReady`, `searchSymbols`, `resolveSymbol`, and `getBars`.

**Verification:**
- `npm install` then `npm run build` succeeds when Node dependencies are available.

### Task 7: Phase 0/1 Verification Docs

**Files:**
- Create: `docs/runbooks/phase-0-1-local-dev.md`

**Steps:**
1. Document local DB/Redis startup.
2. Document API startup.
3. Document frontend startup.
4. Document how to copy TradingView licensed assets locally.
5. Document Phase 1 acceptance checks.

**Verification:**
- Commands are explicit and paths are Windows-friendly.
