# TradingView A Share Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add PostgreSQL/TimescaleDB persistence for symbols and K-lines, extend collector backfill to seven timeframes, and provide the first storage measurement path for Phase 2.

**Architecture:** Keep seed mode as the default fallback, but add a DB repository that activates when `USE_SEED_DATA=false`. Collector writes normalized bars into PostgreSQL through a small storage adapter, while API reads from the same schema. This preserves the Phase 1 frontend/API contract and lets Phase 2 swap data source without UI changes.

**Tech Stack:** Python 3.11+, FastAPI, asyncpg optional runtime dependency, PostgreSQL + TimescaleDB, Redis, pytest, React/Vite.

---

### Task 1: API Database Repository

**Files:**
- Modify: `services/api/requirements.txt`
- Create: `services/api/app/db.py`
- Create: `services/api/app/repositories/postgres.py`
- Modify: `services/api/app/main.py`
- Modify: `services/api/app/routes/symbols.py`
- Modify: `services/api/app/routes/bars.py`
- Test: `services/api/tests/test_api.py`

**Steps:**
1. Add optional `asyncpg` dependency.
2. Add FastAPI lifespan that creates a pool only when seed mode is disabled.
3. Add DB repository methods for symbols and bars.
4. Keep seed mode testable without a database.
5. Add tests for timeframe normalization and seed fallback remaining intact.

**Verification:**
- `python -m pytest services/api/tests -q` passes.

### Task 2: Collector DB Writer

**Files:**
- Modify: `services/collector/requirements.txt`
- Create: `services/collector/collector/storage/postgres.py`
- Modify: `services/collector/collector/backfill.py`
- Test: `services/collector/tests/test_seed_provider.py`

**Steps:**
1. Add optional DB writer adapter.
2. Add `--write-db` and `--database-url`.
3. Default backfill timeframes to all seven Phase 2 periods.
4. Upsert seed symbols before bars.
5. Print write summary.

**Verification:**
- Collector tests pass.
- Backfill CLI emits bars without DB.
- Backfill CLI can write to DB when Docker database is running.

### Task 3: Database and Storage Docs

**Files:**
- Modify: `db/sql/001_init.sql`
- Modify: `scripts/measure_storage.ps1`
- Modify: `docs/runbooks/phase-0-1-local-dev.md`
- Create: `docs/runbooks/phase-2-database-backfill.md`

**Steps:**
1. Add explicit timeframe comments.
2. Add compression notes and safe storage measurement commands.
3. Document Docker DB startup, seed backfill into DB, and API DB mode.

**Verification:**
- SQL remains compatible with TimescaleDB image.
- Docs contain exact Windows commands.

### Task 4: Verify

**Steps:**
1. Run Python tests.
2. Run frontend build.
3. If Docker is available, start TimescaleDB/Redis.
4. Install `asyncpg` if missing.
5. Run collector seed backfill into DB.
6. Run API with `USE_SEED_DATA=false` and query symbols/bars.

**Verification:**
- Tests pass.
- Frontend build passes.
- DB smoke check passes or blocker is documented.
