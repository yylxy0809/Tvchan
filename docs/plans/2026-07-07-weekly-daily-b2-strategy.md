# Weekly Daily B2 Strategy Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a module-C-only offline scan and backtest service for the weekly/daily resonance B2 strategy without changing chan.py or module C compute semantics.

**Architecture:** Add a standalone `services/strategy-service` that reads `chan_c_*`, `chan_c_runs`, `scheme2_chan_c_published_heads`, and `klines`, persists strategy events/backtests into new tables, and supports both static scan and replay-style backtest through CLIs. Keep all strategy semantics above the published Chan structure layer.

**Tech Stack:** Python 3.11, asyncpg, pytest, PostgreSQL/TimescaleDB, standard library csv/json/dataclasses.

---

### Task 1: Audit and contract documents

**Files:**
- Create: `strategy/project_audit.md`
- Create: `strategy/data_contract.md`
- Create: `strategy/schema_v1.sql`
- Create: `strategy/implementation_plan.md`

**Step 1: Write the documents from inspected code and database facts**

Include:
- API/module C source-of-truth findings
- `chan_c_signals` field semantics
- `signal_type` / `bsp_type`
- `ts` / `base_ts`
- market cap / MACD / fractal availability

**Step 2: Verify files exist**

Run:

```powershell
Get-ChildItem strategy
```

Expected: the four files are listed.

### Task 2: Add strategy schema migration

**Files:**
- Create: `db/sql/021_strategy_weekly_daily_b2.sql`

**Step 1: Write migration**

Create:
- `symbol_fundamentals`
- `strategy_definitions`
- `strategy_signal_events`
- `strategy_contexts`
- `strategy_backtest_runs`
- `strategy_backtest_trades`

**Step 2: Verify migration syntax**

Run:

```powershell
Get-Content -Raw db/sql/021_strategy_weekly_daily_b2.sql
```

Expected: SQL contains the six table definitions.

### Task 3: Scaffold strategy service

**Files:**
- Create: `services/strategy-service/requirements.txt`
- Create: `services/strategy-service/app/**`

**Step 1: Add minimal package layout**

Create:
- config
- domain
- repositories
- analyzers
- engine
- backtest
- cli

**Step 2: Verify importability**

Run:

```powershell
python -c "import sys; sys.path.insert(0, r'services/strategy-service'); import app"
```

Expected: exits 0.

### Task 4: Implement repositories

**Files:**
- Create: `services/strategy-service/app/repositories/module_c_repo.py`
- Create: `services/strategy-service/app/repositories/kline_repo.py`
- Create: `services/strategy-service/app/repositories/strategy_repo.py`

**Step 1: Write repository tests first**

Test:
- timeframe mapping
- signal normalization
- SQL selection behavior helpers

**Step 2: Implement minimal repository code**

Support:
- current published head lookup
- latest historical run by `as_of_time`
- signals/strokes/centers fetch
- K-line fetch and MACD helpers
- event/backtest inserts

### Task 5: Implement analyzers

**Files:**
- Create: `services/strategy-service/app/analyzers/*.py`

**Step 1: Write failing unit tests**

Cover:
- raw 3-bar fractals
- weekly context validity
- strength score composition
- entry confidence scoring
- exit priority

**Step 2: Implement minimal logic to satisfy tests**

### Task 6: Implement scan runner

**Files:**
- Create: `services/strategy-service/app/engine/strategy_runner.py`
- Create: `services/strategy-service/app/cli/run_scan.py`

**Step 1: Add a single-symbol scan path**

**Step 2: Expand to multi-symbol scan with optional limit**

**Step 3: Persist contexts/events**

### Task 7: Implement backtest runner

**Files:**
- Create: `services/strategy-service/app/backtest/replay_engine.py`
- Create: `services/strategy-service/app/backtest/trade_simulator.py`
- Create: `services/strategy-service/app/backtest/metrics.py`
- Create: `services/strategy-service/app/backtest/report_writer.py`
- Create: `services/strategy-service/app/cli/run_backtest.py`

**Step 1: Implement exploratory static mode**

**Step 2: Implement event replay mode using latest historical run `<= as_of_time`**

**Step 3: Persist run/trades and write `trades.csv`, `metrics.json`, `report.md`**

### Task 8: Verification

**Files:**
- Create: `services/strategy-service/tests/*.py`

**Step 1: Run unit tests**

```powershell
pytest services/strategy-service/tests -q
```

Expected: PASS

**Step 2: Run a CLI smoke scan**

```powershell
python -m app.cli.run_scan --strategy weekly_daily_b2_resonance_v1 --as-of 2026-01-01 --limit 5
```

Expected: exits 0 and writes scan output.

**Step 3: Run a CLI smoke backtest**

```powershell
python -m app.cli.run_backtest --strategy weekly_daily_b2_resonance_v1 --start 2025-01-01 --end 2025-03-01 --limit 3
```

Expected: exits 0 and writes report artifacts.
