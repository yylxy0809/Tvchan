# DuckDB Kline Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert the historical A-share K-line data in `J:\stock_data.db` from DuckDB into this project's PostgreSQL/TimescaleDB schema so the backend can serve real historical bars without re-importing CSV files or re-fetching old bars from pytdx.

**Architecture:** Use DuckDB as a read-only source, PostgreSQL/TimescaleDB as the canonical target, and a resumable bulk migration script that writes `symbols` first and `klines` second. The first migration phase intentionally excludes Chan analysis data; `J:\chan_data.db` is documented as a later phase because the old Chan database has no segment data and may not be fully synchronized with the K-line database.

**Tech Stack:** DuckDB, Python 3.11+, psycopg 3 `COPY`, PostgreSQL 16, TimescaleDB, Docker Compose, PowerShell or Bash.

---

## 1. Scope And Non-Goals

### In Scope

- Read historical K-line data from `J:\stock_data.db`.
- Read stock metadata from `J:\stock_data.db`.
- Write or update target PostgreSQL/TimescaleDB tables:
  - `symbols`
  - `klines`
- Preserve the original epoch timestamps from DuckDB.
- Support interruption and resume.
- Support smoke migration for one or a few symbols before full migration.
- Provide validation SQL and API checks.

### Out Of Scope For This Phase

- Do not migrate `J:\chan_data.db` yet.
- Do not recompute Chan analysis in this task.
- Do not change frontend code.
- Do not change NAS deployment files unless the migration machine is explicitly deploying the backend.
- Do not introduce a new `klines.source` enum unless the API code is updated too.

Reason for excluding Chan data in this phase:

- `J:\chan_data.db` contains `strokes`, `pivots`, and `buy_sell_points`, but all inspected `segments` counts are `0`.
- The current project has separate target tables for `chan_strokes`, `chan_segments`, `chan_centers`, and `chan_signals`.
- The user's requirement is that 30f/day Chan levels are recursively produced from 5f, not independently calculated from 30f/day K-lines. The old DuckDB structure alone does not prove that lineage.
- K-line migration is the largest and lowest-risk data asset. Finish it first.

---

## 2. Source Database Facts

Source files inspected on the original development machine:

| File | Approx Size | Purpose |
|---|---:|---|
| `J:\stock_data.db` | `93.7 GiB` | A-share K-lines, stock info, industry info, some Chan summary tables |
| `J:\chan_data.db` | `2.3 GiB` | Dedicated Chan analysis results |

### 2.1 DuckDB `stock_data.db` Tables

Inspected user tables:

| Table | Estimated Rows | Role |
|---|---:|---|
| `kline_data` | `268,646,872` | Historical K-lines |
| `stock_info` | `5,667` | Symbol metadata |
| `stock_industry` | `5,529` | Industry metadata |
| `sync_progress` | `397,050` | Old sync checkpoints |
| `chan_analysis` | `91,789` | Old Chan summaries; not migrated in phase 1 |
| `chan_signals` | `3,288,766` | Old Chan signal rows; not migrated in phase 1 |
| `chan_calc_progress` | `10,606` | Old Chan progress |
| `industry_hot` | `86` | Industry hot list |

### 2.2 Source `kline_data` Schema

DuckDB table:

```sql
kline_data (
  code VARCHAR,
  timestamp BIGINT,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  amount DOUBLE,
  frequency VARCHAR
)
```

Observed source code examples:

- `sh.000001`
- `sz.000001`
- `sh.600000`
- `sz.300750`

### 2.3 Source K-Line Row Counts

| Source Frequency | Target Timeframe | Rows | Symbols | First UTC | Last UTC |
|---|---|---:|---:|---|---|
| `5` | `5f` | `48,386,096` | `4,993` | `2025-06-20 01:35:00` | `2026-06-10 07:00:00` |
| `15` | `15f` | `129,947,966` | `5,446` | `2019-01-02 01:45:00` | `2026-06-10 07:00:00` |
| `30` | `30f` | `19,659,496` | `4,993` | `2024-03-19 02:00:00` | `2026-06-10 07:00:00` |
| `60` | `1h` | `32,265,556` | `5,447` | `2019-01-02 02:30:00` | `2026-06-10 07:00:00` |
| `1D` | `1d` | `33,473,455` | `5,678` | `1990-12-19 00:00:00` | `2026-06-10 15:00:00` |
| `1W` | `1w` | `3,965,050` | `5,678` | `1990-12-21 00:00:00` | `2026-06-05 15:00:00` |
| `1M` | `1m` month | `949,253` | `5,678` | `1990-12-31 00:00:00` | `2026-06-05 15:00:00` |

Total rows: `268,646,872`.

Important:

- `1m` in this project means monthly, not 1-minute.
- Do not shift timestamps by 8 hours. The source `timestamp` values are epoch seconds. Store the same instant in PostgreSQL as `timestamptz` with `to_timestamp(epoch_seconds)`.

---

## 3. Target Project Database Architecture

The target database is PostgreSQL with TimescaleDB.

Main migration SQL files:

- `db/sql/001_init.sql`
- `db/sql/006_symbol_identity.sql`
- `db/sql/007_kline_source_priority.sql`

Docker service:

- Compose file: `deploy/docker-compose.backend.yml`
- DB container: `tv_backend_timescaledb`
- DB service name inside Compose: `timescaledb`
- Default DB: `tradingview_local`
- Default user: `trader`

### 3.1 Target `symbols` Table

Defined in `db/sql/001_init.sql`:

```sql
create table if not exists symbols (
    id integer generated always as identity primary key,
    code varchar(16) not null,
    exchange varchar(8) not null,
    name varchar(64) not null,
    asset_type varchar(16) not null default 'stock',
    market varchar(16) not null default 'A_SHARE',
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists uq_symbols_exchange_code
on symbols (exchange, code);
```

Target symbol format used by the API:

- Stored columns:
  - `code='000001'`
  - `exchange='SZ'`
- API symbol string:
  - `000001.SZ`

The API resolves symbols in `services/api/app/repositories/postgres.py`.

### 3.2 Target `klines` Table

Defined in `db/sql/001_init.sql`:

```sql
create table if not exists klines (
    symbol_id integer not null references symbols(id),
    timeframe integer not null,
    ts timestamptz not null,
    open_x1000 integer not null,
    high_x1000 integer not null,
    low_x1000 integer not null,
    close_x1000 integer not null,
    volume bigint not null default 0,
    amount_x100 bigint,
    is_complete boolean not null default true,
    revision integer not null default 0,
    source smallint not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol_id, timeframe, ts)
);

select create_hypertable('klines', 'ts', if_not_exists => true);

create index if not exists idx_klines_symbol_timeframe_ts
on klines (symbol_id, timeframe, ts desc);

create index if not exists idx_klines_symbol_timeframe_source_ts
on klines (symbol_id, timeframe, source, ts desc);
```

### 3.3 Target Timeframe Codes

Defined in `libs/protocol/python/trading_protocol/timeframes.py`:

| Project Code | DB Integer | TradingView Resolution | Meaning |
|---|---:|---|---|
| `5f` | `5` | `5` | 5 minutes |
| `15f` | `15` | `15` | 15 minutes |
| `30f` | `30` | `30` | 30 minutes |
| `1h` | `60` | `60` | 1 hour |
| `1d` | `1440` | `D` | 1 day |
| `1w` | `10080` | `W` | 1 week |
| `1m` | `43200` | `M` | 1 month |

### 3.4 Current Source Codes

Current code comments define:

| Source Code | Meaning |
|---:|---|
| `1` | deterministic seed sample |
| `2` | pytdx real quote server |
| `3` | local TDX CSV |
| `0` | unknown |

For this migration, use `source = 3`.

Reason:

- This is local historical data, similar to local CSV history.
- The API already treats sources `2` and `3` as real data for seed suppression.
- Using a new `source = 4` would require code and schema-comment changes. Do not do that in this migration unless explicitly requested.

Relevant API condition in `services/api/app/repositories/postgres.py`:

```sql
and (
    k.source <> 1
    or not exists (
        select 1
        from klines k_real
        where k_real.symbol_id = k.symbol_id
          and k_real.timeframe = k.timeframe
          and k_real.source in (2, 3)
    )
)
```

---

## 4. Capacity Estimate

The DuckDB file is columnar and compressed. PostgreSQL/TimescaleDB is row-oriented and has btree indexes, so the target size is not the same as the DuckDB file size.

Estimated target size for K-lines only:

| Mode | Estimated Size |
|---|---:|
| Current schema, uncompressed TimescaleDB | `75-95 GiB` |
| Current schema, high safety estimate | `80-100 GiB` |
| With Timescale historical compression later | `35-60 GiB` |

Recommended free disk before full migration:

- Minimum: `150 GiB`
- Safer: `200 GiB+`
- If source DB, Docker volume, WAL, temp files, and old project are on the same disk: `250 GiB+`

Reason for extra space:

- PostgreSQL WAL writes during bulk insert.
- Index maintenance.
- Temporary staging tables.
- Docker volume overhead.
- Failed or interrupted runs may leave partial data until cleaned.

---

## 5. Migration Architecture

```mermaid
flowchart LR
  A["DuckDB stock_data.db<br/>read-only"] --> B["Python migration script<br/>batch by frequency + symbol"]
  B --> C["PostgreSQL temp staging table<br/>per batch"]
  C --> D["symbols<br/>upsert metadata"]
  C --> E["klines hypertable<br/>upsert historical bars"]
  E --> F["API /api/v1/bars<br/>TradingView datafeed"]
  E --> G["future Chan recompute<br/>from 5f bars"]
```

### Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Source access | DuckDB read-only | Avoid corrupting old `.db` files |
| Target writes | PostgreSQL `COPY` into temp table, then `INSERT ... ON CONFLICT` | Fast and resumable |
| Migration unit | `(frequency, code)` | Small transactions, easy resume |
| Progress tracking | PostgreSQL migration progress table | Survives process restart |
| Timestamp handling | Preserve epoch seconds using `to_timestamp()` | Avoid timezone drift |
| Data source marker | `source=3` | Compatible with existing API seed suppression |
| Index handling | Keep primary key; optionally drop secondary indexes during full load | Balance resume safety and speed |

---

## 6. Field Mapping

### 6.1 Symbol Mapping

Source symbol pattern:

```text
sh.600000
sz.000001
```

Target:

| Source | Target `symbols.code` | Target `symbols.exchange` | API Symbol |
|---|---|---|---|
| `sh.600000` | `600000` | `SH` | `600000.SH` |
| `sz.000001` | `000001` | `SZ` | `000001.SZ` |

Symbol names:

1. Prefer `stock_info.name`.
2. If missing, use `stock_industry.name`.
3. If still missing, use the six-digit code itself.

Asset type:

1. Prefer `stock_info.type`.
2. If missing, default to `stock`.

Market:

- Always use `A_SHARE`.

### 6.2 K-Line Mapping

| DuckDB `kline_data` | PostgreSQL `klines` | Transform |
|---|---|---|
| `code` | `symbol_id` | Resolve via `symbols(exchange, code)` |
| `frequency` | `timeframe` | See frequency map below |
| `timestamp` | `ts` | `to_timestamp(timestamp)` |
| `open` | `open_x1000` | `round(open * 1000)` |
| `high` | `high_x1000` | `round(high * 1000)` |
| `low` | `low_x1000` | `round(low * 1000)` |
| `close` | `close_x1000` | `round(close * 1000)` |
| `volume` | `volume` | `round(volume)`, null to `0` |
| `amount` | `amount_x100` | `round(amount * 100)`, null stays null |
| n/a | `is_complete` | `true` |
| n/a | `revision` | `0` |
| n/a | `source` | `3` |

Frequency map:

| DuckDB Frequency | Target Code | Target DB Integer |
|---|---|---:|
| `5` | `5f` | `5` |
| `15` | `15f` | `15` |
| `30` | `30f` | `30` |
| `60` | `1h` | `60` |
| `1D` | `1d` | `1440` |
| `1W` | `1w` | `10080` |
| `1M` | `1m` | `43200` |

---

## 7. Required Preflight Checks

Run these checks on the migration machine before writing any target data.

### 7.1 Verify Python Dependencies

Install in a local virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install duckdb "psycopg[binary]" tqdm
```

Verify:

```powershell
python - <<'PY'
import duckdb
import psycopg
print("duckdb", duckdb.__version__)
print("psycopg", psycopg.__version__)
PY
```

Expected:

- Both imports succeed.

### 7.2 Verify DuckDB Can Be Opened Read-Only

```powershell
python - <<'PY'
import duckdb
path = r"J:\stock_data.db"
con = duckdb.connect(path, read_only=True)
print(con.execute("select count(*) from kline_data").fetchone()[0])
con.close()
PY
```

Expected:

```text
268646872
```

### 7.3 Verify Target PostgreSQL Is Ready

For local Docker:

```powershell
docker ps --filter "name=tv_backend_timescaledb"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select version();"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "select extname from pg_extension where extname='timescaledb';"
```

Expected:

- Container is running.
- PostgreSQL responds.
- `timescaledb` extension exists.

### 7.4 Verify Target Tables Exist

```powershell
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "\dt public.*"
docker exec tv_backend_timescaledb psql -U trader -d tradingview_local -c "\d+ public.klines"
```

Expected:

- `symbols` exists.
- `klines` exists.
- `klines` is a Timescale hypertable.

### 7.5 Stop Competing Writers During Migration

During bulk migration, stop all workers that write K-lines:

```powershell
docker stop tv_backend_market_fill_worker tv_backend_history_backfill_worker tv_backend_tdx_csv_import_worker tv_backend_chan_recompute_worker
```

Ignore "No such container" errors if those profiles are not running.

Rationale:

- Avoid pytdx/CSV workers writing the same `klines` table while the migration is writing.
- Avoid extra Chan recompute jobs while K-line history is incomplete.

---

## 8. Proposed Migration Script

Create a script on the migration machine:

```text
tools/migrations/duckdb_kline_migrate.py
```

If this repository does not already have `tools/migrations`, create it.

### 8.1 Required CLI Options

The script should support:

```text
--duckdb-path J:\stock_data.db
--database-url postgresql://trader:password@127.0.0.1:15432/tradingview_local
--frequencies 5,15,30,60,1D,1W,1M
--symbols 000001.SZ,600519.SH
--limit-symbols 10
--batch-size 50000
--workers 1
--resume
--dry-run
--drop-secondary-indexes
--recreate-secondary-indexes
```

Rules:

- `--dry-run` must not write target data.
- `--symbols` is optional and used for smoke tests.
- `--limit-symbols` is optional and used for staged testing.
- `--resume` should skip completed `(frequency, old_code)` tasks.
- Start with `--workers 1`. Increase only after validation.

### 8.2 Migration Progress Table

Create this table in the target PostgreSQL database:

```sql
create table if not exists duckdb_kline_migration_progress (
    source_db text not null,
    old_frequency text not null,
    old_code text not null,
    target_symbol text not null,
    status text not null,
    row_count bigint not null default 0,
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    primary key (source_db, old_frequency, old_code)
);
```

Recommended statuses:

- `pending`
- `running`
- `success`
- `failed`

Resume behavior:

- If `--resume` is set, skip `success` tasks.
- Reset stale `running` tasks to `pending` on startup, or retry them directly.

### 8.3 Temporary Staging Table

Use a PostgreSQL temporary table per connection:

```sql
create temp table if not exists tmp_duckdb_klines (
    symbol_id integer not null,
    timeframe integer not null,
    ts_epoch bigint not null,
    open_x1000 integer not null,
    high_x1000 integer not null,
    low_x1000 integer not null,
    close_x1000 integer not null,
    volume bigint not null,
    amount_x100 bigint,
    is_complete boolean not null,
    revision integer not null,
    source smallint not null
) on commit preserve rows;
```

For each batch:

```sql
truncate tmp_duckdb_klines;
```

Then `COPY` rows into the temp table.

Then upsert into `klines`:

```sql
insert into klines (
    symbol_id,
    timeframe,
    ts,
    open_x1000,
    high_x1000,
    low_x1000,
    close_x1000,
    volume,
    amount_x100,
    is_complete,
    revision,
    source
)
select
    symbol_id,
    timeframe,
    to_timestamp(ts_epoch),
    open_x1000,
    high_x1000,
    low_x1000,
    close_x1000,
    volume,
    amount_x100,
    is_complete,
    revision,
    source
from tmp_duckdb_klines
on conflict (symbol_id, timeframe, ts) do update
set open_x1000 = excluded.open_x1000,
    high_x1000 = excluded.high_x1000,
    low_x1000 = excluded.low_x1000,
    close_x1000 = excluded.close_x1000,
    volume = excluded.volume,
    amount_x100 = excluded.amount_x100,
    is_complete = excluded.is_complete,
    revision = excluded.revision,
    source = excluded.source,
    updated_at = now();
```

### 8.4 Symbol Upsert SQL

Build the symbol set from the union of:

- `stock_info`
- `stock_industry`
- distinct `kline_data.code`

Why union with `kline_data`:

- K-line data may contain symbols or indexes missing from `stock_info`.
- The migration must not drop K-lines only because metadata is incomplete.

Target upsert:

```sql
insert into symbols (code, exchange, name, asset_type, market, is_active)
values (%s, %s, %s, %s, 'A_SHARE', true)
on conflict (exchange, code) do update
set name = excluded.name,
    asset_type = excluded.asset_type,
    market = excluded.market,
    is_active = excluded.is_active,
    updated_at = now();
```

### 8.5 Query One Migration Unit From DuckDB

One task is one `(frequency, old_code)` pair.

Example DuckDB query:

```sql
select
    timestamp as ts_epoch,
    cast(round(open * 1000) as integer) as open_x1000,
    cast(round(high * 1000) as integer) as high_x1000,
    cast(round(low * 1000) as integer) as low_x1000,
    cast(round(close * 1000) as integer) as close_x1000,
    cast(coalesce(round(volume), 0) as bigint) as volume,
    case
        when amount is null then null
        else cast(round(amount * 100) as bigint)
    end as amount_x100
from kline_data
where frequency = ?
  and code = ?
order by timestamp asc;
```

The Python script supplies:

- `symbol_id` from PostgreSQL `symbols`.
- `timeframe` from the mapping table.
- `is_complete = true`.
- `revision = 0`.
- `source = 3`.

Do not read all rows into memory. Use batches.

---

## 9. Implementation Tasks

### Task 1: Add Migration Script Skeleton

**Files:**

- Create: `tools/migrations/duckdb_kline_migrate.py`

**Step 1: Implement argument parsing**

Required arguments:

- `--duckdb-path`
- `--database-url`

Optional arguments:

- `--frequencies`
- `--symbols`
- `--limit-symbols`
- `--batch-size`
- `--workers`
- `--resume`
- `--dry-run`
- `--drop-secondary-indexes`
- `--recreate-secondary-indexes`

**Step 2: Add connection checks**

- Open DuckDB with `read_only=True`.
- Open PostgreSQL with psycopg.
- Verify `symbols` and `klines` exist.

**Step 3: Run dry-run**

```powershell
python tools/migrations/duckdb_kline_migrate.py `
  --duckdb-path J:\stock_data.db `
  --database-url postgresql://trader:password@127.0.0.1:15432/tradingview_local `
  --dry-run
```

Expected:

- Prints source table counts.
- Prints target DB connection success.
- Writes nothing.

### Task 2: Implement Symbol Migration

**Files:**

- Modify: `tools/migrations/duckdb_kline_migrate.py`

**Step 1: Read source symbols**

Read from DuckDB:

```sql
select code, symbol, name, type
from stock_info;
```

Also read:

```sql
select code, name, industry
from stock_industry;
```

Also read:

```sql
select distinct code
from kline_data;
```

**Step 2: Normalize old code**

Rules:

```text
old code: sh.600000 -> code=600000 exchange=SH
old code: sz.000001 -> code=000001 exchange=SZ
```

Ignore malformed codes by default, but log them.

**Step 3: Upsert symbols**

Use batched `executemany`.

**Step 4: Validate**

```sql
select exchange, count(*)
from symbols
group by exchange
order by exchange;
```

Expected:

- `SH` and `SZ` rows exist.
- Count is at least the distinct symbols needed by `kline_data`.

### Task 3: Implement Migration Progress Tracking

**Files:**

- Modify: `tools/migrations/duckdb_kline_migrate.py`

**Step 1: Create progress table**

Use the DDL in section 8.2.

**Step 2: Generate task list**

From DuckDB:

```sql
select frequency, code, count(*) as row_count
from kline_data
group by frequency, code
order by frequency, code;
```

Apply filters:

- `--frequencies`
- `--symbols`
- `--limit-symbols`

**Step 3: Respect resume mode**

If `--resume`, skip rows with `status='success'`.

**Step 4: Validate**

Run:

```powershell
python tools/migrations/duckdb_kline_migrate.py `
  --duckdb-path J:\stock_data.db `
  --database-url postgresql://trader:password@127.0.0.1:15432/tradingview_local `
  --symbols 000001.SZ `
  --dry-run
```

Expected:

- Shows only tasks related to `sz.000001`.

### Task 4: Implement One-Symbol K-Line Migration

**Files:**

- Modify: `tools/migrations/duckdb_kline_migrate.py`

**Step 1: Create temp staging table**

Use the DDL in section 8.3.

**Step 2: Copy DuckDB batch into staging**

Use psycopg 3 `COPY`.

Pseudo-code:

```python
with pg_conn.cursor() as cur:
    cur.execute("truncate tmp_duckdb_klines")
    with cur.copy(
        "copy tmp_duckdb_klines "
        "(symbol_id,timeframe,ts_epoch,open_x1000,high_x1000,low_x1000,close_x1000,volume,amount_x100,is_complete,revision,source) "
        "from stdin"
    ) as copy:
        for row in rows:
            copy.write_row(row)
```

**Step 3: Upsert into `klines`**

Use the SQL in section 8.3.

**Step 4: Commit per task**

Each `(frequency, old_code)` task should be one transaction.

**Step 5: Smoke migrate `000001.SZ`**

```powershell
python tools/migrations/duckdb_kline_migrate.py `
  --duckdb-path J:\stock_data.db `
  --database-url postgresql://trader:password@127.0.0.1:15432/tradingview_local `
  --symbols 000001.SZ `
  --frequencies 5,15,30,60,1D,1W,1M `
  --resume
```

Expected:

- Migration completes.
- Progress table has `success` rows for `sz.000001`.

### Task 5: Validate Smoke Migration

**Files:**

- No source code changes required unless validation fails.

**Step 1: Compare counts**

DuckDB:

```sql
select frequency, count(*), min(timestamp), max(timestamp)
from kline_data
where code = 'sz.000001'
group by frequency
order by frequency;
```

PostgreSQL:

```sql
select
  k.timeframe,
  count(*),
  extract(epoch from min(k.ts))::bigint as min_ts,
  extract(epoch from max(k.ts))::bigint as max_ts
from klines k
join symbols s on s.id = k.symbol_id
where s.code = '000001' and s.exchange = 'SZ'
group by k.timeframe
order by k.timeframe;
```

Expected:

- Counts match for each migrated frequency.
- Min/max epoch timestamps match.

**Step 2: Compare price samples**

DuckDB:

```sql
select *
from kline_data
where code = 'sz.000001' and frequency = '5'
order by timestamp desc
limit 5;
```

PostgreSQL:

```sql
select
  extract(epoch from k.ts)::bigint as ts,
  k.open_x1000 / 1000.0 as open,
  k.high_x1000 / 1000.0 as high,
  k.low_x1000 / 1000.0 as low,
  k.close_x1000 / 1000.0 as close,
  k.volume,
  k.amount_x100 / 100.0 as amount
from klines k
join symbols s on s.id = k.symbol_id
where s.code = '000001'
  and s.exchange = 'SZ'
  and k.timeframe = 5
order by k.ts desc
limit 5;
```

Expected:

- Timestamps match.
- Prices match within `0.001`.
- Amount matches within `0.01`.

**Step 3: Validate API**

Use the backend API after it is running:

```powershell
curl.exe -H "Authorization: Bearer <API_TOKEN>" "http://127.0.0.1:8001/api/v1/symbols?keyword=000001&limit=20"
curl.exe -H "Authorization: Bearer <API_TOKEN>" "http://127.0.0.1:8001/api/v1/bars?symbol=000001.SZ&timeframe=5f&limit=20"
curl.exe -H "Authorization: Bearer <API_TOKEN>" "http://127.0.0.1:8001/api/v1/chart/window?symbol=000001.SZ&timeframe=5f&limit=300"
```

Expected:

- Symbol lookup returns `000001.SZ`.
- Bars endpoint returns real historical bars.
- Chart window returns bars. Chan may still be generated/fallback unless Chan data is recomputed later.

### Task 6: Full Migration Preparation

**Files:**

- No source code changes required.

**Step 1: Ensure enough disk space**

Required:

- At least `150 GiB` free.
- Prefer `200 GiB+`.

**Step 2: Stop competing writer containers**

```powershell
docker stop tv_backend_market_fill_worker tv_backend_history_backfill_worker tv_backend_tdx_csv_import_worker tv_backend_chan_recompute_worker
```

**Step 3: Optional drop secondary indexes**

For a fresh or controlled full import, drop only secondary indexes:

```sql
drop index if exists idx_klines_symbol_timeframe_ts;
drop index if exists idx_klines_symbol_timeframe_source_ts;
```

Do not drop the primary key unless you are doing a non-resumable one-shot load into a guaranteed empty database.

### Task 7: Full Migration

**Files:**

- No source code changes required.

**Step 1: Run with one worker first**

```powershell
python tools/migrations/duckdb_kline_migrate.py `
  --duckdb-path J:\stock_data.db `
  --database-url postgresql://trader:password@127.0.0.1:15432/tradingview_local `
  --frequencies 5,15,30,60,1D,1W,1M `
  --batch-size 50000 `
  --workers 1 `
  --resume
```

**Step 2: If stable, increase workers carefully**

Try:

```powershell
--workers 2
```

Only increase beyond `2` if:

- Disk I/O is not saturated.
- PostgreSQL CPU is not saturated.
- WAL growth is controlled.
- No lock or timeout errors appear.

Recommended maximum for this dataset: `2-4` workers.

### Task 8: Recreate Secondary Indexes

If secondary indexes were dropped:

```sql
create index if not exists idx_klines_symbol_timeframe_ts
on klines (symbol_id, timeframe, ts desc);

create index if not exists idx_klines_symbol_timeframe_source_ts
on klines (symbol_id, timeframe, source, ts desc);
```

Then run:

```sql
analyze symbols;
analyze klines;
```

### Task 9: Full Validation

**Step 1: Compare counts by timeframe**

DuckDB expected counts:

| Frequency | Expected Rows |
|---|---:|
| `5` | `48,386,096` |
| `15` | `129,947,966` |
| `30` | `19,659,496` |
| `60` | `32,265,556` |
| `1D` | `33,473,455` |
| `1W` | `3,965,050` |
| `1M` | `949,253` |

PostgreSQL:

```sql
select
  case timeframe
    when 5 then '5'
    when 15 then '15'
    when 30 then '30'
    when 60 then '60'
    when 1440 then '1D'
    when 10080 then '1W'
    when 43200 then '1M'
    else timeframe::text
  end as frequency,
  count(*) as rows,
  extract(epoch from min(ts))::bigint as min_ts,
  extract(epoch from max(ts))::bigint as max_ts
from klines
where source = 3
group by timeframe
order by min(timeframe);
```

Expected:

- Counts match imported source rows.
- Min/max timestamps match source rows.

**Step 2: Measure storage**

PowerShell helper exists:

```powershell
.\scripts\measure_storage.ps1 -DatabaseUrl "postgresql://trader:password@127.0.0.1:15432/tradingview_local"
```

Or direct SQL:

```sql
select
  pg_size_pretty(hypertable_size('klines')) as klines_total_size;

select
  relname as relation,
  pg_size_pretty(pg_total_relation_size(relid)) as total_size,
  pg_size_pretty(pg_relation_size(relid)) as table_size,
  pg_size_pretty(pg_indexes_size(relid)) as index_size
from pg_catalog.pg_statio_user_tables
where schemaname = 'public'
order by pg_total_relation_size(relid) desc;
```

**Step 3: Validate API performance**

Run several representative requests:

```powershell
curl.exe -H "Authorization: Bearer <API_TOKEN>" "http://127.0.0.1:8001/api/v1/bars?symbol=000001.SZ&timeframe=5f&limit=5000"
curl.exe -H "Authorization: Bearer <API_TOKEN>" "http://127.0.0.1:8001/api/v1/bars?symbol=600519.SH&timeframe=1d&limit=5000"
curl.exe -H "Authorization: Bearer <API_TOKEN>" "http://127.0.0.1:8001/api/v1/chart/window?symbol=000001.SZ&timeframe=1d&limit=300"
```

Expected:

- HTTP 200.
- Bars are non-empty.
- Response times are acceptable for local LAN usage.

---

## 10. Failure Handling

### Problem: Migration Stops Midway

Action:

- Re-run with `--resume`.
- Script should skip `success` tasks.
- Script should retry `failed` or stale `running` tasks.

### Problem: Duplicate Key Errors

Cause:

- Direct insert without upsert.
- Existing data in `klines`.

Action:

- Use temp staging + `ON CONFLICT`.
- Do not direct `COPY` into `klines` unless target table is empty.

### Problem: Disk Fills Up

Action:

- Stop migration immediately.
- Check Docker volume size.
- Check PostgreSQL WAL growth.
- Consider dropping secondary indexes before resuming.
- Do not delete the DuckDB source.

### Problem: API Returns Seed Data Instead Of Migrated Data

Check:

```sql
select distinct source from klines order by source;
```

Expected:

- Migrated rows use `source = 3`.

If migrated rows accidentally used `source = 4`, the API will still return them because `k.source <> 1`, but seed suppression logic is less explicit. Prefer fixing source to `3`.

### Problem: Timestamps Appear 8 Hours Off

Cause:

- Manual timezone adjustment was applied.

Action:

- Do not add or subtract 8 hours.
- Store `to_timestamp(source_epoch_seconds)` directly.
- API returns epoch seconds; frontend handles display.

---

## 11. Optional Timescale Compression Later

Do not enable compression until after:

- Full K-line migration completes.
- Counts validate.
- API queries validate.
- Frontend loads expected charts.

After validation, consider adding a separate migration to enable Timescale compression for older chunks.

Candidate direction:

```sql
alter table klines set (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol_id,timeframe',
  timescaledb.compress_orderby = 'ts desc'
);
```

Then add a compression policy only for historical chunks.

This should be handled as a separate task because compression affects write/update behavior and operational maintenance.

---

## 12. Handoff Summary For The AI IDE

Implement only the historical K-line migration first.

The minimum successful result is:

1. `symbols` is populated from `stock_data.db`.
2. `klines` contains all selected source K-lines with correct timeframe, timestamp, prices, volume, amount, and `source=3`.
3. Migration can resume after interruption.
4. One-symbol smoke migration validates exactly against DuckDB.
5. Full migration counts match the DuckDB source counts.
6. Backend API returns migrated bars for `000001.SZ` and other sampled symbols.

Do not migrate `chan_data.db` in this phase.

Do not change frontend code.

Do not change backend API behavior unless validation proves the imported K-lines cannot be served correctly.

