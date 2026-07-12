# Device B daily / 30f data-contract audit

**Scope.** Read-only audit of `F:\data` performed on 2026-07-11.  No database
rows were created and no source Parquet file was changed.  This document applies
to the new flat layout (`stock_daily.parquet` and `stock_30min/*.parquet`), not
to the retired zip layout consumed by `collector.native_parquet_import`.

## Decision

The dataset is suitable as the *candidate native source* for `1d` and `30f`,
subject to the ingest gates below.  It is **not** safe to use a blanket
`daily.vol * 100` transform without a per-symbol/day reconciliation audit:
the normal unit is hands (100 shares), but the sampled history contains rows
whose daily volume is already in shares.

| Contract item | Required ingestion behaviour | Evidence / decision |
| --- | --- | --- |
| 30f identity | one `ts_code`, one timezone-aware Shanghai bar-end timestamp; source `parquet_native` (9) | Files are one per symbol in `stock_30min`; `canonical_kline_timestamp("30f", ...)` already rejects non-session labels. |
| 30f session | exactly nine labels on a complete trading day: `09:30, 10:00, 10:30, 11:00, 11:30, 13:30, 14:00, 14:30, 15:00` | 000001.SZ (4,249 days), 000002.SZ (4,246), 600000.SH (4,249), and 688001.SH (1,685) each had only this pattern and nine rows per day.  `09:30` is the approved opening snapshot, not a regular half-hour close. |
| 30f disposition | non-nine-bar day is incomplete, not silently accepted as complete | Profile it; quarantine invalid timestamp/OHLC/negative volume rows.  A normal suspension must be represented by absent source bars / coverage evidence, never fabricated bars. |
| daily timestamp | normalize source `trade_date` (timestamp at midnight in the Parquet file) to `Asia/Shanghai 15:00` using `canonical_kline_timestamp("1d", ..., date_only=True)` | The protocol requires 15:00 labels for daily/weekly/monthly. |
| daily volume | persist shares in `klines.volume`; normally calculate `round(vol * 100)` | Cross-timeframe volume sums match this rule for 14,145 sampled symbol-days except the exceptions described below.  Do not transform intraday `vol`. |
| daily amount | retain source amount as a currency amount and persist with `amount_to_x100`; do not infer volume from amount | Both daily and 30f inputs have `amount` as `double`; its unit is independent evidence only. |
| 1w/1m derivation | derive only from accepted native daily rows after daily audit; use source 8 for output and retain 1d source 9 as truth | `aggregate_timeframes_from_daily` canonicalizes each daily timestamp to Shanghai 15:00, aggregates native-readable daily input, and excludes the current calendar week/month.  Its output timestamp is the last daily bar in the bucket. |
| 1w/1m completion gate | run derivation only after all admitted historical daily input is `is_complete=true`; do not publish partial source periods | The SQL uses `BOOL_AND(is_complete)`, but an operational gate is still required before deriving historic `confirmed` data.  Current-week/current-month are excluded by SQL. |

This is a bounded, reproducible cross-timeframe sample rather than a
population-wide acceptance result; the final full-profiler count remains an
ingestion gate.

## Reproducible observations

The daily file schema contains `open, high, low, close, vol, amount,
trade_date, ts_code` (plus fundamental fields), and `trade_date` is
`timestamp[ns]`.  The 30f files sampled have `ts_code, trade_date, trade_time,
open, high, low, close, vol, amount`.

For each sampled symbol, all 30f daily sums were compared to the matching
daily `vol` while scanning the daily Parquet in record batches:

| Symbol | matched dates | typical `sum(30f.vol) / (daily.vol * 100)` | missing daily dates in 30f span | material exceptions |
| --- | ---: | ---: | ---: | --- |
| 000001.SZ | 4,167 | 1.0 | 82 | 4 dates at 0.944–0.988 |
| 000002.SZ | 4,092 | 1.0 | 154 | 1 date at 0.989 |
| 600000.SH | 4,201 | 1.0 | 48 | 2 dates at 0.961/0.987; 7 dates at about 100 |
| 688001.SH | 1,685 | 1.0 | 0 | 7 dates at exactly 100 |

The seven 100x cases are shared across the two sampled symbols:
`2024-04-03`, `2024-04-19`, `2024-04-26`, `2024-04-30`, `2024-05-24`,
`2024-05-31`, `2024-06-14`.  They show that on those rows the daily `vol`
already has share units, whereas the rest has hand units.  The smaller
0.944–0.989 differences are source reconciliation discrepancies and should
also be recorded rather than rounded away.

## Required importer/audit gates

1. Read both sources in batches and retain raw source values in the audit
   record.  Never load the daily Parquet wholesale.
2. For each daily row with matching accepted 30f bars, calculate
   `ratio = sum(30f.volume) / (daily.vol * 100)`.  If it is within the chosen
   tolerance (recommended 1%), emit `daily.vol * 100`.  If
   `sum(30f.volume) / daily.vol` is within tolerance, emit raw `daily.vol`.
   Otherwise quarantine the daily row for source reconciliation.
3. If no matching 30f day exists, accept daily volume only under an explicit
   auditable default policy; mark the unit decision as inferred, not proven.
   The policy must be applied before 1w/1m derivation.
4. Require daily OHLC validity, non-negative volume/amount, symbol identity,
   and normalized 15:00 timestamps.  Require the nine-label 30f session set
   for a complete day; duplicate `(symbol, 30f, ts)` is a quarantine event.
5. Produce source-coverage and quarantine counts by symbol/timeframe before
   generating weekly/monthly bars or starting Module C.

## Current blocker

The checked-in `native_parquet_import.py` is designed for the old zip paths
`30m_price` / `日线数据/1d_price` and column names `code,date`.  `F:\data`
uses `stock_30min` / `stock_daily.parquet` and `ts_code,trade_date,trade_time`.
It cannot be pointed at the new root safely.  The new-layout importer must
apply the gates above; this audit intentionally does not modify that importer
or start an import.

## Full read-only profiler result (2026-07-11)

The profiler scanned every native 30-minute file in 20 static, non-overlapping
shards and the complete daily file in a separate bounded-memory process. It
only read `F:\data`; it made no database connection and wrote no source file.
Raw machine-readable evidence is retained at
`F:\tv-data\logs\parquet-profile-30m-shard-00-of-20.json` through
`...19-of-20.json`; the daily report is
`F:\tv-data\logs\parquet-profile-1d-full.json`.

| Scope | Files / rows | Result |
| --- | ---: | --- |
| 30f | 5,799 files; 133,591,639 rows; 2009-01-05 through 2026-07-03 | no file/schema, symbol, date, or duplicate-key errors |
| 30f SH/SZ | 5,474 files; 130,944,168 rows | 4,140 OHLC-invalid rows; no session, negative-volume, or negative-amount rows |
| 30f BJ | 325 files; 2,647,471 rows | 292,233 non-native session labels, 61 negative-volume rows, 59 negative-amount rows; 28,496 incomplete days / 113,984 missing expected labels under the nine-label contract |
| 1d | 14,380,024 rows; 5,799 symbols; 2009-01-05 through 2026-07-03 | full scan found no OHLC, negative-volume, or negative-amount rows |

The 20 30f workers used 2,945.762 CPU-seconds in aggregate; the slowest shard
took 153.823 seconds wall-clock. The daily full scan used a 262,144-row batch
and completed in 133.867 seconds. These timings and process metadata are in
the JSON reports.

### 30f BJ blocker

All non-session and negative volume/amount findings belong to the `.BJ`
files. The expected 30f source contract remains the nine labels listed above.
For example, `920030.BJ` contains `15:30` labels and an early sequence of
minute-level labels (`09:31` through `15:29`) inside its nominal 30f Parquet
file. The same condition affects 282 BJ files. It is therefore a source-layout
or session-contract exception, not a valid reason to relax the SH/SZ contract.

**Import decision:** the 30f import gate remains closed for BJ rows until a
symbol/date-qualified provider rule or a clean replacement source is approved.
The importer must quarantine the identified BJ bad rows and retain raw
provenance; it must not silently coerce `15:30` or minute-labelled data into
30-minute bars. SH/SZ still require the 4,140 OHLC rows to be quarantined.
The daily source can proceed only after the per-symbol/day volume-unit policy
is implemented.
