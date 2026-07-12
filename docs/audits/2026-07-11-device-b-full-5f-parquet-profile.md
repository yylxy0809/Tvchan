# Device B full 5f Parquet dry-run profile

Run date: 2026-07-11

## Scope and execution

- Source: `F:\data\stock_5min`.
- Method: 20 static, non-overlapping read-only shards over the sorted file list.
- Memory model: Parquet `iter_batches`, batch size 65,536; each shard retained only its per-file counters and bounded anomaly examples.
- Database writes: 0.
- Source files: 5,000, total 16.39 GB.

Raw shard evidence is retained at `F:\tv-data\logs\parquet-profile-5m-shard-00-of-20.json` through `...19-of-20.json`.

## Full-market results

| Measure | Result |
|---|---:|
| Profiled files | 5,000 / 5,000 |
| Profiled 5f rows | 687,823,927 |
| First bar-end label | 2009-01-05 09:30 Asia/Shanghai |
| Last bar-end label | 2026-07-03 15:00 Asia/Shanghai |
| File/schema errors | 0 |
| File-name / `ts_code` mismatches | 0 |
| `trade_date` / `trade_time` date mismatches | 0 |
| Duplicate logical keys | 0 |
| Invalid session labels | 0 |
| Negative-volume rows | 0 |
| OHLC-invalid rows | 4,310 |
| Files containing OHLC-invalid rows | 1,251 |
| Projected accepted rows | 687,819,617 |
| Projected quarantined rows | 4,310 |
| Projected rejected rows | 0 |

All 14,037,223 observed symbol-days contain the expected 49 five-minute bars, including the accepted 09:30 opening snapshot. `incomplete_session_days=0` and `missing_expected_bars=0` under this explicit bar-end contract.

## Coverage

- Active symbols in master: 5,534.
- Active symbols with a native 5f file: 4,738.
- Active symbols missing native 5f: 796.
- Inactive master symbols: 327; 259 still have a 5f file.

The missing active set remains an explicit import/recompute gate. It is represented by the offline manifest at `services/collector/outputs/missing-5f-manifest/missing_5f_manifest.csv`; its exact split is SH 472 and BJ 324. Native 5f must not be synthesized from 30f.

## Volume decision

The profiler's bounded cross-timeframe sample found intraday volume / (daily volume * 100) = 1.0 for all 41 matched symbol-days, supporting intraday volume in shares and daily volume normally in hundred-shares.

This is not sufficient for a global conversion rule: the independent daily/30f audit found seven global dates where daily volume already appears to be shares, plus a small number of non-exact ratios. The eventual daily importer must evaluate a symbol/day ratio gate and quarantine exceptions; it must not blindly multiply every daily `vol` by 100.

## Result and gate decision

The 5f local-source contract is accepted for file shape, symbol identity, bar-end session interpretation and bounded-memory processing. The authoritative import gate remains **closed**:

1. Quarantine the 4,310 OHLC-invalid rows with provenance rather than repairing/dropping them silently.
2. Establish the daily symbol/day volume normalization and exception policy.
3. Qualify the provider waterfall and preserve an unresolved exception contract for the 796 active symbols without native 5f.

No authoritative database import, published-head update, lifecycle publication or Module C full recompute has been started.
