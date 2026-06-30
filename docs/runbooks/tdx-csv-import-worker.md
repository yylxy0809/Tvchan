# TDX CSV Import Worker

This worker imports zipped CSV history from local TDX-style downloads such as:

```text
D:\BaiduNetdiskDownload\tdx数据
```

The current importer supports these folders:

- `五分钟K线数据` -> `5f`
- `十五分钟K线数据` -> `15f`
- `三十分钟K线数据` -> `30f`
- `六十分钟K线数据` -> `1h`

For Chan recomputation, the important input is `5f`. The `30f` and `1d` Chan
trend levels are recursively derived from stored `5f` bars, not from 30-minute
or daily period K-lines.

## Apply Schema

```powershell
powershell -ExecutionPolicy Bypass -File scripts\apply-db-migrations.ps1
```

This creates:

- `tdx_csv_import_tasks`
- `symbols(exchange, code)` as the symbol identity, so `000001.SH` and
  `000001.SZ` can coexist without polluting each other.

The task key is:

```text
zip_path + timeframe
```

Progress is tracked by `last_entry_index` and `last_entry_name`. If the import
stops midway, rerun with `-ResetRunning`; already completed CSV entries are
skipped and the current archive resumes from the next entry.

## Dry Run

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-tdx-csv-import-worker.ps1 `
  -Root 'D:\BaiduNetdiskDownload\tdx数据' `
  -Timeframes '5f' `
  -DryRun
```

## Safe Pilot

Import only `000001.SZ`, and only a few CSV entries from the first claimed zip.
The default import is intentionally conservative:

- `-Categories 1`
- `-AssetTypes stock`
- `-Fq 0`

This skips index files such as `000001.SH` and skips adjusted duplicate series
inside the same CSV file.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-tdx-csv-import-worker.ps1 `
  -Root 'D:\BaiduNetdiskDownload\tdx数据' `
  -Timeframes '5f' `
  -Symbols 000001.SZ `
  -AssetTypes stock `
  -Fq 0 `
  -TaskLimit 1 `
  -MaxEntriesPerTask 200 `
  -ResetRunning
```

## Batch Import

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-tdx-csv-import-worker.ps1 `
  -Root 'D:\BaiduNetdiskDownload\tdx数据' `
  -Timeframes '5f' `
  -AssetTypes stock `
  -Fq 0 `
  -TaskLimit 2 `
  -Concurrency 1 `
  -EntryBatchSize 50 `
  -BarBatchSize 50000 `
  -ResetRunning
```

Keep `-Concurrency 1` until the database write speed is known. Increase to `2`
only if disk and PostgreSQL remain comfortable.

## Continuous Import

For Docker/NAS deployment, the CSV worker can run as a long-lived loop:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-tdx-csv-import-worker.ps1 `
  -Root 'D:\BaiduNetdiskDownload\tdx数据' `
  -Timeframes '5f' `
  -AssetTypes stock `
  -Fq 0 `
  -TaskLimit 2 `
  -Concurrency 1 `
  -EntryBatchSize 50 `
  -BarBatchSize 50000 `
  -ResetRunning `
  -Loop `
  -LoopInterval 300
```

The task table stores zip-level and entry-level progress, so restarting the
worker resumes from the next unprocessed CSV entry.

## Then Recompute Chan

After importing 5-minute history:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-chan-recompute-worker.ps1 `
  -SymbolLimit 10 `
  -BaseTimeframe 5f `
  -ChanLevels '5f,30f,1d' `
  -TaskLimit 10 `
  -Concurrency 1 `
  -ResetRunning
```

## Inspect Progress

```powershell
docker exec tv_local_timescaledb psql `
  -U trader `
  -d tradingview_local `
  -c "select id, timeframe, status, entries_done, entries_total, bars_read, bars_written, last_entry_name, last_error from tdx_csv_import_tasks order by updated_at desc limit 20;"
```
