# K-line import finalisation and deadlock recovery

The local Parquet importer owns one deterministic `import_run_id` per static
shard.  The run stays `running` while it is resumable.  It is marked
`completed` only when `parameters.tasks` exactly equals the durable checkpoint
count and every checkpoint is completed.  A fully reconciled set containing a
failed checkpoint becomes `failed`.  This tool does not scan or alter
`klines`, and it never deletes quarantine evidence.

After workers have stopped, inspect resumable shards:

```powershell
docker compose -f deploy/docker-compose.backend.yml exec collector python -m collector.kline_import_finalization --database-url $env:DATABASE_URL --unfinished
```

Resume each printed shard with its printed `import_run_id`, static shard index,
and original shard count.  Do not create a replacement ID for a resumable
shard:

```powershell
docker compose -f deploy/docker-compose.backend.yml run --rm collector python -m collector.local_parquet_import --active-only --timeframes 5f,30f,1d --shard-index 8 --shard-count 16 --import-run-id <printed-run-id>
```

The importer attempts finalisation after its complete task loop.  It can also
be checked manually:

```powershell
docker compose -f deploy/docker-compose.backend.yml exec collector python -m collector.kline_import_finalization --database-url $env:DATABASE_URL --import-run-id <run-id> --finalize
```

Only when a replacement set is already registered may stale deadlock runs be
retired.  The following metadata-only command is deliberately explicit: it
requires both the old static shard range and every replacement run ID.  It
does **not** remove already committed rows, checkpoints, or quarantines.

```powershell
docker compose -f deploy/docker-compose.backend.yml exec collector python -m collector.kline_import_finalization --database-url $env:DATABASE_URL --supersede-shards 8-15 --replacement-run-id <replacement-run-8> --replacement-run-id <replacement-run-9>
```

Use the final command only after confirming the replacement IDs correspond to
the same source scope and static-shard plan.  A failed status in this case
means "superseded after deadlock", not that committed K-line data is invalid.
