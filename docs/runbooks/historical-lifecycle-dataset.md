# Historical replay lifecycle dataset

Use this path only for a sealed historical replay contract. It is deliberately
separate from `run_lifecycle_datasets`, whose `observed_time <= as_of` fence is
the correct contract for an online observation-safe snapshot.

The historical exporter selects one exact replay batch and includes events by
causal `effective_time`. A later real `observed_time` is retained as audit
evidence and is not rewritten. The source is fenced by the replay batch ID,
contract SHA-256, sealed parent/child/source batches, completed task/run/head
identity, completed outbox, history provenance, and event provenance. It never
reads `chan_structure_lifecycle_current`.

```powershell
Set-Location services/strategy-service
$env:PYTHONPATH = (Get-Location).Path
python -m app.cli.run_historical_lifecycle_dataset `
  --replay-batch-id 9 `
  --expected-contract-hash <64-character-contract-sha256> `
  --effective-cutoff 2026-07-03T07:00:00Z `
  --output-dir D:\tv-backend\outputs\historical-lifecycle-batch-9
```

Install the Strategy service dependencies first and provide `DATABASE_URL` only
through the process environment. Do not put the URL or credentials in shell
history, command arguments, artifacts, or Git. Run from
`services/strategy-service`; the repository root is not a Python module root.
The output example is deliberately outside the worktree. If another path is
used, confirm it cannot be tracked with `git status --short` before exporting.

The cutoff must equal the immutable replay contract cutoff. The command uses a
read-only repeatable-read transaction and an asyncpg server cursor, writes
`official.jsonl.tmp` incrementally, fsyncs it, and publishes `manifest.json` as
the commit marker. A crash between data and manifest publication leaves a safe
orphan that the next run replaces; a published manifest is never overwritten.
A valid manifest requires zero future-effective, invalid-clock, cross-scope,
or task/run/head/history/outbox/event relationship violations. The relationship
audit is the sealed replay ledger authority; it intentionally does not depend
on the rebuildable lifecycle current projection. Keep generated datasets
outside Git. Each invocation writes to a unique sibling staging directory and
atomically promotes the complete dataset directory, so concurrent runs cannot
share temporary files and an interrupted run cannot publish a manifest.

Run the strict strategy gate from the same exact scope:

```powershell
python -m app.cli.run_official_historical_gate `
  --replay-batch-id 9 `
  --expected-contract-hash <64-character-contract-sha256> `
  --as-of 2026-07-03T07:00:00Z `
  --dataset-manifest D:\tv-backend\outputs\historical-lifecycle-batch-9\manifest.json `
  --output-dir D:\tv-backend\outputs\historical-gate-batch-9
```

Dataset validation and strategy approval are separate. A valid causal dataset
does not change a strict strategy `NO_GO` decision. The current gate is an
upstream visibility audit only: disappeared/reappeared state replay and
parent-bound 30f/5f confirmation traces are not implemented, so it explicitly
adds `official_event_replay_not_implemented` and cannot authorize a formal
backtest. The gate verifies the
dataset content hash and exact scope before producing artifacts. It deliberately
streams the exact database rows again inside the gate's read-only repeatable-read
snapshot and requires that canonical hash to equal the exported JSONL hash; this
full read is a correctness fence and must not be skipped for speed. Gate files
are built in a unique staging directory, include a final `gate-complete.json`
file/hash manifest, and are atomically promoted as one directory. Use a new or
empty output path; published output is never overwritten.

There is no dedicated Strategy Compose service. For reproducible container
execution, use the already built API image only as a one-shot runtime: mount
`services/strategy-service` read-only at `/workspace`, set
`PYTHONPATH=/workspace`, mount the private output directory at `/output`, join
the backend Docker network, and pass `DATABASE_URL` through the environment.
Never print or paste that value, and never mount the worktree as the output
directory. The host command above remains the simplest supported path.
