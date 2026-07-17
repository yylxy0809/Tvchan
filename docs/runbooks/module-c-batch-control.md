# Module C frozen batch control

Use `collector.module_c_batch_control` for every durable production canary or
baseline recompute. Never use `--symbols` or `--symbol-limit` with a non-dry
recompute worker; production scope comes only from the append-only eligibility
build bound to the batch.

## Preconditions

- stop all K-line writers and the Module C stream worker;
- complete the read-only canonical audit and strict five-level eligibility build;
- keep unresolved volume, BJ 30f and missing coverage as excluded dispositions;
- record the code commit, immutable image digest and vendor manifest SHA-256;
- run exactly one healthy lifecycle observer;
- use `shard_count=4` from the first manifest (start shards 0/1, then 2/3).

## Canary

Create a private selection JSON with `contract_version` set to
`module-c-canary-selection-v1` and exactly 20 canonical symbol names. Its
declared traits must cover `main_board`, `chinext`, `star`, `bj`,
`suspended_or_sparse`, `gap`, `price_limit` and `long_history`.

```powershell
python -m collector.worker module-c-batch-control freeze-canary `
  --database-url $env:DATABASE_URL `
  --source-build-id $fullEligibilityBuild `
  --manifest-version module-c-canary-20260717-v1 `
  --selection-manifest $selectionJson `
  --output-dir $ignoredEvidenceDir

python -m collector.worker module-c-batch-control prepare `
  --database-url $env:DATABASE_URL `
  --batch-kind canary --batch-key $batchKey --run-group-id $runGroup `
  --eligibility-build-id $canaryBuild --shard-count 4 `
  --code-commit $commit --image-digest $imageDigest `
  --vendor-manifest-sha256 $vendorHash

python -m collector.worker module-c-batch-control activate `
  --database-url $env:DATABASE_URL --batch-id $batchId
```

Run the recompute workers with both modes, five native levels, concurrency 1,
pool size 1 and the frozen batch/build identifiers. Drain outbox, run lifecycle
reconciliation and `collector.module_c_canary_ab`. Seal only with its exact
passing report:

```powershell
python -m collector.worker module-c-batch-control seal `
  --database-url $env:DATABASE_URL --batch-id $batchId `
  --canary-ab-report $canaryReport --sealed-by device-b
```

## Baseline

Prepare the baseline from the original full-market eligibility build and bind
the sealed canary. Code, image, vendor manifest and config must match.

```powershell
python -m collector.worker module-c-batch-control prepare `
  --database-url $env:DATABASE_URL `
  --batch-kind baseline --batch-key $batchKey --run-group-id $runGroup `
  --eligibility-build-id $fullEligibilityBuild `
  --approved-canary-batch-id $canaryBatchId --shard-count 4 `
  --code-commit $commit --image-digest $imageDigest `
  --vendor-manifest-sha256 $vendorHash
```

Activate, run shards 0/1 then 2/3, drain outbox, reconcile and seal. A resource
stop only stops workers; it does not abort, recreate or mutate the durable
manifest. Never reopen sealed/failed/aborted batches.
