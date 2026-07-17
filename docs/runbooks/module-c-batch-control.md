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

Generate the private selection JSON from the explicit strict-v2 eligibility
build. The selector is read-only and binds its output to that build's canonical
audit, authoritative freshness contract and scope catalog provenance:

```powershell
python -m collector.worker module-c-canary-selection `
  --database-url $env:DATABASE_URL `
  --source-build-id $fullEligibilityBuild `
  --output $selectionJson
```

`module-c-canary-selection-v2` fixes exactly five symbols from each of main
board, ChiNext, STAR and Beijing Exchange. Within each board it selects two
lower-boundary, one median and two upper-boundary samples using the pinned
audit's `5f rows / (1d rows * 49)` activity-coverage ratio; 49 includes the
canonical 09:30 opening snapshot. This ratio is an
auditable sparse/dense coverage proxy, not a claim about traded-value
liquidity. Missing candidates, incomplete five-level evidence or provenance
drift fail closed. Stable ratio, `symbol_id`, and canonical-symbol ordering plus
canonical JSON hashing make repeated selection byte-reproducible.

V2 deliberately does not promote the legacy v1 free-text `gap`, `price_limit`,
`suspended_or_sparse`, or history traits to authoritative evidence. The frozen
strict-v2 inputs do not contain machine-verifiable gap/limit events, and
accepting arbitrary text would defeat deterministic reproduction. Those
scenario checks remain separate regression evidence until a future append-only
trait artifact binds them to the same audit/checkpoint/freshness/catalog hashes;
the v2 manifest does not claim that coverage. This supersedes v1's untyped
trait gate for newly planned canaries.

Legacy `module-c-canary-selection-v1` manifests remain readable for historical
audit compatibility. Use v2 for every newly planned production canary.

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
