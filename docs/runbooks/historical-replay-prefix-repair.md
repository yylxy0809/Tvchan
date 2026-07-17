# Historical replay predecessor-prefix repair

Use this runbook only for a reviewed, manifest-declared defect where a
historical replay head history/outbox was published without its actual prior
run. This is not a general replay or data-editing tool.

The tool never changes K-lines, runs, replay heads/tasks, or parent/child batch
status. It archives the exact history/outbox/event before image, repairs the
declared predecessor fields, deletes only events derived from those histories,
and resets only their completed outboxes. The canonical lifecycle observer then
rebuilds the events from the corrected prefix.

## Preconditions

- Merge and apply migration `042_historical_replay_prefix_repair.sql` twice;
  the second application must be a no-op success.
- Obtain the replay batch contract hash from the durable child batch.
- Stop the canonical lifecycle observer. `apply`, `verify`, and `rollback`
  refuse to run if its session advisory lock is held.
- Keep strategy/event consumers in a maintenance window between `apply` and
  successful `verify`; the two outboxes are deliberately pending during that
  interval.
- Do not unseal/reopen either batch and do not start a replay worker.
- Take the normal database backup/snapshot required by the production change
  process. The repair ledger is a targeted rollback source, not a replacement
  for a database backup.

## Manifest contract

The manifest contains no credentials and must not contain production event or
K-line rows. Timestamps must be the exact UTC values returned by the database,
not calendar-date approximations.

```json
{
  "contract_version": "historical-replay-prefix-repair-v1",
  "repair_id": "00000000-0000-0000-0000-000000000000",
  "batch_id": 9,
  "replay_contract_hash": "<64 lowercase hex>",
  "entries": [
    {
      "history_id": 0,
      "outbox_id": 0,
      "new_run_id": 0,
      "new_cutoff": "2026-04-29T07:00:00Z",
      "current_old_run_id": null,
      "current_old_cutoff": null,
      "predecessor_history_id": 0,
      "target_old_run_id": 0,
      "target_old_cutoff": "2026-04-28T07:00:00Z"
    }
  ]
}
```

Contract v1 requires exactly two entries, and both declared current
`old_run_id`/`old_cutoff` values must be null. This keeps the exceptional tool
bound to the reviewed two-row defect rather than turning it into a general
history editor.

Compute the digest over canonical JSON (sorted keys, compact separators,
ASCII encoding). One safe way, using the reviewed code itself, is:

```powershell
$env:PYTHONPATH = "services/collector"
python -c "import json,sys; from collector.historical_replay_prefix_repair import canonical_sha256; print(canonical_sha256(json.load(open(sys.argv[1],encoding='utf-8'))))" repair-manifest.json
```

Every action requires both the external manifest SHA-256 and replay contract
hash. A changed byte, changed database contract, extra anomaly, missing anomaly,
or row drift fails closed.

## Plan (read-only)

```powershell
$env:PYTHONPATH = "services/collector"
python -m collector.historical_replay_prefix_repair plan `
  --database-url $env:DATABASE_URL `
  --manifest repair-manifest.json `
  --manifest-sha256 <manifest-sha256> `
  --expected-contract-hash <replay-contract-hash>
```

Review the before/target event counts and hashes. The before identity hash
includes database event IDs and `created_at`, freezing the exact rows that a
rollback would restore. `ready=true` proves the full batch predecessor anomaly
set exactly equals the manifest; it does not write.

## Apply

```powershell
python -m collector.historical_replay_prefix_repair apply `
  --database-url $env:DATABASE_URL `
  --manifest repair-manifest.json `
  --manifest-sha256 <manifest-sha256> `
  --expected-contract-hash <replay-contract-hash> `
  --expected-before-event-identity-sha256 <plan-before-event-identity-sha256> `
  --expected-target-event-sha256 <plan-target-event-set-sha256> `
  --actor <operator-or-change-id>
```

Copy the before identity hash and target event-set hash directly from the
reviewed `plan` output. `apply` rejects any changed before-row identity or target
set. It acquires the lifecycle session lock, opens a SERIALIZABLE transaction,
and locks parent, child, history, outbox, then event rows in fixed ID order. It
inserts durable before snapshots before any CAS mutation. Every changed/deleted
row count must be exact or the whole transaction rolls back.

After apply, confirm exactly the declared outboxes are `pending`. Start one
canonical lifecycle observer with the normal image/commit/config and wait for
both to become `completed`. Do not run a second observer name.

## Verify

```powershell
python -m collector.historical_replay_prefix_repair verify `
  --database-url $env:DATABASE_URL `
  --manifest repair-manifest.json `
  --manifest-sha256 <manifest-sha256> `
  --expected-contract-hash <replay-contract-hash> `
  --actor <operator-or-change-id>
```

Verify requires:

- corrected history and payload fields exactly match the snapshots' target;
- every declared outbox is completed;
- the actual lifecycle event set exactly matches a fresh canonical
  `LifecycleObserver` plan, including provenance and causal timestamps;
- the sealed historical finalizer dry-run is ready.

Verification accepts a contract-matching child that is still `running` or was
already atomically `sealed` after observer replay. This closes the observer-to-
verify race without reopening or changing sealed evidence. Apply and rollback
still require a running child.

Then run the normal lifecycle reconciliation and regenerate the frozen
official/observable/diagnostic datasets and strict gate. The strategy decision
must be derived from the regenerated ledger; never rewrite `NO_GO` manually.
Recheck the canonical K-line fingerprint before closing the change.

## Rollback

Rollback accepts an audited `applied` or `verified` repair while the child batch
is still running. It safely handles the exact observer states `pending`,
`processing`, `failed`, `dead_letter`, and `completed`, including one completed
row plus one unfinished row. Target metadata/payload must remain exact; every
completed row must have its complete canonical target events, while unfinished
rows must have no committed events. Any other partial or drifted state is
rejected. A sealed child is always rejected because its evidence is immutable.

```powershell
python -m collector.historical_replay_prefix_repair rollback `
  --database-url $env:DATABASE_URL `
  --manifest repair-manifest.json `
  --manifest-sha256 <manifest-sha256> `
  --expected-contract-hash <replay-contract-hash> `
  --actor <operator-or-change-id>
```

It restores the original history/outbox fields and original lifecycle event IDs
from the durable snapshots. Structure identity rows are retained because they
may be shared; unreferenced identities do not enter lifecycle datasets. A
second rollback is idempotent only when the restored state still exactly
matches the snapshots; otherwise it fails closed.

After rollback, rerun lifecycle reconciliation, historical audit/finalizer
dry-run, downstream dataset generation, and the K-line fingerprint check.
