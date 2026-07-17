# K-line Scope Catalog Bootstrap

The scope catalog is optional metadata used to avoid expensive probes for empty
symbol/timeframe pairs. Canonical `klines` remain authoritative. Until one
generation is complete and active, readers must use the existing correct but
slower fallback.

This is a one-shot maintenance command. It is registered with the collector
CLI, but intentionally has no Docker Compose service, realtime profile, loop,
or automatic restart.

## Safety gates

- Run only after migrations `040_kline_scope_catalog.sql` and
  `041_kline_scope_catalog_generation_fencing.sql` have been reviewed and
  applied with `ON_ERROR_STOP=1`.
- Apply migrations 040 and 041 before starting any collector code from the catalog-aware
  release. Every canonical K-line writer maintains catalog metadata in the same
  transaction, so starting new writers against an older schema must fail rather
  than silently diverge. The production Compose dependency on `db-migrate`
  enforces this order; direct host workers require the operator to enforce it.
- Set `DATABASE_URL` privately; never write a populated URL into Git or logs.
- Take a canonical K-line count/watermark snapshot before and after. The
  bootstrap reads `klines` and writes only the three scope-catalog tables.
- Keep the generation inactive while any scope is `unknown` or incomplete.
- Use bounded batches during a low-I/O window. Interruption is safe: rerun the
  same generation and only its remaining incomplete rows are scanned (both
  `unknown` rows and writer-touched `present` rows with incomplete bounds).

Before applying migration 041, verify that no legacy generation is still
`building`. Migration 041 fails explicitly in that state and never chooses,
fails, completes, or otherwise rewrites a legacy generation for the operator.
Finish or explicitly fail it with the migration-040 tooling before retrying.

```sql
select generation_id, created_at
from kline_scope_catalog_generations
where status = 'building';
```

Apply migrations 040 and 041 when the preceding migration chain is already present:

```powershell
.\scripts\apply-db-migrations.ps1 `
  -ContainerName tv_backend_timescaledb `
  -Only 040_kline_scope_catalog.sql

.\scripts\apply-db-migrations.ps1 `
  -ContainerName tv_backend_timescaledb `
  -Only 041_kline_scope_catalog_generation_fencing.sql
```

## Create and bootstrap a generation

Use the standard collector entrypoint. Every mutating operation requires an
explicit action flag and generation UUID; invoking the command without an
action only reports state.

```powershell
$env:PYTHONPATH = "services/collector;libs/protocol/python;services/api"
$generation = [guid]::NewGuid().ToString()

python -m collector.worker kline-scope-bootstrap `
  --create-generation `
  --generation-id $generation `
  --timeframes 5f,15f,30f,1h,1d,1w,1m
```

Process bounded batches until `selected` becomes zero:

```powershell
do {
  $result = python -m collector.worker kline-scope-bootstrap `
    --bootstrap --generation-id $generation --batch-size 25
  $payload = $result | ConvertFrom-Json
  $payload | ConvertTo-Json -Compress
} while ($payload.selected -gt 0)
```

Inspect the generation before activation:

```powershell
python -m collector.worker kline-scope-bootstrap --generation-id $generation
```

Finalize only when `scope_count == expected_scope_count`, `unknown_count == 0`,
and `incomplete_count == 0`. Finalization validates those conditions and
switches the active pointer in one transaction. Catalog-aware K-line writers
hold a shared lock on the catalog control row for their full write transaction;
finalization takes the matching exclusive lock, so it cannot activate a
generation while a writer is still changing its scope metadata.

```powershell
python -m collector.worker kline-scope-bootstrap `
  --finalize --generation-id $generation
```

Report the management snapshot without changing state. The payload preserves
the active generation report (or `active_generation_id: null`), includes the
control revision, and exposes the single resumable `building_generation` with
its base fence and progress counts when one exists:

```powershell
$snapshot = python -m collector.worker kline-scope-bootstrap | ConvertFrom-Json
$snapshot | ConvertTo-Json -Depth 4
```

If the original UUID was lost after interruption, recover it from the
read-only snapshot rather than creating another generation:

```powershell
$generation = $snapshot.building_generation.generation_id
if (-not $generation) {
  throw "No building scope-catalog generation is available to resume"
}

python -m collector.worker kline-scope-bootstrap `
  --bootstrap --generation-id $generation --batch-size 25
```

After the resumed generation reaches zero remaining work, use that same
discovered UUID for finalization:

```powershell
python -m collector.worker kline-scope-bootstrap `
  --finalize --generation-id $generation
```

If the discovered building generation cannot be completed, mark only that
same generation failed:

```powershell
python -m collector.worker kline-scope-bootstrap `
  --fail --generation-id $generation --failure "operator-approved reason"
```

Failed and building generations are never visible through
`active_kline_scope_catalog`.

## Verification

The active view must expose only complete rows, and the K-line snapshot must be
unchanged:

```sql
select generation_id, status, expected_scope_count, completed_at
from kline_scope_catalog_generations
order by created_at desc;

select state, count(*) scopes, min(min_ts), max(max_ts)
from active_kline_scope_catalog
group by state
order by state;

select count(*) rows, min(ts) min_ts, max(ts) max_ts
from klines;
```

For migration acceptance, use a disposable test database and reset the
`pg_stat_user_tables` counters for `klines` after test fixtures are inserted.
Migration 040 plus bootstrap must leave `n_tup_ins`, `n_tup_upd`, and
`n_tup_del` at zero. Compare the before/after `pg_indexes` and non-internal
`pg_trigger` sets for `klines`; they must be identical.

## Fail-closed rollback

If catalog correctness is in doubt, disable only the active metadata pointer so
readers fall back to canonical K-line probing. Do not delete catalog history or
modify `klines`.

Clear the active pointer before rolling application code back to a pre-catalog
writer release. This prevents a stale complete generation from being trusted
while old writers no longer maintain it.

```sql
begin;
select active_generation_id, revision
from kline_scope_catalog_control
where control_key = 'active'
for update;

do $$
begin
  if exists (
    select 1
    from kline_scope_catalog_generations
    where status = 'building'
  ) then
    raise exception 'refusing pointer clear while a scope catalog generation is building';
  end if;
end
$$;

update kline_scope_catalog_control
set active_generation_id = null,
    revision = revision + 1,
    updated_at = clock_timestamp()
where control_key = 'active';
commit;
```

Record the removed generation ID and resulting revision before committing.
The transaction refuses to clear the pointer while a generation is building.
Every manual pointer clear or restoration must increment `revision`; this
prevents an older building generation from activating after pointer clearing or
an ABA-style pointer restore. Migration 041 enforces this in the database:
changing the pointer requires exactly `revision + 1`; an unchanged pointer may
keep the revision or increment it by exactly one for an explicit fence. A
pre-041 finalizer that changes only the pointer is rejected and its transaction
rolls back. Diagnose or build a new generation; never relabel an incomplete or
failed generation as complete.
