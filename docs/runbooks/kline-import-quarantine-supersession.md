# K-line import quarantine supersession

Use this procedure only when a later strict-v2 canonical audit proves that an
older import-quarantine group is covered by complete, anomaly-free canonical
rows for the exact symbol and timeframe.

Safety contract:

- Never delete or update `kline_import_quarantine`.
- Name every source import run explicitly; there is no “latest” or wildcard mode.
- Use a completed, read-only `module-c-strict-audit-v2` audit explicitly.
- Review dry-run counts first. Unresolved/empty audit scopes remain quarantined.
- The durable record is bound to the exact group row count and maximum identity.
  A later quarantine insert invalidates the match and fails closed.
- A new eligibility audit requires its own supersession evidence; old audit
  evidence is never silently reused.

Dry-run example:

```powershell
python -m collector.kline_quarantine_supersession `
  --database-url $env:DATABASE_URL `
  --audit-run-id <explicit-audit-uuid> `
  --import-run-id <explicit-import-uuid> `
  --justification "reviewed canonical replacement evidence" `
  --dry-run
```

Remove `--dry-run` only after the candidate/retained counts and manifest hash
have been reviewed. Then regenerate strict-v2 eligibility with the same audit;
do not reuse an earlier eligibility build.
