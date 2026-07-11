# Device B GitHub Pull And Push Instructions

## 1. Purpose

Device A has established the shared source-code baseline in:

- Repository: `https://github.com/yylxy0809/Tvchan.git`
- Baseline branch: `master`
- Required baseline anchor: `bbe903c5357cd0aa6d0284a922ebfbe3a6bc9d8a`

The published `origin/master` also contains this instruction document, so its
tip is newer than the anchor above. Treat the current `origin/master` tip as the
integration baseline and verify that the required anchor is its ancestor.

Device B must pull this baseline, preserve its existing local backend work, and push that work to an independent branch for review. Do not execute production imports, database migrations, or Module C recomputation merely because code has been pushed or merged.

## 2. Mandatory Safety Rules

Do not upload any of the following:

- PostgreSQL or TimescaleDB data directories, tablespaces, WAL, Docker volumes, or database backups.
- K-line databases or raw `5f/30f/1d/1w/1m` market-data files.
- Parquet, Arrow, Feather, bulk CSV, ZIP, 7z, or other source-data archives.
- `.env` files, passwords, API keys, tokens, cookies, provider credentials, or admin secrets.
- Runtime logs, process status, temporary files, caches, profiling dumps, or SMB mailbox state.
- TradingView licensed assets under `apps/web/public/charting_library/`.
- Unreviewed copies of third-party `chan.py` source.

Allowed content includes source code, database migration scripts, tests, `.env.example`, Docker definitions, small sanitized audit summaries, and design documents.

Before every commit, run:

```powershell
git status --short
git diff --cached --stat
git diff --cached --check
git diff --cached
```

Inspect the complete staged diff. If a credential or data file appears, unstage it immediately.

## 3. Preserve Existing Device B Work

Do not run `git reset --hard`, `git clean -fd`, or delete the existing Device B worktree.

From the current Device B repository:

```powershell
git status --short
git branch --show-current
git log --oneline --decorate -15
git rev-parse HEAD
```

If there are uncommitted source changes, create a local safety branch and commit only reviewed source files:

```powershell
git switch -c codex/device-b-safety-snapshot
git add <explicit-source-file-paths>
git diff --cached --check
git commit -m "wip(backend): preserve device B source changes"
```

Do not use `git add .` for this safety commit. Add files explicitly so data, logs, and secrets cannot be included accidentally.

If the existing work is already committed on `codex/device-b-bootstrap`, keep that branch and do not recreate it.

Device B previously reported these commits. Verify whether they exist locally and report any mismatch:

```text
155751437e7b9f68d246e3dce2900b526ab366e9
2f900ef41c8c06549104cd31ba6238aa3829dfa3
f5ad537771fbd654071bcadcf6f48ebaeedc21cc
```

Verification command:

```powershell
git show --stat --oneline 155751437e7b9f68d246e3dce2900b526ab366e9
git show --stat --oneline 2f900ef41c8c06549104cd31ba6238aa3829dfa3
git show --stat --oneline f5ad537771fbd654071bcadcf6f48ebaeedc21cc
```

## 4. Connect To The Shared Repository

Configure or correct `origin`:

```powershell
git remote -v
git remote get-url origin
```

If `origin` does not exist:

```powershell
git remote add origin https://github.com/yylxy0809/Tvchan.git
```

If `origin` points elsewhere:

```powershell
git remote set-url origin https://github.com/yylxy0809/Tvchan.git
```

Fetch without modifying the current branch:

```powershell
git fetch origin --prune
git show --no-patch --oneline origin/master
git merge-base --is-ancestor bbe903c5357cd0aa6d0284a922ebfbe3a6bc9d8a origin/master
```

The final command must exit with code `0`. If it does not, stop and report the
actual `origin/master` commit before continuing.

## 5. Integrate The Device A Baseline

The preferred approach is to keep Device B changes on `codex/device-b-bootstrap` and rebase them onto `origin/master`:

```powershell
git switch codex/device-b-bootstrap
git status --short
git rebase origin/master
```

Only rebase when the worktree is clean. If conflicts occur:

1. Stop and inspect each conflict.
2. Preserve Device A contracts and Device B's intentional backend implementation.
3. Do not resolve conflicts by taking all of one side.
4. Do not change `chan.py` algorithm semantics.
5. Run relevant tests after resolving conflicts.

Continue after resolving and staging each conflict:

```powershell
git rebase --continue
```

If the branch has complicated or uncertain history, do not force a rebase. Instead create a fresh branch from the baseline and cherry-pick reviewed commits one at a time:

```powershell
git switch -c codex/device-b-bootstrap-v2 origin/master
git cherry-pick <reviewed-commit-1>
git cherry-pick <reviewed-commit-2>
```

Report which method was used.

## 6. Required Verification Before Push

Device B must run and report:

```powershell
git status --short
git log --oneline --decorate origin/master..HEAD
git diff --stat origin/master...HEAD
git diff --check origin/master...HEAD
```

Also run the relevant backend test suites in the documented Python 3.11 or container environment. At minimum report:

- Exact test commands.
- Passed, failed, skipped, and duration counts.
- Migration checks performed.
- Whether any test used a production database.
- Confirmation that production data was not deleted or rewritten.
- Confirmation that `chan.py` semantics were not modified.

Database migrations must be reviewed separately. Migrations M030-M036 and Module C full recomputation remain blocked unless Device A or the user explicitly approves them.

## 7. Push Device B Branch

Push only the Device B feature branch, never directly to `master`:

```powershell
git push -u origin codex/device-b-bootstrap
```

If a replacement branch was created, push its exact name instead:

```powershell
git push -u origin codex/device-b-bootstrap-v2
```

Do not use `--force` or `--force-with-lease` without explicit approval.

After pushing, open a pull request to `master` using GitHub's web interface:

```text
https://github.com/yylxy0809/Tvchan/compare/master...codex/device-b-bootstrap
```

The pull request description must include:

- Scope and purpose.
- Commit list.
- Files and migrations changed.
- Tests and exact results.
- Database/runtime impact.
- Rollback plan.
- Known limitations, including BJ `5f/30f` coverage.
- Explicit statement that pushing code does not authorize production execution.

## 8. Split Pull Requests By Risk

Do not combine all Device B work into one unreviewable pull request. Prefer:

1. Import preflight, profiling, and quarantine schema/code.
2. Deadlock prevention, deterministic partitioning, single-writer/checkpoint recovery, and tests.
3. K-line normalization and reconciliation logic.
4. Database migrations, one migration group per PR when practical.
5. Module C configuration or lifecycle-layer changes in a separate PR.

M030-M036 must remain separate from already approved migrations. A PR may propose them, but it must not claim they have production approval.

## 9. Required Reply From Device B

After pull and push, Device B must report the following through the GitHub pull request and the agreed status channel:

```text
Device B branch:
Device B HEAD:
origin/master HEAD:
Integration method: rebase / cherry-pick
Pushed branch URL:
Pull request URL:
Commits included:
Tests executed and results:
Migrations included:
Production database operations performed after this instruction:
Uncommitted files remaining:
Excluded data/secrets/logs confirmed:
Known blockers or decisions required:
```

Device A will review the pull request before merge. No production import, M030-M036 migration, or Module C recomputation may start solely because this handoff is complete.
