# Device B Historical Replay / Official Backtest Handoff

Date: 2026-07-15

Branch: `codex/device-b-historical-replay-execution-20260714`

Frozen cutoff: `2026-07-03T07:00:00Z`

Replay batch: `9`

## Outcome

- H1-H5 completed and passed.
- H4/H6 replay execution completed with `33,409` eligible tasks, `10,130` exclusions and `0` failures.
- The final historical lifecycle ledger contains `1,332,787` official events. All `effective_time <= observed_time`; future rows at the frozen cutoff are zero.
- Lifecycle reconciliation passed after intraday publication: outbox blocking `0`, projection mismatches `0`, published-history missing rows `0`.
- H6 formal strategy decision is `NO_GO`. No approximate or diagnostic backtest was presented as official.

## Replay coverage

| Level | Completed tasks | Excluded | Bars | Structures |
|---|---:|---:|---:|---:|
| 5f | 200 | 5,473 | 7,477,400 | 423,214 |
| 30f | 40 | 4,646 | 274,680 | 17,276 |
| 1d | 11,062 | 3 | 27,412,625 | 2,047,904 |
| 1w | 11,058 | 3 | 5,802,155 | 466,663 |
| 1m | 11,049 | 5 | 1,368,959 | 102,371 |

The intraday planner admitted only symbols with both 5f and 30f source coverage. Causal official weekly/daily events produced 14 fixed five-day windows across four symbols, resulting in 200 5f and 40 30f tasks. No current head, structure point time, diagnostic row, or post-hoc candidate was used to choose a window.

## Strict strategy gate

The `weekly_daily_b2_resonance_v1` official waterfall is monotonic:

`5529 source high-level eligible -> 5525 official high-level visible -> 61 dual intraday eligible -> 4 predictive weekly B1 -> 0 predictive weekly B2 -> 0 daily episode -> 0 candidate -> 0 trades`

Minimum unblock condition: produce at least one causal official predictive weekly B2 within the dual-level eligible universe, rebuild downstream daily/intraday episodes, and obtain at least three complete official traces. Confirmed, baseline, current, approximate, sanity-loose and diagnostic data remain forbidden substitutes.

## Verification

- Migrations `037`, `038` and `039`: two consecutive idempotent passes succeeded.
- Collector: `287 passed`.
- Strategy service: `191 passed` (artifact-backed suites used the existing Device B outputs copied only into the ignored worktree output directory).
- API: `121 passed, 1 skipped`.
- H3 canary: zero A/B differences.
- Final outbox: `102,138 completed`, no pending/processing/failed/dead-letter rows.

## Runtime and artifacts

- End-to-end task interval: `56,795s`.
- Task duration p50/p95: `4.70s / 10.28s`.
- Total bars / structures: `42,335,819 / 3,057,428`.
- F drive free at audit: about `672 GiB`.
- Required runtime artifacts are under `outputs/device-b-historical-replay-20260714/` and intentionally ignored by Git. This includes the 1.4GB official JSONL dataset, manifests, coverage, exclusions, reconciliation, ledger, waterfall, rejection traces, metrics and final decision.

## Database impact and rollback

- Added migrations 037-039 schema, durable replay batch/task/head state, isolated replay runs/heads and append-only historical lifecycle events.
- K-lines were read only. No K-line row, valid run or valid head was deleted.
- Replay output is isolated by publication profile/run group. Code rollback is a normal commit revert; database history should remain retained for audit and must not be destructively rolled back.
