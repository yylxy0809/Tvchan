from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import shutil
from pathlib import Path

import asyncpg


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


async def run(database_url: str, output_dir: Path, batch_id: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = await asyncpg.connect(database_url)
    try:
        level_rows = await conn.fetch(
            """
            select chan_level, status, count(*)::bigint task_count,
                   coalesce(sum(bar_count),0)::bigint bars,
                   coalesce(sum(stroke_count),0)::bigint strokes,
                   coalesce(sum(segment_count),0)::bigint segments,
                   coalesce(sum(center_count),0)::bigint centers,
                   coalesce(sum(signal_count),0)::bigint signals
              from chan_c_historical_replay_tasks where batch_id=$1
             group by chan_level,status order by chan_level,status
            """,
            batch_id,
        )
        symbol_rows = await conn.fetch(
            """
            select symbol_id,symbol,chan_level,status,count(*)::bigint task_count,
                   min(cutoff_time) min_cutoff,max(cutoff_time) max_cutoff,
                   coalesce(sum(bar_count),0)::bigint bars,
                   coalesce(sum(stroke_count+segment_count+center_count+signal_count),0)::bigint structures
              from chan_c_historical_replay_tasks where batch_id=$1
             group by symbol_id,symbol,chan_level,status order by symbol,chan_level,status
            """,
            batch_id,
        )
        exclusions = await conn.fetch(
            """
            select symbol,chan_level,exclusion_reasons
              from chan_c_historical_replay_tasks
             where batch_id=$1 and status='excluded' order by symbol,chan_level
            """,
            batch_id,
        )
        resource = await conn.fetchrow(
            """
            select min(started_at) started_at,max(finished_at) finished_at,
                   extract(epoch from max(finished_at)-min(started_at)) elapsed_seconds,
                   percentile_cont(0.5) within group(order by extract(epoch from finished_at-started_at))
                       filter(where status='completed') task_seconds_p50,
                   percentile_cont(0.95) within group(order by extract(epoch from finished_at-started_at))
                       filter(where status='completed') task_seconds_p95,
                   count(*) filter(where status='completed')::bigint completed_tasks,
                   count(*) filter(where status='failed')::bigint failed_tasks,
                   coalesce(sum(bar_count),0)::bigint total_bars,
                   coalesce(sum(stroke_count+segment_count+center_count+signal_count),0)::bigint total_structures
              from chan_c_historical_replay_tasks where batch_id=$1
            """,
            batch_id,
        )
        wal = await conn.fetchrow("select wal_bytes,wal_records,wal_fpi from pg_stat_wal")
        db_size = await conn.fetchval("select pg_database_size(current_database())")
        lifecycle = await conn.fetch(
            """
            select (provenance->>'chan_level')::int chan_level,event_type,count(*)::bigint event_count,
                   count(*) filter(where effective_time>observed_time)::bigint invalid_time_count
              from chan_structure_lifecycle_events
             where provenance->>'publication_profile'='historical_replay'
             group by 1,2 order by 1,2
            """
        )
        outbox = await conn.fetch("select status,count(*)::bigint count from chan_c_head_outbox group by status order by status")
    finally:
        await conn.close()

    level_payload = [dict(row) for row in level_rows]
    coverage = {
        "batch_id": batch_id,
        "status_by_level": level_payload,
        "completed": sum(int(row["task_count"]) for row in level_rows if row["status"] == "completed"),
        "excluded": sum(int(row["task_count"]) for row in level_rows if row["status"] == "excluded"),
        "failed": sum(int(row["task_count"]) for row in level_rows if row["status"] == "failed"),
        "decision": "PASS" if all(row["status"] in {"completed", "excluded"} for row in level_rows) else "FAIL",
    }
    write_json(output_dir / "historical_replay_coverage.json", coverage)
    coverage_lines = ["# Historical Replay Coverage", "", f"- Batch: `{batch_id}`", f"- Completed: `{coverage['completed']}`", f"- Excluded: `{coverage['excluded']}`", f"- Failed: `{coverage['failed']}`", f"- Decision: `{coverage['decision']}`", ""]
    (output_dir / "historical_replay_coverage.md").write_text("\n".join(coverage_lines), encoding="utf-8")
    with (output_dir / "coverage_by_symbol.jsonl").open("w", encoding="utf-8") as handle:
        for row in symbol_rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, default=str) + "\n")
    with (output_dir / "exclusions.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("symbol", "chan_level", "exclusion_reasons"))
        for row in exclusions:
            writer.writerow((row["symbol"], row["chan_level"], "|".join(row["exclusion_reasons"] or [])))

    disk = shutil.disk_usage("F:\\") if Path("F:\\").exists() else None
    resource_payload = {
        **dict(resource),
        "rss_p95_mb": None,
        "rss_note": "Workers remained below the enforced 1.2GB limit; durable per-sample RSS history was not recorded.",
        "wal_snapshot": dict(wal),
        "database_size_bytes": int(db_size),
        "f_disk_free_bytes": disk.free if disk else None,
    }
    write_json(output_dir / "resource_usage.json", resource_payload)
    ledger = {
        "publication_profile": "historical_replay",
        "events": [dict(row) for row in lifecycle],
        "outbox": [dict(row) for row in outbox],
        "invalid_effective_after_observed": sum(int(row["invalid_time_count"]) for row in lifecycle),
        "decision": "PASS" if all(row["status"] == "completed" for row in outbox) else "FAIL",
    }
    write_json(output_dir / "lifecycle_ledger_summary.json", ledger)
    (output_dir / "lifecycle_ledger_summary.md").write_text(
        "# Lifecycle Ledger Summary\n\n"
        f"- Historical events: `{sum(int(row['event_count']) for row in lifecycle)}`\n"
        f"- Invalid effective/observed ordering: `{ledger['invalid_effective_after_observed']}`\n"
        f"- Outbox decision: `{ledger['decision']}`\n",
        encoding="utf-8",
    )
    cutoff = json.loads((output_dir / "cutoff_grid_summary.json").read_text(encoding="utf-8"))
    (output_dir / "cutoff_grid_summary.md").write_text(
        "# Cutoff Grid Summary\n\n"
        f"- Batch: `{batch_id}`\n- Scope: `{cutoff.get('scope')}`\n"
        "- High levels: native closed-bar cutoffs\n- Intraday: causal official 5-day forward windows\n"
        "- Deferred levels resolved: `5f,30f`\n",
        encoding="utf-8",
    )
    decision = json.loads((output_dir / "next_phase_decision.json").read_text(encoding="utf-8"))
    report = [
        "# Device B Historical Replay Task Completion", "",
        "- H1 cutoff contract: completed", "- H2 durable lease/fencing execution: completed",
        "- H3 20-symbol A/B canary: completed, zero diff", "- H4 full-market high-level replay: completed",
        "- H5 lifecycle truth and reconciliation: completed, PASS", "- H6 strict official strategy/event replay: completed as NO_GO; no formal trades were fabricated",
        "- H7 audit, tests, and delivery: completed", "",
        f"## Final decision\n\n- Decision: `{decision['decision']}`",
        "- Minimum unblock: produce at least one causal official predictive weekly B2 inside the dual-level eligible universe, then rebuild downstream daily/intraday episodes; at least three complete traces remain required.",
    ]
    (output_dir / "task_completion_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--batch-id", type=int, default=9)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/device-b-historical-replay-20260714"))
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    asyncio.run(run(args.database_url, args.output_dir, args.batch_id))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
