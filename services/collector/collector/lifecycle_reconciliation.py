from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import asyncpg


OUTBOX_STATUS_SQL = """
select status, count(*)::bigint as count,
       extract(epoch from (clock_timestamp() - min(created_at)))::bigint as oldest_age_seconds
from chan_c_head_outbox
group by status
order by status
"""

PROJECTION_MISMATCH_SQL = """
with event_state as (
    select e.*,
           min(e.effective_time) filter (where e.event_type = 'first_seen')
               over (partition by e.fingerprint) as first_seen_time,
           min(e.effective_time) filter (where e.event_type = 'confirmed')
               over (partition by e.fingerprint) as confirm_time,
           (array_agg(e.run_id) filter (where e.event_type = 'first_seen')
               over (partition by e.fingerprint order by e.effective_time, e.id))[1] as first_seen_run_id,
           (array_agg(e.run_id) filter (where e.event_type = 'confirmed')
               over (partition by e.fingerprint order by e.effective_time, e.id))[1] as confirmed_run_id
    from chan_structure_lifecycle_events e
    join chan_c_head_history h on h.id = e.head_history_id
    where h.publication_profile <> 'historical_replay'
), expected as (
    select distinct on (fingerprint)
           fingerprint, point_time, first_seen_time, confirm_time,
           case when event_type = 'disappeared' then effective_time end as disappear_time,
           case event_type when 'disappeared' then 'disappeared'
                           when 'baseline_observed' then 'baseline_observed'
                           else 'visible' end as current_status,
           current_mode, first_seen_run_id, confirmed_run_id, run_id as last_seen_run_id
    from event_state
    order by fingerprint, effective_time desc, id desc
), mismatches as (
    select coalesce(e.fingerprint, c.fingerprint) as fingerprint
    from expected e
    full join chan_structure_lifecycle_current c using (fingerprint)
    where e.fingerprint is null or c.fingerprint is null
       or e.point_time is distinct from c.point_time
       or e.first_seen_time is distinct from c.first_seen_time
       or e.confirm_time is distinct from c.confirm_time
       or e.disappear_time is distinct from c.disappear_time
       or e.current_status is distinct from c.current_status
       or e.current_mode is distinct from c.current_mode
       or e.first_seen_run_id is distinct from c.first_seen_run_id
       or e.confirmed_run_id is distinct from c.confirmed_run_id
       or e.last_seen_run_id is distinct from c.last_seen_run_id
)
select count(*)::bigint as count,
       (coalesce(array_agg(fingerprint order by fingerprint) filter (where fingerprint is not null), '{}'))[1:20] as samples
from mismatches
"""

HEAD_COVERAGE_SQL = """
with missing as (
    select head.symbol_id, head.chan_level, head.mode, head.run_id,
           history.id as history_id, outbox.status as outbox_status
    from scheme2_chan_c_published_heads head
    left join chan_c_head_history history
      on history.symbol_id = head.symbol_id
     and history.chan_level = head.chan_level
     and history.mode = head.mode
     and history.base_timeframe = head.base_timeframe
     and history.new_run_id = head.run_id
    left join chan_c_head_outbox outbox on outbox.head_history_id = history.id
    where head.status = 'published' and head.run_id is not null
      and (history.id is null or outbox.id is null)
)
select count(*)::bigint as count,
       coalesce(jsonb_agg(to_jsonb(missing)) filter (where symbol_id is not null), '[]'::jsonb) as samples
from (select * from missing order by symbol_id, chan_level, mode limit 20) missing
"""

WATERMARK_SQL = """
select observer_name, last_outbox_id, updated_at
from chan_lifecycle_observer_watermarks
order by observer_name
"""


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row)


async def build_reconciliation(conn: Any) -> dict[str, Any]:
    status_rows = await conn.fetch(OUTBOX_STATUS_SQL)
    statuses = {
        str(row["status"]): {
            "count": int(row["count"]),
            "oldest_age_seconds": int(row["oldest_age_seconds"] or 0),
        }
        for row in status_rows
    }
    projection = _row_dict(await conn.fetchrow(PROJECTION_MISMATCH_SQL))
    head_coverage = _row_dict(await conn.fetchrow(HEAD_COVERAGE_SQL))
    watermarks = [_row_dict(row) for row in await conn.fetch(WATERMARK_SQL)]
    blocking_statuses = sum(statuses.get(name, {}).get("count", 0) for name in ("pending", "processing", "failed", "dead_letter"))
    blockers = []
    if blocking_statuses:
        blockers.append("outbox_not_drained")
    if int(projection["count"]):
        blockers.append("current_projection_differs_from_event_replay")
    if int(head_coverage["count"]):
        blockers.append("published_head_missing_history_or_outbox")
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "readonly": True,
        "outbox": {"statuses": statuses, "blocking_count": blocking_statuses},
        "watermarks": watermarks,
        "projection_replay": {
            "mismatch_count": int(projection["count"]),
            "samples": list(projection.get("samples") or []),
        },
        "published_head_history": {
            "missing_count": int(head_coverage["count"]),
            "samples": head_coverage.get("samples") or [],
        },
        "decision": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
    }


def render_markdown(report: dict[str, Any]) -> str:
    status_lines = [
        f"| {name} | {values['count']} | {values['oldest_age_seconds']} |"
        for name, values in sorted(report["outbox"]["statuses"].items())
    ]
    return "\n".join(
        [
            "# Lifecycle Reconciliation",
            "",
            f"- Decision: `{report['decision']}`",
            f"- Projection replay mismatches: `{report['projection_replay']['mismatch_count']}`",
            f"- Published heads missing history/outbox: `{report['published_head_history']['missing_count']}`",
            f"- Blockers: `{', '.join(report['blockers']) or 'none'}`",
            "",
            "| Outbox status | Count | Oldest age (seconds) |",
            "|---|---:|---:|",
            *status_lines,
            "",
        ]
    )


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Module C lifecycle reconciliation")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"), required=os.getenv("DATABASE_URL") is None)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    conn = await asyncpg.connect(args.database_url)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            report = await build_reconciliation(conn)
    finally:
        await conn.close()
    _atomic_write(args.output_dir / "lifecycle_reconciliation.json", json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    _atomic_write(args.output_dir / "lifecycle_reconciliation.md", render_markdown(report))
    print(json.dumps({"decision": report["decision"], "blockers": report["blockers"]}, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
