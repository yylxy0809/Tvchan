"""Safe finalisation and recovery reporting for local Parquet import runs.

The import control tables are the source of truth for a run's terminal state.
This module never touches ``klines`` or quarantine evidence.  A run becomes
``completed`` only when its declared task count exactly matches durable
completed checkpoints.  A run becomes ``failed`` only when every declared
task has a terminal checkpoint and one or more of those checkpoints failed,
or through the explicit supersede operation below.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID


RUN_REPORT_SQL = """
select
    r.import_run_id, r.status, r.started_at, r.completed_at, r.failure,
    r.parameters,
    coalesce((r.parameters ->> 'tasks')::bigint, 0) as expected_tasks,
    count(c.*) as checkpoint_tasks,
    count(c.*) filter (where c.status = 'completed') as completed_tasks,
    count(c.*) filter (where c.status = 'failed') as failed_tasks,
    count(c.*) filter (where c.status not in ('completed', 'failed')) as unfinished_tasks,
    coalesce(sum(c.accepted_rows) filter (where c.status = 'completed'), 0) as accepted_rows,
    coalesce(sum(c.quarantined_rows) filter (where c.status = 'completed'), 0) as quarantined_rows
from kline_import_runs r
left join kline_import_checkpoints c on c.import_run_id = r.import_run_id
where r.source_name = 'parquet_native'
  and ($1::uuid is null or r.import_run_id = $1)
  and ($2::text is null or r.parameters ->> 'adapter' = $2)
group by r.import_run_id
order by r.started_at, r.import_run_id
"""


def terminal_status(row: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """Return the safe terminal state implied by durable checkpoints.

    Incomplete task sets intentionally remain running: they are resumable and
    must not be confused with a source failure.  A zero expected count also
    remains running so a malformed invocation cannot look successful.
    """
    expected = int(row.get("expected_tasks") or 0)
    checkpoints = int(row.get("checkpoint_tasks") or 0)
    failed = int(row.get("failed_tasks") or 0)
    unfinished = int(row.get("unfinished_tasks") or 0)
    if expected <= 0 or checkpoints != expected or unfinished:
        return None, None
    if failed:
        return "failed", f"{failed} of {expected} declared import tasks have failed checkpoints"
    return "completed", None


def unfinished_shards(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return resumable static shards without guessing or changing their state."""
    selected = []
    for row in rows:
        parameters = row.get("parameters") or {}
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        implied, _ = terminal_status(row)
        if row.get("status") == "running" and implied is None:
            selected.append({
                "import_run_id": str(row["import_run_id"]),
                "shard_index": parameters.get("shard_index"),
                "shard_count": parameters.get("shard_count"),
                "expected_tasks": int(row.get("expected_tasks") or 0),
                "checkpoint_tasks": int(row.get("checkpoint_tasks") or 0),
                "completed_tasks": int(row.get("completed_tasks") or 0),
                "failed_tasks": int(row.get("failed_tasks") or 0),
                "unfinished_tasks": int(row.get("unfinished_tasks") or 0),
            })
    return selected


async def fetch_run_reports(conn, *, import_run_id: UUID | None = None, adapter: str | None = "local_parquet_import") -> list[dict[str, Any]]:
    rows = await conn.fetch(RUN_REPORT_SQL, import_run_id, adapter)
    return [dict(row) for row in rows]


async def finalize_import_run(conn, *, import_run_id: UUID) -> dict[str, Any]:
    """Atomically mark one fully reconciled run terminal, otherwise leave it unchanged."""
    async with conn.transaction():
        await conn.fetchrow(
            "select import_run_id from kline_import_runs where import_run_id=$1 for update",
            import_run_id,
        )
        rows = await conn.fetch(RUN_REPORT_SQL, import_run_id, "local_parquet_import")
        if len(rows) != 1:
            raise ValueError(f"local parquet import run not found: {import_run_id}")
        row = dict(rows[0])
        desired, failure = terminal_status(row)
        current = str(row["status"])
        if desired is not None and current == "running":
            await conn.execute(
                """update kline_import_runs
                   set status=$2, completed_at=now(), failure=$3,
                       summary = coalesce(summary, '{}'::jsonb) || $4::jsonb
                   where import_run_id=$1 and status='running'""",
                import_run_id, desired, failure,
                json.dumps({"expected_tasks": int(row["expected_tasks"]), "checkpoint_tasks": int(row["checkpoint_tasks"]),
                            "completed_tasks": int(row["completed_tasks"]), "failed_tasks": int(row["failed_tasks"]),
                            "unfinished_tasks": int(row["unfinished_tasks"])}),
            )
            row["status"] = desired
            row["failure"] = failure
        row["terminal_decision"] = desired
        return row


async def supersede_running_shards(conn, *, shard_indexes: set[int], replacement_run_ids: set[UUID]) -> list[UUID]:
    """Explicitly retire stale deadlock shards after their replacements exist.

    The replacement proof prevents an operator typo from abandoning the only
    resumable copy of a shard.  This changes control metadata only; all durable
    K-lines, checkpoints and quarantine rows remain intact.
    """
    if not shard_indexes or not replacement_run_ids:
        raise ValueError("superseding requires shard indexes and replacement run IDs")
    async with conn.transaction():
        replacements = await conn.fetch(
            "select import_run_id from kline_import_runs where import_run_id = any($1::uuid[])",
            list(replacement_run_ids),
        )
        found = {row["import_run_id"] for row in replacements}
        if found != replacement_run_ids:
            raise ValueError("one or more replacement run IDs do not exist")
        await conn.fetch("select import_run_id from kline_import_runs where source_name='parquet_native' for update")
        rows = await conn.fetch(RUN_REPORT_SQL, None, "local_parquet_import")
        affected: list[UUID] = []
        for record in rows:
            row = dict(record)
            params = row.get("parameters") or {}
            if isinstance(params, str):
                params = json.loads(params)
            shard = params.get("shard_index")
            if row["status"] != "running" or shard not in shard_indexes or row["import_run_id"] in replacement_run_ids:
                continue
            await conn.execute(
                """update kline_import_runs set status='failed', completed_at=now(),
                       failure=$2, summary=coalesce(summary, '{}'::jsonb) || $3::jsonb
                   where import_run_id=$1 and status='running'""",
                row["import_run_id"],
                "superseded after deadlock; durable rows retained; resume replacement run instead",
                json.dumps({"superseded": True, "replacement_run_ids": sorted(map(str, replacement_run_ids))}),
            )
            affected.append(row["import_run_id"])
        return affected


async def _main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report/finalize local Parquet import control runs")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"), required=os.getenv("DATABASE_URL") is None)
    parser.add_argument("--import-run-id")
    parser.add_argument("--finalize", action="store_true", help="Finalize this run only when durable tasks reconcile")
    parser.add_argument("--unfinished", action="store_true", help="Print resumable shard records")
    parser.add_argument("--supersede-shards", help="Explicit stale shard indexes, e.g. 8-15")
    parser.add_argument("--replacement-run-id", action="append", default=[])
    args = parser.parse_args(list(argv) if argv is not None else None)
    import asyncpg
    conn = await asyncpg.connect(args.database_url)
    try:
        run_id = UUID(args.import_run_id) if args.import_run_id else None
        if args.finalize:
            if run_id is None:
                raise ValueError("--finalize requires --import-run-id")
            payload: Any = await finalize_import_run(conn, import_run_id=run_id)
        elif args.supersede_shards:
            indexes = _parse_shards(args.supersede_shards)
            replacements = {UUID(value) for value in args.replacement_run_id}
            payload = {"superseded_run_ids": [str(value) for value in await supersede_running_shards(conn, shard_indexes=indexes, replacement_run_ids=replacements)]}
        else:
            rows = await fetch_run_reports(conn, import_run_id=run_id)
            payload = unfinished_shards(rows) if args.unfinished else rows
        print(json.dumps(payload, default=str, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        await conn.close()
    return 0


def _parse_shards(value: str) -> set[int]:
    indexes: set[int] = set()
    for item in value.split(","):
        item = item.strip()
        if item.startswith("-"):
            raise ValueError("shard indexes must be non-negative")
        start, separator, end = item.partition("-")
        if not start:
            continue
        if separator:
            indexes.update(range(int(start), int(end) + 1))
        else:
            indexes.add(int(start))
    if any(item < 0 for item in indexes):
        raise ValueError("shard indexes must be non-negative")
    return indexes


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
