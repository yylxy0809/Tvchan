from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


ACTIVE_COVERAGE_SQL = """
select s.id as symbol_id, s.code, s.exchange,
       w.timeframe, w.last_bar_end, w.source, w.updated_at
from symbols s
left join scheme2_ingest_watermarks w on w.symbol_id = s.id
where s.is_active = true
order by s.code, s.exchange, w.timeframe
"""

IMPORT_RUNS_SQL = """
select r.import_run_id::text, r.source_name, r.started_at, r.completed_at,
       r.status, r.parameters, r.summary, r.failure,
       count(c.*)::bigint as checkpoint_count,
       count(c.*) filter (where c.status = 'completed')::bigint as completed_checkpoints,
       count(c.*) filter (where c.status = 'failed')::bigint as failed_checkpoints,
       coalesce(sum(c.accepted_rows), 0)::bigint as accepted_rows,
       coalesce(sum(c.quarantined_rows), 0)::bigint as quarantined_rows
from kline_import_runs r
left join kline_import_checkpoints c on c.import_run_id = r.import_run_id
group by r.import_run_id
order by r.started_at, r.import_run_id
"""

FAILED_CHECKPOINTS_SQL = """
select c.import_run_id::text, c.source_ref, c.source_checksum, c.status,
       c.accepted_rows, c.quarantined_rows, c.error_message, c.updated_at
from kline_import_checkpoints c
where c.status = 'failed'
order by c.import_run_id, c.source_ref
"""

IMPORT_QUARANTINE_SQL = """
select import_run_id::text, coalesce(timeframe, '') as timeframe, reason,
       count(*)::bigint as rows, min(detected_at) as first_detected_at,
       max(detected_at) as last_detected_at
from kline_import_quarantine
group by import_run_id, timeframe, reason
order by import_run_id, timeframe, reason
"""

AUDIT_SUMMARY_SQL = """
select r.audit_run_id::text, r.started_at, r.completed_at, r.status,
       r.apply_mode, r.parameters, r.summary, r.failure,
       count(c.*)::bigint as checkpoint_count,
       count(c.*) filter (where c.status = 'completed')::bigint as completed_checkpoints,
       count(c.*) filter (where c.status = 'failed')::bigint as failed_checkpoints,
       coalesce(sum(c.rows_scanned), 0)::bigint as rows_scanned
from kline_audit_runs r
left join kline_audit_checkpoints c on c.audit_run_id = r.audit_run_id
group by r.audit_run_id
order by r.started_at, r.audit_run_id
"""

AUDIT_QUARANTINE_SQL = """
select audit_run_id::text, timeframe, reason, count(*)::bigint as rows,
       min(quarantined_at) as first_detected_at,
       max(quarantined_at) as last_detected_at
from kline_audit_quarantine
group by audit_run_id, timeframe, reason
order by audit_run_id, timeframe, reason
"""

ARTIFACT_NAMES = (
    "kline_truth_summary.json",
    "kline_truth_summary.md",
    "coverage_by_symbol.jsonl",
    "exceptions.csv",
    "run_manifest.json",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Database timestamp must be timezone-aware")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _dict_rows(rows: Iterable[Any]) -> list[dict[str, Any]]:
    return [_json_value(dict(row)) for row in rows]


@dataclass(frozen=True)
class ManifestMetadata:
    git_commit: str
    image: str
    config_hash: str
    source_roots: tuple[str, ...] = ()
    expected_timeframes: tuple[int, ...] = (5, 30, 1440, 10080, 43200)
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


async def collect_truth_snapshot(conn: Any) -> dict[str, list[dict[str, Any]]]:
    """Read indexed control-plane truth only; this intentionally never reads klines."""
    return {
        "coverage": _dict_rows(await conn.fetch(ACTIVE_COVERAGE_SQL)),
        "import_runs": _dict_rows(await conn.fetch(IMPORT_RUNS_SQL)),
        "failed_checkpoints": _dict_rows(await conn.fetch(FAILED_CHECKPOINTS_SQL)),
        "import_quarantine": _dict_rows(await conn.fetch(IMPORT_QUARANTINE_SQL)),
        "audit_runs": _dict_rows(await conn.fetch(AUDIT_SUMMARY_SQL)),
        "audit_quarantine": _dict_rows(await conn.fetch(AUDIT_QUARANTINE_SQL)),
    }


def _coverage_by_symbol(
    rows: list[dict[str, Any]], expected_timeframes: tuple[int, ...]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    symbols: dict[tuple[int, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (int(row["symbol_id"]), str(row["code"]), str(row["exchange"]))
        item = symbols.setdefault(
            key,
            {"symbol_id": key[0], "symbol": f"{key[1]}.{key[2]}", "watermarks": {}},
        )
        if row.get("timeframe") is not None:
            item["watermarks"][str(row["timeframe"])] = {
                "last_bar_end": row.get("last_bar_end"),
                "source": row.get("source"),
                "updated_at": row.get("updated_at"),
            }
    coverage = sorted(symbols.values(), key=lambda item: item["symbol"])
    exceptions: list[dict[str, Any]] = []
    for item in coverage:
        for timeframe in expected_timeframes:
            watermark = item["watermarks"].get(str(timeframe))
            if not watermark or not watermark.get("last_bar_end"):
                exceptions.append(
                    {
                        "category": "missing_watermark",
                        "symbol": item["symbol"],
                        "timeframe": timeframe,
                        "run_id": "",
                        "source_ref": "",
                        "reason": "active symbol has no committed ingest watermark",
                        "rows": "",
                    }
                )
    return coverage, exceptions


def _universe_hash(coverage: list[dict[str, Any]]) -> str:
    canonical = "\n".join(
        f"{item['symbol_id']}|{item['symbol']}" for item in sorted(coverage, key=lambda row: row["symbol"])
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_artifacts(
    snapshot: dict[str, list[dict[str, Any]]], metadata: ManifestMetadata
) -> dict[str, str]:
    coverage, exceptions = _coverage_by_symbol(snapshot["coverage"], metadata.expected_timeframes)
    for run in snapshot["import_runs"]:
        if run["status"] != "completed" or run["failed_checkpoints"]:
            exceptions.append(
                {
                    "category": "import_run",
                    "symbol": "",
                    "timeframe": "",
                    "run_id": run["import_run_id"],
                    "source_ref": "",
                    "reason": run.get("failure") or f"status={run['status']}",
                    "rows": run["failed_checkpoints"],
                }
            )
    for row in snapshot["failed_checkpoints"]:
        exceptions.append(
            {
                "category": "failed_checkpoint",
                "symbol": "",
                "timeframe": "",
                "run_id": row["import_run_id"],
                "source_ref": row["source_ref"],
                "reason": row.get("error_message") or "failed",
                "rows": row.get("quarantined_rows", ""),
            }
        )
    for category, rows, run_key in (
        ("import_quarantine", snapshot["import_quarantine"], "import_run_id"),
        ("audit_quarantine", snapshot["audit_quarantine"], "audit_run_id"),
    ):
        for row in rows:
            exceptions.append(
                {
                    "category": category,
                    "symbol": "",
                    "timeframe": row.get("timeframe", ""),
                    "run_id": row[run_key],
                    "source_ref": "",
                    "reason": row["reason"],
                    "rows": row["rows"],
                }
            )

    timeframe_coverage = {
        str(timeframe): sum(
            bool(item["watermarks"].get(str(timeframe), {}).get("last_bar_end")) for item in coverage
        )
        for timeframe in metadata.expected_timeframes
    }
    summary = {
        "generated_at": _json_value(metadata.generated_at),
        "active_symbols": len(coverage),
        "expected_timeframes": list(metadata.expected_timeframes),
        "symbols_with_watermark": timeframe_coverage,
        "exception_count": len(exceptions),
        "import_runs": len(snapshot["import_runs"]),
        "audit_runs": len(snapshot["audit_runs"]),
        "quarantined_rows": sum(int(row["rows"]) for row in snapshot["import_quarantine"]),
        "audit_quarantined_rows": sum(int(row["rows"]) for row in snapshot["audit_quarantine"]),
    }
    manifest = {
        "schema_version": 1,
        "generated_at": summary["generated_at"],
        "git_commit": metadata.git_commit,
        "image": metadata.image,
        "config_hash": metadata.config_hash,
        "source_roots": list(metadata.source_roots),
        "active_universe_hash": _universe_hash(coverage),
        "active_symbol_count": len(coverage),
        "import_runs": snapshot["import_runs"],
        "audit_runs": snapshot["audit_runs"],
        "checkpoint_totals": {
            "all": sum(int(run["checkpoint_count"]) for run in snapshot["import_runs"]),
            "completed": sum(int(run["completed_checkpoints"]) for run in snapshot["import_runs"]),
            "failed": sum(int(run["failed_checkpoints"]) for run in snapshot["import_runs"]),
        },
        "quarantine": {
            "import": snapshot["import_quarantine"],
            "audit": snapshot["audit_quarantine"],
        },
    }
    markdown = "\n".join(
        [
            "# K-line truth summary",
            "",
            f"Generated (UTC): `{summary['generated_at']}`",
            f"Active universe: **{summary['active_symbols']}** symbols (`{manifest['active_universe_hash']}`)",
            "",
            "| Timeframe | Symbols with committed watermark |",
            "|---:|---:|",
            *(f"| {tf} | {count} |" for tf, count in timeframe_coverage.items()),
            "",
            f"Exceptions: **{summary['exception_count']}**",
            f"Import quarantine rows: **{summary['quarantined_rows']}**",
            f"Audit quarantine rows: **{summary['audit_quarantined_rows']}**",
            "",
            "This artifact is synthesized from indexed control-plane tables; klines was not scanned.",
        ]
    ) + "\n"
    csv_columns = ("category", "symbol", "timeframe", "run_id", "source_ref", "reason", "rows")
    import io

    csv_buffer = io.StringIO(newline="")
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(exceptions)
    return {
        "kline_truth_summary.json": json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        "kline_truth_summary.md": markdown,
        "coverage_by_symbol.jsonl": "".join(
            json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n" for item in coverage
        ),
        "exceptions.csv": csv_buffer.getvalue(),
        "run_manifest.json": json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    }


def write_artifacts(output_dir: Path, artifacts: dict[str, str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in ARTIFACT_NAMES:
        (output_dir / name).write_text(artifacts[name], encoding="utf-8", newline="\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit durable K-line truth artifacts without scanning klines")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--config-hash", required=True)
    parser.add_argument("--source-root", action="append", default=[])
    parser.add_argument("--expected-timeframes", default="5,30,1440,10080,43200")
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> None:
    import asyncpg

    conn = await asyncpg.connect(args.database_url)
    try:
        snapshot = await collect_truth_snapshot(conn)
    finally:
        await conn.close()
    metadata = ManifestMetadata(
        git_commit=args.git_commit,
        image=args.image,
        config_hash=args.config_hash,
        source_roots=tuple(args.source_root),
        expected_timeframes=tuple(int(value) for value in args.expected_timeframes.split(",")),
    )
    write_artifacts(args.output_dir, build_artifacts(snapshot, metadata))


def main() -> None:
    asyncio.run(_run(parse_args()))


if __name__ == "__main__":
    main()
