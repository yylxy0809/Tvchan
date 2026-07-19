"""Create append-only, audit-bound supersession evidence for import quarantine.

This tool never updates or deletes quarantine/K-line rows.  It records only an
exact historical quarantine group that a later canonical audit found complete
and anomaly-free for the same symbol and timeframe.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import asyncpg


AUDIT_CONTRACT_VERSION = "module-c-strict-audit-v2"
ALLOWED_REASONS = ("ambiguous_volume_unit", "missing_source_file")
TIMEFRAME_CODES = {"5f": 5, "30f": 30, "1d": 1440, "1w": 10080, "1m": 43200}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class SupersessionRecord:
    supersession_id: uuid.UUID
    source_import_run_id: uuid.UUID
    reason: str
    symbol_id: int
    symbol: str
    timeframe: str
    quarantine_rows: int
    max_quarantine_id: int
    canonical_audit_run_id: uuid.UUID
    audit_evidence_sha256: str
    justification: str


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _aware_timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"{label} must be an RFC3339 timestamp") from error
    else:
        raise ValueError(f"{label} must be an RFC3339 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def validate_audit_evidence(audit: Mapping[str, Any]) -> tuple[str, datetime]:
    parameters = _json_object(audit.get("parameters"))
    summary = _json_object(audit.get("summary"))
    if audit.get("status") != "completed" or bool(audit.get("apply_mode")):
        raise ValueError("canonical audit must be completed and read-only")
    if parameters.get("contract_version") != AUDIT_CONTRACT_VERSION:
        raise ValueError("canonical audit contract is not strict-v2")
    evidence_sha256 = summary.get("evidence_sha256")
    if summary.get("evidence_complete") is not True:
        raise ValueError("canonical audit evidence is incomplete")
    if not isinstance(evidence_sha256, str) or not _SHA256_RE.fullmatch(evidence_sha256):
        raise ValueError("canonical audit evidence SHA-256 is invalid")
    return evidence_sha256, _aware_timestamp(parameters.get("observed_at"), "audit observed_at")


def _stable_id(
    audit_run_id: uuid.UUID,
    group: Mapping[str, Any],
) -> uuid.UUID:
    identity = "|".join(
        (
            str(audit_run_id),
            str(group["import_run_id"]),
            str(group["reason"]),
            str(group["symbol_id"]),
            str(group["timeframe"]),
            str(group["quarantine_rows"]),
            str(group["max_quarantine_id"]),
        )
    )
    return uuid.uuid5(uuid.NAMESPACE_URL, f"tvchan:kline-quarantine-supersession:{identity}")


def build_supersession_records(
    *,
    audit_run_id: uuid.UUID,
    audit_evidence_sha256: str,
    audit_observed_at: datetime,
    groups: Iterable[Mapping[str, Any]],
    checkpoints: Iterable[Mapping[str, Any]],
    justification: str,
) -> list[SupersessionRecord]:
    justification = justification.strip()
    if not justification:
        raise ValueError("justification must be non-empty")
    checkpoints = list(checkpoints)
    checkpoint_by_scope = {
        (int(row["symbol_id"]), int(row["timeframe"])): row for row in checkpoints
    }
    if len(checkpoint_by_scope) != len(checkpoints):
        raise ValueError("canonical audit contains duplicate scope checkpoints")
    records: list[SupersessionRecord] = []
    for group in groups:
        reason = str(group["reason"])
        timeframe = str(group["timeframe"])
        if reason not in ALLOWED_REASONS or timeframe not in TIMEFRAME_CODES:
            raise ValueError("unsupported quarantine group")
        if group.get("import_status") != "completed":
            raise ValueError("source import run is not completed")
        completed_at = _aware_timestamp(group.get("import_completed_at"), "import completed_at")
        if completed_at >= audit_observed_at:
            raise ValueError("source import is not older than canonical audit")
        checkpoint = checkpoint_by_scope.get((int(group["symbol_id"]), TIMEFRAME_CODES[timeframe]))
        metadata = _json_object(checkpoint.get("metadata")) if checkpoint else {}
        if (
            checkpoint is None
            or checkpoint.get("status") != "completed"
            or metadata.get("disposition") != "eligible"
        ):
            raise ValueError(f"scope {group['symbol']} {timeframe} is not canonical-eligible")
        if int(checkpoint.get("rows_scanned") or 0) <= 0:
            raise ValueError(f"scope {group['symbol']} {timeframe} has no canonical rows")
        quarantine_rows = int(group["quarantine_rows"])
        max_quarantine_id = int(group["max_quarantine_id"])
        if quarantine_rows <= 0 or max_quarantine_id <= 0:
            raise ValueError("quarantine group bounds are invalid")
        records.append(
            SupersessionRecord(
                supersession_id=_stable_id(audit_run_id, group),
                source_import_run_id=uuid.UUID(str(group["import_run_id"])),
                reason=reason,
                symbol_id=int(group["symbol_id"]),
                symbol=str(group["symbol"]),
                timeframe=timeframe,
                quarantine_rows=quarantine_rows,
                max_quarantine_id=max_quarantine_id,
                canonical_audit_run_id=audit_run_id,
                audit_evidence_sha256=audit_evidence_sha256,
                justification=justification,
            )
        )
    return records


async def create_supersessions(
    connection: asyncpg.Connection,
    *,
    audit_run_id: uuid.UUID,
    import_run_ids: list[uuid.UUID],
    justification: str,
    dry_run: bool,
) -> dict[str, Any]:
    if not import_run_ids:
        raise ValueError("at least one explicit import run ID is required")
    async with connection.transaction(isolation="serializable"):
        audit = await connection.fetchrow(
            "SELECT status,apply_mode,parameters,summary FROM kline_audit_runs "
            "WHERE audit_run_id=$1 FOR SHARE",
            audit_run_id,
        )
        if audit is None:
            raise ValueError("canonical audit run not found")
        evidence_sha256, observed_at = validate_audit_evidence(dict(audit))
        groups = await connection.fetch(
            """
            SELECT q.import_run_id, r.status AS import_status,
                   r.completed_at AS import_completed_at, q.reason,
                   s.id AS symbol_id,
                   upper(btrim(q.symbol_text)) AS symbol,
                   lower(btrim(q.timeframe)) AS timeframe,
                   count(*) AS quarantine_rows, max(q.id) AS max_quarantine_id
            FROM kline_import_quarantine q
            JOIN kline_import_runs r ON r.import_run_id=q.import_run_id
            LEFT JOIN symbols s
              ON upper(s.code || '.' || s.exchange)=upper(btrim(q.symbol_text))
            WHERE q.import_run_id=ANY($1::uuid[])
              AND q.reason=ANY($2::text[])
              AND q.symbol_text IS NOT NULL AND q.timeframe IS NOT NULL
            GROUP BY q.import_run_id,r.status,r.completed_at,q.reason,
                     s.id,upper(btrim(q.symbol_text)),lower(btrim(q.timeframe))
            ORDER BY q.import_run_id,s.id,timeframe,q.reason
            """,
            import_run_ids,
            list(ALLOWED_REASONS),
        )
        if not groups:
            raise ValueError("explicit import runs contain no supersedable quarantine groups")
        observed_import_runs = {uuid.UUID(str(row["import_run_id"])) for row in groups}
        if observed_import_runs != set(import_run_ids):
            raise ValueError("an explicit import run is missing supersedable quarantine groups")
        if any(row["symbol_id"] is None for row in groups):
            raise ValueError("quarantine symbol is not in the authoritative symbol master")
        unsupported = sorted(
            {
                str(row["timeframe"])
                for row in groups
                if str(row["timeframe"]) not in TIMEFRAME_CODES
            }
        )
        if unsupported:
            raise ValueError(f"unsupported quarantine timeframes: {unsupported}")
        checkpoints = await connection.fetch(
            "SELECT symbol_id,timeframe,status,rows_scanned,metadata "
            "FROM kline_audit_checkpoints WHERE audit_run_id=$1 "
            "AND symbol_id=ANY($2::integer[]) AND timeframe=ANY($3::integer[])",
            audit_run_id,
            sorted({int(row["symbol_id"]) for row in groups}),
            sorted({TIMEFRAME_CODES[str(row["timeframe"])] for row in groups}),
        )
        eligible_scopes = {
            (int(row["symbol_id"]), int(row["timeframe"]))
            for row in checkpoints
            if row["status"] == "completed"
            and _json_object(row["metadata"]).get("disposition") == "eligible"
            and int(row["rows_scanned"] or 0) > 0
        }
        eligible_groups = [
            row for row in groups
            if (int(row["symbol_id"]), TIMEFRAME_CODES.get(str(row["timeframe"]), -1))
            in eligible_scopes
        ]
        records = build_supersession_records(
            audit_run_id=audit_run_id,
            audit_evidence_sha256=evidence_sha256,
            audit_observed_at=observed_at,
            groups=eligible_groups,
            checkpoints=checkpoints,
            justification=justification,
        )
        if not dry_run and records:
            await connection.executemany(
                """
                INSERT INTO kline_import_quarantine_supersessions (
                    supersession_id,source_import_run_id,reason,symbol_id,symbol,timeframe,
                    quarantine_rows,max_quarantine_id,canonical_audit_run_id,
                    audit_evidence_sha256,justification
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT DO NOTHING
                """,
                [
                    (
                        row.supersession_id,row.source_import_run_id,row.reason,row.symbol_id,
                        row.symbol,row.timeframe,row.quarantine_rows,row.max_quarantine_id,
                        row.canonical_audit_run_id,row.audit_evidence_sha256,row.justification,
                    )
                    for row in records
                ],
            )
        manifest_sha256 = hashlib.sha256(
            json.dumps(
                [
                    {
                        "source_import_run_id": str(row.source_import_run_id),
                        "reason": row.reason,
                        "symbol_id": row.symbol_id,
                        "symbol": row.symbol,
                        "timeframe": row.timeframe,
                        "quarantine_rows": row.quarantine_rows,
                        "max_quarantine_id": row.max_quarantine_id,
                    }
                    for row in records
                ],
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return {
            "audit_run_id": str(audit_run_id),
            "audit_evidence_sha256": evidence_sha256,
            "candidate_groups": len(groups),
            "superseded_groups": len(records),
            "retained_groups": len(groups) - len(records),
            "requested_insert_groups": 0 if dry_run else len(records),
            "manifest_sha256": manifest_sha256,
            "dry_run": dry_run,
        }


async def _main(args: argparse.Namespace) -> dict[str, Any]:
    connection = await asyncpg.connect(args.database_url)
    try:
        return await create_supersessions(
            connection,
            audit_run_id=uuid.UUID(args.audit_run_id),
            import_run_ids=[uuid.UUID(value) for value in args.import_run_id],
            justification=args.justification,
            dry_run=args.dry_run,
        )
    finally:
        await connection.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--audit-run-id", required=True)
    parser.add_argument("--import-run-id", action="append", required=True)
    parser.add_argument("--justification", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    print(json.dumps(asyncio.run(_main(_parser().parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
