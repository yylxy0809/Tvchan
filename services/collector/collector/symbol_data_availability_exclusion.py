"""Permanently deactivate symbols with audit-proven empty required scopes.

The operation is append-only and never deletes K-lines, runs, heads, or audit
evidence. A symbol is eligible for exclusion only when the same strict-v2
audit checkpoint and its pinned catalog generation both prove an empty scope.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import asyncpg

from collector.kline_sql_gate import _manifest_sha256


AUDIT_CONTRACT_VERSION = "module-c-strict-audit-v2"
REQUIRED_TIMEFRAMES = (5, 30, 1440, 10080, 43200)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class ExclusionCandidate:
    symbol_id: int
    symbol: str
    unavailable_timeframes: tuple[int, ...]


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def validate_audit(
    audit: Mapping[str, Any],
) -> tuple[str, str, uuid.UUID, int, str]:
    parameters = _json_object(audit.get("parameters"))
    summary = _json_object(audit.get("summary"))
    if audit.get("status") != "completed" or bool(audit.get("apply_mode")):
        raise ValueError("canonical audit must be completed and read-only")
    if parameters.get("contract_version") != AUDIT_CONTRACT_VERSION:
        raise ValueError("canonical audit contract is not strict-v2")
    if parameters.get("timeframes") != list(REQUIRED_TIMEFRAMES):
        raise ValueError("canonical audit does not bind the exact five levels")
    evidence_sha256 = str(summary.get("evidence_sha256") or "")
    active_universe_sha256 = str(parameters.get("active_universe_sha256") or "")
    catalog_manifest_sha256 = str(parameters.get("catalog_manifest_sha256") or "")
    catalog_control_revision_raw = parameters.get("catalog_control_revision")
    if summary.get("evidence_complete") is not True or not _SHA256_RE.fullmatch(
        evidence_sha256
    ):
        raise ValueError("canonical audit evidence is incomplete")
    if not _SHA256_RE.fullmatch(active_universe_sha256):
        raise ValueError("canonical audit active universe hash is invalid")
    if not _SHA256_RE.fullmatch(catalog_manifest_sha256):
        raise ValueError("canonical audit catalog manifest hash is invalid")
    try:
        catalog_generation_id = uuid.UUID(str(parameters["catalog_generation_id"]))
        catalog_control_revision = int(catalog_control_revision_raw)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("canonical audit catalog binding is invalid") from error
    if catalog_control_revision < 0:
        raise ValueError("canonical audit catalog revision is invalid")
    return (
        evidence_sha256,
        active_universe_sha256,
        catalog_generation_id,
        catalog_control_revision,
        catalog_manifest_sha256,
    )


def build_candidates(
    *,
    symbols: Iterable[Mapping[str, Any]],
    checkpoints: Iterable[Mapping[str, Any]],
    catalog_rows: Iterable[Mapping[str, Any]],
) -> list[ExclusionCandidate]:
    symbol_rows = list(symbols)
    checkpoint_rows = list(checkpoints)
    catalog_rows = list(catalog_rows)
    expected = {
        (int(symbol["symbol_id"]), timeframe)
        for symbol in symbol_rows
        for timeframe in REQUIRED_TIMEFRAMES
    }
    checkpoint_by_scope = {
        (int(row["symbol_id"]), int(row["timeframe"])): row
        for row in checkpoint_rows
    }
    catalog_by_scope = {
        (int(row["symbol_id"]), int(row["timeframe"])): row for row in catalog_rows
    }
    if (
        len(checkpoint_by_scope) != len(checkpoint_rows)
        or len(catalog_by_scope) != len(catalog_rows)
        or set(checkpoint_by_scope) != expected
        or set(catalog_by_scope) != expected
    ):
        raise ValueError("audit and catalog must contain the exact active five-level universe")

    candidates: list[ExclusionCandidate] = []
    for symbol in symbol_rows:
        symbol_id = int(symbol["symbol_id"])
        unavailable: list[int] = []
        for timeframe in REQUIRED_TIMEFRAMES:
            checkpoint = checkpoint_by_scope[(symbol_id, timeframe)]
            catalog = catalog_by_scope[(symbol_id, timeframe)]
            rows_scanned = int(checkpoint.get("rows_scanned") or 0)
            metadata = _json_object(checkpoint.get("metadata"))
            if checkpoint.get("status") != "completed":
                raise ValueError("canonical audit checkpoint is not completed")
            if rows_scanned == 0:
                if (
                    metadata.get("disposition") != "unresolved"
                    or catalog.get("state") != "empty"
                    or catalog.get("bounds_complete") is not True
                    or catalog.get("min_ts") is not None
                    or catalog.get("max_ts") is not None
                ):
                    raise ValueError("empty audit scope is not confirmed by the pinned catalog")
                unavailable.append(timeframe)
            elif rows_scanned < 0 or catalog.get("state") != "present":
                raise ValueError("present audit scope is inconsistent with the pinned catalog")
        if unavailable:
            candidates.append(
                ExclusionCandidate(
                    symbol_id=symbol_id,
                    symbol=str(symbol["symbol"]),
                    unavailable_timeframes=tuple(unavailable),
                )
            )
    return candidates


def candidate_manifest_sha256(candidates: Sequence[ExclusionCandidate]) -> str:
    payload = [
        {
            "symbol_id": row.symbol_id,
            "symbol": row.symbol,
            "unavailable_timeframes": list(row.unavailable_timeframes),
        }
        for row in candidates
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def active_universe_manifest_sha256(symbols: Iterable[Mapping[str, Any]]) -> str:
    """Use the strict-v2 producer's canonical ordered manifest digest."""
    return _manifest_sha256(
        [
            {"symbol_id": int(row["symbol_id"]), "symbol": str(row["symbol"])}
            for row in symbols
        ]
    )


async def exclude_unavailable_symbols(
    connection: asyncpg.Connection,
    *,
    audit_run_id: uuid.UUID,
    justification: str,
    dry_run: bool,
) -> dict[str, Any]:
    justification = justification.strip()
    if not justification:
        raise ValueError("justification must be non-empty")
    async with connection.transaction(isolation="serializable"):
        audit = await connection.fetchrow(
            "SELECT status,apply_mode,parameters,summary FROM kline_audit_runs "
            "WHERE audit_run_id=$1 FOR SHARE",
            audit_run_id,
        )
        if audit is None:
            raise ValueError("canonical audit run not found")
        (
            evidence_sha256,
            active_universe_sha256,
            generation_id,
            catalog_control_revision,
            catalog_manifest_sha256,
        ) = validate_audit(dict(audit))
        live_catalog = await connection.fetchrow(
            """
            SELECT control.active_generation_id AS generation_id,
                   control.revision,generation.status
            FROM kline_scope_catalog_control control
            JOIN kline_scope_catalog_generations generation
              ON generation.generation_id=control.active_generation_id
            WHERE control.control_key='active'
            FOR SHARE OF control,generation
            """
        )
        if (
            live_catalog is None
            or live_catalog["generation_id"] != generation_id
            or int(live_catalog["revision"]) != catalog_control_revision
            or live_catalog["status"] != "complete"
        ):
            raise ValueError("active catalog binding drifted from the canonical audit")
        lock_clause = "FOR SHARE" if dry_run else "FOR UPDATE"
        symbols = await connection.fetch(
            "SELECT id AS symbol_id,code || '.' || upper(exchange) AS symbol "
            "FROM symbols WHERE is_active=true AND market='A_SHARE' ORDER BY id "
            + lock_clause
        )
        parameters = _json_object(audit["parameters"])
        if len(symbols) != int(parameters.get("active_universe_count") or 0):
            raise ValueError("active universe count drifted from the canonical audit")
        if active_universe_manifest_sha256(symbols) != active_universe_sha256:
            raise ValueError("active universe identity drifted from the canonical audit")
        checkpoints = await connection.fetch(
            "SELECT symbol_id,timeframe,status,rows_scanned,metadata "
            "FROM kline_audit_checkpoints WHERE audit_run_id=$1 "
            "ORDER BY symbol_id,timeframe",
            audit_run_id,
        )
        catalog_rows = await connection.fetch(
            "SELECT symbol_id,timeframe,state,bounds_complete,min_ts,max_ts,updated_at "
            "FROM kline_scope_catalog WHERE generation_id=$1 "
            "AND symbol_id=ANY($2::integer[]) AND timeframe=ANY($3::integer[]) "
            "ORDER BY symbol_id,timeframe",
            generation_id,
            [int(row["symbol_id"]) for row in symbols],
            list(REQUIRED_TIMEFRAMES),
        )
        catalog_manifest = [
            {
                "symbol_id": int(row["symbol_id"]),
                "timeframe": int(row["timeframe"]),
                "state": str(row["state"]),
                "bounds_complete": bool(row["bounds_complete"]),
                "min_ts": row["min_ts"],
                "max_ts": row["max_ts"],
                "updated_at": row["updated_at"],
            }
            for row in catalog_rows
        ]
        if _manifest_sha256(catalog_manifest) != catalog_manifest_sha256:
            raise ValueError("active catalog manifest drifted from the canonical audit")
        candidates = build_candidates(
            symbols=symbols, checkpoints=checkpoints, catalog_rows=catalog_rows
        )
        if not candidates:
            raise ValueError("canonical audit contains no audit-proven empty symbols")
        manifest_sha256 = candidate_manifest_sha256(candidates)
        exclusion_run_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"tvchan:symbol-data-availability:{audit_run_id}:{manifest_sha256}",
        )
        if not dry_run:
            await connection.execute(
                """
                INSERT INTO symbol_data_availability_exclusion_runs (
                    exclusion_run_id,canonical_audit_run_id,audit_evidence_sha256,
                    audit_active_universe_sha256,manifest_sha256,required_timeframes,
                    excluded_symbols,justification
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                exclusion_run_id,
                audit_run_id,
                evidence_sha256,
                active_universe_sha256,
                manifest_sha256,
                list(REQUIRED_TIMEFRAMES),
                len(candidates),
                justification,
            )
            await connection.executemany(
                """
                INSERT INTO symbol_data_availability_exclusions (
                    exclusion_run_id,symbol_id,symbol,unavailable_timeframes
                ) VALUES($1,$2,$3,$4)
                """,
                [
                    (
                        exclusion_run_id,
                        row.symbol_id,
                        row.symbol,
                        list(row.unavailable_timeframes),
                    )
                    for row in candidates
                ],
            )
            result = await connection.execute(
                "UPDATE symbols SET is_active=false,updated_at=clock_timestamp() "
                "WHERE id=ANY($1::integer[]) AND is_active=true",
                [row.symbol_id for row in candidates],
            )
            if int(result.rsplit(" ", 1)[-1]) != len(candidates):
                raise RuntimeError("symbol exclusion update count drifted")
        return {
            "exclusion_run_id": str(exclusion_run_id),
            "canonical_audit_run_id": str(audit_run_id),
            "audit_evidence_sha256": evidence_sha256,
            "audit_active_universe_sha256": active_universe_sha256,
            "excluded_symbols": len(candidates),
            "unavailable_scope_count": sum(
                len(row.unavailable_timeframes) for row in candidates
            ),
            "manifest_sha256": manifest_sha256,
            "dry_run": dry_run,
        }


async def _main(args: argparse.Namespace) -> dict[str, Any]:
    connection = await asyncpg.connect(args.database_url)
    try:
        return await exclude_unavailable_symbols(
            connection,
            audit_run_id=uuid.UUID(args.audit_run_id),
            justification=args.justification,
            dry_run=args.dry_run,
        )
    finally:
        await connection.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--audit-run-id", required=True)
    parser.add_argument("--justification", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    print(json.dumps(asyncio.run(_main(_parser().parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
