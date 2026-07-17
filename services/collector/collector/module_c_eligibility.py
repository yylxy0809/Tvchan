from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import tempfile
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import asyncpg

from collector.kline_sql_gate import (
    ACTIVE_CATALOG_GENERATION_SQL,
    ACTIVE_CATALOG_MANIFEST_SQL,
    ANOMALY_FIELDS,
    _json_value,
    _manifest_sha256,
)


TIMEFRAMES = ("5f", "30f", "1d", "1w", "1m")
TIMEFRAME_CODES = {"5f": 5, "30f": 30, "1d": 1440, "1w": 10080, "1m": 43200}
CODE_TO_TIMEFRAME = {value: key for key, value in TIMEFRAME_CODES.items()}
DERIVED_TIMEFRAMES = {"1w", "1m"}
FRESHNESS_CONTRACT_VERSION = "module-c-authoritative-freshness-v1"
AUDIT_CONTRACT_VERSION = "module-c-strict-audit-v2"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class FreshnessContract:
    contract_version: str
    as_of: datetime
    trading_calendar_id: str
    trading_calendar_sha256: str
    expected_closed_watermarks: dict[str, datetime]
    normalized: dict[str, Any]
    sha256: str


@dataclass(frozen=True)
class StrictInputs:
    symbols: list[Symbol]
    coverage: dict[tuple[int, str], datetime]
    canonical_dispositions: dict[tuple[int, str], str]
    freshness_reasons: dict[tuple[int, str], str]
    audit_evidence_sha256: str
    audit_checkpoint_sha256: str
    audit_active_universe_sha256: str
    catalog_generation_id: uuid.UUID
    catalog_control_revision: int
    catalog_manifest_sha256: str


@dataclass(frozen=True)
class Symbol:
    symbol_id: int
    code: str
    exchange: str

    @property
    def name(self) -> str:
        return f"{self.code}.{self.exchange.upper()}"


@dataclass(frozen=True)
class Disposition:
    symbol_id: int
    symbol: str
    timeframe: str
    timeframe_code: int
    eligible: bool
    reasons: tuple[str, ...]
    covered_until: datetime | None
    unresolved_rows: int

    def json_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["reasons"] = list(self.reasons)
        record["covered_until"] = (
            self.covered_until.isoformat() if self.covered_until is not None else None
        )
        return record


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _parse_aware_timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an RFC3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{label} must be an RFC3339 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def _canonical_sha256(record: Mapping[str, Any]) -> str:
    payload = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload + b"\n").hexdigest()


def parse_freshness_contract(payload: Mapping[str, Any]) -> FreshnessContract:
    if not isinstance(payload, Mapping) or set(payload) != {
        "contract_version",
        "as_of",
        "trading_calendar",
        "expected_closed_watermarks",
    }:
        raise ValueError("authoritative freshness contract must use the exact schema")
    if payload["contract_version"] != FRESHNESS_CONTRACT_VERSION:
        raise ValueError("unsupported authoritative freshness contract_version")
    calendar = payload["trading_calendar"]
    if not isinstance(calendar, dict) or set(calendar) != {"id", "sha256"}:
        raise ValueError("trading_calendar must use the exact id/sha256 schema")
    calendar_id = calendar["id"]
    calendar_sha256 = calendar["sha256"]
    if not isinstance(calendar_id, str) or not calendar_id.strip():
        raise ValueError("trading_calendar.id must be non-empty")
    if not isinstance(calendar_sha256, str) or not _SHA256_RE.fullmatch(calendar_sha256):
        raise ValueError("trading_calendar.sha256 must be lowercase SHA-256")
    raw_watermarks = payload["expected_closed_watermarks"]
    if not isinstance(raw_watermarks, dict) or set(raw_watermarks) != set(TIMEFRAMES):
        raise ValueError("expected_closed_watermarks must contain the exact five levels")
    as_of = _parse_aware_timestamp(payload["as_of"], "as_of")
    watermarks = {
        timeframe: _parse_aware_timestamp(
            raw_watermarks[timeframe],
            f"expected_closed_watermarks.{timeframe}",
        )
        for timeframe in TIMEFRAMES
    }
    if any(watermark > as_of for watermark in watermarks.values()):
        raise ValueError("expected closed watermark cannot be after as_of")
    normalized = {
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": _utc_text(as_of),
        "trading_calendar": {
            "id": calendar_id.strip(),
            "sha256": calendar_sha256,
        },
        "expected_closed_watermarks": {
            timeframe: _utc_text(watermarks[timeframe]) for timeframe in TIMEFRAMES
        },
    }
    return FreshnessContract(
        contract_version=FRESHNESS_CONTRACT_VERSION,
        as_of=as_of,
        trading_calendar_id=calendar_id.strip(),
        trading_calendar_sha256=calendar_sha256,
        expected_closed_watermarks=watermarks,
        normalized=normalized,
        sha256=_canonical_sha256(normalized),
    )


def load_freshness_contract(path: Path) -> FreshnessContract:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("authoritative freshness contract must be a JSON object")
    return parse_freshness_contract(payload)


def evaluate_dispositions(
    symbols: Iterable[Symbol],
    coverage: Mapping[tuple[int, str], datetime],
    unresolved: Mapping[tuple[str, str], int],
    missing_source: Mapping[tuple[str, str], int],
    canonical_dispositions: Mapping[tuple[int, str], str] | None = None,
    freshness_reasons: Mapping[tuple[int, str], str] | None = None,
) -> list[Disposition]:
    canonical_required = canonical_dispositions is not None
    canonical_dispositions = canonical_dispositions or {}
    freshness_reasons = freshness_reasons or {}
    rows: list[Disposition] = []
    for symbol in sorted(symbols, key=lambda item: item.symbol_id):
        daily_unresolved = unresolved.get((symbol.name, "1d"), 0)
        for timeframe in TIMEFRAMES:
            reasons: list[str] = []
            direct_unresolved = unresolved.get((symbol.name, timeframe), 0)
            direct_missing = missing_source.get((symbol.name, timeframe), 0)
            covered_until = coverage.get((symbol.symbol_id, timeframe))

            if symbol.exchange.upper() == "BJ" and timeframe == "30f":
                reasons.append("bj_30f_excluded")
            if direct_unresolved:
                reasons.append("unresolved_ambiguous_volume_unit")
            if timeframe in DERIVED_TIMEFRAMES and daily_unresolved:
                reasons.append("daily_unresolved_propagated")
            if direct_missing:
                reasons.append("missing_source_file")
            if canonical_required and canonical_dispositions.get(
                (symbol.symbol_id, timeframe)
            ) != "eligible":
                reasons.append("canonical_gate_unresolved")
            freshness_reason = freshness_reasons.get((symbol.symbol_id, timeframe))
            if freshness_reason:
                reasons.append(freshness_reason)
            if covered_until is None:
                reasons.append("missing_ingest_watermark")

            unresolved_rows = direct_unresolved
            if timeframe in DERIVED_TIMEFRAMES:
                unresolved_rows += daily_unresolved
            rows.append(
                Disposition(
                    symbol_id=symbol.symbol_id,
                    symbol=symbol.name,
                    timeframe=timeframe,
                    timeframe_code=TIMEFRAME_CODES[timeframe],
                    eligible=not reasons,
                    reasons=tuple(reasons),
                    covered_until=covered_until,
                    unresolved_rows=unresolved_rows,
                )
            )
    return rows


def build_summary(rows: Iterable[Disposition]) -> dict[str, Any]:
    rows = list(rows)
    by_timeframe: dict[str, dict[str, Any]] = {}
    for timeframe in TIMEFRAMES:
        selected = [row for row in rows if row.timeframe == timeframe]
        reason_counts = Counter(reason for row in selected for reason in row.reasons)
        by_timeframe[timeframe] = {
            "total": len(selected),
            "eligible": sum(row.eligible for row in selected),
            "excluded": sum(not row.eligible for row in selected),
            "reasons": dict(sorted(reason_counts.items())),
        }
    return {"rows": len(rows), "by_timeframe": by_timeframe}


def _stable_hash(records: Iterable[object]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _write_outputs(
    output_dir: Path,
    rows: list[Disposition],
    metadata: Mapping[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "jsonl": output_dir / "module_c_eligible_universe.jsonl",
        "contract_jsonl": output_dir / "module_c_eligibility.jsonl",
        "csv": output_dir / "module_c_eligible_universe.csv",
        "summary": output_dir / "module_c_eligibility_summary.json",
        "excluded_summary": output_dir / "excluded_summary.json",
        "excluded_markdown": output_dir / "excluded_summary.md",
    }
    temporary: dict[str, Path] = {}
    try:
        for key, target in paths.items():
            handle = tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", newline="", delete=False, dir=output_dir,
                prefix=f".{target.name}.", suffix=".tmp",
            )
            temporary[key] = Path(handle.name)
            with handle:
                if key in {"jsonl", "contract_jsonl"}:
                    for row in rows:
                        handle.write(json.dumps(row.json_record(), ensure_ascii=False, sort_keys=True) + "\n")
                elif key == "csv":
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=("symbol_id", "symbol", "timeframe", "timeframe_code", "eligible", "reasons", "covered_until", "unresolved_rows"),
                    )
                    writer.writeheader()
                    for row in rows:
                        record = row.json_record()
                        record["reasons"] = ";".join(record["reasons"])
                        writer.writerow(record)
                elif key == "summary":
                    json.dump(metadata, handle, ensure_ascii=False, indent=2, sort_keys=True)
                    handle.write("\n")
                elif key == "excluded_summary":
                    json.dump(metadata["excluded_summary"], handle, ensure_ascii=False, indent=2, sort_keys=True)
                    handle.write("\n")
                else:
                    excluded = metadata["excluded_summary"]
                    handle.write("# Module C excluded scopes\n\n")
                    handle.write(f"Excluded scopes: {excluded['excluded_scopes']}\n\n")
                    handle.write("| Reason | Scopes |\n|---|---:|\n")
                    for reason, count in excluded["reasons"].items():
                        handle.write(f"| {reason} | {count} |\n")
        for key, target in paths.items():
            os.replace(temporary[key], target)
    finally:
        for path in temporary.values():
            path.unlink(missing_ok=True)


def _semantic_bound(value: Any, *, empty: bool, label: str) -> datetime | None:
    sentinel = (
        value is None
        or str(value).lower() == "-infinity"
        or (isinstance(value, datetime) and value.year == datetime.min.year)
    )
    if empty:
        if not sentinel:
            raise RuntimeError(f"empty audit checkpoint has a finite {label}")
        return None
    if sentinel or not isinstance(value, datetime):
        raise RuntimeError(f"non-empty audit checkpoint is missing {label}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise RuntimeError(f"audit checkpoint {label} must be timezone-aware")
    return value.astimezone(timezone.utc)


async def _verify_active_catalog(
    connection: asyncpg.Connection,
    symbols: list[Symbol],
    *,
    generation_id: uuid.UUID,
    control_revision: int,
    expected_scope_count: int,
    manifest_sha256: str,
) -> None:
    generation = await connection.fetchrow(ACTIVE_CATALOG_GENERATION_SQL)
    if generation is None:
        raise RuntimeError("active complete K-line scope catalog generation is missing")
    symbol_ids = [symbol.symbol_id for symbol in symbols]
    if (
        uuid.UUID(str(generation["generation_id"])) != generation_id
        or int(generation["revision"]) != control_revision
        or int(generation["expected_scope_count"]) != expected_scope_count
        or not set(symbol_ids).issubset({int(value) for value in generation["symbol_ids"]})
        or not set(TIMEFRAME_CODES.values()).issubset(
            {int(value) for value in generation["timeframes"]}
        )
    ):
        raise RuntimeError("active K-line scope catalog no longer matches audit evidence")
    catalog_rows = await connection.fetch(
        ACTIVE_CATALOG_MANIFEST_SQL,
        generation_id,
        symbol_ids,
        list(TIMEFRAME_CODES.values()),
    )
    catalog: list[dict[str, Any]] = []
    for row in catalog_rows:
        state = str(row["state"])
        min_ts = row["min_ts"]
        max_ts = row["max_ts"]
        if not bool(row["bounds_complete"]) or not (
            (state == "empty" and min_ts is None and max_ts is None)
            or (
                state == "present"
                and min_ts is not None
                and max_ts is not None
                and min_ts <= max_ts
            )
        ):
            raise RuntimeError("active K-line scope catalog contains invalid evidence")
        catalog.append({
            "symbol_id": int(row["symbol_id"]),
            "timeframe": int(row["timeframe"]),
            "state": state,
            "bounds_complete": True,
            "min_ts": _json_value(min_ts),
            "max_ts": _json_value(max_ts),
            "updated_at": _json_value(row["updated_at"]),
        })
    expected_keys = {
        (symbol_id, timeframe)
        for symbol_id in symbol_ids
        for timeframe in TIMEFRAME_CODES.values()
    }
    observed_keys = {
        (record["symbol_id"], record["timeframe"])
        for record in catalog
    }
    if (
        len(catalog) != len(expected_keys)
        or observed_keys != expected_keys
        or _manifest_sha256(catalog) != manifest_sha256
    ):
        raise RuntimeError("active K-line scope catalog manifest does not match audit evidence")


async def _load_strict_inputs(
    connection: asyncpg.Connection,
    audit_run_id: str,
    freshness: FreshnessContract,
) -> StrictInputs:
    audit = await connection.fetchrow(
        "SELECT status,apply_mode,parameters,summary FROM kline_audit_runs "
        "WHERE audit_run_id=$1::uuid FOR SHARE",
        audit_run_id,
    )
    if audit is None or audit["status"] != "completed" or bool(audit["apply_mode"]):
        raise RuntimeError("strict-v2 requires a completed read-only canonical audit")
    parameters = _json_object(audit["parameters"])
    summary = _json_object(audit["summary"])
    if parameters.get("contract_version") != AUDIT_CONTRACT_VERSION:
        raise RuntimeError("canonical audit is not strict producer v2 evidence")
    if summary.get("evidence_complete") is not True:
        raise RuntimeError("canonical audit evidence_complete is not true")
    evidence_sha256 = str(parameters.get("evidence_sha256") or "")
    unsigned_evidence = dict(parameters)
    unsigned_evidence.pop("evidence_sha256", None)
    if (
        not _SHA256_RE.fullmatch(evidence_sha256)
        or _manifest_sha256([unsigned_evidence]) != evidence_sha256
        or summary.get("evidence_sha256") != evidence_sha256
    ):
        raise RuntimeError("canonical audit evidence_sha256 is invalid")
    if parameters.get("timeframes") != list(TIMEFRAME_CODES.values()):
        raise RuntimeError("canonical audit does not bind the exact five levels")
    active_count = int(parameters.get("active_universe_count") or 0)
    active_sha256 = str(parameters.get("active_universe_sha256") or "")
    catalog_sha256 = str(parameters.get("catalog_manifest_sha256") or "")
    try:
        catalog_generation_id = uuid.UUID(str(parameters["catalog_generation_id"]))
        catalog_control_revision = int(parameters["catalog_control_revision"])
        catalog_expected = int(parameters["catalog_expected_scope_count"])
        catalog_required = int(parameters["catalog_required_scope_count"])
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("canonical audit catalog provenance is incomplete") from error
    if (
        active_count <= 0
        or catalog_control_revision < 0
        or catalog_required != active_count * len(TIMEFRAMES)
        or catalog_expected < catalog_required
        or not _SHA256_RE.fullmatch(active_sha256)
        or not _SHA256_RE.fullmatch(catalog_sha256)
    ):
        raise RuntimeError("canonical audit catalog provenance is invalid")

    symbol_rows = await connection.fetch(
        "SELECT id AS symbol_id,code,exchange FROM symbols "
        "WHERE is_active=TRUE AND market='A_SHARE' ORDER BY id"
    )
    symbols = [
        Symbol(int(row["symbol_id"]), str(row["code"]), str(row["exchange"]))
        for row in symbol_rows
    ]
    universe_manifest = [
        {"symbol_id": symbol.symbol_id, "symbol": symbol.name}
        for symbol in symbols
    ]
    if len(symbols) != active_count or _manifest_sha256(universe_manifest) != active_sha256:
        raise RuntimeError("active universe does not match canonical audit evidence")
    await _verify_active_catalog(
        connection,
        symbols,
        generation_id=catalog_generation_id,
        control_revision=catalog_control_revision,
        expected_scope_count=catalog_expected,
        manifest_sha256=catalog_sha256,
    )

    checkpoint_rows = await connection.fetch(
        "SELECT symbol_id,timeframe,status,shard_start,shard_end,rows_scanned,"
        "metadata FROM kline_audit_checkpoints "
        "WHERE audit_run_id=$1::uuid ORDER BY symbol_id,timeframe,shard_start,shard_end",
        audit_run_id,
    )
    expected_keys = {
        (symbol.symbol_id, timeframe)
        for symbol in symbols
        for timeframe in TIMEFRAME_CODES.values()
    }
    observed_keys: set[tuple[int, int]] = set()
    coverage: dict[tuple[int, str], datetime] = {}
    canonical_dispositions: dict[tuple[int, str], str] = {}
    freshness_reasons: dict[tuple[int, str], str] = {}
    checkpoint_manifest: list[dict[str, Any]] = []
    anomaly_aggregates = Counter({field: 0 for field in ANOMALY_FIELDS})
    rows_scanned_total = 0
    for row in checkpoint_rows:
        symbol_id = int(row["symbol_id"])
        timeframe_code = int(row["timeframe"])
        key = (symbol_id, timeframe_code)
        if key in observed_keys or key not in expected_keys:
            raise RuntimeError("canonical audit lacks an exact five-level checkpoint set")
        observed_keys.add(key)
        if row["status"] != "completed" or timeframe_code not in CODE_TO_TIMEFRAME:
            raise RuntimeError("canonical audit checkpoint is not completed")
        timeframe = CODE_TO_TIMEFRAME[timeframe_code]
        metadata = _json_object(row["metadata"])
        anomalies: dict[str, int] = {}
        for field in ANOMALY_FIELDS:
            value = metadata.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RuntimeError(
                    f"canonical audit checkpoint {field} is not a non-negative integer"
                )
            anomalies[field] = value
            anomaly_aggregates[field] += value
        anomaly_total = sum(anomalies.values())
        disposition = str(metadata.get("disposition") or "")
        expected_disposition = "eligible" if anomaly_total == 0 else "unresolved"
        if disposition != expected_disposition:
            raise RuntimeError("canonical audit checkpoint disposition is inconsistent")
        rows_scanned = int(row["rows_scanned"])
        if rows_scanned < 0:
            raise RuntimeError("canonical audit checkpoint rows_scanned is negative")
        rows_scanned_total += rows_scanned
        empty = rows_scanned == 0
        shard_start = _semantic_bound(row["shard_start"], empty=empty, label="shard_start")
        shard_end = _semantic_bound(row["shard_end"], empty=empty, label="shard_end")
        if shard_start is not None and shard_end is not None and shard_start > shard_end:
            raise RuntimeError("canonical audit checkpoint bounds are inverted")
        expected = freshness.expected_closed_watermarks[timeframe]
        if shard_end is not None:
            if shard_end > expected:
                raise RuntimeError(
                    f"canonical audit checkpoint {symbol_id}/{timeframe} exceeds authoritative watermark"
                )
            coverage[(symbol_id, timeframe)] = shard_end
            if shard_end < expected:
                freshness_reasons[(symbol_id, timeframe)] = "authoritative_freshness_stale"
        else:
            freshness_reasons[(symbol_id, timeframe)] = "authoritative_freshness_stale"
        canonical_dispositions[(symbol_id, timeframe)] = disposition
        checkpoint_manifest.append({
            "symbol_id": symbol_id,
            "timeframe": timeframe_code,
            "status": "completed",
            "actual_rows": rows_scanned,
            "actual_shard_start": _utc_text(shard_start) if shard_start else None,
            "actual_shard_end": _utc_text(shard_end) if shard_end else None,
            "disposition": disposition,
            "anomaly_total": anomaly_total,
            **anomalies,
        })
    if observed_keys != expected_keys or len(checkpoint_rows) != active_count * len(TIMEFRAMES):
        raise RuntimeError("canonical audit lacks an exact five-level checkpoint set")
    if int(summary.get("checkpoints") or 0) != len(checkpoint_rows):
        raise RuntimeError("canonical audit summary checkpoint count is inconsistent")
    disposition_counts = Counter(canonical_dispositions.values())
    def summary_integer(field: str) -> int:
        value = summary[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(field)
        return value

    try:
        summary_checkpoints = summary_integer("checkpoints")
        summary_rows_scanned = summary_integer("rows_scanned")
        summary_eligible = summary_integer("eligible")
        summary_unresolved = summary_integer("unresolved")
        summary_anomalies = {
            field: summary_integer(field) for field in ANOMALY_FIELDS
        }
        summary_anomaly_total = summary_integer("anomaly_total")
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("canonical audit summary is incomplete") from error
    if (
        summary_checkpoints != len(checkpoint_rows)
        or summary_rows_scanned != rows_scanned_total
        or summary_eligible != disposition_counts["eligible"]
        or summary_unresolved != disposition_counts["unresolved"]
        or summary_eligible + summary_unresolved != len(checkpoint_rows)
        or summary_anomalies != dict(anomaly_aggregates)
        or summary_anomaly_total != sum(anomaly_aggregates.values())
        or not isinstance(summary.get("gate_pass"), bool)
        or summary["gate_pass"] is not (summary_anomaly_total == 0)
    ):
        raise RuntimeError("canonical audit summary aggregates are inconsistent")

    return StrictInputs(
        symbols=symbols,
        coverage=coverage,
        canonical_dispositions=canonical_dispositions,
        freshness_reasons=freshness_reasons,
        audit_evidence_sha256=evidence_sha256,
        audit_checkpoint_sha256=_manifest_sha256(checkpoint_manifest),
        audit_active_universe_sha256=active_sha256,
        catalog_generation_id=catalog_generation_id,
        catalog_control_revision=catalog_control_revision,
        catalog_manifest_sha256=catalog_sha256,
    )


async def _load_quarantine_inputs(connection: asyncpg.Connection) -> tuple[
    dict[tuple[str, str], int], dict[tuple[str, str], int]
]:
    issue_rows = await connection.fetch(
        "SELECT upper(symbol_text) AS symbol, lower(timeframe) AS timeframe, reason, count(*) AS rows "
        "FROM kline_import_quarantine "
        "WHERE reason = ANY($1::text[]) AND symbol_text IS NOT NULL AND timeframe IS NOT NULL "
        "GROUP BY upper(symbol_text), lower(timeframe), reason",
        ["ambiguous_volume_unit", "missing_source_file"],
    )
    unresolved: dict[tuple[str, str], int] = {}
    missing_source: dict[tuple[str, str], int] = {}
    for row in issue_rows:
        target = unresolved if row["reason"] == "ambiguous_volume_unit" else missing_source
        key = (str(row["symbol"]), str(row["timeframe"]))
        target[key] = target.get(key, 0) + int(row["rows"])
    return unresolved, missing_source


async def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    if not args.audit_run_id:
        raise ValueError("strict-v2 requires explicit --audit-run-id")
    if not args.freshness_contract:
        raise ValueError("strict-v2 requires --freshness-contract")
    audit_run_id = str(uuid.UUID(str(args.audit_run_id)))
    freshness = load_freshness_contract(Path(args.freshness_contract))
    connection = await asyncpg.connect(args.database_url)
    try:
        async with connection.transaction(isolation="repeatable_read"):
            strict = await _load_strict_inputs(connection, audit_run_id, freshness)
            unresolved, missing_source = await _load_quarantine_inputs(connection)
            rows = evaluate_dispositions(
                strict.symbols,
                strict.coverage,
                unresolved,
                missing_source,
                strict.canonical_dispositions,
                strict.freshness_reasons,
            )
            active_hash = strict.audit_active_universe_sha256
            manifest_hash = _stable_hash(row.json_record() for row in rows)
            summary = build_summary(rows)
            build_id = uuid.UUID(args.build_id) if args.build_id else uuid.uuid4()
            parameters = {
                "policy": "strict-v2",
                "canonical_audit_run_id": audit_run_id,
                "audit_evidence_sha256": strict.audit_evidence_sha256,
                "audit_checkpoint_sha256": strict.audit_checkpoint_sha256,
                "freshness_contract": freshness.normalized,
                "freshness_contract_version": freshness.contract_version,
                "freshness_contract_sha256": freshness.sha256,
                "catalog_generation_id": str(strict.catalog_generation_id),
                "catalog_control_revision": strict.catalog_control_revision,
                "catalog_manifest_sha256": strict.catalog_manifest_sha256,
                "audit_active_universe_sha256": strict.audit_active_universe_sha256,
            }
            metadata = {
                "build_id": str(build_id),
                "manifest_version": args.manifest_version,
                "config_hash": args.config_hash,
                "active_universe_hash": active_hash,
                "manifest_hash": manifest_hash,
                "active_symbols": len(strict.symbols),
                **parameters,
                "excluded_summary": {
                    "excluded_scopes": sum(not row.eligible for row in rows),
                    "reasons": dict(sorted(Counter(
                        reason for row in rows for reason in row.reasons
                    ).items())),
                },
                **summary,
            }
            if not args.dry_run:
                await connection.execute(
                    "INSERT INTO module_c_eligibility_builds "
                    "(build_id,manifest_version,config_hash,active_universe_hash,manifest_hash,"
                    "active_symbols,disposition_rows,parameters,summary,canonical_audit_run_id,"
                    "audit_evidence_sha256,audit_checkpoint_sha256,freshness_contract_version,"
                    "freshness_contract_sha256,catalog_generation_id,catalog_control_revision,"
                    "catalog_manifest_sha256,audit_active_universe_sha256) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::uuid,$11,$12,"
                    "$13,$14,$15::uuid,$16,$17,$18)",
                    build_id, args.manifest_version, args.config_hash, active_hash, manifest_hash,
                    len(strict.symbols), len(rows), json.dumps(parameters, sort_keys=True),
                    json.dumps(summary, sort_keys=True), audit_run_id,
                    strict.audit_evidence_sha256, strict.audit_checkpoint_sha256,
                    freshness.contract_version, freshness.sha256,
                    strict.catalog_generation_id, strict.catalog_control_revision,
                    strict.catalog_manifest_sha256, strict.audit_active_universe_sha256,
                )
                await connection.copy_records_to_table(
                    "module_c_eligibility",
                    records=[(
                        build_id, row.symbol_id, row.symbol, row.timeframe_code, row.eligible,
                        list(row.reasons), row.covered_until, row.unresolved_rows,
                    ) for row in rows],
                    columns=("build_id", "symbol_id", "symbol", "timeframe", "eligible", "reasons", "covered_until", "unresolved_rows"),
                )
            _write_outputs(args.output_dir, rows, metadata)
        return metadata
    finally:
        await connection.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build strict five-level Module C eligibility")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--manifest-version", required=True)
    parser.add_argument("--config-hash", required=True)
    parser.add_argument("--build-id")
    parser.add_argument("--audit-run-id", required=True)
    parser.add_argument("--freshness-contract", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    metadata = asyncio.run(build_manifest(_parser().parse_args()))
    print(json.dumps(metadata, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
