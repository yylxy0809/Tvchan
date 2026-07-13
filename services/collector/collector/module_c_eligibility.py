from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import tempfile
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import asyncpg


TIMEFRAMES = ("5f", "30f", "1d", "1w", "1m")
TIMEFRAME_CODES = {"5f": 5, "30f": 30, "1d": 1440, "1w": 10080, "1m": 43200}
CODE_TO_TIMEFRAME = {value: key for key, value in TIMEFRAME_CODES.items()}
DERIVED_TIMEFRAMES = {"1w", "1m"}


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


def evaluate_dispositions(
    symbols: Iterable[Symbol],
    coverage: Mapping[tuple[int, str], datetime],
    unresolved: Mapping[tuple[str, str], int],
    missing_source: Mapping[tuple[str, str], int],
    canonical_dispositions: Mapping[tuple[int, str], str] | None = None,
) -> list[Disposition]:
    canonical_dispositions = canonical_dispositions or {}
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
            if canonical_dispositions and canonical_dispositions.get(
                (symbol.symbol_id, timeframe)
            ) != "eligible":
                reasons.append("canonical_gate_unresolved")
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


async def _load_inputs(connection: asyncpg.Connection, audit_run_id: str | None = None) -> tuple[
    list[Symbol], dict[tuple[int, str], datetime], dict[tuple[str, str], int],
    dict[tuple[str, str], int], dict[tuple[int, str], str], str | None,
]:
    symbol_rows = await connection.fetch(
        "SELECT id, code, exchange FROM symbols WHERE is_active = TRUE ORDER BY id"
    )
    symbols = [Symbol(int(row["id"]), str(row["code"]), str(row["exchange"])) for row in symbol_rows]

    coverage_rows = await connection.fetch(
        "SELECT symbol_id, timeframe, max(last_bar_end) AS covered_until "
        "FROM scheme2_ingest_watermarks WHERE timeframe = ANY($1::integer[]) "
        "GROUP BY symbol_id, timeframe",
        list(TIMEFRAME_CODES.values()),
    )
    coverage = {
        (int(row["symbol_id"]), CODE_TO_TIMEFRAME[int(row["timeframe"])]): row["covered_until"]
        for row in coverage_rows
    }
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
    if audit_run_id is None:
        audit_run_id = await connection.fetchval(
            "SELECT audit_run_id::text FROM kline_audit_runs "
            "WHERE status = 'completed' ORDER BY completed_at DESC LIMIT 1"
        )
    canonical_dispositions: dict[tuple[int, str], str] = {}
    if audit_run_id is not None:
        audit_rows = await connection.fetch(
            "SELECT symbol_id, timeframe, metadata->>'disposition' AS disposition "
            "FROM kline_audit_checkpoints WHERE audit_run_id = $1::uuid",
            audit_run_id,
        )
        canonical_dispositions = {
            (int(row["symbol_id"]), CODE_TO_TIMEFRAME[int(row["timeframe"])]):
                str(row["disposition"] or "unresolved")
            for row in audit_rows
            if int(row["timeframe"]) in CODE_TO_TIMEFRAME
        }
    return symbols, coverage, unresolved, missing_source, canonical_dispositions, audit_run_id


async def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    connection = await asyncpg.connect(args.database_url)
    try:
        symbols, coverage, unresolved, missing_source, canonical_dispositions, audit_run_id = (
            await _load_inputs(connection, args.audit_run_id)
        )
        rows = evaluate_dispositions(
            symbols, coverage, unresolved, missing_source, canonical_dispositions
        )
        active_hash = _stable_hash(symbol.name for symbol in symbols)
        manifest_hash = _stable_hash(row.json_record() for row in rows)
        summary = build_summary(rows)
        build_id = uuid.UUID(args.build_id) if args.build_id else uuid.uuid4()
        metadata = {
            "build_id": str(build_id),
            "manifest_version": args.manifest_version,
            "config_hash": args.config_hash,
            "active_universe_hash": active_hash,
            "manifest_hash": manifest_hash,
            "active_symbols": len(symbols),
            "canonical_audit_run_id": audit_run_id,
            "excluded_summary": {
                "excluded_scopes": sum(not row.eligible for row in rows),
                "reasons": dict(sorted(Counter(
                    reason for row in rows for reason in row.reasons
                ).items())),
            },
            **summary,
        }
        if not args.dry_run:
            async with connection.transaction():
                await connection.execute(
                    "INSERT INTO module_c_eligibility_builds "
                    "(build_id, manifest_version, config_hash, active_universe_hash, manifest_hash, active_symbols, disposition_rows, parameters, summary) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
                    build_id, args.manifest_version, args.config_hash, active_hash, manifest_hash,
                    len(symbols), len(rows), json.dumps({"policy": "strict-v1"}), json.dumps(summary),
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
    parser.add_argument("--audit-run-id")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    metadata = asyncio.run(build_manifest(_parser().parse_args()))
    print(json.dumps(metadata, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
