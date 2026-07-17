from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import uuid
from collections import defaultdict
from fractions import Fraction
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from collector.module_c_eligibility import CODE_TO_TIMEFRAME, Disposition, _stable_hash
from trading_protocol import MODULE_C_CONFIG_HASH


CONTRACT_VERSION = "module-c-canary-selection-v2"
BARS_PER_COMPLETE_5F_SESSION = 49
ACTIVITY_BASIS = "pinned-audit-5f-rows-per-49-bar-1d-session-v1"
BOARD_ORDER = ("main_board", "chinext", "star", "bj")
BOARD_QUOTAS = {board: 5 for board in BOARD_ORDER}
BOUNDARY_COUNTS = {"lower": 2, "middle": 1, "upper": 2}
SOURCE_FIELDS = (
    "eligibility_build_id",
    "eligibility_manifest_sha256",
    "canonical_audit_run_id",
    "audit_evidence_sha256",
    "audit_checkpoint_sha256",
    "freshness_contract_version",
    "freshness_contract_sha256",
    "catalog_generation_id",
    "catalog_control_revision",
    "catalog_manifest_sha256",
    "audit_active_universe_sha256",
)
SHA_FIELDS = frozenset(
    {
        "eligibility_manifest_sha256",
        "audit_evidence_sha256",
        "audit_checkpoint_sha256",
        "freshness_contract_sha256",
        "catalog_manifest_sha256",
        "audit_active_universe_sha256",
    }
)


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload + b"\n").hexdigest()


def _board(symbol: str) -> str | None:
    code, separator, exchange = symbol.upper().partition(".")
    if not separator:
        return None
    if exchange == "BJ":
        return "bj"
    if exchange == "SH" and code.startswith(("688", "689")):
        return "star"
    if exchange == "SZ" and code.startswith(("300", "301")):
        return "chinext"
    if (
        exchange == "SH" and code.startswith(("600", "601", "603", "605"))
    ) or (
        exchange == "SZ" and code.startswith(("000", "001", "002", "003"))
    ):
        return "main_board"
    return None


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _normalized_source(source: Mapping[str, Any]) -> dict[str, Any]:
    if set(source) != set(SOURCE_FIELDS):
        raise ValueError("selection source must use the exact strict-v2 schema")
    normalized = {field: str(source[field]) for field in SOURCE_FIELDS}
    normalized["catalog_control_revision"] = _integer(
        source["catalog_control_revision"], "catalog_control_revision"
    )
    for field in SHA_FIELDS:
        value = normalized[field]
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError(f"selection source {field} must be lowercase SHA-256")
    for field in (
        "eligibility_build_id",
        "canonical_audit_run_id",
        "catalog_generation_id",
    ):
        try:
            normalized[field] = str(uuid.UUID(normalized[field]))
        except ValueError as error:
            raise ValueError(f"selection source {field} must be a UUID") from error
    if (
        normalized["freshness_contract_version"]
        != "module-c-authoritative-freshness-v1"
    ):
        raise ValueError("selection source freshness contract version is unsupported")
    return normalized


def _policy() -> dict[str, Any]:
    return {
        "symbol_count": 20,
        "board_quotas": BOARD_QUOTAS,
        "activity_boundary_counts_per_board": BOUNDARY_COUNTS,
        "activity_basis": ACTIVITY_BASIS,
        "bars_per_complete_5f_session": BARS_PER_COMPLETE_5F_SESSION,
        "legacy_free_text_scenario_traits": "not_authoritative_without_frozen_evidence",
        "tie_break": ["activity_ratio", "symbol_id", "symbol"],
    }


def _candidate_rows(
    dispositions: Sequence[Mapping[str, Any]],
    checkpoints: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    disposition_by_symbol: dict[int, dict[int, Mapping[str, Any]]] = defaultdict(dict)
    names: dict[int, str] = {}
    for row in dispositions:
        symbol_id = _integer(row.get("symbol_id"), "disposition symbol_id")
        timeframe = _integer(row.get("timeframe"), "disposition timeframe")
        symbol = str(row.get("symbol") or "").strip().upper()
        if timeframe not in CODE_TO_TIMEFRAME or not symbol or _board(symbol) is None:
            continue
        if timeframe in disposition_by_symbol[symbol_id]:
            raise ValueError("source build contains duplicate symbol/timeframe dispositions")
        if symbol_id in names and names[symbol_id] != symbol:
            raise ValueError("source build contains inconsistent canonical symbol identity")
        names[symbol_id] = symbol
        disposition_by_symbol[symbol_id][timeframe] = row

    checkpoint_by_symbol: dict[int, dict[int, Mapping[str, Any]]] = defaultdict(dict)
    for row in checkpoints:
        symbol_id = _integer(row.get("symbol_id"), "checkpoint symbol_id")
        timeframe = _integer(row.get("timeframe"), "checkpoint timeframe")
        if timeframe not in CODE_TO_TIMEFRAME:
            raise ValueError("canonical audit contains an unsupported timeframe")
        if timeframe in checkpoint_by_symbol[symbol_id]:
            raise ValueError("canonical audit contains duplicate symbol/timeframe checkpoints")
        if row.get("status") != "completed":
            raise ValueError("canonical audit checkpoint is not completed")
        _integer(row.get("rows_scanned"), "checkpoint rows_scanned")
        checkpoint_by_symbol[symbol_id][timeframe] = row

    candidates: list[dict[str, Any]] = []
    expected_levels = set(CODE_TO_TIMEFRAME)
    for symbol_id, levels in disposition_by_symbol.items():
        if set(levels) != expected_levels:
            raise ValueError("every selection candidate must have exactly five dispositions")
        checkpoints_for_symbol = checkpoint_by_symbol.get(symbol_id, {})
        if set(checkpoints_for_symbol) != expected_levels:
            raise ValueError("every selection candidate must have exactly five audit checkpoints")
        if not bool(levels[5].get("eligible")) or not bool(levels[1440].get("eligible")):
            continue
        five_minute_rows = _integer(
            checkpoints_for_symbol[5].get("rows_scanned"), "5f rows_scanned"
        )
        daily_rows = _integer(
            checkpoints_for_symbol[1440].get("rows_scanned"), "1d rows_scanned"
        )
        if daily_rows == 0:
            continue
        symbol = names[symbol_id]
        candidates.append(
            {
                "symbol_id": symbol_id,
                "symbol": symbol,
                "board": _board(symbol),
                "eligible_timeframes": [
                    CODE_TO_TIMEFRAME[level]
                    for level in sorted(levels)
                    if bool(levels[level].get("eligible"))
                ],
                "five_minute_rows": five_minute_rows,
                "daily_rows": daily_rows,
                "activity_ratio": Fraction(
                    five_minute_rows,
                    daily_rows * BARS_PER_COMPLETE_5F_SESSION,
                ),
            }
        )
    return candidates


def build_selection_manifest(
    *,
    source: Mapping[str, Any],
    dispositions: Sequence[Mapping[str, Any]],
    checkpoints: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_source = _normalized_source(source)
    by_board: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in _candidate_rows(dispositions, checkpoints):
        by_board[str(candidate["board"])].append(candidate)

    selected: list[dict[str, Any]] = []
    for board in BOARD_ORDER:
        ordered = sorted(
            by_board[board],
            key=lambda row: (
                row["activity_ratio"],
                row["symbol_id"],
                row["symbol"],
            ),
        )
        quota = BOARD_QUOTAS[board]
        if len(ordered) < quota:
            raise ValueError(
                f"selection board {board} has {len(ordered)} candidates; {quota} required"
            )
        picks = [
            ("lower", ordered[0]),
            ("lower", ordered[1]),
            ("middle", ordered[(len(ordered) - 1) // 2]),
            ("upper", ordered[-2]),
            ("upper", ordered[-1]),
        ]
        identities = {int(row["symbol_id"]) for _, row in picks}
        if len(identities) != quota:
            raise ValueError(f"selection board {board} cannot provide distinct boundary samples")
        for boundary, row in picks:
            selected.append(
                {
                    "symbol_id": row["symbol_id"],
                    "symbol": row["symbol"],
                    "board": board,
                    "activity_boundary": boundary,
                    "traits": [board, f"{boundary}_activity_boundary"],
                    "eligible_timeframes": row["eligible_timeframes"],
                    "evidence": {
                        "basis": ACTIVITY_BASIS,
                        "canonical_audit_run_id": normalized_source[
                            "canonical_audit_run_id"
                        ],
                        "five_minute_rows": row["five_minute_rows"],
                        "daily_rows": row["daily_rows"],
                        "activity_ratio_numerator": row["activity_ratio"].numerator,
                        "activity_ratio_denominator": row["activity_ratio"].denominator,
                    },
                }
            )

    unsigned: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "source": normalized_source,
        "policy": _policy(),
        "symbols": selected,
    }
    return {**unsigned, "selection_sha256": _canonical_sha256(unsigned)}


def validate_selection_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if set(payload) != {
        "contract_version",
        "source",
        "policy",
        "symbols",
        "selection_sha256",
    }:
        raise ValueError("selection-v2 manifest must use the exact schema")
    if payload["contract_version"] != CONTRACT_VERSION:
        raise ValueError("Unsupported canary selection contract_version")
    source = _normalized_source(payload["source"])
    if payload["policy"] != _policy():
        raise ValueError("selection-v2 policy does not match the deterministic contract")
    symbols = payload["symbols"]
    if not isinstance(symbols, list) or len(symbols) != 20:
        raise ValueError("Canary selection must contain exactly 20 symbols")
    if len({int(entry.get("symbol_id", -1)) for entry in symbols}) != 20 or len(
        {str(entry.get("symbol") or "").upper() for entry in symbols}
    ) != 20:
        raise ValueError("Canary selection symbols must be 20 unique identities")
    board_counts: dict[str, int] = defaultdict(int)
    boundary_counts: dict[tuple[str, str], int] = defaultdict(int)
    for entry in symbols:
        if set(entry) != {
            "symbol_id",
            "symbol",
            "board",
            "activity_boundary",
            "traits",
            "eligible_timeframes",
            "evidence",
        }:
            raise ValueError("selection-v2 symbol entry must use the exact schema")
        symbol = str(entry.get("symbol") or "").strip().upper()
        board = str(entry.get("board") or "")
        boundary = str(entry.get("activity_boundary") or "")
        evidence = entry.get("evidence")
        if _board(symbol) != board or board not in BOARD_QUOTAS:
            raise ValueError("selection-v2 board evidence is inconsistent")
        if boundary not in BOUNDARY_COUNTS or not isinstance(evidence, Mapping):
            raise ValueError("selection-v2 activity boundary evidence is incomplete")
        if evidence.get("basis") != ACTIVITY_BASIS or str(
            evidence.get("canonical_audit_run_id")
        ) != source["canonical_audit_run_id"]:
            raise ValueError("selection-v2 activity evidence is not audit-bound")
        if set(evidence) != {
            "basis",
            "canonical_audit_run_id",
            "five_minute_rows",
            "daily_rows",
            "activity_ratio_numerator",
            "activity_ratio_denominator",
        }:
            raise ValueError("selection-v2 activity evidence must use the exact schema")
        if entry.get("traits") != [board, f"{boundary}_activity_boundary"]:
            raise ValueError("selection-v2 traits are inconsistent")
        eligible_timeframes = entry.get("eligible_timeframes")
        if (
            not isinstance(eligible_timeframes, list)
            or not eligible_timeframes
            or len(set(eligible_timeframes)) != len(eligible_timeframes)
            or any(value not in CODE_TO_TIMEFRAME.values() for value in eligible_timeframes)
            or "5f" not in eligible_timeframes
            or "1d" not in eligible_timeframes
        ):
            raise ValueError("selection-v2 eligible timeframe evidence is incomplete")
        numerator = _integer(
            evidence.get("activity_ratio_numerator"), "activity_ratio_numerator"
        )
        denominator = _integer(
            evidence.get("activity_ratio_denominator"), "activity_ratio_denominator"
        )
        five_minute_rows = _integer(evidence.get("five_minute_rows"), "five_minute_rows")
        daily_rows = _integer(evidence.get("daily_rows"), "daily_rows")
        if denominator == 0 or daily_rows == 0 or Fraction(
            numerator, denominator
        ) != Fraction(
            five_minute_rows,
            daily_rows * BARS_PER_COMPLETE_5F_SESSION,
        ):
            raise ValueError("selection-v2 activity ratio evidence is inconsistent")
        board_counts[board] += 1
        boundary_counts[(board, boundary)] += 1
    if dict(board_counts) != BOARD_QUOTAS or any(
        boundary_counts[(board, boundary)] != count
        for board in BOARD_ORDER
        for boundary, count in BOUNDARY_COUNTS.items()
    ):
        raise ValueError("selection-v2 board or activity boundary quotas are incomplete")
    unsigned = {key: payload[key] for key in payload if key != "selection_sha256"}
    if payload["selection_sha256"] != _canonical_sha256(unsigned):
        raise ValueError("selection-v2 canonical SHA-256 is invalid")
    return dict(payload)


def validate_selection_source(
    payload: Mapping[str, Any], source_build: Mapping[str, Any], *, build_id: str
) -> None:
    source = _normalized_source(payload["source"])
    expected = {
        "eligibility_build_id": str(uuid.UUID(str(build_id))),
        "eligibility_manifest_sha256": str(source_build["manifest_hash"]),
        **{
            field: (
                int(source_build[field])
                if field == "catalog_control_revision"
                else str(source_build[field])
            )
            for field in SOURCE_FIELDS
            if field
            not in {"eligibility_build_id", "eligibility_manifest_sha256"}
        },
    }
    if source != expected:
        raise RuntimeError("selection-v2 source provenance does not match eligibility build")


async def select_from_build(
    connection: asyncpg.Connection, source_build_id: str
) -> dict[str, Any]:
    from collector.module_c_batch_control import (
        revalidate_strict_v2_build,
        validate_strict_build,
    )

    source_build_id = str(uuid.UUID(str(source_build_id)))
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        build = await connection.fetchrow(
            """
            select build_id::text, config_hash, active_universe_hash, manifest_hash,
                   parameters,
                   canonical_audit_run_id::text, audit_evidence_sha256,
                   audit_checkpoint_sha256, freshness_contract_version,
                   freshness_contract_sha256, catalog_generation_id::text,
                   catalog_control_revision, catalog_manifest_sha256,
                   audit_active_universe_sha256, active_symbols, disposition_rows
              from module_c_eligibility_builds
             where build_id=$1::uuid
            """,
            source_build_id,
        )
        if build is None:
            raise RuntimeError(f"Unknown source eligibility build: {source_build_id}")
        if str(build["config_hash"]) != MODULE_C_CONFIG_HASH:
            raise RuntimeError("Source eligibility config_hash is not the production contract")
        if str(build["active_universe_hash"]) != str(
            build["audit_active_universe_sha256"]
        ):
            raise RuntimeError("Source eligibility active universe is not audit-bound")
        await validate_strict_build(
            connection, build, build_id=source_build_id, require_v2=True
        )
        await revalidate_strict_v2_build(
            connection, build, build_id=source_build_id
        )
        return await rebuild_selection_manifest(
            connection, source_build_id=source_build_id, build=build
        )


async def rebuild_selection_manifest(
    connection: asyncpg.Connection,
    *,
    source_build_id: str,
    build: Mapping[str, Any],
) -> dict[str, Any]:
    dispositions = await connection.fetch(
        """
        select symbol_id, symbol, timeframe, eligible, reasons, covered_until,
               unresolved_rows
          from module_c_eligibility
         where build_id=$1::uuid
         order by symbol_id, timeframe
        """,
        source_build_id,
    )
    checkpoints = await connection.fetch(
        """
        select symbol_id, timeframe, status, rows_scanned
          from kline_audit_checkpoints
         where audit_run_id=$1::uuid
         order by symbol_id, timeframe
        """,
        build["canonical_audit_run_id"],
    )
    disposition_objects = [
        Disposition(
            symbol_id=int(row["symbol_id"]),
            symbol=str(row["symbol"]),
            timeframe=CODE_TO_TIMEFRAME[int(row["timeframe"])],
            timeframe_code=int(row["timeframe"]),
            eligible=bool(row["eligible"]),
            reasons=tuple(row["reasons"] or ()),
            covered_until=row["covered_until"],
            unresolved_rows=int(row["unresolved_rows"] or 0),
        )
        for row in dispositions
    ]
    if _stable_hash(row.json_record() for row in disposition_objects) != str(
        build["manifest_hash"]
    ):
        raise RuntimeError("Source eligibility disposition manifest hash drifted")
    source = {
        "eligibility_build_id": str(source_build_id),
        "eligibility_manifest_sha256": str(build["manifest_hash"]),
        **{
            field: (
                int(build[field])
                if field == "catalog_control_revision"
                else str(build[field])
            )
            for field in SOURCE_FIELDS
            if field not in {"eligibility_build_id", "eligibility_manifest_sha256"}
        },
    }
    return build_selection_manifest(
        source=source,
        dispositions=[dict(row) for row in dispositions],
        checkpoints=[dict(row) for row in checkpoints],
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic 20-symbol canary selection from strict-v2 evidence"
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--source-build-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    return args


async def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    connection = await asyncpg.connect(args.database_url)
    try:
        manifest = await select_from_build(connection, args.source_build_id)
    finally:
        await connection.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    temporary = args.output.with_name(f".{args.output.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(output, encoding="utf-8")
        os.replace(temporary, args.output)
    finally:
        temporary.unlink(missing_ok=True)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
