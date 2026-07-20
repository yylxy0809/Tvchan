from __future__ import annotations

import argparse
import asyncio
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
from trading_protocol.module_c_canary_selection import (
    ACTIVITY_BASIS,
    BARS_PER_COMPLETE_5F_SESSION,
    BOARD_ORDER,
    BOARD_QUOTAS,
    BOUNDARY_COUNTS,
    CONTRACT_VERSION,
    SOURCE_FIELDS,
    canonical_selection_sha256,
    classify_board,
    normalize_selection_source,
    selection_policy,
    selection_policy_for,
    selection_spec,
    validate_selection_manifest,
)


# Compatibility aliases keep the Collector builder API stable while the pure contract
# has one implementation shared with API/report consumers.
_canonical_sha256 = canonical_selection_sha256
_board = classify_board
_normalized_source = normalize_selection_source
_policy = selection_policy


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


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
    contract_version: str = CONTRACT_VERSION,
) -> dict[str, Any]:
    normalized_source = _normalized_source(source)
    board_order, board_quotas, boundary_counts, _boundary_order = selection_spec(contract_version)
    by_board: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in _candidate_rows(dispositions, checkpoints):
        by_board[str(candidate["board"])].append(candidate)

    selected: list[dict[str, Any]] = []
    for board in board_order:
        ordered = sorted(
            by_board[board],
            key=lambda row: (
                row["activity_ratio"],
                row["symbol_id"],
                row["symbol"],
            ),
        )
        quota = board_quotas[board]
        if len(ordered) < quota:
            raise ValueError(
                f"selection board {board} has {len(ordered)} candidates; {quota} required"
            )
        lower = boundary_counts["lower"]
        middle = boundary_counts["middle"]
        upper = boundary_counts["upper"]
        middle_start = (len(ordered) - middle) // 2
        picks = [
            *(("lower", ordered[index]) for index in range(lower)),
            *(("middle", ordered[index]) for index in range(middle_start, middle_start + middle)),
            *(("upper", ordered[index]) for index in range(len(ordered) - upper, len(ordered))),
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
        "contract_version": contract_version,
        "source": normalized_source,
        "policy": selection_policy_for(contract_version),
        "symbols": selected,
    }
    return {**unsigned, "selection_sha256": _canonical_sha256(unsigned)}


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
    connection: asyncpg.Connection, source_build_id: str, *, contract_version: str = CONTRACT_VERSION
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
            connection, build, build_id=source_build_id, for_share=False
        )
        return await rebuild_selection_manifest(
            connection, source_build_id=source_build_id, build=build,
            contract_version=contract_version,
        )


async def rebuild_selection_manifest(
    connection: asyncpg.Connection,
    *,
    source_build_id: str,
    build: Mapping[str, Any],
    contract_version: str = CONTRACT_VERSION,
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
        contract_version=contract_version,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic 20-symbol canary selection from strict-v2 evidence"
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--source-build-id", required=True)
    parser.add_argument("--contract-version", default=CONTRACT_VERSION)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    return args


async def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    connection = await asyncpg.connect(args.database_url)
    try:
        manifest = await select_from_build(
            connection, args.source_build_id, contract_version=args.contract_version
        )
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
