from __future__ import annotations

from pathlib import Path

import pytest

from collector.symbol_data_availability_exclusion import (
    REQUIRED_TIMEFRAMES,
    active_universe_manifest_sha256,
    build_candidates,
    candidate_manifest_sha256,
    validate_audit,
)


def _audit():
    return {
        "status": "completed",
        "apply_mode": False,
        "parameters": {
            "contract_version": "module-c-strict-audit-v2",
            "timeframes": list(REQUIRED_TIMEFRAMES),
            "active_universe_sha256": "a" * 64,
            "catalog_generation_id": "11111111-1111-1111-1111-111111111111",
            "catalog_control_revision": 7,
            "catalog_manifest_sha256": "c" * 64,
        },
        "summary": {"evidence_complete": True, "evidence_sha256": "b" * 64},
    }


def _evidence(empty_scopes=()):
    symbols = [
        {"symbol_id": 1, "symbol": "600000.SH"},
        {"symbol_id": 2, "symbol": "920047.BJ"},
    ]
    checkpoints = []
    catalog = []
    for symbol in symbols:
        for timeframe in REQUIRED_TIMEFRAMES:
            empty = (symbol["symbol_id"], timeframe) in set(empty_scopes)
            checkpoints.append(
                {
                    "symbol_id": symbol["symbol_id"],
                    "timeframe": timeframe,
                    "status": "completed",
                    "rows_scanned": 0 if empty else 100,
                    "metadata": {"disposition": "unresolved" if empty else "eligible"},
                }
            )
            catalog.append(
                {
                    "symbol_id": symbol["symbol_id"],
                    "timeframe": timeframe,
                    "state": "empty" if empty else "present",
                    "bounds_complete": True,
                    "min_ts": None if empty else "2020-01-01T00:00:00Z",
                    "max_ts": None if empty else "2026-07-17T07:00:00Z",
                }
            )
    return symbols, checkpoints, catalog


def test_only_audit_and_catalog_proven_empty_symbol_is_excluded() -> None:
    symbols, checkpoints, catalog = _evidence({(2, 5), (2, 30)})
    candidates = build_candidates(
        symbols=symbols, checkpoints=checkpoints, catalog_rows=catalog
    )
    assert [(row.symbol, row.unavailable_timeframes) for row in candidates] == [
        ("920047.BJ", (5, 30))
    ]
    assert len(candidate_manifest_sha256(candidates)) == 64


def test_active_universe_hash_matches_strict_v2_json_lines_contract() -> None:
    symbols, _, _ = _evidence()
    expected = __import__("hashlib").sha256(
        (
            '{"symbol":"600000.SH","symbol_id":1}\n'
            '{"symbol":"920047.BJ","symbol_id":2}\n'
        ).encode("utf-8")
    ).hexdigest()
    assert active_universe_manifest_sha256(symbols) == expected


def test_empty_scope_without_matching_catalog_evidence_fails_closed() -> None:
    symbols, checkpoints, catalog = _evidence({(2, 5)})
    catalog[-5]["state"] = "present"
    with pytest.raises(ValueError, match="not confirmed"):
        build_candidates(symbols=symbols, checkpoints=checkpoints, catalog_rows=catalog)


def test_exact_five_level_universe_is_required() -> None:
    symbols, checkpoints, catalog = _evidence({(2, 5)})
    with pytest.raises(ValueError, match="exact active five-level universe"):
        build_candidates(
            symbols=symbols, checkpoints=checkpoints[:-1], catalog_rows=catalog
        )


def test_audit_must_be_completed_read_only_strict_v2() -> None:
    evidence, active_hash, generation, revision, catalog_hash = validate_audit(_audit())
    assert evidence == "b" * 64
    assert active_hash == "a" * 64
    assert str(generation) == "11111111-1111-1111-1111-111111111111"
    assert revision == 7
    assert catalog_hash == "c" * 64
    for replacement in (
        {"status": "running"},
        {"apply_mode": True},
        {"parameters": {"contract_version": "legacy"}},
    ):
        row = _audit()
        row.update(replacement)
        with pytest.raises(ValueError):
            validate_audit(row)


def test_migration_is_append_only_and_blocks_reactivation() -> None:
    sql = (
        Path(__file__).parents[3]
        / "db"
        / "sql"
        / "049_symbol_data_availability_exclusion.sql"
    ).read_text(encoding="utf-8").lower()
    assert "symbol_data_availability_exclusion_runs" in sql
    assert "symbol_data_availability_exclusions" in sql
    assert "before update or delete" in sql
    assert "new.is_active := false" in sql
    assert "before update of is_active on symbols" in sql
    assert "on conflict do nothing" not in sql
