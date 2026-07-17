from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import pytest

import collector.module_c_batch_control as batch_control
from collector.module_c_batch_control import load_selection
from collector.module_c_canary_selection import (
    BARS_PER_COMPLETE_5F_SESSION,
    BOARD_QUOTAS,
    build_selection_manifest,
    select_from_build,
    validate_selection_manifest,
    validate_selection_source,
)
from collector.module_c_eligibility import CODE_TO_TIMEFRAME, Disposition, _stable_hash
from trading_protocol import MODULE_C_CONFIG_HASH


def _source() -> dict[str, object]:
    return {
        "eligibility_build_id": "11111111-1111-1111-1111-111111111111",
        "eligibility_manifest_sha256": "0" * 64,
        "canonical_audit_run_id": "22222222-2222-2222-2222-222222222222",
        "audit_evidence_sha256": "1" * 64,
        "audit_checkpoint_sha256": "2" * 64,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": "3" * 64,
        "catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": "4" * 64,
        "audit_active_universe_sha256": "5" * 64,
    }


def _symbols() -> list[tuple[int, str]]:
    names = []
    prefixes = (
        ("600", "SH"),
        ("300", "SZ"),
        ("688", "SH"),
        ("920", "BJ"),
    )
    symbol_id = 1
    for prefix, exchange in prefixes:
        for suffix in range(6):
            names.append((symbol_id, f"{prefix}{suffix:03d}.{exchange}"))
            symbol_id += 1
    return names


def _evidence():
    dispositions = []
    checkpoints = []
    for symbol_id, symbol in _symbols():
        for timeframe in (5, 30, 1440, 10080, 43200):
            eligible = not (symbol.endswith(".BJ") and timeframe == 30)
            dispositions.append(
                {
                    "symbol_id": symbol_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "eligible": eligible,
                    "reasons": [] if eligible else ["bj_30f_excluded"],
                    "covered_until": None,
                    "unresolved_rows": 0,
                }
            )
            checkpoints.append(
                {
                    "symbol_id": symbol_id,
                    "timeframe": timeframe,
                    "status": "completed",
                    "rows_scanned": 1000 + symbol_id if timeframe == 5 else 100,
                }
            )
    return dispositions, checkpoints


def test_selection_v2_is_deterministic_and_covers_fixed_board_boundaries() -> None:
    dispositions, checkpoints = _evidence()
    first = build_selection_manifest(
        source=_source(), dispositions=dispositions, checkpoints=checkpoints
    )
    second = build_selection_manifest(
        source=_source(),
        dispositions=list(reversed(dispositions)),
        checkpoints=list(reversed(checkpoints)),
    )

    assert first == second
    assert len(first["symbols"]) == 20
    assert first["policy"]["board_quotas"] == BOARD_QUOTAS
    assert {
        board: sum(row["board"] == board for row in first["symbols"])
        for board in BOARD_QUOTAS
    } == BOARD_QUOTAS
    assert len(first["selection_sha256"]) == 64
    assert validate_selection_manifest(first) == first


def test_selection_v2_fails_closed_when_one_board_lacks_candidates() -> None:
    dispositions, checkpoints = _evidence()
    dispositions = [row for row in dispositions if not str(row["symbol"]).endswith(".BJ")]
    checkpoints = [
        row for row in checkpoints if int(row["symbol_id"]) <= 18
    ]

    with pytest.raises(ValueError, match="board bj has 0 candidates; 5 required"):
        build_selection_manifest(
            source=_source(), dispositions=dispositions, checkpoints=checkpoints
        )


def test_activity_evidence_uses_canonical_49_bar_complete_session() -> None:
    dispositions, checkpoints = _evidence()
    for row in checkpoints:
        if row["timeframe"] == 5:
            row["rows_scanned"] = 4900
        elif row["timeframe"] == 1440:
            row["rows_scanned"] = 100
    manifest = build_selection_manifest(
        source=_source(), dispositions=dispositions, checkpoints=checkpoints
    )

    assert BARS_PER_COMPLETE_5F_SESSION == 49
    assert all(
        row["evidence"]["activity_ratio_numerator"] == 1
        and row["evidence"]["activity_ratio_denominator"] == 1
        for row in manifest["symbols"]
    )


def test_selection_v2_rejects_incomplete_five_level_evidence() -> None:
    dispositions, checkpoints = _evidence()
    checkpoints = [
        row
        for row in checkpoints
        if not (row["symbol_id"] == 1 and row["timeframe"] == 30)
    ]

    with pytest.raises(ValueError, match="exactly five audit checkpoints"):
        build_selection_manifest(
            source=_source(), dispositions=dispositions, checkpoints=checkpoints
        )


def test_selection_v2_canonical_hash_and_source_binding_reject_tampering(tmp_path) -> None:
    dispositions, checkpoints = _evidence()
    manifest = build_selection_manifest(
        source=_source(), dispositions=dispositions, checkpoints=checkpoints
    )
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    names, digest, loaded = load_selection(path)
    assert len(names) == 20
    assert digest == manifest["selection_sha256"]
    assert loaded == manifest
    validate_selection_source(
        manifest,
        {
            **{
                field: value
                for field, value in _source().items()
                if field not in {"eligibility_build_id", "eligibility_manifest_sha256"}
            },
            "manifest_hash": "0" * 64,
        },
        build_id="11111111-1111-1111-1111-111111111111",
    )

    manifest["symbols"][0]["symbol"] = "600999.SH"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical SHA-256"):
        load_selection(path)


class _ReadOnlyConnection:
    def __init__(self, dispositions, checkpoints) -> None:
        self.dispositions = dispositions
        self.checkpoints = checkpoints
        self.transaction_args = None

    @asynccontextmanager
    async def transaction(self, **kwargs):
        self.transaction_args = kwargs
        yield

    async def fetchrow(self, _sql, _build_id):
        source = _source()
        manifest_hash = _stable_hash(
            Disposition(
                symbol_id=int(row["symbol_id"]),
                symbol=str(row["symbol"]),
                timeframe=CODE_TO_TIMEFRAME[int(row["timeframe"])],
                timeframe_code=int(row["timeframe"]),
                eligible=bool(row["eligible"]),
                reasons=tuple(row["reasons"]),
                covered_until=row["covered_until"],
                unresolved_rows=int(row["unresolved_rows"]),
            ).json_record()
            for row in self.dispositions
        )
        return {
            "build_id": source["eligibility_build_id"],
            "config_hash": MODULE_C_CONFIG_HASH,
            "active_universe_hash": source["audit_active_universe_sha256"],
            "manifest_hash": manifest_hash,
            "parameters": {"policy": "strict-v2"},
            **{
                field: value
                for field, value in source.items()
                if field not in {"eligibility_build_id", "eligibility_manifest_sha256"}
            },
            "active_symbols": 24,
            "disposition_rows": 120,
        }

    async def fetch(self, sql, *_args):
        if "from module_c_eligibility" in sql.lower():
            return self.dispositions
        if "from kline_audit_checkpoints" in sql.lower():
            return self.checkpoints
        raise AssertionError(sql)


def test_select_from_build_uses_one_read_only_repeatable_read_snapshot(monkeypatch) -> None:
    dispositions, checkpoints = _evidence()
    connection = _ReadOnlyConnection(dispositions, checkpoints)
    calls = []

    async def no_op(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(batch_control, "validate_strict_build", no_op)
    monkeypatch.setattr(batch_control, "revalidate_strict_v2_build", no_op)

    manifest = asyncio.run(
        select_from_build(
            connection, "11111111-1111-1111-1111-111111111111"
        )
    )

    assert manifest["source"]["eligibility_build_id"] == _source()[
        "eligibility_build_id"
    ]
    assert connection.transaction_args == {
        "isolation": "repeatable_read",
        "readonly": True,
    }
    assert len(calls) == 2
