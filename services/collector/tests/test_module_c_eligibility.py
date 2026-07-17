from __future__ import annotations

import asyncio
import json
from argparse import Namespace
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from collector.module_c_eligibility import (
    FRESHNESS_CONTRACT_VERSION,
    Symbol,
    _load_strict_inputs,
    build_manifest,
    build_summary,
    evaluate_dispositions,
    load_freshness_contract,
    parse_freshness_contract,
)
from collector.kline_sql_gate import ANOMALY_FIELDS, _json_value, _manifest_sha256


NOW = datetime(2026, 7, 3, 7, tzinfo=timezone.utc)


def _coverage(symbol_id: int) -> dict[tuple[int, str], datetime]:
    return {(symbol_id, timeframe): NOW for timeframe in ("5f", "30f", "1d", "1w", "1m")}


def test_complete_symbol_is_eligible_at_all_five_levels() -> None:
    rows = evaluate_dispositions([Symbol(1, "600000", "SH")], _coverage(1), {}, {})
    assert len(rows) == 5
    assert all(row.eligible and not row.reasons for row in rows)


def test_bj_30f_is_always_excluded() -> None:
    rows = evaluate_dispositions([Symbol(2, "920000", "BJ")], _coverage(2), {}, {})
    bj_30f = next(row for row in rows if row.timeframe == "30f")
    assert not bj_30f.eligible
    assert bj_30f.reasons == ("bj_30f_excluded",)


def test_daily_unresolved_propagates_to_week_and_month() -> None:
    symbol = Symbol(3, "000001", "SZ")
    rows = evaluate_dispositions(
        [symbol], _coverage(3), {(symbol.name, "1d"): 7}, {},
    )
    dispositions = {row.timeframe: row for row in rows}
    assert dispositions["1d"].reasons == ("unresolved_ambiguous_volume_unit",)
    assert dispositions["1w"].reasons == ("daily_unresolved_propagated",)
    assert dispositions["1m"].reasons == ("daily_unresolved_propagated",)
    assert dispositions["1w"].unresolved_rows == 7


def test_missing_source_and_watermark_have_stable_reasons() -> None:
    symbol = Symbol(4, "600001", "SH")
    coverage = _coverage(4)
    coverage.pop((4, "5f"))
    rows = evaluate_dispositions(
        [symbol], coverage, {}, {(symbol.name, "5f"): 1},
    )
    five = next(row for row in rows if row.timeframe == "5f")
    assert five.reasons == ("missing_source_file", "missing_ingest_watermark")
    assert not five.eligible


def test_canonical_gate_unresolved_fails_closed() -> None:
    symbol = Symbol(5, "600002", "SH")
    canonical = {
        (5, timeframe): "eligible" for timeframe in ("5f", "30f", "1d", "1w", "1m")
    }
    canonical[(5, "30f")] = "unresolved"
    rows = evaluate_dispositions([symbol], _coverage(5), {}, {}, canonical)
    by_timeframe = {row.timeframe: row for row in rows}
    assert by_timeframe["5f"].eligible
    assert by_timeframe["30f"].reasons == ("canonical_gate_unresolved",)


def test_explicit_empty_canonical_map_fails_closed() -> None:
    symbol = Symbol(6, "600003", "SH")
    rows = evaluate_dispositions([symbol], _coverage(6), {}, {}, {})
    assert all(not row.eligible for row in rows)
    assert all("canonical_gate_unresolved" in row.reasons for row in rows)


def test_authoritative_freshness_contract_is_exact_timezone_aware_and_stable(tmp_path) -> None:
    payload = {
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": "2026-07-03T07:00:00+00:00",
        "trading_calendar": {"id": "sse-szse-2026-v1", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            "5f": "2026-07-03T07:00:00+00:00",
            "30f": "2026-07-03T07:00:00+00:00",
            "1d": "2026-07-03T07:00:00+00:00",
            "1w": "2026-06-27T07:00:00+00:00",
            "1m": "2026-06-30T07:00:00+00:00",
        },
    }
    path = tmp_path / "freshness.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    first = load_freshness_contract(path)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    second = load_freshness_contract(path)

    assert first.sha256 == second.sha256
    assert first.contract_version == FRESHNESS_CONTRACT_VERSION
    assert first.expected_closed_watermarks["5f"] == NOW

    payload["expected_closed_watermarks"].pop("1m")
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="exact five"):
        load_freshness_contract(path)

    payload["expected_closed_watermarks"]["1m"] = "2026-06-30T15:00:00+08:00"
    payload["extra"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="exact schema"):
        load_freshness_contract(path)


def test_freshness_contract_rejects_naive_timestamps(tmp_path) -> None:
    payload = {
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": "2026-07-03T07:00:00",
        "trading_calendar": {"id": "calendar", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            timeframe: "2026-07-03T07:00:00+00:00"
            for timeframe in ("5f", "30f", "1d", "1w", "1m")
        },
    }
    path = tmp_path / "freshness.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="timezone-aware"):
        load_freshness_contract(path)


def test_freshness_mapping_validator_rejects_non_exact_contract() -> None:
    payload = {
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": "2026-07-03T07:00:00+00:00",
        "trading_calendar": {"id": "calendar", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            timeframe: "2026-07-03T07:00:00+00:00"
            for timeframe in ("5f", "30f", "1d", "1w", "1m")
        },
        "unexpected": True,
    }
    with pytest.raises(ValueError, match="exact schema"):
        parse_freshness_contract(payload)


class StrictInputConnection:
    observed_at = datetime(2026, 7, 3, 8, tzinfo=timezone.utc)
    checkpoint_end = NOW
    generation_id = UUID("11111111-1111-1111-1111-111111111111")

    def __init__(self, *, checkpoint_count: int = 5, evidence_complete: bool = True) -> None:
        self.checkpoint_count = checkpoint_count
        self.evidence_complete = evidence_complete
        self.transaction_isolations: list[str | None] = []
        self.transaction_failed = False
        self.executed: list[str] = []
        self.copied = 0
        self.catalog_revision = 7
        self.catalog_manifest_sha256_override: str | None = None
        self.catalog_updated_at = datetime(2026, 7, 3, 7, tzinfo=timezone.utc)
        self.checkpoint_metadata_overrides: dict[str, object] = {}
        self.checkpoint_metadata_by_timeframe: dict[int, dict[str, object]] = {}
        self.summary_overrides: dict[str, object] = {}
        self.fetchrow_sql: list[str] = []

    def catalog_rows(self):
        return [
            {
                "symbol_id": 1,
                "timeframe": timeframe,
                "state": "present",
                "bounds_complete": True,
                "min_ts": datetime(2020, 1, 1, tzinfo=timezone.utc),
                "max_ts": self.checkpoint_end,
                "updated_at": self.catalog_updated_at,
            }
            for timeframe in (5, 30, 1440, 10080, 43200)
        ]

    def catalog_manifest_sha256(self) -> str:
        return _manifest_sha256([
            {
                "symbol_id": row["symbol_id"],
                "timeframe": row["timeframe"],
                "state": row["state"],
                "bounds_complete": True,
                "min_ts": _json_value(row["min_ts"]),
                "max_ts": _json_value(row["max_ts"]),
                "updated_at": _json_value(row["updated_at"]),
            }
            for row in self.catalog_rows()
        ])

    @asynccontextmanager
    async def transaction(self, *, isolation=None):
        self.transaction_isolations.append(isolation)
        try:
            yield
        except Exception:
            self.transaction_failed = True
            raise

    async def execute(self, sql: str, *_args):
        self.executed.append(" ".join(sql.lower().split()))
        return "INSERT 0 1"

    async def copy_records_to_table(self, *_args, **_kwargs):
        self.copied += 1

    async def close(self):
        return None

    async def fetchrow(self, sql: str, *_args):
        normalized = " ".join(sql.lower().split())
        self.fetchrow_sql.append(normalized)
        if "from kline_scope_catalog_control control" in normalized:
            return {
                "generation_id": self.generation_id,
                "revision": self.catalog_revision,
                "status": "complete",
                "expected_scope_count": 5,
                "symbol_ids": [1],
                "timeframes": [5, 30, 1440, 10080, 43200],
            }
        if "from kline_audit_runs" in normalized:
            parameters = {
                "contract_version": "module-c-strict-audit-v2",
                "engine": "sql_gate",
                "apply_mode": False,
                "timeframes": [5, 30, 1440, 10080, 43200],
                "observed_at": self.observed_at.isoformat(),
                "observed_wal_lsn": "0/16B6C50",
                "transaction_snapshot": "100:100:",
                "active_universe_count": 1,
                "active_universe_sha256": _manifest_sha256([
                    {"symbol_id": 1, "symbol": "600000.SH"}
                ]),
                "catalog_generation_id": str(self.generation_id),
                "catalog_control_revision": 7,
                "catalog_expected_scope_count": 5,
                "catalog_required_scope_count": 5,
                "catalog_manifest_sha256": (
                    self.catalog_manifest_sha256_override
                    or self.catalog_manifest_sha256()
                ),
            }
            parameters["evidence_sha256"] = _manifest_sha256([parameters])
            summary = {
                "contract_version": "module-c-strict-audit-v2",
                "observed_at": self.observed_at.isoformat(),
                "evidence_sha256": parameters["evidence_sha256"],
                "evidence_complete": self.evidence_complete,
                "checkpoints": 5,
                "rows_scanned": 50,
                "eligible": 5,
                "unresolved": 0,
                **{field: 0 for field in ANOMALY_FIELDS},
                "anomaly_total": 0,
                "gate_pass": True,
                **self.summary_overrides,
            }
            return {
                "status": "completed",
                "apply_mode": False,
                "parameters": parameters,
                "summary": summary,
            }
        raise AssertionError(sql)

    async def fetch(self, sql: str, *_args):
        normalized = " ".join(sql.lower().split())
        if "from symbols" in normalized:
            return [{"symbol_id": 1, "code": "600000", "exchange": "SH"}]
        if "from kline_scope_catalog" in normalized:
            return self.catalog_rows()
        if "from kline_audit_checkpoints" in normalized:
            return [
                {
                    "symbol_id": 1,
                    "timeframe": timeframe,
                    "status": "completed",
                    "shard_start": datetime(2020, 1, 1, tzinfo=timezone.utc),
                    "shard_end": self.checkpoint_end,
                    "rows_scanned": 10,
                    "metadata": {
                        **{field: 0 for field in ANOMALY_FIELDS},
                        "disposition": "eligible",
                        **self.checkpoint_metadata_overrides,
                        **self.checkpoint_metadata_by_timeframe.get(timeframe, {}),
                    },
                }
                for timeframe in (5, 30, 1440, 10080, 43200)[: self.checkpoint_count]
            ]
        if "from kline_import_quarantine" in normalized:
            return []
        raise AssertionError(sql)


def _freshness_fixture(tmp_path: Path) -> Path:
    payload = {
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": "2026-07-03T07:00:00+00:00",
        "trading_calendar": {"id": "calendar", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            timeframe: "2026-07-03T07:00:00+00:00"
            for timeframe in ("5f", "30f", "1d", "1w", "1m")
        },
    }
    path = tmp_path / "freshness.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _bind_producer_hash(row, producer_hash: str):
    if "parameters" not in row:
        return row

    parameters = row["parameters"]
    parameters["active_universe_sha256"] = producer_hash
    parameters.pop("evidence_sha256", None)
    parameters["evidence_sha256"] = _manifest_sha256([parameters])
    row["summary"]["evidence_sha256"] = parameters["evidence_sha256"]
    return row


def test_strict_input_row_lock_is_explicitly_disabled_for_readonly_callers(
    tmp_path,
) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    readonly = StrictInputConnection()
    asyncio.run(
        _load_strict_inputs(
            readonly,
            str(UUID(int=1)),
            freshness,
            for_share=False,
        )
    )
    audit_sql = next(
        sql for sql in readonly.fetchrow_sql if "from kline_audit_runs" in sql
    )
    assert "for share" not in audit_sql

    locked = StrictInputConnection()
    asyncio.run(_load_strict_inputs(locked, str(UUID(int=1)), freshness))
    audit_sql = next(
        sql for sql in locked.fetchrow_sql if "from kline_audit_runs" in sql
    )
    assert "for share" in audit_sql


def test_strict_input_requires_complete_exact_audit_and_producer_universe_hash(
    tmp_path, monkeypatch
) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()

    # The consumer must use the producer's canonical universe serializer.
    from collector.kline_sql_gate import _manifest_sha256

    producer_hash = _manifest_sha256([{"symbol_id": 1, "symbol": "600000.SH"}])
    original = connection.fetchrow

    async def fetchrow(sql: str, *args):
        row = await original(sql, *args)
        return _bind_producer_hash(row, producer_hash)

    monkeypatch.setattr(connection, "fetchrow", fetchrow)
    result = asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))

    assert len(result.audit_evidence_sha256) == 64
    assert result.catalog_generation_id == connection.generation_id
    assert result.audit_active_universe_sha256 == producer_hash
    assert len(result.audit_checkpoint_sha256) == 64
    assert result.coverage[(1, "5f")] == connection.checkpoint_end

    incomplete = StrictInputConnection(checkpoint_count=4)
    original_incomplete = incomplete.fetchrow

    async def incomplete_fetchrow(sql: str, *args):
        row = await original_incomplete(sql, *args)
        return _bind_producer_hash(row, producer_hash)

    monkeypatch.setattr(incomplete, "fetchrow", incomplete_fetchrow)
    with pytest.raises(RuntimeError, match="exact five-level checkpoint"):
        asyncio.run(_load_strict_inputs(incomplete, str(UUID(int=1)), freshness))

    incomplete_evidence = StrictInputConnection(evidence_complete=False)
    original_evidence = incomplete_evidence.fetchrow

    async def evidence_fetchrow(sql: str, *args):
        row = await original_evidence(sql, *args)
        return _bind_producer_hash(row, producer_hash)

    monkeypatch.setattr(incomplete_evidence, "fetchrow", evidence_fetchrow)
    with pytest.raises(RuntimeError, match="evidence_complete"):
        asyncio.run(_load_strict_inputs(incomplete_evidence, str(UUID(int=1)), freshness))


def test_strict_disposition_excludes_checkpoint_behind_authoritative_watermark(
    tmp_path, monkeypatch
) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    connection.checkpoint_end = datetime(2026, 7, 3, 6, tzinfo=timezone.utc)
    from collector.kline_sql_gate import _manifest_sha256
    producer_hash = _manifest_sha256([{"symbol_id": 1, "symbol": "600000.SH"}])
    original = connection.fetchrow

    async def fetchrow(sql: str, *args):
        row = await original(sql, *args)
        return _bind_producer_hash(row, producer_hash)

    monkeypatch.setattr(connection, "fetchrow", fetchrow)
    result = asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))
    assert result.freshness_reasons[(1, "5f")] == "authoritative_freshness_stale"


def test_strict_input_rejects_checkpoint_after_authoritative_watermark(
    tmp_path, monkeypatch
) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    connection.checkpoint_end = datetime(2026, 7, 3, 8, tzinfo=timezone.utc)
    from collector.kline_sql_gate import _manifest_sha256
    producer_hash = _manifest_sha256([{"symbol_id": 1, "symbol": "600000.SH"}])
    original = connection.fetchrow

    async def fetchrow(sql: str, *args):
        row = await original(sql, *args)
        return _bind_producer_hash(row, producer_hash)

    monkeypatch.setattr(connection, "fetchrow", fetchrow)
    with pytest.raises(RuntimeError, match="exceeds authoritative watermark"):
        asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))


def test_strict_input_recomputes_producer_evidence_hash(tmp_path, monkeypatch) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    from collector.kline_sql_gate import _manifest_sha256
    producer_hash = _manifest_sha256([{"symbol_id": 1, "symbol": "600000.SH"}])
    original = connection.fetchrow

    async def fetchrow(sql: str, *args):
        row = _bind_producer_hash(await original(sql, *args), producer_hash)
        row["parameters"]["catalog_control_revision"] = 8
        return row

    monkeypatch.setattr(connection, "fetchrow", fetchrow)
    with pytest.raises(RuntimeError, match="evidence_sha256"):
        asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))


def test_strict_input_rejects_summary_disposition_count_mismatch(
    tmp_path, monkeypatch
) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    from collector.kline_sql_gate import _manifest_sha256
    producer_hash = _manifest_sha256([{"symbol_id": 1, "symbol": "600000.SH"}])
    original = connection.fetchrow

    async def fetchrow(sql: str, *args):
        row = _bind_producer_hash(await original(sql, *args), producer_hash)
        if "summary" in row:
            row["summary"]["eligible"] = 4
            row["summary"]["unresolved"] = 1
        return row

    monkeypatch.setattr(connection, "fetchrow", fetchrow)
    with pytest.raises(RuntimeError, match="summary aggregates"):
        asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))


@pytest.mark.parametrize("field", ANOMALY_FIELDS)
def test_strict_input_requires_every_checkpoint_anomaly_field(field, tmp_path) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    connection.checkpoint_metadata_overrides[field] = None

    with pytest.raises(RuntimeError, match=field):
        asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))


@pytest.mark.parametrize("field", ["rows_scanned", *ANOMALY_FIELDS, "anomaly_total"])
def test_strict_input_rejects_each_tampered_summary_aggregate(field, tmp_path) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    connection.summary_overrides[field] = 1

    with pytest.raises(RuntimeError, match="summary aggregates"):
        asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))


def test_strict_input_rejects_active_catalog_revision_or_manifest_drift(tmp_path) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    revision_drift = StrictInputConnection()
    revision_drift.catalog_revision = 8
    with pytest.raises(RuntimeError, match="catalog no longer matches"):
        asyncio.run(_load_strict_inputs(revision_drift, str(UUID(int=1)), freshness))

    manifest_drift = StrictInputConnection()
    manifest_drift.catalog_manifest_sha256_override = "0" * 64
    with pytest.raises(RuntimeError, match="manifest does not match"):
        asyncio.run(_load_strict_inputs(manifest_drift, str(UUID(int=1)), freshness))


def test_strict_input_accepts_consistent_unresolved_checkpoint_and_excludes_scope(
    tmp_path,
) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    connection.checkpoint_metadata_by_timeframe[5] = {
        "invalid_ohlc": 1,
        "disposition": "unresolved",
    }
    connection.summary_overrides.update({
        "invalid_ohlc": 1,
        "anomaly_total": 1,
        "eligible": 4,
        "unresolved": 1,
        "gate_pass": False,
    })

    strict = asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))
    rows = evaluate_dispositions(
        strict.symbols,
        strict.coverage,
        {},
        {},
        strict.canonical_dispositions,
        strict.freshness_reasons,
    )
    five_minute = next(row for row in rows if row.timeframe == "5f")
    assert five_minute.eligible is False
    assert "canonical_gate_unresolved" in five_minute.reasons


def test_strict_input_rejects_gate_pass_inconsistent_with_anomaly_total(tmp_path) -> None:
    freshness = load_freshness_contract(_freshness_fixture(tmp_path))
    connection = StrictInputConnection()
    connection.summary_overrides["gate_pass"] = False

    with pytest.raises(RuntimeError, match="summary aggregates"):
        asyncio.run(_load_strict_inputs(connection, str(UUID(int=1)), freshness))


def test_dry_run_performs_full_validation_inside_repeatable_read(
    tmp_path, monkeypatch
) -> None:
    freshness_path = _freshness_fixture(tmp_path)
    connection = StrictInputConnection()
    from collector.kline_sql_gate import _manifest_sha256
    producer_hash = _manifest_sha256([{"symbol_id": 1, "symbol": "600000.SH"}])
    original = connection.fetchrow

    async def fetchrow(sql: str, *args):
        return _bind_producer_hash(await original(sql, *args), producer_hash)

    async def connect(_database_url: str):
        return connection

    monkeypatch.setattr(connection, "fetchrow", fetchrow)
    monkeypatch.setattr("collector.module_c_eligibility.asyncpg.connect", connect)
    args = Namespace(
        database_url="postgresql://audit",
        manifest_version="strict-v2-test",
        config_hash="config",
        build_id=str(UUID(int=2)),
        audit_run_id=str(UUID(int=1)),
        freshness_contract=freshness_path,
        output_dir=tmp_path / "outputs",
        dry_run=True,
    )

    metadata = asyncio.run(build_manifest(args))

    assert metadata["audit_checkpoint_sha256"]
    assert connection.transaction_isolations == ["repeatable_read"]
    assert connection.executed == []
    assert connection.copied == 0


def test_output_failure_rolls_back_non_dry_build(tmp_path, monkeypatch) -> None:
    connection = StrictInputConnection()

    async def connect(_database_url: str):
        return connection

    def fail_outputs(*_args):
        raise OSError("output unavailable")

    monkeypatch.setattr("collector.module_c_eligibility.asyncpg.connect", connect)
    monkeypatch.setattr("collector.module_c_eligibility._write_outputs", fail_outputs)
    args = Namespace(
        database_url="postgresql://audit",
        manifest_version="strict-v2-test",
        config_hash="config",
        build_id=str(UUID(int=2)),
        audit_run_id=str(UUID(int=1)),
        freshness_contract=_freshness_fixture(tmp_path),
        output_dir=tmp_path / "outputs",
        dry_run=False,
    )

    with pytest.raises(OSError, match="output unavailable"):
        asyncio.run(build_manifest(args))

    assert connection.transaction_failed is True
    assert len(connection.executed) == 1
    assert connection.copied == 1


def test_non_dry_build_requires_explicit_audit_and_freshness_before_connect(tmp_path) -> None:
    args = Namespace(
        database_url="postgresql://unused",
        manifest_version="strict-v2-test",
        config_hash="config",
        build_id=None,
        audit_run_id=None,
        freshness_contract=None,
        output_dir=tmp_path,
        dry_run=False,
    )
    with pytest.raises(ValueError, match="explicit --audit-run-id"):
        asyncio.run(build_manifest(args))

    args.audit_run_id = str(UUID(int=1))
    with pytest.raises(ValueError, match="--freshness-contract"):
        asyncio.run(build_manifest(args))


def test_summary_counts_each_level_instead_of_market_total() -> None:
    symbols = [Symbol(1, "600000", "SH"), Symbol(2, "920000", "BJ")]
    rows = evaluate_dispositions(symbols, {**_coverage(1), **_coverage(2)}, {}, {})
    summary = build_summary(rows)
    assert summary["rows"] == 10
    assert summary["by_timeframe"]["5f"]["eligible"] == 2
    assert summary["by_timeframe"]["30f"]["eligible"] == 1
    assert summary["by_timeframe"]["30f"]["excluded"] == 1


def test_migration_enforces_versioned_append_only_rows() -> None:
    sql = (
        Path(__file__).parents[3] / "db" / "sql" / "031_module_c_eligibility.sql"
    ).read_text(encoding="utf-8")
    assert "manifest_version text not null unique" in sql
    assert "before update or delete on module_c_eligibility_builds" in sql.lower()
    assert "before update or delete on module_c_eligibility" in sql.lower()
    assert "disposition_rows = active_symbols * 5" in sql
