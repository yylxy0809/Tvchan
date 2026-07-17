from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from collector import kline_sql_gate as gate
from collector.kline_sql_gate import (
    PRIMARY_KEY_SQL,
    TIMEFRAMES,
    AuditRunAlreadyClaimed,
    InvalidAuditEvidence,
    _capture_snapshot_evidence,
    _claim_audit_run,
    _finalize_audit_run,
    build_gate_sql,
    parse_args,
    summarize,
)


def test_gate_has_five_database_side_checkpoint_workers() -> None:
    assert TIMEFRAMES == (5, 30, 1440, 10080, 43200)
    for timeframe in TIMEFRAMES:
        sql = build_gate_sql(timeframe).lower()
        assert "insert into kline_audit_checkpoints" in sql
        assert "join symbols" in sql
        expected_materialization = "materialized" if timeframe in (10080, 43200) else "not materialized"
        assert f"base as {expected_materialization}" in sql
        assert "jsonb_build_object" in sql
        assert "on conflict" in sql
        assert "select k.*" in sql
        assert "from universe" in sql
        assert "'missing_rows'" in sql
    assert "where false" in build_gate_sql(5).lower()
    assert "having count(*) > 1" in build_gate_sql(1440).lower()


def test_physical_duplicate_guarantee_is_validated_from_catalog() -> None:
    assert "pg_constraint" in PRIMARY_KEY_SQL
    assert "contype='p'" in PRIMARY_KEY_SQL


def test_validation_contract_is_aggregated_in_sql() -> None:
    sql = build_gate_sql(5).lower()
    assert "least(b.open_x1000,b.close_x1000,b.high_x1000)" in sql
    assert "greatest(b.open_x1000,b.close_x1000,b.low_x1000)" in sql
    assert "b.volume < 0" in sql
    assert "b.amount_x100 < 0" in sql
    assert "between 575 and 690" in sql
    assert "between 785 and 900" in sql
    assert "unexpected_source" in sql
    assert "source not in (2,4,9)" in sql


def test_30_minute_session_contract_includes_opening_snapshot() -> None:
    sql = build_gate_sql(30)
    assert "= 570" in sql
    assert "BETWEEN 600 AND 690" in sql
    assert "BETWEEN 810 AND 900" in sql
    assert "% 30 = 0" in sql


def test_logical_duplicate_keys_match_timeframe_contract() -> None:
    assert "lts::date AS logical_key" in build_gate_sql(1440)
    assert "date_trunc('week', lts) AS logical_key" in build_gate_sql(10080)
    assert "date_trunc('month', lts) AS logical_key" in build_gate_sql(43200)


def test_higher_periods_require_closed_daily_basis_and_source_8() -> None:
    for timeframe, bucket in ((10080, "week"), (43200, "month")):
        sql = build_gate_sql(timeframe).lower()
        assert "daily_ends" in sql
        assert f"date_trunc('{bucket}', (select observed_at from evidence_context)" in sql
        assert "b.ts is distinct from d.expected_ts" in sql
        assert "d.expected_ts is null" in sql
        assert "source not in (8)" in sql
        assert "not b.is_complete" in sql


def test_summary_completes_scan_and_reports_gate_result() -> None:
    clean = {
        "checkpoints": 5, "rows_scanned": 100, "eligible": 5, "unresolved": 0,
        "invalid_ohlc": 0, "negative_volume": 0, "negative_amount": 0,
        "illegal_sessions": 0, "incomplete_rows": 0, "logical_duplicate_rows": 0,
        "unexpected_source": 0, "current_open_periods": 0,
        "timestamp_mismatches": 0, "missing_daily_basis": 0,
        "missing_higher_periods": 0,
        "missing_rows": 0,
    }
    status, summary = summarize(clean)
    assert status == "completed"
    assert summary["anomaly_total"] == 0
    assert summary["gate_pass"] is True

    dirty = dict(clean, illegal_sessions=2, unresolved=1)
    status, summary = summarize(dirty)
    assert status == "completed"
    assert summary["anomaly_total"] == 2
    assert summary["gate_pass"] is False


def test_cli_requires_the_exact_five_level_contract() -> None:
    args = parse_args(["--database-url", "postgresql://audit"])
    assert args.timeframe == list(TIMEFRAMES)

    args = parse_args([
        "--database-url", "postgresql://audit",
        "--timeframe", "43200", "--timeframe", "5", "--timeframe", "30",
        "--timeframe", "1440", "--timeframe", "10080",
    ])
    assert args.timeframe == list(TIMEFRAMES)

    with pytest.raises(SystemExit):
        parse_args([
            "--database-url", "postgresql://audit",
            "--timeframe", "5", "--timeframe", "30",
        ])


class EvidenceConnection:
    observed_at = datetime(2026, 7, 18, 1, 2, 3, tzinfo=timezone.utc)

    async def fetchrow(self, sql: str, *args: object):
        normalized = " ".join(sql.lower().split())
        if "pg_current_wal_lsn" in normalized:
            return {"observed_at": self.observed_at, "evidence_lsn": "0/16B6C50"}
        if "from kline_scope_catalog_control" in normalized:
            return {
                "generation_id": uuid4(),
                "revision": 9,
                "status": "complete",
                "expected_scope_count": 14,
                "symbol_ids": [1, 2],
                "timeframes": [5, 15, 30, 60, 1440, 10080, 43200],
            }
        raise AssertionError(sql)

    async def fetch(self, sql: str, *args: object):
        normalized = " ".join(sql.lower().split())
        if "from symbols" in normalized:
            return [
                {"symbol_id": 1, "code": "000001", "exchange": "SZ"},
                {"symbol_id": 2, "code": "600000", "exchange": "SH"},
            ]
        if "from kline_scope_catalog" in normalized:
            generation_id, symbol_ids, timeframes = args
            assert generation_id is not None
            return [
                {
                    "symbol_id": symbol_id,
                    "timeframe": timeframe,
                    "state": "present",
                    "bounds_complete": True,
                    "min_ts": self.observed_at,
                    "max_ts": self.observed_at,
                    "updated_at": self.observed_at,
                }
                for symbol_id in symbol_ids
                for timeframe in timeframes
            ]
        raise AssertionError(sql)


def test_snapshot_evidence_is_exact_ordered_and_complete() -> None:
    evidence = asyncio.run(_capture_snapshot_evidence(EvidenceConnection()))

    assert evidence["contract_version"] == "module-c-strict-audit-v2"
    assert evidence["apply_mode"] is False
    assert evidence["timeframes"] == list(TIMEFRAMES)
    assert evidence["observed_at"] == "2026-07-18T01:02:03+00:00"
    assert evidence["evidence_lsn"] == "0/16B6C50"
    assert evidence["active_universe_count"] == 2
    assert len(evidence["active_universe_sha256"]) == 64
    assert evidence["catalog_control_revision"] == 9
    assert evidence["catalog_required_scope_count"] == 10
    assert len(evidence["catalog_manifest_sha256"]) == 64
    assert len(evidence["evidence_sha256"]) == 64


def test_snapshot_evidence_rejects_incomplete_active_catalog() -> None:
    class IncompleteEvidenceConnection(EvidenceConnection):
        async def fetch(self, sql: str, *args: object):
            rows = await super().fetch(sql, *args)
            if "from kline_scope_catalog" in " ".join(sql.lower().split()):
                return rows[:-1]
            return rows

    with pytest.raises(InvalidAuditEvidence, match="catalog scope manifest"):
        asyncio.run(_capture_snapshot_evidence(IncompleteEvidenceConnection()))


def test_snapshot_evidence_allows_inactive_generation_superset() -> None:
    class SupersetEvidenceConnection(EvidenceConnection):
        async def fetchrow(self, sql: str, *args: object):
            row = await super().fetchrow(sql, *args)
            if "from kline_scope_catalog_control" in " ".join(sql.lower().split()):
                return {**row, "expected_scope_count": 21, "symbol_ids": [1, 2, 99]}
            return row

    evidence = asyncio.run(_capture_snapshot_evidence(SupersetEvidenceConnection()))

    assert evidence["active_universe_count"] == 2
    assert evidence["catalog_expected_scope_count"] == 21
    assert evidence["catalog_required_scope_count"] == 10


class ClaimConnection:
    def __init__(self, status: str | None) -> None:
        self.status = status
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, sql: str, *args: object):
        assert "for update" in sql.lower()
        return None if self.status is None else {"status": self.status}

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((" ".join(sql.lower().split()), args))
        return "DELETE 4" if sql.lower().lstrip().startswith("delete") else "UPDATE 1"


@pytest.mark.parametrize("status", ["running", "completed"])
def test_claim_never_resets_running_or_completed_audit(status: str) -> None:
    connection = ClaimConnection(status)
    with pytest.raises(AuditRunAlreadyClaimed, match=status):
        asyncio.run(_claim_audit_run(connection, uuid4()))
    assert connection.executed == []


def test_claim_may_reuse_failed_uuid_only_after_clearing_checkpoints() -> None:
    connection = ClaimConnection("failed")
    asyncio.run(_claim_audit_run(connection, uuid4()))

    statements = [sql for sql, _args in connection.executed]
    assert statements[0].startswith("delete from kline_audit_checkpoints")
    assert statements[1].startswith("update kline_audit_runs")
    assert "apply_mode=false" in statements[1]


def _summary_record(*, checkpoints: int, eligible: int, unresolved: int) -> dict[str, int]:
    return {
        "checkpoints": checkpoints,
        "rows_scanned": 100,
        "eligible": eligible,
        "unresolved": unresolved,
        "invalid_ohlc": 0,
        "negative_volume": 0,
        "negative_amount": 0,
        "illegal_sessions": 0,
        "incomplete_rows": 0,
        "logical_duplicate_rows": 0,
        "unexpected_source": 0,
        "current_open_periods": 0,
        "timestamp_mismatches": 0,
        "missing_daily_basis": 0,
        "missing_higher_periods": 0,
        "missing_rows": 0,
    }


class FinalConnection:
    def __init__(self, record: dict[str, int]) -> None:
        self.record = record
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, sql: str, *args: object):
        assert "from kline_audit_checkpoints" in sql.lower()
        return self.record

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((" ".join(sql.lower().split()), args))
        return "UPDATE 1"


def test_finalization_fails_closed_when_checkpoint_contract_is_incomplete() -> None:
    connection = FinalConnection(_summary_record(checkpoints=9, eligible=9, unresolved=0))
    evidence = {
        "contract_version": "module-c-strict-audit-v2",
        "observed_at": "2026-07-18T01:02:03+00:00",
        "evidence_lsn": "0/16B6C50",
        "evidence_sha256": "a" * 64,
        "active_universe_count": 2,
    }

    with pytest.raises(InvalidAuditEvidence, match="checkpoints=9 expected=10"):
        asyncio.run(_finalize_audit_run(connection, uuid4(), evidence))

    assert len(connection.executed) == 1
    sql, args = connection.executed[0]
    assert "set status='failed'" in sql
    assert "status='running'" in sql
    assert "checkpoints=9 expected=10" in args[3]


def test_finalization_requires_every_checkpoint_disposition() -> None:
    connection = FinalConnection(_summary_record(checkpoints=10, eligible=8, unresolved=1))
    evidence = {
        "contract_version": "module-c-strict-audit-v2",
        "observed_at": "2026-07-18T01:02:03+00:00",
        "evidence_lsn": "0/16B6C50",
        "evidence_sha256": "a" * 64,
        "active_universe_count": 2,
    }

    with pytest.raises(InvalidAuditEvidence, match="dispositions=9 checkpoints=10"):
        asyncio.run(_finalize_audit_run(connection, uuid4(), evidence))

    assert "set status='failed'" in connection.executed[0][0]


def test_coordinator_connect_failure_marks_claimed_run_failed(monkeypatch) -> None:
    class SetupConnection(ClaimConnection):
        def __init__(self) -> None:
            super().__init__(None)

        async def fetchrow(self, sql: str, *args: object):
            if "pg_constraint" in sql.lower():
                return {
                    "convalidated": True,
                    "definition": "PRIMARY KEY (symbol_id, timeframe, ts)",
                }
            return await super().fetchrow(sql, *args)

        async def close(self) -> None:
            return None

    class FailureConnection:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[object, ...]]] = []

        async def execute(self, sql: str, *args: object) -> str:
            self.executed.append((" ".join(sql.lower().split()), args))
            return "UPDATE 1"

        async def close(self) -> None:
            return None

    setup = SetupConnection()
    failure = FailureConnection()
    connections = 0

    async def connect(_database_url: str):
        nonlocal connections
        connections += 1
        if connections == 1:
            return setup
        if connections == 2:
            raise OSError("coordinator unavailable")
        if connections == 3:
            return failure
        raise AssertionError("unexpected connection")

    monkeypatch.setattr(gate.asyncpg, "connect", connect)

    with pytest.raises(OSError, match="coordinator unavailable"):
        asyncio.run(gate.run_gate("postgresql://audit", str(uuid4())))

    assert connections == 3
    assert len(failure.executed) == 1
    sql, args = failure.executed[0]
    assert "set status='failed'" in sql
    assert "status='running'" in sql
    assert args[1] == "coordinator unavailable"
