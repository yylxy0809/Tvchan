from __future__ import annotations

import asyncio
import json
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
        assert "c.generation_id=$3::uuid" in sql
        assert "'catalog_empty_has_rows'" in sql
        assert "'catalog_present_missing_rows'" in sql
        assert "'catalog_present_bounds_mismatch'" in sql
        assert "'catalog_scope_missing'" in sql
        assert "'catalog_scope_unknown'" in sql
        assert "'catalog_scope_incomplete'" in sql
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


def test_catalog_scope_is_cross_checked_inside_each_imported_snapshot() -> None:
    sql = " ".join(build_gate_sql(5).lower().split())

    assert "catalog_scope as materialized" in sql
    assert "join kline_scope_catalog c" in sql
    assert "c.generation_id=$3::uuid" in sql
    assert "c.state='empty' and m.rows_scanned > 0" in sql
    assert "c.state='present' and coalesce(m.rows_scanned,0)=0" in sql
    assert "c.state='present' and coalesce(m.rows_scanned,0)>0" in sql
    assert "c.min_ts is distinct from m.shard_start" in sql
    assert "c.max_ts is distinct from m.shard_end" in sql
    assert "c.symbol_id is null then 1 else 0 end::bigint as catalog_scope_missing" in sql
    assert "c.state is distinct from 'present'" in sql
    assert "c.state is distinct from 'empty'" in sql
    assert "c.bounds_complete is distinct from true" in sql


def test_catalog_empty_scope_without_rows_is_unresolved() -> None:
    sql = " ".join(build_gate_sql(5).lower().split())

    assert "case when m.rows_scanned=0 then 1 else 0 end" in sql
    assert "case when anomaly_total=0 then 'eligible' else 'unresolved' end" in sql


def test_worker_binds_captured_generation_to_snapshot_query(monkeypatch) -> None:
    class Transaction:
        async def start(self) -> None:
            return None

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

    class Connection:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[object, ...]]] = []

        def transaction(self, **_kwargs: object) -> Transaction:
            return Transaction()

        async def execute(
            self,
            sql: str,
            *args: object,
            **_kwargs: object,
        ) -> str:
            self.executed.append((sql, args))
            return "OK"

        async def close(self) -> None:
            return None

    connection = Connection()

    async def connect(_database_url: str) -> Connection:
        return connection

    monkeypatch.setattr(gate.asyncpg, "connect", connect)
    run_id = uuid4()
    generation_id = uuid4()
    observed_at = datetime(2026, 7, 18, 1, 2, 3, tzinfo=timezone.utc)

    asyncio.run(gate._worker(
        "postgresql://audit",
        "00000003-0000001B-1",
        str(run_id),
        5,
        observed_at,
        generation_id,
    ))

    sql, args = connection.executed[-1]
    assert "c.generation_id=$3::uuid" in sql
    assert args == (run_id, observed_at, generation_id)


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
        "catalog_empty_has_rows": 0,
        "catalog_present_missing_rows": 0,
        "catalog_present_bounds_mismatch": 0,
        "catalog_scope_missing": 0,
        "catalog_scope_unknown": 0,
        "catalog_scope_incomplete": 0,
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

    for field in (
        "catalog_empty_has_rows",
        "catalog_present_missing_rows",
        "catalog_present_bounds_mismatch",
        "catalog_scope_missing",
        "catalog_scope_unknown",
        "catalog_scope_incomplete",
    ):
        status, summary = summarize(dict(clean, **{field: 1}, eligible=4, unresolved=1))
        assert status == "completed"
        assert summary["anomaly_total"] == 1
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
            return {
                "observed_at": self.observed_at,
                "observed_wal_lsn": "0/16B6C50",
                "transaction_snapshot": "100:100:",
            }
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
    assert evidence["observed_wal_lsn"] == "0/16B6C50"
    assert evidence["transaction_snapshot"] == "100:100:"
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
    def __init__(self, status: str | None, parameters: object = None) -> None:
        self.status = status
        self.parameters = parameters
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, sql: str, *args: object):
        assert "for update" in sql.lower()
        return None if self.status is None else {
            "status": self.status,
            "parameters": self.parameters,
        }

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((" ".join(sql.lower().split()), args))
        return "DELETE 4" if sql.lower().lstrip().startswith("delete") else "UPDATE 1"


def test_claim_never_resets_completed_audit() -> None:
    connection = ClaimConnection("completed")
    with pytest.raises(AuditRunAlreadyClaimed, match="completed"):
        asyncio.run(_claim_audit_run(connection, uuid4(), str(uuid4())))
    assert connection.executed == []


@pytest.mark.parametrize("status", ["running", "failed"])
def test_locked_claim_recovers_incomplete_run_after_clearing_checkpoints(
    status: str,
) -> None:
    parameters = (
        {"lock_protocol_version": gate.AUDIT_LOCK_PROTOCOL_VERSION}
        if status == "running" else None
    )
    connection = ClaimConnection(status, parameters)
    owner_id = str(uuid4())
    asyncio.run(_claim_audit_run(connection, uuid4(), owner_id))

    statements = [sql for sql, _args in connection.executed]
    assert statements[0].startswith("delete from kline_audit_checkpoints")
    assert statements[1].startswith("update kline_audit_runs")
    assert "apply_mode=false" in statements[1]
    assert "status in ('running','failed')" in statements[1]
    pending = json.loads(connection.executed[1][1][1])
    assert pending["lock_protocol_version"] == gate.AUDIT_LOCK_PROTOCOL_VERSION
    assert pending["lock_owner_id"] == owner_id


@pytest.mark.parametrize("parameters", [None, {}, {"lock_protocol_version": "legacy"}])
def test_locked_claim_refuses_legacy_running_without_operator_confirmation(
    parameters: object,
) -> None:
    connection = ClaimConnection("running", parameters)

    with pytest.raises(AuditRunAlreadyClaimed, match="operator confirmation"):
        asyncio.run(_claim_audit_run(connection, uuid4(), str(uuid4())))

    assert connection.executed == []


def test_concurrent_same_uuid_is_rejected_by_advisory_lock(monkeypatch) -> None:
    class LockedConnection:
        def __init__(self) -> None:
            self.closed = False

        async def fetchval(self, sql: str, *_args: object) -> bool:
            assert "pg_try_advisory_lock" in sql.lower()
            return False

        async def fetchrow(self, _sql: str, *_args: object):
            raise AssertionError("schema and claim must not run without the UUID lock")

        async def close(self) -> None:
            self.closed = True

    setup = LockedConnection()

    async def connect(_database_url: str) -> LockedConnection:
        return setup

    monkeypatch.setattr(gate.asyncpg, "connect", connect)
    run_id = str(uuid4())

    with pytest.raises(AuditRunAlreadyClaimed, match=run_id):
        asyncio.run(gate.run_gate("postgresql://audit", run_id))

    assert setup.closed is True


class RunGateSetupConnection(ClaimConnection):
    def __init__(self, *, heartbeat_error: Exception | None = None, unlock: bool = True):
        super().__init__(None)
        self.heartbeat_error = heartbeat_error
        self.unlock = unlock
        self.calls: list[str] = []
        self.closed = False

    async def fetchval(self, sql: str, *_args: object):
        normalized = " ".join(sql.lower().split())
        self.calls.append(normalized)
        if "pg_try_advisory_lock" in normalized:
            return True
        if normalized == "select 1":
            if self.heartbeat_error is not None:
                raise self.heartbeat_error
            return 1
        if "pg_advisory_unlock" in normalized:
            return self.unlock
        raise AssertionError(sql)

    async def fetchrow(self, sql: str, *args: object):
        if "pg_constraint" in sql.lower():
            return {
                "convalidated": True,
                "definition": "PRIMARY KEY (symbol_id, timeframe, ts)",
            }
        return await super().fetchrow(sql, *args)

    async def close(self) -> None:
        self.closed = True


def test_watchdog_failure_cancels_claimed_gate(monkeypatch) -> None:
    setup = RunGateSetupConnection(heartbeat_error=ConnectionError("lock session lost"))
    gate_cancelled = False

    async def connect(_database_url: str) -> RunGateSetupConnection:
        return setup

    async def blocked_gate(*_args: object):
        nonlocal gate_cancelled
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            gate_cancelled = True
            raise

    monkeypatch.setattr(gate.asyncpg, "connect", connect)
    monkeypatch.setattr(gate, "_run_claimed_gate", blocked_gate)

    with pytest.raises(gate.AuditLockOwnershipLost, match="watchdog"):
        asyncio.run(gate.run_gate("postgresql://audit", str(uuid4())))

    assert gate_cancelled is True
    assert any("pg_advisory_unlock" in sql for sql in setup.calls)
    assert setup.closed is True


def test_unlock_false_reports_ownership_lost(monkeypatch) -> None:
    setup = RunGateSetupConnection(unlock=False)

    async def connect(_database_url: str) -> RunGateSetupConnection:
        return setup

    async def completed_gate(*_args: object):
        return "audit-id", {"gate_pass": True}

    monkeypatch.setattr(gate.asyncpg, "connect", connect)
    monkeypatch.setattr(gate, "_run_claimed_gate", completed_gate)

    with pytest.raises(gate.AuditLockOwnershipLost, match="unlock"):
        asyncio.run(gate.run_gate("postgresql://audit", str(uuid4())))

    assert setup.closed is True


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
        "catalog_empty_has_rows": 0,
        "catalog_present_missing_rows": 0,
        "catalog_present_bounds_mismatch": 0,
        "catalog_scope_missing": 0,
        "catalog_scope_unknown": 0,
        "catalog_scope_incomplete": 0,
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
        "observed_wal_lsn": "0/16B6C50",
        "transaction_snapshot": "100:100:",
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
        "observed_wal_lsn": "0/16B6C50",
        "transaction_snapshot": "100:100:",
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
            self.lock_calls: list[str] = []
            self.closed = False

        async def fetchval(self, sql: str, *_args: object) -> bool:
            normalized = " ".join(sql.lower().split())
            self.lock_calls.append(normalized)
            if "pg_try_advisory_lock" in normalized:
                return True
            if normalized == "select 1":
                return 1
            if "pg_advisory_unlock" in normalized:
                return True
            raise AssertionError(sql)

        async def fetchrow(self, sql: str, *args: object):
            if "pg_constraint" in sql.lower():
                return {
                    "convalidated": True,
                    "definition": "PRIMARY KEY (symbol_id, timeframe, ts)",
                }
            return await super().fetchrow(sql, *args)

        async def close(self) -> None:
            self.closed = True

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
    assert len(setup.lock_calls) == 3
    assert "pg_try_advisory_lock" in setup.lock_calls[0]
    assert setup.lock_calls[1] == "select 1"
    assert "pg_advisory_unlock" in setup.lock_calls[2]
    assert setup.closed is True
