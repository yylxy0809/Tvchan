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
from collector.module_c_eligibility import (
    FRESHNESS_CONTRACT_VERSION,
    parse_freshness_contract,
)


def _freshness_contract(*, as_of: str = "2026-07-18T08:00:00+08:00"):
    return parse_freshness_contract({
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": as_of,
        "trading_calendar": {"id": "sse-szse-2026-v1", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            "5f": "2026-07-17T15:00:00+08:00",
            "30f": "2026-07-17T15:00:00+08:00",
            "1d": "2026-07-17T15:00:00+08:00",
            "1w": "2026-07-17T15:00:00+08:00",
            "1m": "2026-06-30T15:00:00+08:00",
        },
    })


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


def test_worker_binds_captured_generation_to_snapshot_query() -> None:
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

    run_id = uuid4()
    generation_id = uuid4()
    observed_at = datetime(2026, 7, 18, 1, 2, 3, tzinfo=timezone.utc)

    asyncio.run(gate._worker(
        connection,
        "00000003-0000001B-1",
        str(run_id),
        5,
        observed_at,
        generation_id,
    ))

    sql, args = connection.executed[-1]
    assert "c.generation_id=$3::uuid" in sql
    assert args == (run_id, observed_at, generation_id, None)


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


def test_higher_periods_share_explicit_closed_cutoff_contract_with_aggregator() -> None:
    for timeframe in (10080, 43200):
        sql = " ".join(build_gate_sql(timeframe).lower().split())

        assert "$4::timestamptz is null" in sql
        assert "d.expected_ts <= $4::timestamptz" in sql
        assert "d.expected_ts > $4::timestamptz" in sql
        assert ") and b.symbol_id is null" in sql
        assert "max(ts) from klines" not in sql


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


def test_cli_accepts_exact_authoritative_freshness_contract(tmp_path) -> None:
    contract_path = tmp_path / "freshness.json"
    contract_path.write_text(
        json.dumps(_freshness_contract().normalized),
        encoding="utf-8",
    )
    args = parse_args([
        "--database-url", "postgresql://audit",
        "--freshness-contract", str(contract_path),
    ])

    assert args.freshness_contract.expected_closed_watermarks["1w"] == datetime(
        2026, 7, 17, 7, tzinfo=timezone.utc,
    )
    assert args.freshness_contract.contract_version == FRESHNESS_CONTRACT_VERSION


def test_cli_rejects_non_exact_freshness_contract(tmp_path) -> None:
    payload = _freshness_contract().normalized
    payload["expected_closed_watermarks"].pop("1m")
    contract_path = tmp_path / "freshness.json"
    contract_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(SystemExit):
        parse_args([
            "--database-url", "postgresql://audit",
            "--freshness-contract", str(contract_path),
        ])


def test_programmatic_gate_rejects_unvalidated_freshness_object_before_db() -> None:
    class Forged:
        normalized = {"contract_version": FRESHNESS_CONTRACT_VERSION}
        contract_version = FRESHNESS_CONTRACT_VERSION
        sha256 = "0" * 64

    with pytest.raises(ValueError, match="invalid authoritative freshness"):
        asyncio.run(gate.run_gate("postgresql://audit", freshness_contract=Forged()))


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


def test_snapshot_evidence_binds_authoritative_freshness_contract() -> None:
    contract = _freshness_contract()

    evidence = asyncio.run(_capture_snapshot_evidence(EvidenceConnection(), contract))

    assert evidence["freshness_contract"] == contract.normalized
    assert evidence["freshness_contract_version"] == FRESHNESS_CONTRACT_VERSION
    assert evidence["freshness_contract_sha256"] == contract.sha256
    assert evidence["trading_calendar_id"] == "sse-szse-2026-v1"
    assert evidence["trading_calendar_sha256"] == "a" * 64


def test_snapshot_evidence_rejects_future_authoritative_as_of() -> None:
    contract = _freshness_contract(as_of="2026-07-18T10:00:00+08:00")

    with pytest.raises(InvalidAuditEvidence, match="after the audit snapshot"):
        asyncio.run(_capture_snapshot_evidence(EvidenceConnection(), contract))


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


@pytest.mark.parametrize("status", ["running", "failed"])
def test_recovery_rejects_changed_freshness_contract(status: str) -> None:
    contract = _freshness_contract()
    parameters = {
        "lock_protocol_version": gate.AUDIT_LOCK_PROTOCOL_VERSION,
        "freshness_contract_version": FRESHNESS_CONTRACT_VERSION,
        "freshness_contract_sha256": "0" * 64,
    }
    connection = ClaimConnection(status, parameters)

    with pytest.raises(AuditRunAlreadyClaimed, match="freshness contract changed"):
        asyncio.run(_claim_audit_run(connection, uuid4(), str(uuid4()), contract))

    assert connection.executed == []


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


def test_claim_and_writer_fence_keys_are_distinct() -> None:
    claim_key, writer_fence_key = gate._advisory_lock_keys(uuid4())

    assert claim_key != writer_fence_key


def test_takeover_is_rejected_while_old_writer_shared_lock_exists(monkeypatch) -> None:
    class SetupConnection:
        def __init__(self) -> None:
            self.try_count = 0
            self.unlock_count = 0
            self.closed = False

        async def fetchval(self, sql: str, *_args: object) -> bool:
            normalized = " ".join(sql.lower().split())
            if "pg_try_advisory_lock" in normalized:
                self.try_count += 1
                return self.try_count == 1
            if "pg_advisory_unlock" in normalized:
                self.unlock_count += 1
                return True
            raise AssertionError(sql)

        async def fetchrow(self, _sql: str, *_args: object):
            raise AssertionError("takeover must fail before durable claim")

        async def close(self) -> None:
            self.closed = True

    setup = SetupConnection()

    async def connect(_database_url: str) -> SetupConnection:
        return setup

    monkeypatch.setattr(gate.asyncpg, "connect", connect)

    with pytest.raises(gate.AuditLockOwnershipLost, match="writer fence"):
        asyncio.run(gate.run_gate("postgresql://audit", str(uuid4())))

    assert setup.try_count == 2
    assert setup.unlock_count == 1
    assert setup.closed is True


def test_durable_claim_happens_after_five_worker_shared_locks(monkeypatch) -> None:
    events: list[str] = []

    class SetupConnection(ClaimConnection):
        def __init__(self) -> None:
            super().__init__(None)
            self.try_count = 0

        async def fetchval(self, sql: str, *_args: object):
            normalized = " ".join(sql.lower().split())
            if "pg_try_advisory_lock" in normalized:
                self.try_count += 1
                events.append(f"exclusive_{self.try_count}")
                return True
            if "pg_advisory_unlock" in normalized:
                events.append("exclusive_unlock")
                return True
            if normalized == "select 1":
                return 1
            raise AssertionError(sql)

        async def fetchrow(self, sql: str, *args: object):
            if "pg_constraint" in sql.lower():
                events.append("primary_key")
                return {
                    "convalidated": True,
                    "definition": "PRIMARY KEY (symbol_id, timeframe, ts)",
                }
            events.append("durable_claim")
            return await super().fetchrow(sql, *args)

        async def close(self) -> None:
            events.append("setup_close")

    class WorkerConnection:
        def __init__(self, index: int) -> None:
            self.index = index

        async def fetchval(self, sql: str, *_args: object) -> bool:
            normalized = " ".join(sql.lower().split())
            if "pg_try_advisory_lock_shared" in normalized:
                events.append(f"shared_{self.index}")
                return True
            if "pg_advisory_unlock_shared" in normalized:
                events.append(f"shared_unlock_{self.index}")
                return True
            raise AssertionError(sql)

        async def close(self) -> None:
            events.append(f"worker_close_{self.index}")

    setup = SetupConnection()
    workers = [WorkerConnection(index) for index in range(5)]
    connections: list[object] = [setup, *workers]

    async def connect(_database_url: str):
        return connections.pop(0)

    async def completed_gate(*_args: object):
        return "audit-id", {"gate_pass": True}

    monkeypatch.setattr(gate.asyncpg, "connect", connect)
    monkeypatch.setattr(gate, "_run_with_lock_watchdog", completed_gate)

    asyncio.run(gate.run_gate("postgresql://audit", str(uuid4())))

    claim_index = events.index("durable_claim")
    assert all(events.index(f"shared_{index}") < claim_index for index in range(5))
    assert events.index("exclusive_unlock") < events.index("shared_0")
    assert events.count("exclusive_unlock") == 2
    for index in range(5):
        assert f"shared_unlock_{index}" in events
        assert f"worker_close_{index}" in events
    assert events[-1] == "setup_close"


def test_finalizer_mutation_holds_shared_writer_fence(monkeypatch) -> None:
    events: list[str] = []

    class Connection:
        async def fetchval(self, sql: str, *_args: object) -> bool:
            normalized = " ".join(sql.lower().split())
            if "pg_try_advisory_lock_shared" in normalized:
                events.append("shared_lock")
                return True
            if "pg_advisory_unlock_shared" in normalized:
                events.append("shared_unlock")
                return True
            raise AssertionError(sql)

        async def close(self) -> None:
            events.append("close")

    connection = Connection()

    async def connect(_database_url: str) -> Connection:
        return connection

    async def finalize(_connection: Connection) -> str:
        events.append("finalize")
        return "completed"

    monkeypatch.setattr(gate.asyncpg, "connect", connect)

    result = asyncio.run(gate._run_fenced_writer("postgresql://audit", 42, finalize))

    assert result == "completed"
    assert events == ["shared_lock", "finalize", "shared_unlock", "close"]


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
            self.events: list[str] = []

        async def fetchval(self, sql: str, *_args: object) -> bool:
            normalized = " ".join(sql.lower().split())
            if "pg_try_advisory_lock_shared" in normalized:
                self.events.append("shared_lock")
                return True
            if "pg_advisory_unlock_shared" in normalized:
                self.events.append("shared_unlock")
                return True
            raise AssertionError(sql)

        async def execute(self, sql: str, *args: object) -> str:
            self.events.append("execute")
            self.executed.append((" ".join(sql.lower().split()), args))
            return "UPDATE 1"

        async def close(self) -> None:
            self.events.append("close")

    class WorkerConnection:
        async def fetchval(self, sql: str, *_args: object) -> bool:
            normalized = " ".join(sql.lower().split())
            if "pg_try_advisory_lock_shared" in normalized:
                return True
            if "pg_advisory_unlock_shared" in normalized:
                return True
            raise AssertionError(sql)

        async def close(self) -> None:
            return None

    setup = SetupConnection()
    workers = [WorkerConnection() for _index in range(5)]
    failure = FailureConnection()
    connections = 0

    async def connect(_database_url: str):
        nonlocal connections
        connections += 1
        if connections == 1:
            return setup
        if 2 <= connections <= 6:
            return workers[connections - 2]
        if connections == 7:
            raise OSError("coordinator unavailable")
        if connections == 8:
            return failure
        raise AssertionError("unexpected connection")

    monkeypatch.setattr(gate.asyncpg, "connect", connect)

    with pytest.raises(OSError, match="coordinator unavailable"):
        asyncio.run(gate.run_gate("postgresql://audit", str(uuid4())))

    assert connections == 8
    assert len(failure.executed) == 1
    sql, args = failure.executed[0]
    assert "set status='failed'" in sql
    assert "status='running'" in sql
    assert args[1] == "coordinator unavailable"
    assert failure.events == ["shared_lock", "execute", "shared_unlock", "close"]
    assert len(setup.lock_calls) == 5
    assert "pg_try_advisory_lock" in setup.lock_calls[0]
    assert "pg_try_advisory_lock" in setup.lock_calls[1]
    assert any(sql == "select 1" for sql in setup.lock_calls)
    assert sum("pg_advisory_unlock" in sql for sql in setup.lock_calls) == 2
    assert setup.closed is True
