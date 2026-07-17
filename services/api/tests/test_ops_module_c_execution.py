from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.main import create_app
from app.repositories.module_c_ops import (
    ACTIVE_CATALOG_SQL,
    BATCH_SQL,
    CHECKPOINT_EVIDENCE_SQL,
    FRESHNESS_SQL,
    LIVE_UNIVERSE_SQL,
    RUNNING_COUNTS_SQL,
    TASK_HEALTH_SQL,
    TASK_PROGRESS_SQL,
    get_module_c_execution_status,
)


NOW = datetime(2026, 7, 18, 1, 0, tzinfo=UTC)


class FakeConnection:
    def __init__(
        self,
        *,
        batch: dict | None = None,
        task_rows: list[dict] | None = None,
        checkpoint_scopes: int = 20,
        future_scopes: int = 0,
    ) -> None:
        self.batch = batch
        self.task_rows = list(task_rows or [])
        self.checkpoint_scopes = checkpoint_scopes
        self.future_scopes = future_scopes
        self.queries: list[str] = []
        self.transaction_args: dict[str, object] | None = None

    @asynccontextmanager
    async def transaction(self, **kwargs):
        self.transaction_args = kwargs
        yield

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        self.queries.append(normalized)
        if "module-c-execution:running-counts" in normalized:
            return {
                "observed_at": NOW,
                "running_parent_batches": 1,
                "running_child_batches": 1,
                "running_tasks": 2,
            }
        if "module-c-execution:batch" in normalized:
            return self.batch
        if "module-c-execution:task-health" in normalized:
            return {
                "retryable_failed": 1,
                "exhausted_failed": 2,
                "expired_leases": 3,
            }
        raise AssertionError(query)

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        self.queries.append(normalized)
        if "module-c-execution:tasks" in normalized:
            return self.task_rows
        if "module-c-execution:freshness" in normalized:
            expected = args[3]
            return [
                {
                    "chan_level": code,
                    "timeframe": timeframe,
                    "expected_closed_watermark": expected[index],
                    "actual_min": NOW,
                    "actual_max": expected[index],
                    "empty_scopes": 0,
                    "stale_scopes": 0,
                    "future_scopes": self.future_scopes,
                    "checkpoint_scopes": self.checkpoint_scopes,
                }
                for index, (code, timeframe) in enumerate(
                    ((5, "5f"), (30, "30f"), (1440, "1d"), (10080, "1w"), (43200, "1m"))
                )
            ]
        if "module-c-execution:checkpoint-evidence" in normalized:
            return list((self.batch or {}).get("_checkpoint_rows", []))
        if "module-c-execution:live-universe" in normalized:
            return list((self.batch or {}).get("_live_universe_rows", []))
        if "module-c-execution:active-catalog" in normalized:
            return list((self.batch or {}).get("_active_catalog_rows", []))
        raise AssertionError(query)

    async def execute(self, *_args, **_kwargs):
        raise AssertionError("read-only execution status must not execute SQL")


class AcquireContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_args) -> None:
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.acquires = 0

    def acquire(self) -> AcquireContext:
        self.acquires += 1
        return AcquireContext(self.connection)

    async def fetchrow(self, *_args):
        # Authentication may check the durable user-token store. It must not
        # acquire the execution-status snapshot before rejecting the request.
        return None


LEVELS = ((5, "5f"), (30, "30f"), (1440, "1d"), (10080, "1w"), (43200, "1m"))
ANOMALY_FIELDS = (
    "invalid_ohlc", "negative_volume", "negative_amount", "illegal_sessions",
    "incomplete_rows", "logical_duplicate_rows", "unexpected_source",
    "current_open_periods", "timestamp_mismatches", "missing_daily_basis",
    "missing_higher_periods", "catalog_empty_has_rows",
    "catalog_present_missing_rows", "catalog_present_bounds_mismatch",
    "catalog_scope_missing", "catalog_scope_unknown", "catalog_scope_incomplete",
    "missing_rows",
)


def _manifest_sha256(records: list[dict]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(
            json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _batch(*, audit_active_universe_count: int = 20, **overrides) -> dict:
    live_universe_rows = [
        {"symbol_id": symbol_id, "code": f"{symbol_id:06d}", "exchange": "SH"}
        for symbol_id in range(1, audit_active_universe_count + 1)
    ]
    active_universe_sha = _manifest_sha256([
        {"symbol_id": row["symbol_id"], "symbol": f"{row['code']}.{row['exchange']}"}
        for row in live_universe_rows
    ])
    active_catalog_rows = [
        {
            "symbol_id": symbol_id,
            "timeframe": code,
            "state": "present",
            "bounds_complete": True,
            "min_ts": NOW,
            "max_ts": NOW,
            "updated_at": NOW,
        }
        for symbol_id in range(1, audit_active_universe_count + 1)
        for code, _timeframe in LEVELS
    ]
    catalog_sha = _manifest_sha256([
        {
            "symbol_id": row["symbol_id"],
            "timeframe": row["timeframe"],
            "state": row["state"],
            "bounds_complete": True,
            "min_ts": row["min_ts"].isoformat(),
            "max_ts": row["max_ts"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
        }
        for row in active_catalog_rows
    ])
    freshness_contract = {
        "contract_version": "module-c-authoritative-freshness-v1",
        "as_of": NOW.isoformat(),
        "trading_calendar": {"id": "sse", "sha256": "9" * 64},
        "expected_closed_watermarks": {
            timeframe: NOW.isoformat() for _code, timeframe in LEVELS
        },
    }
    freshness_sha = _manifest_sha256([freshness_contract])
    checkpoint_rows = []
    checkpoint_manifest = []
    zero_anomalies = {field: 0 for field in ANOMALY_FIELDS}
    for symbol_id in range(1, audit_active_universe_count + 1):
        for code, _timeframe in LEVELS:
            checkpoint_rows.append({
                "symbol_id": symbol_id,
                "timeframe": code,
                "status": "completed",
                "shard_start": NOW,
                "shard_end": NOW,
                "rows_scanned": 1,
                "metadata": {"disposition": "eligible", **zero_anomalies},
            })
            checkpoint_manifest.append({
                "symbol_id": symbol_id,
                "timeframe": code,
                "status": "completed",
                "actual_rows": 1,
                "actual_shard_start": NOW.isoformat(),
                "actual_shard_end": NOW.isoformat(),
                "disposition": "eligible",
                "anomaly_total": 0,
                **zero_anomalies,
            })
    checkpoint_sha = _manifest_sha256(checkpoint_manifest)
    audit_parameters = {
        "contract_version": "module-c-strict-audit-v2",
        "engine": "sql_gate",
        "apply_mode": False,
        "timeframes": [code for code, _timeframe in LEVELS],
        "observed_at": NOW.isoformat(),
        "observed_wal_lsn": "0/1",
        "transaction_snapshot": "1:2:",
        "active_universe_count": audit_active_universe_count,
        "active_universe_sha256": active_universe_sha,
        "catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "catalog_control_revision": 7,
        "catalog_expected_scope_count": audit_active_universe_count * 5,
        "catalog_required_scope_count": audit_active_universe_count * 5,
        "catalog_manifest_sha256": catalog_sha,
    }
    audit_parameters["evidence_sha256"] = _manifest_sha256([audit_parameters])
    audit_summary = {
        "evidence_complete": True,
        "evidence_sha256": audit_parameters["evidence_sha256"],
        "checkpoints": audit_active_universe_count * 5,
        "rows_scanned": audit_active_universe_count * 5,
        "eligible": audit_active_universe_count * 5,
        "unresolved": 0,
        "anomaly_total": 0,
        "gate_pass": True,
        **zero_anomalies,
    }
    provenance = {
        "canonical_audit_run_id": "22222222-2222-2222-2222-222222222222",
        "audit_evidence_sha256": audit_parameters["evidence_sha256"],
        "audit_checkpoint_sha256": checkpoint_sha,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": freshness_sha,
        "catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": catalog_sha,
        "audit_active_universe_sha256": active_universe_sha,
    }
    build_parameters = {
        "policy": "strict-v2",
        "freshness_contract": freshness_contract,
        **provenance,
    }
    value = {
        "batch_id": 42,
        "batch_key": "canary-42",
        "batch_kind": "canary",
        "parent_status": "running",
        "child_status": "running",
        "publication_namespace": "production",
        "child_publication_namespace": "production",
        "profile_id": "module-c-native-5lvl",
        "child_profile_id": "module-c-native-5lvl",
        "run_group_id": "run-42",
        "child_run_group_id": "run-42",
        "code_commit": "a" * 40,
        "image_digest": "sha256:image",
        "vendor_manifest_sha256": "b" * 64,
        "config_hash": "config",
        "child_config_hash": "config",
        "created_at": NOW,
        "started_at": NOW,
        "finished_at": None,
        "updated_at": NOW,
        "shard_count": 4,
        "active_symbols": 20,
        "disposition_rows": 100,
        "latest_task_update": NOW,
        "frozen_effective_config": {
            "contract": "module-c-native-five-level-v1",
            "levels": ["5f", "30f", "1d", "1w", "1m"],
            "modes": ["confirmed", "predictive"],
            "concurrency_per_worker": 1,
            "shard_count": 4,
            "eligibility_build_id": "11111111-1111-1111-1111-111111111111",
            "max_attempts": 3,
        },
        "frozen_contract": "module-c-native-five-level-v1",
        "frozen_levels": ["5f", "30f", "1d", "1w", "1m"],
        "frozen_modes": ["confirmed", "predictive"],
        "frozen_concurrency_per_worker": 1,
        "frozen_shard_count": 4,
        "frozen_max_attempts": 3,
        "frozen_eligibility_build_id": "11111111-1111-1111-1111-111111111111",
        "freshness_as_of": NOW,
        "freshness_expected_5f": NOW,
        "freshness_expected_30f": NOW,
        "freshness_expected_1d": NOW,
        "freshness_expected_1w": NOW,
        "freshness_expected_1m": NOW,
        "policy": "strict-v2",
        "eligibility_build_id": "11111111-1111-1111-1111-111111111111",
        "manifest_version": "strict-v2-42",
        "eligibility_manifest_sha256": "c" * 64,
        "build_manifest_sha256": "c" * 64,
        "build_config_hash": "config",
        "canonical_audit_run_id": "22222222-2222-2222-2222-222222222222",
        "audit_evidence_sha256": provenance["audit_evidence_sha256"],
        "audit_checkpoint_sha256": provenance["audit_checkpoint_sha256"],
        "audit_status": "completed",
        "audit_apply_mode": False,
        "audit_active_universe_count": audit_active_universe_count,
        "audit_parameters": audit_parameters,
        "audit_summary": audit_summary,
        "build_parameters": build_parameters,
        "build_active_universe_sha256": active_universe_sha,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": freshness_sha,
        "catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": catalog_sha,
        "audit_active_universe_sha256": active_universe_sha,
        "catalog_generation_status": "complete",
        "live_catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "live_catalog_control_revision": 7,
        "_checkpoint_rows": checkpoint_rows,
        "_live_universe_rows": live_universe_rows,
        "_active_catalog_rows": active_catalog_rows,
    }
    value.update(overrides)
    return value


def test_repository_returns_structured_bound_provenance_in_one_readonly_snapshot() -> None:
    connection = FakeConnection(
        batch=_batch(),
        task_rows=[{
            "chan_level": 5,
            "status": "running",
            "count": 2,
            "attempts": 2,
            "bars": 100,
            "strokes": 10,
            "segments": 4,
            "centers": 2,
            "signals": 1,
            "latest_update": NOW,
        }],
    )

    result = asyncio.run(get_module_c_execution_status(connection, batch_id=None))

    assert result["observed_at"] == NOW
    assert result["readonly"] is True
    assert result["running_tasks"] == 2
    assert result["batch"]["execution"]["tasks"][0]["bars"] == 100
    assert result["batch"]["execution"]["retryable_failed"] == 1
    assert result["batch"]["execution"]["exhausted_failed"] == 2
    assert result["batch"]["execution"]["expired_leases"] == 3
    assert result["batch"]["frozen_config"]["max_attempts"] == 3
    assert result["batch"]["freshness"]["status"] == "current"
    assert len(result["batch"]["freshness"]["actual_checkpoint_watermarks"]) == 5
    provenance = result["batch"]["provenance"]
    assert provenance["evidence_complete"] is True
    assert provenance["frozen_config_matches"] is True
    assert provenance["catalog_is_active"] is True
    assert provenance["catalog_revision_matches"] is True
    assert provenance["eligibility_manifest_matches"] is True
    assert provenance["config_hash_matches"] is True
    assert provenance["execution_identity_matches"] is True
    assert provenance["drift_reasons"] == []
    assert "notes" not in result["batch"]
    assert "last_error" not in result["batch"]["execution"]["tasks"][0]


def test_repository_sql_is_read_only_and_freshness_uses_only_pinned_checkpoints() -> None:
    sql = " ".join(
        (
            RUNNING_COUNTS_SQL,
            BATCH_SQL,
            TASK_PROGRESS_SQL,
            TASK_HEALTH_SQL,
            FRESHNESS_SQL,
            CHECKPOINT_EVIDENCE_SQL,
            LIVE_UNIVERSE_SQL,
            ACTIVE_CATALOG_SQL,
        )
    ).lower()
    for forbidden in (
        " insert ",
        " update ",
        " delete ",
        " for update",
        "pg_advisory",
        " from klines",
        " join klines",
    ):
        assert forbidden not in sql
    assert "join kline_audit_checkpoints" in FRESHNESS_SQL.lower()
    assert "audit.parameters as audit_parameters" in BATCH_SQL.lower()
    assert "lease_until is null" in TASK_HEALTH_SQL.lower()
    assert "checkpoint.shard_end > levels.expected_closed_watermark" in FRESHNESS_SQL


def test_repository_reports_drift_as_facts_without_claiming_freshness() -> None:
    connection = FakeConnection(
        batch=_batch(
            audit_status="failed",
            catalog_generation_status="superseded",
            live_catalog_generation_id="44444444-4444-4444-4444-444444444444",
            live_catalog_control_revision=8,
            build_manifest_sha256="0" * 64,
            build_config_hash="other",
        )
    )

    result = asyncio.run(get_module_c_execution_status(connection, batch_id=42))
    provenance = result["batch"]["provenance"]

    assert provenance["catalog_is_active"] is False
    assert provenance["catalog_revision_matches"] is False
    assert provenance["eligibility_manifest_matches"] is False
    assert provenance["config_hash_matches"] is False
    assert set(provenance["drift_reasons"]) == {
        "canonical_audit_not_completed",
        "catalog_generation_not_complete",
        "catalog_generation_not_active",
        "catalog_control_revision_drift",
        "eligibility_manifest_drift",
        "config_hash_drift",
        "freshness_evidence_unavailable",
    }
    assert "fresh" not in provenance


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("child_config_hash", "other"),
        ("child_run_group_id", "other"),
        ("child_publication_namespace", "other"),
        ("child_profile_id", "other"),
        ("batch_kind", "historical"),
    ],
)
def test_execution_identity_drift_is_fail_visible(field, value) -> None:
    result = asyncio.run(
        get_module_c_execution_status(
            FakeConnection(batch=_batch(**{field: value})), batch_id=42
        )
    )

    provenance = result["batch"]["provenance"]
    assert provenance["execution_identity_matches"] is False
    assert provenance["evidence_complete"] is False
    assert "execution_identity_drift" in provenance["drift_reasons"]
    if field == "child_config_hash":
        assert provenance["config_hash_matches"] is False
        assert "config_hash_drift" in provenance["drift_reasons"]


def test_freshness_uses_pinned_audit_universe_not_canary_subset() -> None:
    connection = FakeConnection(
        batch=_batch(active_symbols=2, audit_active_universe_count=3),
        checkpoint_scopes=3,
    )

    result = asyncio.run(get_module_c_execution_status(connection, batch_id=42))

    assert result["batch"]["freshness"]["status"] == "current"
    assert result["batch"]["provenance"]["evidence_complete"] is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("frozen_contract", "legacy"),
        ("frozen_levels", ["30f", "5f", "1d", "1w", "1m"]),
        ("frozen_modes", ["predictive", "confirmed"]),
        ("frozen_concurrency_per_worker", 2),
        ("frozen_shard_count", 3),
        ("frozen_eligibility_build_id", "99999999-9999-9999-9999-999999999999"),
    ],
)
def test_frozen_execution_contract_drift_blocks_evidence(field, value) -> None:
    result = asyncio.run(
        get_module_c_execution_status(FakeConnection(batch=_batch(**{field: value})), batch_id=42)
    )

    provenance = result["batch"]["provenance"]
    assert provenance["frozen_config_matches"] is False
    assert provenance["evidence_complete"] is False
    assert "frozen_execution_contract_drift" in provenance["drift_reasons"]


def test_frozen_execution_contract_rejects_extra_keys() -> None:
    batch = _batch()
    batch["frozen_effective_config"] = {
        **batch["frozen_effective_config"], "unexpected": True
    }

    provenance = asyncio.run(
        get_module_c_execution_status(FakeConnection(batch=batch), batch_id=42)
    )["batch"]["provenance"]

    assert provenance["frozen_config_matches"] is False
    assert provenance["evidence_complete"] is False


def test_build_parameters_and_canonical_freshness_sha_are_revalidated() -> None:
    batch = _batch()
    batch["build_parameters"] = dict(batch["build_parameters"])
    batch["build_parameters"]["freshness_contract"] = dict(
        batch["build_parameters"]["freshness_contract"]
    )
    batch["build_parameters"]["freshness_contract"]["trading_calendar"] = {
        "id": "changed", "sha256": "9" * 64
    }

    result = asyncio.run(
        get_module_c_execution_status(FakeConnection(batch=batch), batch_id=42)
    )

    provenance = result["batch"]["provenance"]
    assert provenance["evidence_complete"] is False
    assert "freshness_contract_drift" in provenance["drift_reasons"]
    assert result["batch"]["freshness"]["status"] == "unavailable"


@pytest.mark.parametrize("target", ["column", "parameter"])
def test_all_nine_typed_provenance_fields_must_match(target) -> None:
    batch = _batch()
    if target == "column":
        batch["catalog_manifest_sha256"] = "0" * 64
    else:
        batch["build_parameters"] = dict(batch["build_parameters"])
        batch["build_parameters"]["catalog_manifest_sha256"] = "0" * 64

    result = asyncio.run(
        get_module_c_execution_status(FakeConnection(batch=batch), batch_id=42)
    )

    provenance = result["batch"]["provenance"]
    assert provenance["evidence_complete"] is False
    assert "strict_provenance_parameters_drift" in provenance["drift_reasons"]


@pytest.mark.parametrize("evidence", ["audit", "checkpoint"])
def test_audit_evidence_and_exact_checkpoint_manifest_are_recomputed(evidence) -> None:
    batch = _batch(audit_active_universe_count=2)
    if evidence == "audit":
        batch["audit_parameters"] = dict(batch["audit_parameters"])
        batch["audit_parameters"]["engine"] = "tampered"
    else:
        batch["_checkpoint_rows"] = list(batch["_checkpoint_rows"][:-1])

    result = asyncio.run(
        get_module_c_execution_status(
            FakeConnection(batch=batch, checkpoint_scopes=2), batch_id=42
        )
    )

    provenance = result["batch"]["provenance"]
    assert provenance["evidence_complete"] is False
    assert "canonical_audit_evidence_or_checkpoint_drift" in provenance["drift_reasons"]


def test_future_checkpoint_is_counted_and_cannot_be_current() -> None:
    result = asyncio.run(
        get_module_c_execution_status(
            FakeConnection(batch=_batch(), future_scopes=1), batch_id=42
        )
    )

    freshness = result["batch"]["freshness"]
    assert freshness["status"] == "unavailable"
    assert all(row["future_scopes"] == 1 for row in freshness["actual_checkpoint_watermarks"])
    assert "5f_future_scopes" in freshness["reasons"]
    assert "authoritative_freshness_future" in result["batch"]["provenance"]["drift_reasons"]
    assert result["batch"]["provenance"]["evidence_complete"] is False


def test_live_active_universe_and_catalog_manifests_are_recomputed() -> None:
    universe_drift = _batch(audit_active_universe_count=2)
    universe_drift["_live_universe_rows"] = list(universe_drift["_live_universe_rows"])
    universe_drift["_live_universe_rows"][0] = {
        **universe_drift["_live_universe_rows"][0], "code": "999999"
    }
    catalog_drift = _batch(audit_active_universe_count=2)
    catalog_drift["_active_catalog_rows"] = list(catalog_drift["_active_catalog_rows"])
    catalog_drift["_active_catalog_rows"][0] = {
        **catalog_drift["_active_catalog_rows"][0], "updated_at": NOW.replace(hour=2)
    }

    universe = asyncio.run(
        get_module_c_execution_status(
            FakeConnection(batch=universe_drift, checkpoint_scopes=2), batch_id=42
        )
    )["batch"]["provenance"]
    catalog = asyncio.run(
        get_module_c_execution_status(
            FakeConnection(batch=catalog_drift, checkpoint_scopes=2), batch_id=42
        )
    )["batch"]["provenance"]

    assert universe["live_universe_matches"] is False
    assert "active_universe_drift" in universe["drift_reasons"]
    assert universe["evidence_complete"] is False
    assert catalog["catalog_manifest_matches"] is False
    assert "active_catalog_manifest_drift" in catalog["drift_reasons"]
    assert catalog["evidence_complete"] is False


def test_repository_returns_null_batch_without_task_query() -> None:
    connection = FakeConnection(batch=None)

    result = asyncio.run(get_module_c_execution_status(connection, batch_id=None))

    assert result["batch"] is None
    assert len(connection.queries) == 2


def _client(pool: FakePool | None = None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(
        api_token="api-token", admin_api_token="admin-token"
    )
    if pool is not None:
        app.state.db_pool = pool
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    ("authorization", "expected"),
    [(None, 401), ("Bearer bad-token", 403), ("Bearer api-token", 403)],
)
def test_route_authenticates_before_pool_access(authorization, expected) -> None:
    pool = FakePool(FakeConnection(batch=_batch()))
    headers = {"Authorization": authorization} if authorization else {}

    response = _client(pool).get("/api/v1/admin/ops/module-c/execution", headers=headers)

    assert response.status_code == expected
    assert pool.acquires == 0


def test_route_uses_one_repeatable_read_readonly_transaction() -> None:
    connection = FakeConnection(batch=_batch())
    response = _client(FakePool(connection)).get(
        "/api/v1/admin/ops/module-c/execution",
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 200
    assert response.json()["batch"]["batch_id"] == 42
    assert connection.transaction_args == {"isolation": "repeatable_read", "readonly": True}


def test_route_returns_404_only_for_explicit_unknown_batch() -> None:
    connection = FakeConnection(batch=None)
    client = _client(FakePool(connection))
    headers = {"Authorization": "Bearer admin-token"}

    assert client.get("/api/v1/admin/ops/module-c/execution", headers=headers).status_code == 200
    response = client.get(
        "/api/v1/admin/ops/module-c/execution?batch_id=999", headers=headers
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "module_c_execution_batch_not_found"


def test_route_returns_generic_503_when_pool_is_missing() -> None:
    response = _client().get(
        "/api/v1/admin/ops/module-c/execution",
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "module_c_execution_database_unavailable"


class DatabaseError(RuntimeError):
    def __init__(self, message: str, *, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    ("error", "expected_status", "expected_detail"),
    [
        (
            DatabaseError("relation name and internal details", sqlstate="42P01"),
            503,
            "module_c_execution_schema_not_deployed",
        ),
        (
            DatabaseError("password and internal database details"),
            503,
            "module_c_execution_database_unavailable",
        ),
        (TimeoutError("slow query text"), 504, "module_c_execution_query_timeout"),
    ],
)
def test_route_maps_database_failures_to_generic_status(
    monkeypatch, error, expected_status, expected_detail
) -> None:
    async def fail(*_args, **_kwargs):
        raise error

    monkeypatch.setattr("app.routes.ops._module_c_execution_snapshot", fail)
    response = _client(FakePool(FakeConnection(batch=_batch()))).get(
        "/api/v1/admin/ops/module-c/execution",
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == expected_status
    assert response.json()["detail"] == expected_detail
    assert "internal" not in response.text
