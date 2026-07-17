from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.main import create_app
from app.repositories.module_c_ops import (
    BATCH_SQL,
    FRESHNESS_SQL,
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
    ) -> None:
        self.batch = batch
        self.task_rows = list(task_rows or [])
        self.checkpoint_scopes = checkpoint_scopes
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
                    "checkpoint_scopes": self.checkpoint_scopes,
                }
                for index, (code, timeframe) in enumerate(
                    ((5, "5f"), (30, "30f"), (1440, "1d"), (10080, "1w"), (43200, "1m"))
                )
            ]
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


def _batch(**overrides) -> dict:
    value = {
        "batch_id": 42,
        "batch_key": "canary-42",
        "batch_kind": "canary",
        "parent_status": "running",
        "child_status": "running",
        "publication_namespace": "production",
        "profile_id": "module-c-native-5lvl",
        "run_group_id": "run-42",
        "code_commit": "a" * 40,
        "image_digest": "sha256:image",
        "vendor_manifest_sha256": "b" * 64,
        "config_hash": "config",
        "created_at": NOW,
        "started_at": NOW,
        "finished_at": None,
        "updated_at": NOW,
        "shard_count": 4,
        "active_symbols": 20,
        "disposition_rows": 100,
        "latest_task_update": NOW,
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
        "audit_evidence_sha256": "d" * 64,
        "audit_checkpoint_sha256": "e" * 64,
        "audit_status": "completed",
        "audit_apply_mode": False,
        "audit_active_universe_count": 20,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": "f" * 64,
        "catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": "1" * 64,
        "audit_active_universe_sha256": "2" * 64,
        "catalog_generation_status": "complete",
        "live_catalog_generation_id": "33333333-3333-3333-3333-333333333333",
        "live_catalog_control_revision": 7,
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
    assert provenance["catalog_is_active"] is True
    assert provenance["catalog_revision_matches"] is True
    assert provenance["eligibility_manifest_matches"] is True
    assert provenance["config_hash_matches"] is True
    assert provenance["drift_reasons"] == []
    assert "notes" not in result["batch"]
    assert "last_error" not in result["batch"]["execution"]["tasks"][0]


def test_repository_sql_is_read_only_and_freshness_uses_only_pinned_checkpoints() -> None:
    sql = " ".join(
        (RUNNING_COUNTS_SQL, BATCH_SQL, TASK_PROGRESS_SQL, TASK_HEALTH_SQL, FRESHNESS_SQL)
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
    assert "audit.parameters->>'active_universe_count'" in BATCH_SQL.lower()


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


def test_freshness_uses_pinned_audit_universe_not_canary_subset() -> None:
    connection = FakeConnection(
        batch=_batch(active_symbols=20, audit_active_universe_count=5529),
        checkpoint_scopes=5529,
    )

    result = asyncio.run(get_module_c_execution_status(connection, batch_id=42))

    assert result["batch"]["freshness"]["status"] == "current"
    assert result["batch"]["provenance"]["evidence_complete"] is True


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
