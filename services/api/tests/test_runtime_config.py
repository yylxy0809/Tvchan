from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.main import create_app


class FakeRuntimeConfigPool:
    def __init__(self, rows: dict[str, dict] | None = None) -> None:
        self.rows = rows or {}

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if normalized.startswith("select key, value, version, updated_at from runtime_config"):
            return self.rows.get(args[0])
        if normalized.startswith("insert into runtime_config"):
            key, raw_value = args
            value = json.loads(raw_value)
            now = datetime.now(UTC)
            existing = self.rows.get(key)
            version = 1 if existing is None else existing["version"] + 1
            row = {
                "key": key,
                "value": value,
                "version": version,
                "updated_at": now,
            }
            self.rows[key] = row
            return row
        raise AssertionError(f"unexpected fetchrow query: {query}")


def _client(settings: Settings, pool: FakeRuntimeConfigPool | None = None) -> TestClient:
    api_app = create_app()
    api_app.dependency_overrides[get_settings] = lambda: settings
    client = TestClient(api_app)
    if pool is not None:
        api_app.state.db_pool = pool
    return client


def test_public_feature_config_is_open_and_reads_runtime_config() -> None:
    updated_at = datetime(2026, 7, 1, 8, 30, tzinfo=UTC)
    pool = FakeRuntimeConfigPool(
        {
            "frontend.features": {
                "key": "frontend.features",
                "value": '{"chanStudy":false,"chartDataTransport":"websocket"}',
                "version": 3,
                "updated_at": updated_at,
            }
        }
    )
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.get("/api/v1/config/features")

    assert response.status_code == 200
    assert response.json() == {
        "key": "frontend.features",
        "value": {"chanStudy": False, "chartDataTransport": "websocket"},
        "version": 3,
        "updated_at": "2026-07-01T08:30:00Z",
    }


def test_admin_can_update_runtime_config_and_increment_version() -> None:
    pool = FakeRuntimeConfigPool(
        {
            "frontend.features": {
                "key": "frontend.features",
                "value": {},
                "version": 1,
                "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
            }
        }
    )
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.put(
        "/api/v1/admin/runtime-config/frontend.features",
        json={"value": {"chanStudy": False, "rightSidebar": {"news": False}}},
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["key"] == "frontend.features"
    assert body["value"] == {"chanStudy": False, "rightSidebar": {"news": False}}
    assert body["version"] == 2
    assert pool.rows["frontend.features"]["value"] == body["value"]


def test_generic_runtime_config_cannot_bypass_wencai_https_host_allowlist() -> None:
    pool = FakeRuntimeConfigPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.put(
        "/api/v1/admin/runtime-config/wencai.config",
        json={"value": {"base_url": "http://127.0.0.1:8080", "api_key": "secret"}},
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 422
    assert "allowed HTTPS host" in response.json()["detail"]
    assert "wencai.config" not in pool.rows


def test_wencai_config_exposes_runtime_config_version() -> None:
    pool = FakeRuntimeConfigPool(
        {
            "wencai.config": {
                "key": "wencai.config",
                "value": {"api_key": "secret"},
                "version": 4,
                "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
            }
        }
    )
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.get("/api/v1/admin/wencai/config", headers={"Authorization": "Bearer admin-token"})

    assert response.status_code == 200
    assert response.json()["config_version"] == 4


def test_user_token_cannot_update_runtime_config() -> None:
    pool = FakeRuntimeConfigPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.put(
        "/api/v1/admin/runtime-config/frontend.features",
        json={"value": {"chanStudy": False}},
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Admin token required"


def test_runtime_config_requires_database_pool() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"))

    response = client.get("/api/v1/config/features")

    assert response.status_code == 503
    assert response.json()["detail"] == "Database runtime config store is not available"
