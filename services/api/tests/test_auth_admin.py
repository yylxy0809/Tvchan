from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.core.security import hash_token
from app.main import create_app


class FakeTokenPool:
    def __init__(self) -> None:
        self.rows: dict[int, dict] = {}
        self.next_id = 1
        self.touched: list[int] = []

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if "where token_hash = $1" in normalized:
            token_hash = args[0]
            for row in self.rows.values():
                if row["token_hash"] == token_hash and row["is_active"]:
                    return row
            return None
        if normalized.startswith("insert into user_api_tokens"):
            token_hash, label, display_name = args
            now = datetime.now(UTC)
            row = {
                "id": self.next_id,
                "token_hash": token_hash,
                "label": label,
                "display_name": display_name,
                "role": "user",
                "is_active": True,
                "created_at": now,
                "updated_at": now,
                "disabled_at": None,
                "last_used_at": None,
            }
            self.rows[self.next_id] = row
            self.next_id += 1
            return row
        if normalized.startswith("update user_api_tokens set is_active"):
            token_id = args[0]
            row = self.rows.get(token_id)
            if row is None:
                return None
            now = datetime.now(UTC)
            row["is_active"] = False
            row["disabled_at"] = row["disabled_at"] or now
            row["updated_at"] = now
            return row
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if normalized.startswith("select id, label"):
            return sorted(
                self.rows.values(),
                key=lambda row: (row["created_at"], row["id"]),
                reverse=True,
            )
        raise AssertionError(f"unexpected fetch query: {query}")

    async def execute(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if normalized.startswith("delete from user_api_tokens"):
            token_id = args[0]
            if token_id in self.rows:
                del self.rows[token_id]
                return "DELETE 1"
            return "DELETE 0"
        if normalized.startswith("update user_api_tokens set last_used_at"):
            token_id = args[0]
            if token_id in self.rows:
                self.rows[token_id]["last_used_at"] = datetime.now(UTC)
                self.touched.append(token_id)
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute query: {query}")


def _client(settings: Settings, pool: FakeTokenPool | None = None) -> TestClient:
    api_app = create_app()
    api_app.dependency_overrides[get_settings] = lambda: settings
    client = TestClient(api_app)
    if pool is not None:
        api_app.state.db_pool = pool
    return client


def test_login_keeps_api_token_as_user_when_admin_token_is_not_configured() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token=""))

    response = client.post("/api/v1/auth/login", json={"token": "api-token"})

    assert response.status_code == 200
    assert response.json() == {
        "valid": True,
        "role": "user",
        "display_name": "API token",
        "label": "api-token",
        "token_id": None,
    }


def test_empty_admin_token_keeps_management_surface_disabled() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token=""))

    response = client.get(
        "/api/v1/admin/tokens",
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Admin token required"


def test_login_distinguishes_admin_token_from_compatible_api_token() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"))

    admin_response = client.post("/api/v1/auth/login", json={"token": "admin-token"})
    api_response = client.post("/api/v1/auth/login", json={"token": "api-token"})

    assert admin_response.json()["role"] == "admin"
    assert api_response.json()["role"] == "user"
    assert api_response.json()["valid"] is True


def test_api_token_still_accesses_existing_data_routes_when_admin_token_is_configured() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"))

    response = client.get(
        "/api/v1/symbols",
        params={"keyword": "000001"},
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["symbol"] == "000001.SZ"


def test_admin_can_create_list_disable_and_delete_user_tokens() -> None:
    pool = FakeTokenPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)
    admin_headers = {"Authorization": "Bearer admin-token"}

    create_response = client.post(
        "/api/v1/admin/tokens",
        json={"label": "desk-1", "display_name": "Desk 1"},
        headers=admin_headers,
    )
    assert create_response.status_code == 201
    created = create_response.json()
    plain_token = created["token"]
    token_id = created["id"]
    assert pool.rows[token_id]["token_hash"] == hash_token(plain_token)
    assert pool.rows[token_id]["token_hash"] != plain_token

    list_response = client.get("/api/v1/admin/tokens", headers=admin_headers)
    assert list_response.status_code == 200
    listed = list_response.json()["items"]
    assert len(listed) == 1
    assert listed[0]["label"] == "desk-1"
    assert "token" not in listed[0]

    login_response = client.post("/api/v1/auth/login", json={"token": plain_token})
    assert login_response.status_code == 200
    assert login_response.json()["role"] == "user"
    assert pool.touched == [token_id]

    disable_response = client.post(
        f"/api/v1/admin/tokens/{token_id}/disable",
        headers=admin_headers,
    )
    assert disable_response.status_code == 200
    assert disable_response.json()["is_active"] is False

    disabled_login_response = client.post("/api/v1/auth/login", json={"token": plain_token})
    assert disabled_login_response.json() == {
        "valid": False,
        "role": None,
        "display_name": None,
        "label": None,
        "token_id": None,
    }

    delete_response = client.delete(
        f"/api/v1/admin/tokens/{token_id}",
        headers=admin_headers,
    )
    assert delete_response.status_code == 204
    assert token_id not in pool.rows


def test_user_token_cannot_access_admin_token_management() -> None:
    pool = FakeTokenPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)
    created = client.post(
        "/api/v1/admin/tokens",
        json={"label": "desk-1"},
        headers={"Authorization": "Bearer admin-token"},
    ).json()

    response = client.get(
        "/api/v1/admin/tokens",
        headers={"Authorization": f"Bearer {created['token']}"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Admin token required"


def test_admin_token_management_requires_database_pool() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"))

    response = client.get(
        "/api/v1/admin/tokens",
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Database token store is not available"


def test_module_c_execution_status_preserves_admin_auth_boundary_before_database() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"))
    path = "/api/v1/admin/ops/module-c/execution"

    assert client.get(path).status_code == 401
    user_response = client.get(path, headers={"Authorization": "Bearer api-token"})
    assert user_response.status_code == 403
    assert user_response.json()["detail"] == "Admin token required"
    admin_response = client.get(path, headers={"Authorization": "Bearer admin-token"})
    assert admin_response.status_code == 503
    assert admin_response.json()["detail"] == "module_c_execution_database_unavailable"
