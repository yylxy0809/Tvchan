from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.core.security import hash_token
from app.main import create_app


class FakeUserSettingsPool:
    def __init__(self) -> None:
        self.settings_rows: dict[tuple[str, str], dict] = {}
        self.token_rows: dict[int, dict] = {}
        self.touched_tokens: list[int] = []

    def add_user_token(self, token: str, *, token_id: int = 1) -> None:
        now = datetime.now(UTC)
        self.token_rows[token_id] = {
            "id": token_id,
            "token_hash": hash_token(token),
            "label": "desk",
            "display_name": "Desk",
            "role": "user",
            "is_active": True,
            "created_at": now,
            "updated_at": now,
            "disabled_at": None,
            "last_used_at": None,
        }

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if "from user_api_tokens" in normalized and "where token_hash = $1" in normalized:
            token_hash = args[0]
            for row in self.token_rows.values():
                if row["token_hash"] == token_hash and row["is_active"]:
                    return row
            return None
        if normalized.startswith("insert into user_settings"):
            owner_token_hash, bucket, raw_value = args
            key = (owner_token_hash, bucket)
            now = datetime.now(UTC)
            existing = self.settings_rows.get(key)
            row = {
                "bucket": bucket,
                "value": json.loads(raw_value),
                "version": 1 if existing is None else existing["version"] + 1,
                "updated_at": now,
            }
            self.settings_rows[key] = row
            return row
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if normalized.startswith("select bucket, value, version, updated_at from user_settings"):
            owner_token_hash = args[0]
            return [
                row
                for (owner, _bucket), row in sorted(self.settings_rows.items())
                if owner == owner_token_hash
            ]
        raise AssertionError(f"unexpected fetch query: {query}")

    async def execute(self, query: str, *args):
        normalized = " ".join(query.lower().split())
        if normalized.startswith("update user_api_tokens set last_used_at"):
            token_id = args[0]
            if token_id in self.token_rows:
                self.token_rows[token_id]["last_used_at"] = datetime.now(UTC)
                self.touched_tokens.append(token_id)
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute query: {query}")


def _client(settings: Settings, pool: FakeUserSettingsPool | None = None) -> TestClient:
    api_app = create_app()
    api_app.dependency_overrides[get_settings] = lambda: settings
    client = TestClient(api_app)
    if pool is not None:
        api_app.state.db_pool = pool
    return client


def _items_by_bucket(response_json: dict) -> dict:
    return {item["bucket"]: item for item in response_json["items"]}


def test_user_settings_are_isolated_between_admin_and_api_tokens() -> None:
    pool = FakeUserSettingsPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    admin_response = client.put(
        "/api/v1/user/settings/theme",
        json={"value": {"mode": "dark"}},
        headers={"Authorization": "Bearer admin-token"},
    )
    api_response = client.put(
        "/api/v1/user/settings/theme",
        json={"value": {"mode": "light"}},
        headers={"Authorization": "Bearer api-token"},
    )
    api_watchlist_response = client.put(
        "/api/v1/user/settings/watchlist",
        json={"value": ["000001.SZ", "600000.SH"]},
        headers={"Authorization": "Bearer api-token"},
    )

    assert admin_response.status_code == 200
    assert api_response.status_code == 200
    assert api_watchlist_response.status_code == 200
    assert admin_response.json()["version"] == 1
    assert api_response.json()["version"] == 1

    admin_items = _items_by_bucket(
        client.get(
            "/api/v1/user/settings",
            headers={"Authorization": "Bearer admin-token"},
        ).json()
    )
    api_items = _items_by_bucket(
        client.get(
            "/api/v1/user/settings",
            headers={"Authorization": "Bearer api-token"},
        ).json()
    )

    assert set(admin_items) == {"theme"}
    assert admin_items["theme"]["value"] == {"mode": "dark"}
    assert api_items["theme"]["value"] == {"mode": "light"}
    assert api_items["watchlist"]["value"] == ["000001.SZ", "600000.SH"]
    assert (hash_token("admin-token"), "theme") in pool.settings_rows
    assert (hash_token("api-token"), "theme") in pool.settings_rows


def test_user_setting_update_increments_version() -> None:
    pool = FakeUserSettingsPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)
    headers = {"Authorization": "Bearer api-token"}

    first = client.put(
        "/api/v1/user/settings/indicatorSettings",
        json={"value": {"ma": {"enabled": True}}},
        headers=headers,
    )
    second = client.put(
        "/api/v1/user/settings/indicatorSettings",
        json={"value": {"ma": {"enabled": False}}},
        headers=headers,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["version"] == 2
    assert second.json()["value"] == {"ma": {"enabled": False}}


def test_database_user_token_can_save_user_settings() -> None:
    pool = FakeUserSettingsPool()
    pool.add_user_token("desk-token", token_id=7)
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.put(
        "/api/v1/user/settings/layout",
        json={"value": {"rightSidebar": "collapsed"}},
        headers={"Authorization": "Bearer desk-token"},
    )

    assert response.status_code == 200
    assert response.json()["bucket"] == "layout"
    assert response.json()["value"] == {"rightSidebar": "collapsed"}
    assert pool.touched_tokens == [7]


def test_user_settings_require_token() -> None:
    pool = FakeUserSettingsPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.get("/api/v1/user/settings")

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing bearer token"


def test_user_settings_require_database_pool() -> None:
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"))

    response = client.get(
        "/api/v1/user/settings",
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Database user settings store is not available"


def test_unknown_user_setting_bucket_is_rejected() -> None:
    pool = FakeUserSettingsPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.put(
        "/api/v1/user/settings/unknown",
        json={"value": {}},
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown user setting bucket"
