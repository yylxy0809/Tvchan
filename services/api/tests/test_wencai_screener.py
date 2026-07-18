from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime

import pytest
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
            existing = self.rows.get(key)
            row = {
                "key": key,
                "value": json.loads(raw_value),
                "version": 1 if existing is None else existing["version"] + 1,
                "updated_at": datetime.now(UTC),
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


def test_wencai_service_requires_auth_config() -> None:
    from app.services.wencai_client import WencaiConfig, WencaiConfigError, query_wencai

    with pytest.raises(WencaiConfigError, match="IWENCAI_API_KEY|cookie"):
        asyncio.run(
            query_wencai(
                query="今日涨停",
                page=1,
                page_size=50,
                config=WencaiConfig(cookie=""),
            )
        )


def test_wencai_service_prefers_openapi_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import WencaiConfig, query_wencai

    captured: dict[str, object] = {}

    def fake_openapi(**kwargs):
        captured.update(kwargs)
        return {
            "code_count": 3,
            "datas": [
                {"股票代码": "600001.SH", "股票简称": "甲公司", "最新价": 10.5, "涨跌幅": 9.99},
                {"股票代码": "000002.SZ", "股票简称": "乙公司", "最新价": 8.2, "涨跌幅": -1.25},
            ],
        }

    monkeypatch.setattr(wencai_client, "_call_iwencai_openapi", fake_openapi)

    result = asyncio.run(
        query_wencai(
            query="今日涨停",
            page=2,
            page_size=2,
            config=WencaiConfig(
                base_url="https://openapi.iwencai.com",
                api_key="api-key",
                cookie="cookie=value",
                timeout_seconds=6,
            ),
        )
    )

    assert captured["query"] == "今日涨停"
    assert captured["page"] == "2"
    assert captured["limit"] == "2"
    assert captured["api_key"] == "api-key"
    assert result.total == 3
    assert [item.code for item in result.items] == ["600001", "000002"]


def test_wencai_service_rotates_enabled_keys_by_priority_and_never_exposes_them(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import WencaiApiKey, WencaiConfig, query_wencai

    attempted: list[str] = []

    def fake_openapi(**kwargs):
        attempted.append(kwargs["api_key"])
        if kwargs["api_key"] == "first-secret":
            raise wencai_client.WencaiUpstreamError("WenCai OpenAPI HTTP 429: quota exhausted")
        return {"code_count": 0, "datas": []}

    monkeypatch.setattr(wencai_client, "_call_iwencai_openapi", fake_openapi)
    config = WencaiConfig(api_keys=(
        WencaiApiKey(label="primary", key="first-secret", priority=1),
        WencaiApiKey(label="backup", key="second-secret", priority=2),
        WencaiApiKey(label="disabled", key="disabled-secret", enabled=False, priority=0),
    ))

    assert asyncio.run(query_wencai(query="test", page=1, page_size=1, config=config)).total == 0
    assert attempted == ["first-secret", "second-secret"]
    assert "first-secret" not in repr(config)


def test_wencai_service_retries_timeout_once_before_switching_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import WencaiApiKey, WencaiConfig, query_wencai

    attempted: list[str] = []

    def fake_openapi(**kwargs):
        attempted.append(kwargs["api_key"])
        if kwargs["api_key"] == "first-secret":
            raise wencai_client.WencaiUpstreamError("timeout")
        return {"code_count": 0, "datas": []}

    monkeypatch.setattr(wencai_client, "_call_iwencai_openapi", fake_openapi)
    config = WencaiConfig(api_keys=(WencaiApiKey(key="first-secret"), WencaiApiKey(key="second-secret", priority=1)))

    asyncio.run(query_wencai(query="test", page=1, page_size=1, config=config))
    assert attempted == ["first-secret", "first-secret", "second-secret"]


def test_wencai_service_cookie_provider_fetches_only_bounded_first_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import WencaiConfig, query_wencai

    captured: dict[str, object] = {}

    def fake_get(**kwargs):
        captured.update(kwargs)
        return [
            {"股票代码": "600001", "股票简称": "甲公司", "最新价": 10.5, "涨跌幅": 9.99},
            {"股票代码": "000002", "股票简称": "乙公司", "最新价": 8.2, "涨跌幅": -1.25},
            {"股票代码": "300003", "股票简称": "丙公司", "最新价": 12, "涨跌幅": 3.5},
        ]

    monkeypatch.setattr(wencai_client, "_call_pywencai_get", fake_get)

    result = asyncio.run(
        query_wencai(
            query="今日涨停",
            page=1,
            page_size=2,
            config=WencaiConfig(cookie="cookie=value", user_agent="ua", pro=True, timeout_seconds=6),
        )
    )

    assert captured["query"] == "今日涨停"
    assert captured["cookie"] == "cookie=value"
    assert captured["user_agent"] == "ua"
    assert captured["pro"] is True
    assert captured["loop"] is False
    assert captured["perpage"] == 2
    assert result.total == 2
    assert [item.code for item in result.items] == ["600001", "000002"]


def test_wencai_service_cookie_provider_rejects_later_pages_before_offload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import (
        WencaiConfig,
        WencaiPaginationError,
        query_wencai,
    )

    async def unexpected_to_thread(*_args, **_kwargs):
        raise AssertionError("later pages must fail before creating an offload")

    monkeypatch.setattr(wencai_client.asyncio, "to_thread", unexpected_to_thread)
    with pytest.raises(WencaiPaginationError, match="first page"):
        asyncio.run(
            query_wencai(
                query="今日涨停",
                page=2,
                page_size=20,
                config=WencaiConfig(cookie="cookie=value"),
            )
        )


def test_wencai_service_cookie_provider_bounds_concurrent_offloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import (
        WencaiCapacityError,
        WencaiConfig,
        query_wencai,
    )

    calls = 0
    controls: dict[str, tuple[threading.Event, threading.Event]] = {}

    def blocking_get(**kwargs):
        nonlocal calls
        calls += 1
        started, release = controls[kwargs["query"]]
        started.set()
        release.wait(timeout=2)
        return []

    monkeypatch.setattr(wencai_client, "_call_pywencai_get", blocking_get)
    monkeypatch.setattr(
        wencai_client,
        "_COOKIE_FETCH_LIMITER",
        wencai_client._CookieFetchLimiter(1),
    )

    async def scenario(label: str) -> None:
        started = threading.Event()
        release = threading.Event()
        controls[label] = (started, release)
        calls_before = calls
        config = WencaiConfig(cookie="cookie=value")
        first = asyncio.create_task(
            query_wencai(query=label, page=1, page_size=20, config=config)
        )
        try:
            while not started.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(WencaiCapacityError, match="busy"):
                await query_wencai(query="second", page=1, page_size=20, config=config)
            assert calls == calls_before + 1
        finally:
            release.set()
            await first

    asyncio.run(scenario("first-loop"))
    asyncio.run(scenario("second-loop"))
    assert calls == 2


def test_wencai_service_cookie_provider_holds_slot_until_cancelled_worker_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import (
        WencaiCapacityError,
        WencaiConfig,
        query_wencai,
    )

    started = threading.Event()
    release = threading.Event()
    calls = 0

    def blocking_get(**_kwargs):
        nonlocal calls
        calls += 1
        started.set()
        release.wait(timeout=2)
        return []

    monkeypatch.setattr(wencai_client, "_call_pywencai_get", blocking_get)
    monkeypatch.setattr(
        wencai_client,
        "_COOKIE_FETCH_LIMITER",
        wencai_client._CookieFetchLimiter(1),
    )

    async def scenario() -> None:
        config = WencaiConfig(cookie="cookie=value")
        first = asyncio.create_task(
            query_wencai(query="first", page=1, page_size=20, config=config)
        )
        while not started.is_set():
            await asyncio.sleep(0.001)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        with pytest.raises(WencaiCapacityError, match="busy"):
            await query_wencai(query="still-busy", page=1, page_size=20, config=config)
        assert calls == 1

        release.set()
        for _ in range(100):
            try:
                await query_wencai(query="after-release", page=1, page_size=20, config=config)
                break
            except WencaiCapacityError:
                await asyncio.sleep(0.001)
        else:
            raise AssertionError("slot was not released after the worker finished")
        assert calls == 2

    asyncio.run(scenario())


def test_wencai_service_cookie_provider_sanitizes_upstream_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services import wencai_client
    from app.services.wencai_client import WencaiConfig, WencaiUpstreamError, query_wencai

    def failing_get(**_kwargs):
        raise RuntimeError("cookie=private-value")

    monkeypatch.setattr(wencai_client, "_call_pywencai_get", failing_get)
    with pytest.raises(WencaiUpstreamError) as raised:
        asyncio.run(
            query_wencai(
                query="test",
                page=1,
                page_size=20,
                config=WencaiConfig(cookie="cookie=value"),
            )
        )
    assert "private-value" not in str(raised.value)


def test_wencai_screener_endpoint_reads_config_and_returns_page(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services import wencai_client

    def fake_get(**_kwargs):
        return [
            {"股票代码": "600001", "股票简称": "甲公司", "最新价": 10.5, "涨跌幅": 9.99},
            {"股票代码": "000002", "股票简称": "乙公司", "最新价": 8.2, "涨跌幅": -1.25},
        ]

    monkeypatch.setattr(wencai_client, "_call_pywencai_get", fake_get)
    pool = FakeRuntimeConfigPool(
        {
            "wencai.config": {
                "key": "wencai.config",
                "value": {"cookie": "cookie=value", "timeout_seconds": 5},
                "version": 1,
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            }
        }
    )
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.get(
        "/api/v1/screener/wencai",
        params={"q": "今日涨停", "page": 1, "page_size": 50},
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "今日涨停"
    assert body["total"] == 2
    assert body["page"] == 1
    assert body["page_size"] == 50
    assert [item["code"] for item in body["items"]] == ["600001", "000002"]


@pytest.mark.parametrize(
    ("error_name", "status_code", "detail"),
    [
        ("WencaiPaginationError", 422, "WenCai cookie provider supports the first page only"),
        ("WencaiCapacityError", 429, "WenCai cookie provider is busy"),
    ],
)
def test_wencai_screener_maps_cookie_provider_bounds_without_leaking_details(
    monkeypatch: pytest.MonkeyPatch,
    error_name: str,
    status_code: int,
    detail: str,
) -> None:
    from app.routes import screener
    from app.services import wencai_client

    async def fail_query(**_kwargs):
        raise getattr(wencai_client, error_name)("private cookie detail")

    monkeypatch.setattr(screener, "query_wencai", fail_query)
    client = _client(Settings(api_token="api-token", wencai_cookie="cookie=value"))
    response = client.get(
        "/api/v1/screener/wencai",
        params={"q": "今日涨停", "page": 1, "page_size": 20},
        headers={"Authorization": "Bearer api-token"},
    )

    assert response.status_code == status_code
    assert response.json() == {"detail": detail}
    assert "private" not in response.text


def test_wencai_admin_test_uses_submitted_config_without_saving(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services import wencai_client

    def fake_openapi(**kwargs):
        assert kwargs["api_key"] == "fresh-key"
        return {"code_count": 1, "datas": [{"股票代码": "600001", "股票简称": "甲公司"}]}

    monkeypatch.setattr(wencai_client, "_call_iwencai_openapi", fake_openapi)
    pool = FakeRuntimeConfigPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.post(
        "/api/v1/admin/wencai/test",
        json={"api_key": "fresh-key", "timeout_seconds": 3},
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["sample_count"] == 1
    assert body["source"] == "iwencai"
    assert body["capability"] == "screener"
    assert "wencai.config" not in pool.rows


def test_wencai_admin_config_masks_cookie_and_preserves_existing_secret() -> None:
    pool = FakeRuntimeConfigPool(
        {
            "wencai.config": {
                "key": "wencai.config",
                "value": {
                    "base_url": "https://openapi.iwencai.com",
                    "api_key": "sk-test123456",
                    "cookie": "abcdef123456",
                    "user_agent": "ua",
                    "pro": False,
                },
                "version": 1,
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            }
        }
    )
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    get_response = client.get(
        "/api/v1/admin/wencai/config",
        headers={"Authorization": "Bearer admin-token"},
    )

    assert get_response.status_code == 200
    assert get_response.json()["api_key"] == "sk-t...3456"
    assert get_response.json()["cookie"] == "abcd...3456"

    put_response = client.put(
        "/api/v1/admin/wencai/config",
        json={
            "base_url": "https://openapi.iwencai.com",
            "api_key": "sk-t...3456",
            "cookie": "abcd...3456",
            "user_agent": "new-ua",
            "pro": True,
        },
        headers={"Authorization": "Bearer admin-token"},
    )

    assert put_response.status_code == 200
    assert pool.rows["wencai.config"]["value"]["api_key"] == "sk-test123456"
    assert pool.rows["wencai.config"]["value"]["cookie"] == "abcdef123456"
    assert pool.rows["wencai.config"]["value"]["user_agent"] == "new-ua"


def test_wencai_admin_rejects_non_https_or_non_allowlisted_base_url_without_saving() -> None:
    pool = FakeRuntimeConfigPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    for path in ("/api/v1/admin/wencai/config", "/api/v1/admin/wencai/test"):
        response = client.request(
            "PUT" if path.endswith("config") else "POST",
            path,
            json={"base_url": "http://127.0.0.1:8080", "api_key": "secret"},
            headers={"Authorization": "Bearer admin-token"},
        )
        assert response.status_code == 422
        assert "allowed HTTPS host" in response.json()["detail"]
    assert "wencai.config" not in pool.rows
