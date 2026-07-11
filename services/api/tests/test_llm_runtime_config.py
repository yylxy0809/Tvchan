from __future__ import annotations

import json
import asyncio
from datetime import UTC, datetime

import httpx
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


def test_llm_admin_providers_masks_api_keys_and_preserves_existing_secret() -> None:
    pool = FakeRuntimeConfigPool(
        {
            "llm.providers": {
                "key": "llm.providers",
                "value": {
                    "active_provider_id": "siliconflow",
                    "providers": [
                        {
                            "id": "siliconflow",
                            "name": "硅基流动",
                            "base_url": "https://api.siliconflow.cn/v1",
                            "api_key": "sk-abcdef123456",
                            "models": ["deepseek-ai/DeepSeek-V3.2"],
                            "active_model": "deepseek-ai/DeepSeek-V3.2",
                            "enabled": True,
                            "timeout_seconds": 20,
                        }
                    ],
                },
                "version": 1,
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            }
        }
    )
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    get_response = client.get(
        "/api/v1/admin/llm/providers",
        headers={"Authorization": "Bearer admin-token"},
    )

    assert get_response.status_code == 200
    assert get_response.json()["providers"][0]["api_key"] == "sk-a...3456"

    put_response = client.put(
        "/api/v1/admin/llm/providers",
        json={
            "active_provider_id": "siliconflow",
            "providers": [
                {
                    "id": "siliconflow",
                    "name": "硅基流动",
                    "base_url": "https://api.siliconflow.cn/v1",
                    "api_key": "sk-a...3456",
                    "models": ["deepseek-ai/DeepSeek-V3.2", "Qwen/Qwen3"],
                    "active_model": "Qwen/Qwen3",
                    "enabled": True,
                    "timeout_seconds": 15,
                }
            ],
        },
        headers={"Authorization": "Bearer admin-token"},
    )

    assert put_response.status_code == 200
    stored = pool.rows["llm.providers"]["value"]["providers"][0]
    assert stored["api_key"] == "sk-abcdef123456"
    assert stored["active_model"] == "Qwen/Qwen3"


def test_llm_admin_test_uses_submitted_provider_without_saving(monkeypatch) -> None:
    calls: list[httpx.Request] = []
    original_client = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["authorization"] == "Bearer sk-test"
        body = json.loads(request.content)
        assert body["model"] == "deepseek-ai/DeepSeek-V3.2"
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "conditions": [
                                        {
                                            "level": "1d",
                                            "kind": "structure",
                                            "direction": "up",
                                            "value": "trend",
                                            "raw": "日线趋势上涨",
                                        }
                                    ],
                                    "unsupported": [],
                                }
                            )
                        }
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)

    def mock_client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)
    pool = FakeRuntimeConfigPool()
    client = _client(Settings(api_token="api-token", admin_api_token="admin-token"), pool)

    response = client.post(
        "/api/v1/admin/llm/test",
        json={
            "id": "siliconflow",
            "name": "硅基流动",
            "base_url": "https://api.siliconflow.cn/v1",
            "api_key": "sk-test",
            "models": ["deepseek-ai/DeepSeek-V3.2"],
            "active_model": "deepseek-ai/DeepSeek-V3.2",
            "enabled": True,
            "timeout_seconds": 5,
        },
        headers={"Authorization": "Bearer admin-token"},
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["provider"] == "siliconflow"
    assert response.json()["model"] == "deepseek-ai/DeepSeek-V3.2"
    assert calls[0].url == "https://api.siliconflow.cn/v1/chat/completions"
    assert "llm.providers" not in pool.rows


def test_runtime_llm_provider_is_preferred_over_environment() -> None:
    from app.services.llm_config import resolve_active_llm_provider

    pool = FakeRuntimeConfigPool(
        {
            "llm.providers": {
                "key": "llm.providers",
                "value": {
                    "active_provider_id": "runtime",
                    "providers": [
                        {
                            "id": "runtime",
                            "name": "运行时",
                            "base_url": "https://runtime.example/v1",
                            "api_key": "runtime-key",
                            "models": ["runtime-model"],
                            "active_model": "runtime-model",
                            "enabled": True,
                            "timeout_seconds": 7,
                        }
                    ],
                },
                "version": 1,
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            }
        }
    )

    provider = asyncio.run(
        resolve_active_llm_provider(
            pool,
            Settings(
                llm_api_key="env-key",
                llm_base_url="https://env.example/v1",
                llm_model="env-model",
            ),
        )
    )

    assert provider is not None
    assert provider.api_key == "runtime-key"
    assert provider.base_url == "https://runtime.example/v1"
    assert provider.model == "runtime-model"
