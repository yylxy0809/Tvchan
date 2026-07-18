from __future__ import annotations

import asyncio
import gzip
import hashlib
import json

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.testclient import TestClient
import pytest

from app.core.security import TokenPrincipal, require_token
from app.history import export_store
from app.main import create_app
from app.routes import history as history_routes


AUTH_HEADERS = {"Authorization": "Bearer dev-local-token"}


@pytest.fixture(autouse=True)
def _clear_export_store() -> None:
    export_store.clear()
    yield
    export_store.clear()


def test_history_export_manifest_and_chunks_round_trip() -> None:
    client = TestClient(create_app())
    bars = [
        {
            "time": 1717200000 + index * 60,
            "open": 10 + index,
            "high": 10.5 + index,
            "low": 9.5 + index,
            "close": 10.25 + index,
            "volume": 1000 + index,
        }
        for index in range(20)
    ]
    payload = {
        "bars": bars,
        "metadata": {"symbol": "000001.SZ", "resolution": "1"},
        "chunk_size_bytes": 32,
    }

    response = client.post(
        "/api/v1/history/export",
        json=payload,
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 200
    manifest = response.json()
    assert manifest["format"] == "json"
    assert manifest["compression"] == "gzip"
    assert manifest["bar_count"] == len(bars)
    assert manifest["metadata"] == payload["metadata"]
    assert manifest["chunk_count"] == len(manifest["chunks"])
    assert manifest["chunk_count"] > 1

    compressed = bytearray()
    for chunk_manifest in manifest["chunks"]:
        chunk_response = client.get(chunk_manifest["href"], headers=AUTH_HEADERS)

        assert chunk_response.status_code == 200
        assert chunk_response.headers["content-type"] == "application/gzip"
        assert (
            chunk_response.headers["x-history-export-request-id"]
            == manifest["request_id"]
        )
        assert int(chunk_response.headers["x-history-export-chunk-index"]) == (
            chunk_manifest["index"]
        )
        assert len(chunk_response.content) == chunk_manifest["size_bytes"]
        assert hashlib.sha256(chunk_response.content).hexdigest() == (
            chunk_manifest["sha256"]
        )
        compressed.extend(chunk_response.content)

    exported = json.loads(gzip.decompress(bytes(compressed)).decode("utf-8"))
    assert exported["request_id"] == manifest["request_id"]
    assert exported["metadata"] == payload["metadata"]
    assert exported["bars"] == bars


def test_history_export_missing_chunk_returns_404() -> None:
    client = TestClient(create_app())
    response = client.post(
        "/api/v1/history/export",
        json={"bars": [], "metadata": {"symbol": "000001.SZ"}},
        headers=AUTH_HEADERS,
    )
    request_id = response.json()["request_id"]

    missing_response = client.get(
        f"/api/v1/history/export/{request_id}/chunks/999",
        headers=AUTH_HEADERS,
    )
    unknown_response = client.get(
        "/api/v1/history/export/not-found/chunks/0",
        headers=AUTH_HEADERS,
    )

    assert missing_response.status_code == 404
    assert unknown_response.status_code == 404


def test_history_export_rejects_declared_and_streamed_oversize_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(history_routes, "MAX_HISTORY_EXPORT_REQUEST_BYTES", 64)
    client = TestClient(create_app())

    declared = client.post(
        "/api/v1/history/export",
        content=b"{}",
        headers={**AUTH_HEADERS, "Content-Type": "application/json", "Content-Length": "65"},
    )
    streamed = client.post(
        "/api/v1/history/export",
        content=(chunk for chunk in (b'{"metadata":"', b"x" * 80, b'"}')),
        headers={**AUTH_HEADERS, "Content-Type": "application/json", "Content-Length": "2"},
    )

    assert declared.status_code == 413
    assert streamed.status_code == 413
    assert export_store.record_count == 0


def test_history_export_rejects_chunked_body_without_content_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ChunkedRequest:
        headers: dict[str, str] = {}

        async def stream(self):
            yield b'{"metadata":"'
            yield b"x" * 80
            yield b'"}'

    monkeypatch.setattr(history_routes, "MAX_HISTORY_EXPORT_REQUEST_BYTES", 64)

    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            history_routes._read_history_export_request(  # noqa: SLF001
                ChunkedRequest(),  # type: ignore[arg-type]
            )
        )

    assert caught.value.status_code == 413
    assert export_store.record_count == 0


def test_history_export_validation_and_chunk_amplification_fail_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = TestClient(create_app())
    invalid = client.post(
        "/api/v1/history/export",
        content=b'{"bars":"not-a-list"}',
        headers={**AUTH_HEADERS, "Content-Type": "application/json"},
    )
    monkeypatch.setattr(export_store, "max_chunks", 1)
    amplified = client.post(
        "/api/v1/history/export",
        json={"bars": [{"time": 1, "note": "x" * 200}], "chunk_size_bytes": 1},
        headers=AUTH_HEADERS,
    )

    assert invalid.status_code == 422
    assert amplified.status_code == 413
    assert export_store.record_count == 0


def test_history_export_validation_error_does_not_reflect_request_content() -> None:
    client = TestClient(create_app())
    secret = "do-not-reflect-this-client-value"

    response = client.post(
        "/api/v1/history/export",
        json={"bars": secret},
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid history export request"}
    assert secret not in response.text


def test_history_export_chunks_are_private_to_the_authenticated_principal() -> None:
    app = create_app()
    app.dependency_overrides[require_token] = lambda: TokenPrincipal(
        role="user", label="api-token",
    )
    client = TestClient(app)
    created = client.post(
        "/api/v1/history/export",
        json={"bars": [], "metadata": {"symbol": "000001.SZ"}},
        headers={"Authorization": "Bearer token-a"},
    )
    href = created.json()["chunks"][0]["href"]

    denied = client.get(href, headers={"Authorization": "Bearer token-b"})
    allowed = client.get(href, headers={"Authorization": "Bearer token-a"})

    assert denied.status_code == 404
    assert allowed.status_code == 200


def test_history_export_owner_capacity_returns_retryable_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(export_store, "max_records_per_owner", 1)
    client = TestClient(create_app())

    first = client.post(
        "/api/v1/history/export", json={"bars": []}, headers=AUTH_HEADERS,
    )
    second = client.post(
        "/api/v1/history/export", json={"bars": []}, headers=AUTH_HEADERS,
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["retry-after"] == "1"
    assert export_store.record_count == 1


def test_history_export_busy_and_global_capacity_are_fail_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    principal = TokenPrincipal(role="user", label="api-token")
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials="dev-local-token",
    )
    owner_key = history_routes._history_export_owner_key(  # noqa: SLF001
        principal,
        credentials,
    )
    client = TestClient(create_app())
    with export_store.reserve_build(owner_key):
        busy = client.post(
            "/api/v1/history/export", json={"bars": []}, headers=AUTH_HEADERS,
        )
    monkeypatch.setattr(export_store, "max_records", 0)
    full = client.post(
        "/api/v1/history/export", json={"bars": []}, headers=AUTH_HEADERS,
    )

    assert busy.status_code == 429
    assert busy.headers["retry-after"] == "1"
    assert full.status_code == 507
    assert full.headers["retry-after"] == "1"
    assert export_store.record_count == 0


def test_history_export_slow_body_times_out_and_releases_build_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SlowRequest:
        headers: dict[str, str] = {}

        async def stream(self):
            await asyncio.sleep(0.05)
            yield b"{}"

    monkeypatch.setattr(history_routes, "HISTORY_EXPORT_READ_TIMEOUT_SECONDS", 0.01)
    principal = TokenPrincipal(role="user", label="api-token")
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials="dev-local-token",
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            history_routes.create_history_export(
                SlowRequest(),  # type: ignore[arg-type]
                principal,
                credentials,
            )
        )
    response = TestClient(create_app()).post(
        "/api/v1/history/export",
        json={"bars": []},
        headers=AUTH_HEADERS,
    )

    assert caught.value.status_code == 408
    assert response.status_code == 200
