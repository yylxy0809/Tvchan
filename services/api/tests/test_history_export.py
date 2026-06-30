from __future__ import annotations

import gzip
import hashlib
import json

from fastapi.testclient import TestClient

from app.main import create_app


AUTH_HEADERS = {"Authorization": "Bearer dev-local-token"}


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
