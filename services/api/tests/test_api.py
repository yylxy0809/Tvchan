from __future__ import annotations

import json
import os

import httpx
import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.core.config import get_settings
from app.main import app
from app.models import BarsResponse, BarResponse, ChanOverlayResponse, ChanStrokeResponse, ChanPointResponse
from app.routes import chan
from app.routes.chart import _snapshot_id

TOKEN_HEADER = {"Authorization": "Bearer dev-local-token"}


@pytest.fixture(autouse=True)
def clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _mock_chan_service(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_SERVICE_URL", "http://chan-service.test")
    original_client = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        item = json.loads(request.read())
        first = item["bars"][0]
        last = item["bars"][-1]
        mode = item["modes"][0]
        body = {
            "symbol": item["symbol"],
            "timeframe": item["timeframe"],
            "snapshot_version": "test-snapshot",
            "base_timeframe": "5f",
            "base_ts_semantics": "bar_end",
            "engine": "module-b:chan.py",
            "strokes": [
                {
                    "id": f"{level}:stroke",
                    "level": level,
                    "mode": mode,
                    "start": {"time": first["time"], "price": first["open"], "base_ts": first["time"]},
                    "end": {"time": last["time"], "price": last["close"], "base_ts": last["time"]},
                    "begin_base_ts": first["time"],
                    "end_base_ts": last["time"],
                    "direction": "up",
                    "confirmed": True,
                }
                for level in item["chan_levels"]
            ],
            "segments": [],
            "centers": [],
            "signals": [],
            "channels": [],
        }
        return httpx.Response(200, json=body)

    transport = httpx.MockTransport(handler)

    def mock_client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)


def test_health_is_open() -> None:
    client = TestClient(app)
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["seed_data"] is True


def test_symbols_require_token() -> None:
    client = TestClient(app)
    response = client.get("/api/v1/symbols")
    assert response.status_code == 401


def test_symbol_search() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/symbols", params={"keyword": "平安"}, headers=TOKEN_HEADER
    )
    assert response.status_code == 200
    symbols = [item["symbol"] for item in response.json()["items"]]
    assert "000001.SZ" in symbols


def test_get_seed_bars() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/bars",
        params={"symbol": "000001.SZ", "timeframe": "5f", "limit": 20},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "000001.SZ"
    assert body["timeframe"] == "5f"
    assert len(body["bars"]) > 0
    for bar in body["bars"]:
        assert bar["low"] <= bar["open"] <= bar["high"]
        assert bar["low"] <= bar["close"] <= bar["high"]


def test_month_resolution_maps_to_month_timeframe() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/bars",
        params={"symbol": "000001.SZ", "timeframe": "M", "limit": 3},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    assert response.json()["timeframe"] == "1m"


def test_weekly_seed_bars_exist_on_weekend_runs() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/bars",
        params={"symbol": "000001.SZ", "timeframe": "1w", "limit": 3},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    assert len(response.json()["bars"]) > 0


def test_get_chan_overlay_requires_formal_backend_when_no_precomputed_data() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/chan/overlay",
        params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 60},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 503
    assert "requires precomputed data or CHAN_SERVICE_URL" in response.json()["detail"]


def test_get_chart_window(monkeypatch) -> None:
    _mock_chan_service(monkeypatch)
    client = TestClient(app)
    response = client.get(
        "/api/v1/chart/window",
        params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 40},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "chart-window.v1"
    assert body["snapshot_id"]
    assert body["symbol"] == "000001.SZ"
    assert body["chart_timeframe"] == "15f"
    assert body["range"]["limit"] == 40
    assert len(body["bars"]) > 0
    assert body["chan"]["symbol"] == "000001.SZ"
    assert body["chan"]["chart_timeframe"] == "15f"
    assert body["chan"]["levels"] == ["5f", "30f", "1d"]
    assert body["chan"]["base_timeframe"] == "5f"
    assert body["chan"]["base_ts_semantics"] == "bar_end"
    assert body["chan"]["requested_bar_count"] == 40


def test_get_chart_bundle_v2(monkeypatch) -> None:
    _mock_chan_service(monkeypatch)
    client = TestClient(app)
    response = client.get(
        "/api/v2/chart/bundle",
        params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 40},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "chart-bundle.v2"
    assert body["snapshot_id"]
    assert body["symbol"] == "000001.SZ"
    assert body["chart_timeframe"] == "15f"
    assert body["range"]["limit"] == 40
    assert len(body["bars"]) > 0
    assert body["chan"]["symbol"] == "000001.SZ"
    assert body["chan"]["chart_timeframe"] == "15f"
    assert body["chan"]["levels"] == ["5f", "30f", "1d"]
    assert body["chan"]["base_timeframe"] == "5f"
    assert body["chan"]["base_ts_semantics"] == "bar_end"
    assert body["chan"]["requested_bar_count"] == 40


def test_get_chart_bundle_v3(monkeypatch) -> None:
    _mock_chan_service(monkeypatch)
    client = TestClient(app)
    response = client.get(
        "/api/v3/chart/bundle",
        params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 40},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "chart-bundle.v3"
    assert body["snapshot_id"]
    assert body["snapshot_version"]
    assert body["symbol"] == "000001.SZ"
    assert body["chart_timeframe"] == "15f"
    assert body["base_timeframe"] == "5f"
    assert body["bar_time_semantics"] == "bar_end"
    assert body["analysis_levels"] == ["5f", "30f", "1d"]
    assert body["range"]["limit"] == 40
    assert len(body["bars"]) > 0
    assert set(body["chan"]["levels"]) == {"5f", "30f", "1d"}
    assert set(body["chan"]["levels"]["5f"]) == {
        "bar_count",
        "strokes",
        "segments",
        "centers",
        "signals",
        "channels",
    }
    assert body["source_watermarks"]["aggregation_source"] == "canonical-5f"
    assert body["source_watermarks"]["analysis_generated_at"] > 0
    assert isinstance(body["warnings"], list)


def test_chart_snapshot_id_tracks_chan_snapshot_version() -> None:
    bars = BarsResponse(
        symbol="000001.SZ",
        timeframe="15f",
        bars=[
            BarResponse(
                time=1_700_000_000,
                open=10,
                high=11,
                low=9,
                close=10.5,
                volume=1000,
                amount=None,
                complete=True,
                revision=0,
            ),
            BarResponse(
                time=1_700_000_900,
                open=10.5,
                high=11.2,
                low=10.2,
                close=11,
                volume=1200,
                amount=None,
                complete=True,
                revision=1,
            ),
        ],
    )

    first = _snapshot_id(bars, "chan-service:chan.py", "snapshot-a")
    second = _snapshot_id(bars, "chan-service:chan.py", "snapshot-b")

    assert first != second


def test_get_chan_overlay_can_use_chan_service(monkeypatch) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("CHAN_SERVICE_URL", "http://chan-service.test")

    captured: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = request.read()
        assert request.url.path == "/analyze"
        item = __import__("json").loads(payload)
        captured.append(item)
        assert item["timeframe"] == "5f"
        assert item["chan_levels"] == ["5f", "30f", "1d"]
        mode = item["modes"][0]
        assert len(item["bars"]) == 37
        first = item["bars"][0]
        last = item["bars"][-1]
        body = {
            "symbol": item["symbol"],
            "timeframe": item["timeframe"],
            "snapshot_version": "test-snapshot",
            "base_timeframe": "5f",
            "base_ts_semantics": "bar_end",
            "engine": "chan.py",
            "strokes": [
                {
                    "id": f"{level}:stroke",
                    "level": level,
                    "mode": mode,
                    "start": {
                        "time": first["time"],
                        "price": first["open"],
                        "base_ts": first["time"],
                    },
                    "end": {
                        "time": last["time"],
                        "price": last["close"],
                        "base_ts": last["time"],
                    },
                    "begin_base_ts": first["time"],
                    "end_base_ts": last["time"],
                    "direction": "up",
                    "confirmed": True,
                }
                for level in item["chan_levels"]
            ],
            "segments": [],
            "centers": [],
            "signals": [],
        }
        return httpx.Response(200, json=body)

    transport = httpx.MockTransport(handler)
    original_client = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)
    try:
        client = TestClient(app)
        response = client.get(
            "/api/v1/chan/overlay",
            params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 37},
            headers=TOKEN_HEADER,
        )
    finally:
        if "CHAN_SERVICE_URL" in os.environ:
            monkeypatch.delenv("CHAN_SERVICE_URL")
        get_settings.cache_clear()

    assert response.status_code == 200
    body = response.json()
    assert body["engine"] == "chan-service:chan.py"
    assert body["chart_timeframe"] == "15f"
    assert body["levels"] == ["5f", "30f", "1d"]
    assert body["snapshot_version"] == "test-snapshot"
    assert body["base_timeframe"] == "5f"
    assert body["base_ts_semantics"] == "bar_end"
    assert body["requested_bar_count"] == 37
    assert body["bars_by_level"] == {"5f": 37, "30f": 37, "1d": 37}
    assert len(captured) == 1
    assert captured[0]["timeframe"] == "5f"
    assert captured[0]["chan_levels"] == ["5f", "30f", "1d"]


def test_get_chan_overlay_uses_precomputed_database_when_chan_service_is_disabled(monkeypatch) -> None:
    async def fake_resolve_symbol_db(pool, symbol):
        assert pool == "fake-pool"
        return {"symbol": "000001.SZ"}

    async def fake_get_bars_db(pool, symbol, timeframe, start, end, limit):
        assert pool == "fake-pool"
        base = 1_700_000_000
        return [
            {
                "time": base + index * 300,
                "open": 10 + index,
                "high": 10.5 + index,
                "low": 9.5 + index,
                "close": 10.2 + index,
                "volume": 1000 + index,
                "amount": None,
                "complete": True,
                "revision": 0,
            }
            for index in range(limit)
        ]

    async def fake_precomputed(pool, **kwargs):
        assert pool == "fake-pool"
        assert kwargs["levels"] == ["5f", "30f", "1d"]
        assert kwargs["requested_bar_count"] == 5
        assert set(kwargs["bars_by_level"]) == {"5f", "15f"}
        assert len(kwargs["bars_by_level"]["5f"]) == 5
        assert len(kwargs["bars_by_level"]["15f"]) == 5
        return ChanOverlayResponse(
            symbol="000001.SZ",
            chart_timeframe=kwargs["chart_timeframe"],
            levels=kwargs["levels"],
            modes=kwargs["modes"],
            snapshot_version="db-snapshot",
            base_timeframe="5f",
            base_ts_semantics="bar_end",
            engine="database:chan-precomputed",
            requested_bar_count=kwargs["requested_bar_count"],
            bars_by_level={"5f": 5, "30f": 5, "1d": 5},
            strokes=[
                ChanStrokeResponse(
                    id="cached-stroke",
                    level="5f",
                    mode="confirmed",
                    start=ChanPointResponse(time=1_700_000_000, price=10, base_ts=1_700_000_000),
                    end=ChanPointResponse(time=1_700_001_200, price=12, base_ts=1_700_001_200),
                    begin_base_ts=1_700_000_000,
                    end_base_ts=1_700_001_200,
                    direction="up",
                    confirmed=True,
                )
            ],
            segments=[],
            centers=[],
            signals=[],
        )

    monkeypatch.setattr(chan, "resolve_symbol_db", fake_resolve_symbol_db)
    monkeypatch.setattr(chan, "get_bars_db", fake_get_bars_db)
    monkeypatch.setattr(chan, "get_precomputed_chan_overlay_db", fake_precomputed)
    app.dependency_overrides[get_settings] = lambda: Settings(
        use_seed_data=False,
        chan_service_url=None,
    )
    try:
        with TestClient(app) as client:
            app.state.db_pool = "fake-pool"
            response = client.get(
                "/api/v1/chan/overlay",
                params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 5},
                headers=TOKEN_HEADER,
            )
            app.state.db_pool = None
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    body = response.json()
    assert body["engine"] == "database:chan-precomputed"
    assert body["snapshot_version"] == "db-snapshot"
    assert body["strokes"][0]["id"] == "cached-stroke"


def test_get_chan_overlay_returns_503_when_chan_service_is_enabled_but_fails(monkeypatch) -> None:
    async def fake_resolve_symbol_db(pool, symbol):
        assert pool == "fake-pool"
        return {"symbol": "000001.SZ"}

    async def fake_get_bars_db(pool, symbol, timeframe, start, end, limit):
        assert pool == "fake-pool"
        base = 1_700_000_000
        return [
            {
                "time": base + index * 300,
                "open": 10 + index,
                "high": 10.5 + index,
                "low": 9.5 + index,
                "close": 10.2 + index,
                "volume": 1000 + index,
                "amount": None,
                "complete": True,
                "revision": 0,
            }
            for index in range(limit)
        ]

    async def fake_analyze_with_chan_service(**kwargs):
        raise chan.ChanServiceError("Chan service analyze failed: boom")

    async def fake_precomputed(pool, **kwargs):
        assert pool == "fake-pool"
        return None

    monkeypatch.setattr(chan, "resolve_symbol_db", fake_resolve_symbol_db)
    monkeypatch.setattr(chan, "get_bars_db", fake_get_bars_db)
    monkeypatch.setattr(chan, "analyze_with_chan_service", fake_analyze_with_chan_service)
    monkeypatch.setattr(chan, "get_precomputed_chan_overlay_db", fake_precomputed)
    app.dependency_overrides[get_settings] = lambda: Settings(
        use_seed_data=False,
        chan_service_url="http://chan-service.test",
    )
    try:
        with TestClient(app) as client:
            app.state.db_pool = "fake-pool"
            response = client.get(
                "/api/v1/chan/overlay",
                params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 5},
                headers=TOKEN_HEADER,
            )
            app.state.db_pool = None
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 503
    assert "Chan service analyze failed" in response.json()["detail"]
