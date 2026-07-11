from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.core.config import get_settings
from app.main import app
from app.models import BarsResponse, BarResponse, ChanOverlayResponse, ChanStrokeResponse, ChanPointResponse
from app.routes import chan, chart, health
from app.routes.chart import _snapshot_id

TOKEN_HEADER = {"Authorization": "Bearer dev-local-token"}


@pytest.fixture(autouse=True)
def clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_health_is_open() -> None:
    client = TestClient(app)
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["seed_data"] is True


def test_health_reports_module_c_coverage(monkeypatch) -> None:
    async def fake_module_c_status(_pool, _settings):
        return {
            "ready": True,
            "configured_config_hash": "module-c:test",
            "current_config_heads": 2,
            "published_smoke_candidate": {"symbol": "000001.SZ"},
            "coverage": [],
        }

    monkeypatch.setattr(health, "_module_c_status", fake_module_c_status)
    app.dependency_overrides[get_settings] = lambda: Settings(use_seed_data=False)
    try:
        response = TestClient(app).get("/api/v1/health")
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    body = response.json()
    assert body["module_c"]["ready"] is True
    assert body["module_c"]["published_smoke_candidate"]["symbol"] == "000001.SZ"


def test_chart_overlay_v3_accepts_bounded_twenty_month_request() -> None:
    response = TestClient(app).get(
        "/api/v3/chart/overlay",
        params={
            "symbol": "000001.SZ",
            "timeframe": "1m",
            "from": "2024-11-29T07:00:00Z",
            "to": "2026-06-30T07:00:00Z",
            "limit": 20,
        },
        headers=TOKEN_HEADER,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["levels"] == ["1m"]
    assert body["engine"] == "database:chan-published-empty"


def test_chart_overlay_v3_rejects_abusive_monthly_window_with_structured_error() -> None:
    response = TestClient(app).get(
        "/api/v3/chart/overlay",
        params={
            "symbol": "000001.SZ",
            "timeframe": "1m",
            "from": "2000-01-01T00:00:00Z",
            "to": "2026-07-01T00:00:00Z",
            "limit": 20,
        },
        headers=TOKEN_HEADER,
    )

    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "overlay_window_too_large"


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


def test_get_chan_overlay_returns_empty_published_overlay_when_no_precomputed_data() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/chan/overlay",
        params={"symbol": "000001.SZ", "timeframe": "15f", "limit": 60},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["engine"] == "database:chan-published-empty"
    assert body["strokes"] == []
    assert body["centers"] == []
    assert body["signals"] == []


def test_backend_chan_display_levels_follow_chart_timeframe() -> None:
    assert chan._display_levels_for_chart("5f") == ["5f", "30f", "1d"]
    assert chan._display_levels_for_chart("15f") == ["5f", "30f", "1d"]
    assert chan._display_levels_for_chart("30f") == ["30f", "1d"]
    assert chan._display_levels_for_chart("1d") == ["1d", "1w"]


def test_get_chart_window() -> None:
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
    assert body["chan"]["engine"] == "database:chan-published-empty"


def test_get_chart_bundle_v2() -> None:
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
    assert body["chan"]["engine"] == "database:chan-published-empty"


def test_get_chart_bundle_v3() -> None:
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


def test_get_chart_bars_v3_does_not_call_chan(monkeypatch) -> None:
    async def fail_if_called(**_kwargs):
        raise AssertionError("bars endpoint must not build chan overlay")

    monkeypatch.setattr(chart, "build_chan_overlay", fail_if_called)
    client = TestClient(app)
    response = client.get(
        "/api/v3/chart/bars",
        params={"symbol": "000001.SZ", "timeframe": "30f", "limit": 20},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "000001.SZ"
    assert body["timeframe"] == "30f"
    assert len(body["bars"]) > 0
    assert "chan" not in body


def test_get_chart_bars_v3_rejects_a_limit_above_transport_cap() -> None:
    response = TestClient(app).get(
        "/api/v3/chart/bars",
        params={"symbol": "000001.SZ", "timeframe": "5f", "limit": 5001},
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 422


def test_get_chart_bars_v3_passes_an_exclusive_end_to_repository(monkeypatch) -> None:
    async def fake_resolve_symbol_db(_pool, _symbol):
        return {"symbol": "000001.SZ", "code": "000001", "exchange": "SZ", "name": "Ping An", "asset_type": "stock"}

    async def fake_get_bars_db(_pool, _symbol, _timeframe, start, end, limit, *, end_exclusive=False):
        assert start is not None
        assert end == datetime(2026, 1, 2, tzinfo=UTC)
        assert limit == 3
        assert end_exclusive is True
        return []

    monkeypatch.setattr(chart, "resolve_symbol_db", fake_resolve_symbol_db)
    monkeypatch.setattr(chart, "get_bars_db", fake_get_bars_db)
    app.dependency_overrides[get_settings] = lambda: Settings(use_seed_data=False)
    try:
        with TestClient(app) as client:
            app.state.db_pool = "fake-pool"
            response = client.get(
                "/api/v3/chart/bars",
                params={"symbol": "000001.SZ", "timeframe": "5f", "from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z", "limit": 3},
                headers=TOKEN_HEADER,
            )
            app.state.db_pool = None
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    assert response.json()["bars"] == []


def test_get_chart_bars_v3_returns_empty_for_unknown_database_symbol(monkeypatch) -> None:
    async def fake_resolve_symbol_db(pool, symbol):
        assert pool == "fake-pool"
        assert symbol == "920394.BJ"
        return None

    monkeypatch.setattr(chart, "resolve_symbol_db", fake_resolve_symbol_db)
    app.dependency_overrides[get_settings] = lambda: Settings(use_seed_data=False)
    try:
        with TestClient(app) as client:
            app.state.db_pool = "fake-pool"
            response = client.get(
                "/api/v3/chart/bars",
                params={"symbol": "920394.BJ", "timeframe": "5f", "limit": 64},
                headers=TOKEN_HEADER,
            )
            app.state.db_pool = None
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "920394.BJ"
    assert body["timeframe"] == "5f"
    assert body["bars"] == []


def test_get_chart_overlay_v3_returns_empty_for_unknown_database_symbol(monkeypatch) -> None:
    async def fake_windowed_module_c_overlay(pool, **kwargs):
        assert pool == "fake-pool"
        assert kwargs["symbol"] == "920394.BJ"
        return None

    monkeypatch.setattr(chan, "get_windowed_module_c_overlay_db", fake_windowed_module_c_overlay)
    app.dependency_overrides[get_settings] = lambda: Settings(use_seed_data=False)
    try:
        with TestClient(app) as client:
            app.state.db_pool = "fake-pool"
            response = client.get(
                "/api/v3/chart/overlay",
                params={
                    "symbol": "920394.BJ", "timeframe": "5f", "limit": 64,
                    "from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z",
                },
                headers=TOKEN_HEADER,
            )
            app.state.db_pool = None
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    assert response.json()["engine"] == "database:chan-published-empty"


def test_get_chart_overlay_v3_returns_empty_when_module_c_has_no_published_head(monkeypatch) -> None:
    async def fake_windowed_module_c_overlay(pool, **kwargs):
        assert pool == "fake-pool"
        assert kwargs["levels"] == ["1d", "1w"]
        return None

    monkeypatch.setattr(chan, "get_windowed_module_c_overlay_db", fake_windowed_module_c_overlay)
    app.dependency_overrides[get_settings] = lambda: Settings(use_seed_data=False)
    try:
        with TestClient(app) as client:
            app.state.db_pool = "fake-pool"
            response = client.get(
                "/api/v3/chart/overlay",
                params={
                    "symbol": "000001.SZ", "timeframe": "1d", "limit": 300,
                    "from": "2025-07-01T00:00:00Z", "to": "2026-06-30T00:00:00Z",
                },
                headers=TOKEN_HEADER,
            )
            app.state.db_pool = None
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    assert response.json()["engine"] == "database:chan-published-empty"


def test_get_chart_overlay_v3_respects_levels(monkeypatch) -> None:
    async def fake_build_chan_overlay(**kwargs):
        assert kwargs["levels"] == "30f,1d"
        assert kwargs["modes"] == "confirmed"
        return ChanOverlayResponse(
            symbol=kwargs["symbol"],
            chart_timeframe=kwargs["timeframe"],
            levels=["30f", "1d"],
            modes=["confirmed"],
            snapshot_version="test-overlay",
            base_timeframe="5f",
            base_ts_semantics="bar_end",
            engine="test-overlay-builder",
            requested_bar_count=kwargs["limit"],
            bars_by_level={"30f": 2, "1d": 1},
            strokes=[
                ChanStrokeResponse(
                    id="30f-stroke",
                    level="30f",
                    mode="confirmed",
                    start=ChanPointResponse(time=1_780_950_600, price=10.0),
                    end=ChanPointResponse(time=1_780_952_400, price=10.8),
                    direction="up",
                    confirmed=True,
                ),
                ChanStrokeResponse(
                    id="1d-stroke",
                    level="1d",
                    mode="confirmed",
                    start=ChanPointResponse(time=1_780_900_000, price=9.8),
                    end=ChanPointResponse(time=1_780_952_400, price=10.8),
                    direction="up",
                    confirmed=True,
                ),
            ],
            segments=[],
            centers=[],
            signals=[],
        )

    monkeypatch.setattr(chart, "build_chan_overlay", fake_build_chan_overlay)
    client = TestClient(app)
    response = client.get(
        "/api/v3/chart/overlay",
        params={
            "symbol": "000001.SZ",
            "timeframe": "30f",
            "limit": 40,
            "levels": "30f,1d",
            "modes": "confirmed",
        },
        headers=TOKEN_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["chart_timeframe"] == "30f"
    assert body["levels"] == ["30f", "1d"]
    assert {item["level"] for item in body["strokes"]} == {"30f", "1d"}


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

    first = _snapshot_id(bars, "database:chan-module-c-windowed", "snapshot-a")
    second = _snapshot_id(bars, "database:chan-module-c-windowed", "snapshot-b")

    assert first != second
