from __future__ import annotations

import asyncio
import json

from fastapi.testclient import TestClient

from app.main import create_app
from app.models import (
    BarResponse,
    ChanOverlayResponse,
    ChartBundleChanLevelResponse,
    ChartBundleChanResponse,
    ChartBundleSourceWatermarksResponse,
    ChartBundleV3Response,
    ChartWindowRangeResponse,
    ChartWindowResponse,
)
from app.routes import chart_ws
from app.routes import realtime


def test_realtime_rejects_bad_token() -> None:
    client = TestClient(create_app())
    try:
        with client.websocket_connect("/ws/v1/realtime?token=bad-token"):
            raise AssertionError("websocket should not connect")
    except Exception:
        pass


def test_chart_ws_rejects_bad_token() -> None:
    client = TestClient(create_app())
    try:
        with client.websocket_connect("/ws/v2/chart?token=bad-token"):
            raise AssertionError("websocket should not connect")
    except Exception:
        pass


def test_chart_ws_request_response_protocol(monkeypatch) -> None:
    overlay = ChanOverlayResponse(
        symbol="000001.SZ",
        chart_timeframe="5f",
        levels=["5f", "30f", "1d"],
        modes=["confirmed", "predictive"],
        snapshot_version="snapshot-test",
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="database:chan-module-c-windowed",
        requested_bar_count=20,
        bars_by_level={"5f": 20, "30f": 20, "1d": 20},
        strokes=[],
        segments=[],
        centers=[],
        signals=[],
    )
    bars = [
        BarResponse(
            time=1_780_000_000,
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=100,
            amount=1000,
            complete=True,
            revision=0,
        )
    ]

    async def fake_build_chan_overlay(**kwargs):
        return overlay

    async def fake_build_chart_window(**kwargs):
        return ChartWindowResponse(
            schema_version="chart-window.v1",
            snapshot_id="window-test",
            symbol="000001.SZ",
            chart_timeframe="5f",
            range=ChartWindowRangeResponse(from_time=None, to_time=None, limit=20),
            bars=bars,
            chan=overlay,
        )

    async def fake_build_chart_bundle_v3(**kwargs):
        levels = {
            level: ChartBundleChanLevelResponse(
                bar_count=20,
                strokes=[],
                segments=[],
                centers=[],
                signals=[],
                channels=[],
            )
            for level in ["5f", "30f", "1d"]
        }
        return ChartBundleV3Response(
            schema_version="chart-bundle.v3",
            snapshot_id="bundle-test",
            snapshot_version="snapshot-test",
            symbol="000001.SZ",
            chart_timeframe="5f",
            base_timeframe="5f",
            bar_time_semantics="bar_end",
            range=ChartWindowRangeResponse(from_time=None, to_time=None, limit=20),
            analysis_levels=["5f", "30f", "1d"],
            bars=bars,
            chan=ChartBundleChanResponse(engine="database:chan-module-c-windowed", levels=levels),
            source_watermarks=ChartBundleSourceWatermarksResponse(
                canonical_5f_last_complete_end=1_780_000_000,
                canonical_5f_last_seen_end=1_780_000_000,
                view_last_complete_end=1_780_000_000,
                analysis_generated_at=1_780_000_000,
                analysis_source="test",
                aggregation_source="canonical-5f",
            ),
            warnings=[],
        )

    monkeypatch.setattr(chart_ws, "build_chan_overlay", fake_build_chan_overlay)
    monkeypatch.setattr(chart_ws, "build_chart_window", fake_build_chart_window)
    monkeypatch.setattr(chart_ws, "build_chart_bundle_v3", fake_build_chart_bundle_v3)

    client = TestClient(create_app())
    with client.websocket_connect("/ws/v2/chart?token=dev-local-token") as ws:
        ws.send_json({"type": "ping", "request_id": "ping_1"})
        assert ws.receive_json()["type"] == "pong"

        ws.send_json(
            {
                "type": "get_bars",
                "request_id": "bars_1",
                "symbol": "000001.SZ",
                "timeframe": "5f",
                "limit": 20,
            }
        )
        bars_message = ws.receive_json()
        assert bars_message["type"] == "bars"
        assert bars_message["request_id"] == "bars_1"
        assert bars_message["symbol"] == "000001.SZ"
        assert bars_message["timeframe"] == "5f"
        assert len(bars_message["bars"]) > 0

        ws.send_json(
            {
                "type": "get_chan",
                "request_id": "chan_1",
                "symbol": "000001.SZ",
                "timeframe": "5f",
                "limit": 20,
                "from": 1_780_000_000,
                "to": 1_780_001_200,
                "levels": ["5f", "30f", "1d"],
                "modes": ["confirmed", "predictive"],
            }
        )
        chan_message = ws.receive_json()
        assert chan_message["type"] == "chan_full"
        assert chan_message["request_id"] == "chan_1"
        assert chan_message["chan"]["levels"] == ["5f", "30f", "1d"]

        ws.send_json(
            {
                "type": "get_chart_window",
                "request_id": "window_1",
                "symbol": "000001.SZ",
                "timeframe": "5f",
                "limit": 20,
            }
        )
        window_message = ws.receive_json()
        assert window_message["type"] == "chart_window"
        assert window_message["request_id"] == "window_1"
        window = window_message["window"]
        assert window["schema_version"] == "chart-window.v1"
        assert window["snapshot_id"]
        assert len(window["bars"]) > 0
        assert window["chan"]["levels"] == ["5f", "30f", "1d"]

        ws.send_json(
            {
                "type": "get_chart_bundle",
                "request_id": "bundle_1",
                "symbol": "000001.SZ",
                "timeframe": "5f",
                "limit": 20,
            }
        )
        bundle_message = ws.receive_json()
        assert bundle_message["type"] == "chart_bundle"
        assert bundle_message["request_id"] == "bundle_1"
        bundle = bundle_message["bundle"]
        assert bundle["schema_version"] == "chart-bundle.v3"
        assert bundle["snapshot_id"]
        assert len(bundle["bars"]) > 0
        assert set(bundle["chan"]["levels"]) == {"5f", "30f", "1d"}


def test_chart_ws_subscribe_chan_emits_snapshot_and_delta(monkeypatch) -> None:
    monkeypatch.setattr(chart_ws, "CHAN_SNAPSHOT_INTERVAL_SECONDS", 0.01)
    emitted = {"count": 0}

    async def fake_build_chan_overlay(**kwargs):
        emitted["count"] += 1
        snapshot_version = "snapshot-1" if emitted["count"] == 1 else "snapshot-2"
        return ChanOverlayResponse(
            symbol="000001.SZ",
            chart_timeframe="5f",
            levels=["5f", "30f", "1d"],
            modes=["confirmed", "predictive"],
            snapshot_version=snapshot_version,
            base_timeframe="5f",
            base_ts_semantics="bar_end",
            engine="database:chan-module-c-windowed",
            requested_bar_count=20,
            bars_by_level={"5f": 20, "30f": 20, "1d": 20},
            strokes=[],
            segments=[],
            centers=[],
            signals=[],
        )

    monkeypatch.setattr(chart_ws, "build_chan_overlay", fake_build_chan_overlay)

    client = TestClient(create_app())
    with client.websocket_connect("/ws/v2/chart?token=dev-local-token") as ws:
        ws.send_json(
            {
                "type": "subscribe_chan",
                "id": "chan_sub_1",
                "symbol": "000001.SZ",
                "timeframe": "5f",
                "limit": 20,
                "from": 1_780_000_000,
                "to": 1_780_001_200,
                "levels": ["5f", "30f", "1d"],
                "modes": ["confirmed", "predictive"],
            }
        )
        subscribed = ws.receive_json()
        assert subscribed["type"] == "chan_subscribed"
        assert subscribed["chart_timeframe"] == "5f"
        assert subscribed["levels"] == ["5f", "30f", "1d"]

        first = ws.receive_json()
        assert first["type"] == "chan_overlay"
        assert first["kind"] == "snapshot"
        assert first["id"] == "chan_sub_1"
        assert first["snapshot_version"] == "snapshot-1"
        assert first["range"] == {"from": 1_780_000_000, "to": 1_780_001_200}

        second = ws.receive_json()
        assert second["type"] == "chan_overlay"
        # A version-only change is a bounded snapshot, not a fake empty delta.
        assert second["kind"] == "snapshot"
        assert second["id"] == "chan_sub_1"
        assert second["snapshot_version"] == "snapshot-2"

        ws.send_json({"type": "unsubscribe_chan", "id": "chan_sub_1"})
        assert ws.receive_json() == {"type": "chan_unsubscribed", "id": "chan_sub_1"}


def test_realtime_ping_and_subscribe() -> None:
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"
        ws.send_json(
            {
                "type": "subscribe",
                "id": "sub_1",
                "symbol": "000001.SZ",
                "timeframes": ["5f"],
            }
        )
        assert ws.receive_json() == {"type": "subscribed", "id": "sub_1"}


def test_realtime_sends_bar_update(monkeypatch) -> None:
    monkeypatch.setattr(realtime, "UPDATE_INTERVAL_SECONDS", 0.01)

    async def no_redis(_redis_url):
        return None

    monkeypatch.setattr(realtime, "_try_create_redis_client", no_redis)
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
        ws.send_json(
            {
                "type": "subscribe",
                "id": "sub_1",
                "symbol": "000001.SZ",
                "timeframes": ["5f"],
            }
        )
        assert ws.receive_json() == {"type": "subscribed", "id": "sub_1"}
        message = ws.receive_json()
        assert message["type"] == "bar_update"
        assert message["seq"] == 1
        assert message["symbol"] == "000001.SZ"
        assert message["timeframe"] == "5f"
        assert message["snapshot_version"].startswith("rt:000001.SZ:5f:")
        assert "bar" in message


def test_realtime_parses_redis_bar_update() -> None:
    payload = realtime._parse_redis_message(
        '{"symbol":"000001.sz","timeframe":"5","bar":{"time":1}}'
    )
    assert payload == {
        "symbol": "000001.SZ",
        "timeframe": "5f",
        "bar": {"time": 1},
    }


def test_realtime_matches_subscriptions() -> None:
    subscriptions = {
        "sub_1": {
            "id": "sub_1",
            "symbol": "000001.SZ",
            "timeframes": ["5f", "1d"],
        }
    }
    assert realtime._matches_subscriptions(
        {"symbol": "000001.SZ", "timeframe": "5f", "bar": {}},
        subscriptions,
    )
    assert not realtime._matches_subscriptions(
        {"symbol": "000002.SZ", "timeframe": "5f", "bar": {}},
        subscriptions,
    )


def test_realtime_sends_redis_pubsub_update() -> None:
    class FakeWebSocket:
        def __init__(self) -> None:
            self.messages = []

        async def send_json(self, payload):
            self.messages.append(payload)

    class FakePubSub:
        def __init__(self) -> None:
            self.closed = False
            self.unsubscribed = False

        async def subscribe(self, _channel):
            return None

        def listen(self):
            async def iterator():
                yield {
                    "type": "message",
                    "data": json.dumps(
                        {
                            "symbol": "000001.SZ",
                            "timeframe": "5f",
                            "bar": {"time": 1, "close": 10.5},
                        }
                    ),
                }

            return iterator()

        async def unsubscribe(self, _channel):
            self.unsubscribed = True

        async def aclose(self):
            self.closed = True

    class FakeRedisClient:
        def __init__(self, pubsub):
            self.pubsub_instance = pubsub
            self.closed = False

        def pubsub(self):
            return self.pubsub_instance

        async def aclose(self):
            self.closed = True

    async def scenario():
        websocket = FakeWebSocket()
        pubsub = FakePubSub()
        client = FakeRedisClient(pubsub)
        subscriptions = {
            "sub_1": {
                "id": "sub_1",
                "symbol": "000001.SZ",
                "timeframes": ["5f"],
            }
        }

        await realtime._send_redis_updates(websocket, subscriptions, client)
        expected_version = realtime._bar_snapshot_version(
            "000001.SZ",
            "5f",
            {"time": 1, "close": 10.5},
        )

        assert websocket.messages == [
            {
                "type": "bar_update",
                "seq": 1,
                "symbol": "000001.SZ",
                "timeframe": "5f",
                "snapshot_version": expected_version,
                "bar": {"time": 1, "close": 10.5},
                "source": "redis",
            }
        ]
        assert pubsub.unsubscribed
        assert pubsub.closed
        assert client.closed

    asyncio.run(scenario())


def test_bar_snapshot_version_is_monotonic_for_same_symbol_and_timeframe() -> None:
    first = realtime._bar_snapshot_version(
        "000001.SZ",
        "5f",
        {"time": 1, "revision": 1, "complete": False},
    )
    second = realtime._bar_snapshot_version(
        "000001.SZ",
        "5f",
        {"time": 1, "revision": 2, "complete": False},
    )
    third = realtime._bar_snapshot_version(
        "000001.SZ",
        "5f",
        {"time": 2, "revision": 0, "complete": True},
    )
    assert first < second < third


def test_bar_snapshot_version_changes_for_ohlcv_revision() -> None:
    first = realtime._bar_snapshot_version(
        "000001.SZ",
        "5f",
        {"time": 1, "open": 10, "high": 10.5, "low": 9.9, "close": 10.2, "volume": 100},
    )
    second = realtime._bar_snapshot_version(
        "000001.SZ",
        "5f",
        {"time": 1, "open": 10, "high": 10.6, "low": 9.9, "close": 10.2, "volume": 100},
    )

    assert first != second
