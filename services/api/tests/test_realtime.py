from __future__ import annotations

import asyncio
import base64
from concurrent.futures import CancelledError as FutureCancelledError
import json
import threading
import time
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

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
from app.market_sidebar.service import SidebarContext


def _ws_bearer_protocol(token: str) -> str:
    encoded = base64.urlsafe_b64encode(token.encode()).decode().rstrip("=")
    return f"tvchan.bearer.{encoded}"


def test_realtime_rejects_bad_token() -> None:
    client = TestClient(create_app())
    try:
        with client.websocket_connect("/ws/v1/realtime?token=bad-token"):
            raise AssertionError("websocket should not connect")
    except Exception:
        pass


def test_realtime_accepts_bearer_subprotocol_without_query_token() -> None:
    client = TestClient(create_app())
    with client.websocket_connect(
        "/ws/v1/realtime", subprotocols=[_ws_bearer_protocol("dev-local-token")]
    ) as ws:
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


def test_realtime_disconnect_waits_for_producer_cleanup(monkeypatch) -> None:
    cleanup_finished = asyncio.Event()
    producer_started = asyncio.Event()

    class FakeWebSocket:
        scope = {"app": SimpleNamespace(state=SimpleNamespace())}
        query_params = {}

        async def accept(self) -> None:
            return None

        async def receive_json(self):
            await producer_started.wait()
            raise realtime.WebSocketDisconnect()

    async def producer() -> None:
        try:
            producer_started.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.01)
            cleanup_finished.set()

    async def create_producer_task(_websocket, _subscriptions):
        return asyncio.create_task(producer())

    async def scenario() -> None:
        monkeypatch.setattr(
            realtime,
            "get_settings",
            lambda: SimpleNamespace(api_token="", admin_api_token=""),
        )
        monkeypatch.setattr(realtime, "_create_producer_task", create_producer_task)

        await realtime.realtime_ws(FakeWebSocket())

        assert cleanup_finished.is_set()

    asyncio.run(scenario())


def test_chart_ws_rejects_bad_token() -> None:
    client = TestClient(create_app())
    try:
        with client.websocket_connect("/ws/v2/chart?token=bad-token"):
            raise AssertionError("websocket should not connect")
    except Exception:
        pass


def test_chart_ws_accepts_bearer_subprotocol_without_query_token() -> None:
    client = TestClient(create_app())
    with client.websocket_connect(
        "/ws/v2/chart", subprotocols=[_ws_bearer_protocol("dev-local-token")]
    ) as ws:
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


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


def test_chart_ws_ping_is_not_blocked_by_a_slow_chart_query(monkeypatch) -> None:
    async def slow_overlay(**_kwargs):
        await asyncio.sleep(0.1)
        return ChanOverlayResponse(
            symbol="000001.SZ", chart_timeframe="5f", levels=["5f", "30f", "1d"],
            modes=["confirmed", "predictive"], snapshot_version="slow-snapshot",
            base_timeframe="5f", base_ts_semantics="bar_end", engine="test",
            requested_bar_count=20, bars_by_level={"5f": 20, "30f": 20, "1d": 20},
            strokes=[], segments=[], centers=[], signals=[], channels=[],
        )

    monkeypatch.setattr(chart_ws, "build_chan_overlay", slow_overlay)
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v2/chart?token=dev-local-token") as ws:
        ws.send_json({
            "type": "get_chan", "request_id": "slow", "symbol": "000001.SZ",
            "timeframe": "5f", "limit": 20,
        })
        ws.send_json({"type": "ping", "request_id": "ping-during-query"})
        pong = ws.receive_json()
        assert pong["type"] == "pong"
        assert pong["request_id"] == "ping-during-query"
        response = ws.receive_json()
        assert response["type"] == "chan_full"
        assert response["request_id"] == "slow"


def test_chart_ws_bounds_in_flight_requests_without_blocking_ping(monkeypatch) -> None:
    calls = 0

    async def slow_overlay(**_kwargs):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return ChanOverlayResponse(
            symbol="000001.SZ", chart_timeframe="5f", levels=["5f", "30f", "1d"],
            modes=["confirmed", "predictive"], snapshot_version="bounded",
            base_timeframe="5f", base_ts_semantics="bar_end", engine="test",
            requested_bar_count=20, bars_by_level={"5f": 20, "30f": 20, "1d": 20},
            strokes=[], segments=[], centers=[], signals=[], channels=[],
        )

    monkeypatch.setattr(chart_ws, "build_chan_overlay", slow_overlay)
    monkeypatch.setattr(chart_ws, "MAX_IN_FLIGHT_CHART_REQUESTS", 1)
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v2/chart?token=dev-local-token") as ws:
        base = {"type": "get_chan", "symbol": "000001.SZ", "timeframe": "5f", "limit": 20}
        ws.send_json({**base, "request_id": "first"})
        ws.send_json({**base, "request_id": "rejected"})
        ws.send_json({"type": "ping", "request_id": "still-responsive"})
        messages = [ws.receive_json() for _ in range(3)]
        types = [message["type"] for message in messages]
        assert types.index("pong") < types.index("chan_full")
        assert any(
            message.get("request_id") == "rejected"
            and message.get("error") == "Too many in-flight chart requests"
            for message in messages
        )
        assert calls == 1


def test_chart_ws_disconnect_cancels_in_flight_query(monkeypatch) -> None:
    started = threading.Event()
    cancelled = threading.Event()

    async def never_finishes(**_kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(chart_ws, "build_chan_overlay", never_finishes)
    client = TestClient(create_app())
    try:
        with client.websocket_connect("/ws/v2/chart?token=dev-local-token") as ws:
            ws.send_json({
                "type": "get_chan", "request_id": "cancel-me", "symbol": "000001.SZ",
                "timeframe": "5f", "limit": 20,
            })
            assert started.wait(timeout=1)
    except FutureCancelledError:
        # Starlette's synchronous test transport cancels the ASGI future when
        # exiting with an intentionally unfinished server task.
        pass
    assert cancelled.wait(timeout=1)


def test_chart_ws_sender_serializes_all_producers() -> None:
    class Socket:
        def __init__(self):
            self.active = 0
            self.max_active = 0
            self.messages = []

        async def send_json(self, payload):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0)
            self.messages.append(payload)
            self.active -= 1

        async def close(self, **_kwargs):
            pass

    async def scenario():
        socket = Socket()
        sender = chart_ws._ChartSender(socket)
        writer = asyncio.create_task(sender.run())
        await asyncio.gather(*(
            sender.send_json({"type": "event", "value": value})
            for value in range(8)
        ))
        await sender._outbox.join()
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        assert socket.max_active == 1
        assert len(socket.messages) == 8

    asyncio.run(scenario())


def test_chart_ws_sender_enforces_byte_budget(monkeypatch) -> None:
    class Socket:
        async def send_json(self, _payload):
            return None

    async def scenario():
        monkeypatch.setattr(chart_ws, "MAX_CHART_OUTBOX_BYTES", 100)
        monkeypatch.setattr(chart_ws, "CHART_SEND_TIMEOUT_SECONDS", 0.01)
        sender = chart_ws._ChartSender(Socket())
        await sender.send_json({"type": "event", "value": "x" * 45})
        with pytest.raises(TimeoutError):
            await sender.send_json({"type": "event", "value": "y" * 45})
        with pytest.raises(ValueError, match="outbound byte budget"):
            await sender.send_json({"type": "event", "value": "z" * 100})

    asyncio.run(scenario())


def test_chart_ws_global_query_slot_is_pool_linked_and_times_out(monkeypatch) -> None:
    async def scenario():
        assert chart_ws._chart_query_capacity(1) == 1
        assert chart_ws._chart_query_capacity(8) == 7
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        adapter = chart_ws._RequestAdapter(
            SimpleNamespace(state=SimpleNamespace(chart_query_semaphore=semaphore))
        )
        monkeypatch.setattr(chart_ws, "CHART_QUERY_SLOT_TIMEOUT_SECONDS", 0.01)
        with pytest.raises(chart_ws.ChartQueryCapacityError, match="server is busy"):
            async with chart_ws._chart_query_slot(adapter):
                raise AssertionError("capacity guard was bypassed")

    asyncio.run(scenario())


def test_chart_ws_cleanup_has_a_total_deadline() -> None:
    async def scenario():
        release = asyncio.Event()

        async def delays_cancellation():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

        task = asyncio.create_task(delays_cancellation())
        await asyncio.sleep(0)
        started = time.monotonic()
        await chart_ws._cancel_chart_tasks(task, timeout=0.01)
        assert time.monotonic() - started < 0.2
        assert not task.done()
        release.set()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(scenario())


def test_chart_ws_redis_client_has_read_timeout(monkeypatch) -> None:
    import redis.asyncio as redis

    captured = {}

    class Client:
        async def ping(self):
            return True

    def fake_from_url(url, **kwargs):
        captured.update(url=url, **kwargs)
        return Client()

    async def scenario():
        monkeypatch.setattr(redis, "from_url", fake_from_url)
        client = await chart_ws._try_create_redis_client("redis://test")
        assert client is not None
        assert captured["socket_connect_timeout"] == 0.2
        assert captured["socket_timeout"] == 0.5

    asyncio.run(scenario())


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


def test_chart_ws_bounds_subscriptions_per_connection(monkeypatch) -> None:
    overlay_calls = 0

    async def unexpected_overlay(**_kwargs):
        nonlocal overlay_calls
        overlay_calls += 1
        raise AssertionError("subscription admission must not build overlays")

    async def idle_publisher(*_args):
        await asyncio.sleep(3600)

    monkeypatch.setattr(chart_ws, "build_chan_overlay", unexpected_overlay)
    monkeypatch.setattr(chart_ws, "_publish_chan_updates", idle_publisher)
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v2/chart?token=dev-local-token") as ws:
        def subscribe(subscription_id: str) -> dict:
            ws.send_json(
                {
                    "type": "subscribe_chan",
                    "id": subscription_id,
                    "symbol": "000001.SZ",
                    "timeframe": "5f",
                    "from": 1_780_000_000,
                    "to": 1_780_001_200,
                }
            )
            return ws.receive_json()

        for index in range(chart_ws.MAX_SUBSCRIPTIONS_PER_CONNECTION):
            assert subscribe(f"sub_{index}")["type"] == "chan_subscribed"

        # Replacing an existing id is allowed and does not consume another slot.
        assert subscribe("sub_0")["type"] == "chan_subscribed"

        rejected = subscribe("overflow")
        assert rejected == {
            "type": "error",
            "request_id": "",
            "error": (
                "Too many subscriptions; maximum is "
                f"{chart_ws.MAX_SUBSCRIPTIONS_PER_CONNECTION}"
            ),
        }

        # The rejected id was not inserted, so releasing one real slot admits a new id.
        ws.send_json({"type": "unsubscribe_chan", "id": "sub_1"})
        assert ws.receive_json() == {"type": "chan_unsubscribed", "id": "sub_1"}
        assert subscribe("after_release")["type"] == "chan_subscribed"
        assert overlay_calls == 0


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


def test_realtime_bounds_subscriptions_per_connection(monkeypatch) -> None:
    async def idle_producer(_websocket, _subscriptions):
        return asyncio.create_task(asyncio.sleep(3600))

    monkeypatch.setattr(realtime, "_create_producer_task", idle_producer)
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
        def subscribe(subscription_id: str) -> dict:
            ws.send_json(
                {
                    "type": "subscribe",
                    "id": subscription_id,
                    "symbol": "000001.SZ",
                    "timeframes": ["5f"],
                }
            )
            return ws.receive_json()

        for index in range(realtime.MAX_SUBSCRIPTIONS_PER_CONNECTION):
            assert subscribe(f"sub_{index}") == {
                "type": "subscribed",
                "id": f"sub_{index}",
            }

        assert subscribe("sub_0") == {"type": "subscribed", "id": "sub_0"}
        assert subscribe("overflow") == {
            "type": "error",
            "message": (
                "Too many subscriptions; maximum is "
                f"{realtime.MAX_SUBSCRIPTIONS_PER_CONNECTION}"
            ),
        }

        ws.send_json({"type": "unsubscribe", "id": "sub_1"})
        assert ws.receive_json() == {"type": "unsubscribed", "id": "sub_1"}
        assert subscribe("after_release") == {
            "type": "subscribed",
            "id": "after_release",
        }


def test_realtime_rejects_amplifying_timeframe_inputs(monkeypatch) -> None:
    async def idle_producer(_websocket, _subscriptions):
        return asyncio.create_task(asyncio.sleep(3600))

    monkeypatch.setattr(realtime, "_create_producer_task", idle_producer)
    client = TestClient(create_app())
    with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
        invalid_inputs = [
            "5f",
            ["5f"] * 8,
            ["unsupported"],
        ]
        for index, timeframes in enumerate(invalid_inputs):
            ws.send_json(
                {
                    "type": "subscribe",
                    "id": f"invalid_{index}",
                    "symbol": "000001.SZ",
                    "timeframes": timeframes,
                }
            )
            assert ws.receive_json()["type"] == "error"

        # Every protocol timeframe remains valid; aliases and duplicates are de-duplicated.
        ws.send_json(
            {
                "type": "subscribe",
                "id": "deduped",
                "symbol": "000001.SZ",
                "timeframes": ["5f", "5", "15f", "1h"],
            }
        )
        assert ws.receive_json() == {"type": "subscribed", "id": "deduped"}

        ws.send_json(
            {
                "type": "subscribe",
                "id": "default_missing",
                "symbol": "000001.SZ",
            }
        )
        assert ws.receive_json() == {
            "type": "subscribed",
            "id": "default_missing",
        }

        for index, timeframes in enumerate((None, [], "")):
            ws.send_json(
                {
                    "type": "subscribe",
                    "id": f"default_{index}",
                    "symbol": "000001.SZ",
                    "timeframes": timeframes,
                }
            )
            assert ws.receive_json() == {
                "type": "subscribed",
                "id": f"default_{index}",
            }

    assert realtime._normalize_subscription_timeframes(
        ["5f", "5", "15f", "1h"]
    ) == [
        "5f",
        "15f",
        "1h",
    ]
    assert realtime._normalize_subscription_timeframes(None) == ["5f"]
    assert realtime._normalize_subscription_timeframes([]) == ["5f"]


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


def test_sidebar_update_parser_accepts_local_publications_and_filters_context() -> None:
    context = SidebarContext(
        "connection", "right-sidebar", "000001.SZ", 4, (),
        frozenset({"chan_strategy"}), watchlist_revision=3,
    )
    local = realtime._parse_sidebar_update('{"type":"chan_head_update","symbol":"000001.SZ"}')
    current = realtime._parse_sidebar_update('{"source":"iwencai","chart_symbol":"000001.SZ","chart_epoch":4,"watchlist_revision":3}')
    stale = realtime._parse_sidebar_update('{"source":"iwencai","chart_symbol":"000001.SZ","chart_epoch":3,"watchlist_revision":3}')

    assert local == {"type": "chan_head_update", "symbol": "000001.SZ", "source": "local_db"}
    assert realtime._sidebar_update_matches(local, context)
    assert realtime._sidebar_update_matches(current, context)
    assert not realtime._sidebar_update_matches(stale, context)


def test_sidebar_relay_marks_ready_only_after_subscription(monkeypatch) -> None:
    class PubSub:
        def __init__(self): self.subscribed = False
        async def subscribe(self, *_channels): self.subscribed = True
        def listen(self):
            async def iterator():
                await asyncio.Event().wait()
                yield {}
            return iterator()
        async def unsubscribe(self, *_channels): pass
        async def aclose(self): pass

    class Redis:
        def __init__(self): self.pubsub_instance = PubSub()
        def pubsub(self): return self.pubsub_instance
        async def aclose(self): pass

    async def scenario():
        client = Redis()
        async def fake_create(_url): return client
        monkeypatch.setattr(realtime, "_try_create_redis_client", fake_create)
        ready = asyncio.Event()
        context = SidebarContext("connection", "sidebar", "000001.SZ", 1, (), frozenset())
        task = asyncio.create_task(realtime._relay_sidebar_updates(asyncio.Queue(), "redis://test", context, ready))
        await asyncio.wait_for(ready.wait(), timeout=1)
        assert client.pubsub_instance.subscribed
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass

    asyncio.run(scenario())


def test_sidebar_relay_does_not_mark_ready_when_redis_is_unavailable(monkeypatch) -> None:
    async def scenario():
        async def no_redis(_url): return None
        monkeypatch.setattr(realtime, "_try_create_redis_client", no_redis)
        ready = asyncio.Event()
        context = SidebarContext("connection", "sidebar", "000001.SZ", 1, (), frozenset())
        await realtime._relay_sidebar_updates(asyncio.Queue(), "redis://test", context, ready)
        assert not ready.is_set()

    asyncio.run(scenario())


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
