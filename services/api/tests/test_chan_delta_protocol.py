from __future__ import annotations

import asyncio

import pytest

from app.routes import chart_ws
from app.services.chan_delta_protocol import (
    ChanEventValidationError,
    diff_objects,
    validate_chan_event,
    validate_chan_source_event,
)


def _event(*, kind: str = "snapshot", sequence: int = 1) -> dict:
    return {
        "type": "chan_overlay",
        "schema_version": "chan-event.v1",
        "kind": kind,
        "id": "sub-1",
        "symbol": "000001.SZ",
        "chart_timeframe": "5f",
        "modes": ["confirmed", "predictive"],
        "snapshot_version": "v2" if kind == "delta" else "v1",
        "base_version": "v1" if kind == "delta" else None,
        "sequence": sequence,
        "range": {"from": 100, "to": 200},
        "upserts": {
            "strokes": [{"id": "stroke-1"}],
            "segments": [],
            "centers": [],
            "signals": [],
            "channels": [],
        },
        "deletes": {"strokes": [], "segments": [], "centers": [], "signals": [], "channels": []},
    }


def test_chan_event_envelope_validates_required_delta_contract() -> None:
    event = _event(kind="delta")
    assert validate_chan_event(event) is event

    event["base_version"] = None
    with pytest.raises(ChanEventValidationError, match="base_version"):
        validate_chan_event(event)


def test_envelopes_require_typed_id_and_exact_source_fields() -> None:
    event = _event()
    event["id"] = 7
    with pytest.raises(ChanEventValidationError, match="id is required"):
        validate_chan_event(event)

    source = _source_event(1, 1, "v1")
    assert validate_chan_source_event(source) is source
    source.pop("snapshot_version")
    with pytest.raises(ChanEventValidationError, match="missing"):
        validate_chan_source_event(source)


def test_delta_rejects_full_overlay_and_duplicate_stable_ids() -> None:
    event = _event(kind="delta")
    event["chan"] = {"strokes": []}
    with pytest.raises(ChanEventValidationError, match="unknown"):
        validate_chan_event(event)

    event = _event(kind="delta")
    event["upserts"]["strokes"].append({"id": "stroke-1"})
    with pytest.raises(ChanEventValidationError, match="duplicate"):
        validate_chan_event(event)


def test_channels_are_diffed_by_stable_id() -> None:
    upserts, deletes = diff_objects(
        {"channels": [{"id": "channel-a", "high": 10}]},
        {
            "channels": [
                {"id": "channel-a", "high": 11},
                {"id": "channel-b", "high": 12},
            ]
        },
    )
    assert upserts["channels"] == [
        {"id": "channel-a", "high": 11},
        {"id": "channel-b", "high": 12},
    ]
    assert deletes["channels"] == []

    upserts, deletes = diff_objects(
        {"channels": [{"id": "channel-a"}, {"id": "channel-b"}]},
        {"channels": [{"id": "channel-b"}]},
    )
    assert upserts["channels"] == []
    assert deletes["channels"] == ["channel-a"]


def test_subscription_mapping_and_range_are_exact() -> None:
    message = {
        "symbol": "000001.SZ",
        "timeframe": "30f",
        "levels": ["30f", "1d"],
        "modes": ["confirmed"],
        "from": 100,
        "to": 200,
    }
    params = chart_ws._ChartRequestParams.from_subscription(message)
    assert params.levels_tuple == ("30f", "1d")

    message["levels"] = ["5f", "30f", "1d"]
    with pytest.raises(ValueError, match="Levels for 30f"):
        chart_ws._ChartRequestParams.from_subscription(message)


def test_source_sequence_drops_duplicates_and_resyncs_gaps() -> None:
    params = chart_ws._ChartRequestParams.from_subscription(
        {"timeframe": "5f", "from": 100, "to": 200}
    )
    subscription = chart_ws._ChartSubscription(params, "chan")
    assert (
        chart_ws._accept_source_event(subscription, _source_event(4, 1, "v1")) == "next"
    )
    assert (
        chart_ws._accept_source_event(subscription, _source_event(4, 1, "v1"))
        == "ignore"
    )
    assert (
        chart_ws._accept_source_event(subscription, _source_event(3, 1, "v1"))
        == "ignore"
    )
    assert (
        chart_ws._accept_source_event(subscription, _source_event(6, 2, "v2")) == "gap"
    )


def _source_event(sequence: int, run_id: int, version: str) -> dict:
    return {
        "type": "chan_head_update",
        "schema_version": "chan-head.v1",
        "id": f"000001.SZ:5f:confirmed:{run_id}:{version}",
        "symbol": "000001.SZ",
        "level": "5f",
        "mode": "confirmed",
        "sequence": sequence,
        "snapshot_version": version,
        "run_id": run_id,
        "bar_until": "2026-07-11T01:00:00+00:00",
    }


def test_producer_events_drive_snapshot_delta_and_gap_resync(monkeypatch) -> None:
    class WebSocket:
        def __init__(self):
            self.events: list[dict] = []

        async def send_json(self, event):
            self.events.append(event)

    overlays = iter(
        [
            {
                "snapshot_version": "000001.SZ:module-c:5f:confirmed:1:v1",
                "strokes": [{"id": "a"}],
            },
            {
                "snapshot_version": "000001.SZ:module-c:5f:confirmed:2:v2",
                "strokes": [{"id": "b"}],
            },
            {
                "snapshot_version": "000001.SZ:module-c:5f:confirmed:3:v3",
                "strokes": [{"id": "c"}],
            },
        ]
    )

    async def fake_build_chan_overlay(**_kwargs):
        payload = next(overlays)
        return _Model({**payload, "segments": [], "centers": [], "signals": []})

    monkeypatch.setattr(chart_ws, "build_chan_overlay", fake_build_chan_overlay)

    async def scenario() -> None:
        params = chart_ws._ChartRequestParams.from_subscription(
            {"timeframe": "5f", "modes": ["confirmed"], "from": 100, "to": 200}
        )
        subscription = chart_ws._ChartSubscription(params, "chan")
        websocket = WebSocket()
        subscriptions = {"sub": subscription}
        for event in (
            _source_event(10, 1, "v1"),
            _source_event(11, 2, "v2"),
            _source_event(13, 3, "v3"),
        ):
            await chart_ws._send_chan_subscription_updates(
                websocket, object(), subscriptions, object(), event
            )

        assert [event["type"] for event in websocket.events] == [
            "chan_overlay",
            "chan_overlay",
            "chan_resync_required",
            "chan_overlay",
        ]
        assert [websocket.events[index]["kind"] for index in (0, 1, 3)] == [
            "snapshot",
            "delta",
            "snapshot",
        ]
        assert websocket.events[1]["base_version"].endswith("1:v1")
        assert websocket.events[2]["reason"] == "source_sequence_gap"
        assert websocket.events[3]["range"] == {"from": 100, "to": 200}

    asyncio.run(scenario())


def test_source_version_mismatch_preserves_last_valid_until_polling_resync(
    monkeypatch,
) -> None:
    class WebSocket:
        def __init__(self):
            self.events: list[dict] = []

        async def send_json(self, event):
            self.events.append(event)

    overlays = iter(
        [
            {
                "snapshot_version": "000001.SZ:module-c:5f:confirmed:1:v1",
                "strokes": [{"id": "old"}],
            },
            {
                "snapshot_version": "000001.SZ:module-c:5f:confirmed:2:v2",
                "strokes": [{"id": "new"}],
            },
        ]
    )

    async def fake_build_chan_overlay(**_kwargs):
        return _Model(
            {
                **next(overlays),
                "segments": [],
                "centers": [],
                "signals": [],
            }
        )

    monkeypatch.setattr(chart_ws, "build_chan_overlay", fake_build_chan_overlay)

    async def scenario() -> None:
        params = chart_ws._ChartRequestParams.from_subscription(
            {"timeframe": "5f", "modes": ["confirmed"], "from": 100, "to": 200}
        )
        subscription = chart_ws._ChartSubscription(params, "chan")
        subscription.snapshot_version = "last-valid"
        subscription.objects["strokes"] = [{"id": "valid"}]
        websocket = WebSocket()
        subscriptions = {"sub": subscription}

        await chart_ws._send_chan_subscription_updates(
            websocket, object(), subscriptions, object(), _source_event(20, 2, "v2")
        )
        assert subscription.snapshot_version == "last-valid"
        assert websocket.events == [
            {
                "type": "chan_resync_required",
                "schema_version": "chan-event.v1",
                "id": "sub",
                "symbol": "000001.SZ",
                "chart_timeframe": "5f",
                "modes": ["confirmed"],
                "sequence": 1,
                "range": {"from": 100, "to": 200},
                "reason": "source_version_mismatch",
                "source_event_id": "000001.SZ:5f:confirmed:2:v2",
                "source_sequence": 20,
            }
        ]

        await chart_ws._send_chan_subscription_updates(
            websocket, object(), subscriptions, object(), None
        )
        assert websocket.events[-1]["kind"] == "snapshot"
        assert websocket.events[-1]["sequence"] == 2
        assert subscription.snapshot_version.endswith("2:v2")

    asyncio.run(scenario())


class _Model:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def model_dump(self, **_kwargs) -> dict:
        return self.payload


def test_delta_is_emitted_only_against_matching_server_base() -> None:
    class WebSocket:
        def __init__(self):
            self.events: list[dict] = []

        async def send_json(self, event):
            self.events.append(event)

    async def scenario() -> None:
        params = chart_ws._ChartRequestParams.from_subscription(
            {"timeframe": "5f", "from": 100, "to": 200}
        )
        subscription = chart_ws._ChartSubscription(params, "chan")
        websocket = WebSocket()
        first = {
            "snapshot_version": "v1",
            "strokes": [{"id": "a"}],
            "segments": [],
            "centers": [],
            "signals": [],
        }
        second = {
            "snapshot_version": "v2",
            "strokes": [{"id": "b"}],
            "segments": [],
            "centers": [],
            "signals": [],
        }
        await chart_ws._send_overlay_event(
            websocket, "sub", subscription, first, force_snapshot=False
        )
        await chart_ws._send_overlay_event(
            websocket, "sub", subscription, second, force_snapshot=False
        )
        assert websocket.events[0]["kind"] == "snapshot"
        assert websocket.events[1]["kind"] == "delta"
        assert websocket.events[1]["base_version"] == "v1"
        assert websocket.events[1]["deletes"]["strokes"] == ["a"]

    asyncio.run(scenario())


def test_redis_failure_keeps_subscription_state_for_polling() -> None:
    class WebSocket:
        def __init__(self):
            self.events: list[dict] = []

        async def send_json(self, event):
            self.events.append(event)

    async def scenario() -> None:
        params = chart_ws._ChartRequestParams.from_subscription(
            {"timeframe": "5f", "from": 100, "to": 200}
        )
        subscription = chart_ws._ChartSubscription(params, "chan")
        subscription.snapshot_version = "last-valid"
        subscription.sequence = 7
        subscription.objects = {
            "strokes": [{"id": "a"}],
            "segments": [],
            "centers": [],
            "signals": [],
        }
        websocket = WebSocket()
        await chart_ws._send_overlay_event(
            websocket,
            "sub",
            subscription,
            {
                "snapshot_version": "next",
                "strokes": [{"id": "b"}],
                "segments": [],
                "centers": [],
                "signals": [],
            },
            force_snapshot=False,
        )
        assert websocket.events[0]["kind"] == "delta"
        assert websocket.events[0]["base_version"] == "last-valid"

    asyncio.run(scenario())


def test_bundle_subscribe_is_disabled_by_default() -> None:
    assert not chart_ws._legacy_chart_bundle_subscriptions_enabled()
