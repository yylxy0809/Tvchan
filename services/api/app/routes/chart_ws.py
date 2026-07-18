from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.config import get_settings
from app.core.security import (
    AuthenticationServiceUnavailable,
    authenticate_token_value,
)
from app.routes.chan import DEFAULT_MODES, _display_levels_for_chart, build_chan_overlay
from app.routes.chart import (
    build_bars_response,
    build_chart_bundle_v3,
    build_chart_window,
)
from app.services.chan_delta_protocol import (
    CHAN_EVENT_SCHEMA_VERSION,
    OBJECT_GROUPS,
    ChanEventValidationError,
    diff_objects,
    overlay_objects,
    validate_chan_event,
    validate_chan_source_event,
)
from trading_protocol import normalize_timeframe

router = APIRouter(tags=["chart-ws"])
CHAN_SNAPSHOT_INTERVAL_SECONDS = 3.0
REDIS_CHAN_HEAD_UPDATE_CHANNEL = "chan:head_updates"


@router.websocket("/ws/v2/chart")
async def chart_ws(websocket: WebSocket) -> None:
    settings = get_settings()
    token = websocket.query_params.get("token")
    if settings.api_token or settings.admin_api_token:
        app = websocket.scope.get("app")
        try:
            principal = await authenticate_token_value(
                token, getattr(getattr(app, "state", None), "db_pool", None), settings
            )
        except AuthenticationServiceUnavailable:
            # A pre-accept close is converted to an HTTP 403 by ASGI servers.
            await websocket.accept()
            await websocket.close(code=1013)
            return
    else:
        principal = True
    if principal is None:
        # Complete the handshake so the client receives the policy close code.
        await websocket.accept()
        await websocket.close(code=1008)
        return

    await websocket.accept()
    request_adapter = _RequestAdapter(websocket.scope.get("app"))
    subscriptions: dict[str, _ChartSubscription] = {}
    producer_task = asyncio.create_task(
        _publish_chan_updates(websocket, request_adapter, subscriptions, settings)
    )
    try:
        while True:
            message = await websocket.receive_json()
            request_id = str(message.get("request_id") or "")
            try:
                await _handle_chart_message(
                    websocket, request_adapter, message, request_id, subscriptions
                )
            except Exception as exc:
                await websocket.send_json(
                    {
                        "type": "error",
                        "request_id": request_id,
                        "error": _error_message(exc),
                    }
                )
    except WebSocketDisconnect:
        return
    finally:
        producer_task.cancel()


async def _handle_chart_message(
    websocket: WebSocket,
    request_adapter: "_RequestAdapter",
    message: dict[str, Any],
    request_id: str,
    subscriptions: dict[str, "_ChartSubscription"],
) -> None:
    msg_type = str(message.get("type") or "")
    settings = get_settings()
    if msg_type == "ping":
        await websocket.send_json(
            {
                "type": "pong",
                "request_id": request_id,
                "ts": int(datetime.now().timestamp()),
            }
        )
        return
    params = _ChartRequestParams.from_message(message)
    if msg_type == "get_bars":
        bars = await build_bars_response(
            request=request_adapter,
            symbol=params.symbol,
            timeframe=params.timeframe,
            from_ts=params.from_ts,
            to_ts=params.to_ts,
            limit=params.limit,
            settings=settings,
        )
        await websocket.send_json(
            {
                "type": "bars",
                "request_id": request_id,
                "symbol": bars.symbol,
                "timeframe": bars.timeframe,
                "bars": [_dump_model(bar) for bar in bars.bars],
            }
        )
        return
    if msg_type == "get_chan":
        chan = await build_chan_overlay(
            request=request_adapter,
            symbol=params.symbol,
            timeframe=params.timeframe,
            levels=params.levels,
            modes=params.modes,
            from_ts=params.from_ts,
            to_ts=params.to_ts,
            limit=params.limit,
            settings=settings,
        )
        await websocket.send_json(
            {"type": "chan_full", "request_id": request_id, "chan": _dump_model(chan)}
        )
        return
    if msg_type == "get_chart_window":
        window = await build_chart_window(
            request=request_adapter,
            symbol=params.symbol,
            timeframe=params.timeframe,
            levels=params.levels,
            modes=params.modes,
            from_ts=params.from_ts,
            to_ts=params.to_ts,
            limit=params.limit,
            settings=settings,
        )
        await websocket.send_json(
            {
                "type": "chart_window",
                "request_id": request_id,
                "window": _dump_model(window),
            }
        )
        return
    if msg_type == "get_chart_bundle":
        bundle = await build_chart_bundle_v3(
            request=request_adapter,
            symbol=params.symbol,
            timeframe=params.timeframe,
            levels=params.levels,
            modes=params.modes,
            from_ts=params.from_ts,
            to_ts=params.to_ts,
            limit=params.limit,
            settings=settings,
        )
        await websocket.send_json(
            {
                "type": "chart_bundle",
                "request_id": request_id,
                "bundle": _dump_model(bundle),
            }
        )
        return
    if msg_type == "subscribe_chart_bundle":
        if not _legacy_chart_bundle_subscriptions_enabled():
            raise ValueError("subscribe_chart_bundle is disabled; use subscribe_chan")
        await _subscribe_legacy_bundle(websocket, message, request_id, subscriptions)
        return
    if msg_type == "subscribe_chan":
        subscription_id = str(message.get("id") or request_id or "")
        if not subscription_id:
            raise ValueError("subscribe_chan requires id or request_id")
        params = _ChartRequestParams.from_subscription(message)
        subscriptions[subscription_id] = _ChartSubscription(params=params, kind="chan")
        await websocket.send_json(
            {
                "type": "chan_subscribed",
                "id": subscription_id,
                "symbol": params.symbol,
                "chart_timeframe": params.timeframe,
                "levels": list(params.levels_tuple),
                "modes": list(params.modes_tuple),
                "schema_version": CHAN_EVENT_SCHEMA_VERSION,
            }
        )
        return
    if msg_type in {"unsubscribe_chan", "unsubscribe_chart_bundle"}:
        subscription_id = str(message.get("id") or request_id or "")
        if not subscription_id:
            raise ValueError(f"{msg_type} requires id or request_id")
        subscriptions.pop(subscription_id, None)
        await websocket.send_json(
            {
                "type": (
                    "chan_unsubscribed"
                    if msg_type == "unsubscribe_chan"
                    else "chart_bundle_unsubscribed"
                ),
                "id": subscription_id,
            }
        )
        return
    raise ValueError(f"Unsupported message type: {msg_type}")


async def _subscribe_legacy_bundle(
    websocket: WebSocket,
    message: dict[str, Any],
    request_id: str,
    subscriptions: dict[str, "_ChartSubscription"],
) -> None:
    subscription_id = str(message.get("id") or request_id or "")
    if not subscription_id:
        raise ValueError("subscribe_chart_bundle requires id or request_id")
    subscriptions[subscription_id] = _ChartSubscription(
        params=_ChartRequestParams.from_message(message), kind="legacy_bundle"
    )
    await websocket.send_json(
        {"type": "chart_bundle_subscribed", "id": subscription_id}
    )


async def _publish_chan_updates(
    websocket: WebSocket,
    request_adapter: "_RequestAdapter",
    subscriptions: dict[str, "_ChartSubscription"],
    settings: Any,
) -> None:
    redis_client = await _try_create_redis_client(settings.redis_url)
    if redis_client is None:
        await _publish_chan_updates_polling(
            websocket, request_adapter, subscriptions, settings
        )
        return
    pubsub = redis_client.pubsub()
    try:
        await pubsub.subscribe(REDIS_CHAN_HEAD_UPDATE_CHANNEL)
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=CHAN_SNAPSHOT_INTERVAL_SECONDS
            )
            if subscriptions:
                await _send_chan_subscription_updates(
                    websocket,
                    request_adapter,
                    subscriptions,
                    settings,
                    _parse_chan_head_event(message.get("data") if message else None),
                )
    except asyncio.CancelledError:
        raise
    except Exception:
        # Keep the last valid per-subscription state while polling recovers Redis loss.
        await _publish_chan_updates_polling(
            websocket, request_adapter, subscriptions, settings
        )
    finally:
        try:
            await pubsub.unsubscribe(REDIS_CHAN_HEAD_UPDATE_CHANNEL)
            await pubsub.aclose()
            await redis_client.aclose()
        except Exception:
            pass


async def _publish_chan_updates_polling(
    websocket: WebSocket,
    request_adapter: "_RequestAdapter",
    subscriptions: dict[str, "_ChartSubscription"],
    settings: Any,
) -> None:
    while True:
        await asyncio.sleep(CHAN_SNAPSHOT_INTERVAL_SECONDS)
        await _send_chan_subscription_updates(
            websocket, request_adapter, subscriptions, settings, None
        )


async def _send_chan_subscription_updates(
    websocket: WebSocket,
    request_adapter: "_RequestAdapter",
    subscriptions: dict[str, "_ChartSubscription"],
    settings: Any,
    event: dict[str, Any] | None,
) -> None:
    for subscription_id, subscription in list(subscriptions.items()):
        if event is not None and not _event_matches_subscription(event, subscription):
            continue
        try:
            if subscription.kind == "legacy_bundle":
                await _send_legacy_bundle(
                    websocket, request_adapter, subscription_id, subscription, settings
                )
                continue
            source_decision = _accept_source_event(subscription, event)
            if source_decision == "ignore":
                continue
            force_snapshot = source_decision == "gap"
            if force_snapshot and event is not None:
                subscription.pending_source_heads[_source_stream(event)] = event
                await _send_resync_required(
                    websocket,
                    subscription_id,
                    subscription,
                    reason="source_sequence_gap",
                    source_event=event,
                )
            overlay = await build_chan_overlay(
                request=request_adapter,
                symbol=subscription.params.symbol,
                timeframe=subscription.params.timeframe,
                levels=subscription.params.levels,
                modes=subscription.params.modes,
                from_ts=subscription.params.from_ts,
                to_ts=subscription.params.to_ts,
                limit=subscription.params.limit,
                settings=settings,
                authoritative_window=True,
            )
            payload = _dump_model(overlay)
            if event is not None:
                stream = _source_stream(event)
                if not _overlay_contains_source_head(payload, event):
                    subscription.pending_source_heads[stream] = event
                    if source_decision != "gap":
                        await _send_resync_required(
                            websocket,
                            subscription_id,
                            subscription,
                            reason="source_version_mismatch",
                            source_event=event,
                        )
                    continue
                subscription.pending_source_heads.pop(stream, None)
            if subscription.pending_source_heads:
                if not all(
                    _overlay_contains_source_head(payload, pending)
                    for pending in subscription.pending_source_heads.values()
                ):
                    continue
                subscription.pending_source_heads.clear()
                force_snapshot = True
            await _send_overlay_event(
                websocket,
                subscription_id,
                subscription,
                payload,
                force_snapshot=bool(force_snapshot),
            )
        except Exception as exc:
            await websocket.send_json(
                {"type": "error", "id": subscription_id, "error": _error_message(exc)}
            )


def _accept_source_event(
    subscription: "_ChartSubscription", event: dict[str, Any] | None
) -> str:
    if event is None:
        return "poll"
    validate_chan_source_event(event)
    stream = _source_stream(event)
    previous = subscription.source_heads.get(stream)
    source_sequence = int(event["sequence"])
    if previous is not None and source_sequence <= int(previous["sequence"]):
        return "ignore"
    subscription.source_heads[stream] = event
    if previous is not None and event["id"] == previous["id"]:
        return "ignore"
    if previous is not None and source_sequence != int(previous["sequence"]) + 1:
        return "gap"
    return "next"


def _source_stream(event: dict[str, Any]) -> tuple[str, str]:
    return str(event["level"]), str(event["mode"])


def _overlay_contains_source_head(
    overlay: dict[str, Any], source_event: dict[str, Any]
) -> bool:
    token = (
        f"{source_event['level']}:{source_event['mode']}:"
        f"{source_event['run_id']}:{source_event['snapshot_version']}"
    )
    return token in str(overlay.get("snapshot_version") or "")


async def _send_resync_required(
    websocket: WebSocket,
    subscription_id: str,
    subscription: "_ChartSubscription",
    *,
    reason: str,
    source_event: dict[str, Any],
) -> None:
    subscription.sequence += 1
    await websocket.send_json(
        {
            "type": "chan_resync_required",
            "schema_version": CHAN_EVENT_SCHEMA_VERSION,
            "id": subscription_id,
            "symbol": subscription.params.symbol,
            "chart_timeframe": subscription.params.timeframe,
            "modes": list(subscription.params.modes_tuple),
            "sequence": subscription.sequence,
            "range": subscription.params.range,
            "reason": reason,
            "source_event_id": source_event["id"],
            "source_sequence": source_event["sequence"],
        }
    )


async def _send_overlay_event(
    websocket: WebSocket,
    subscription_id: str,
    subscription: "_ChartSubscription",
    overlay: dict[str, Any],
    *,
    force_snapshot: bool,
) -> None:
    version = str(overlay.get("snapshot_version") or "")
    objects = overlay_objects(overlay)
    if not version:
        raise ChanEventValidationError("Chan overlay snapshot_version is required")
    if subscription.snapshot_version == version and not force_snapshot:
        return
    subscription.sequence += 1
    if subscription.snapshot_version is None or force_snapshot:
        event = _event(
            subscription_id,
            subscription,
            "snapshot",
            version,
            None,
            objects,
            _empty_deletes(),
        )
    else:
        upserts, deletes = diff_objects(subscription.objects, objects)
        if not any(upserts.values()) and not any(deletes.values()):
            # A changed version without stable-object changes is a bounded resync, never a fake delta.
            event = _event(
                subscription_id,
                subscription,
                "snapshot",
                version,
                None,
                objects,
                _empty_deletes(),
            )
        else:
            event = _event(
                subscription_id,
                subscription,
                "delta",
                version,
                subscription.snapshot_version,
                upserts,
                deletes,
            )
    validate_chan_event(event)
    await websocket.send_json(event)
    subscription.snapshot_version = version
    subscription.objects = objects


def _event(
    subscription_id: str,
    subscription: "_ChartSubscription",
    kind: str,
    snapshot_version: str,
    base_version: str | None,
    upserts: dict[str, list[dict[str, Any]]],
    deletes: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "type": "chan_overlay",
        "schema_version": CHAN_EVENT_SCHEMA_VERSION,
        "kind": kind,
        "id": subscription_id,
        "symbol": subscription.params.symbol,
        "chart_timeframe": subscription.params.timeframe,
        "modes": list(subscription.params.modes_tuple),
        "snapshot_version": snapshot_version,
        "base_version": base_version,
        "sequence": subscription.sequence,
        "range": subscription.params.range,
        "upserts": upserts,
        "deletes": deletes,
    }


async def _send_legacy_bundle(
    websocket: WebSocket,
    request_adapter: "_RequestAdapter",
    subscription_id: str,
    subscription: "_ChartSubscription",
    settings: Any,
) -> None:
    params = subscription.params
    bundle = await build_chart_bundle_v3(
        request=request_adapter,
        symbol=params.symbol,
        timeframe=params.timeframe,
        levels=params.levels,
        modes=params.modes,
        from_ts=params.from_ts,
        to_ts=params.to_ts,
        limit=params.limit,
        settings=settings,
    )
    version = bundle.snapshot_version or bundle.snapshot_id
    if subscription.snapshot_version == version:
        return
    subscription.snapshot_version = version
    await websocket.send_json(
        {
            "type": "chart_bundle_snapshot",
            "id": subscription_id,
            "symbol": bundle.symbol,
            "timeframe": bundle.chart_timeframe,
            "snapshot_version": version,
            "bundle": _dump_model(bundle),
        }
    )


async def _try_create_redis_client(redis_url: str):
    try:
        import redis.asyncio as redis
    except ImportError:
        return None
    client = None
    try:
        client = redis.from_url(
            redis_url, decode_responses=True, socket_connect_timeout=0.2
        )
        await client.ping()
        return client
    except Exception:
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass
        return None


def _parse_chan_head_event(data: Any) -> dict[str, Any] | None:
    if data is None:
        return None
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    if not isinstance(data, str):
        raise ChanEventValidationError("Chan source event must be JSON text")
    try:
        payload = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ChanEventValidationError("Chan source event must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ChanEventValidationError("Chan source event must be an object")
    if isinstance(payload.get("symbol"), str):
        payload["symbol"] = payload["symbol"].upper()
    return validate_chan_source_event(payload)


def _event_matches_subscription(
    event: dict[str, Any], subscription: "_ChartSubscription"
) -> bool:
    if str(event.get("symbol") or "").upper() != subscription.params.symbol:
        return False
    level = str(event.get("level") or "")
    mode = str(event.get("mode") or "")
    return (
        level in subscription.params.levels_tuple
        and mode in subscription.params.modes_tuple
    )


def _legacy_chart_bundle_subscriptions_enabled() -> bool:
    return os.getenv("ENABLE_LEGACY_CHART_BUNDLE_SUBSCRIBE", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


class _RequestAdapter:
    def __init__(self, app: Any) -> None:
        self.app = app


class _ChartSubscription:
    def __init__(self, params: "_ChartRequestParams", kind: str) -> None:
        self.params = params
        self.kind = kind
        self.snapshot_version: str | None = None
        self.sequence = 0
        self.source_heads: dict[tuple[str, str], dict[str, Any]] = {}
        self.pending_source_heads: dict[tuple[str, str], dict[str, Any]] = {}
        self.objects = {group: [] for group in OBJECT_GROUPS}


class _ChartRequestParams:
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        levels: str,
        modes: str,
        from_ts: datetime | None,
        to_ts: datetime | None,
        limit: int,
    ) -> None:
        self.symbol, self.timeframe, self.levels, self.modes = (
            symbol,
            timeframe,
            levels,
            modes,
        )
        self.from_ts, self.to_ts, self.limit = from_ts, to_ts, limit
        self.levels_tuple = tuple(item for item in levels.split(",") if item)
        self.modes_tuple = tuple(item for item in modes.split(",") if item)

    @property
    def range(self) -> dict[str, int]:
        if self.from_ts is None or self.to_ts is None:
            raise ValueError("subscribe_chan requires an inclusive from/to range")
        return {
            "from": int(self.from_ts.timestamp()),
            "to": int(self.to_ts.timestamp()),
        }

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> "_ChartRequestParams":
        return cls(
            symbol=str(message.get("symbol") or "000001.SZ").upper(),
            timeframe=normalize_timeframe(str(message.get("timeframe") or "5f")),
            levels=_list_or_csv(message.get("levels"), "5f,30f,1d,1w,1m"),
            modes=_list_or_csv(message.get("modes"), "confirmed,predictive"),
            from_ts=_epoch_to_datetime(message.get("from")),
            to_ts=_epoch_to_datetime(message.get("to")),
            limit=_read_limit(message.get("limit")),
        )

    @classmethod
    def from_subscription(cls, message: dict[str, Any]) -> "_ChartRequestParams":
        params = cls.from_message(message)
        expected_levels = tuple(_display_levels_for_chart(params.timeframe))
        if message.get("levels") is None or message.get("levels") == "":
            params.levels = ",".join(expected_levels)
            params.levels_tuple = expected_levels
        if params.levels_tuple != expected_levels:
            raise ValueError(
                f"Levels for {params.timeframe} must be: {', '.join(expected_levels)}"
            )
        if (
            not params.modes_tuple
            or any(mode not in DEFAULT_MODES for mode in params.modes_tuple)
            or len(set(params.modes_tuple)) != len(params.modes_tuple)
        ):
            raise ValueError("Unsupported or duplicate Chan modes")
        _ = params.range
        return params


def _list_or_csv(value: Any, default: str) -> str:
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return ",".join(items) if items else default
    return value.strip() if isinstance(value, str) and value.strip() else default


def _epoch_to_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    return datetime.fromtimestamp(float(value), tz=UTC)


def _read_limit(value: Any) -> int:
    limit = 300 if value is None or value == "" else int(value)
    if limit < 1 or limit > 5000:
        raise ValueError("limit must be between 1 and 5000")
    return limit


def _empty_deletes() -> dict[str, list[str]]:
    return {group: [] for group in OBJECT_GROUPS}


def _dump_model(model: Any) -> dict[str, Any]:
    return (
        model.model_dump(mode="json", by_alias=True)
        if hasattr(model, "model_dump")
        else json.loads(model.json(by_alias=True))
    )


def _error_message(exc: Exception) -> str:
    return str(exc.detail) if isinstance(exc, StarletteHTTPException) else str(exc)
