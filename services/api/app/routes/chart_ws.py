from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.config import get_settings
from app.core.security import authenticate_token_value
from app.routes.chan import build_chan_overlay
from app.routes.chart import build_bars_response, build_chart_bundle_v3, build_chart_window

router = APIRouter(tags=["chart-ws"])
CHAN_SNAPSHOT_INTERVAL_SECONDS = 3.0


@router.websocket("/ws/v2/chart")
async def chart_ws(websocket: WebSocket) -> None:
    settings = get_settings()
    token = websocket.query_params.get("token")
    if settings.api_token or settings.admin_api_token:
        app = websocket.scope.get("app")
        principal = await authenticate_token_value(
            token,
            getattr(getattr(app, "state", None), "db_pool", None),
            settings,
        )
    else:
        principal = True
    if principal is None:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    request_adapter = _RequestAdapter(websocket.scope.get("app"))
    subscriptions: dict[str, _ChartSubscription] = {}
    producer_task = asyncio.create_task(
        _publish_chan_snapshots(
            websocket=websocket,
            request_adapter=request_adapter,
            subscriptions=subscriptions,
            settings=settings,
        )
    )
    try:
        while True:
            message = await websocket.receive_json()
            request_id = str(message.get("request_id") or "")
            try:
                await _handle_chart_message(
                    websocket=websocket,
                    request_adapter=request_adapter,
                    message=message,
                    request_id=request_id,
                    subscriptions=subscriptions,
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
            {"type": "pong", "request_id": request_id, "ts": int(datetime.now().timestamp())}
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
            {
                "type": "chan_full",
                "request_id": request_id,
                "chan": _dump_model(chan),
            }
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

    if msg_type in {"subscribe_chan", "subscribe_chart_bundle"}:
        subscription_id = str(message.get("id") or request_id or "")
        if not subscription_id:
            raise ValueError(f"{msg_type} requires id or request_id")
        subscriptions[subscription_id] = _ChartSubscription(params=params, kind=msg_type)
        response_type = (
            "chart_bundle_subscribed"
            if msg_type == "subscribe_chart_bundle"
            else "chan_subscribed"
        )
        await websocket.send_json(
            {
                "type": response_type,
                "id": subscription_id,
                "symbol": params.symbol,
                "timeframe": params.timeframe,
            }
        )
        return

    if msg_type in {"unsubscribe_chan", "unsubscribe_chart_bundle"}:
        subscription_id = str(message.get("id") or request_id or "")
        if not subscription_id:
            raise ValueError(f"{msg_type} requires id or request_id")
        subscriptions.pop(subscription_id, None)
        response_type = (
            "chart_bundle_unsubscribed"
            if msg_type == "unsubscribe_chart_bundle"
            else "chan_unsubscribed"
        )
        await websocket.send_json({"type": response_type, "id": subscription_id})
        return

    raise ValueError(f"Unsupported message type: {msg_type}")


async def _publish_chan_snapshots(
    websocket: WebSocket,
    request_adapter: "_RequestAdapter",
    subscriptions: dict[str, "_ChartSubscription"],
    settings,
) -> None:
    last_versions: dict[str, str] = {}
    try:
        while True:
            await asyncio.sleep(CHAN_SNAPSHOT_INTERVAL_SECONDS)
            if not subscriptions:
                continue
            for subscription_id, subscription in list(subscriptions.items()):
                params = subscription.params
                try:
                    if subscription.kind == "subscribe_chart_bundle":
                        bundle = await build_chart_bundle_v3(
                            request=request_adapter,
                            symbol=params.symbol,
                            timeframe=params.timeframe,
                            from_ts=params.from_ts,
                            to_ts=params.to_ts,
                            limit=params.limit,
                            settings=settings,
                        )
                        previous = last_versions.get(subscription_id)
                        current = bundle.snapshot_version or bundle.snapshot_id
                        event_type = "chart_bundle_snapshot" if previous is None else "chart_bundle_delta"
                        if previous == current:
                            continue
                        last_versions[subscription_id] = current
                        await websocket.send_json(
                            {
                                "type": event_type,
                                "id": subscription_id,
                                "symbol": bundle.symbol,
                                "timeframe": bundle.chart_timeframe,
                                "snapshot_version": current,
                                "bundle": _dump_model(bundle),
                            }
                        )
                        continue

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
                    previous = last_versions.get(subscription_id)
                    current = chan.snapshot_version
                    event_type = "chan_snapshot" if previous is None else "chan_delta"
                    if previous == current:
                        continue
                    last_versions[subscription_id] = current
                    await websocket.send_json(
                        {
                            "type": event_type,
                            "id": subscription_id,
                            "symbol": chan.symbol,
                            "timeframe": params.timeframe,
                            "snapshot_version": current,
                            "chan": _dump_model(chan),
                        }
                    )
                except Exception as exc:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "id": subscription_id,
                            "error": _error_message(exc),
                        }
                    )
    except asyncio.CancelledError:
        raise


class _RequestAdapter:
    def __init__(self, app: Any) -> None:
        self.app = app


class _ChartSubscription:
    def __init__(self, params: "_ChartRequestParams", kind: str) -> None:
        self.params = params
        self.kind = kind


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
        self.symbol = symbol
        self.timeframe = timeframe
        self.levels = levels
        self.modes = modes
        self.from_ts = from_ts
        self.to_ts = to_ts
        self.limit = limit

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> "_ChartRequestParams":
        return cls(
            symbol=str(message.get("symbol") or "000001.SZ").upper(),
            timeframe=str(message.get("timeframe") or "5f"),
            levels=_list_or_csv(message.get("levels"), "5f,30f,1d"),
            modes=_list_or_csv(message.get("modes"), "confirmed,predictive"),
            from_ts=_epoch_to_datetime(message.get("from")),
            to_ts=_epoch_to_datetime(message.get("to")),
            limit=_read_limit(message.get("limit")),
        )


def _list_or_csv(value: Any, default: str) -> str:
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return ",".join(items) if items else default
    if isinstance(value, str) and value.strip():
        return value
    return default


def _epoch_to_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    return datetime.fromtimestamp(float(value), tz=UTC)


def _read_limit(value: Any) -> int:
    if value is None or value == "":
        return 300
    limit = int(value)
    if limit < 1 or limit > 5000:
        raise ValueError("limit must be between 1 and 5000")
    return limit


def _dump_model(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json", by_alias=True)
    return json.loads(model.json(by_alias=True))


def _error_message(exc: Exception) -> str:
    if isinstance(exc, StarletteHTTPException):
        return str(exc.detail)
    return str(exc)
