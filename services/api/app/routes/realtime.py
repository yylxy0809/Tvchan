from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.config import get_settings
from app.core.security import authenticate_token_value
from app.repositories.bars import generate_seed_bars, resolve_symbol
from app.repositories.postgres import get_bars_db, resolve_symbol_db
from trading_protocol import normalize_timeframe

router = APIRouter(tags=["realtime"])
UPDATE_INTERVAL_SECONDS = 3.0
REDIS_BAR_UPDATE_CHANNEL = "market:bar_updates"


@router.websocket("/ws/v1/realtime")
async def realtime_ws(websocket: WebSocket) -> None:
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
    subscriptions: dict[str, dict[str, Any]] = {}
    producer_task = await _create_producer_task(websocket, subscriptions)
    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            if msg_type == "ping":
                await websocket.send_json({"type": "pong", "ts": _now_ts()})
            elif msg_type == "subscribe":
                sub_id = str(message.get("id") or f"sub_{len(subscriptions) + 1}")
                symbol = str(message.get("symbol") or "000001.SZ").upper()
                timeframes = message.get("timeframes") or ["5f"]
                subscriptions[sub_id] = {
                    "id": sub_id,
                    "symbol": symbol,
                    "timeframes": [normalize_timeframe(item) for item in timeframes],
                }
                await websocket.send_json({"type": "subscribed", "id": sub_id})
            elif msg_type == "unsubscribe":
                sub_id = str(message.get("id") or "")
                subscriptions.pop(sub_id, None)
                await websocket.send_json({"type": "unsubscribed", "id": sub_id})
            else:
                await websocket.send_json(
                    {"type": "error", "message": f"Unsupported message type: {msg_type}"}
                )
    except WebSocketDisconnect:
        return
    finally:
        producer_task.cancel()


async def _create_producer_task(websocket: WebSocket, subscriptions):
    settings = get_settings()
    redis_client = await _try_create_redis_client(settings.redis_url)
    if redis_client is not None:
        return asyncio.create_task(_send_redis_updates(websocket, subscriptions, redis_client))
    return asyncio.create_task(_send_updates(websocket, subscriptions))


async def _try_create_redis_client(redis_url: str):
    try:
        import redis.asyncio as redis
    except ImportError:
        return None
    client = None
    try:
        client = redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=0.2,
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


async def _send_redis_updates(websocket: WebSocket, subscriptions, redis_client) -> None:
    seq = 0
    pubsub = redis_client.pubsub()
    try:
        await pubsub.subscribe(REDIS_BAR_UPDATE_CHANNEL)
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            payload = _parse_redis_message(message.get("data"))
            if payload is None or not _matches_subscriptions(payload, subscriptions):
                continue
            seq += 1
            await websocket.send_json(
                _bar_update_event(
                    seq=seq,
                    symbol=payload["symbol"],
                    timeframe=payload["timeframe"],
                    bar=payload["bar"],
                    source="redis",
                )
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        await _send_updates(websocket, subscriptions)
    finally:
        try:
            await pubsub.unsubscribe(REDIS_BAR_UPDATE_CHANNEL)
            await pubsub.aclose()
            await redis_client.aclose()
        except Exception:
            pass


def _parse_redis_message(data: Any) -> dict | None:
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    if not isinstance(data, str):
        return None
    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if not {"symbol", "timeframe", "bar"}.issubset(payload):
        return None
    try:
        payload["timeframe"] = normalize_timeframe(str(payload["timeframe"]))
    except ValueError:
        return None
    payload["symbol"] = str(payload["symbol"]).upper()
    return payload


def _matches_subscriptions(payload: dict, subscriptions) -> bool:
    for subscription in list(subscriptions.values()):
        if subscription["symbol"] != payload["symbol"]:
            continue
        if payload["timeframe"] in subscription["timeframes"]:
            return True
    return False


async def _send_updates(websocket: WebSocket, subscriptions) -> None:
    settings = get_settings()
    seq = 0
    while True:
        await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
        for subscription in list(subscriptions.values()):
            for timeframe in subscription["timeframes"]:
                bar = await _latest_bar(
                    websocket,
                    symbol=subscription["symbol"],
                    timeframe=timeframe,
                    use_seed_data=settings.use_seed_data,
                )
                if bar is None:
                    continue
                seq += 1
                await websocket.send_json(
                    _bar_update_event(
                        seq=seq,
                        symbol=subscription["symbol"],
                        timeframe=timeframe,
                        bar=bar,
                    )
                )


async def _latest_bar(
    websocket: WebSocket, symbol: str, timeframe: str, use_seed_data: bool
) -> dict | None:
    if use_seed_data:
        if resolve_symbol(symbol) is None:
            return None
        bars = generate_seed_bars(symbol, timeframe, limit=1)
        return bars[-1].as_api_dict() if bars else None

    app = websocket.scope.get("app")
    pool = getattr(getattr(app, "state", None), "db_pool", None)
    if pool is None:
        return None
    symbol_row = await resolve_symbol_db(pool, symbol)
    if symbol_row is None:
        return None
    bars = await get_bars_db(pool, symbol_row["symbol"], timeframe, None, None, 1)
    return bars[-1] if bars else None


def _now_ts() -> int:
    return int(datetime.now().timestamp())


def _bar_update_event(
    *,
    seq: int,
    symbol: str,
    timeframe: str,
    bar: dict[str, Any],
    source: str | None = None,
) -> dict[str, Any]:
    payload = {
        "type": "bar_update",
        "seq": seq,
        "symbol": symbol,
        "timeframe": timeframe,
        "snapshot_version": _bar_snapshot_version(symbol, timeframe, bar),
        "bar": dict(bar),
    }
    if source:
        payload["source"] = source
    return payload


def _bar_snapshot_version(symbol: str, timeframe: str, bar: dict[str, Any]) -> str:
    normalized_timeframe = normalize_timeframe(timeframe)
    bar_time = int(bar.get("time") or 0)
    revision = int(bar.get("revision") or 0)
    complete = 1 if bool(bar.get("complete")) else 0
    return (
        f"rt:{symbol.upper()}:{normalized_timeframe}:"
        f"{bar_time:012d}:{revision:06d}:{complete}"
    )
