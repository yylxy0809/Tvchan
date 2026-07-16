from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.config import get_settings
from app.core.security import authenticate_token_value
from app.market_sidebar.dto import SetSidebarContext
from app.market_sidebar.service import (
    SidebarAggregator,
    SidebarContext,
    sidebar_stream_id,
)
from app.repositories.bars import generate_seed_bars, resolve_symbol
from app.repositories.postgres import get_bars_db, resolve_symbol_db
from trading_protocol import normalize_timeframe

router = APIRouter(tags=["realtime"])
UPDATE_INTERVAL_SECONDS = 3.0
SIDEBAR_DEMAND_TTL_SECONDS = 30
REDIS_BAR_UPDATE_CHANNEL = "market:bar_updates"
SIDEBAR_UPDATE_CHANNEL = "market:sidebar:updates"
CHAN_UPDATE_CHANNEL = "chan:head_updates"
STRATEGY_UPDATE_CHANNEL = "strategy:signal_updates"
logger = logging.getLogger(__name__)


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
    connection_id = uuid4().hex
    subscriptions: dict[str, dict[str, Any]] = {}
    producer_task = await _create_producer_task(websocket, subscriptions)
    sidebar_task: asyncio.Task | None = None
    sidebar_listener_task: asyncio.Task | None = None
    sidebar_context: SidebarContext | None = None
    sidebar_demand_key: str | None = None
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
                if sidebar_context is not None and sidebar_context.subscription_id == sub_id:
                    await _cancel_task(sidebar_task)
                    await _cancel_task(sidebar_listener_task)
                    await websocket.scope["app"].state.market_sidebar_aggregator.unsubscribe(
                        connection_id, sub_id
                    )
                    await _safe_delete_demand(
                        websocket.scope["app"].state.market_sidebar_repository,
                        sidebar_demand_key,
                    )
                    sidebar_task = None
                    sidebar_listener_task = None
                    sidebar_context = None
                    sidebar_demand_key = None
                await websocket.send_json({"type": "unsubscribed", "id": sub_id})
            elif msg_type == "set_sidebar_context":
                try:
                    sidebar_message = SetSidebarContext.model_validate(message)
                except ValueError as exc:
                    await websocket.send_json(
                        {"type": "error", "message": f"Invalid sidebar context: {exc}"}
                    )
                    continue
                app = websocket.scope["app"]
                repository = app.state.market_sidebar_repository
                aggregator = app.state.market_sidebar_aggregator
                if aggregator.repository is not repository:
                    aggregator = SidebarAggregator(repository)
                    app.state.market_sidebar_aggregator = aggregator
                context = SidebarContext(
                    connection_id=connection_id,
                    subscription_id=sidebar_message.subscription_id,
                    chart_symbol=sidebar_message.chart_symbol,
                    chart_epoch=sidebar_message.chart_epoch,
                    watchlist_symbols=tuple(sidebar_message.watchlist_symbols),
                    channels=frozenset(sidebar_message.channels),
                    watchlist_id=sidebar_message.watchlist_id,
                    watchlist_revision=sidebar_message.watchlist_revision,
                )
                await _cancel_task(sidebar_task)
                await _cancel_task(sidebar_listener_task)
                if (
                    sidebar_context is not None
                    and sidebar_stream_id(sidebar_context) != sidebar_stream_id(context)
                ):
                    await aggregator.unsubscribe(connection_id, sidebar_context.subscription_id)
                next_demand_key = _sidebar_demand_key(
                    connection_id,
                    context.subscription_id,
                )
                if sidebar_demand_key is not None and sidebar_demand_key != next_demand_key:
                    await _safe_delete_demand(repository, sidebar_demand_key)
                sidebar_demand_key = next_demand_key
                await _safe_set_demand(repository, sidebar_demand_key, context)
                sidebar_context = context
                event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
                await event_queue.put({"type": "sidebar_context_confirmed"})
                await websocket.send_json(
                    {
                        "type": "sidebar_context_set",
                        "subscription_id": context.subscription_id,
                        "chart_symbol": context.chart_symbol,
                        "chart_epoch": context.chart_epoch,
                        "watchlist_id": context.watchlist_id,
                        "watchlist_revision": context.watchlist_revision,
                        "stream_id": sidebar_stream_id(context),
                        "sequence": 0,
                        "snapshot_version": 0,
                        "cursor": {"sequence": 0, "snapshot_version": 0},
                    }
                )
                sidebar_task = asyncio.create_task(
                    _send_sidebar_updates(
                        websocket,
                        context,
                        aggregator,
                        event_queue,
                        after_sequence=sidebar_message.after_sequence,
                        snapshot_version=sidebar_message.snapshot_version,
                    )
                )
                relay_ready = asyncio.Event()
                sidebar_listener_task = asyncio.create_task(
                    _relay_sidebar_updates(event_queue, settings.redis_url, context, relay_ready)
                )
                # The refresh must follow Redis subscription or its update can be missed.
                try:
                    await asyncio.wait_for(relay_ready.wait(), timeout=1.0)
                except TimeoutError:
                    # Redis is unavailable or subscription failed, so publishing a stream request
                    # cannot produce a notification for this connection.
                    logger.warning("Sidebar relay was not ready; refresh request was not published")
                else:
                    asyncio.create_task(aggregator.request_refresh(context, "context_confirmed"))
            else:
                await websocket.send_json(
                    {"type": "error", "message": f"Unsupported message type: {msg_type}"}
                )
    except WebSocketDisconnect:
        return
    finally:
        producer_task.cancel()
        await _cancel_task(sidebar_task)
        await _cancel_task(sidebar_listener_task)
        repository = getattr(websocket.scope["app"].state, "market_sidebar_repository", None)
        await _safe_delete_demand(repository, sidebar_demand_key)
        aggregator = getattr(websocket.scope["app"].state, "market_sidebar_aggregator", None)
        if aggregator is not None:
            await aggregator.disconnect(connection_id)


async def _send_sidebar_updates(
    websocket: WebSocket,
    context: SidebarContext,
    aggregator: SidebarAggregator,
    event_queue: asyncio.Queue[dict[str, Any]],
    *,
    after_sequence: int,
    snapshot_version: int,
) -> None:
    sequence = after_sequence
    version = snapshot_version
    while True:
        await event_queue.get()
        events = await aggregator.delta_events(context, sequence, version)
        for event in events:
            await websocket.send_json(event)
            sequence = event["sequence"]
            version = event["snapshot_version"]


async def _relay_sidebar_updates(
    queue: asyncio.Queue[dict[str, Any]],
    redis_url: str,
    context: SidebarContext,
    ready: asyncio.Event | None = None,
) -> None:
    """Turn provider/local publish events into queue notifications, never a polling loop."""
    client = await _try_create_redis_client(redis_url)
    if client is None:
        return
    pubsub = client.pubsub()
    try:
        await pubsub.subscribe(
            SIDEBAR_UPDATE_CHANNEL,
            CHAN_UPDATE_CHANNEL,
            STRATEGY_UPDATE_CHANNEL,
        )
        if ready is not None:
            ready.set()
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            payload = _parse_sidebar_update(message.get("data"))
            if payload is not None and _sidebar_update_matches(payload, context):
                queue.put_nowait(payload)
    except asyncio.CancelledError:
        raise
    finally:
        try:
            await pubsub.unsubscribe(
                SIDEBAR_UPDATE_CHANNEL,
                CHAN_UPDATE_CHANNEL,
                STRATEGY_UPDATE_CHANNEL,
            )
            await pubsub.aclose()
            await client.aclose()
        except Exception:
            pass


def _parse_sidebar_update(data: Any) -> dict | None:
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
    if payload.get("source") == "iwencai":
        return payload
    if payload.get("source") == "local_db" or payload.get("type") in {
        "chan_head_update",
        "strategy_signal_update",
        "strategy_lifecycle_update",
    }:
        payload["source"] = "local_db"
        return payload
    return None


def _sidebar_update_matches(payload: dict[str, Any], context: SidebarContext) -> bool:
    symbol = str(payload.get("chart_symbol") or payload.get("symbol") or "").upper()
    if symbol != context.chart_symbol:
        return False
    if payload.get("source") == "local_db":
        return True
    try:
        return (
            int(payload.get("chart_epoch")) == context.chart_epoch
            and int(payload.get("watchlist_revision")) == context.watchlist_revision
        )
    except (TypeError, ValueError):
        return False


def _sidebar_demand_key(connection_id: str, subscription_id: str) -> str:
    return f"market:sidebar:demand:{connection_id}:{subscription_id}"


def _sidebar_demand_payload(context: SidebarContext) -> dict[str, Any]:
    return {
        "chart_symbol": context.chart_symbol,
        "watchlist_symbols": list(context.watchlist_symbols),
        "updated_at": datetime.now().astimezone().isoformat(),
    }


async def _safe_set_demand(repository, key: str, context: SidebarContext) -> None:
    try:
        await repository.set_demand(
            key,
            _sidebar_demand_payload(context),
            SIDEBAR_DEMAND_TTL_SECONDS,
        )
    except Exception:
        pass


async def _safe_delete_demand(repository, key: str | None) -> None:
    if repository is None or key is None:
        return
    try:
        await repository.delete_demand(key)
    except Exception:
        pass


async def _cancel_task(task: asyncio.Task | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


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
    fingerprint = hashlib.blake2s(
        "|".join(
            str(bar.get(field) or "")
            for field in ("open", "high", "low", "close", "volume")
        ).encode("utf-8"),
        digest_size=6,
    ).hexdigest()
    return (
        f"rt:{symbol.upper()}:{normalized_timeframe}:"
        f"{bar_time:012d}:{fingerprint}:{revision:06d}:{complete}"
    )
