from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any


class SidebarSnapshotRepository(ABC):
    @abstractmethod
    async def get_json(self, key: str) -> dict | None:
        raise NotImplementedError

    @abstractmethod
    async def get_json_many(self, keys: list[str] | tuple[str, ...]) -> list[dict | None]:
        raise NotImplementedError

    @abstractmethod
    async def set_demand(self, key: str, value: dict, ttl_seconds: int) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete_demand(self, key: str) -> None:
        raise NotImplementedError

    async def publish_refresh(self, event: dict) -> None:
        """Request an event-driven iWencai cache fill without waiting for it."""

    async def get_local_projection(self, symbol: str) -> dict:
        """Return published local Chan/strategy data; external data never belongs here."""
        return {
            "chan_state": {"source": "local_db", "stroke_states": []},
            "strategy_signals": [],
        }

    async def close(self) -> None:
        """Release optional repository resources."""


class RedisSidebarSnapshotRepository(SidebarSnapshotRepository):
    """Read snapshots and maintain ephemeral sidebar demand registrations."""

    def __init__(self, redis_url: str, client: Any | None = None, db_pool: Any | None = None) -> None:
        self._redis_url = redis_url
        self._client = client
        self._db_pool = db_pool

    def set_db_pool(self, pool: Any | None) -> None:
        self._db_pool = pool

    async def get_json(self, key: str) -> dict | None:
        return (await self.get_json_many([key]))[0]

    async def get_json_many(self, keys: list[str] | tuple[str, ...]) -> list[dict | None]:
        if not keys:
            return []
        try:
            client = await self._get_client()
            values = await client.mget(list(keys))
        except Exception:
            return [None] * len(keys)
        return [_decode_json_object(raw) for raw in values]

    async def set_demand(self, key: str, value: dict, ttl_seconds: int) -> None:
        _validate_demand_key(key)
        client = await self._get_client()
        await client.set(
            key,
            json.dumps(value, separators=(",", ":")),
            ex=ttl_seconds,
        )

    async def delete_demand(self, key: str) -> None:
        _validate_demand_key(key)
        client = await self._get_client()
        await client.delete(key)

    async def publish_refresh(self, event: dict) -> None:
        client = await self._get_client()
        await client.xadd(
            "market:sidebar:refresh_requests",
            {"payload": json.dumps(event, separators=(",", ":"))},
            maxlen=10_000,
            approximate=True,
        )

    async def publish_config_changed(self, version: int) -> None:
        client = await self._get_client()
        await client.publish(
            "market:sidebar:config_changed",
            json.dumps({"type": "iwencai_config_changed", "config_version": version}),
        )

    async def get_local_projection(self, symbol: str) -> dict:
        """Read bounded, published local projections without coupling to cache/provider state."""
        if self._db_pool is None:
            return _local_unavailable()
        try:
            acquire = getattr(self._db_pool, "acquire", None)
            if acquire is None:
                return await _read_local_projection(self._db_pool, symbol)
            async with acquire() as connection:
                return await _read_local_projection(connection, symbol)
        except Exception:
            return _local_unavailable()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_client(self):
        if self._client is None:
            import redis.asyncio as redis

            self._client = redis.from_url(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=0.2,
                socket_timeout=0.5,
            )
        return self._client


def _validate_demand_key(key: str) -> None:
    if not key.startswith("market:sidebar:demand:"):
        raise ValueError("repository writes are restricted to sidebar demand keys")


def _decode_json_object(raw: Any) -> dict | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return value if isinstance(value, dict) else None


async def _read_local_projection(connection: Any, symbol: str) -> dict:
    chan_status = {"source": "local_db", "stroke_states": []}
    strategy_signals: list[dict] = []
    try:
        heads = await connection.fetch(
            """
            select distinct on (head.chan_level)
                   head.chan_level, head.mode, head.snapshot_version,
                   stroke.direction, stroke.is_confirmed, stroke.anchor_time,
                   stroke.anchor_price_x1000
            from scheme2_chan_c_published_heads head
            join symbols symbol on symbol.id = head.symbol_id
            left join lateral (
                select detail.direction, detail.is_confirmed,
                       coalesce(detail.end_base_ts, detail.end_ts) as anchor_time,
                       detail.end_price_x1000 as anchor_price_x1000
                from chan_c_strokes detail
                where detail.run_id = head.run_id
                  and detail.mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
                order by coalesce(detail.end_base_ts, detail.end_ts) desc, detail.seq desc, detail.id desc
                limit 1
            ) stroke on true
            where symbol.code || '.' || symbol.exchange = $1
              and head.chan_level = any($2::integer[])
              and head.base_timeframe = head.chan_level
              and head.status = 'published'
            order by head.chan_level,
                     case head.mode when 'predictive' then 0 when 'confirmed' then 1 else 2 end,
                     coalesce(head.published_at, head.updated_at) desc
            limit 5
            """,
            symbol,
            [5, 30, 1440, 10080, 43200],
        )
        stroke_states = [
            _stroke_state(row)
            for row in heads
            if int(row["chan_level"]) in {5, 30, 1440}
        ]
        chan_status = {"source": "local_db", "stroke_states": stroke_states}
    except Exception:
        pass

    try:
        rows = await connection.fetch(
            """
            select signal.id::text as event_id, event_type, status, strategy_code, strategy_version, source_level,
                   source_signal_type, source_signal_side, point_time, first_seen_time,
                   confirm_time, disappear_time, source_snapshot_version,
                   confidence_score, strength_score
            from strategy_signal_events signal
            join symbols symbol on symbol.id = signal.symbol_id
            where symbol.code || '.' || symbol.exchange = $1
            order by coalesce(signal.updated_at, signal.first_seen_time, signal.created_at) desc
            limit 20
            """,
            symbol,
        )
        strategy_signals = [_strategy_signal(row) for row in rows]
    except Exception:
        pass
    return {"chan_state": chan_status, "strategy_signals": strategy_signals}


def _local_unavailable() -> dict:
    return {
        "chan_state": {"source": "local_db", "stroke_states": []},
        "strategy_signals": [],
    }


def _level_label(value: int) -> str:
    return {5: "5f", 30: "30f", 1440: "1d", 10080: "1w", 43200: "1m"}[value]


def _json_time(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _unix_time(value: Any) -> int | None:
    return int(value.timestamp()) if hasattr(value, "timestamp") else None


def _stroke_state(row: Any) -> dict:
    level = _level_label(int(row["chan_level"]))
    direction = {1: "up", -1: "down"}.get(row["direction"], "unknown")
    mode = row["mode"] if row["mode"] in {"confirmed", "predictive"} else None
    return {
        "level": level,
        "label": f"{level} stroke",
        "direction": direction,
        "stateLabel": f"{level} {direction}" if direction != "unknown" else f"{level} unavailable",
        "mode": mode,
        "modeLabel": mode.capitalize() if mode else "Unavailable",
        "confirmed": row["is_confirmed"],
        "anchorTime": _unix_time(row["anchor_time"]),
        "anchorPrice": float(row["anchor_price_x1000"]) / 1000 if row["anchor_price_x1000"] is not None else None,
    }


def _strategy_signal(row: Any) -> dict:
    side = row["source_signal_side"]
    value = row["source_signal_type"] or row["event_type"] or row["status"]
    return {
        "key": row["event_id"],
        "event_id": row["event_id"],
        "label": row["strategy_code"],
        "value": str(value),
        "tone": "up" if side == "buy" else "down" if side == "sell" else "neutral",
        "source": "local_db",
        "event_type": row["event_type"],
        "status": row["status"],
        "source_level": row["source_level"],
        "source_signal_type": row["source_signal_type"],
        "source_signal_side": side,
        "point_time": _json_time(row["point_time"]),
        "first_seen_time": _json_time(row["first_seen_time"]),
        "confirm_time": _json_time(row["confirm_time"]),
        "disappear_time": _json_time(row["disappear_time"]),
        "source_snapshot_version": row["source_snapshot_version"],
        "confidence_score": float(row["confidence_score"]) if row["confidence_score"] is not None else None,
        "strength_score": float(row["strength_score"]) if row["strength_score"] is not None else None,
    }
