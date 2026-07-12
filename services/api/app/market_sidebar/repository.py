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

    async def close(self) -> None:
        """Release optional repository resources."""


class RedisSidebarSnapshotRepository(SidebarSnapshotRepository):
    """Read snapshots and maintain ephemeral sidebar demand registrations."""

    def __init__(self, redis_url: str, client: Any | None = None) -> None:
        self._redis_url = redis_url
        self._client = client

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
