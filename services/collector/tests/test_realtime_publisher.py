from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest
import redis.asyncio as redis

from collector.realtime_publisher import publish_chan_head_update


class FakeRedis:
    def __init__(self) -> None:
        self.sequences: dict[str, int] = {}
        self.published: list[tuple[str, dict]] = []
        self.closed = False

    async def incr(self, key: str) -> int:
        self.sequences[key] = self.sequences.get(key, 0) + 1
        return self.sequences[key]

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append((channel, json.loads(payload)))
        return 1

    async def aclose(self) -> None:
        self.closed = True


def test_chan_head_publish_uses_atomic_per_stream_sequence_and_stable_event_id(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        client = FakeRedis()
        monkeypatch.setattr(redis, "from_url", lambda *_args, **_kwargs: client)

        kwargs = {
            "redis_url": "redis://unused",
            "symbol": "000001.sz",
            "level": "5f",
            "modes": ["confirmed"],
            "bar_until": datetime.fromtimestamp(200, UTC),
            "run_id": 77,
            "snapshot_version": "committed-v77",
        }
        assert await publish_chan_head_update(**kwargs)
        assert await publish_chan_head_update(**kwargs)

        first = client.published[0][1]
        second = client.published[1][1]
        assert first == {
            "type": "chan_head_update",
            "schema_version": "chan-head.v1",
            "id": "000001.SZ:5f:confirmed:77:committed-v77",
            "symbol": "000001.SZ",
            "level": "5f",
            "mode": "confirmed",
            "sequence": 1,
            "snapshot_version": "committed-v77",
            "run_id": 77,
            "bar_until": "1970-01-01T00:03:20+00:00",
        }
        assert second["sequence"] == 2
        assert second["id"] == first["id"]
        assert list(client.sequences) == ["chan:head_sequence:000001.SZ:5f:confirmed"]
        assert client.closed

    asyncio.run(scenario())


def test_chan_head_publish_rejects_missing_committed_identity() -> None:
    with pytest.raises(ValueError, match="committed run_id"):
        asyncio.run(
            publish_chan_head_update(
                redis_url="redis://unused",
                symbol="000001.SZ",
                level="5f",
                modes=["confirmed"],
                bar_until="2026-07-11T00:00:00+00:00",
                run_id=0,
                snapshot_version="version",
            )
        )

    with pytest.raises(ValueError, match="committed snapshot_version"):
        asyncio.run(
            publish_chan_head_update(
                redis_url="redis://unused",
                symbol="000001.SZ",
                level="5f",
                modes=["confirmed"],
                bar_until="2026-07-11T00:00:00+00:00",
                run_id=1,
                snapshot_version="",
            )
        )
