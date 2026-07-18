from __future__ import annotations

import asyncio
from datetime import date

import pytest

from collector.market_data.trading_day_cache import (
    RELEASE_LEASE_SCRIPT,
    CacheKey,
    RedisTradingDayCache,
)


class RacingRedis:
    def __init__(self, value: str) -> None:
        self.value = value
        self.eval_calls: list[tuple[str, int, str, str]] = []
        self.deleted = 0

    async def get(self, _key: str) -> str | None:
        observed = self.value
        self.value = "successor-token"
        return observed

    async def delete(self, _key: str) -> int:
        self.value = ""
        self.deleted += 1
        return 1

    async def eval(self, script: str, key_count: int, key: str, token: str) -> int:
        self.eval_calls.append((script, key_count, key, token))
        self.value = "successor-token"
        if self.value != token:
            return 0
        self.value = ""
        self.deleted += 1
        return 1


class StableRedis(RacingRedis):
    async def eval(self, script: str, key_count: int, key: str, token: str) -> int:
        self.eval_calls.append((script, key_count, key, token))
        if self.value != token:
            return 0
        self.value = ""
        self.deleted += 1
        return 1


def test_release_cannot_delete_a_successor_lease() -> None:
    redis = RacingRedis("old-token")
    cache = RedisTradingDayCache(redis)
    key = CacheKey(date(2026, 7, 18), "quote", "000001.SZ")

    asyncio.run(cache.release(key, "old-token"))

    assert redis.value == "successor-token"
    assert redis.deleted == 0
    assert redis.eval_calls[0][1:] == (1, key.lease_key(), "old-token")
    assert " ".join(redis.eval_calls[0][0].split()) == " ".join(
        RELEASE_LEASE_SCRIPT.split()
    ) == (
        'if redis.call("GET", KEYS[1]) == ARGV[1] then '
        'return redis.call("DEL", KEYS[1]) end return 0'
    )
    assert key.lease_key() not in redis.eval_calls[0][0]
    assert "old-token" not in redis.eval_calls[0][0]


def test_release_deletes_only_the_matching_lease_token() -> None:
    redis = StableRedis("owner-token")
    cache = RedisTradingDayCache(redis)
    key = CacheKey(date(2026, 7, 18), "quote", "000001.SZ")

    asyncio.run(cache.release(key, "owner-token"))

    assert redis.value == ""
    assert redis.deleted == 1


def test_release_propagates_redis_failure_without_non_atomic_fallback() -> None:
    class FailingRedis:
        async def eval(self, _script, _key_count, _key, _token):
            raise ConnectionError("redis unavailable")

        async def get(self, _key):
            raise AssertionError("GET fallback is forbidden")

        async def delete(self, _key):
            raise AssertionError("DELETE fallback is forbidden")

    cache = RedisTradingDayCache(FailingRedis())
    key = CacheKey(date(2026, 7, 18), "quote", "000001.SZ")
    secret = "lease-token-must-not-leak"

    with pytest.raises(ConnectionError) as caught:
        asyncio.run(cache.release(key, secret))

    assert secret not in str(caught.value)
