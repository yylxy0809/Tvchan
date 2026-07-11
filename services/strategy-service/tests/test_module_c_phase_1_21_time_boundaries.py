import asyncio
from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.repositories.module_c_repo import ModuleCRepository


class Conn:
    def __init__(self): self.calls = []
    async def fetch(self, query, *args):
        self.calls.append((query, args))
        if "from symbols where" in query:
            return [{"id": 1, "symbol": "000001.SZ"}]
        return [{"symbol_id": 1, "timeframe": 30, "ts": datetime(2025, 1, 1, 8, tzinfo=UTC), "open_x1000": 1, "high_x1000": 1, "low_x1000": 1, "close_x1000": 1, "volume": 1, "is_complete": True}]


def test_fetch_complete_klines_normalizes_database_bounds_to_aware_utc():
    conn = Conn()
    rows = asyncio.run(ModuleCRepository(None).fetch_complete_klines(symbols=["000001.SZ"], levels=("30f",), start="2025-01-01T16:00:00+08:00", end=datetime(2025, 1, 1, 9, tzinfo=UTC), conn=conn))
    query, args = conn.calls[-1]
    assert args[2:] == (datetime(2025, 1, 1, 8, tzinfo=UTC), datetime(2025, 1, 1, 9, tzinfo=UTC))
    assert rows[0]["symbol"] == "000001.SZ"
    assert rows[0]["ts"] == datetime(2025, 1, 1, 8, tzinfo=UTC)


def test_fetch_complete_klines_rejects_naive_database_bounds_before_reading():
    conn = Conn()
    with pytest.raises(ValueError, match="Naive"):
        asyncio.run(ModuleCRepository(None).fetch_complete_klines(symbols=["000001.SZ"], levels=("30f",), start=datetime(2025, 1, 1), end=datetime(2025, 1, 1, tzinfo=UTC), conn=conn))
    assert conn.calls == []


def test_fetch_complete_klines_validates_bounds_before_empty_set_early_return():
    conn = Conn()
    with pytest.raises(ValueError, match="Naive"):
        asyncio.run(ModuleCRepository(None).fetch_complete_klines(symbols=[], levels=(), start=datetime(2025, 1, 1), end=datetime(2025, 1, 1, tzinfo=UTC), conn=conn))
    with pytest.raises(ValueError, match="explicit start and end"):
        asyncio.run(ModuleCRepository(None).fetch_complete_klines(symbols=[], levels=(), start=None, end=None, conn=conn))
    assert conn.calls == []


class IntradayConn:
    def __init__(self, ts): self.ts = ts
    async def fetch(self, *_args): return [{"symbol": "000001.SZ", "timeframe": 30, "ts": self.ts, "is_complete": True}]


def test_intraday_kline_returns_are_normalized_and_naive_values_rejected():
    episode = [{"symbol": "000001.SZ", "daily_setup_first_seen_time": "2025-01-01T08:00:00+00:00", "trigger_window_end": "2025-01-01T09:00:00+00:00"}]
    rows = asyncio.run(ModuleCRepository(None).fetch_intraday_klines_for_episodes(episode, conn=IntradayConn(datetime(2025, 1, 1, 17, tzinfo=timezone(timedelta(hours=8))))))
    assert rows[0]["ts"] == datetime(2025, 1, 1, 9, tzinfo=UTC)
    with pytest.raises(ValueError, match="Naive"):
        asyncio.run(ModuleCRepository(None).fetch_intraday_klines_for_episodes(episode, conn=IntradayConn(datetime(2025, 1, 1, 9))))
