from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from app.repositories.postgres import TIMEFRAME_TO_DB, get_bars_db


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class FakeAcquire:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakePool:
    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)

    async def fetch(self, query, *args):
        return await self.conn.fetch(query, *args)


class FakeConn:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.args: list[tuple] = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
        return [
            {
                "ts": datetime.fromtimestamp(1000, tz=UTC),
                "open_x1000": 10000,
                "high_x1000": 10100,
                "low_x1000": 9900,
                "close_x1000": 10050,
                "volume": 100,
                "amount_x100": 12345,
                "is_complete": True,
                "revision": 0,
            }
        ]


def test_get_bars_db_excludes_seed_when_real_source_exists_for_stored_timeframe() -> None:
    async def scenario():
        conn = FakeConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="1w",
            start=None,
            end=None,
            limit=300,
        )
        assert rows[0]["close"] == 10.05
        query = conn.queries[0]
        assert "k.source <> 1" in query
        assert "k_real.source in (2, 3, 4)" in query
        assert conn.args[0][0:3] == ("000001", "SZ", TIMEFRAME_TO_DB["1w"])

    asyncio.run(scenario())


def test_get_bars_db_derives_1d_from_canonical_5f() -> None:
    class DerivedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            assert args[2] == TIMEFRAME_TO_DB["5f"]
            return [
                _db_bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1),
                _db_bar("2026-04-27 11:30", open=10.1, high=10.5, low=10.0, close=10.4),
                _db_bar("2026-04-27 13:05", open=10.4, high=10.6, low=10.3, close=10.5),
                _db_bar("2026-04-27 15:00", open=10.5, high=10.8, low=10.4, close=10.7),
            ]

    async def scenario():
        conn = DerivedConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="1d",
            start=None,
            end=None,
            limit=1,
        )
        assert len(rows) == 1
        assert _format_time(rows[0]["time"]) == "2026-04-27 15:00"
        assert rows[0]["open"] == 10.0
        assert rows[0]["close"] == 10.7
        assert conn.args[0][0:3] == ("000001", "SZ", TIMEFRAME_TO_DB["5f"])

    asyncio.run(scenario())


def test_get_bars_db_derives_supported_higher_timeframes_from_canonical_5f() -> None:
    class DerivedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            assert args[2] == TIMEFRAME_TO_DB["5f"]
            return _base_5f_rows()

    async def scenario(timeframe: str):
        conn = DerivedConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe=timeframe,
            start=None,
            end=None,
            limit=1,
        )
        assert rows
        assert conn.args[0][0:3] == ("000001", "SZ", TIMEFRAME_TO_DB["5f"])

    for timeframe in ("15f", "30f", "1h", "1d"):
        asyncio.run(scenario(timeframe))


def test_get_bars_db_keeps_0930_in_first_derived_slot() -> None:
    class DerivedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            assert args[2] == TIMEFRAME_TO_DB["5f"]
            return [
                _db_bar("2026-04-27 09:30", open=9.9, high=10.0, low=9.8, close=9.95),
                _db_bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1),
                _db_bar("2026-04-27 09:40", open=10.1, high=10.3, low=10.0, close=10.2),
                _db_bar("2026-04-27 09:45", open=10.2, high=10.4, low=10.1, close=10.3),
            ]

    async def scenario():
        conn = DerivedConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="15f",
            start=None,
            end=None,
            limit=2,
        )
        assert len(rows) == 1
        assert _format_time(rows[0]["time"]) == "2026-04-27 09:45"
        assert rows[0]["open"] == 9.9
        assert rows[0]["high"] == 10.4
        assert rows[0]["low"] == 9.8
        assert rows[0]["close"] == 10.3

    asyncio.run(scenario())


def _db_bar(
    value: str,
    *,
    open: float,
    high: float,
    low: float,
    close: float,
) -> dict:
    ts = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI_TZ)
    return {
        "ts": ts.astimezone(UTC),
        "open_x1000": int(open * 1000),
        "high_x1000": int(high * 1000),
        "low_x1000": int(low * 1000),
        "close_x1000": int(close * 1000),
        "volume": 100,
        "amount_x100": None,
        "is_complete": True,
        "revision": 0,
    }


def _base_5f_rows() -> list[dict]:
    return [
        _db_bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1),
        _db_bar("2026-04-27 09:40", open=10.1, high=10.3, low=10.0, close=10.2),
        _db_bar("2026-04-27 09:45", open=10.2, high=10.4, low=10.1, close=10.3),
        _db_bar("2026-04-27 10:00", open=10.3, high=10.5, low=10.2, close=10.4),
        _db_bar("2026-04-27 15:00", open=10.4, high=10.8, low=10.3, close=10.7),
    ]


def _format_time(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M")
