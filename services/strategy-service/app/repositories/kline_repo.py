from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime
from math import isnan

import asyncpg

from app.domain.enums import LEVEL_TO_DB


@dataclass(slots=True)
class KlineBar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class KlineRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self._series_cache: dict[tuple[int, str], list[KlineBar]] = {}
        self._series_times: dict[tuple[int, str], list[datetime]] = {}

    async def prime_symbol_cache(
        self,
        symbol_id: int,
        *,
        start_time: datetime,
        end_time: datetime,
        timeframes: tuple[str, ...] = ("30f", "1d", "1w"),
    ) -> None:
        for timeframe in timeframes:
            cached_start = start_time if timeframe == "30f" else None
            await self._prime_klines(symbol_id, timeframe, start=cached_start, end=end_time)

    def release_symbol_cache(self, symbol_id: int) -> None:
        keys = [key for key in self._series_cache if key[0] == symbol_id]
        for key in keys:
            self._series_cache.pop(key, None)
            self._series_times.pop(key, None)

    async def get_klines(
        self,
        symbol_id: int,
        timeframe: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[KlineBar]:
        key = (symbol_id, timeframe)
        cached = self._series_cache.get(key)
        cached_times = self._series_times.get(key)
        if cached is not None and cached_times is not None:
            left = bisect_left(cached_times, start) if start is not None else 0
            right = bisect_right(cached_times, end) if end is not None else len(cached)
            bars = cached[left:right]
            return bars[:limit] if limit is not None else bars

        sql = """
            select ts, open_x1000, high_x1000, low_x1000, close_x1000, volume
            from klines
            where symbol_id = $1
              and timeframe = $2
              and source = any(array[2,3,4,5,6,7,8,9]::smallint[])
              and ($3::timestamptz is null or ts >= $3)
              and ($4::timestamptz is null or ts <= $4)
            order by ts
        """
        params: list[object] = [symbol_id, LEVEL_TO_DB[timeframe], start, end]
        if limit is not None:
            sql += " limit $5"
            params.append(limit)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [
            KlineBar(
                ts=row["ts"],
                open=row["open_x1000"] / 1000,
                high=row["high_x1000"] / 1000,
                low=row["low_x1000"] / 1000,
                close=row["close_x1000"] / 1000,
                volume=int(row["volume"] or 0),
            )
            for row in rows
        ]

    async def get_next_open(self, symbol_id: int, timeframe: str, after_time: datetime) -> tuple[datetime, float] | None:
        key = (symbol_id, timeframe)
        cached = self._series_cache.get(key)
        cached_times = self._series_times.get(key)
        if cached is not None and cached_times is not None:
            index = bisect_right(cached_times, after_time)
            if index >= len(cached):
                return None
            bar = cached[index]
            return bar.ts, bar.open
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select ts, open_x1000
                from klines
                where symbol_id = $1
                  and timeframe = $2
                  and source = any(array[2,3,4,5,6,7,8,9]::smallint[])
                  and ts > $3
                order by ts
                limit 1
                """,
                symbol_id,
                LEVEL_TO_DB[timeframe],
                after_time,
            )
        if row is None:
            return None
        return row["ts"], row["open_x1000"] / 1000

    async def get_close_at_or_before(self, symbol_id: int, timeframe: str, as_of_time: datetime) -> tuple[datetime, float] | None:
        key = (symbol_id, timeframe)
        cached = self._series_cache.get(key)
        cached_times = self._series_times.get(key)
        if cached is not None and cached_times is not None:
            index = bisect_right(cached_times, as_of_time) - 1
            if index < 0:
                return None
            bar = cached[index]
            return bar.ts, bar.close
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select ts, close_x1000
                from klines
                where symbol_id = $1
                  and timeframe = $2
                  and source = any(array[2,3,4,5,6,7,8,9]::smallint[])
                  and ts <= $3
                order by ts desc
                limit 1
                """,
                symbol_id,
                LEVEL_TO_DB[timeframe],
                as_of_time,
            )
        if row is None:
            return None
        return row["ts"], row["close_x1000"] / 1000

    async def get_latest_market_cap(self, symbol_id: int) -> float | None:
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                """
                select 1
                from information_schema.tables
                where table_schema = 'public'
                  and table_name = 'symbol_fundamentals'
                """
            )
            if not exists:
                return None
            row = await conn.fetchrow(
                """
                select market_cap_x100
                from symbol_fundamentals
                where symbol_id = $1
                """,
                symbol_id,
            )
        if row is None or row["market_cap_x100"] is None:
            return None
        return row["market_cap_x100"] / 100

    async def _prime_klines(
        self,
        symbol_id: int,
        timeframe: str,
        *,
        start: datetime | None,
        end: datetime,
    ) -> None:
        key = (symbol_id, timeframe)
        if key in self._series_cache:
            return
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select ts, open_x1000, high_x1000, low_x1000, close_x1000, volume
                from klines
                where symbol_id = $1
                  and timeframe = $2
                  and source = any(array[2,3,4,5,6,7,8,9]::smallint[])
                  and ($3::timestamptz is null or ts >= $3)
                  and ts <= $4
                order by ts
                """,
                symbol_id,
                LEVEL_TO_DB[timeframe],
                start,
                end,
            )
        bars = [
            KlineBar(
                ts=row["ts"],
                open=row["open_x1000"] / 1000,
                high=row["high_x1000"] / 1000,
                low=row["low_x1000"] / 1000,
                close=row["close_x1000"] / 1000,
                volume=int(row["volume"] or 0),
            )
            for row in rows
        ]
        self._series_cache[key] = bars
        self._series_times[key] = [bar.ts for bar in bars]


def compute_macd(bars: list[KlineBar], fast: int = 12, slow: int = 26, signal: int = 9) -> list[dict]:
    if not bars:
        return []
    closes = [bar.close for bar in bars]
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    difs = [fast_value - slow_value for fast_value, slow_value in zip(ema_fast, ema_slow)]
    deas = _ema(difs, signal)
    hist = [dif - dea for dif, dea in zip(difs, deas)]
    return [
        {
            "ts": bar.ts,
            "dif": difs[index],
            "dea": deas[index],
            "histogram": hist[index],
        }
        for index, bar in enumerate(bars)
    ]


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    result: list[float] = []
    ema_value = values[0]
    for value in values:
        if isnan(value):
            value = ema_value
        ema_value = alpha * value + (1 - alpha) * ema_value
        result.append(ema_value)
    return result
