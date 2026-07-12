from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from collector.storage.postgres import source_priority_case

from .contracts import Freshness, MarketDataResult, Quote


_SOURCE = "local_postgres_canonical"
_SYMBOL = re.compile(r"^(\d{6})\.(SH|SZ|BJ)$", re.IGNORECASE)
_FRESH_FOR = timedelta(minutes=15)
_DELAYED_FOR = timedelta(days=1)

# Resolve each requested symbol once, then use bounded index-backed lookups for its
# latest 5-minute bar and the previous Shanghai trading day's daily close.
_SOURCE_PRIORITY = source_priority_case("source")
_LATEST_QUOTES_SQL = f"""
WITH requested AS (
    SELECT split_part(value, '.', 1) AS code, split_part(value, '.', 2) AS exchange
    FROM unnest($1::text[]) AS request(value)
), resolved AS (
    SELECT requested.code, requested.exchange, symbols.id AS symbol_id
    FROM requested
    LEFT JOIN symbols ON symbols.code = requested.code AND symbols.exchange = requested.exchange
)
SELECT resolved.code, resolved.exchange,
       intraday.ts AS provider_ts,
       intraday.close_x1000,
       intraday.volume,
       intraday.amount_x100,
       previous_daily.close_x1000 AS previous_close_x1000
FROM resolved
LEFT JOIN LATERAL (
    SELECT ts, close_x1000, volume, amount_x100
    FROM klines
    WHERE symbol_id = resolved.symbol_id
      AND timeframe = 5
      AND is_complete = TRUE
      AND ts >= now() - INTERVAL '45 days'
    ORDER BY ts DESC, ({_SOURCE_PRIORITY}) DESC, revision DESC, updated_at DESC
    LIMIT 1
) AS intraday ON TRUE
LEFT JOIN LATERAL (
    SELECT daily.close_x1000
    FROM klines AS daily
    WHERE daily.symbol_id = resolved.symbol_id
      AND daily.timeframe = 1440
      AND daily.is_complete = TRUE
      AND daily.ts >= now() - INTERVAL '120 days'
      AND daily.ts < (date_trunc('day', intraday.ts AT TIME ZONE 'Asia/Shanghai') AT TIME ZONE 'Asia/Shanghai')
    ORDER BY daily.ts DESC, ({source_priority_case("daily.source")}) DESC,
             daily.revision DESC, daily.updated_at DESC
    LIMIT 1
) AS previous_daily ON TRUE
"""


class LocalPostgresQuoteSource:
    """Read normalized quotes from canonical local K-lines without fabricating data."""

    def __init__(
        self,
        database_url: str,
        *,
        pool_factory: Callable[[str], Awaitable[Any]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self._database_url = database_url
        self._pool_factory = pool_factory
        self._now = now or (lambda: datetime.now(UTC))
        self._pool: Any | None = None
        self._pool_lock = asyncio.Lock()

    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]:
        requested = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        valid = tuple(symbol for symbol in requested if _SYMBOL.fullmatch(symbol))
        results = {
            symbol: self._unavailable("invalid_symbol")
            for symbol in requested
            if symbol not in valid
        }
        if not valid:
            return results
        try:
            pool = await self._get_pool()
            async with pool.acquire() as connection:
                rows = await connection.fetch(_LATEST_QUOTES_SQL, list(valid))
        except Exception:
            results.update({symbol: self._unavailable("local_quote_source_unavailable") for symbol in valid})
            return results

        by_symbol = {f"{row['code']}.{row['exchange']}": row for row in rows}
        for symbol in valid:
            row = by_symbol.get(symbol)
            if row is None or row["provider_ts"] is None or row["previous_close_x1000"] is None:
                results[symbol] = self._unavailable("canonical_quote_unavailable")
                continue
            previous_close = row["previous_close_x1000"] / 1000
            price = row["close_x1000"] / 1000
            results[symbol] = MarketDataResult.available(
                Quote(
                    symbol=symbol,
                    price=price,
                    change=price - previous_close,
                    change_percent=(price - previous_close) / previous_close * 100 if previous_close else None,
                    volume=float(row["volume"]),
                    amount=None if row["amount_x100"] is None else row["amount_x100"] / 100,
                ),
                source=_SOURCE,
                provider_ts=row["provider_ts"],
                freshness=self._freshness(row["provider_ts"]),
            )
        return results

    async def close(self) -> None:
        async with self._pool_lock:
            if self._pool is not None:
                await self._pool.close()
                self._pool = None

    async def _get_pool(self) -> Any:
        async with self._pool_lock:
            if self._pool is None:
                if self._pool_factory is not None:
                    self._pool = await self._pool_factory(self._database_url)
                else:
                    import asyncpg

                    self._pool = await asyncpg.create_pool(self._database_url)
            return self._pool

    def _freshness(self, provider_ts: datetime) -> Freshness:
        age = self._now() - provider_ts.astimezone(UTC)
        if age <= _FRESH_FOR:
            return Freshness.LIVE
        if age <= _DELAYED_FOR:
            return Freshness.DELAYED
        return Freshness.STALE

    @staticmethod
    def _unavailable(error: str) -> MarketDataResult[Quote]:
        return MarketDataResult.unavailable(source=_SOURCE, error=error)
