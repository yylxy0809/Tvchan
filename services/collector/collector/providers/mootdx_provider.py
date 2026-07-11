from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from trading_protocol import Bar, SymbolInfo, canonical_kline_timestamp, normalize_timeframe

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class MootdxProvider(MarketDataProvider):
    name = "mootdx"

    async def list_symbols(self) -> list[SymbolInfo]:
        raise RuntimeError("mootdx symbol discovery is not configured; use pytdx for symbol listing")

    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        return await asyncio.to_thread(self._get_bars_sync, symbol, timeframe, start, end, limit)

    async def healthcheck(self) -> ProviderHealth:
        try:
            import mootdx  # noqa: F401
        except ImportError:
            return ProviderHealth(self.name, False, "mootdx package is not installed")
        return ProviderHealth(self.name, True, "mootdx package is importable")

    def _get_bars_sync(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> list[Bar]:
        try:
            from mootdx.quotes import Quotes
        except ImportError as exc:
            raise RuntimeError("mootdx package is not installed") from exc

        client = Quotes.factory(market="std")
        code, _exchange = split_symbol(symbol)
        frequency = mootdx_frequency(normalize_timeframe(timeframe))
        frame = client.bars(symbol=code, frequency=frequency, offset=0)
        if frame is None or len(frame) == 0:
            return []
        rows = frame.tail(limit).to_dict("records")
        bars = [
            Bar(
                symbol=symbol.upper(),
                timeframe=normalize_timeframe(timeframe),
                ts=parse_mootdx_datetime(row, normalize_timeframe(timeframe)),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(float(row.get("volume") or row.get("vol") or 0)),
                amount=None if row.get("amount") is None else float(row.get("amount")),
                source=self.name,
            )
            for row in rows
        ]
        if start is not None:
            bars = [bar for bar in bars if bar.ts >= start]
        if end is not None:
            bars = [bar for bar in bars if bar.ts <= end]
        return sorted(bars, key=lambda bar: bar.ts)


def split_symbol(symbol: str) -> tuple[str, str]:
    normalized = symbol.strip().upper()
    if "." not in normalized:
        return normalized, "SH" if normalized.startswith("6") else "SZ"
    code, exchange = normalized.split(".", 1)
    return code, exchange


def mootdx_frequency(timeframe: str) -> int | str:
    return {
        "5f": 5,
        "15f": 15,
        "30f": 30,
        "1h": 60,
        "1d": 9,
        "1w": "w",
        "1m": "m",
    }[timeframe]


def parse_mootdx_datetime(row: dict, timeframe: str) -> datetime:
    value = row.get("datetime") or row.get("date") or row.get("time")
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=SHANGHAI_TZ)
        local = parsed.astimezone(SHANGHAI_TZ)
        return canonical_kline_timestamp(
            timeframe,
            parsed,
            date_only=(local.hour, local.minute, local.second, local.microsecond) == (0, 0, 0, 0),
        )
    if value is None and "index" in row:
        value = row["index"]
    text = str(value).replace("/", "-")
    parsed = datetime.fromisoformat(text)
    return canonical_kline_timestamp(
        timeframe,
        parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=SHANGHAI_TZ),
        date_only=len(text) == 10,
    )
