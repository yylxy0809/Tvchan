from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from trading_protocol import Bar, SymbolInfo, canonical_kline_timestamp, normalize_timeframe

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class HttpKlineProvider(MarketDataProvider):
    name = "http"

    def __init__(self, *, timeout: float = 5.0) -> None:
        self.timeout = timeout

    async def list_symbols(self) -> list[SymbolInfo]:
        raise RuntimeError(f"{self.name} does not support symbol discovery")

    async def healthcheck(self) -> ProviderHealth:
        try:
            await self.get_bars("000001.SZ", "5f", limit=1)
        except Exception as exc:
            return ProviderHealth(self.name, False, str(exc)[:200])
        return ProviderHealth(self.name, True, "sample K-line request succeeded")


class TencentKlineProvider(HttpKlineProvider):
    name = "tencent"

    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        normalized = normalize_timeframe(timeframe)
        if normalized not in {"5f", "15f", "30f", "1h", "1d"}:
            raise RuntimeError(f"tencent provider does not support {normalized}")
        market_code = tencent_symbol(symbol)
        ktype = {
            "5f": "m5",
            "15f": "m15",
            "30f": "m30",
            "1h": "m60",
            "1d": "day",
        }[normalized]
        params = f"{market_code},{ktype},,,{limit}"
        url = (
            "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/kline/kline"
            if normalized == "1d"
            else "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/kline/mkline"
        )
        async with httpx.AsyncClient(
            timeout=self.timeout,
            trust_env=False,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as client:
            response = await client.get(url, params={"param": params})
            response.raise_for_status()
            payload = response.json()
        source_data = payload.get("data", {}).get(market_code, {})
        rows = source_data.get(ktype) or source_data.get(f"qfq{ktype}") or []
        bars = [tencent_row_to_bar(symbol, normalized, row) for row in rows[-limit:]]
        return filter_range(bars, start, end)


class BaiduKlineProvider(HttpKlineProvider):
    name = "baidu"

    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        normalized = normalize_timeframe(timeframe)
        if normalized not in {"5f", "15f", "30f", "1d"}:
            raise RuntimeError(f"baidu provider does not support {normalized}")
        code, exchange = split_symbol(symbol)
        query_code = f"{exchange.lower()}{code}"
        url = "https://finance.pae.baidu.com/vapi/v1/getquotation"
        params = {
            "srcid": "5353",
            "pointType": "string",
            "group": "quotation_kline_ab",
            "query": query_code,
            "code": query_code,
            "market_type": "ab",
            "newFormat": "1",
            "finClientType": "pc",
            "ktype": {"5f": "5", "15f": "15", "30f": "30", "1d": "day"}[normalized],
        }
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        rows = (
            payload.get("Result", [{}])[0]
            .get("DisplayData", {})
            .get("resultData", {})
            .get("tplData", {})
            .get("result", {})
            .get("kline", [])
        )
        bars = [baidu_row_to_bar(symbol, normalized, row) for row in rows[-limit:]]
        return filter_range(bars, start, end)


def split_symbol(symbol: str) -> tuple[str, str]:
    normalized = symbol.strip().upper()
    if "." not in normalized:
        return normalized, "SH" if normalized.startswith("6") else "SZ"
    code, exchange = normalized.split(".", 1)
    return code, exchange


def tencent_symbol(symbol: str) -> str:
    code, exchange = split_symbol(symbol)
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(exchange, exchange.lower())
    return f"{prefix}{code}"


def parse_local_datetime(value: str, timeframe: str, *, timestamp_is_bar_end: bool = False) -> datetime:
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M", "%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=SHANGHAI_TZ)
            date_only = fmt == "%Y%m%d"
            break
        except ValueError:
            continue
    else:
        dt = datetime.fromisoformat(text).replace(tzinfo=SHANGHAI_TZ)
        date_only = len(text) == 10 and text[4:5] == "-" and text[7:8] == "-"
    return canonical_kline_timestamp(timeframe, dt, date_only=date_only)


def tencent_row_to_bar(symbol: str, timeframe: str, row: list) -> Bar:
    return Bar(
        symbol=symbol.upper(),
        timeframe=timeframe,
        ts=parse_local_datetime(row[0], timeframe, timestamp_is_bar_end=True),
        open=float(row[1]),
        close=float(row[2]),
        high=float(row[3]),
        low=float(row[4]),
        volume=tencent_volume_to_shares(symbol, row[5]),
        source="tencent",
    )


def tencent_volume_to_shares(symbol: str, raw_volume: object) -> int:
    volume = int(float(raw_volume or 0))
    code, exchange = split_symbol(symbol)
    if exchange == "SH" and code.startswith(("688", "689")):
        return volume
    return volume * 100


def baidu_row_to_bar(symbol: str, timeframe: str, row) -> Bar:
    if isinstance(row, dict):
        values = row
        ts_value = values.get("date") or values.get("time")
        open_value = values.get("open")
        high_value = values.get("high")
        low_value = values.get("low")
        close_value = values.get("close")
        volume_value = values.get("volume") or values.get("vol") or 0
    else:
        values = list(row)
        ts_value, open_value, high_value, low_value, close_value, volume_value = values[:6]
    return Bar(
        symbol=symbol.upper(),
        timeframe=timeframe,
        ts=parse_local_datetime(str(ts_value), timeframe),
        open=float(open_value),
        high=float(high_value),
        low=float(low_value),
        close=float(close_value),
        volume=int(float(volume_value or 0)),
        source="baidu",
    )


def filter_range(bars: list[Bar], start: datetime | None, end: datetime | None) -> list[Bar]:
    if start is not None:
        bars = [bar for bar in bars if bar.ts >= start]
    if end is not None:
        bars = [bar for bar in bars if bar.ts <= end]
    return sorted(bars, key=lambda bar: bar.ts)
