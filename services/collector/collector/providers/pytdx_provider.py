from __future__ import annotations

import time
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from collector.providers.seed import SeedProvider
from trading_protocol import Bar, SymbolInfo, normalize_timeframe

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

TDX_SERVERS: tuple[tuple[str, int], ...] = (
    ("124.70.199.56", 7709),
    ("119.147.212.81", 7709),
    ("101.227.73.20", 7709),
    ("101.227.77.254", 7709),
    ("14.215.128.18", 7709),
    ("59.173.18.69", 7709),
    ("60.28.29.69", 7709),
    ("218.60.29.136", 7709),
    ("122.192.35.44", 7709),
    ("221.231.141.60", 7709),
    ("43.242.46.178", 7709),
    ("124.160.88.183", 7709),
    ("47.103.48.45", 7709),
    ("47.100.236.28", 7709),
    ("120.79.60.82", 7709),
    ("112.74.214.43", 7709),
)

TIMEFRAME_TO_TDX_CATEGORY: dict[str, str] = {
    "5f": "KLINE_TYPE_5MIN",
    "15f": "KLINE_TYPE_15MIN",
    "30f": "KLINE_TYPE_30MIN",
    "1h": "KLINE_TYPE_1HOUR",
    "1d": "KLINE_TYPE_DAILY",
    "1w": "KLINE_TYPE_WEEKLY",
    "1m": "KLINE_TYPE_MONTHLY",
}


class PytdxProvider(MarketDataProvider):
    name = "pytdx"

    def __init__(
        self,
        host: str | None = None,
        port: int = 7709,
        timeout: int = 10,
        retries: int = 3,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.retries = retries
        self._active_server: tuple[str, int] | None = None

    async def list_symbols(self, limit: int | None = None) -> list[SymbolInfo]:
        from pytdx.hq import TdxHq_API

        api = TdxHq_API()
        server = self._connect(api)
        if server is None:
            return await SeedProvider().list_symbols()
        try:
            symbols: dict[str, SymbolInfo] = {}
            for market, exchange in ((0, "SZ"), (1, "SH")):
                count = int(api.get_security_count(market) or 0)
                for start in range(0, count, 1000):
                    rows = api.get_security_list(market, start) or []
                    for item in rows:
                        code = str(item.get("code", "")).strip()
                        if not _is_a_share_code(exchange, code):
                            continue
                        name = str(item.get("name") or code).strip()
                        symbol = f"{code}.{exchange}"
                        symbols[symbol] = SymbolInfo(
                            symbol=symbol,
                            code=code,
                            exchange=exchange,
                            name=name,
                        )
                        if limit is not None and len(symbols) >= limit:
                            return [symbols[key] for key in sorted(symbols)]
            if not symbols:
                return await SeedProvider().list_symbols()
            return [symbols[key] for key in sorted(symbols)]
        finally:
            api.disconnect()

    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        bars = await self.get_bars_page(symbol, timeframe, offset=0, limit=limit)
        if start is not None:
            start_dt = _coerce_datetime(start)
            bars = [bar for bar in bars if bar.ts >= start_dt]
        if end is not None:
            end_dt = _coerce_datetime(end)
            bars = [bar for bar in bars if bar.ts <= end_dt]
        return bars

    async def get_bars_page(
        self,
        symbol: str,
        timeframe: str,
        *,
        offset: int,
        limit: int,
    ) -> list[Bar]:
        return await asyncio.to_thread(
            self._get_bars_page_sync,
            symbol,
            timeframe,
            offset,
            limit,
        )

    def _get_bars_page_sync(
        self,
        symbol: str,
        timeframe: str,
        offset: int,
        limit: int,
    ) -> list[Bar]:
        from pytdx.hq import TdxHq_API
        from pytdx.params import TDXParams

        normalized = normalize_timeframe(timeframe)
        category = getattr(TDXParams, TIMEFRAME_TO_TDX_CATEGORY[normalized])
        market, code = _split_tdx_symbol(symbol)
        api = TdxHq_API()
        server = self._connect(api)
        if server is None:
            raise RuntimeError(
                "No reachable pytdx quote server. Run "
                "`python -m collector.backfill --provider pytdx --probe-servers` "
                "to inspect connectivity, or pass --tdx-host."
            )
        try:
            raw = api.get_security_bars(category, market, code, offset, limit) or []
        finally:
            api.disconnect()

        return sorted(
            [
                _tdx_bar_to_bar(symbol=symbol, timeframe=normalized, item=item)
                for item in raw
            ],
            key=lambda bar: bar.ts,
        )

    async def healthcheck(self) -> ProviderHealth:
        try:
            from pytdx.hq import TdxHq_API
        except ImportError:
            return ProviderHealth(
                name=self.name,
                ok=False,
                message="pytdx package is not installed",
            )
        api = TdxHq_API()
        server = self._connect(api)
        api.disconnect()
        if server is None:
            return ProviderHealth(
                name=self.name,
                ok=False,
                message="No reachable pytdx quote server",
            )
        return ProviderHealth(
            name=self.name,
            ok=True,
            message=f"pytdx quote server reachable: {server[0]}:{server[1]}",
        )

    def _connect(self, api) -> tuple[str, int] | None:
        candidates: list[tuple[str, int]] = []
        if self.host:
            candidates.append((self.host, self.port))
        if self._active_server:
            candidates.append(self._active_server)
        candidates.extend(TDX_SERVERS)

        seen: set[tuple[str, int]] = set()
        for host, port in candidates:
            if (host, port) in seen:
                continue
            seen.add((host, port))
            for _attempt in range(self.retries):
                connected = False
                try:
                    connected = bool(api.connect(host, port, time_out=self.timeout))
                    if connected:
                        self._active_server = (host, port)
                        return self._active_server
                except Exception:
                    time.sleep(0.2)
                finally:
                    if not connected:
                        try:
                            api.disconnect()
                        except Exception:
                            pass
        return None

    async def probe_servers(self) -> list[dict]:
        from pytdx.hq import TdxHq_API

        candidates: list[tuple[str, int]] = []
        if self.host:
            candidates.append((self.host, self.port))
        else:
            candidates.extend(TDX_SERVERS)
        seen: set[tuple[str, int]] = set()
        results: list[dict] = []
        for host, port in candidates:
            if (host, port) in seen:
                continue
            seen.add((host, port))
            ok = False
            error = None
            for _attempt in range(self.retries):
                api = TdxHq_API()
                try:
                    ok = bool(api.connect(host, port, time_out=self.timeout))
                    if ok:
                        error = None
                        break
                except Exception as exc:
                    error = str(exc)
                    time.sleep(0.2)
                finally:
                    try:
                        api.disconnect()
                    except Exception:
                        pass
            results.append({"host": host, "port": port, "ok": ok, "error": error})
        return results


def _split_tdx_symbol(symbol: str) -> tuple[int, str]:
    normalized = symbol.strip().upper()
    if "." in normalized:
        code, exchange = normalized.split(".", 1)
    else:
        code = normalized
        exchange = "SH" if code.startswith("6") else "SZ"
    market = 1 if exchange == "SH" else 0
    return market, code


def _is_a_share_code(exchange: str, code: str) -> bool:
    if len(code) != 6 or not code.isdigit():
        return False
    if exchange == "SH":
        return code.startswith(("600", "601", "603", "605", "688"))
    if exchange == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    return False


def _tdx_bar_to_bar(symbol: str, timeframe: str, item: dict) -> Bar:
    ts = _parse_tdx_datetime(item)
    volume = int(float(item.get("vol") or item.get("volume") or 0))
    amount = item.get("amount")
    return Bar(
        symbol=symbol.strip().upper(),
        timeframe=timeframe,
        ts=ts,
        open=float(item["open"]),
        high=float(item["high"]),
        low=float(item["low"]),
        close=float(item["close"]),
        volume=volume,
        amount=None if amount is None else float(amount),
        complete=True,
        revision=0,
        source="pytdx",
    )


def _parse_tdx_datetime(item: dict) -> datetime:
    value = item.get("datetime")
    if isinstance(value, datetime):
        return _coerce_datetime(value)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=SHANGHAI_TZ)
            except ValueError:
                continue
    year = int(item.get("year"))
    month = int(item.get("month"))
    day = int(item.get("day"))
    hour = int(item.get("hour") or 0)
    minute = int(item.get("minute") or 0)
    return datetime(year, month, day, hour, minute, tzinfo=SHANGHAI_TZ)


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ)
