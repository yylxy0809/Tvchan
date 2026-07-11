from __future__ import annotations

import time
import asyncio
import os
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from collector.providers.seed import SeedProvider
from trading_protocol import Bar, SymbolInfo, canonical_kline_timestamp, normalize_timeframe

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class PytdxLunchReopenRowError(ValueError):
    def __init__(self, reason: str, *, symbol: str, timeframe: str, timestamp: datetime) -> None:
        self.reason = reason
        super().__init__(
            f"pytdx_lunch_reopen:{reason}:symbol={symbol}:timeframe={timeframe}:ts={timestamp.isoformat()}"
        )

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
        self._api = None
        self._api_lock = threading.Lock()

    async def list_symbols(self, limit: int | None = None) -> list[SymbolInfo]:
        from pytdx.hq import TdxHq_API

        api = TdxHq_API()
        server = self._connect(api)
        if server is None:
            return await _seed_symbols_if_explicitly_enabled(
                "No reachable pytdx quote server for symbol discovery"
            )
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
                return await _seed_symbols_if_explicitly_enabled(
                    "pytdx returned no A-share symbols"
                )
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
        with self._api_lock:
            raw = None
            last_error: Exception | None = None
            for _attempt in range(max(1, self.retries + 1)):
                api = self._get_connected_api(TdxHq_API)
                if api is None:
                    break
                try:
                    raw = api.get_security_bars(category, market, code, offset, limit) or []
                    break
                except Exception as exc:
                    last_error = exc
                    self._disconnect_cached_api()
                    time.sleep(0.2)
            if raw is None:
                raise RuntimeError(
                    "No reachable pytdx quote server. Run "
                    "`python -m collector.backfill --provider pytdx --probe-servers` "
                    "to inspect connectivity, or pass --tdx-host."
                ) from last_error

        return _tdx_rows_to_bars(symbol, normalized, raw)

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
            candidates.extend(_parse_tdx_servers(self.host, self.port))
        elif self._active_server:
            candidates.append(self._active_server)
            candidates.extend(TDX_SERVERS)
        else:
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

    def _get_connected_api(self, api_factory):
        if self._api is not None:
            return self._api

        api = api_factory()
        server = self._connect(api)
        if server is None:
            try:
                api.disconnect()
            except Exception:
                pass
            return None
        self._api = api
        return self._api

    def _disconnect_cached_api(self) -> None:
        if self._api is None:
            return
        try:
            self._api.disconnect()
        except Exception:
            pass
        self._api = None

    async def probe_servers(self) -> list[dict]:
        from pytdx.hq import TdxHq_API

        candidates: list[tuple[str, int]] = []
        if self.host:
            candidates.extend(_parse_tdx_servers(self.host, self.port))
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


def _parse_tdx_servers(hosts: str, default_port: int) -> list[tuple[str, int]]:
    candidates: list[tuple[str, int]] = []
    for raw in hosts.split(","):
        item = raw.strip()
        if not item:
            continue
        if ":" in item:
            host, port_text = item.rsplit(":", 1)
            try:
                port = int(port_text)
            except ValueError:
                port = default_port
        else:
            host = item
            port = default_port
        candidates.append((host.strip(), port))
    return candidates


def _is_a_share_code(exchange: str, code: str) -> bool:
    if len(code) != 6 or not code.isdigit():
        return False
    if exchange == "SH":
        return code.startswith(("600", "601", "603", "605", "688"))
    if exchange == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    return False


async def _seed_symbols_if_explicitly_enabled(reason: str) -> list[SymbolInfo]:
    if os.getenv("PYTDX_ALLOW_SEED_SYMBOL_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return await SeedProvider().list_symbols()
    raise RuntimeError(f"{reason}; seed fallback is disabled in production")


def _tdx_bar_to_bar(symbol: str, timeframe: str, item: dict) -> Bar:
    ts = _parse_tdx_datetime(item, timeframe)
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


def _tdx_rows_to_bars(symbol: str, timeframe: str, items: list[dict]) -> list[Bar]:
    normalized = normalize_timeframe(timeframe)
    if normalized not in {"5f", "15f", "30f", "1h"}:
        return sorted(
            [_tdx_bar_to_bar(symbol, normalized, item) for item in items],
            key=lambda bar: bar.ts,
        )

    parsed_rows = [(item, _parse_tdx_raw_datetime(item)) for item in items]
    morning_closes = {
        timestamp.date(): item
        for item, timestamp in parsed_rows
        if (timestamp.hour, timestamp.minute) == (11, 30)
    }
    retained: list[dict] = []
    for item, timestamp in parsed_rows:
        if (timestamp.hour, timestamp.minute) != (13, 0):
            retained.append(item)
            continue
        comparator = morning_closes.get(timestamp.date())
        if comparator is None:
            raise PytdxLunchReopenRowError(
                "missing_1130_comparator",
                symbol=symbol,
                timeframe=normalized,
                timestamp=timestamp,
            )
        mismatch_reason = _lunch_reopen_mismatch_reason(comparator, item)
        if mismatch_reason is not None:
            raise PytdxLunchReopenRowError(
                mismatch_reason,
                symbol=symbol,
                timeframe=normalized,
                timestamp=timestamp,
            )

    return sorted(
        [_tdx_bar_to_bar(symbol, normalized, item) for item in retained],
        key=lambda bar: bar.ts,
    )


def _lunch_reopen_mismatch_reason(morning: dict, lunch: dict) -> str | None:
    for field in ("open", "high", "low", "close"):
        if float(morning[field]) != float(lunch[field]):
            return "mismatched_ohlcv"
    morning_volume = morning.get("vol") if morning.get("vol") is not None else morning.get("volume", 0)
    lunch_volume = lunch.get("vol") if lunch.get("vol") is not None else lunch.get("volume", 0)
    if float(morning_volume or 0) != float(lunch_volume or 0):
        return "mismatched_ohlcv"
    morning_amount = morning.get("amount")
    lunch_amount = lunch.get("amount")
    if (morning_amount is None) != (lunch_amount is None):
        return "amount_mismatch"
    if morning_amount is not None and float(morning_amount) != float(lunch_amount):
        return "amount_mismatch"
    return None


def _parse_tdx_datetime(item: dict, timeframe: str) -> datetime:
    parsed = _parse_tdx_raw_datetime(item)
    return canonical_kline_timestamp(
        timeframe,
        parsed,
        date_only=(parsed.hour, parsed.minute, parsed.second, parsed.microsecond) == (0, 0, 0, 0),
    )


def _parse_tdx_raw_datetime(item: dict) -> datetime:
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
