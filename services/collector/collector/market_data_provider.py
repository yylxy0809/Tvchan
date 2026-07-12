from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import signal
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol

from .market_data import MarketDataResult, UnifiedMarketDataProvider


_LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MarketDemand:
    active_symbols: tuple[str, ...]
    watchlist_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "active_symbols",
            tuple(dict.fromkeys(symbol.strip().upper() for symbol in self.active_symbols if symbol.strip())),
        )
        object.__setattr__(
            self,
            "watchlist_symbols",
            tuple(dict.fromkeys(symbol.strip().upper() for symbol in self.watchlist_symbols if symbol.strip())),
        )


class DemandRepository(Protocol):
    async def get_demand(self) -> MarketDemand: ...


class StaticDemandRepository:
    def __init__(self, demand: MarketDemand) -> None:
        self._demand = demand

    async def get_demand(self) -> MarketDemand:
        return self._demand


class RedisDemandRepository:
    def __init__(
        self,
        redis: Any,
        *,
        max_contexts: int = 1000,
        scan_count: int = 100,
        max_pages: int = 20,
        timeout_seconds: float = 0.25,
        max_context_bytes: int = 16_384,
        max_watchlist_symbols: int = 100,
        max_symbol_length: int = 32,
    ) -> None:
        if min(max_contexts, scan_count, max_pages, max_context_bytes, max_watchlist_symbols, max_symbol_length) <= 0 or timeout_seconds <= 0:
            raise ValueError("Redis demand scan limits must be positive")
        self._redis = redis
        self._max_contexts = max_contexts
        self._scan_count = scan_count
        self._max_pages = max_pages
        self._timeout_seconds = timeout_seconds
        self._max_context_bytes = max_context_bytes
        self._max_watchlist_symbols = max_watchlist_symbols
        self._max_symbol_length = max_symbol_length

    async def get_demand(self) -> MarketDemand:
        cursor: int | str = 0
        keys: list[str] = []
        started = time.monotonic()
        pages = 0
        while True:
            elapsed = time.monotonic() - started
            if pages >= self._max_pages or elapsed >= self._timeout_seconds:
                _LOG.warning(
                    "market_data_demand_scan_truncated",
                    extra={"event": "demand_scan_truncated", "reason": "page_budget" if pages >= self._max_pages else "time_budget", "pages": pages, "contexts": len(keys)},
                )
                break
            try:
                async with asyncio.timeout(self._timeout_seconds - elapsed):
                    cursor, page = await self._redis.scan(
                        cursor=cursor, match="market:sidebar:demand:*", count=self._scan_count
                    )
            except TimeoutError:
                _LOG.warning(
                    "market_data_demand_scan_truncated",
                    extra={"event": "demand_scan_truncated", "reason": "time_budget", "pages": pages, "contexts": len(keys)},
                )
                break
            pages += 1
            remaining = self._max_contexts - len(keys)
            keys.extend(page[:remaining])
            if not cursor or len(keys) >= self._max_contexts:
                if len(keys) >= self._max_contexts and cursor:
                    _LOG.warning(
                        "market_data_demand_scan_truncated",
                        extra={"event": "demand_scan_truncated", "reason": "context_budget", "pages": pages, "contexts": len(keys)},
                    )
                break
        if not keys:
            return MarketDemand(())
        records = await self._redis.mget(keys)
        active: list[str] = []
        watchlist: list[str] = []
        for raw in records:
            if isinstance(raw, bytes):
                if len(raw) > self._max_context_bytes:
                    self._log_context_truncation("record_size")
                    continue
                try:
                    raw = raw.decode("utf-8")
                except UnicodeDecodeError:
                    continue
            if not isinstance(raw, str) or len(raw.encode("utf-8")) > self._max_context_bytes:
                self._log_context_truncation("record_size")
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(value, dict):
                continue
            chart_symbol = value.get("chart_symbol")
            symbols = value.get("watchlist_symbols")
            updated_at = value.get("updated_at")
            if not isinstance(chart_symbol, str) or not 0 < len(chart_symbol) <= self._max_symbol_length or not chart_symbol.strip():
                continue
            if not isinstance(symbols, list):
                continue
            if len(symbols) > self._max_watchlist_symbols:
                self._log_context_truncation("watchlist_budget")
                symbols = symbols[:self._max_watchlist_symbols]
            if not all(isinstance(symbol, str) and 0 < len(symbol) <= self._max_symbol_length and symbol.strip() for symbol in symbols):
                continue
            if not isinstance(updated_at, str) or not updated_at:
                continue
            active.append(chart_symbol)
            watchlist.extend(symbols)
        return MarketDemand(tuple(active), tuple(watchlist))

    @staticmethod
    def _log_context_truncation(reason: str) -> None:
        _LOG.warning("market_data_demand_context_truncated", extra={"event": "demand_context_truncated", "reason": reason})


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    interval_seconds: float = 5.0
    timeout_seconds: float = 12.0
    max_symbols: int = 200
    max_provider_concurrency: int = 16
    max_structured_symbols_per_refresh: int = 2

    def __post_init__(self) -> None:
        if min(
            self.interval_seconds,
            self.timeout_seconds,
            self.max_symbols,
            self.max_provider_concurrency,
            self.max_structured_symbols_per_refresh,
        ) <= 0:
            raise ValueError("runtime intervals must be positive")


@dataclass(frozen=True, slots=True)
class ProcessConfig:
    provider_factory: str
    demand_factory: str | None
    demand: MarketDemand | None
    redis_url: str
    runtime: RuntimeConfig
    max_contexts: int
    max_demand_scan_pages: int
    demand_scan_timeout_seconds: float
    max_demand_context_bytes: int
    max_demand_watchlist_symbols: int
    max_demand_symbol_length: int


_TTLS = {"quote": 30, "profile": 86_400, "finance": 86_400, "fund": 600, "strength": 120, "news": 86_400}
_NEWS_REFRESH_SUCCESS_SECONDS = 900.0
_NEWS_REFRESH_FAILURE_SECONDS = 60.0
_NEWS_REFRESH_AUTH_FAILURE_SECONDS = 3600.0


class MarketDataProviderRuntime:
    def __init__(self, provider: UnifiedMarketDataProvider, demand_repository: DemandRepository, redis: Any, config: RuntimeConfig) -> None:
        self._provider = provider
        self._demand_repository = demand_repository
        self._redis = redis
        self._config = config
        self._structured_cursor = 0
        self._news_retry_at: dict[str, float] = {}

    async def refresh_once(self) -> bool:
        try:
            async with asyncio.timeout(self._config.timeout_seconds):
                demand = await self._demand_repository.get_demand()
                if not demand.active_symbols:
                    return False
                demand = self._bound_demand(demand)
                symbols = tuple(dict.fromkeys((*demand.active_symbols, *demand.watchlist_symbols)))
                structured_symbols = self._next_structured_symbols(demand.active_symbols)
                monotonic_now = time.monotonic()
                news_symbols = tuple(
                    symbol for symbol in structured_symbols
                    if monotonic_now >= self._news_retry_at.get(symbol, 0.0)
                )
                limiter = asyncio.Semaphore(self._config.max_provider_concurrency)
                quotes, profiles, funds, strength, due_news_items = await asyncio.gather(
                    self._limited(limiter, self._provider.get_quotes, symbols),
                    self._bounded_map(structured_symbols, limiter, self._provider.get_profile),
                    self._bounded_map(structured_symbols, limiter, self._provider.get_capital_flow),
                    self._limited(limiter, self._provider.get_market_strength),
                    self._bounded_map(news_symbols, limiter, self._provider.get_news),
                )
                news_by_symbol = dict(zip(news_symbols, due_news_items, strict=True))
                for symbol, result in news_by_symbol.items():
                    delay = (
                        _NEWS_REFRESH_SUCCESS_SECONDS
                        if _available(result)
                        else _NEWS_REFRESH_AUTH_FAILURE_SECONDS
                        if getattr(result, "error", None) == "authentication"
                        else _NEWS_REFRESH_FAILURE_SECONDS
                    )
                    self._news_retry_at[symbol] = monotonic_now + delay
                news_items = [news_by_symbol.get(symbol) for symbol in structured_symbols]
                snapshots = self._build_snapshots(
                    demand, structured_symbols, quotes, profiles, funds, strength, news_items
                )
                if not snapshots:
                    return False
                pipeline = self._redis.pipeline(transaction=True)
                for key, domain, payload in snapshots:
                    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=_json_default)
                    pipeline.set(key, encoded, ex=_TTLS[domain])
                    pipeline.publish(key, encoded)
                await pipeline.execute()
                return True
        except Exception as exc:
            _LOG.warning(
                "market_data_refresh_failed",
                extra={"event": "refresh_failed", "error_type": type(exc).__name__},
            )
            return False

    def _bound_demand(self, demand: MarketDemand) -> MarketDemand:
        symbols = tuple(dict.fromkeys((*demand.active_symbols, *demand.watchlist_symbols)))
        if len(symbols) <= self._config.max_symbols:
            return demand
        retained = set(symbols[:self._config.max_symbols])
        active = tuple(symbol for symbol in demand.active_symbols if symbol in retained)
        watchlist = tuple(symbol for symbol in demand.watchlist_symbols if symbol in retained)
        _LOG.warning(
            "market_data_demand_truncated",
            extra={"event": "demand_truncated", "requested_symbols": len(symbols), "retained_symbols": len(retained)},
        )
        return MarketDemand(active, watchlist)

    def _next_structured_symbols(self, symbols: tuple[str, ...]) -> tuple[str, ...]:
        if len(symbols) <= self._config.max_structured_symbols_per_refresh:
            self._structured_cursor = 0
            return symbols
        start = self._structured_cursor % len(symbols)
        count = self._config.max_structured_symbols_per_refresh
        selected = tuple(symbols[(start + offset) % len(symbols)] for offset in range(count))
        self._structured_cursor = (start + count) % len(symbols)
        return selected

    async def _limited(self, limiter: asyncio.Semaphore, call, *args):
        async with limiter:
            return await call(*args)

    async def _bounded_map(self, symbols, limiter: asyncio.Semaphore, call):
        results: list[Any] = [None] * len(symbols)
        next_index = 0

        async def worker() -> None:
            nonlocal next_index
            while next_index < len(symbols):
                index = next_index
                next_index += 1
                results[index] = await self._limited(limiter, call, symbols[index])

        await asyncio.gather(*(worker() for _ in range(min(self._config.max_provider_concurrency, len(symbols)))))
        return results

    def _build_snapshots(self, demand, structured_symbols, quotes, profiles, funds, strength, news_items):
        snapshots: list[tuple[str, str, dict[str, Any]]] = []
        for symbol in tuple(dict.fromkeys((*demand.active_symbols, *demand.watchlist_symbols))):
            result = quotes.get(symbol)
            if _available(result):
                snapshots.append((f"market:quote:{symbol}", "quote", _result_payload(result)))
        for symbol, profile, fund, news in zip(structured_symbols, profiles, funds, news_items, strict=True):
            if _available(profile):
                base = _result_payload(profile)
                identity = {key: value for key, value in base.items() if key not in {"market_cap", "pe_ratio", "pb_ratio", "turnover_rate"}}
                finance = {key: base.get(key) for key in ("symbol", "market_cap", "pe_ratio", "pb_ratio", "turnover_rate")}
                finance.update(_metadata_payload(profile))
                snapshots.extend(((f"market:profile:{symbol}", "profile", identity), (f"market:finance:{symbol}", "finance", finance)))
            if _available(fund):
                snapshots.append((f"market:fund:{symbol}", "fund", _result_payload(fund)))
            if _available(news):
                payload = _metadata_payload(news)
                payload["items"] = [_to_json(item) for item in news.value]
                snapshots.append((f"market:news:{symbol}", "news", payload))
        if _available(strength):
            snapshots.append(("market:strength:latest", "strength", _result_payload(strength)))
        return snapshots

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.refresh_once()
            if stop.is_set():
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._config.interval_seconds)
            except TimeoutError:
                pass


def config_from_env(env: Mapping[str, str] = os.environ) -> ProcessConfig:
    provider_factory = env.get("MARKET_DATA_PROVIDER_FACTORY", "").strip()
    if not provider_factory:
        raise ValueError("MARKET_DATA_PROVIDER_FACTORY is required")
    demand_factory = env.get("MARKET_DATA_DEMAND_REPOSITORY_FACTORY", "").strip() or None
    active = env.get("MARKET_DATA_ACTIVE_SYMBOL", "").strip()
    demand = None
    if active:
        watchlist = tuple(value.strip() for value in env.get("MARKET_DATA_WATCHLIST", "").split(",") if value.strip())
        demand = MarketDemand((active,), watchlist)
    max_contexts = int(env.get("MARKET_DATA_MAX_CONTEXTS", "1000"))
    if max_contexts <= 0:
        raise ValueError("MARKET_DATA_MAX_CONTEXTS must be positive")
    max_demand_scan_pages = int(env.get("MARKET_DATA_MAX_DEMAND_SCAN_PAGES", "20"))
    demand_scan_timeout_seconds = float(env.get("MARKET_DATA_DEMAND_SCAN_TIMEOUT_SECONDS", "0.25"))
    if max_demand_scan_pages <= 0 or demand_scan_timeout_seconds <= 0:
        raise ValueError("MARKET_DATA demand scan limits must be positive")
    max_demand_context_bytes = int(env.get("MARKET_DATA_MAX_DEMAND_CONTEXT_BYTES", "16384"))
    max_demand_watchlist_symbols = int(env.get("MARKET_DATA_MAX_DEMAND_WATCHLIST_SYMBOLS", "100"))
    max_demand_symbol_length = int(env.get("MARKET_DATA_MAX_DEMAND_SYMBOL_LENGTH", "32"))
    if min(max_demand_context_bytes, max_demand_watchlist_symbols, max_demand_symbol_length) <= 0:
        raise ValueError("MARKET_DATA demand record limits must be positive")
    return ProcessConfig(
        provider_factory, demand_factory, demand,
        env.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
        RuntimeConfig(
            float(env.get("MARKET_DATA_INTERVAL_SECONDS", "5")),
            float(env.get("MARKET_DATA_TIMEOUT_SECONDS", "12")),
            int(env.get("MARKET_DATA_MAX_SYMBOLS", "200")),
            int(env.get("MARKET_DATA_MAX_PROVIDER_CONCURRENCY", "16")),
            int(env.get("MARKET_DATA_MAX_STRUCTURED_SYMBOLS_PER_REFRESH", "2")),
        ),
        max_contexts,
        max_demand_scan_pages,
        demand_scan_timeout_seconds,
        max_demand_context_bytes,
        max_demand_watchlist_symbols,
        max_demand_symbol_length,
    )


def _load_factory(path: str):
    module_name, separator, name = path.partition(":")
    if not separator or not module_name or not name:
        raise ValueError("factory must use module:callable syntax")
    factory = getattr(importlib.import_module(module_name), name)
    if not callable(factory):
        raise TypeError("configured factory is not callable")
    return factory


async def _main() -> None:
    config = config_from_env()
    provider = _load_factory(config.provider_factory)()
    import redis.asyncio as redis
    client = redis.from_url(config.redis_url, decode_responses=True)
    if config.demand_factory:
        repository = _load_factory(config.demand_factory)()
    elif config.demand:
        repository = StaticDemandRepository(config.demand)
    else:
        repository = RedisDemandRepository(
            client,
            max_contexts=config.max_contexts,
            max_pages=config.max_demand_scan_pages,
            timeout_seconds=config.demand_scan_timeout_seconds,
            max_context_bytes=config.max_demand_context_bytes,
            max_watchlist_symbols=config.max_demand_watchlist_symbols,
            max_symbol_length=config.max_demand_symbol_length,
        )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, stop.set)
        except NotImplementedError:
            signal.signal(name, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        await MarketDataProviderRuntime(provider, repository, client, config.runtime).run(stop)
    finally:
        close = getattr(provider, "close", None)
        if close:
            result = close()
            if asyncio.iscoroutine(result):
                await result
        await client.aclose()


def _available(result: MarketDataResult[Any] | None) -> bool:
    return result is not None and result.value is not None


def _metadata_payload(result: MarketDataResult[Any]) -> dict[str, Any]:
    metadata = result.metadata
    return {"source": metadata.source, "provider_version": metadata.provider_version, "provider_ts": metadata.provider_ts, "received_at": metadata.received_at, "freshness": metadata.freshness, "error": result.error}


def _result_payload(result: MarketDataResult[Any]) -> dict[str, Any]:
    payload = _to_json(result.value)
    payload.update(_metadata_payload(result))
    return payload


def _to_json(value: Any) -> Any:
    return asdict(value) if is_dataclass(value) else value


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, Enum)):
        return value.isoformat() if isinstance(value, datetime) else value.value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


if __name__ == "__main__":
    asyncio.run(_main())
