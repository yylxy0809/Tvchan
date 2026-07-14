from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

from .contracts import CapitalFlow, MarketDataResult, MarketLeaderDetail, MarketStrength, MarketThemeDetail, NewsItem, NewsSource, Profile, ProviderError, Quote, SidebarContext, Themes, Valuation
from .provider import UnifiedMarketDataProvider


NOTTE_FUNCTION_ID = "e4157137-02c9-4052-85c0-9ee5c2c91682"
NOTTE_SOURCE = "notte"


class NotteFunction(Protocol):
    def run(self, **variables: object) -> object: ...


@dataclass(frozen=True, slots=True)
class NotteConfig:
    api_key: str = field(repr=False)
    function_id: str = NOTTE_FUNCTION_ID
    news_limit: int = 20
    timeout_seconds: float = 30.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ, *, timeout_seconds: float = 30.0) -> NotteConfig:
        api_key = env.get("NOTTE_API_KEY", "").strip()
        if not api_key:
            raise ValueError("NOTTE_API_KEY is required")
        if timeout_seconds <= 0:
            raise ValueError("Notte timeout must be positive")
        function_id = env.get("NOTTE_FUNCTION_ID", NOTTE_FUNCTION_ID).strip()
        if not function_id:
            raise ValueError("NOTTE_FUNCTION_ID is required")
        configured_timeout = env.get("NOTTE_TIMEOUT_SECONDS", "").strip()
        if configured_timeout:
            try:
                timeout_seconds = float(configured_timeout)
            except ValueError as exc:
                raise ValueError("NOTTE_TIMEOUT_SECONDS must be a number") from exc
        return cls(api_key=api_key, function_id=function_id, timeout_seconds=timeout_seconds)


class NotteSidebarProvider(UnifiedMarketDataProvider):
    """Adapter for the deployed Notte sidebar Function."""

    def __init__(self, config: NotteConfig, function: NotteFunction | None = None, *, today: Callable[[], date] | None = None, monotonic: Callable[[], float] = time.monotonic):
        self._config = config
        _silence_notte_logs()
        try:
            self._function = function or _create_function(config.api_key, config.function_id)
        except Exception:
            raise RuntimeError("unable to initialize Notte Function") from None
        self._today = today or date.today
        self._monotonic = monotonic
        self._context: ContextVar[SidebarContext | None] = ContextVar("notte_sidebar_context", default=None)
        self._bundles: dict[tuple[date, str, tuple[str, ...]], tuple[Mapping[str, object], date, datetime | None]] = {}
        self._flights: dict[tuple[date, str, tuple[str, ...]], asyncio.Task] = {}
        self._failures: dict[tuple[date, str, tuple[str, ...]], tuple[ProviderError, float]] = {}
        self._last_key: tuple[date, str, tuple[str, ...]] | None = None

    async def prepare_context(self, context: SidebarContext) -> None:
        self._context.set(context)

    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]:
        symbols = tuple(dict.fromkeys(symbols))
        try:
            result, trading_date, provider_ts = await self._bundle(symbols[0] if symbols else "", symbols)
            quotes = {_text(item, "symbol"): item for item in _quote_records(_section(result, "quotes", "watchlist_quotes")) if _text(item, "symbol")}
            return {symbol: _available(_quote(symbol, quotes.get(symbol)), trading_date, provider_ts) for symbol in symbols}
        except TimeoutError:
            return {symbol: _unavailable(self._today(), ProviderError.TIMEOUT) for symbol in symbols}
        except Exception:
            return {symbol: _unavailable(self._today()) for symbol in symbols}

    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]:
        return await self._one(symbol, ("profile", "active_symbol_profile"), lambda item: Profile(symbol, _profile_text(item, "name"), _profile_text(item, "exchange"), _profile_text(item, "industry"), _profile_text(item, "description", "business_summary"), _number(item, "market_cap"), _number(item, "pe_ratio"), _number(item, "pb_ratio"), _number(item, "turnover_rate")))

    async def get_valuation(self, symbol: str) -> MarketDataResult[Valuation]:
        return await self._one(symbol, ("valuation",), lambda item: Valuation(symbol, _number(item, "market_cap"), _number_any(item, "pe_ratio", "pe_ttm"), _number_any(item, "pb_ratio", "pb"), _number_any(item, "ps_ratio", "ps_ttm")))

    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]:
        return await self._one(symbol, ("capital_flow",), lambda item: CapitalFlow(symbol, _number(item, "net_inflow"), _number(item, "main_net_inflow"), _number(item, "large_net_inflow"), _number(item, "medium_net_inflow"), _number(item, "small_net_inflow")))

    async def get_themes(self, symbol: str) -> MarketDataResult[Themes]:
        return await self._one(symbol, ("themes",), lambda item: _themes(symbol, item))

    async def get_market_strength(self) -> MarketDataResult[MarketStrength]:
        try:
            result, trading_date, provider_ts = await self._current_bundle()
            item = _mapping(_section(result, "market_strength", "strength"))
            leader_details = _leader_details(item.get("leaders"))
            theme_details = _theme_details(result.get("themes")) or _theme_details(item.get("themes"))
            leaders = _strings(item.get("leaders")) or tuple(detail.name for detail in leader_details)
            themes = _strings(item.get("themes")) or tuple(detail.name for detail in theme_details)
            value = MarketStrength(_number(item, "score"), leaders, themes, _integer(item, "up_count"), _integer(item, "down_count"), _integer(item, "limit_up_count"), _integer(item, "limit_down_count"), _number(item, "index_level"), _number(item, "index_change_percent"), leader_details, theme_details)
            return _available(value, trading_date, provider_ts) if _meaningful(value) else _unavailable(trading_date)
        except TimeoutError:
            return _unavailable(self._today(), ProviderError.TIMEOUT)
        except Exception:
            return _unavailable(self._today())

    async def get_news(self, symbol: str, since: datetime | None = None) -> MarketDataResult[tuple[NewsItem, ...]]:
        try:
            result, trading_date, provider_ts = await self._current_bundle(symbol)
            first_seen = provider_ts or datetime.now().astimezone()
            items = tuple(item for raw in _news_records(_section(result, "news")) if (item := _news_item(raw, symbol, first_seen)) is not None and (since is None or item.published_at >= since))
            return _available(items, trading_date, provider_ts)
        except TimeoutError:
            return _unavailable(self._today(), ProviderError.TIMEOUT)
        except Exception:
            return _unavailable(self._today())

    async def _one(self, symbol: str, names: tuple[str, ...], make: Callable[[Mapping[str, object]], object]) -> MarketDataResult[Any]:
        try:
            result, trading_date, provider_ts = await self._current_bundle(symbol)
            value = make(_mapping(_section(result, *names)))
            return _available(value, trading_date, provider_ts) if _meaningful(value) else _unavailable(trading_date)
        except TimeoutError:
            return _unavailable(self._today(), ProviderError.TIMEOUT)
        except Exception:
            return _unavailable(self._today())

    async def _current_bundle(self, symbol: str = "") -> tuple[Mapping[str, object], date, datetime | None]:
        context = self._context.get()
        if context is None and not symbol and self._last_key is not None:
            cached = self._bundles.get(self._last_key)
            if cached is not None:
                return cached
        chart_symbol = context.chart_symbol if context is not None else symbol
        watchlist = (chart_symbol, *(context.watchlist_symbols if context is not None else ()))
        return await self._bundle(chart_symbol, tuple(item for item in watchlist if item))

    async def _bundle(self, chart_symbol: str, watchlist_symbols: Sequence[str]) -> tuple[Mapping[str, object], date, datetime | None]:
        symbols = tuple(dict.fromkeys(watchlist_symbols))
        key = (self._today(), chart_symbol, symbols)
        cached = self._bundles.get(key)
        if cached is not None:
            self._last_key = key
            return cached
        failed = self._failures.get(key)
        if failed is not None and self._monotonic() < failed[1]:
            if failed[0] is ProviderError.TIMEOUT:
                raise TimeoutError("Notte context is in timeout cooldown")
            raise RuntimeError("Notte context is unavailable")
        if failed is not None:
            self._failures.pop(key, None)
        task = self._flights.get(key)
        if task is None:
            task = asyncio.create_task(self._run_bundle(chart_symbol, symbols))
            self._flights[key] = task
        try:
            bundle = await asyncio.shield(task)
            self._bundles[key] = bundle
            self._last_key = key
            return bundle
        except TimeoutError:
            self._failures[key] = (ProviderError.TIMEOUT, self._monotonic() + 300)
            raise
        except Exception:
            self._failures[key] = (ProviderError.UNAVAILABLE, self._monotonic() + 300)
            raise RuntimeError("Notte Function call failed") from None
        finally:
            if task.done() and self._flights.get(key) is task:
                self._flights.pop(key, None)

    async def _run_bundle(self, chart_symbol: str, watchlist_symbols: Sequence[str]) -> tuple[Mapping[str, object], date, datetime | None]:
        _silence_notte_logs()
        response = await asyncio.wait_for(asyncio.to_thread(
            self._function.run,
            request_id=str(uuid4()),
            chart_symbol=chart_symbol,
            watchlist_symbols=",".join(watchlist_symbols),
            sections="quotes,profile,valuation,capital_flow,themes,market_strength,news",
            news_limit=self._config.news_limit,
        ), timeout=self._config.timeout_seconds)
        result = _mapping(response if isinstance(response, Mapping) else getattr(response, "result"))
        provider_ts = _datetime(result.get("provider_ts") or result.get("as_of") or result.get("generated_at"))
        return result, _trading_date(result.get("trading_date"), provider_ts, self._today()), provider_ts


def _create_function(api_key: str, function_id: str) -> NotteFunction:
    from notte_sdk import NotteClient

    _silence_notte_logs()
    return NotteClient(api_key=api_key).Function(function_id)


class _DiscardNotteLog(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return False


_DISCARD_NOTTE_LOG = _DiscardNotteLog()


def _silence_notte_logs() -> None:
    """Notte's live-viewer logs can contain authenticated URLs."""
    try:
        from loguru import logger as loguru_logger

        loguru_logger.configure(handlers=[{
            "sink": sys.stderr,
            "level": "WARNING",
            "filter": lambda record: record["extra"].get("package") != "notte",
        }])
    except ImportError:
        pass
    manager = logging.Logger.manager.loggerDict
    names = ("notte", "notte_sdk")
    for name in names:
        logger = logging.getLogger(name)
        logger.disabled = True
        logger.propagate = False
        logger.addFilter(_DISCARD_NOTTE_LOG)
    for name, logger in manager.items():
        if isinstance(logger, logging.Logger) and (name == "notte" or name.startswith("notte.") or name == "notte_sdk" or name.startswith("notte_sdk.")):
            logger.disabled = True
            logger.propagate = False
            logger.addFilter(_DISCARD_NOTTE_LOG)


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise ValueError("Notte Function result must be an object")
    return value


def _section(result: Mapping[str, object], *names: str) -> object:
    for name in names:
        if name in result:
            return result[name]
    raise ValueError("Notte Function result is missing requested section")


def _records(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, Mapping):
        return (value,)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("Notte Function section must be an array")
    return tuple(_mapping(item) for item in value)


def _quote_records(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, Mapping) and "symbol" not in value:
        return tuple(_mapping(item) for item in value.values())
    return _records(value)


def _news_records(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, Mapping) and "items" in value:
        return _records(value["items"])
    return _records(value)


def _trading_date(value: object, provider_ts: datetime | None, fallback: date) -> date:
    if value is None:
        return provider_ts.date() if provider_ts else fallback
    if not isinstance(value, str):
        raise ValueError("trading_date must be an ISO date")
    return date.fromisoformat(value)


def _datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO datetime")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return parsed


def _text(item: Mapping[str, object], *names: str) -> str | None:
    for name in names:
        value = item.get(name)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
    return None


def _profile_text(item: Mapping[str, object], *names: str) -> str | None:
    value = _text(item, *names)
    return None if value and ("<" in value or ">" in value) else value


def _number(item: Mapping[str, object], name: str) -> float | None:
    value = item.get(name)
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _number_any(item: Mapping[str, object], *names: str) -> float | None:
    for name in names:
        value = _number(item, name)
        if value is not None:
            return value
    return None


def _integer(item: Mapping[str, object], name: str) -> int | None:
    value = _number(item, name)
    return int(value) if value is not None and value.is_integer() else None


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(dict.fromkeys(item.strip() for item in value if isinstance(item, str) and item.strip()))


def _leader_details(value: object) -> tuple[MarketLeaderDetail, ...]:
    return tuple(
        MarketLeaderDetail(name, _number(item, "change_percent"))
        for item in _records_or_empty(value)
        if (name := _text(item, "name", "symbol"))
    )


def _theme_details(value: object) -> tuple[MarketThemeDetail, ...]:
    details = []
    for item in _records_or_empty(value):
        name = _text(item, "name")
        if not name:
            continue
        inflow_wan = _number(item, "main_net_inflow_wan")
        if inflow_wan is None:
            inflow = _number(item, "main_net_inflow")
            inflow_wan = inflow / 10_000 if inflow is not None else None
        details.append(MarketThemeDetail(name, _number(item, "change_percent"), inflow_wan))
    return tuple(details)


def _quote(symbol: str, item: Mapping[str, object] | None) -> Quote | None:
    if item is None:
        return None
    return Quote(symbol, _number(item, "price"), _number(item, "change"), _number(item, "change_percent"), _number(item, "volume"), _number(item, "amount"), _number(item, "turnover_rate"))


def _themes(symbol: str, item: Mapping[str, object]) -> Themes:
    item_symbol = (_text(item, "symbol", "code") or "").upper()
    if item_symbol != symbol.upper():
        return Themes(symbol)
    return Themes(symbol, _text(item, "industry"), _strings(item.get("concepts")), _profile_text(item, "business_summary"))


def _news_item(item: Mapping[str, object], symbol: str, fallback_first_seen: datetime) -> NewsItem | None:
    event_id, title, published_at = _text(item, "id", "event_id"), _text(item, "title"), _datetime(item.get("published_at"))
    if not event_id or not title or published_at is None:
        return None
    first_seen = _datetime(item.get("first_seen_at") or item.get("fetched_at")) or fallback_first_seen
    sources = tuple(NewsSource(name, url) for source in _records_or_empty(item.get("sources")) if (name := _text(source, "name", "source_name")) and (url := _safe_url(_text(source, "url"))))
    if not sources:
        name = _text(item, "source_name", "source")
        url = _safe_url(_text(item, "url"))
        if name and url:
            sources = (NewsSource(name, url),)
    return NewsItem(event_id, symbol, _text(item, "category") or "news", title, _text(item, "fact_summary", "summary") or title, published_at, first_seen, source=NOTTE_SOURCE, sources=sources, impact_tags=_strings(item.get("impact_tags")))


def _records_or_empty(value: object) -> tuple[Mapping[str, object], ...]:
    try:
        return _records(value)
    except ValueError:
        return ()


def _safe_url(value: str | None) -> str | None:
    if value and urlsplit(value).scheme in {"http", "https"}:
        return value
    return None


def _available(value: Any, trading_date: date, provider_ts: datetime | None) -> MarketDataResult[Any]:
    return MarketDataResult.available(value, trading_date=trading_date, provider_ts=provider_ts, source=NOTTE_SOURCE)


def _unavailable(trading_date: date, error: ProviderError = ProviderError.SCHEMA) -> MarketDataResult[Any]:
    return MarketDataResult.unavailable(error=error, trading_date=trading_date, source=NOTTE_SOURCE)


def _meaningful(value: object) -> bool:
    fields = getattr(value, "__dataclass_fields__", {})
    for name in fields:
        if name == "symbol":
            continue
        item = getattr(value, name)
        if item not in (None, "", (), []):
            return True
    return False
