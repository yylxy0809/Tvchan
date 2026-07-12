from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from .contracts import (
    CapitalFlow,
    MarketDataResult,
    MarketDataSnapshot,
    MarketStrength,
    NewsItem,
    Profile,
    Quote,
    SidebarContext,
)
from .provider import UnifiedMarketDataProvider


class MarketDataCoordinator:
    def __init__(self, provider: UnifiedMarketDataProvider, *, timeout_seconds: float = 5.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._provider = provider
        self._timeout_seconds = timeout_seconds
        self._inflight: dict[SidebarContext, asyncio.Task[MarketDataSnapshot]] = {}
        self._quote_cache: dict[str, MarketDataResult[Quote]] = {}
        self._profile_cache: dict[str, MarketDataResult[Profile]] = {}
        self._capital_flow_cache: dict[str, MarketDataResult[CapitalFlow]] = {}
        self._market_strength_cache: MarketDataResult[MarketStrength] | None = None
        self._news_cache: dict[str, MarketDataResult[tuple[NewsItem, ...]]] = {}

    async def load_context(self, context: SidebarContext) -> MarketDataSnapshot:
        task = self._inflight.get(context)
        if task is None:
            task = asyncio.create_task(self._load_context(context))
            self._inflight[context] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._inflight.get(context) is task:
                self._inflight.pop(context, None)

    async def _load_context(self, context: SidebarContext) -> MarketDataSnapshot:
        symbols = tuple(dict.fromkeys((context.chart_symbol, *context.watchlist_symbols)))
        quotes_task = asyncio.create_task(self._provider.get_quotes(symbols))
        profile_task = asyncio.create_task(self._provider.get_profile(context.chart_symbol))
        capital_flow_task = asyncio.create_task(self._provider.get_capital_flow(context.chart_symbol))
        market_strength_task = asyncio.create_task(self._provider.get_market_strength())
        news_task = asyncio.create_task(self._provider.get_news(context.chart_symbol))
        quotes, profile, capital_flow, market_strength, news = await asyncio.gather(
            self._with_timeout(quotes_task, {}),
            self._with_timeout(profile_task, None),
            self._with_timeout(capital_flow_task, None),
            self._with_timeout(market_strength_task, None),
            self._with_timeout(news_task, None),
        )

        normalized_quotes: dict[str, MarketDataResult[Quote]] = {}
        for symbol in symbols:
            result = quotes.get(symbol) if quotes else None
            normalized_quotes[symbol] = self._resolve_quote(symbol, result)
        active_profile = self._resolve_profile(context.chart_symbol, profile)
        resolved_capital_flow = self._resolve_domain(
            context.chart_symbol, capital_flow, self._capital_flow_cache
        )
        resolved_market_strength = self._resolve_market_strength(market_strength)
        resolved_news = self._resolve_domain(context.chart_symbol, news, self._news_cache)

        return MarketDataSnapshot(
            context=context,
            active_quote=normalized_quotes[context.chart_symbol],
            active_profile=active_profile,
            watchlist_quotes={symbol: normalized_quotes[symbol] for symbol in context.watchlist_symbols},
            capital_flow=resolved_capital_flow,
            market_strength=resolved_market_strength,
            news=resolved_news,
        )

    async def _with_timeout(self, awaitable, timeout_value):
        try:
            return await asyncio.wait_for(awaitable, timeout=self._timeout_seconds)
        except TimeoutError:
            return timeout_value
        except Exception:
            return timeout_value

    def _resolve_quote(
        self, symbol: str, result: MarketDataResult[Quote] | None
    ) -> MarketDataResult[Quote]:
        if result is not None and result.value is not None:
            self._quote_cache[symbol] = result
            return result
        cached = self._quote_cache.get(symbol)
        if cached is not None and self._cache_valid(cached, timedelta(minutes=5)):
            return cached.as_stale(result.error if result is not None else "provider timeout")
        if result is not None:
            return result
        return MarketDataResult.unavailable(source="unified_market_data", error="provider timeout")

    def _resolve_domain(self, key, result, cache):
        if result is not None and result.value is not None:
            cache[key] = result
            return result
        cached = cache.get(key)
        if cached is not None and self._cache_valid(cached, timedelta(days=1)):
            return cached.as_stale(result.error if result is not None else "provider timeout")
        if result is not None:
            return result
        return MarketDataResult.unavailable(source="unified_market_data", error="provider timeout")

    def _resolve_market_strength(
        self, result: MarketDataResult[MarketStrength] | None
    ) -> MarketDataResult[MarketStrength]:
        if result is not None and result.value is not None:
            self._market_strength_cache = result
            return result
        if self._market_strength_cache is not None and self._cache_valid(
            self._market_strength_cache, timedelta(minutes=10)
        ):
            return self._market_strength_cache.as_stale(
                result.error if result is not None else "provider timeout"
            )
        if result is not None:
            return result
        return MarketDataResult.unavailable(source="unified_market_data", error="provider timeout")

    def _resolve_profile(
        self, symbol: str, result: MarketDataResult[Profile] | None
    ) -> MarketDataResult[Profile]:
        if result is not None and result.value is not None and result.value.symbol == symbol:
            self._profile_cache[symbol] = result
            return result
        cached = self._profile_cache.get(symbol)
        if cached is not None and self._cache_valid(cached, timedelta(days=1)):
            return cached.as_stale(result.error if result is not None else "provider timeout")
        if result is not None:
            return result
        return MarketDataResult.unavailable(source="unified_market_data", error="provider timeout")

    @staticmethod
    def _cache_valid(result: MarketDataResult, ttl: timedelta) -> bool:
        return datetime.now(UTC) - result.metadata.received_at.astimezone(UTC) <= ttl
