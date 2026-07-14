from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable

from .contracts import CapitalFlow, MarketDataResult, MarketStrength, NewsItem, Profile, Quote, SidebarContext, Themes, Valuation


class UnifiedMarketDataProvider(ABC):
    """External market-data contract used by the sidebar worker."""

    async def prepare_context(self, context: SidebarContext) -> None:
        """Bind one request context without performing network I/O."""

    @abstractmethod
    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]: ...
    @abstractmethod
    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]: ...
    @abstractmethod
    async def get_valuation(self, symbol: str) -> MarketDataResult[Valuation]: ...
    @abstractmethod
    async def get_themes(self, symbol: str) -> MarketDataResult[Themes]: ...
    @abstractmethod
    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]: ...
    @abstractmethod
    async def get_market_strength(self) -> MarketDataResult[MarketStrength]: ...
    @abstractmethod
    async def get_news(self, symbol: str, since: datetime | None = None) -> MarketDataResult[tuple[NewsItem, ...]]: ...


class FallbackMarketDataProvider(UnifiedMarketDataProvider):
    """Use the fallback only when the primary has no valid value."""

    def __init__(self, primary: UnifiedMarketDataProvider, fallback: UnifiedMarketDataProvider) -> None:
        self.primary = primary
        self.fallback = fallback

    async def prepare_context(self, context: SidebarContext) -> None:
        await self.primary.prepare_context(context)
        await self.fallback.prepare_context(context)

    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]:
        requested = tuple(dict.fromkeys(symbols))
        primary = await self.primary.get_quotes(requested)
        missing = tuple(symbol for symbol in requested if primary.get(symbol) is None or primary[symbol].value is None)
        if missing:
            primary.update(await self.fallback.get_quotes(missing))
        return primary

    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]:
        return await self._one("get_profile", symbol)

    async def get_valuation(self, symbol: str) -> MarketDataResult[Valuation]:
        return await self._one("get_valuation", symbol)

    async def get_themes(self, symbol: str) -> MarketDataResult[Themes]:
        return await self._one("get_themes", symbol)

    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]:
        return await self._one("get_capital_flow", symbol)

    async def get_market_strength(self) -> MarketDataResult[MarketStrength]:
        result = await self.primary.get_market_strength()
        return result if result.value is not None else await self.fallback.get_market_strength()

    async def get_news(self, symbol: str, since: datetime | None = None) -> MarketDataResult[tuple[NewsItem, ...]]:
        result = await self.primary.get_news(symbol, since)
        return result if result.value is not None else await self.fallback.get_news(symbol, since)

    async def _one(self, method: str, symbol: str):
        result = await getattr(self.primary, method)(symbol)
        return result if result.value is not None else await getattr(self.fallback, method)(symbol)
