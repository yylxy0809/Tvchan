from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Protocol

from .contracts import CapitalFlow, MarketDataResult, MarketStrength, NewsItem, Profile, Quote


class UnifiedMarketDataProvider(ABC):
    @abstractmethod
    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]:
        """Return normalized quotes in one provider-level batch."""

    @abstractmethod
    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]:
        pass

    @abstractmethod
    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]:
        pass

    @abstractmethod
    async def get_market_strength(self) -> MarketDataResult[MarketStrength]:
        pass

    @abstractmethod
    async def get_news(
        self, symbol: str, since: datetime | None = None
    ) -> MarketDataResult[tuple[NewsItem, ...]]:
        pass


class QuoteSource(Protocol):
    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]: ...


class StructuredMarketSource(Protocol):
    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]: ...

    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]: ...

    async def get_market_strength(self) -> MarketDataResult[MarketStrength]: ...


class StructuredNewsSource(Protocol):
    async def get_news(
        self, symbol: str, since: datetime | None = None
    ) -> MarketDataResult[tuple[NewsItem, ...]]: ...


class CompositeMarketDataProvider(UnifiedMarketDataProvider):
    """Combine partial providers without exposing their implementation to callers."""

    def __init__(self, quotes: QuoteSource, market: StructuredMarketSource, news: StructuredNewsSource) -> None:
        self._quotes = quotes
        self._market = market
        self._news = news

    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]:
        requested = tuple(dict.fromkeys(symbols))
        try:
            return await self._quotes.get_quotes(requested)
        except Exception:
            return {
                symbol: MarketDataResult.unavailable(
                    source="local_postgres_canonical", error="local quote source unavailable"
                )
                for symbol in requested
            }

    async def close(self) -> None:
        closed: set[int] = set()
        for source in (self._quotes, self._market, self._news):
            if id(source) in closed:
                continue
            closed.add(id(source))
            close = getattr(source, "close", None)
            if close is None:
                continue
            result = close()
            if hasattr(result, "__await__"):
                await result

    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]:
        try:
            return await self._market.get_profile(symbol)
        except Exception:
            return MarketDataResult.unavailable(
                source="westock_data", error="market data provider unavailable"
            )

    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]:
        try:
            return await self._market.get_capital_flow(symbol)
        except Exception:
            return MarketDataResult.unavailable(
                source="westock_data", error="market data provider unavailable"
            )

    async def get_market_strength(self) -> MarketDataResult[MarketStrength]:
        try:
            return await self._market.get_market_strength()
        except Exception:
            return MarketDataResult.unavailable(
                source="westock_data", error="market data provider unavailable"
            )

    async def get_news(
        self, symbol: str, since: datetime | None = None
    ) -> MarketDataResult[tuple[NewsItem, ...]]:
        try:
            return await self._news.get_news(symbol, since)
        except Exception:
            return MarketDataResult.unavailable(
                source="iwencai_news_search", error="news provider unavailable"
            )
