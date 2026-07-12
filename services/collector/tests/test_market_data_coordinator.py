from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from collector.market_data import (
    CapitalFlow,
    Freshness,
    MarketDataCoordinator,
    MarketDataResult,
    MarketStrength,
    NewsItem,
    Profile,
    Quote,
    SidebarContext,
    UnifiedMarketDataProvider,
)


class FakeProvider(UnifiedMarketDataProvider):
    def __init__(self) -> None:
        self.quote_calls: list[tuple[str, ...]] = []
        self.profile_calls: list[str] = []
        self.delay = 0.0
        self.unavailable = False
        self.domain_calls: list[tuple[str, str | None]] = []

    async def get_quotes(self, symbols):
        self.quote_calls.append(tuple(symbols))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.unavailable:
            return {
                symbol: MarketDataResult.unavailable(source="fake", error="circuit open")
                for symbol in symbols
            }
        return {
            symbol: MarketDataResult.available(
                Quote(symbol=symbol, price=10.0), source="fake", freshness=Freshness.LIVE
            )
            for symbol in symbols
        }

    async def get_profile(self, symbol):
        self.profile_calls.append(symbol)
        if self.delay:
            await asyncio.sleep(self.delay)
        return MarketDataResult.available(Profile(symbol=symbol, name=symbol), source="fake")

    async def get_capital_flow(self, symbol):
        self.domain_calls.append(("capital_flow", symbol))
        if self.delay:
            await asyncio.sleep(self.delay)
        return MarketDataResult.available(CapitalFlow(symbol=symbol, main_net_inflow=100), source="fake")

    async def get_market_strength(self):
        self.domain_calls.append(("market_strength", None))
        if self.delay:
            await asyncio.sleep(self.delay)
        return MarketDataResult.available(MarketStrength(score=88), source="fake")

    async def get_news(self, symbol, since=None):
        self.domain_calls.append(("news", symbol))
        if self.delay:
            await asyncio.sleep(self.delay)
        return MarketDataResult.available(
            (
                NewsItem(
                    event_id="event-1",
                    symbol=symbol,
                    category="company",
                    title="title",
                    fact_summary="summary",
                    published_at=datetime(2026, 7, 12, tzinfo=UTC),
                    first_seen_at=datetime(2026, 7, 12, tzinfo=UTC),
                    source="fake",
                ),
            ),
            source="fake",
        )


def test_load_context_batches_unique_chart_and_watchlist_quotes() -> None:
    provider = FakeProvider()
    coordinator = MarketDataCoordinator(provider)
    context = SidebarContext(
        chart_symbol="000001.SZ",
        chart_epoch=18,
        watchlist_symbols=("600000.SH", "000001.SZ", "600000.SH"),
        watchlist_revision=7,
    )

    snapshot = asyncio.run(coordinator.load_context(context))

    assert provider.quote_calls == [("000001.SZ", "600000.SH")]
    assert provider.profile_calls == ["000001.SZ"]
    assert snapshot.context == context
    assert snapshot.active_profile.value.symbol == "000001.SZ"
    assert set(snapshot.watchlist_quotes) == {"600000.SH", "000001.SZ"}
    assert snapshot.capital_flow.value.symbol == "000001.SZ"
    assert snapshot.market_strength.value.score == 88
    assert snapshot.news.value[0].symbol == "000001.SZ"
    assert provider.domain_calls == [
        ("capital_flow", "000001.SZ"),
        ("market_strength", None),
        ("news", "000001.SZ"),
    ]


def test_empty_watchlist_still_loads_chart_symbol() -> None:
    provider = FakeProvider()
    coordinator = MarketDataCoordinator(provider)
    context = SidebarContext(chart_symbol="000001.SZ", chart_epoch=3)

    snapshot = asyncio.run(coordinator.load_context(context))

    assert provider.quote_calls == [("000001.SZ",)]
    assert snapshot.active_quote.value.symbol == "000001.SZ"


def test_timeout_returns_structured_degradation_instead_of_blocking() -> None:
    provider = FakeProvider()
    provider.delay = 0.05
    coordinator = MarketDataCoordinator(provider, timeout_seconds=0.001)
    context = SidebarContext(chart_symbol="000001.SZ", chart_epoch=1)

    snapshot = asyncio.run(coordinator.load_context(context))

    assert snapshot.active_quote.metadata.freshness is Freshness.UNAVAILABLE
    assert snapshot.active_profile.metadata.freshness is Freshness.UNAVAILABLE
    assert snapshot.active_quote.error == "provider timeout"
    assert snapshot.capital_flow.metadata.freshness is Freshness.UNAVAILABLE
    assert snapshot.market_strength.metadata.freshness is Freshness.UNAVAILABLE
    assert snapshot.news.metadata.freshness is Freshness.UNAVAILABLE


def test_news_timeout_is_isolated_from_other_domains() -> None:
    class SlowNewsProvider(FakeProvider):
        async def get_news(self, symbol, since=None):
            await asyncio.sleep(0.05)
            return await super().get_news(symbol, since)

    coordinator = MarketDataCoordinator(SlowNewsProvider(), timeout_seconds=0.001)

    snapshot = asyncio.run(
        coordinator.load_context(SidebarContext(chart_symbol="000001.SZ", chart_epoch=1))
    )

    assert snapshot.active_quote.metadata.freshness is Freshness.LIVE
    assert snapshot.active_profile.metadata.freshness is Freshness.LIVE
    assert snapshot.capital_flow.metadata.freshness is Freshness.LIVE
    assert snapshot.market_strength.metadata.freshness is Freshness.LIVE
    assert snapshot.news.metadata.freshness is Freshness.UNAVAILABLE


def test_timeout_uses_last_successful_snapshot_as_stale_fallback() -> None:
    provider = FakeProvider()
    coordinator = MarketDataCoordinator(provider, timeout_seconds=0.01)
    context = SidebarContext(chart_symbol="000001.SZ", chart_epoch=1)
    first = asyncio.run(coordinator.load_context(context))
    provider.delay = 0.05

    second = asyncio.run(coordinator.load_context(context))

    assert second.active_quote.value == first.active_quote.value
    assert second.active_quote.metadata.freshness is Freshness.STALE
    assert second.active_profile.metadata.freshness is Freshness.STALE
    assert second.capital_flow.metadata.freshness is Freshness.STALE
    assert second.market_strength.metadata.freshness is Freshness.STALE
    assert second.news.metadata.freshness is Freshness.STALE


def test_provider_degradation_preserves_source_and_reason() -> None:
    provider = FakeProvider()
    provider.unavailable = True
    coordinator = MarketDataCoordinator(provider)

    snapshot = asyncio.run(
        coordinator.load_context(SidebarContext(chart_symbol="000001.SZ", chart_epoch=1))
    )

    assert snapshot.active_quote.metadata.source == "fake"
    assert snapshot.active_quote.error == "circuit open"


def test_concurrent_identical_context_requests_are_coalesced() -> None:
    async def run() -> None:
        provider = FakeProvider()
        provider.delay = 0.01
        coordinator = MarketDataCoordinator(provider)
        context = SidebarContext(chart_symbol="000001.SZ", chart_epoch=1)

        await asyncio.gather(coordinator.load_context(context), coordinator.load_context(context))

        assert provider.quote_calls == [("000001.SZ",)]
        assert provider.profile_calls == ["000001.SZ"]

    asyncio.run(run())
