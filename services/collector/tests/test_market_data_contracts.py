from __future__ import annotations

from datetime import UTC, datetime

import pytest

from collector.market_data import (
    CapitalFlow,
    CompositeMarketDataProvider,
    Freshness,
    MarketDataMetadata,
    MarketDataResult,
    NewsItem,
    NewsSource,
    Profile,
    Quote,
    SidebarContext,
    UnifiedMarketDataProvider,
)


def test_market_data_metadata_requires_timezone_aware_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        MarketDataMetadata(
            source="westock_data",
            provider_version="1.0.4",
            provider_ts=datetime(2026, 7, 12, 9, 30),
            received_at=datetime.now(UTC),
            freshness=Freshness.LIVE,
        )


def test_result_keeps_structured_unavailable_state_without_fake_value() -> None:
    result = MarketDataResult.unavailable(source="westock_data", error="timeout")

    assert result.value is None
    assert result.metadata.freshness is Freshness.UNAVAILABLE
    assert result.error == "timeout"


def test_sidebar_context_keeps_chart_and_watchlist_independent() -> None:
    context = SidebarContext(
        chart_symbol="000001.SZ",
        chart_epoch=18,
        watchlist_symbols=("600000.SH", "600000.SH", "000001.SZ"),
        watchlist_revision=7,
    )

    assert context.chart_symbol == "000001.SZ"
    assert context.chart_epoch == 18
    assert context.watchlist_symbols == ("600000.SH", "000001.SZ")


def test_quote_is_a_standard_immutable_dto() -> None:
    quote = Quote(symbol="000001.SZ", price=10.5, change_percent=1.2, volume=100)

    assert quote.symbol == "000001.SZ"
    with pytest.raises(AttributeError):
        quote.price = 11.0  # type: ignore[misc]


def test_profile_and_capital_flow_expose_sidebar_finance_fields() -> None:
    profile = Profile(symbol="000001.SZ", turnover_rate=3.2)
    capital_flow = CapitalFlow(symbol="000001.SZ", net_inflow=1200)

    assert profile.turnover_rate == 3.2
    assert capital_flow.net_inflow == 1200


def test_unified_provider_cannot_be_instantiated_without_implementations() -> None:
    with pytest.raises(TypeError):
        UnifiedMarketDataProvider()  # type: ignore[abstract]


def test_news_item_has_auditable_sources_and_timezone_aware_timestamps() -> None:
    item = NewsItem(
        event_id="sha256:event",
        symbol="000001.SZ",
        category="announcement",
        title="公告",
        fact_summary="事实摘要",
        published_at=datetime(2026, 7, 12, 9, 30, tzinfo=UTC),
        first_seen_at=datetime(2026, 7, 12, 9, 31, tzinfo=UTC),
        sources=(NewsSource(name="交易所", url="https://example.test/news/1"),),
        impact_tags=("业绩", "银行"),
        source="iwencai_news_search",
    )

    assert item.sources[0].name == "交易所"
    assert item.impact_tags == ("业绩", "银行")

    with pytest.raises(ValueError, match="timezone-aware"):
        NewsItem(
            event_id="sha256:event",
            symbol="000001.SZ",
            category="announcement",
            title="公告",
            fact_summary="事实摘要",
            published_at=datetime(2026, 7, 12, 9, 30, tzinfo=UTC),
            first_seen_at=datetime(2026, 7, 12, 9, 31),
            source="iwencai_news_search",
        )


class PartialMarketSource:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def get_quotes(self, symbols):
        symbols = tuple(symbols)
        self.calls.append(("quotes", symbols))
        return {symbol: MarketDataResult.available(Quote(symbol=symbol), source="westock_data") for symbol in symbols}

    async def get_profile(self, symbol):
        self.calls.append(("profile", symbol))
        return MarketDataResult.unavailable(source="westock_data")

    async def get_capital_flow(self, symbol):
        self.calls.append(("capital_flow", symbol))
        return MarketDataResult.unavailable(source="westock_data")

    async def get_market_strength(self):
        self.calls.append(("market_strength", None))
        return MarketDataResult.unavailable(source="westock_data")


class PartialQuoteSource:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def get_quotes(self, symbols):
        symbols = tuple(symbols)
        self.calls.append(symbols)
        return {symbol: MarketDataResult.available(Quote(symbol=symbol), source="local_postgres_canonical") for symbol in symbols}


class PartialNewsSource:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def get_news(self, symbol, since=None):
        self.calls.append((symbol, since))
        return MarketDataResult.unavailable(source="iwencai_news_search")


def test_composite_provider_delegates_each_domain_to_its_partial_source() -> None:
    import asyncio

    market = PartialMarketSource()
    quotes = PartialQuoteSource()
    news = PartialNewsSource()
    provider = CompositeMarketDataProvider(quotes, market, news)
    since = datetime(2026, 7, 12, tzinfo=UTC)

    async def invoke_all() -> None:
        await provider.get_quotes(["000001.SZ"])
        await provider.get_profile("000001.SZ")
        await provider.get_capital_flow("000001.SZ")
        await provider.get_market_strength()
        await provider.get_news("000001.SZ", since)

    asyncio.run(invoke_all())

    assert quotes.calls == [("000001.SZ",)]
    assert market.calls == [
        ("profile", "000001.SZ"),
        ("capital_flow", "000001.SZ"),
        ("market_strength", None),
    ]
    assert news.calls == [("000001.SZ", since)]


def test_composite_provider_sanitizes_partial_source_exceptions() -> None:
    import asyncio

    secret = "IWENCAI_API_KEY=do-not-leak"

    class FailingMarket(PartialMarketSource):
        async def get_quotes(self, symbols):
            raise RuntimeError(f"raw payload and {secret}")

        async def get_profile(self, symbol):
            raise RuntimeError(secret)

        async def get_capital_flow(self, symbol):
            raise RuntimeError(secret)

        async def get_market_strength(self):
            raise RuntimeError(secret)

    class FailingNews(PartialNewsSource):
        async def get_news(self, symbol, since=None):
            raise RuntimeError(secret)

    class FailingQuotes(PartialQuoteSource):
        async def get_quotes(self, symbols):
            raise RuntimeError(secret)

    provider = CompositeMarketDataProvider(FailingQuotes(), FailingMarket(), FailingNews())

    async def invoke_all():
        return (
            await provider.get_quotes(["000001.SZ", "600000.SH"]),
            await provider.get_profile("000001.SZ"),
            await provider.get_capital_flow("000001.SZ"),
            await provider.get_market_strength(),
            await provider.get_news("000001.SZ"),
        )

    quotes, *results = asyncio.run(invoke_all())
    all_results = [*quotes.values(), *results]

    assert set(quotes) == {"000001.SZ", "600000.SH"}
    assert all(result.metadata.freshness is Freshness.UNAVAILABLE for result in all_results)
    assert all(secret not in (result.error or "") for result in all_results)
    assert {result.error for result in all_results} == {
        "local quote source unavailable",
        "market data provider unavailable",
        "news provider unavailable",
    }
