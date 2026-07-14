import asyncio
from datetime import date
from collector.market_data import CapitalFlow, MarketDataCoordinator, MarketDataResult, MarketStrength, Profile, Quote, SidebarContext
from collector.market_data.trading_day_cache import TradingCalendar

class Calendar(TradingCalendar):
    def __init__(self, today, trading=True): self.today, self.trading = today, trading
    def trading_day(self, now=None): return self.today
    def is_trading_day(self, now=None): return self.trading
class Provider:
    def __init__(self): self.calls = 0
    async def prepare_context(self, context): self.prepared = context
    async def get_quotes(self, symbols):
        self.calls += 1; await asyncio.sleep(.001)
        return {symbol: MarketDataResult.available(Quote(symbol, price=10), trading_date=date(2026, 7, 10)) for symbol in symbols}
    async def get_profile(self, symbol): return MarketDataResult.available(Profile(symbol), trading_date=date(2026, 7, 10))
    async def get_valuation(self, symbol):
        from collector.market_data import Valuation
        return MarketDataResult.available(Valuation(symbol), trading_date=date(2026, 7, 10))
    async def get_themes(self, symbol):
        from collector.market_data import Themes
        return MarketDataResult.available(Themes(symbol), trading_date=date(2026, 7, 10))
    async def get_capital_flow(self, symbol): return MarketDataResult.available(CapitalFlow(symbol), trading_date=date(2026, 7, 10))
    async def get_market_strength(self): return MarketDataResult.available(MarketStrength(), trading_date=date(2026, 7, 10))
    async def get_news(self, symbol, since=None): return MarketDataResult.available((), trading_date=date(2026, 7, 10))
def test_same_key_single_flight_and_same_day_cache():
    async def run():
        provider = Provider(); coordinator = MarketDataCoordinator(provider, calendar=Calendar(date(2026, 7, 10)))
        await asyncio.gather(*(coordinator.ensure_snapshot("quote", "000001.SZ") for _ in range(100)))
        await coordinator.ensure_snapshot("quote", "000001.SZ")
        assert provider.calls == 1
    asyncio.run(run())
def test_non_trading_day_cold_cache_fetches_latest_trading_day_once():
    async def run():
        provider = Provider(); result = await MarketDataCoordinator(provider, calendar=Calendar(date(2026, 7, 10), False)).ensure_snapshot("quote", "000001.SZ")
        assert provider.calls == 1 and result.metadata.freshness.value == "fresh"
    asyncio.run(run())


def test_load_context_prepares_provider_before_domain_fetches():
    async def run():
        provider = Provider()
        context = SidebarContext("000001.SZ", 3, ("600000.SH",), 2)
        await MarketDataCoordinator(provider, calendar=Calendar(date(2026, 7, 10))).load_context(context)
        assert provider.prepared == context

    asyncio.run(run())
