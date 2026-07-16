import asyncio
import json
from datetime import date

from collector.market_data import CapitalFlow, MarketDataResult, MarketStrength, Profile, Quote
from collector.market_data_provider import PostgresWencaiConfigLoader, REFRESH_CHANNEL, UPDATE_CHANNEL, MarketDataProviderRuntime, parse_sidebar_event
from collector.market_data.trading_day_cache import TradingCalendar


class Redis:
    def __init__(self): self.values, self.published = {}, []
    async def get(self, key): return self.values.get(key)
    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values: return False
        self.values[key] = value
        return True
    async def delete(self, key): self.values.pop(key, None)
    async def publish(self, channel, payload): self.published.append((channel, json.loads(payload)))

class Loader:
    def __init__(self): self.version, self.calls = 1, 0
    async def load(self): self.calls += 1; return self.version, {"IWENCAI_API_KEY": f"key-{self.version}", "IWENCAI_BASE_URL": "https://iwencai.example", "IWENCAI_ALLOWED_HOSTS": "iwencai.example"}

class Provider:
    def __init__(self): self.calls = 0
    async def get_quotes(self, symbols):
        self.calls += 1; return {symbol: MarketDataResult.available(Quote(symbol, price=10), trading_date=date(2026, 7, 10)) for symbol in symbols}
    async def get_profile(self, symbol): return MarketDataResult.available(Profile(symbol, name="name"), trading_date=date(2026, 7, 10))
    async def get_valuation(self, symbol):
        from collector.market_data import Valuation
        return MarketDataResult.available(Valuation(symbol), trading_date=date(2026, 7, 10))
    async def get_themes(self, symbol):
        from collector.market_data import Themes
        return MarketDataResult.available(Themes(symbol), trading_date=date(2026, 7, 10))
    async def get_capital_flow(self, symbol): return MarketDataResult.available(CapitalFlow(symbol), trading_date=date(2026, 7, 10))
    async def get_market_strength(self): return MarketDataResult.available(MarketStrength(), trading_date=date(2026, 7, 10))
    async def get_news(self, symbol, since=None): return MarketDataResult.available((), trading_date=date(2026, 7, 10))
class Calendar(TradingCalendar):
    def trading_day(self, now=None): return date(2026, 7, 10)
    def is_trading_day(self, now=None): return True

def event(): return {"type": "sidebar_refresh_requested", "reason": "subscribe", "chart_symbol": "000001.SZ", "chart_epoch": 2, "watchlist_symbols": ["600000.SH"], "watchlist_revision": 3}

def test_channel_contract_and_event_cache_update_hot_reload():
    async def run():
        redis, loader, providers = Redis(), Loader(), []
        def factory(**kwargs): providers.append(Provider()); return providers[-1]
        runtime = MarketDataProviderRuntime(redis, loader, provider_factory=factory, calendar=Calendar())
        assert await runtime.handle_message(event())
        assert REFRESH_CHANNEL == "market:sidebar:refresh_requests"
        assert UPDATE_CHANNEL == "market:sidebar:updates"
        assert "sidebar:iwencai:2026-07-10:quote:000001.SZ" in redis.values
        assert "sidebar:iwencai:2026-07-10:valuation:000001.SZ" in redis.values
        assert redis.published == [(UPDATE_CHANNEL, {"source": "iwencai", "chart_symbol": "000001.SZ", "chart_epoch": 2, "watchlist_revision": 3})]
        await runtime.handle_message(event())
        assert providers[0].calls == 1
        loader.version = 2
        await runtime.handle_message(event())
        assert len(providers) == 2
    asyncio.run(run())

def test_malformed_event_is_ignored_without_config_or_transport():
    async def run():
        redis, loader = Redis(), Loader()
        runtime = MarketDataProviderRuntime(redis, loader, provider_factory=lambda **kwargs: (_ for _ in ()).throw(AssertionError()))
        assert not await runtime.handle_message("not json")
        assert parse_sidebar_event({"type": "bad"}) is None
        assert loader.calls == 0 and redis.values == {} and redis.published == []
    asyncio.run(run())

def test_postgres_wencai_config_loader_reads_versioned_admin_config():
    class Pool:
        async def fetchrow(self, sql, key):
            assert key == "wencai.config"
            return {"version": 7, "value": {"api_key": "rotated", "base_url": "https://iwencai.example", "timeout_seconds": 9}}
    version, config = asyncio.run(PostgresWencaiConfigLoader(Pool()).load())
    assert version == 7 and config["IWENCAI_API_KEY"] == "rotated" and config["IWENCAI_TIMEOUT_SECONDS"] == "5.0"


def test_postgres_wencai_config_loader_migrates_legacy_key_to_api_key_pool():
    class Pool:
        async def fetchrow(self, sql, key):
            return {"version": 7, "value": {"api_key": "rotated", "base_url": "https://iwencai.example"}}
    version, config = asyncio.run(PostgresWencaiConfigLoader(Pool()).load())
    assert version == 7
    assert json.loads(config["IWENCAI_API_KEYS"]) == [{"label": "default", "key": "rotated", "enabled": True, "priority": 0}]


def test_stream_processing_acks_only_success_and_supports_payload_or_data(caplog):
    class StreamRedis(Redis):
        def __init__(self): super().__init__(); self.acks = []
        async def xack(self, stream, group, message_id): self.acks.append((stream, group, message_id))

    async def run():
        redis = StreamRedis()
        runtime = MarketDataProviderRuntime(redis, Loader(), provider_factory=lambda **_kwargs: Provider(), calendar=Calendar())

        async def handle(raw):
            if raw == "boom": raise RuntimeError("upstream unavailable")
            return raw == "ok"

        runtime.handle_message = handle
        await runtime._process_stream_messages("workers", [
            ("1-0", {"payload": "ok"}),
            ("2-0", {b"data": "boom"}),
            ("3-0", {"payload": "invalid"}),
        ])
        assert redis.acks == [(REFRESH_CHANNEL, "workers", "1-0")]

    asyncio.run(run())
    assert "leaving pending" in caplog.text


def test_stream_claims_abandoned_pending_entries():
    class ClaimRedis(Redis):
        def __init__(self): super().__init__(); self.claim_args = None; self.acks = []
        async def xautoclaim(self, *args, **kwargs):
            self.claim_args = (args, kwargs)
            return "0-0", [("9-0", {"data": "ok"})], []
        async def xack(self, stream, group, message_id): self.acks.append((stream, group, message_id))

    async def run():
        redis = ClaimRedis()
        runtime = MarketDataProviderRuntime(redis, Loader(), provider_factory=lambda **_kwargs: Provider(), calendar=Calendar())
        async def handle(raw): return raw == "ok"
        runtime.handle_message = handle
        await runtime._claim_pending("workers", "consumer-a")
        assert redis.claim_args[0][:5] == (REFRESH_CHANNEL, "workers", "consumer-a", 60_000, "0-0")
        assert redis.acks == [(REFRESH_CHANNEL, "workers", "9-0")]

    asyncio.run(run())


def test_stream_claim_scans_all_pending_batches():
    class ClaimRedis(Redis):
        def __init__(self): super().__init__(); self.starts = []; self.acks = []
        async def xautoclaim(self, stream, group, consumer, min_idle, start, *, count):
            self.starts.append(start)
            if start == "0-0": return "4-0", [("3-0", {"payload": "ok"})], []
            return "0-0", [("5-0", {"data": "ok"})], []
        async def xack(self, stream, group, message_id): self.acks.append(message_id)

    async def run():
        redis = ClaimRedis()
        runtime = MarketDataProviderRuntime(redis, Loader(), provider_factory=lambda **_kwargs: Provider(), calendar=Calendar())
        runtime.handle_message = lambda raw: asyncio.sleep(0, result=raw == "ok")
        await runtime._claim_pending("workers", "consumer-a")
        assert redis.starts == ["0-0", "4-0"]
        assert redis.acks == ["3-0", "5-0"]

    asyncio.run(run())


def test_stream_replays_current_consumer_pending_entries_before_new_reads():
    class ReplayRedis(Redis):
        def __init__(self): super().__init__(); self.calls = 0; self.acks = []
        async def xreadgroup(self, *args, **kwargs):
            self.calls += 1
            assert args[2] == {REFRESH_CHANNEL: "0"}
            return [(REFRESH_CHANNEL, [("8-0", {"payload": "ok"})])] if self.calls == 1 else []
        async def xack(self, stream, group, message_id): self.acks.append((stream, group, message_id))

    async def run():
        redis = ReplayRedis()
        runtime = MarketDataProviderRuntime(redis, Loader(), provider_factory=lambda **_kwargs: Provider(), calendar=Calendar())
        async def handle(raw): return raw == "ok"
        runtime.handle_message = handle
        await runtime._replay_pending("workers", "consumer-a")
        assert redis.acks == [(REFRESH_CHANNEL, "workers", "8-0")]
        assert redis.calls == 2

    asyncio.run(run())
