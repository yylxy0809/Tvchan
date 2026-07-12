from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from collector.market_data import (
    CapitalFlow,
    MarketDataResult,
    MarketStrength,
    NewsItem,
    Profile,
    Quote,
)
from collector.market_data_provider import (
    MarketDataProviderRuntime,
    MarketDemand,
    RedisDemandRepository,
    RuntimeConfig,
    config_from_env,
)


class DemandRepository:
    async def get_demand(self):
        return MarketDemand(("000001.SZ", "300001.SZ"), ("600000.SH", "000001.SZ"))


class Provider:
    def __init__(self):
        self.fail = False
        self.quote_calls = []
        self.profile_calls = []
        self.fund_calls = []
        self.news_calls = []
        self.strength_calls = 0

    async def get_quotes(self, symbols):
        self.quote_calls.append(tuple(symbols))
        if self.fail:
            raise RuntimeError("provider failed")
        return {symbol: MarketDataResult.available(Quote(symbol, price=10), source="fake") for symbol in symbols}

    async def get_profile(self, symbol):
        self.profile_calls.append(symbol)
        return MarketDataResult.available(Profile(symbol, name="Ping An", market_cap=12, pe_ratio=8, turnover_rate=3.2), source="fake")

    async def get_capital_flow(self, symbol):
        self.fund_calls.append(symbol)
        return MarketDataResult.available(CapitalFlow(symbol, net_inflow=5, main_net_inflow=3), source="fake")

    async def get_market_strength(self):
        self.strength_calls += 1
        return MarketDataResult.available(MarketStrength(score=88, leaders=("000001.SZ",)), source="fake")

    async def get_news(self, symbol, since=None):
        self.news_calls.append(symbol)
        now = datetime(2026, 7, 12, tzinfo=UTC)
        return MarketDataResult.available((NewsItem("n1", symbol, "company", "title", "fact", now, now, "fake"),), source="fake")


class Pipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def set(self, key, value, ex=None):
        self.commands.append(("set", key, value, ex))
        return self

    def publish(self, channel, value):
        self.commands.append(("publish", channel, value))
        return self

    async def execute(self):
        self.redis.transactions.append(self.commands)


class Redis:
    def __init__(self):
        self.transactions = []

    def pipeline(self, transaction=True):
        assert transaction is True
        return Pipeline(self)


def test_refresh_batches_demand_and_atomically_publishes_all_snapshots() -> None:
    redis = Redis()
    provider = Provider()
    runtime = MarketDataProviderRuntime(provider, DemandRepository(), redis, RuntimeConfig(interval_seconds=1, timeout_seconds=1))

    assert asyncio.run(runtime.refresh_once()) is True

    assert len(redis.transactions) == 1
    commands = redis.transactions[0]
    keys = {command[1] for command in commands if command[0] == "set"}
    assert keys == {
        "market:quote:000001.SZ", "market:quote:300001.SZ", "market:quote:600000.SH",
        "market:profile:000001.SZ", "market:finance:000001.SZ",
        "market:fund:000001.SZ", "market:strength:latest", "market:news:000001.SZ",
        "market:profile:300001.SZ", "market:finance:300001.SZ",
        "market:fund:300001.SZ", "market:news:300001.SZ",
    }
    assert {command[1] for command in commands if command[0] == "publish"} == keys
    payloads = {command[1]: json.loads(command[2]) for command in commands if command[0] == "set"}
    assert payloads["market:profile:000001.SZ"]["name"] == "Ping An"
    assert payloads["market:finance:000001.SZ"]["pe_ratio"] == 8
    assert payloads["market:finance:000001.SZ"]["turnover_rate"] == 3.2
    assert payloads["market:fund:000001.SZ"]["net_inflow"] == 5
    assert payloads["market:news:000001.SZ"]["items"][0]["event_id"] == "n1"
    assert provider.quote_calls == [("000001.SZ", "300001.SZ", "600000.SH")]
    assert provider.profile_calls == ["000001.SZ", "300001.SZ"]
    assert provider.fund_calls == ["000001.SZ", "300001.SZ"]
    assert provider.news_calls == ["000001.SZ", "300001.SZ"]
    assert provider.strength_calls == 1


def test_news_fetch_is_cooled_down_while_quotes_continue_refreshing() -> None:
    redis = Redis()
    provider = Provider()
    runtime = MarketDataProviderRuntime(
        provider,
        DemandRepository(),
        redis,
        RuntimeConfig(interval_seconds=1, timeout_seconds=1),
    )

    assert asyncio.run(runtime.refresh_once()) is True
    assert asyncio.run(runtime.refresh_once()) is True

    assert provider.news_calls == ["000001.SZ", "300001.SZ"]
    assert provider.quote_calls == [
        ("000001.SZ", "300001.SZ", "600000.SH"),
        ("000001.SZ", "300001.SZ", "600000.SH"),
    ]


def test_failed_cycle_does_not_replace_or_publish_snapshots() -> None:
    redis = Redis()
    provider = Provider()
    runtime = MarketDataProviderRuntime(provider, DemandRepository(), redis, RuntimeConfig(1, 1))
    assert asyncio.run(runtime.refresh_once()) is True
    provider.fail = True

    assert asyncio.run(runtime.refresh_once()) is False
    assert len(redis.transactions) == 1


def test_cycle_timeout_is_bounded_and_preserves_snapshots() -> None:
    class SlowProvider(Provider):
        async def get_quotes(self, symbols):
            await asyncio.sleep(0.05)
            return await super().get_quotes(symbols)

    redis = Redis()
    runtime = MarketDataProviderRuntime(SlowProvider(), DemandRepository(), redis, RuntimeConfig(1, 0.001))
    assert asyncio.run(runtime.refresh_once()) is False
    assert redis.transactions == []


def test_run_stops_gracefully_without_starting_an_extra_cycle() -> None:
    async def exercise():
        stop = asyncio.Event()

        class OnceDemand(DemandRepository):
            calls = 0

            async def get_demand(self):
                self.calls += 1
                stop.set()
                return await super().get_demand()

        demand = OnceDemand()
        runtime = MarketDataProviderRuntime(Provider(), demand, Redis(), RuntimeConfig(60, 1))
        await runtime.run(stop)
        assert demand.calls == 1

    asyncio.run(exercise())


def test_config_requires_provider_factory_and_a_demand_source_without_leaking_secret() -> None:
    secret = "must-not-appear"
    with pytest.raises(ValueError) as exc:
        config_from_env({"IWENCAI_API_KEY": secret})
    assert secret not in str(exc.value)
    assert "MARKET_DATA_PROVIDER_FACTORY" in str(exc.value)


def test_static_env_keeps_single_active_symbol_compatibility() -> None:
    config = config_from_env({
        "MARKET_DATA_PROVIDER_FACTORY": "example:create",
        "MARKET_DATA_ACTIVE_SYMBOL": "000001.sz",
        "MARKET_DATA_WATCHLIST": "600000.sh",
    })
    assert config.demand == MarketDemand(("000001.SZ",), ("600000.SH",))


def test_env_configures_runtime_and_redis_scan_budgets() -> None:
    config = config_from_env({
        "MARKET_DATA_PROVIDER_FACTORY": "example:create",
        "MARKET_DATA_ACTIVE_SYMBOL": "000001.SZ",
        "MARKET_DATA_MAX_SYMBOLS": "7",
        "MARKET_DATA_MAX_PROVIDER_CONCURRENCY": "3",
        "MARKET_DATA_MAX_DEMAND_SCAN_PAGES": "4",
        "MARKET_DATA_DEMAND_SCAN_TIMEOUT_SECONDS": "0.1",
        "MARKET_DATA_MAX_DEMAND_CONTEXT_BYTES": "1024",
        "MARKET_DATA_MAX_DEMAND_WATCHLIST_SYMBOLS": "9",
        "MARKET_DATA_MAX_DEMAND_SYMBOL_LENGTH": "20",
    })

    assert config.runtime.max_symbols == 7
    assert config.runtime.max_provider_concurrency == 3
    assert config.max_demand_scan_pages == 4
    assert config.demand_scan_timeout_seconds == 0.1
    assert config.max_demand_context_bytes == 1024
    assert config.max_demand_watchlist_symbols == 9
    assert config.max_demand_symbol_length == 20


def test_redis_demand_repository_aggregates_valid_contexts_and_bounds_scan() -> None:
    class DemandRedis:
        async def scan(self, cursor, match, count):
            assert match == "market:sidebar:demand:*"
            return (1, ["market:sidebar:demand:a", "market:sidebar:demand:bad", "market:sidebar:demand:b"])

        async def mget(self, keys):
            assert keys == ["market:sidebar:demand:a", "market:sidebar:demand:bad"]
            return [
                json.dumps({"chart_symbol": "000001.SZ", "watchlist_symbols": ["600000.SH"], "updated_at": "2026-07-12T00:00:00Z"}),
                "not-json",
            ]

    repository = RedisDemandRepository(DemandRedis(), max_contexts=2, scan_count=20)
    demand = asyncio.run(repository.get_demand())
    assert demand == MarketDemand(("000001.SZ",), ("600000.SH",))


def test_redis_demand_repository_ignores_malformed_record_shapes() -> None:
    class DemandRedis:
        async def scan(self, cursor, match, count):
            return (0, ["one", "two", "three"])

        async def mget(self, keys):
            return [
                json.dumps({"chart_symbol": "300001.SZ", "watchlist_symbols": ["000001.SZ"], "updated_at": "now"}),
                json.dumps({"chart_symbol": "", "watchlist_symbols": ["600000.SH"], "updated_at": "now"}),
                json.dumps({"chart_symbol": "600000.SH", "watchlist_symbols": "000001.SZ", "updated_at": "now"}),
            ]

    demand = asyncio.run(RedisDemandRepository(DemandRedis(), max_contexts=10).get_demand())
    assert demand == MarketDemand(("300001.SZ",), ("000001.SZ",))


def test_redis_demand_repository_bounds_context_size_watchlist_and_symbol_lengths(caplog) -> None:
    class DemandRedis:
        async def scan(self, cursor, match, count):
            return (0, ["oversized", "watchlist", "long-symbol"])

        async def mget(self, keys):
            return [
                "x" * 200,
                json.dumps({"chart_symbol": "000001.SZ", "watchlist_symbols": ["600000.SH", "300001.SZ", "000001.SZ"], "updated_at": "now"}),
                json.dumps({"chart_symbol": "000002.SZ", "watchlist_symbols": ["x" * 33], "updated_at": "now"}),
            ]

    demand = asyncio.run(
        RedisDemandRepository(DemandRedis(), max_context_bytes=150, max_watchlist_symbols=2, max_symbol_length=32).get_demand()
    )

    assert demand == MarketDemand(("000001.SZ",), ("600000.SH", "300001.SZ"))
    assert {record.reason for record in caplog.records if record.event == "demand_context_truncated"} == {"record_size", "watchlist_budget"}
    assert "600000.SH" not in caplog.text


def test_redis_demand_repository_stops_at_page_budget_without_logging_keys(caplog) -> None:
    class DemandRedis:
        calls = 0

        async def scan(self, cursor, match, count):
            self.calls += 1
            return (self.calls, ["market:sidebar:demand:secret-context"])

        async def mget(self, keys):
            return [json.dumps({"chart_symbol": "000001.SZ", "watchlist_symbols": [], "updated_at": "now"})]

    redis = DemandRedis()
    demand = asyncio.run(RedisDemandRepository(redis, max_contexts=10, max_pages=2).get_demand())

    assert demand == MarketDemand(("000001.SZ",))
    assert redis.calls == 2
    assert "secret-context" not in caplog.text
    assert any(record.event == "demand_scan_truncated" and record.reason == "page_budget" for record in caplog.records)


def test_redis_demand_repository_cancels_slow_scan_at_time_budget() -> None:
    class DemandRedis:
        cancelled = False

        async def scan(self, cursor, match, count):
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                self.cancelled = True
                raise

        async def mget(self, keys):
            raise AssertionError("mget must not run after scan timeout")

    redis = DemandRedis()
    demand = asyncio.run(RedisDemandRepository(redis, timeout_seconds=0.001).get_demand())

    assert demand == MarketDemand(())
    assert redis.cancelled is True


def test_runtime_truncates_deduplicated_symbols_and_bounds_global_provider_concurrency(caplog) -> None:
    class ManyDemand:
        async def get_demand(self):
            return MarketDemand(tuple(f"{value:06d}.SZ" for value in range(10)), ("000000.SZ", "000010.SZ"))

    class LimitedProvider(Provider):
        active = 0
        maximum = 0

        async def _track(self, operation, *args):
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            try:
                await asyncio.sleep(0.001)
                return await operation(*args)
            finally:
                self.active -= 1

        async def get_quotes(self, symbols):
            return await self._track(super().get_quotes, symbols)

        async def get_profile(self, symbol):
            return await self._track(super().get_profile, symbol)

        async def get_capital_flow(self, symbol):
            return await self._track(super().get_capital_flow, symbol)

        async def get_market_strength(self):
            return await self._track(super().get_market_strength)

        async def get_news(self, symbol, since=None):
            return await self._track(super().get_news, symbol, since)

    provider = LimitedProvider()
    runtime = MarketDataProviderRuntime(provider, ManyDemand(), Redis(), RuntimeConfig(1, 1, max_symbols=3, max_provider_concurrency=2))

    assert asyncio.run(runtime.refresh_once()) is True
    assert provider.quote_calls == [("000000.SZ", "000001.SZ", "000002.SZ")]
    assert provider.profile_calls == ["000000.SZ", "000001.SZ"]
    assert provider.maximum <= 2
    assert any(record.event == "demand_truncated" for record in caplog.records)
    assert "000009.SZ" not in caplog.text


def test_runtime_propagates_cancellation_and_cancels_provider_work() -> None:
    class SlowProvider(Provider):
        cancelled = False

        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()

        async def get_quotes(self, symbols):
            try:
                self.started.set()
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    async def exercise():
        provider = SlowProvider()
        runtime = MarketDataProviderRuntime(provider, DemandRepository(), Redis(), RuntimeConfig(1, 2))
        task = asyncio.create_task(runtime.refresh_once())
        await provider.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert provider.cancelled is True

    asyncio.run(exercise())


def test_runtime_round_robins_bounded_structured_symbols() -> None:
    runtime = MarketDataProviderRuntime(
        Provider(),
        DemandRepository(),
        Redis(),
        RuntimeConfig(max_structured_symbols_per_refresh=2),
    )
    symbols = ("000001.SZ", "000002.SZ", "000003.SZ")

    assert runtime._next_structured_symbols(symbols) == ("000001.SZ", "000002.SZ")
    assert runtime._next_structured_symbols(symbols) == ("000003.SZ", "000001.SZ")
    assert runtime._next_structured_symbols(symbols) == ("000002.SZ", "000003.SZ")
