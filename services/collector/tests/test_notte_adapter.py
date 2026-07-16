import asyncio
from datetime import date
import logging
import time

from collector.market_data.notte import NotteConfig, NotteSidebarProvider


class Function:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def run(self, **variables):
        self.calls.append(variables)
        return type("FunctionRunResponse", (), {"result": self.result})()


def test_notte_function_receives_sidebar_contract_and_normalizes_response(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")
    function = Function(
        {
            "trading_date": "2026-07-10",
            "provider_ts": "2026-07-10T15:00:00+08:00",
            "quotes": [{"symbol": "000001.SZ", "price": "10.5", "change_percent": "1.2"}],
            "profile": {"name": "<b>unsafe</b>", "exchange": "SZ", "industry": "Banking", "description": "Retail bank"},
            "valuation": {"market_cap": "1000000", "pe_ratio": "5.2", "pb_ratio": "0.6", "ps_ratio": "1.1"},
            "capital_flow": {"net_inflow": "42"},
            "themes": {"symbol": "000001.SZ", "industry": "Banking", "concepts": ["Finance", "Finance", " "]},
            "market_strength": {"score": "72.5", "up_count": 3200, "leaders": ["000001.SZ", "000001.SZ"]},
            "news": [
                {
                    "id": "news-1",
                    "category": "company",
                    "title": "Result",
                    "summary": "Facts",
                    "published_at": "2026-07-10T14:32:00+08:00",
                    "first_seen_at": "2026-07-10T14:35:00+08:00",
                    "sources": [{"name": "Example", "url": "https://example.com/news"}],
                }
            ],
        }
    )
    provider = NotteSidebarProvider(NotteConfig.from_env(), function=function, today=lambda: date(2026, 7, 10))

    async def run():
        quotes = await provider.get_quotes(("000001.SZ",))
        profile = await provider.get_profile("000001.SZ")
        valuation = await provider.get_valuation("000001.SZ")
        flow = await provider.get_capital_flow("000001.SZ")
        themes = await provider.get_themes("000001.SZ")
        strength = await provider.get_market_strength()
        news = await provider.get_news("000001.SZ")

        assert quotes["000001.SZ"].value.price == 10.5
        assert profile.value.name is None
        assert profile.value.description == "Retail bank"
        assert valuation.value.market_cap == 1_000_000.0
        assert flow.value.net_inflow == 42.0
        assert themes.value.concepts == ("Finance",)
        assert strength.value.leaders == ("000001.SZ",)
        assert news.value[0].published_at.isoformat() == "2026-07-10T14:32:00+08:00"
        return quotes, news

    quotes, news = asyncio.run(run())
    assert len(function.calls) == 1
    assert function.calls[0] == {
        "request_id": function.calls[0]["request_id"],
        "chart_symbol": "000001.SZ",
        "watchlist_symbols": "000001.SZ",
        "sections": "quotes,profile,valuation,capital_flow,themes,market_strength,news",
        "news_limit": 20,
    }
    assert quotes["000001.SZ"].metadata.source == "notte"
    assert news.metadata.source == "notte"


def test_notte_concurrent_domain_reads_share_one_function_run(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")
    function = Function(
        {
            "trading_date": "2026-07-10",
            "quotes": [{"symbol": "000001.SZ", "price": 10.5}],
            "profile": {"name": "Ping An Bank"},
            "valuation": {"market_cap": 1_000_000},
            "capital_flow": {"main_net_inflow": 42},
            "themes": {"symbol": "000001.SZ", "industry": "Banking"},
            "market_strength": {"score": 50},
            "news": [],
        }
    )
    provider = NotteSidebarProvider(NotteConfig.from_env(), function=function)

    async def run():
        from collector.market_data.contracts import SidebarContext

        await provider.prepare_context(SidebarContext("000001.SZ", 1, ("600000.SH",), 1))
        await asyncio.gather(
            provider.get_profile("000001.SZ"),
            provider.get_valuation("000001.SZ"),
            provider.get_capital_flow("000001.SZ"),
            provider.get_themes("000001.SZ"),
            provider.get_market_strength(),
            provider.get_news("000001.SZ"),
        )

    asyncio.run(run())
    assert len(function.calls) == 1
    assert function.calls[0]["watchlist_symbols"] == "000001.SZ,600000.SH"


def test_notte_normalizes_production_mapping_shapes(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")
    function = Function(
        {
            "trading_date": "2026-07-10",
            "generated_at": "2026-07-10T15:01:00+08:00",
            "quotes": {
                "000001.SZ": {"symbol": "000001.SZ", "price": 10.5, "amount": 42},
                "600000.SH": {"symbol": "600000.SH", "price": 8.1},
            },
            "profile": {"symbol": "000001.SZ", "name": "Ping An Bank", "exchange": "SZ"},
            "valuation": {"symbol": "000001.SZ", "market_cap": 100, "pe_ttm": 5.2, "pb": 0.6, "ps_ttm": 1.1},
            "capital_flow": {"symbol": "000001.SZ", "main_net_inflow": 8},
            "themes": [{"name": "Glass Fiber", "change_percent": 3, "main_net_inflow": 123_450_000}],
            "market_strength": {"leaders": [], "themes": []},
            "news": {
                "symbol": "000001.SZ",
                "items": [{
                    "id": "n1", "title": "Headline", "url": "https://example.com/n1",
                    "source_name": "Example", "published_at": "2026-07-10T14:00:00+08:00",
                    "category": "company", "related_symbols": ["000001.SZ"],
                }],
            },
        }
    )
    provider = NotteSidebarProvider(NotteConfig.from_env(), function=function)

    async def run():
        quotes = await provider.get_quotes(("000001.SZ", "600000.SH"))
        valuation = await provider.get_valuation("000001.SZ")
        themes = await provider.get_themes("000001.SZ")
        strength = await provider.get_market_strength()
        news = await provider.get_news("000001.SZ")
        assert quotes["000001.SZ"].value.amount == 42
        assert quotes["600000.SH"].value.price == 8.1
        assert valuation.value.pe_ratio == 5.2
        assert valuation.value.pb_ratio == 0.6
        assert themes.value is None
        assert strength.value.theme_details[0].name == "Glass Fiber"
        assert strength.value.theme_details[0].change_percent == 3
        assert strength.value.theme_details[0].main_net_inflow_wan == 12_345
        assert news.value[0].sources[0].url == "https://example.com/n1"
        assert news.value[0].fact_summary == "Headline"

    asyncio.run(run())


def test_notte_rejects_non_finite_numbers_and_invalid_dates(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")
    function = Function(
        {
            "trading_date": "not-a-date",
            "quotes": [{"symbol": "000001.SZ", "price": "NaN"}],
            "news": [{"id": "bad", "title": "Bad", "published_at": "yesterday"}],
        }
    )
    provider = NotteSidebarProvider(NotteConfig.from_env(), function=function)

    async def run():
        quote = (await provider.get_quotes(("000001.SZ",)))["000001.SZ"]
        news = await provider.get_news("000001.SZ")
        assert quote.value is None
        assert news.value is None

    asyncio.run(run())


def test_notte_never_logs_or_exposes_api_key(monkeypatch, caplog, capsys):
    secret = "notte-secret-must-not-appear"
    monkeypatch.setenv("NOTTE_API_KEY", secret)
    logger = logging.getLogger("notte_sdk.live_viewer")
    logger.disabled = False

    class LoggingFunction:
        def run(self, **variables):
            logger.info("live viewer URL: https://viewer.example/?token=%s", secret)
            raise RuntimeError(secret)

    config = NotteConfig.from_env()
    provider = NotteSidebarProvider(config, function=LoggingFunction())

    async def run():
        result = (await provider.get_quotes(("000001.SZ",)))["000001.SZ"]
        assert result.value is None

    with caplog.at_level(logging.INFO):
        asyncio.run(run())
        from notte_core.common.logging import logger as notte_logger

        notte_logger.info("live viewer URL: https://viewer.example/?token=%s", secret)
    assert secret not in repr(config)
    assert secret not in caplog.text
    assert secret not in capsys.readouterr().err


def test_notte_function_timeout_does_not_block_event_loop(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")

    class SlowFunction:
        def run(self, **variables):
            time.sleep(0.05)
            return {"trading_date": "2026-07-10", "quotes": []}

    provider = NotteSidebarProvider(NotteConfig.from_env(timeout_seconds=0.01), function=SlowFunction())

    async def run():
        task = asyncio.create_task(provider.get_quotes(("000001.SZ",)))
        await asyncio.sleep(0)
        assert not task.done()
        result = await task
        assert result["000001.SZ"].value is None
        assert result["000001.SZ"].error.value == "timeout"

    asyncio.run(run())


def test_notte_timeout_opens_context_circuit_and_does_not_run_again(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")

    class SlowFunction:
        def __init__(self):
            self.calls = 0

        def run(self, **variables):
            self.calls += 1
            time.sleep(0.05)
            return {"trading_date": "2026-07-10", "quotes": []}

    function = SlowFunction()
    provider = NotteSidebarProvider(NotteConfig.from_env(timeout_seconds=0.01), function=function)

    async def run():
        await provider.get_quotes(("000001.SZ",))
        await provider.get_profile("000001.SZ")

    asyncio.run(run())
    assert function.calls == 1


def test_notte_timeout_circuit_retries_after_cooldown(monkeypatch):
    monkeypatch.setenv("NOTTE_API_KEY", "test-key")
    clock = [100.0]

    class SlowThenFastFunction:
        def __init__(self):
            self.calls = 0

        def run(self, **variables):
            self.calls += 1
            if self.calls == 1:
                time.sleep(0.15)
            return {"trading_date": "2026-07-10", "quotes": [{"symbol": "000001.SZ", "price": 10.5}]}

    function = SlowThenFastFunction()
    provider = NotteSidebarProvider(NotteConfig.from_env(timeout_seconds=0.05), function=function, monotonic=lambda: clock[0])

    async def run():
        first = (await provider.get_quotes(("000001.SZ",)))["000001.SZ"]
        await asyncio.sleep(0.16)
        clock[0] += 301
        second = (await provider.get_quotes(("000001.SZ",)))["000001.SZ"]
        assert first.value is None
        assert second.value.price == 10.5

    asyncio.run(run())
    assert function.calls == 2
