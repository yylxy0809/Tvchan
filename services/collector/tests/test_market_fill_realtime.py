from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

import collector.market_fill as market_fill
from trading_protocol import Bar, SymbolInfo


def _bar(*, close: float, revision: int, source: str) -> Bar:
    return Bar(
        symbol="000001.SZ",
        timeframe="5f",
        ts=datetime(2026, 7, 21, 1, 35, tzinfo=UTC),
        open=10,
        high=11,
        low=9,
        close=close,
        volume=100,
        revision=revision,
        source=source,
    )


def test_market_fill_publishes_the_database_canonical_winner(monkeypatch) -> None:
    provider_bar = _bar(close=10.2, revision=1, source="pytdx")
    canonical_bar = _bar(close=10.8, revision=4, source="parquet_5f")

    class Provider:
        async def get_bars(self, *_args, **_kwargs):
            return [provider_bar]

    class Writer:
        async def upsert_bars(self, bars):
            assert bars == [provider_bar]
            return 1

        async def get_canonical_bar(self, symbol, timeframe, timestamp):
            assert (symbol, timeframe, timestamp) == (
                "000001.SZ",
                "5f",
                provider_bar.ts,
            )
            return canonical_bar

    published: list[Bar] = []

    async def publish(**kwargs):
        published.append(kwargs["bar"])
        return True

    monkeypatch.setattr(market_fill, "publish_bar_update", publish)

    written = asyncio.run(
        market_fill.fill_one_timeframe(
            provider=Provider(),
            kline_writer=Writer(),
            symbol=SymbolInfo(
                symbol="000001.SZ", code="000001", exchange="SZ", name="Ping An"
            ),
            timeframe="5f",
            limit=1,
            sleep=0,
            skip_publish=False,
            redis_url="redis://unused",
        )
    )

    assert written == 1
    assert published == [canonical_bar]


def test_market_fill_fails_closed_when_redis_publish_fails(monkeypatch) -> None:
    provider_bar = _bar(close=10.2, revision=1, source="pytdx")

    class Provider:
        async def get_bars(self, *_args, **_kwargs):
            return [provider_bar]

    class Writer:
        async def upsert_bars(self, _bars):
            return 1

        async def get_canonical_bar(self, *_args):
            return provider_bar

    async def failed_publish(**_kwargs):
        return False

    monkeypatch.setattr(market_fill, "publish_bar_update", failed_publish)

    with pytest.raises(RuntimeError, match="Redis bar publication failed"):
        asyncio.run(
            market_fill.fill_one_timeframe(
                provider=Provider(),
                kline_writer=Writer(),
                symbol=SymbolInfo(
                    symbol="000001.SZ", code="000001", exchange="SZ", name="Ping An"
                ),
                timeframe="5f",
                limit=1,
                sleep=0,
                skip_publish=False,
                redis_url="redis://unused",
            )
        )
