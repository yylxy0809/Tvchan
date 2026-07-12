from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from collector.market_data import Freshness
from collector.market_data.local_quotes import LocalPostgresQuoteSource


class Connection:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch(self, sql, symbols):
        self.calls.append((sql, symbols))
        return self.rows


class Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return None


class Pool:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False

    def acquire(self):
        return Acquire(self.connection)

    async def close(self):
        self.closed = True


def test_local_quotes_reads_requested_symbols_in_one_bounded_query_and_calculates_daily_change() -> None:
    provider_ts = datetime(2026, 7, 12, 1, 35, tzinfo=UTC)
    connection = Connection([
        {
            "code": "000001",
            "exchange": "SZ",
            "provider_ts": provider_ts,
            "close_x1000": 10_500,
            "volume": 123,
            "amount_x100": 456_700,
            "previous_close_x1000": 10_000,
        }
    ])
    pool = Pool(connection)

    async def create_pool(url):
        assert url == "postgresql://local"
        return pool

    source = LocalPostgresQuoteSource(
        "postgresql://local",
        pool_factory=create_pool,
        now=lambda: datetime(2026, 7, 12, 1, 40, tzinfo=UTC),
    )
    results = asyncio.run(source.get_quotes(["000001.sz", "600000.SH", "000001.SZ"]))

    quote = results["000001.SZ"]
    assert connection.calls and connection.calls[0][1] == ["000001.SZ", "600000.SH"]
    assert len(connection.calls) == 1
    sql = connection.calls[0][0].lower()
    assert "revision desc" in sql
    assert "updated_at desc" in sql
    assert "case" in sql
    assert "interval '45 days'" in sql
    assert "interval '120 days'" in sql
    assert quote.value is not None
    assert quote.value.price == 10.5
    assert quote.value.change == 0.5
    assert quote.value.change_percent == 5
    assert quote.value.volume == 123
    assert quote.value.amount == 4567
    assert quote.metadata.provider_ts == provider_ts
    assert quote.metadata.freshness is Freshness.LIVE
    assert results["600000.SH"].value is None
    assert results["600000.SH"].error == "canonical_quote_unavailable"

    asyncio.run(source.close())
    assert pool.closed is True


def test_local_quotes_do_not_guess_when_daily_previous_close_or_symbol_is_missing() -> None:
    connection = Connection([
        {
            "code": "000001",
            "exchange": "SZ",
            "provider_ts": datetime(2026, 7, 12, 1, 35, tzinfo=UTC),
            "close_x1000": 10_500,
            "volume": 123,
            "amount_x100": None,
            "previous_close_x1000": None,
        }
    ])
    pool = Pool(connection)

    async def create_pool(_):
        return pool

    source = LocalPostgresQuoteSource("postgresql://local", pool_factory=create_pool)
    results = asyncio.run(source.get_quotes(["000001.SZ", "not-a-symbol"]))

    assert results["000001.SZ"].value is None
    assert results["000001.SZ"].error == "canonical_quote_unavailable"
    assert results["NOT-A-SYMBOL"].value is None
    assert results["NOT-A-SYMBOL"].error == "invalid_symbol"
