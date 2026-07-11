from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from collector.providers.pool import BarQualityError, MarketDataPool, validate_bars
from trading_protocol import Bar, SymbolInfo


class FakeProvider(MarketDataProvider):
    def __init__(self, name: str, bars: list[Bar] | None = None, error: Exception | None = None) -> None:
        self.name = name
        self._bars = bars or []
        self._error = error

    async def list_symbols(self) -> list[SymbolInfo]:
        return [SymbolInfo("000001.SZ", "000001", "SZ", "平安银行")]

    async def get_bars(self, symbol, timeframe, start=None, end=None, limit=300):
        if self._error is not None:
            raise self._error
        return self._bars

    async def healthcheck(self) -> ProviderHealth:
        return ProviderHealth(self.name, self._error is None, "")


def bar(minute: int, *, source: str = "fake", high: float = 10.2, low: float = 9.8) -> Bar:
    return Bar(
        symbol="000001.SZ",
        timeframe="5f",
        ts=datetime(2024, 6, 3, 9, minute, tzinfo=UTC),
        open=10.0,
        high=high,
        low=low,
        close=10.1,
        volume=100,
        source=source,
    )


def test_market_data_pool_primary_failover_uses_first_valid_source() -> None:
    valid = [bar(35, source="backup")]
    pool = MarketDataPool(
        [
            FakeProvider("primary", error=RuntimeError("blocked")),
            FakeProvider("backup", bars=valid),
        ]
    )

    result = asyncio.run(pool.get_bars("000001.SZ", "5f", limit=1))

    assert result == valid
    report = pool.report_for("000001.SZ", "5f")
    assert report is not None
    assert report.winning_source == "backup"
    assert [attempt.status for attempt in report.attempts] == ["failed", "success"]
    assert report.attempts[1].winner is True


def test_market_data_pool_keeps_quality_failed_bars_for_snapshot() -> None:
    invalid = [bar(35, source="bad", high=9.9, low=10.2)]
    valid = [bar(35, source="backup")]
    pool = MarketDataPool(
        [
            FakeProvider("bad", bars=invalid),
            FakeProvider("backup", bars=valid),
        ]
    )

    asyncio.run(pool.get_bars("000001.SZ", "5f", limit=1))

    report = pool.report_for("000001.SZ", "5f")
    assert report is not None
    assert report.attempts[0].status == "quality_failed"
    assert report.attempts[0].bars == invalid
    assert report.attempts[0].quality_flags == {"invalid_ohlc_at": 0}


def test_validate_bars_rejects_duplicate_or_unordered_bars() -> None:
    with pytest.raises(BarQualityError, match="duplicate"):
        validate_bars(symbol="000001.SZ", timeframe="5f", bars=[bar(35), bar(35)])
