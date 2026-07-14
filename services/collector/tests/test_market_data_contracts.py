from datetime import UTC, date, datetime
import pytest
from collector.market_data import Freshness, MarketDataMetadata, MarketDataResult, ProviderError

def test_external_sidebar_metadata_accepts_iwencai_or_notte_and_fresh_is_trading_day_bound():
    assert MarketDataMetadata(source="notte").source == "notte"
    with pytest.raises(ValueError): MarketDataMetadata(source="westock")
    with pytest.raises(ValueError): MarketDataMetadata(freshness=Freshness.FRESH)
    result = MarketDataResult.available("value", trading_date=date(2026, 7, 10), provider_ts=datetime(2026, 7, 10, tzinfo=UTC))
    assert result.metadata.source == "iwencai"
    assert result.as_stale(ProviderError.TIMEOUT).metadata.freshness is Freshness.STALE
