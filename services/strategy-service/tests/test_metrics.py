from __future__ import annotations

from datetime import datetime, timezone

from app.backtest.metrics import compute_metrics
from app.domain.models import SymbolInfo, Trade


def test_compute_metrics_counts_and_profit_factor():
    symbol = SymbolInfo(1, "000001.SZ", "000001", "SZ", "平安银行")
    trade_a = Trade(symbol, datetime(2026, 1, 1, tzinfo=timezone.utc), 10.0, "trigger", 80.0, "30f", 9.5, 9.5, {})
    trade_a.exit_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
    trade_a.exit_price = 11.0
    trade_b = Trade(symbol, datetime(2026, 1, 3, tzinfo=timezone.utc), 10.0, "trigger", 80.0, "5f", 9.5, 9.5, {})
    trade_b.exit_time = datetime(2026, 1, 4, tzinfo=timezone.utc)
    trade_b.exit_price = 9.0
    metrics = compute_metrics([trade_a, trade_b])
    assert metrics["total_trades"] == 2
    assert metrics["win_rate"] == 0.5
    assert metrics["median_return"] == 0.0
    assert metrics["entry_level_distribution"] == {"30f": 1, "5f": 1}
