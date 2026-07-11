from __future__ import annotations

from datetime import UTC, datetime

from app.domain.models import SymbolInfo
from app.engine.module_c_history_backfill import build_backfill_dry_run
from app.repositories.kline_repo import KlineBar


def _bar(ts: str) -> KlineBar:
    value = datetime.fromisoformat(ts).replace(tzinfo=UTC)
    return KlineBar(ts=value, open=1.0, high=1.0, low=1.0, close=1.0, volume=1)


def test_backfill_dry_run_counts_snapshots_per_level():
    symbol = SymbolInfo(symbol_id=1, symbol="000001.SZ", code="000001", exchange="SZ", name="平安银行")
    bars_by_symbol = {
        "000001.SZ": {
            "5f": [_bar("2026-01-02T07:00:00")],
            "30f": [_bar("2026-01-02T07:00:00")],
            "1d": [_bar("2026-01-02T07:00:00")],
            "1w": [_bar("2026-01-02T07:00:00")],
            "1m": [_bar("2026-01-02T07:00:00")],
        }
    }

    payload = build_backfill_dry_run(
        symbols=[symbol],
        bars_by_symbol=bars_by_symbol,
        profile="research_daily_close",
        warmup_start=datetime(2025, 1, 1, tzinfo=UTC),
        backtest_start=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 1, 31, tzinfo=UTC),
        levels=("5f", "30f", "1d", "1w", "1m"),
        mode="predictive",
    )

    assert payload["estimated_symbols"] == 1
    assert payload["estimated_total_runs"] == 5
    assert payload["estimated_snapshots_by_level"]["1d"] == 1
