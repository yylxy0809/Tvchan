from __future__ import annotations

from trading_protocol import analyze_chan_placeholder


def test_placeholder_signals_are_not_capped_to_recent_items() -> None:
    prices = [10, 12, 9, 13, 8, 14, 7, 15, 6, 16, 5, 17, 4]
    bars = [
        {
            "time": 1_700_000_000 + index * 300,
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price,
            "volume": 1000,
            "complete": True,
            "revision": 1,
        }
        for index, price in enumerate(prices)
    ]

    result = analyze_chan_placeholder(
        symbol="000001.SZ",
        level="5f",
        modes=["confirmed"],
        bars=bars,
    )

    assert len(result.strokes) > 3
    assert len(result.signals) == len(result.strokes)
