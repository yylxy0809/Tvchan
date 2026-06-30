from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.repositories.postgres import aggregate_5f_bars


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def test_aggregate_5f_bars_to_30f_uses_a_share_session_end_time() -> None:
    rows = [
        _bar("2026-04-27 09:35", open=11.33, high=11.57, low=11.29, close=11.48, volume=10),
        _bar("2026-04-27 09:40", open=11.48, high=11.53, low=11.46, close=11.51, volume=20),
        _bar("2026-04-27 09:45", open=11.51, high=11.54, low=11.49, close=11.50, volume=30),
        _bar("2026-04-27 09:50", open=11.50, high=11.53, low=11.48, close=11.51, volume=40),
        _bar("2026-04-27 09:55", open=11.51, high=11.53, low=11.46, close=11.46, volume=50),
        _bar("2026-04-27 10:00", open=11.47, high=11.49, low=11.46, close=11.46, volume=60),
    ]

    result = aggregate_5f_bars(rows, "30f")

    assert len(result) == 1
    assert _format_time(result[0]["time"]) == "2026-04-27 10:00"
    assert result[0]["open"] == 11.33
    assert result[0]["high"] == 11.57
    assert result[0]["low"] == 11.29
    assert result[0]["close"] == 11.46
    assert result[0]["volume"] == 210


def test_aggregate_5f_bars_to_15f_splits_morning_slots() -> None:
    rows = [
        _bar("2026-04-27 09:35", close=1),
        _bar("2026-04-27 09:40", close=2),
        _bar("2026-04-27 09:45", close=3),
        _bar("2026-04-27 09:50", close=4),
        _bar("2026-04-27 09:55", close=5),
        _bar("2026-04-27 10:00", close=6),
    ]

    result = aggregate_5f_bars(rows, "15f")

    assert [_format_time(item["time"]) for item in result] == [
        "2026-04-27 09:45",
        "2026-04-27 10:00",
    ]
    assert [item["close"] for item in result] == [3, 6]


def test_aggregate_5f_bars_to_1h_handles_afternoon_session() -> None:
    rows = [
        _bar("2026-04-27 13:05", close=1),
        _bar("2026-04-27 13:30", close=2),
        _bar("2026-04-27 14:00", close=3),
        _bar("2026-04-27 14:05", close=4),
        _bar("2026-04-27 14:30", close=5),
        _bar("2026-04-27 15:00", close=6),
    ]

    result = aggregate_5f_bars(rows, "1h")

    assert [_format_time(item["time"]) for item in result] == [
        "2026-04-27 14:00",
        "2026-04-27 15:00",
    ]
    assert [item["close"] for item in result] == [3, 6]


def test_aggregate_5f_bars_to_1d_uses_bar_end_market_close() -> None:
    rows = [
        _bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1, volume=10),
        _bar("2026-04-27 11:30", open=10.1, high=10.5, low=10.0, close=10.4, volume=20),
        _bar("2026-04-27 13:05", open=10.4, high=10.6, low=10.3, close=10.5, volume=30),
        _bar("2026-04-27 15:00", open=10.5, high=10.8, low=10.4, close=10.7, volume=40),
    ]

    result = aggregate_5f_bars(rows, "1d")

    assert len(result) == 1
    assert _format_time(result[0]["time"]) == "2026-04-27 15:00"
    assert result[0]["open"] == 10.0
    assert result[0]["high"] == 10.8
    assert result[0]["low"] == 9.9
    assert result[0]["close"] == 10.7
    assert result[0]["volume"] == 100
    assert result[0]["complete"] is True


def test_aggregate_5f_bars_to_1d_marks_partial_day_incomplete() -> None:
    rows = [
        _bar("2026-04-27 09:35", close=1),
        _bar("2026-04-27 11:30", close=2),
    ]

    result = aggregate_5f_bars(rows, "1d")

    assert _format_time(result[0]["time"]) == "2026-04-27 15:00"
    assert result[0]["close"] == 2
    assert result[0]["complete"] is False


def _bar(
    value: str,
    *,
    open: float = 1,
    high: float = 1,
    low: float = 1,
    close: float = 1,
    volume: int = 1,
) -> dict:
    ts = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI_TZ)
    return {
        "time": int(ts.timestamp()),
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": None,
        "complete": True,
        "revision": 0,
    }


def _format_time(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M")
