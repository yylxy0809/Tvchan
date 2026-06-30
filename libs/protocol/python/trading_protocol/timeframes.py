from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Timeframe:
    code: str
    minutes: int
    tradingview_resolution: str
    label: str


TIMEFRAMES: dict[str, Timeframe] = {
    "5f": Timeframe("5f", 5, "5", "5 minutes"),
    "15f": Timeframe("15f", 15, "15", "15 minutes"),
    "30f": Timeframe("30f", 30, "30", "30 minutes"),
    "1h": Timeframe("1h", 60, "60", "1 hour"),
    "1d": Timeframe("1d", 1440, "D", "1 day"),
    "1w": Timeframe("1w", 10080, "W", "1 week"),
    "1m": Timeframe("1m", 43200, "M", "1 month"),
}

TRADINGVIEW_TO_TIMEFRAME = {
    value.tradingview_resolution: value.code for value in TIMEFRAMES.values()
}


def normalize_timeframe(value: str) -> str:
    normalized = value.strip()
    if normalized in TIMEFRAMES:
        return normalized
    upper = normalized.upper()
    if upper in TRADINGVIEW_TO_TIMEFRAME:
        return TRADINGVIEW_TO_TIMEFRAME[upper]
    raise ValueError(f"Unsupported timeframe: {value}")


def timeframe_minutes(value: str) -> int:
    return TIMEFRAMES[normalize_timeframe(value)].minutes

