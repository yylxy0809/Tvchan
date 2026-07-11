from __future__ import annotations

from datetime import datetime

from app.repositories.kline_repo import KlineBar


def latest_bottom_fractal_time(bars: list[KlineBar], *, after: datetime | None = None) -> datetime | None:
    latest: datetime | None = None
    for index in range(1, len(bars) - 1):
        left, mid, right = bars[index - 1], bars[index], bars[index + 1]
        if after is not None and right.ts <= after:
            continue
        if mid.low < left.low and mid.low < right.low:
            latest = right.ts
    return latest


def latest_top_fractal_time(bars: list[KlineBar], *, after: datetime | None = None) -> datetime | None:
    latest: datetime | None = None
    for index in range(1, len(bars) - 1):
        left, mid, right = bars[index - 1], bars[index], bars[index + 1]
        if after is not None and right.ts <= after:
            continue
        if mid.high > left.high and mid.high > right.high:
            latest = right.ts
    return latest
