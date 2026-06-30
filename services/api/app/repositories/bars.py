from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from trading_protocol import Bar, SymbolInfo, normalize_timeframe
from trading_protocol.timeframes import timeframe_minutes

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

SEED_SYMBOLS: list[SymbolInfo] = [
    SymbolInfo("000001.SZ", "000001", "SZ", "平安银行"),
    SymbolInfo("000002.SZ", "000002", "SZ", "万科A"),
    SymbolInfo("000063.SZ", "000063", "SZ", "中兴通讯"),
    SymbolInfo("000333.SZ", "000333", "SZ", "美的集团"),
    SymbolInfo("000651.SZ", "000651", "SZ", "格力电器"),
    SymbolInfo("600000.SH", "600000", "SH", "浦发银行"),
    SymbolInfo("600519.SH", "600519", "SH", "贵州茅台"),
    SymbolInfo("600887.SH", "600887", "SH", "伊利股份"),
    SymbolInfo("601318.SH", "601318", "SH", "中国平安"),
    SymbolInfo("601398.SH", "601398", "SH", "工商银行"),
]


def search_symbols(keyword: str = "", limit: int = 20) -> list[SymbolInfo]:
    value = keyword.strip().lower()
    if not value:
        return SEED_SYMBOLS[:limit]
    matches = [
        item
        for item in SEED_SYMBOLS
        if value in item.symbol.lower()
        or value in item.code.lower()
        or value in item.name.lower()
    ]
    return matches[:limit]


def resolve_symbol(symbol: str) -> SymbolInfo | None:
    normalized = symbol.strip().upper()
    return next((item for item in SEED_SYMBOLS if item.symbol == normalized), None)


def generate_seed_bars(
    symbol: str,
    timeframe: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 300,
) -> list[Bar]:
    symbol_info = resolve_symbol(symbol)
    if symbol_info is None:
        return []

    tf = normalize_timeframe(timeframe)
    step_minutes = timeframe_minutes(tf)
    now = datetime.now(SHANGHAI_TZ).replace(second=0, microsecond=0)
    end_dt = _coerce_datetime(end) or now
    start_dt = _coerce_datetime(start) or end_dt - _default_window(tf)
    timestamps = _generate_timestamps(start_dt, end_dt, step_minutes, limit)
    base = _base_price(symbol_info.symbol)

    bars: list[Bar] = []
    previous_close = base
    for index, ts in enumerate(timestamps):
        wave = math.sin(index / 8) * 0.08 + math.cos(index / 19) * 0.04
        drift = index * 0.002
        open_price = max(0.01, previous_close)
        close_price = max(0.01, base + wave + drift)
        high = max(open_price, close_price) + 0.03 + (index % 5) * 0.005
        low = min(open_price, close_price) - 0.03 - (index % 3) * 0.004
        volume = 100_000 + (index % 37) * 7_300 + _symbol_seed(symbol_info.symbol) * 17
        amount = volume * close_price
        bars.append(
            Bar(
                symbol=symbol_info.symbol,
                timeframe=tf,
                ts=ts,
                open=round(open_price, 3),
                high=round(high, 3),
                low=round(max(0.01, low), 3),
                close=round(close_price, 3),
                volume=volume,
                amount=round(amount, 2),
                complete=ts < now,
                revision=0 if ts < now else 1,
                source="seed",
            )
        )
        previous_close = close_price
    return bars


def _coerce_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ)


def _default_window(timeframe: str) -> timedelta:
    if timeframe in {"5f", "15f", "30f", "1h"}:
        return timedelta(days=20)
    if timeframe == "1d":
        return timedelta(days=365)
    if timeframe == "1w":
        return timedelta(days=365 * 3)
    return timedelta(days=365 * 8)


def _generate_timestamps(
    start: datetime, end: datetime, step_minutes: int, limit: int
) -> list[datetime]:
    if step_minutes == 10080:
        return _generate_weekly_timestamps(start, end, limit)
    if step_minutes == 43200:
        return _generate_monthly_timestamps(start, end, limit)

    timestamps: list[datetime] = []
    start = start.astimezone(SHANGHAI_TZ)
    end = end.astimezone(SHANGHAI_TZ)
    step = timedelta(minutes=step_minutes)
    current = _align_end(end, step_minutes)
    while current >= start and len(timestamps) < limit:
        if _is_trading_timestamp(current, step_minutes):
            timestamps.append(current)
        current -= step
    return list(reversed(timestamps))


def _generate_weekly_timestamps(
    start: datetime, end: datetime, limit: int
) -> list[datetime]:
    start = start.astimezone(SHANGHAI_TZ)
    current = _align_end(end, 1440)
    current -= timedelta(days=current.weekday())
    timestamps: list[datetime] = []
    while current >= start and len(timestamps) < limit:
        timestamps.append(current)
        current -= timedelta(days=7)
    return list(reversed(timestamps))


def _generate_monthly_timestamps(
    start: datetime, end: datetime, limit: int
) -> list[datetime]:
    start = start.astimezone(SHANGHAI_TZ)
    cursor = end.astimezone(SHANGHAI_TZ).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    timestamps: list[datetime] = []
    while cursor >= start and len(timestamps) < limit:
        timestamps.append(_first_weekday_of_month(cursor))
        cursor = _previous_month(cursor)
    return list(reversed(timestamps))


def _first_weekday_of_month(value: datetime) -> datetime:
    current = value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _previous_month(value: datetime) -> datetime:
    if value.month == 1:
        return value.replace(year=value.year - 1, month=12)
    return value.replace(month=value.month - 1)


def _align_end(value: datetime, step_minutes: int) -> datetime:
    if step_minutes >= 1440:
        aligned = value.replace(hour=0, minute=0, second=0, microsecond=0)
        while aligned.weekday() >= 5:
            aligned -= timedelta(days=1)
        return aligned
    minute_of_day = value.hour * 60 + value.minute
    aligned_minute = minute_of_day - (minute_of_day % step_minutes)
    return value.replace(
        hour=aligned_minute // 60,
        minute=aligned_minute % 60,
        second=0,
        microsecond=0,
    )


def _is_trading_timestamp(value: datetime, step_minutes: int) -> bool:
    if step_minutes >= 1440:
        return value.weekday() < 5
    if value.weekday() >= 5:
        return False
    minutes = value.hour * 60 + value.minute
    morning = 9 * 60 + 30 <= minutes <= 11 * 60 + 30
    afternoon = 13 * 60 <= minutes <= 15 * 60
    return morning or afternoon


def _base_price(symbol: str) -> float:
    seed = _symbol_seed(symbol)
    if symbol == "600519.SH":
        return 1500 + seed % 300
    return 5 + (seed % 800) / 20


def _symbol_seed(symbol: str) -> int:
    return sum((index + 1) * ord(char) for index, char in enumerate(symbol))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
