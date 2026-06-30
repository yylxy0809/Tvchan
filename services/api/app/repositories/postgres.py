from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from trading_protocol import normalize_timeframe
from trading_protocol.timeframes import TIMEFRAMES


TIMEFRAME_TO_DB = {code: value.minutes for code, value in TIMEFRAMES.items()}
DB_TO_TIMEFRAME = {value.minutes: code for code, value in TIMEFRAMES.items()}
DERIVED_FROM_5F = {
    "15f": 15,
    "30f": 30,
    "1h": 60,
    "1d": 1440,
}
DERIVED_BASE_BAR_MULTIPLIERS = {
    "15f": 3,
    "30f": 6,
    "1h": 12,
    "1d": 49,
}
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


async def search_symbols_db(pool, keyword: str = "", limit: int = 20) -> list[dict]:
    value = f"%{keyword.strip()}%"
    if keyword.strip():
        rows = await pool.fetch(
            """
            select code, exchange, name, asset_type
            from symbols
            where is_active = true
              and (code ilike $1 or name ilike $1 or (code || '.' || exchange) ilike $1)
            order by code
            limit $2
            """,
            value,
            limit,
        )
    else:
        rows = await pool.fetch(
            """
            select code, exchange, name, asset_type
            from symbols
            where is_active = true
            order by code
            limit $1
            """,
            limit,
        )
    return [_symbol_row_to_dict(row) for row in rows]


async def resolve_symbol_db(pool, symbol: str) -> dict | None:
    code, exchange = split_symbol(symbol)
    row = await pool.fetchrow(
        """
        select code, exchange, name, asset_type
        from symbols
        where code = $1 and exchange = $2 and is_active = true
        """,
        code,
        exchange,
    )
    return _symbol_row_to_dict(row) if row else None


async def get_bars_db(
    pool,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict]:
    normalized = normalize_timeframe(timeframe)
    if normalized in DERIVED_FROM_5F:
        base_limit = _base_limit_for_derived(normalized, limit)
        base_rows = await _fetch_bars_db(pool, symbol, "5f", start, end, base_limit)
        return aggregate_5f_bars(base_rows, normalized)[-limit:]
    return await _fetch_bars_db(pool, symbol, normalized, start, end, limit)


async def _fetch_bars_db(
    pool,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict]:
    normalized = normalize_timeframe(timeframe)
    timeframe_code = TIMEFRAME_TO_DB[normalized]
    code, exchange = split_symbol(symbol)
    symbol_id = await pool.fetchval(
        """
        select id
        from symbols
        where code = $1 and exchange = $2 and is_active = true
        """,
        code,
        exchange,
    )
    if symbol_id is None:
        return []

    lower_bounds = await _candidate_lower_bounds(
        pool,
        symbol_id=symbol_id,
        timeframe_code=timeframe_code,
        timeframe=normalized,
        start=start,
        end=end,
        limit=limit,
    )
    for source_codes in ([2, 3, 4], [1]):
        for lower_bound in lower_bounds:
            rows = await _fetch_bars_by_symbol_id(
                pool,
                symbol_id=symbol_id,
                timeframe_code=timeframe_code,
                start=lower_bound,
                end=end,
                limit=limit,
                source_codes=source_codes,
            )
            if len(rows) >= limit or start is not None or end is not None:
                return [bar_row_to_api_dict(row) for row in reversed(rows)]
            if rows and lower_bound is None:
                return [bar_row_to_api_dict(row) for row in reversed(rows)]
    return []


async def _candidate_lower_bounds(
    pool,
    *,
    symbol_id: int,
    timeframe_code: int,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[datetime | None]:
    if start is not None:
        return [start]
    if end is not None:
        return [None]
    watermark = await pool.fetchval(
        """
        select last_bar_end
        from scheme2_ingest_watermarks
        where symbol_id = $1 and timeframe = $2
        """,
        symbol_id,
        timeframe_code,
    )
    if watermark is None:
        return [None]
    days = _lookback_days(timeframe, limit)
    return [
        watermark - timedelta(days=days),
        watermark - timedelta(days=days * 2),
        watermark - timedelta(days=days * 4),
        None,
    ]


async def _fetch_bars_by_symbol_id(
    pool,
    *,
    symbol_id: int,
    timeframe_code: int,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    source_codes: list[int],
):
    if start is None and end is None:
        return await pool.fetch(
            """
            select
                ts,
                open_x1000,
                high_x1000,
                low_x1000,
                close_x1000,
                volume,
                amount_x100,
                is_complete,
                revision
            from klines
            where symbol_id = $1
              and timeframe = $2
              and source = any($3::smallint[])
            order by ts desc
            limit $4
            """,
            symbol_id,
            timeframe_code,
            source_codes,
            limit,
        )
    if start is None:
        return await pool.fetch(
            """
            select
                ts,
                open_x1000,
                high_x1000,
                low_x1000,
                close_x1000,
                volume,
                amount_x100,
                is_complete,
                revision
            from klines
            where symbol_id = $1
              and timeframe = $2
              and source = any($3::smallint[])
              and ts <= $4
            order by ts desc
            limit $5
            """,
            symbol_id,
            timeframe_code,
            source_codes,
            end,
            limit,
        )
    if end is None:
        return await pool.fetch(
            """
            select
                ts,
                open_x1000,
                high_x1000,
                low_x1000,
                close_x1000,
                volume,
                amount_x100,
                is_complete,
                revision
            from klines
            where symbol_id = $1
              and timeframe = $2
              and source = any($3::smallint[])
              and ts >= $4
            order by ts desc
            limit $5
            """,
            symbol_id,
            timeframe_code,
            source_codes,
            start,
            limit,
        )
    return await pool.fetch(
        """
        select
            ts,
            open_x1000,
            high_x1000,
            low_x1000,
            close_x1000,
            volume,
            amount_x100,
            is_complete,
            revision
        from klines
        where symbol_id = $1
          and timeframe = $2
          and source = any($3::smallint[])
          and ts >= $4
          and ts <= $5
        order by ts desc
        limit $6
        """,
        symbol_id,
        timeframe_code,
        source_codes,
        start,
        end,
        limit,
    )


def _lookback_days(timeframe: str, limit: int) -> int:
    if timeframe == "5f":
        bars_per_day = 48
    elif timeframe == "15f":
        bars_per_day = 16
    elif timeframe == "30f":
        bars_per_day = 8
    elif timeframe == "1h":
        bars_per_day = 4
    elif timeframe == "1d":
        bars_per_day = 1
    else:
        bars_per_day = 1
    return max(30, int(limit / bars_per_day * 2.4) + 20)


def aggregate_5f_bars(bars: list[dict], timeframe: str) -> list[dict]:
    normalized = normalize_timeframe(timeframe)
    minutes = DERIVED_FROM_5F.get(normalized)
    if minutes is None:
        return bars
    if normalized == "1d":
        return _aggregate_5f_daily_bars(bars)

    grouped: dict[int, dict] = {}
    order: list[int] = []
    for bar in sorted(bars, key=lambda item: int(item["time"])):
        slot_time = _slot_end_time(bar["time"], minutes)
        if slot_time is None:
            continue
        item = grouped.get(slot_time)
        amount = bar.get("amount")
        if item is None:
            grouped[slot_time] = {
                "time": slot_time,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": int(bar.get("volume") or 0),
                "amount": amount,
                "complete": bool(bar.get("complete", True)),
                "revision": int(bar.get("revision") or 0),
            }
            order.append(slot_time)
            continue
        item["high"] = max(item["high"], bar["high"])
        item["low"] = min(item["low"], bar["low"])
        item["close"] = bar["close"]
        item["volume"] += int(bar.get("volume") or 0)
        item["complete"] = bool(item["complete"] and bar.get("complete", True))
        item["revision"] = max(int(item["revision"]), int(bar.get("revision") or 0))
        if item["amount"] is None:
            item["amount"] = amount
        elif amount is not None:
            item["amount"] += amount

    return [grouped[key] for key in sorted(order)]


def _aggregate_5f_daily_bars(bars: list[dict]) -> list[dict]:
    grouped: dict[int, dict] = {}
    order: list[int] = []
    for bar in sorted(bars, key=lambda item: int(item["time"])):
        slot_time = _daily_end_time(bar["time"])
        item = grouped.get(slot_time)
        amount = bar.get("amount")
        if item is None:
            grouped[slot_time] = {
                "time": slot_time,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": int(bar.get("volume") or 0),
                "amount": amount,
                "complete": bool(bar.get("complete", True)),
                "revision": int(bar.get("revision") or 0),
                "_last_base_time": int(bar["time"]),
            }
            order.append(slot_time)
            continue
        item["high"] = max(item["high"], bar["high"])
        item["low"] = min(item["low"], bar["low"])
        item["close"] = bar["close"]
        item["volume"] += int(bar.get("volume") or 0)
        item["complete"] = bool(item["complete"] and bar.get("complete", True))
        item["revision"] = max(int(item["revision"]), int(bar.get("revision") or 0))
        item["_last_base_time"] = int(bar["time"])
        if item["amount"] is None:
            item["amount"] = amount
        elif amount is not None:
            item["amount"] += amount

    result = []
    for key in sorted(order):
        item = grouped[key]
        last_base_time = item.pop("_last_base_time")
        item["complete"] = bool(item["complete"] and _is_daily_close(last_base_time))
        result.append(item)
    return result


def _daily_end_time(epoch_seconds: int) -> int:
    dt = datetime.fromtimestamp(epoch_seconds, tz=SHANGHAI_TZ)
    slot_dt = datetime.combine(dt.date(), time(hour=15, minute=0), tzinfo=SHANGHAI_TZ)
    return int(slot_dt.timestamp())


def _is_daily_close(epoch_seconds: int) -> bool:
    dt = datetime.fromtimestamp(epoch_seconds, tz=SHANGHAI_TZ)
    return dt.hour * 60 + dt.minute >= 15 * 60


def _slot_end_time(epoch_seconds: int, minutes: int) -> int | None:
    dt = datetime.fromtimestamp(epoch_seconds, tz=SHANGHAI_TZ)
    total_minutes = dt.hour * 60 + dt.minute
    slot_minutes = _session_slot_minutes(total_minutes, minutes)
    if slot_minutes is None:
        return None
    slot_dt = datetime.combine(
        dt.date(),
        time(hour=slot_minutes // 60, minute=slot_minutes % 60),
        tzinfo=SHANGHAI_TZ,
    )
    return int(slot_dt.timestamp())


def _session_slot_minutes(total_minutes: int, minutes: int) -> int | None:
    morning_start = 9 * 60 + 30
    morning_end = 11 * 60 + 30
    afternoon_start = 13 * 60
    afternoon_end = 15 * 60
    if morning_start <= total_minutes <= morning_end:
        return _ceil_to_slot(morning_start, total_minutes, minutes)
    if afternoon_start <= total_minutes <= afternoon_end:
        return _ceil_to_slot(afternoon_start, total_minutes, minutes)
    return None


def _ceil_to_slot(session_start: int, total_minutes: int, minutes: int) -> int:
    elapsed = total_minutes - session_start
    if elapsed <= 0:
        return session_start + minutes
    return session_start + ((elapsed + minutes - 1) // minutes) * minutes


def _base_limit_for_derived(timeframe: str, limit: int) -> int:
    multiplier = DERIVED_BASE_BAR_MULTIPLIERS[timeframe]
    return max(limit, limit * multiplier + 64)


def split_symbol(symbol: str) -> tuple[str, str]:
    normalized = symbol.strip().upper()
    if "." in normalized:
        code, exchange = normalized.split(".", 1)
        return code, exchange
    if normalized.startswith("6"):
        return normalized, "SH"
    return normalized, "SZ"


def bar_row_to_api_dict(row) -> dict:
    return {
        "time": int(row["ts"].timestamp()),
        "open": row["open_x1000"] / 1000,
        "high": row["high_x1000"] / 1000,
        "low": row["low_x1000"] / 1000,
        "close": row["close_x1000"] / 1000,
        "volume": row["volume"],
        "amount": None if row["amount_x100"] is None else row["amount_x100"] / 100,
        "complete": row["is_complete"],
        "revision": row["revision"],
    }


def _symbol_row_to_dict(row) -> dict:
    return {
        "symbol": f"{row['code']}.{row['exchange']}",
        "code": row["code"],
        "exchange": row["exchange"],
        "name": row["name"],
        "asset_type": row["asset_type"],
    }
