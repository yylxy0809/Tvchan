from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from trading_protocol import (
    code_to_source,
    canonical_kline_timestamp,
    kline_logical_key,
    normalize_timeframe,
    source_priority,
    source_priority_with_coverage,
    source_priority_with_coverage_sql,
)
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
READER_PHYSICAL_ROW_GUARD = 32
PERIOD_CACHE_TIMEFRAMES = {10080, 43200}


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
    end_exclusive: bool = False,
) -> list[dict]:
    normalized = normalize_timeframe(timeframe)
    if normalized in DERIVED_FROM_5F:
        stored_rows = await _fetch_bars_db(pool, symbol, normalized, start, end, limit, end_exclusive)
        if stored_rows:
            return stored_rows
        base_limit = _base_limit_for_derived(normalized, limit)
        base_rows = await _fetch_bars_db(pool, symbol, "5f", start, end, base_limit, end_exclusive)
        return aggregate_5f_bars(base_rows, normalized)[-limit:]
    return await _fetch_bars_db(pool, symbol, normalized, start, end, limit, end_exclusive)


async def _fetch_bars_db(
    pool,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    end_exclusive: bool,
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

    if timeframe_code in PERIOD_CACHE_TIMEFRAMES:
        cached_rows = await _fetch_period_chart_cache(
            pool,
            symbol_id=symbol_id,
            timeframe_code=timeframe_code,
            start=start,
            end=end,
            limit=limit,
            end_exclusive=end_exclusive,
        )
        if len(cached_rows) >= limit or (
            start is not None
            and cached_rows
            and (cache_floor := await _period_chart_cache_floor(pool, symbol_id, timeframe_code)) is not None
            and start >= cache_floor
        ):
            return [bar_row_to_api_dict(row) for row in reversed(cached_rows)]

    lower_bounds = await _candidate_lower_bounds(
        pool,
        symbol_id=symbol_id,
        timeframe_code=timeframe_code,
        timeframe=normalized,
        start=start,
        end=end,
        limit=limit,
    )
    for source_codes in ([2, 3, 4, 5, 6, 7, 8, 9], [1]):
        for index, lower_bound in enumerate(lower_bounds):
            rows = await _fetch_bars_by_symbol_id(
                pool,
                symbol_id=symbol_id,
                timeframe_code=timeframe_code,
                start=lower_bound,
                end=end,
                limit=limit,
                source_codes=source_codes,
                end_exclusive=end_exclusive,
            )
            if (
                len(rows) >= limit
                or start is not None
                or (rows and index == len(lower_bounds) - 1)
            ):
                return [bar_row_to_api_dict(row) for row in reversed(rows)]
            if rows and lower_bound is None:
                return [bar_row_to_api_dict(row) for row in reversed(rows)]
    return []


async def _fetch_period_chart_cache(
    pool,
    *,
    symbol_id: int,
    timeframe_code: int,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    end_exclusive: bool,
):
    """Read the compact weekly/monthly chart projection when it covers a request."""
    try:
        return await pool.fetch(
            """
            select ts, open_x1000, high_x1000, low_x1000, close_x1000,
                   volume, amount_x100, is_complete, revision
            from chart_period_bars
            where symbol_id = $1
              and timeframe = $2
              and ($3::timestamptz is null or ts >= $3)
              and ($4::timestamptz is null or ($6::boolean and ts < $4) or (not $6::boolean and ts <= $4))
            order by ts desc
            limit $5
            """,
            symbol_id,
            timeframe_code,
            start,
            end,
            limit,
            end_exclusive,
        )
    except Exception as exc:
        # Permit a rolling deployment where the API restarts before migration
        # 026 has been applied; the canonical hypertable remains authoritative.
        if getattr(exc, "sqlstate", None) == "42P01":
            return []
        raise


async def _period_chart_cache_floor(pool, symbol_id: int, timeframe_code: int) -> datetime | None:
    try:
        return await pool.fetchval(
            "select min(ts) from chart_period_bars where symbol_id = $1 and timeframe = $2",
            symbol_id,
            timeframe_code,
        )
    except Exception as exc:
        if getattr(exc, "sqlstate", None) == "42P01":
            return None
        raise


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
        anchor = end
    else:
        watermark = await _ingest_watermark(
            pool,
            symbol_id=symbol_id,
            timeframe_code=timeframe_code,
        )
        anchor = watermark or datetime.now(tz=SHANGHAI_TZ)
    days = _lookback_days(timeframe, limit)
    return [
        _subtract_lookback(anchor, days),
        _subtract_lookback(anchor, days * 2),
        _subtract_lookback(anchor, days * 4),
        None,
    ]


def _subtract_lookback(anchor: datetime, days: int) -> datetime:
    try:
        return anchor - timedelta(days=days)
    except OverflowError:
        return datetime.min.replace(tzinfo=anchor.tzinfo)


async def _ingest_watermark(pool, *, symbol_id: int, timeframe_code: int) -> datetime | None:
    return await pool.fetchval(
        """
        select last_bar_end
        from scheme2_ingest_watermarks
        where symbol_id = $1 and timeframe = $2
        """,
        symbol_id,
        timeframe_code,
    )


async def _fetch_bars_by_symbol_id(
    pool,
    *,
    symbol_id: int,
    timeframe_code: int,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    source_codes: list[int],
    end_exclusive: bool,
):
    # Canonicalization can remove a small number of duplicate physical rows
    # after SQL has applied its limit. Fetch a bounded guard to avoid widening
    # the time range solely because the requested row count was short by that
    # duplicate count.
    raw_limit = min(5000, limit + max(READER_PHYSICAL_ROW_GUARD, limit // 10))
    is_period_timeframe = timeframe_code in {10080, 43200}
    period_cte = """
        ), daily_period_ends as (
            select case when $2 = 10080 then date_trunc('week', daily.ts at time zone 'Asia/Shanghai')
                        else date_trunc('month', daily.ts at time zone 'Asia/Shanghai') end as period_key,
                   max((date_trunc('day', daily.ts at time zone 'Asia/Shanghai') + interval '15 hours') at time zone 'Asia/Shanghai') as final_ts
            from klines daily
            where daily.symbol_id = $1 and daily.timeframe = 1440 and daily.source = any($3::smallint[])
              and ($4::timestamptz is null or daily.ts >= $4 - interval '7 days')
              and ($5::timestamptz is null or daily.ts <= $5)
            group by 1
    """ if is_period_timeframe else ""
    period_join = "left join daily_period_ends period_ends on period_ends.period_key = canonical.period_key" if is_period_timeframe else ""
    final_ts = "period_ends.final_ts" if is_period_timeframe else "null::timestamptz"
    query = f"""
        with canonical as (
            select
                case when timeframe in (1440, 10080, 43200)
                    then (date_trunc('day', ts at time zone 'Asia/Shanghai') + interval '15 hours') at time zone 'Asia/Shanghai'
                else ts end as ts,
                case when timeframe = 10080 then date_trunc('week', ts at time zone 'Asia/Shanghai')
                     when timeframe = 43200 then date_trunc('month', ts at time zone 'Asia/Shanghai')
                     else case when timeframe = 1440 then date_trunc('day', ts at time zone 'Asia/Shanghai') else ts at time zone 'Asia/Shanghai' end end as period_key,
                open_x1000, high_x1000, low_x1000, close_x1000, volume, amount_x100,
                is_complete, revision, source, updated_at
            from klines
            where symbol_id = $1
              and timeframe = $2
              and source = any($3::smallint[])
              -- Keep the canonical filter below authoritative, but bound the raw
              -- scan so Timescale can exclude historical chunks first.
              and (
                  $4::timestamptz is null
                  or ts >= case when $2 in (1440, 10080, 43200)
                      then date_trunc('day', $4 at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai'
                      else $4
                  end
              )
        {period_cte}), covered as (
            select canonical.*, max(coverage.covered_until) as parquet_coverage_end, {final_ts} as final_ts
            from canonical
            left join kline_source_coverage coverage
              on coverage.symbol_id = $1
             and coverage.timeframe = $2
             and coverage.source in (4, 9)
            {period_join}
            group by canonical.ts, canonical.open_x1000, canonical.high_x1000,
                     canonical.low_x1000, canonical.close_x1000, canonical.volume,
                     canonical.amount_x100, canonical.is_complete, canonical.revision,
                     canonical.source, canonical.updated_at, canonical.period_key, {final_ts}
        ), scoped as (
            select * from covered
            where ($4::timestamptz is null or ts >= $4)
              and ($5::timestamptz is null or ($7::boolean and ts < $5) or (not $7::boolean and ts <= $5))
              and ($2 <> 10080 or period_key < date_trunc('week', now() at time zone 'Asia/Shanghai'))
              and ($2 <> 43200 or period_key < date_trunc('month', now() at time zone 'Asia/Shanghai'))
        ), ranked as (
            select *, row_number() over (
                partition by period_key
                order by ({source_priority_with_coverage_sql('source', 'ts', 'parquet_coverage_end')}) desc,
                         is_complete desc, revision desc, updated_at desc
            ) as rn
            from scoped
        )
        select
            coalesce(final_ts, ts) as ts,
            open_x1000,
            high_x1000,
            low_x1000,
            close_x1000,
            volume,
            amount_x100,
            is_complete,
            revision,
            source,
            updated_at
        from ranked
        where rn = 1
    """
    rows = await pool.fetch(
        f"{query} order by ts desc limit $6",
        symbol_id,
        timeframe_code,
        source_codes,
        start,
        end,
        raw_limit,
        end_exclusive,
    )
    return canonicalize_bar_rows(DB_TO_TIMEFRAME[timeframe_code], rows)[:limit]


def canonicalize_bar_rows(timeframe: str, rows, *, parquet_coverage_end: datetime | None = None) -> list[dict]:
    """Temporary reader guard for legacy rows that share a logical bar period."""
    winners: dict[tuple[str, datetime], dict] = {}
    period_final_ts: dict[tuple[str, datetime], datetime] = {}
    normalized = normalize_timeframe(timeframe)
    materialized_rows = [dict(row) for row in rows]
    if normalized not in {"5f", "30f", "1d", "1w", "1m"} or any(
        "source" not in row for row in materialized_rows
    ):
        return sorted(materialized_rows, key=lambda item: item["ts"], reverse=True)
    fallback_updated_at = datetime.min.replace(tzinfo=SHANGHAI_TZ)
    if parquet_coverage_end is None:
        parquet_coverage_end = max(
            (row["ts"] for row in materialized_rows if int(row.get("source", 0)) in {4, 9}),
            default=None,
        )
    for row in materialized_rows:
        try:
            _unused, logical_ts = kline_logical_key(normalized, row["ts"])
            local = row["ts"].astimezone(SHANGHAI_TZ)
            canonical_ts = canonical_kline_timestamp(
                normalized,
                row["ts"],
                date_only=normalized in {"1d", "1w", "1m"} and (local.hour, local.minute) == (0, 0),
            )
        except ValueError:
            continue
        row["ts"] = canonical_ts
        key = (normalized, logical_ts)
        period_final_ts[key] = max(canonical_ts, period_final_ts.get(key, canonical_ts))
        source = int(row.get("source", 0))
        order = (
            source_priority_with_coverage(code_to_source(source), canonical_ts, parquet_coverage_end),
            bool(row.get("is_complete", False)),
            int(row.get("revision", 0)),
            row.get("updated_at") or fallback_updated_at,
        )
        existing = winners.get(key)
        if existing is None:
            winners[key] = row
            continue
        existing_source = int(existing.get("source", 0))
        existing_order = (
            source_priority_with_coverage(code_to_source(existing_source), existing["ts"], parquet_coverage_end),
            bool(existing.get("is_complete", False)),
            int(existing.get("revision", 0)),
            existing.get("updated_at") or fallback_updated_at,
        )
        if order > existing_order:
            winners[key] = row
    for key, winner in winners.items():
        winner["ts"] = period_final_ts[key]
    return sorted(winners.values(), key=lambda item: item["ts"], reverse=True)


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
    elif timeframe == "1w":
        return max(365, limit * 9)
    elif timeframe == "1m":
        return max(730, limit * 40)
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
