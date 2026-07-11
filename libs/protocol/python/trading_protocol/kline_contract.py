from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from .timeframes import normalize_timeframe


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
OPENING_SNAPSHOT_LABEL = (9, 30)

# Source codes are database identifiers, not a precedence ordering.
SOURCE_CODES = {
    "seed": 1,
    "pytdx": 2,
    "tdx_csv": 3,
    "parquet_5f": 4,
    "mootdx": 5,
    "tencent": 6,
    "baidu": 7,
    "derived_5f": 8,
    "parquet_native": 9,
}
SOURCE_NAMES = {code: name for name, code in SOURCE_CODES.items()}
SOURCE_PRIORITIES = {
    "parquet_native": 9,
    "parquet_5f": 8,
    "tdx_csv": 7,
    "pytdx": 6,
    "mootdx": 5,
    "tencent": 4,
    "baidu": 3,
    "derived_5f": 2,
    "seed": 1,
}


def source_to_code(source: str) -> int:
    return SOURCE_CODES.get(source, 0)


def code_to_source(code: int) -> str:
    return SOURCE_NAMES.get(code, "database")


def source_priority(source: str | int) -> int:
    name = code_to_source(source) if isinstance(source, int) else source
    return SOURCE_PRIORITIES.get(name, 0)


def source_priority_with_coverage(
    source: str | int,
    timestamp: datetime,
    parquet_coverage_end: datetime | None,
) -> int:
    """Prefer pytdx only after the persisted parquet/native coverage boundary."""
    name = code_to_source(source) if isinstance(source, int) else source
    if name == "pytdx" and parquet_coverage_end is not None and timestamp > parquet_coverage_end:
        return 10
    return source_priority(name)


def source_priority_sql(column: str) -> str:
    """Return the database equivalent of ``source_priority`` for K-line queries."""
    return f"""case {column}
        when 9 then 9 when 4 then 8 when 3 then 7 when 2 then 6
        when 5 then 5 when 6 then 4 when 7 then 3 when 8 then 2 when 1 then 1
        else 0 end"""


def source_priority_with_coverage_sql(column: str, timestamp: str, coverage_end: str) -> str:
    return f"""case when {column} = 2 and {coverage_end} is not null and {timestamp} > {coverage_end}
        then 10 else ({source_priority_sql(column)}) end"""


def should_replace_kline(
    *,
    existing_source: str | int,
    existing_revision: int,
    existing_complete: bool,
    incoming_source: str | int,
    incoming_revision: int,
    incoming_complete: bool,
) -> bool:
    existing_priority = source_priority(existing_source)
    incoming_priority = source_priority(incoming_source)
    if incoming_priority != existing_priority:
        return incoming_priority > existing_priority
    return incoming_revision > existing_revision or (incoming_complete and not existing_complete)


def canonical_kline_timestamp(timeframe: str, timestamp: datetime, *, date_only: bool = False) -> datetime:
    """Return the canonical A-share bar-end label without rounding intraday input."""
    normalized = normalize_timeframe(timeframe)
    if timestamp.tzinfo is None:
        raise ValueError("K-line timestamp must be timezone-aware")
    local = timestamp.astimezone(SHANGHAI_TZ)
    if normalized in {"1d", "1w", "1m"}:
        if date_only:
            return datetime.combine(local.date(), time(15, 0), tzinfo=SHANGHAI_TZ)
        if (local.hour, local.minute, local.second, local.microsecond) != (15, 0, 0, 0):
            raise ValueError(f"{normalized} timestamp must be a 15:00 bar-end label")
        return local
    if normalized in {"5f", "15f", "30f", "1h"}:
        valid_labels = _intraday_bar_end_labels(normalized)
        if (local.hour, local.minute) not in valid_labels or local.second or local.microsecond:
            raise ValueError(f"{normalized} timestamp is not an A-share session bar-end label")
        return local
    return local


def kline_logical_key(timeframe: str, timestamp: datetime) -> tuple[str, datetime]:
    normalized = normalize_timeframe(timeframe)
    local = timestamp.astimezone(SHANGHAI_TZ)
    date_only = normalized in {"1d", "1w", "1m"} and (local.hour, local.minute) == (0, 0)
    canonical = canonical_kline_timestamp(normalized, timestamp, date_only=date_only)
    if normalized == "1w":
        return normalized, datetime.combine(local.date() - timedelta(days=local.weekday()), time(), tzinfo=SHANGHAI_TZ)
    if normalized == "1m":
        return normalized, datetime.combine(local.date().replace(day=1), time(), tzinfo=SHANGHAI_TZ)
    return normalized, canonical


def _intraday_bar_end_labels(timeframe: str) -> set[tuple[int, int]]:
    minutes = {"5f": 5, "15f": 15, "30f": 30, "1h": 60}[timeframe]
    # Approved native history includes one opening auction/snapshot before regular bar ends.
    labels: set[tuple[int, int]] = {OPENING_SNAPSHOT_LABEL}
    for start, end in ((9 * 60 + 30, 11 * 60 + 30), (13 * 60, 15 * 60)):
        for value in range(start + minutes, end + 1, minutes):
            labels.add((value // 60, value % 60))
    return labels
