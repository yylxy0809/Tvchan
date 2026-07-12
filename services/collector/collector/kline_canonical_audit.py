"""Bounded, resumable canonical K-line audit and repair worker.

The worker intentionally does not create missing bars.  It only removes ranked
duplicates or normalizes a sole, valid higher-timeframe fallback when a daily
bar provides an unambiguous canonical close.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import time
import uuid
import weakref
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import asyncpg

from trading_protocol.kline_contract import (
    SHANGHAI_TZ,
    canonical_kline_timestamp,
    kline_logical_key,
    source_priority_with_coverage,
)
from trading_protocol.timeframes import TIMEFRAMES


DEFAULT_TIMEFRAMES = "5f,15f,30f,1h,1d,1w,1m"
TIMEFRAME_CODES = {name: value.minutes for name, value in TIMEFRAMES.items()}
INTRADAY = {"5f", "15f", "30f", "1h"}
HIGHER = {"1d", "1w", "1m"}
SHARD_INTERVALS = {**{name: "3 months" for name in INTRADAY}, "1d": "1 year", "1w": "5 years", "1m": "5 years"}
KLINE_COLUMNS = (
    "symbol_id,timeframe,ts,open_x1000,high_x1000,low_x1000,close_x1000,"
    "volume,amount_x100,is_complete,revision,source,created_at,updated_at"
)


@dataclass(frozen=True)
class AuditRow:
    symbol_id: int
    timeframe: str
    ts: datetime
    open_x1000: int
    high_x1000: int
    low_x1000: int
    close_x1000: int
    volume: int
    amount_x100: int | None
    is_complete: bool
    revision: int
    source: int
    updated_at: datetime
    created_at: datetime | None = None

    @classmethod
    def from_record(cls, record: Any, timeframe: str) -> "AuditRow":
        values = dict(record)
        # SQL selects the stored minutes code; the audit loop already supplies
        # the normalized timeframe name used by the contract helpers.
        values.pop("timeframe", None)
        return cls(timeframe=timeframe, **values)

    def ohlcv(self) -> tuple[int, int, int, int, int, int | None]:
        return self.open_x1000, self.high_x1000, self.low_x1000, self.close_x1000, self.volume, self.amount_x100


@dataclass
class PlannedActions:
    winner: AuditRow | None = None
    quarantine: list[AuditRow] = field(default_factory=list)
    delete: list[AuditRow] = field(default_factory=list)
    normalize: tuple[AuditRow, datetime] | None = None
    unresolved: bool = False
    disagreement: bool = False
    reasons: list[str] = field(default_factory=list)
    quarantine_reason: str | None = None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and safely repair canonical A-share K-lines")
    parser.add_argument("--timeframes", default=os.getenv("KLINE_AUDIT_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument("--symbol-id-min", type=int)
    parser.add_argument("--symbols-file")
    parser.add_argument("--symbol-id-max", type=int)
    parser.add_argument("--start", help="Inclusive ISO date/timestamp bound")
    parser.add_argument("--end", help="Inclusive ISO date/timestamp bound")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--statement-timeout-seconds", type=int, default=20)
    parser.add_argument("--lock-timeout-seconds", type=int, default=1)
    parser.add_argument("--transaction-group-cap", type=int, default=500)
    parser.add_argument(
        "--single-window",
        action="store_true",
        help="Audit each symbol/timeframe in one indexed range scan instead of calendar shards.",
    )
    parser.add_argument("--output-dir", default="outputs/kline-canonical-audit")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--audit-run-id")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    args = parser.parse_args(argv)
    if args.apply and not args.audit_run_id:
        parser.error("--audit-run-id is required with --apply")
    if args.audit_run_id:
        try:
            args.audit_run_id = str(uuid.UUID(args.audit_run_id))
        except ValueError:
            parser.error("--audit-run-id must be a UUID")
    if args.symbols_file and (args.symbol_id_min is not None or args.symbol_id_max is not None):
        parser.error("--symbols-file cannot be combined with --symbol-id-min/max")
    if args.concurrency < 1 or args.statement_timeout_seconds < 1 or args.lock_timeout_seconds < 1:
        parser.error("concurrency and timeouts must be positive")
    if not 1 <= args.transaction_group_cap <= 500:
        parser.error("--transaction-group-cap must be between 1 and 500")
    try:
        args.start = _parse_bound(args.start) if args.start else None
        args.end = _parse_bound(args.end, end_of_date=True) if args.end else None
    except ValueError as error:
        parser.error(str(error))
    if args.start and args.end and args.start > args.end:
        parser.error("--start must be <= --end")
    args.timeframes = ",".join(parse_timeframes(args.timeframes))
    return args


def _parse_bound(raw: str, *, end_of_date: bool = False) -> datetime:
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        parsed = date.fromisoformat(raw)
        local = datetime.combine(parsed, datetime.max.time() if end_of_date else datetime.min.time(), tzinfo=SHANGHAI_TZ)
        return local.astimezone(timezone.utc)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--start/--end timestamps must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def parse_timeframes(raw: str) -> list[str]:
    values = [value.strip().lower() for value in raw.split(",") if value.strip()]
    invalid = [value for value in values if value not in TIMEFRAME_CODES]
    if invalid or not values:
        raise ValueError(f"unsupported timeframes: {','.join(invalid or ['none'])}")
    return list(dict.fromkeys(values))


def logical_period_key(timeframe: str, timestamp: datetime) -> tuple[str, datetime]:
    return kline_logical_key(timeframe, timestamp)


def validate_bar(value: AuditRow) -> list[str]:
    reasons: list[str] = []
    try:
        canonical_kline_timestamp(value.timeframe, value.ts, date_only=value.timeframe in HIGHER and _is_local_midnight(value.ts))
    except ValueError:
        reasons.append("invalid_timestamp")
    if (
        value.volume < 0
        or value.amount_x100 is not None and value.amount_x100 < 0
        or value.low_x1000 > min(value.open_x1000, value.close_x1000, value.high_x1000)
        or value.high_x1000 < max(value.open_x1000, value.close_x1000, value.low_x1000)
    ):
        reasons.append("invalid_ohlcv")
    return reasons


def choose_winner(rows: Sequence[AuditRow], coverage_end: datetime | None) -> AuditRow:
    """Use the persisted source-coverage precedence and Task 0A tie breakers."""
    return max(
        rows,
        key=lambda value: (
            source_priority_with_coverage(value.source, _ranking_timestamp(value), coverage_end),
            value.is_complete,
            value.revision,
            value.updated_at,
        ),
    )


def _ranking_timestamp(value: AuditRow) -> datetime:
    """Match the reader's canonical timestamp guard before source precedence."""
    return canonical_kline_timestamp(
        value.timeframe,
        value.ts,
        date_only=value.timeframe in HIGHER and _is_local_midnight(value.ts),
    ).astimezone(timezone.utc)


def build_shard_sql(timeframe: str) -> str:
    """Read one bounded symbol/timeframe date window; never aggregate a hypertable."""
    interval = SHARD_INTERVALS[timeframe]
    return f"""
WITH bounds AS (
    SELECT min(k.ts) AS min_ts, max(k.ts) AS max_ts
    FROM klines k
    WHERE k.symbol_id = $1::integer AND k.timeframe = $2::integer
      AND ($3::timestamptz IS NULL OR k.ts >= $3::timestamptz)
      AND ($4::timestamptz IS NULL OR k.ts <= $4::timestamptz)
), windowed AS (
    SELECT b.min_ts AS window_start,
           least(b.min_ts + interval '{interval}', b.max_ts) AS window_end
    FROM bounds b WHERE b.min_ts IS NOT NULL
)
SELECT {KLINE_COLUMNS}
FROM klines k CROSS JOIN windowed w
WHERE k.symbol_id = $1::integer AND k.timeframe = $2::integer
  AND k.ts >= w.window_start AND k.ts <= w.window_end
ORDER BY k.ts, k.source, k.revision, k.updated_at
"""


BOUNDS_SQL = """SELECT min(ts) AS min_ts, max(ts) AS max_ts FROM klines
WHERE symbol_id = $1::integer AND timeframe = $2::integer
  AND ($3::timestamptz IS NULL OR ts >= $3::timestamptz)
  AND ($4::timestamptz IS NULL OR ts <= $4::timestamptz)"""
WINDOW_ROWS_SQL = f"""SELECT {KLINE_COLUMNS} FROM klines
WHERE symbol_id = $1::integer AND timeframe = $2::integer
  AND ts >= $3::timestamptz AND ts <= $4::timestamptz
ORDER BY ts, source, revision, updated_at"""
DAILY_PERIOD_ROWS_SQL = f"""SELECT {KLINE_COLUMNS} FROM klines
WHERE symbol_id = $1::integer AND timeframe = 1440
  AND ts >= $2::timestamptz AND ts < $3::timestamptz
ORDER BY ts, source, revision, updated_at"""
ACTIVE_SYMBOLS_SQL = "SELECT id FROM symbols WHERE is_active = TRUE AND market = 'A_SHARE' AND ($1::integer IS NULL OR id >= $1) AND ($2::integer IS NULL OR id <= $2) ORDER BY id"
COVERAGE_SQL = "SELECT max(covered_until) FROM kline_source_coverage WHERE symbol_id = $1 AND timeframe = $2 AND source IN (4, 9)"
CHECKPOINT_SQL = """INSERT INTO kline_audit_checkpoints
(audit_run_id,symbol_id,timeframe,shard_start,shard_end,status,rows_scanned,metadata)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
ON CONFLICT (audit_run_id,symbol_id,timeframe,shard_start,shard_end) DO UPDATE
SET status=excluded.status,rows_scanned=excluded.rows_scanned,metadata=excluded.metadata,updated_at=now()"""


QUARANTINE_SQL = f"""
INSERT INTO kline_audit_quarantine (audit_run_id, reason, conflict_details, {KLINE_COLUMNS})
VALUES ($1::uuid, $2::text, $3::jsonb, $4::integer, $5::integer, $6::timestamptz,
        $7::integer, $8::integer, $9::integer, $10::integer, $11::bigint, $12::bigint,
        $13::boolean, $14::integer, $15::smallint, $16::timestamptz, $17::timestamptz)
ON CONFLICT (audit_run_id, symbol_id, timeframe, ts, source, revision, updated_at) DO NOTHING
"""
DELETE_SQL = """DELETE FROM klines WHERE symbol_id = $1 AND timeframe = $2 AND ts = $3
AND source = $4 AND revision = $5 AND updated_at = $6"""
NORMALIZE_INSERT_SQL = f"""
INSERT INTO klines ({KLINE_COLUMNS})
SELECT $1::integer, $2::integer, $3::timestamptz, $4::integer, $5::integer,
       $6::integer, $7::integer, $8::bigint, $9::bigint, $10::boolean,
       $11::integer, $12::smallint, $13::timestamptz, now()
WHERE NOT EXISTS (
    SELECT 1 FROM klines target
    WHERE target.symbol_id = $1 AND target.timeframe = $2 AND target.ts = $3
)
"""


async def _normalize_timestamp(connection: Any, value: AuditRow, target: datetime) -> None:
    """Move one bar without a cross-chunk UPDATE.

    TimescaleDB rejects timestamp updates that move a row between chunks.  The
    delete and insert remain in the caller's transaction, so a target collision
    or failed insert restores the original row on rollback.
    """
    deleted = await connection.execute(DELETE_SQL, *_identity_values(value))
    if not deleted.endswith("1"):
        raise RuntimeError("normalization source row changed")
    inserted = await connection.execute(NORMALIZE_INSERT_SQL, *_normalized_row_values(value, target))
    if not inserted.endswith("1"):
        raise RuntimeError("normalization target collided")


async def apply_actions(connection: Any, audit_run_id: str, quarantine: Sequence[AuditRow], delete: Sequence[AuditRow], *, reasons: dict[AuditRow, str] | None = None, normalize: tuple[AuditRow, datetime] | None = None, lock_timeout_seconds: int = 1) -> None:
    """Quarantine first; an exception leaves both quarantine and deletes rolled back."""
    async with connection.transaction():
        await connection.execute("SELECT set_config('lock_timeout', $1, true)", f"{lock_timeout_seconds}s")
        for value in quarantine:
            await connection.execute(QUARANTINE_SQL, audit_run_id, (reasons or {}).get(value, "duplicate_loser"), json.dumps({"winner": True}), *_row_values(value))
        for value in delete:
            await connection.execute(DELETE_SQL, value.symbol_id, TIMEFRAME_CODES[value.timeframe], value.ts, value.source, value.revision, value.updated_at)
        if normalize:
            value, target = normalize
            await _normalize_timestamp(connection, value, target)


async def apply_action_batches(
    connection: Any,
    audit_run_id: str,
    groups: Sequence[PlannedActions],
    *,
    group_cap: int,
    lock_timeout_seconds: int,
) -> None:
    """Apply at most ``group_cap`` logical groups per transaction."""
    for batch in _chunks(groups, group_cap):
        async with connection.transaction():
            await connection.execute("SELECT set_config('lock_timeout', $1, true)", f"{lock_timeout_seconds}s")
            for actions in batch:
                reasons = {value: "ohlcv_disagreement" if actions.disagreement else "duplicate_loser" for value in actions.quarantine}
                if getattr(actions, "quarantine_reason", None):
                    reasons = {value: actions.quarantine_reason for value in actions.quarantine}
                for value in actions.quarantine:
                    await connection.execute(QUARANTINE_SQL, audit_run_id, reasons[value], json.dumps({"winner": True}), *_row_values(value))
                for value in actions.delete:
                    await connection.execute(DELETE_SQL, *_identity_values(value))
                if actions.normalize:
                    value, target = actions.normalize
                    await _normalize_timestamp(connection, value, target)


def _chunks(values: Sequence[PlannedActions], size: int) -> Iterable[Sequence[PlannedActions]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


class AuditRunner:
    def __init__(self, *, apply: bool, audit_run_id: str | None, transaction_cap: int = 500) -> None:
        self.apply = apply
        self.audit_run_id = audit_run_id
        self.transaction_cap = transaction_cap
        self.write_count = 0
        self.completed_shards: set[tuple[int, str, str, str]] = set()

    async def plan_group(self, rows: Sequence[AuditRow], coverage_end: datetime | None, expected_timestamp: datetime | None = None, *, now: datetime | None = None) -> PlannedActions:
        actions = PlannedActions()
        invalid = {value: validate_bar(value) for value in rows}
        valid = [value for value in rows if not invalid[value]]
        if not valid:
            actions.unresolved = True
            actions.reasons = sorted({reason for values in invalid.values() for reason in values})
            return actions
        winner = choose_winner(valid, coverage_end)
        actions.winner = winner
        actions.disagreement = len({value.ohlcv() for value in valid}) > 1
        losers = [value for value in rows if value is not winner]
        if len(valid) > 1:
            actions.quarantine = losers
            actions.delete = losers
            if actions.disagreement:
                actions.reasons.append("ohlcv_disagreement")
        elif len(rows) > 1:
            # The valid winner remains. Invalid competing physical rows are
            # quarantined before deletion rather than being silently discarded.
            actions.quarantine = losers
            actions.delete = losers
            actions.reasons.append("invalid_duplicate_loser")
        elif winner.timeframe in {"1w", "1m"} and not is_closed_calendar_period(winner.ts, winner.timeframe, now=now):
            actions.unresolved = True
            actions.reasons.append("open_calendar_period")
        elif expected_timestamp and winner.ts != expected_timestamp and winner.timeframe in HIGHER:
            if validate_bar(winner):
                actions.unresolved = True
            else:
                actions.normalize = (winner, expected_timestamp)
        elif winner.timeframe in {"1w", "1m"} and _is_local_midnight(winner.ts):
            actions.unresolved = True
            actions.reasons.append("no_trustworthy_daily_period_end")
        return actions

    def next_groups(self, groups: Iterable[Sequence[AuditRow]]) -> list[Sequence[AuditRow]]:
        result: list[Sequence[AuditRow]] = []
        for group in groups:
            if len(result) == self.transaction_cap:
                break
            result.append(group)
        return result

    def should_skip_shard(self, symbol_id: int, timeframe: str, start: str, end: str) -> bool:
        return (symbol_id, timeframe, start, end) in self.completed_shards


def _window_start(value: datetime, timeframe: str) -> datetime:
    local = value.astimezone(SHANGHAI_TZ)
    if timeframe in INTRADAY:
        return datetime(local.year, ((local.month - 1) // 3) * 3 + 1, 1, tzinfo=SHANGHAI_TZ)
    if timeframe == "1d":
        return datetime(local.year, 1, 1, tzinfo=SHANGHAI_TZ)
    if timeframe == "1w":
        return datetime.combine(local.date() - timedelta(days=local.weekday()), datetime.min.time(), tzinfo=SHANGHAI_TZ)
    if timeframe == "1m":
        return datetime(local.year, local.month, 1, tzinfo=SHANGHAI_TZ)
    raise ValueError(f"unsupported timeframe: {timeframe}")


def aligned_windows(start: datetime, end: datetime, timeframe: str) -> Iterable[tuple[datetime, datetime]]:
    current = _window_start(start, timeframe)
    while current <= end:
        if timeframe in INTRADAY:
            month = current.month + 3
            next_start = datetime(current.year + (month - 1) // 12, (month - 1) % 12 + 1, 1, tzinfo=SHANGHAI_TZ)
        elif timeframe == "1d":
            next_start = datetime(current.year + 1, 1, 1, tzinfo=SHANGHAI_TZ)
        elif timeframe == "1m":
            next_start = datetime(current.year + 5, 1, 1, tzinfo=SHANGHAI_TZ)
        else:
            jan_boundary = datetime(current.year + 5, 1, 1, tzinfo=SHANGHAI_TZ)
            # Keep every window boundary on Monday so a logical week is never split.
            next_start = jan_boundary + timedelta(days=(-jan_boundary.weekday()) % 7)
        yield max(current, start), min(next_start - timedelta(microseconds=1), end)
        current = next_start


def _group_rows(rows: Sequence[AuditRow]) -> Iterable[list[AuditRow]]:
    grouped: dict[tuple[str, datetime], list[AuditRow]] = defaultdict(list)
    for value in rows:
        try:
            key = logical_period_key(value.timeframe, value.ts)
        except ValueError:
            key = (value.timeframe, value.ts)
        grouped[key].append(value)
    return grouped.values()


def plan_lunch_reopen_duplicate(
    value: AuditRow,
    comparator: AuditRow | None,
    *,
    cross_source_match: bool = False,
    has_valid_afternoon_bar: bool = False,
) -> PlannedActions:
    """Safely remove only the known source2 13:00 lunch-reopen duplicate."""
    actions = PlannedActions()
    # 13:00 is never a canonical A-share bar label.  A real bar later in the
    # same afternoon proves the source continued normally, so this lone
    # pseudo-bar is safely quarantined instead of requiring an unrelated 11:30
    # bar to have identical OHLCV.
    if has_valid_afternoon_bar:
        actions.quarantine = [value]
        actions.delete = [value]
        actions.quarantine_reason = "invalid_lunch_reopen_timestamp"
        return actions
    if comparator is None or comparator.source != value.source:
        actions.unresolved = True
        actions.disagreement = True
        actions.reasons.append("missing_same_source_1130_comparator")
        if cross_source_match:
            actions.reasons.append("cross_source_match")
        return actions
    actions.winner = comparator
    price_volume_match = (
        value.open_x1000,
        value.high_x1000,
        value.low_x1000,
        value.close_x1000,
        value.volume,
    ) == (
        comparator.open_x1000,
        comparator.high_x1000,
        comparator.low_x1000,
        comparator.close_x1000,
        comparator.volume,
    )
    if not price_volume_match:
        actions.unresolved = True
        actions.disagreement = True
        actions.reasons.append("lunch_reopen_ohlcv_disagreement")
        return actions
    if value.amount_x100 != comparator.amount_x100:
        actions.unresolved = True
        actions.disagreement = True
        actions.reasons.append("amount_unproven")
        return actions
    actions.quarantine = [value]
    actions.delete = [value]
    actions.quarantine_reason = "lunch_reopen_duplicate"
    return actions


def _lunch_reopen_plans(rows: Sequence[AuditRow], coverage_end: datetime | None) -> dict[AuditRow, PlannedActions]:
    comparators: dict[date, list[AuditRow]] = defaultdict(list)
    cross_source: dict[date, list[AuditRow]] = defaultdict(list)
    candidates: list[AuditRow] = []
    valid_afternoons: set[tuple[str, date]] = set()
    for value in rows:
        local = value.ts.astimezone(SHANGHAI_TZ)
        if (
            value.timeframe in INTRADAY
            and not validate_bar(value)
            and (local.hour, local.minute) > (13, 0)
        ):
            valid_afternoons.add((value.timeframe, local.date()))
        if not validate_bar(value) and (local.hour, local.minute) == (11, 30):
            if value.source == 2:
                comparators[local.date()].append(value)
            else:
                cross_source[local.date()].append(value)
        if value.timeframe in INTRADAY and value.source == 2 and (local.hour, local.minute) == (13, 0) and "invalid_timestamp" in validate_bar(value):
            candidates.append(value)
    winners = {day: choose_winner(values, coverage_end) for day, values in comparators.items()}
    plans: dict[AuditRow, PlannedActions] = {}
    for value in candidates:
        day = value.ts.astimezone(SHANGHAI_TZ).date()
        matching_cross_source = any(_same_lunch_ohlcv(value, other) for other in cross_source[day])
        plans[value] = plan_lunch_reopen_duplicate(
            value,
            winners.get(day),
            cross_source_match=matching_cross_source,
            has_valid_afternoon_bar=(value.timeframe, day) in valid_afternoons,
        )
    return plans


def _same_lunch_ohlcv(left: AuditRow, right: AuditRow) -> bool:
    return left.ohlcv() == right.ohlcv()


def canonical_period_end_from_daily(rows: Sequence[AuditRow], coverage_end: datetime | None) -> datetime | None:
    """Return only a trustworthy canonical daily close for weekly/monthly repair."""
    candidates: list[AuditRow] = []
    for value in rows:
        if value.timeframe != "1d" or not value.is_complete or validate_bar(value):
            continue
        try:
            canonical_kline_timestamp("1d", value.ts, date_only=False)
        except ValueError:
            continue
        candidates.append(value)
    winners = [choose_winner(group, coverage_end) for group in _group_rows(candidates)]
    return max((value.ts for value in winners), default=None)


async def _expected_timestamp(connection: asyncpg.Connection, value: AuditRow, *, timeout: float) -> datetime | None:
    if value.timeframe == "1d" and _is_local_midnight(value.ts):
        return canonical_kline_timestamp("1d", value.ts, date_only=True)
    if value.timeframe not in {"1w", "1m"}:
        return None
    period_start, period_end = period_bounds(value.ts, value.timeframe)
    records = await connection.fetch(DAILY_PERIOD_ROWS_SQL, value.symbol_id, period_start, period_end, timeout=timeout)
    coverage = await connection.fetchval(COVERAGE_SQL, value.symbol_id, TIMEFRAME_CODES["1d"], timeout=timeout)
    return canonical_period_end_from_daily([AuditRow.from_record(record, "1d") for record in records], coverage)


def period_bounds(value: datetime, timeframe: str) -> tuple[datetime, datetime]:
    local = value.astimezone(SHANGHAI_TZ)
    if timeframe == "1w":
        start_date = local.date() - timedelta(days=local.weekday())
        start = datetime.combine(start_date, datetime.min.time(), tzinfo=SHANGHAI_TZ)
        return start.astimezone(timezone.utc), (start + timedelta(days=7)).astimezone(timezone.utc)
    if timeframe == "1m":
        start = datetime(local.year, local.month, 1, tzinfo=SHANGHAI_TZ)
        month = local.month + 1
        end = datetime(local.year + (month - 1) // 12, (month - 1) % 12 + 1, 1, tzinfo=SHANGHAI_TZ)
        return start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    raise ValueError("period bounds support only 1w and 1m")


def is_closed_calendar_period(value: datetime, timeframe: str, *, now: datetime | None = None) -> bool:
    """Weekly/monthly repairs are permitted only after the Shanghai period closes."""
    local = value.astimezone(SHANGHAI_TZ)
    current = (now or datetime.now(timezone.utc)).astimezone(SHANGHAI_TZ)
    if timeframe == "1w":
        return local.date() - timedelta(days=local.weekday()) < current.date() - timedelta(days=current.weekday())
    if timeframe == "1m":
        return (local.year, local.month) < (current.year, current.month)
    raise ValueError("closed-period check supports only 1w and 1m")


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _fsync_directory(path: Path, *, platform: str | None = None) -> None:
    """Persist a same-directory rename on POSIX; Windows does not support it."""
    if (platform or os.name) == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class AtomicJsonlWriter:
    """Stream records to a unique sibling temp file, then atomically publish it."""
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.temp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        self._handle = self.temp_path.open("x", encoding="utf-8")
        self._closed = False
        self._records = 0
        self._finalizer = weakref.finalize(self, self._cleanup_file, self._handle, self.temp_path)

    @staticmethod
    def _cleanup_file(handle: Any, path: Path) -> None:
        if not handle.closed:
            handle.close()
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def write(self, record: dict[str, Any]) -> None:
        self._handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        self._records += 1
        if self._records % 100 == 0:
            self.flush()

    def flush(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def promote(self) -> None:
        if not self._closed:
            self.flush()
            self._handle.close()
            self._closed = True
        # Path.replace is an atomic replace on the same Windows volume.
        self.temp_path.replace(self.path)
        _fsync_directory(self.path.parent)
        self._finalizer.detach()

    def cleanup(self) -> None:
        if not self._closed:
            self._handle.close()
            self._closed = True
        self._finalizer()


def _checkpoint_state_dir(output: Path) -> Path:
    return output / ".checkpoint-state"


def _checkpoint_marker_path(output: Path, record: dict[str, Any]) -> Path:
    identity = "|".join(str(record[key]) for key in ("symbol_id", "timeframe", "shard_start", "shard_end"))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return _checkpoint_state_dir(output) / f"{digest}.json"


def write_checkpoint_marker(output: Path, record: dict[str, Any]) -> Path:
    """Durably commit one completed shard independently of final report files."""
    marker = _checkpoint_marker_path(output, record)
    marker.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker.with_name(f".{marker.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(marker)
        _fsync_directory(marker.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return marker


def load_checkpoint_markers(output: Path) -> tuple[set[tuple[int, str, str, str]], int]:
    completed: set[tuple[int, str, str, str]] = set()
    invalid = 0
    state = _checkpoint_state_dir(output)
    if not state.exists():
        return completed, invalid
    for marker in state.glob("*.json"):
        try:
            with marker.open("r", encoding="utf-8") as stream:
                entry = json.load(stream)
            if entry.get("status") != "completed":
                invalid += 1
                continue
            completed.add((entry["symbol_id"], entry["timeframe"], entry["shard_start"], entry["shard_end"]))
        except (OSError, ValueError, KeyError, TypeError):
            invalid += 1
    return completed, invalid


def clear_checkpoint_state(output: Path) -> None:
    """Clear only marker/temp files for this output run, never arbitrary paths."""
    state = _checkpoint_state_dir(output)
    if not state.exists():
        return
    for child in state.iterdir():
        if child.is_file() and (child.suffix == ".json" or child.suffix == ".tmp"):
            child.unlink()


def consolidate_checkpoint_markers(output: Path) -> tuple[int, int]:
    writer = AtomicJsonlWriter(output / "checkpoints.jsonl")
    completed, invalid = load_checkpoint_markers(output)
    state = _checkpoint_state_dir(output)
    records = 0
    try:
        for marker in state.glob("*.json") if state.exists() else ():
            try:
                with marker.open("r", encoding="utf-8") as stream:
                    entry = json.load(stream)
                if entry.get("status") == "completed":
                    writer.write(entry)
                    records += 1
            except (OSError, ValueError, KeyError, TypeError):
                continue
        writer.promote()
    except BaseException:
        writer.cleanup()
        raise
    return records, invalid


async def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    """Run bounded shard scans.  Apply mode writes only planned repair groups."""
    run_id = args.audit_run_id or str(uuid.uuid4())
    started = time.monotonic()
    runner = AuditRunner(apply=args.apply, audit_run_id=run_id)
    output = Path(args.output_dir) / run_id
    if args.resume:
        runner.completed_shards, marker_failures = load_checkpoint_markers(output)
    else:
        clear_checkpoint_state(output)
        marker_failures = 0
    conflict_writer = AtomicJsonlWriter(output / "conflicts.jsonl")
    unresolved_writer = AtomicJsonlWriter(output / "unresolved.jsonl")
    stats: Counter[str] = Counter()
    per_timeframe: dict[str, Counter[str]] = defaultdict(Counter)
    per_source: dict[str, Counter[str]] = defaultdict(Counter)
    affected_symbols: set[int] = set()
    affected_overflow = 0
    parameters = {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in vars(args).items()
        if key != "database_url"
    }
    async with asyncpg.create_pool(args.database_url, min_size=1, max_size=args.concurrency) as pool:
        async with pool.acquire() as connection:
            if args.apply:
                await connection.execute("SELECT set_config('statement_timeout', $1, false)", f"{args.statement_timeout_seconds}s")
            symbols = [record["id"] for record in await connection.fetch(ACTIVE_SYMBOLS_SQL, args.symbol_id_min, args.symbol_id_max, timeout=args.statement_timeout_seconds)]
            if args.symbols_file:
                requested = {int(line.strip()) for line in Path(args.symbols_file).read_text(encoding="utf-8").splitlines() if line.strip()}
                symbols = [symbol_id for symbol_id in symbols if symbol_id in requested]
            if args.apply:
                await connection.execute("""INSERT INTO kline_audit_runs(audit_run_id,status,apply_mode,parameters)
                    VALUES ($1,'running',$2,$3::jsonb) ON CONFLICT (audit_run_id) DO NOTHING""", run_id, args.apply, json.dumps(parameters))
            if args.apply and args.resume:
                rows = await connection.fetch("SELECT symbol_id,timeframe,shard_start,shard_end FROM kline_audit_checkpoints WHERE audit_run_id=$1 AND status='completed'", run_id, timeout=args.statement_timeout_seconds)
                runner.completed_shards.update({(r["symbol_id"], next(name for name, code in TIMEFRAME_CODES.items() if code == r["timeframe"]), r["shard_start"].isoformat(), r["shard_end"].isoformat()) for r in rows})

        semaphore = asyncio.Semaphore(args.concurrency)

        async def audit_symbol_timeframe(symbol_id: int, timeframe: str) -> None:
            nonlocal affected_overflow
            async with semaphore, pool.acquire() as connection:
                if args.apply:
                    await connection.execute("SELECT set_config('statement_timeout', $1, false)", f"{args.statement_timeout_seconds}s")
                bounds = await connection.fetchrow(BOUNDS_SQL, symbol_id, TIMEFRAME_CODES[timeframe], args.start, args.end, timeout=args.statement_timeout_seconds)
                if not bounds or bounds["min_ts"] is None:
                    return
                effective_start = max(bounds["min_ts"], args.start) if args.start else bounds["min_ts"]
                effective_end = min(bounds["max_ts"], args.end) if args.end else bounds["max_ts"]
                if effective_start > effective_end:
                    return
                coverage = await connection.fetchval(COVERAGE_SQL, symbol_id, TIMEFRAME_CODES[timeframe], timeout=args.statement_timeout_seconds)
                windows = (
                    [(effective_start, effective_end)]
                    if args.single_window
                    else aligned_windows(effective_start, effective_end, timeframe)
                )
                for shard_start, shard_end in windows:
                    key = (symbol_id, timeframe, shard_start.isoformat(), shard_end.isoformat())
                    if key in runner.completed_shards:
                        continue
                    records = await connection.fetch(WINDOW_ROWS_SQL, symbol_id, TIMEFRAME_CODES[timeframe], shard_start, shard_end, timeout=args.statement_timeout_seconds)
                    values = [AuditRow.from_record(record, timeframe) for record in records]
                    lunch_plans = _lunch_reopen_plans(values, coverage)
                    pending_repairs: list[PlannedActions] = []

                    async def flush_repairs() -> None:
                        if not pending_repairs:
                            return
                        await apply_action_batches(
                            connection,
                            run_id,
                            pending_repairs,
                            group_cap=args.transaction_group_cap,
                            lock_timeout_seconds=args.lock_timeout_seconds,
                        )
                        for planned in pending_repairs:
                            runner.write_count += len(planned.quarantine) + len(planned.delete) + int(planned.normalize is not None)
                            stats["applied_quarantine"] += len(planned.quarantine)
                            stats["applied_delete"] += len(planned.delete)
                            stats["applied_normalize"] += int(planned.normalize is not None)
                            per_timeframe[timeframe]["applied_quarantine"] += len(planned.quarantine)
                            per_timeframe[timeframe]["applied_delete"] += len(planned.delete)
                            per_timeframe[timeframe]["applied_normalize"] += int(planned.normalize is not None)
                            for loser in planned.delete:
                                per_source[str(loser.source)]["applied_delete"] += 1
                        pending_repairs.clear()
                    stats["rows_scanned"] += len(values)
                    per_timeframe[timeframe]["rows_scanned"] += len(values)
                    for value in values:
                        per_source[str(value.source)]["rows_scanned"] += 1
                    for group in _group_rows(values):
                        stats["logical_groups"] += 1
                        candidate = group[0]
                        partial_period = False
                        if timeframe in {"1w", "1m"}:
                            period_start, period_end = period_bounds(candidate.ts, timeframe)
                            partial_period = period_start < effective_start or period_end - timedelta(microseconds=1) > effective_end
                        if candidate in lunch_plans:
                            actions = lunch_plans[candidate]
                        else:
                            expected = None
                            if len(group) == 1 and not partial_period and not (candidate.timeframe in {"1w", "1m"} and not is_closed_calendar_period(candidate.ts, candidate.timeframe)):
                                expected = await _expected_timestamp(connection, candidate, timeout=args.statement_timeout_seconds)
                            actions = await runner.plan_group(group, coverage, expected)
                        if partial_period:
                            actions.quarantine = []
                            actions.delete = []
                            actions.normalize = None
                            actions.unresolved = True
                            actions.reasons.append("partial_explicit_period_range")
                        for value in group:
                            for reason in validate_bar(value):
                                stats[reason] += 1
                                per_timeframe[timeframe][reason] += 1
                        exact_physical = len(group) > 1 and len({value.ts for value in group}) < len(group)
                        logical_duplicate = len(group) > 1
                        if exact_physical:
                            stats["exact_physical_duplicates"] += 1
                            per_timeframe[timeframe]["exact_physical_duplicates"] += 1
                        if logical_duplicate:
                            stats["logical_period_duplicates"] += 1
                            per_timeframe[timeframe]["logical_period_duplicates"] += 1
                        source_conflict = len({value.source for value in group}) > 1
                        if actions.disagreement:
                            stats["disagreements"] += 1
                            per_timeframe[timeframe]["disagreements"] += 1
                        if source_conflict:
                            stats["source_conflicts"] += 1
                            per_timeframe[timeframe]["source_conflicts"] += 1
                        if actions.disagreement or source_conflict:
                            conflict_writer.write({"symbol_id": symbol_id, "timeframe": timeframe, "reasons": list(dict.fromkeys([reason for reason, present in (("ohlcv_disagreement", actions.disagreement), ("source_conflict", source_conflict)) if present] + actions.reasons)), "rows": [_json_row(v) for v in group], "winner": _json_row(actions.winner) if actions.winner else None})
                        if actions.unresolved:
                            stats["unresolved"] += 1
                            per_timeframe[timeframe]["unresolved"] += 1
                            unresolved_writer.write({"symbol_id": symbol_id, "timeframe": timeframe, "reasons": actions.reasons, "rows": [_json_row(v) for v in group]})
                        if logical_duplicate or actions.unresolved or actions.normalize:
                            if symbol_id in affected_symbols:
                                pass
                            elif len(affected_symbols) < 5522:
                                affected_symbols.add(symbol_id)
                            else:
                                affected_overflow += 1
                        stats["planned_quarantine"] += len(actions.quarantine)
                        stats["planned_delete"] += len(actions.delete)
                        stats["planned_normalize"] += int(actions.normalize is not None)
                        per_timeframe[timeframe]["planned_quarantine"] += len(actions.quarantine)
                        per_timeframe[timeframe]["planned_delete"] += len(actions.delete)
                        per_timeframe[timeframe]["planned_normalize"] += int(actions.normalize is not None)
                        for value in actions.quarantine:
                            per_source[str(value.source)]["planned_quarantine"] += 1
                        for value in actions.delete:
                            per_source[str(value.source)]["planned_delete"] += 1
                        if args.apply and (actions.delete or actions.normalize):
                            pending_repairs.append(actions)
                            if len(pending_repairs) == args.transaction_group_cap:
                                await flush_repairs()
                    if args.apply:
                        await flush_repairs()
                    if args.apply:
                        await connection.execute(CHECKPOINT_SQL, run_id, symbol_id, TIMEFRAME_CODES[timeframe], shard_start, shard_end, "completed", len(values), json.dumps({"logical_groups": stats["logical_groups"]}))
                    write_checkpoint_marker(output, {"symbol_id": symbol_id, "timeframe": timeframe, "shard_start": shard_start.isoformat(), "shard_end": shard_end.isoformat(), "status": "completed", "rows_scanned": len(values)})

        failure_count = 0
        labels = ((symbol_id, timeframe) for symbol_id in symbols for timeframe in parse_timeframes(args.timeframes))
        while batch := [label for _, label in zip(range(args.concurrency), labels)]:
            results = await asyncio.gather(*(audit_symbol_timeframe(symbol_id, timeframe) for symbol_id, timeframe in batch), return_exceptions=True)
            for (symbol_id, timeframe), result in zip(batch, results):
                if isinstance(result, Exception):
                    failure_count += 1
                    unresolved_writer.write({"symbol_id": symbol_id, "timeframe": timeframe, "error": str(result)})
        stats["failures"] = failure_count
        listed_symbols = sorted(affected_symbols)
        summary = {"audit_run_id": run_id, "parameters": parameters, "active_symbols": len(symbols), **stats, "checkpoint_marker_failures": marker_failures, "affected_symbol_count": len(affected_symbols) + affected_overflow, "affected_symbols": listed_symbols, "affected_symbols_truncated": affected_overflow > 0, "affected_symbol_policy": "first 5522 unique symbols retained; count is conservative after truncation", "applied_writes": runner.write_count, "before_rows": stats["rows_scanned"], "after_rows": stats["rows_scanned"] - (stats["applied_delete"] if args.apply else 0), "per_timeframe": {name: dict(values) for name, values in per_timeframe.items()}, "per_source": {name: dict(values) for name, values in per_source.items()}, "elapsed_seconds": round(time.monotonic() - started, 3)}
        if args.apply:
            async with pool.acquire() as connection:
                await connection.execute("UPDATE kline_audit_runs SET status=$2,completed_at=now(),summary=$3::jsonb WHERE audit_run_id=$1", run_id, "failed" if failure_count else "completed", json.dumps(summary))
    _atomic_write(output / "summary.json", json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    _atomic_write(output / "summary.md", "# K-line canonical audit\n\n```json\n" + json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n```\n")
    conflict_writer.promote()
    unresolved_writer.promote()
    consolidate_checkpoint_markers(output)
    return summary


def _json_row(value: AuditRow | None) -> dict[str, Any] | None:
    return None if value is None else {key: (item.isoformat() if isinstance(item, datetime) else item) for key, item in value.__dict__.items()}


def _is_local_midnight(value: datetime) -> bool:
    local = value.astimezone(SHANGHAI_TZ)
    return (local.hour, local.minute, local.second, local.microsecond) == (0, 0, 0, 0)


def _row_values(value: AuditRow) -> tuple[Any, ...]:
    return (value.symbol_id, TIMEFRAME_CODES[value.timeframe], value.ts, value.open_x1000, value.high_x1000, value.low_x1000, value.close_x1000, value.volume, value.amount_x100, value.is_complete, value.revision, value.source, value.created_at or value.updated_at, value.updated_at)


def _identity_values(value: AuditRow) -> tuple[Any, ...]:
    return (value.symbol_id, TIMEFRAME_CODES[value.timeframe], value.ts, value.source, value.revision, value.updated_at)


def _normalized_row_values(value: AuditRow, target: datetime) -> tuple[Any, ...]:
    return (
        value.symbol_id,
        TIMEFRAME_CODES[value.timeframe],
        target,
        value.open_x1000,
        value.high_x1000,
        value.low_x1000,
        value.close_x1000,
        value.volume,
        value.amount_x100,
        value.is_complete,
        value.revision,
        value.source,
        value.created_at or value.updated_at,
    )


def main() -> None:
    args = parse_args()
    # The CLI entrypoint deliberately requires an explicit database URL.  This
    # prevents a default invocation from silently touching any environment.
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    summary = asyncio.run(run_audit(args))
    print(json.dumps(summary, ensure_ascii=False, default=str))
    if summary.get("failures", 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
