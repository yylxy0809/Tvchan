"""Safely append the three approved aggregate incremental Parquet files.

This adapter is intentionally narrower than :mod:`local_parquet_import`:

* ``stock_5min_data.parquet`` is 5-minute data (``5f``);
* ``stock_30min_data.parquet`` is 30-minute data (``30f``);
* ``stock_daily(1).parquet`` is daily data (``1d``).

In particular, the vendor's ``stock_1min`` name means *one minute*, while the
project's ``1m`` protocol value means *one month*.  It is never discovered by
this importer.  Each bounded row-group chunk has a file-SHA identity and its
accepted rows, quarantines and checkpoint commit in one database transaction.
Canonical rows are append-only: keys at or below the original scope maximum
must already exist and match exactly; only later keys may be inserted.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import sqlite3
import tempfile
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from collector.kline_import_quarantine import ImportCheckpoint, QuarantineRecord, commit_import_batch
from collector.kline_scope_catalog import record_present_scopes
from collector.module_c_eligibility import load_freshness_contract
from collector.native_parquet_import import (
    NativeParquetWriter,
    ParsedBatch,
    create_kline_stage,
    infer_asset_type,
    normalize_symbol,
    register_source_coverage,
    upsert_watermarks,
    valid_ohlc,
)
from collector.storage.postgres import amount_to_x100, price_to_x1000, source_to_code, timeframe_to_db_code
from trading_protocol import canonical_kline_timestamp

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SOURCE_NAME = "parquet_native"
SOURCE_CODE = source_to_code(SOURCE_NAME)
DEFAULT_ROOT = r"F:\data"
DEFAULT_BATCH_SIZE = 25_000
IMPORT_RUN_NAMESPACE = UUID("6b602069-d89c-4e35-8e63-c88c8e09e555")
POLICY_VERSION = "aggregate-increment-append-only-v1"
GLOBAL_WRITER_LOCK_KEY = 4_741_925_817_007_181
POLICY_QUARANTINE_REASONS = {
    "excluded_bj_default_source_mismatch",
    "excluded_bj_30f_policy",
    "zero_turnover_halt_day_not_freshness",
    "symbol_not_in_authoritative_master",
    "symbol_not_in_pinned_active_universe",
}

FILE_CONTRACTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "stock_5min_data.parquet": (
        "5f", ("trade_date", "trade_time", "ts_code", "open", "high", "low", "close", "vol", "amount"),
    ),
    "stock_30min_data.parquet": (
        "30f", ("trade_date", "trade_time", "ts_code", "open", "high", "low", "close", "vol", "amount"),
    ),
    "stock_daily(1).parquet": (
        "1d", ("trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount"),
    ),
}


@dataclass(frozen=True)
class AggregateTask:
    timeframe: str
    path: Path
    row_group: int
    batch_index: int
    batch_size: int
    row_offset: int
    row_count: int
    file_sha256: str
    source_ref: str
    source_checksum: str


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_tasks(
    root: str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    filenames: Iterable[str] | None = None,
) -> list[AggregateTask]:
    """Freeze an exact bounded task manifest without reading row payloads."""
    if batch_size < 1 or batch_size > 100_000:
        raise ValueError("batch_size must be between 1 and 100000")
    import pyarrow.parquet as pq

    root = Path(root)
    requested = tuple(filenames) if filenames is not None else tuple(FILE_CONTRACTS)
    unknown = sorted(set(requested) - set(FILE_CONTRACTS))
    if unknown:
        raise ValueError(f"unsupported aggregate file: {unknown[0]}")
    tasks: list[AggregateTask] = []
    for filename in requested:
        path = root / filename
        if not path.is_file():
            raise FileNotFoundError(path)
        timeframe, required = FILE_CONTRACTS[filename]
        parquet = pq.ParquetFile(path)
        missing = sorted(set(required) - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError(f"{filename} missing required columns: {','.join(missing)}")
        checksum = file_sha256(path)
        global_offset = 0
        for row_group in range(parquet.metadata.num_row_groups):
            row_count = int(parquet.metadata.row_group(row_group).num_rows)
            for batch_index in range(math.ceil(row_count / batch_size)):
                local_offset = batch_index * batch_size
                count = min(batch_size, row_count - local_offset)
                source_ref = (
                    f"{filename}@sha256={checksum}"
                    f"#row_group={row_group};offset={local_offset};rows={count}"
                )
                source_checksum = (
                    f"sha256={checksum};row_group={row_group};offset={local_offset};rows={count}"
                )
                tasks.append(AggregateTask(
                    timeframe=timeframe,
                    path=path,
                    row_group=row_group,
                    batch_index=batch_index,
                    batch_size=batch_size,
                    row_offset=global_offset + local_offset,
                    row_count=count,
                    file_sha256=checksum,
                    source_ref=source_ref,
                    source_checksum=source_checksum,
                ))
            global_offset += row_count
    return tasks


def load_authoritative_symbol_meta(root: str | Path) -> dict[str, dict[str, Any]]:
    """Load the vendor security master in bounded batches.

    This validates identity metadata only.  Production writes additionally
    require membership in the pinned active catalog universe; this importer
    never creates or activates a symbol.
    """
    import pyarrow.parquet as pq

    path = Path(root) / "stock_basic_data.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"authoritative symbol master is required: {path}")
    parquet = pq.ParquetFile(path)
    required = {"ts_code", "name", "list_status"}
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"symbol master missing required columns: {','.join(missing)}")
    result: dict[str, dict[str, Any]] = {}
    for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(required)):
        rows = batch.to_pydict()
        for symbol_value, name, status in zip(
            rows["ts_code"], rows["name"], rows["list_status"], strict=True,
        ):
            if not symbol_value or status != "L":
                continue
            symbol = normalize_symbol(str(symbol_value))
            result[symbol] = {"name": str(name or symbol), "is_active": True}
    return result


def _read_task_batch(task: AggregateTask, columns: tuple[str, ...]):
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(task.path)
    batches = parquet.iter_batches(
        batch_size=task.batch_size,
        row_groups=[task.row_group],
        columns=list(columns),
        use_threads=False,
    )
    # A task offset is always aligned to the frozen batch size.  Iterating and
    # discarding preceding batches keeps memory bounded without materialising
    # the entire row group.
    for index, batch in enumerate(batches):
        if index == task.batch_index:
            if batch.num_rows != task.row_count:
                raise RuntimeError("Parquet row-group shape changed after manifest discovery")
            return batch
    raise RuntimeError("Parquet row-group ended before the frozen task offset")


def parse_task(
    task: AggregateTask,
    *,
    symbol_meta: dict[str, dict[str, Any]],
    symbols_filter: set[str] | None = None,
    exchanges: set[str] | None = None,
    allow_bj: bool = False,
    halted_days: set[tuple[str, str]] | None = None,
    pinned_symbols: set[str] | None = None,
    expected_closed: dict[str, datetime] | None = None,
) -> ParsedBatch:
    required = FILE_CONTRACTS[task.path.name][1]
    batch = _read_task_batch(task, required)
    rows = batch.to_pydict()
    symbols: dict[tuple[str, str], tuple] = {}
    bars: list[tuple] = []
    quarantines: list[QuarantineRecord] = []
    for index in range(batch.num_rows):
        source_row = task.row_offset + index
        raw = {column: rows[column][index] for column in required}
        raw_ts = str(raw["trade_date"] if task.timeframe == "1d" else raw["trade_time"])
        try:
            symbol = normalize_symbol(str(raw["ts_code"]))
        except (TypeError, ValueError) as exc:
            quarantines.append(_quarantine(task, source_row, raw, raw_ts, None, f"invalid_symbol:{exc}"))
            continue
        if symbols_filter and symbol not in symbols_filter:
            continue
        if pinned_symbols is not None and symbol not in pinned_symbols:
            quarantines.append(_quarantine(
                task, source_row, raw, raw_ts, symbol, "symbol_not_in_pinned_active_universe",
            ))
            continue
        meta = symbol_meta.get(symbol)
        if meta is None:
            quarantines.append(_quarantine(
                task, source_row, raw, raw_ts, symbol, "symbol_not_in_authoritative_master",
            ))
            continue
        code, exchange = symbol.split(".", 1)
        selected_exchanges = exchanges or {"SH", "SZ"}
        if exchange == "BJ" and (not allow_bj or task.timeframe == "30f"):
            reason = "excluded_bj_30f_policy" if task.timeframe == "30f" else "excluded_bj_default_source_mismatch"
            quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, reason))
            continue
        if exchange not in selected_exchanges:
            continue
        try:
            open_value, high_value, low_value, close_value = (
                float(raw[key]) for key in ("open", "high", "low", "close")
            )
            volume_value = float(raw["vol"])
            amount_value = None if raw["amount"] is None else float(raw["amount"])
            if volume_value < 0:
                raise ValueError("negative_volume")
            if amount_value is not None and amount_value < 0:
                raise ValueError("negative_amount")
            bar_ts = _bar_timestamp(task.timeframe, raw)
        except (TypeError, ValueError, OverflowError) as exc:
            quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, f"invalid_value:{exc}"))
            continue
        if not valid_ohlc(open_value, high_value, low_value, close_value):
            quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, "invalid_ohlc"))
            continue
        if expected_closed is not None and bar_ts > expected_closed[task.timeframe]:
            quarantines.append(_quarantine(
                task, source_row, raw, raw_ts, symbol, "after_authoritative_expected_closed_bound",
            ))
            continue
        canonical_amount = None if amount_value is None else amount_value * (1000 if task.timeframe == "1d" else 1)
        flat = open_value == high_value == low_value == close_value
        if volume_value == 0 and (canonical_amount is None or canonical_amount == 0) and flat:
            day_key = (symbol, str(raw["trade_date"])[:10])
            if halted_days is None or day_key in halted_days:
                quarantines.append(_quarantine(
                    task, source_row, raw, raw_ts, symbol, "zero_turnover_halt_day_not_freshness",
                ))
                continue
        if volume_value == 0:
            if (canonical_amount not in (None, 0)) or not flat:
                quarantines.append(_quarantine(
                    task, source_row, raw, raw_ts, symbol, "zero_volume_inconsistent_bar",
                ))
                continue
            normalized_volume = 0
        else:
            # These three aggregate files have a frozen, audited unit contract:
            # intraday volume is shares; daily volume is hundred-shares.  Do
            # not re-guess the unit row-by-row from rounded turnover values.
            normalized_volume = int(round(volume_value * (100 if task.timeframe == "1d" else 1)))
        symbols[(code, exchange)] = (
            code, exchange, str(meta["name"]), infer_asset_type(code), "A_SHARE", True,
        )
        bars.append((
            code, exchange, timeframe_to_db_code(task.timeframe), bar_ts,
            price_to_x1000(open_value), price_to_x1000(high_value),
            price_to_x1000(low_value), price_to_x1000(close_value),
            normalized_volume, amount_to_x100(canonical_amount), True, 0, SOURCE_CODE,
        ))
    return ParsedBatch(
        symbols=symbols,
        bars=bars,
        quarantines=quarantines,
        last_source_row=task.row_offset + batch.num_rows - 1 if batch.num_rows else None,
    )


def find_zero_turnover_halt_days(root: str | Path) -> set[tuple[str, str]]:
    """Find symbol-days whose complete approved source slice is flat/no-trade."""
    import pyarrow.parquet as pq

    state: dict[tuple[str, str], bool] = {}
    for filename in FILE_CONTRACTS:
        path = Path(root) / filename
        parquet = pq.ParquetFile(path)
        columns = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
        for batch in parquet.iter_batches(batch_size=65_536, columns=columns, use_threads=False):
            rows = batch.to_pydict()
            for values in zip(*(rows[column] for column in columns), strict=True):
                symbol_value, trade_date, open_value, high_value, low_value, close_value, volume, amount = values
                if not symbol_value:
                    continue
                symbol = normalize_symbol(str(symbol_value))
                key = (symbol, str(trade_date)[:10])
                row_is_halt = (
                    float(volume or 0) == 0
                    and float(amount or 0) == 0
                    and open_value == high_value == low_value == close_value
                )
                state[key] = state.get(key, True) and row_is_halt
    return {key for key, every_row_is_halt in state.items() if every_row_is_halt}


def verify_frozen_file(task: AggregateTask) -> None:
    """Fail before parsing if a source changed after manifest discovery."""
    if file_sha256(task.path) != task.file_sha256:
        raise RuntimeError(f"aggregate source changed after manifest discovery: {task.path.name}")


def symbol_master_evidence(root: str | Path) -> dict[str, Any]:
    import pyarrow.parquet as pq

    path = Path(root) / "stock_basic_data.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    parquet = pq.ParquetFile(path)
    schema_text = str(parquet.schema_arrow)
    return {
        "name": path.name,
        "sha256": file_sha256(path),
        "size": path.stat().st_size,
        "rows": int(parquet.metadata.num_rows),
        "schema_sha256": hashlib.sha256(schema_text.encode()).hexdigest(),
    }


def verify_symbol_master(root: str | Path, evidence: dict[str, Any]) -> None:
    current = symbol_master_evidence(root)
    if current != evidence:
        raise RuntimeError("authoritative symbol master changed after manifest discovery")


def parse_expected_closed(args: argparse.Namespace, *, required: bool) -> tuple[dict[str, datetime], dict[str, Any] | None]:
    contract_path = getattr(args, "freshness_contract", None)
    naked = {
        timeframe: getattr(args, "expected_closed_" + timeframe, None)
        for timeframe in ("5f", "30f", "1d")
    }
    if contract_path:
        if any(naked.values()):
            raise ValueError("authoritative --freshness-contract cannot be combined with naked expected bounds")
        contract = load_freshness_contract(Path(contract_path))
        return (
            {timeframe: contract.expected_closed_watermarks[timeframe] for timeframe in ("5f", "30f", "1d")},
            {
                "contract_version": contract.contract_version,
                "sha256": contract.sha256,
                "normalized": contract.normalized,
            },
        )
    if required:
        raise ValueError("--freshness-contract is required for write mode")
    result: dict[str, datetime] = {}
    for timeframe in ("5f", "30f", "1d"):
        raw = naked[timeframe]
        if not raw:
            continue
        value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError(f"--expected-closed-{timeframe} must include a timezone")
        result[timeframe] = value
    return result, None


def parse_expected_dispositions(
    args: argparse.Namespace, *, required: bool,
) -> tuple[dict[str, int], set[str]]:
    raw_counts = getattr(args, "expected_quarantine_counts", None)
    if not raw_counts:
        if required:
            raise ValueError("--expected-quarantine-counts is required for write mode")
        counts: dict[str, int] = {}
    else:
        decoded = json.loads(str(raw_counts))
        if not isinstance(decoded, dict):
            raise ValueError("--expected-quarantine-counts must be a JSON object")
        counts = {str(key): int(value) for key, value in decoded.items() if int(value) != 0}
    invalid = set(counts) - POLICY_QUARANTINE_REASONS
    if invalid or any(value < 0 for value in counts.values()):
        raise ValueError("expected quarantine counts contain a non-policy reason or negative count")
    symbols = {
        normalize_symbol(item)
        for item in str(getattr(args, "expected_excluded_symbols", "") or "").split(",")
        if item.strip()
    }
    symbol_exclusion_count = sum(
        counts.get(reason, 0)
        for reason in ("symbol_not_in_authoritative_master", "symbol_not_in_pinned_active_universe")
    )
    if symbol_exclusion_count == 0 and symbols:
        raise ValueError("expected excluded symbols require a nonzero matching quarantine count")
    return counts, symbols


def preflight_manifest(
    tasks: Iterable[AggregateTask],
    *,
    symbol_meta: dict[str, dict[str, Any]],
    symbols_filter: set[str],
    exchanges: set[str],
    allow_bj: bool,
    halted_days: set[tuple[str, str]],
    pinned_symbols: set[str] | None,
    expected_closed: dict[str, datetime] | None,
    expected_quarantine_counts: dict[str, int],
    expected_excluded_symbols: set[str],
) -> dict[str, Any]:
    """Stream all accepted keys through a disk-backed duplicate detector."""
    accepted = 0
    quarantined = 0
    reasons: Counter[str] = Counter()
    excluded_symbols: set[str] = set()
    with tempfile.TemporaryDirectory(prefix="aggregate-increment-preflight-") as folder:
        database = sqlite3.connect(str(Path(folder) / "keys.sqlite3"))
        try:
            database.execute("pragma journal_mode=off")
            database.execute("pragma synchronous=off")
            database.execute(
                "create table keys(k text primary key,digest text not null,conflict integer not null default 0)"
            )
            for task in tasks:
                parsed = parse_task(
                    task,
                    symbol_meta=symbol_meta,
                    symbols_filter=symbols_filter,
                    exchanges=exchanges,
                    allow_bj=allow_bj,
                    halted_days=halted_days,
                    pinned_symbols=pinned_symbols,
                    expected_closed=expected_closed,
                )
                records = []
                for bar in parsed.bars:
                    key = f"{bar[0]}.{bar[1]}|{bar[2]}|{bar[3].isoformat()}"
                    digest = hashlib.sha256(json.dumps(bar, default=str, separators=(",", ":")).encode()).hexdigest()
                    records.append((key, digest))
                database.executemany(
                    """insert into keys(k,digest) values(?,?)
                       on conflict(k) do update set conflict=1
                       where keys.digest<>excluded.digest""",
                    records,
                )
                database.commit()
                conflict = database.execute("select k from keys where conflict=1 limit 1").fetchone()
                if conflict is not None:
                    raise RuntimeError("aggregate manifest contains conflicting cross-batch K-line keys")
                accepted += len(parsed.bars)
                quarantined += len(parsed.quarantines)
                reasons.update(item.reason for item in parsed.quarantines)
                excluded_symbols.update(
                    item.symbol_text
                    for item in parsed.quarantines
                    if item.reason in {
                        "symbol_not_in_authoritative_master",
                        "symbol_not_in_pinned_active_universe",
                    } and item.symbol_text
                )
        finally:
            database.close()
    actual_counts = dict(sorted(reasons.items()))
    if actual_counts != dict(sorted(expected_quarantine_counts.items())):
        raise RuntimeError(
            "aggregate quarantine disposition drift: "
            f"expected={dict(sorted(expected_quarantine_counts.items()))},actual={actual_counts}"
        )
    if excluded_symbols != expected_excluded_symbols:
        raise RuntimeError(
            "aggregate excluded-symbol disposition drift: "
            f"expected={sorted(expected_excluded_symbols)},actual={sorted(excluded_symbols)}"
        )
    return {
        "accepted_rows": accepted,
        "quarantined_rows": quarantined,
        "quarantine_counts": actual_counts,
        "excluded_symbols": sorted(excluded_symbols),
        "decision": "PASS",
    }


def _bar_timestamp(timeframe: str, raw: dict[str, Any]) -> datetime:
    value = raw["trade_date"] if timeframe == "1d" else raw["trade_time"]
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value))
    return canonical_kline_timestamp(
        timeframe,
        value.replace(tzinfo=SHANGHAI_TZ),
        date_only=timeframe == "1d",
    )


def _quarantine(
    task: AggregateTask,
    source_row: int,
    raw: dict[str, Any],
    raw_ts: str,
    symbol: str | None,
    reason: str,
) -> QuarantineRecord:
    return QuarantineRecord(
        SOURCE_NAME, task.source_ref, source_row, symbol, task.timeframe,
        raw_ts, reason[:500], raw,
    )


def bind_quarantines_to_run(parsed: ParsedBatch, import_run_id: UUID) -> ParsedBatch:
    """Make policy evidence unique to the frozen run/policy identity."""
    return ParsedBatch(
        symbols=parsed.symbols,
        bars=parsed.bars,
        quarantines=[
            replace(item, source_ref=f"{item.source_ref}@run_id={import_run_id}")
            for item in parsed.quarantines
        ],
        last_source_row=parsed.last_source_row,
    )


def _batch_timestamp_bounds(bars: list[tuple]) -> tuple[datetime, datetime]:
    """Return static bounds used to prune hypertable chunks during verification."""
    if not bars:
        raise ValueError("staged K-line batch must not be empty")
    timestamps = [row[3] for row in bars]
    return min(timestamps), max(timestamps)


async def _fetch_stage_mismatch(conn: Any, sql: str, bars: list[tuple]) -> Any:
    batch_min_ts, batch_max_ts = _batch_timestamp_bounds(bars)
    return await conn.fetchrow(sql, batch_min_ts, batch_max_ts)


APPEND_ONLY_MISMATCH_SQL = """
select symbol.code || '.' || symbol.exchange as symbol, stage.timeframe, stage.bar_end
  from _native_parquet_kline_stage stage
  join symbols symbol on symbol.code=stage.code and symbol.exchange=stage.exchange
  join _aggregate_scope_original scope
    on scope.symbol_id=symbol.id and scope.timeframe=stage.timeframe
  left join klines existing
    on existing.symbol_id=symbol.id and existing.timeframe=stage.timeframe and existing.ts=stage.bar_end
   and existing.ts >= $1 and existing.ts <= $2
 where stage.bar_end <= scope.original_max
   and (
       existing.symbol_id is null
       or existing.open_x1000 is distinct from stage.open_x1000
       or existing.high_x1000 is distinct from stage.high_x1000
       or existing.low_x1000 is distinct from stage.low_x1000
       or existing.close_x1000 is distinct from stage.close_x1000
       or existing.volume is distinct from stage.volume
       or existing.amount_x100 is distinct from stage.amount_x100
       or existing.is_complete is distinct from stage.is_complete
       or existing.revision is distinct from stage.revision
       or existing.source is distinct from stage.source
   )
 limit 1
"""

INSERT_NEW_ROWS_SQL = """
with staged_rows as (
    select distinct on (symbol.id, stage.timeframe, stage.bar_end)
           symbol.id as symbol_id, stage.*
      from _native_parquet_kline_stage stage
      join symbols symbol on symbol.code=stage.code and symbol.exchange=stage.exchange
     order by symbol.id, stage.timeframe, stage.bar_end
), inserted as (
    insert into klines(symbol_id,timeframe,ts,open_x1000,high_x1000,low_x1000,close_x1000,
                       volume,amount_x100,is_complete,revision,source)
    select stage.symbol_id,stage.timeframe,stage.bar_end,stage.open_x1000,stage.high_x1000,
           stage.low_x1000,stage.close_x1000,stage.volume,stage.amount_x100,
           stage.is_complete,stage.revision,stage.source
      from staged_rows stage
      join _aggregate_scope_original scope
        on scope.symbol_id=stage.symbol_id and scope.timeframe=stage.timeframe
     where scope.original_max is null or stage.bar_end > scope.original_max
    on conflict (symbol_id,timeframe,ts) do nothing
    returning 1
)
select count(*)::bigint from inserted
"""

ALL_STAGE_MATCH_SQL = APPEND_ONLY_MISMATCH_SQL.replace(
    "stage.bar_end <= scope.original_max\n   and (", "(",
)

LOCK_ACTIVE_SYMBOLS_SQL = """
select symbol.id,symbol.code,symbol.exchange
  from symbols symbol
  join (select distinct code,exchange from _native_parquet_kline_stage) stage
    on stage.code=symbol.code and stage.exchange=symbol.exchange
 where symbol.is_active is true
 order by symbol.id
 for share of symbol
"""

CREATE_SCOPE_ORIGINAL_SQL = """
create temp table _aggregate_scope_original on commit drop as
select scope.symbol_id,
       scope.timeframe,
       count(catalog.symbol_id)::integer as catalog_rows,
       bool_and(
           catalog.symbol_id is not null
           and catalog.bounds_complete is true
           and (
               (catalog.state='present' and catalog.min_ts is not null
                and catalog.max_ts is not null and catalog.min_ts <= catalog.max_ts)
               or
               (catalog.state='empty' and catalog.min_ts is null and catalog.max_ts is null)
           )
       ) as catalog_valid,
       max(catalog.max_ts) filter (where catalog.state='present') as original_max
  from (select distinct symbol.id as symbol_id,stage.timeframe
          from _native_parquet_kline_stage stage
          join symbols symbol on symbol.code=stage.code and symbol.exchange=stage.exchange) scope
  left join kline_scope_catalog catalog
    on catalog.generation_id=$1
   and catalog.symbol_id=scope.symbol_id
   and catalog.timeframe=scope.timeframe
 group by scope.symbol_id,scope.timeframe
"""

INVALID_SCOPE_ORIGINAL_SQL = """
select symbol_id,timeframe,catalog_rows,catalog_valid
  from _aggregate_scope_original
 where catalog_rows <> 1 or not catalog_valid
 limit 1
"""


async def _create_scope_original(conn: Any, catalog_generation_id: UUID) -> None:
    await conn.execute(CREATE_SCOPE_ORIGINAL_SQL, catalog_generation_id)
    invalid_original = await conn.fetchrow(INVALID_SCOPE_ORIGINAL_SQL)
    if invalid_original is not None:
        raise RuntimeError(
            "aggregate increment scope catalog bounds are missing or incomplete: "
            f"symbol_id={invalid_original['symbol_id']} "
            f"timeframe={invalid_original['timeframe']}"
        )


class AggregateIncrementWriter(NativeParquetWriter):
    """Single-connection writer for the aggregate append-only contract."""

    def __init__(self, database_url: str) -> None:
        super().__init__(database_url, pool_min_size=1, pool_max_size=1)
        self._owns_global_lock = False
        self._lock_conn = None

    async def open(self) -> None:
        import asyncpg

        # The lock-owning connection is also the only writer connection.  A
        # network loss therefore aborts the current transaction and makes all
        # later writes fail, with no check-then-write ownership window.
        try:
            self._lock_conn = await asyncpg.connect(self.database_url)
            self._owns_global_lock = bool(await self._lock_conn.fetchval(
                "select pg_try_advisory_lock($1::bigint)", GLOBAL_WRITER_LOCK_KEY,
            ))
        except Exception:
            if self._lock_conn is not None:
                await self._lock_conn.close()
                self._lock_conn = None
            raise
        if not self._owns_global_lock:
            await self._lock_conn.close()
            self._lock_conn = None
            raise RuntimeError("another aggregate incremental importer owns the global writer lock")

    async def close(self) -> None:
        release_error: BaseException | None = None
        if self._lock_conn is not None and self._owns_global_lock:
            try:
                released = bool(await self._lock_conn.fetchval(
                    "select pg_advisory_unlock($1::bigint)", GLOBAL_WRITER_LOCK_KEY,
                ))
                if not released:
                    release_error = RuntimeError("aggregate incremental importer lost its global writer lock")
            except BaseException as exc:
                release_error = exc
            finally:
                self._owns_global_lock = False
                await self._lock_conn.close()
                self._lock_conn = None
        if release_error is not None:
            raise release_error

    async def create_aggregate_run(self, *, import_run_id: UUID, parameters: dict[str, Any]) -> None:
        if self._lock_conn is None:
            raise RuntimeError("aggregate writer is not open")
        encoded = json.dumps(parameters, sort_keys=True, default=str)
        conn = self._lock_conn
        async with conn.transaction():
            await conn.execute(
                """insert into kline_import_runs(import_run_id,source_name,status,parameters)
                   values($1,$2,'running',$3::jsonb) on conflict(import_run_id) do nothing""",
                import_run_id, SOURCE_NAME, encoded,
            )
            row = await conn.fetchrow(
                "select source_name,status,parameters from kline_import_runs where import_run_id=$1 for update",
                import_run_id,
            )
            if row is None or str(row["source_name"]) != SOURCE_NAME:
                raise RuntimeError("import run identity belongs to a different source")
            stored = row["parameters"]
            if isinstance(stored, str):
                stored = json.loads(stored)
            if stored != parameters:
                raise RuntimeError("import run manifest changed; use a new run ID")
            if str(row["status"]) == "failed":
                raise RuntimeError("failed import run cannot be resumed")

    async def fetch_pinned_catalog(self) -> dict[str, Any]:
        if self._lock_conn is None:
            raise RuntimeError("aggregate writer is not open")
        conn = self._lock_conn
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            control = await conn.fetchrow(
                """select control.active_generation_id,control.revision,
                          generation.status,generation.expected_scope_count
                     from kline_scope_catalog_control control
                     left join kline_scope_catalog_generations generation
                       on generation.generation_id=control.active_generation_id
                    where control.control_key='active'"""
            )
            if (
                control is None
                or control["active_generation_id"] is None
                or str(control["status"]) != "complete"
            ):
                raise RuntimeError("active complete K-line scope catalog is required")
            rows = await conn.fetch(
                """select distinct symbol.code || '.' || symbol.exchange as symbol
                     from active_kline_scope_catalog catalog
                     join symbols symbol on symbol.id=catalog.symbol_id
                    where symbol.is_active is true"""
            )
            symbols = {str(row["symbol"]) for row in rows}
            catalog_count = int(await conn.fetchval(
                "select count(*) from kline_scope_catalog where generation_id=$1",
                control["active_generation_id"],
            ))
            if catalog_count != int(control["expected_scope_count"]):
                raise RuntimeError("active K-line scope catalog manifest is incomplete")
        manifest = json.dumps(sorted(symbols), separators=(",", ":"))
        return {
            "generation_id": str(control["active_generation_id"]),
            "revision": int(control["revision"]),
            "expected_scope_count": int(control["expected_scope_count"]),
            "active_symbols": symbols,
            "active_universe_sha256": hashlib.sha256(manifest.encode()).hexdigest(),
        }

    async def append_import_batch(
        self,
        *,
        import_run_id: UUID,
        task: AggregateTask,
        parsed: ParsedBatch,
        catalog_generation_id: UUID,
        catalog_revision: int,
    ) -> int:
        if self._lock_conn is None or self._lock_conn.is_closed() or not self._owns_global_lock:
            raise RuntimeError("aggregate writer lost its lock-owning connection")
        return await commit_import_batch(
            self._lock_conn,
            import_run_id=import_run_id,
            checkpoint=ImportCheckpoint(task.source_ref, task.source_checksum, parsed.last_source_row),
            quarantines=parsed.quarantines,
            write_accepted=lambda transaction_conn: self._append_only_on_connection(
                transaction_conn,
                bars=parsed.bars,
                catalog_generation_id=catalog_generation_id,
                catalog_revision=catalog_revision,
            ),
        )

    async def completed_import_checkpoint(
        self, *, import_run_id: UUID, task: AggregateTask,
    ) -> tuple[int, int] | None:
        if self._lock_conn is None or self._lock_conn.is_closed():
            raise RuntimeError("aggregate writer lost its lock-owning connection")
        row = await self._lock_conn.fetchrow(
            """select accepted_rows,quarantined_rows from kline_import_checkpoints
                where import_run_id=$1 and source_ref=$2 and source_checksum=$3 and status='completed'""",
            import_run_id, task.source_ref, task.source_checksum,
        )
        return None if row is None else (int(row["accepted_rows"]), int(row["quarantined_rows"]))

    async def _append_only_on_connection(
        self,
        conn,
        *,
        bars: list[tuple],
        catalog_generation_id: UUID,
        catalog_revision: int,
    ) -> int:
        if not bars:
            return 0
        control = await conn.fetchrow(
            """select active_generation_id,revision from kline_scope_catalog_control
                where control_key='active' for share"""
        )
        if (
            control is None
            or control["active_generation_id"] != catalog_generation_id
            or int(control["revision"]) != catalog_revision
        ):
            raise RuntimeError("active K-line scope catalog drifted after import manifest freeze")
        await create_kline_stage(conn)
        await conn.copy_records_to_table(
            "_native_parquet_kline_stage", records=bars,
            columns=["code", "exchange", "timeframe", "bar_end", "open_x1000", "high_x1000",
                     "low_x1000", "close_x1000", "volume", "amount_x100", "is_complete", "revision", "source"],
        )
        expected_symbols = int(await conn.fetchval(
            """select count(*) from (
                   select distinct code,exchange from _native_parquet_kline_stage
               ) stage"""
        ))
        locked_symbols = await conn.fetch(LOCK_ACTIVE_SYMBOLS_SQL)
        if len(locked_symbols) != expected_symbols:
            raise RuntimeError("aggregate increment contains an inactive or unpinned database symbol")
        missing_pinned_symbol = await conn.fetchrow(
            """select symbol.code,symbol.exchange
                 from unnest($2::text[]) target(symbol_text)
                 join symbols symbol on symbol.code || '.' || symbol.exchange=target.symbol_text
                 left join kline_scope_catalog catalog
                   on catalog.generation_id=$1 and catalog.symbol_id=symbol.id
                where catalog.symbol_id is null limit 1""",
            catalog_generation_id,
            [f"{row['code']}.{row['exchange']}" for row in locked_symbols],
        )
        if missing_pinned_symbol is not None:
            raise RuntimeError("aggregate increment contains a symbol outside the pinned database universe")
        missing_scope = await conn.fetchrow(
            """select stage.code,stage.exchange,stage.timeframe
                 from _native_parquet_kline_stage stage
                 join symbols symbol on symbol.code=stage.code and symbol.exchange=stage.exchange
                 left join kline_scope_catalog catalog
                   on catalog.generation_id=$1 and catalog.symbol_id=symbol.id
                  and catalog.timeframe=stage.timeframe
                where catalog.symbol_id is null limit 1""",
            catalog_generation_id,
        )
        if missing_scope is not None:
            raise RuntimeError("aggregate increment scope is outside the pinned active catalog manifest")
        duplicate = await conn.fetchrow(
            """select code,exchange,timeframe,bar_end from _native_parquet_kline_stage
               group by code,exchange,timeframe,bar_end
               having count(distinct row(open_x1000,high_x1000,low_x1000,close_x1000,
                                         volume,amount_x100,is_complete)) > 1 limit 1"""
        )
        if duplicate is not None:
            raise RuntimeError("aggregate increment contains conflicting duplicate K-line keys")
        # Operationally this importer has one pool connection.  Transaction
        # advisory locks make that uniqueness explicit across accidental
        # second processes, with one deterministic lock per scope.
        scopes = await conn.fetch(
            """select distinct symbol.id as symbol_id,stage.timeframe
                 from _native_parquet_kline_stage stage
                 join symbols symbol on symbol.code=stage.code and symbol.exchange=stage.exchange
                order by symbol.id,stage.timeframe"""
        )
        for scope in scopes:
            await conn.execute("select pg_advisory_xact_lock($1,$2)", scope["symbol_id"], scope["timeframe"])
        await _create_scope_original(conn, catalog_generation_id)
        mismatch = await _fetch_stage_mismatch(conn, APPEND_ONLY_MISMATCH_SQL, bars)
        if mismatch is not None:
            raise RuntimeError(
                f"append-only overlap mismatch for {mismatch['symbol']} timeframe={mismatch['timeframe']}"
            )
        inserted = int(await conn.fetchval(INSERT_NEW_ROWS_SQL))
        # Recheck after ON CONFLICT so an unexpected concurrent writer cannot
        # turn a conflicting insert into a silent no-op.
        mismatch = await _fetch_stage_mismatch(conn, ALL_STAGE_MATCH_SQL, bars)
        if mismatch is not None:
            raise RuntimeError("append-only post-insert verification failed")
        await register_source_coverage(conn)
        scope_rows = await conn.fetch(
            """select symbol.id as symbol_id,stage.timeframe,min(stage.bar_end) as min_ts,max(stage.bar_end) as max_ts
                 from _native_parquet_kline_stage stage
                 join symbols symbol on symbol.code=stage.code and symbol.exchange=stage.exchange
                group by symbol.id,stage.timeframe order by symbol.id,stage.timeframe"""
        )
        await record_present_scopes(conn, scopes=[
            (row["symbol_id"], row["timeframe"], row["min_ts"], row["max_ts"]) for row in scope_rows
        ])
        await upsert_watermarks(conn)
        return inserted

    async def finalize_aggregate_run(
        self,
        *,
        import_run_id: UUID,
        expected_tasks: int,
        expected_parameters: dict[str, Any],
        catalog_generation_id: UUID,
        catalog_revision: int,
    ) -> str:
        if self._lock_conn is None or self._lock_conn.is_closed() or not self._owns_global_lock:
            raise RuntimeError("aggregate writer lost its lock-owning connection")
        conn = self._lock_conn
        async with conn.transaction():
            locked = await conn.fetchrow(
                "select status,parameters from kline_import_runs where import_run_id=$1 for update",
                import_run_id,
            )
            if locked is None:
                raise RuntimeError("import run does not exist")
            stored = locked["parameters"]
            if isinstance(stored, str):
                stored = json.loads(stored)
            if stored != expected_parameters:
                raise RuntimeError("import run policy or manifest changed before finalization")
            control = await conn.fetchrow(
                """select active_generation_id,revision from kline_scope_catalog_control
                    where control_key='active' for share"""
            )
            if (
                control is None
                or control["active_generation_id"] != catalog_generation_id
                or int(control["revision"]) != catalog_revision
            ):
                raise RuntimeError("active K-line scope catalog drifted before finalization")
            row = await conn.fetchrow(
                """select count(*)::bigint as tasks,
                          count(*) filter(where status='completed')::bigint as completed
                     from kline_import_checkpoints where import_run_id=$1""",
                import_run_id,
            )
            if row is None or int(row["tasks"]) != expected_tasks or int(row["completed"]) != expected_tasks:
                return "running"
            await conn.execute(
                """update kline_import_runs set status='completed',completed_at=now(),
                       summary=jsonb_build_object('expected_tasks',$2::integer,'completed_tasks',$2::integer)
                     where import_run_id=$1 and status='running'""",
                import_run_id, expected_tasks,
            )
            return "completed"


def manifest_run_id(
    root: str | Path,
    tasks: Iterable[AggregateTask],
    *,
    symbols: Iterable[str] = (),
    exchanges: Iterable[str] = ("SH", "SZ"),
    allow_bj: bool = False,
    catalog_generation_id: str | None = None,
    catalog_revision: int | None = None,
    symbol_master_sha256: str | None = None,
    freshness_contract_sha256: str | None = None,
    disposition_manifest_sha256: str | None = None,
) -> UUID:
    identity = json.dumps({
        "adapter": "aggregate_increment_import",
        "root": str(Path(root).resolve()),
        "tasks": [(task.source_ref, task.source_checksum) for task in tasks],
        "symbols": sorted(symbols),
        "exchanges": sorted(exchanges),
        "allow_bj": bool(allow_bj),
        "catalog_generation_id": catalog_generation_id,
        "catalog_revision": catalog_revision,
        "symbol_master_sha256": symbol_master_sha256,
        "freshness_contract_sha256": freshness_contract_sha256,
        "disposition_manifest_sha256": disposition_manifest_sha256,
        "policy_version": POLICY_VERSION,
    }, sort_keys=True, separators=(",", ":"))
    return uuid5(IMPORT_RUN_NAMESPACE, identity)


async def run_once(args: argparse.Namespace) -> dict[str, Any]:
    batch_size = int(getattr(args, "batch_size", DEFAULT_BATCH_SIZE))
    tasks = discover_tasks(args.root, batch_size=batch_size)
    master_evidence = symbol_master_evidence(args.root)
    symbol_meta = load_authoritative_symbol_meta(args.root)
    symbols_filter = {
        normalize_symbol(item) for item in str(getattr(args, "symbols", "") or "").split(",") if item.strip()
    }
    exchanges = {
        item.strip().upper() for item in str(getattr(args, "exchanges", "SH,SZ") or "").split(",") if item.strip()
    }
    invalid_exchanges = exchanges - {"SH", "SZ", "BJ"}
    if not exchanges or invalid_exchanges:
        raise ValueError(f"invalid exchanges: {','.join(sorted(invalid_exchanges or {'none'}))}")
    allow_bj = bool(getattr(args, "allow_bj", False))
    if "BJ" in exchanges and not allow_bj:
        raise ValueError("BJ import requires explicit --allow-bj; BJ 30f remains prohibited")
    halted_days = find_zero_turnover_halt_days(args.root)
    expected_closed, freshness_contract = parse_expected_closed(args, required=not bool(args.dry_run))
    expected_quarantine_counts, expected_excluded_symbols = parse_expected_dispositions(
        args, required=not bool(args.dry_run),
    )
    expected_closed_text = {
        timeframe: value.isoformat() for timeframe, value in sorted(expected_closed.items())
    }
    expected_closed_sha256 = hashlib.sha256(
        json.dumps(expected_closed_text, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    disposition_manifest = {
        "quarantine_counts": dict(sorted(expected_quarantine_counts.items())),
        "excluded_symbols": sorted(expected_excluded_symbols),
    }
    disposition_manifest_sha256 = hashlib.sha256(
        json.dumps(disposition_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    source_files = []
    for filename in FILE_CONTRACTS:
        file_tasks = [task for task in tasks if task.path.name == filename]
        source_files.append({
            "name": filename,
            "sha256": file_tasks[0].file_sha256,
            "rows": sum(task.row_count for task in file_tasks),
            "tasks": len(file_tasks),
        })
    summary: dict[str, Any] = {
        "adapter": "aggregate_increment_import",
        "root": str(Path(args.root).resolve()),
        "tasks": len(tasks),
        "files": list(FILE_CONTRACTS),
        "source_files": source_files,
        "symbol_master": master_evidence,
        "timeframes": ["5f", "30f", "1d"],
        "batch_size": batch_size,
        "writer_pool": 1,
        "policy_version": POLICY_VERSION,
        "symbols": sorted(symbols_filter),
        "exchanges": sorted(exchanges),
        "allow_bj": allow_bj,
        "halted_symbol_days": len(halted_days),
        "expected_closed": expected_closed_text,
        "expected_closed_sha256": expected_closed_sha256,
        "freshness_contract": freshness_contract,
        "expected_quarantine_counts": dict(sorted(expected_quarantine_counts.items())),
        "expected_excluded_symbols": sorted(expected_excluded_symbols),
        "disposition_manifest_sha256": disposition_manifest_sha256,
        "dry_run": bool(args.dry_run),
    }
    if args.dry_run:
        summary["validation_level"] = "manifest_only"
        return summary
    if not getattr(args, "database_url", None):
        raise ValueError("--database-url or DATABASE_URL is required for write mode")
    writer = AggregateIncrementWriter(args.database_url)
    await writer.open()
    try:
        catalog_pin = await writer.fetch_pinned_catalog()
        pinned_symbols = catalog_pin["active_symbols"]
        if not pinned_symbols:
            raise RuntimeError("active K-line scope catalog universe is missing or empty")
        missing_requested = symbols_filter - pinned_symbols
        if missing_requested:
            raise RuntimeError(
                "requested symbols are outside the pinned active universe: " + ",".join(sorted(missing_requested))
            )
        verified_preflight: set[Path] = set()
        for task in tasks:
            if task.path not in verified_preflight:
                verify_frozen_file(task)
                verified_preflight.add(task.path)
        verify_symbol_master(args.root, master_evidence)
        preflight = preflight_manifest(
            tasks,
            symbol_meta=symbol_meta,
            symbols_filter=symbols_filter,
            exchanges=exchanges,
            allow_bj=allow_bj,
            halted_days=halted_days,
            pinned_symbols=pinned_symbols,
            expected_closed=expected_closed,
            expected_quarantine_counts=expected_quarantine_counts,
            expected_excluded_symbols=expected_excluded_symbols,
        )
        # Tail verification closes the preflight-read race before the first
        # durable run/checkpoint or K-line write.
        for task in tasks:
            if task.path in verified_preflight:
                verify_frozen_file(task)
                verified_preflight.remove(task.path)
        verify_symbol_master(args.root, master_evidence)
        run_id = UUID(args.import_run_id) if getattr(args, "import_run_id", None) else manifest_run_id(
            args.root,
            tasks,
            symbols=symbols_filter,
            exchanges=exchanges,
            allow_bj=allow_bj,
            catalog_generation_id=catalog_pin["generation_id"],
            catalog_revision=catalog_pin["revision"],
            symbol_master_sha256=master_evidence["sha256"],
            freshness_contract_sha256=freshness_contract["sha256"],
            disposition_manifest_sha256=disposition_manifest_sha256,
        )
        parameters = {
            **summary,
            "dry_run": False,
            "manifest_sha256": hashlib.sha256(
                json.dumps([(task.source_ref, task.source_checksum) for task in tasks], separators=(",", ":")).encode()
            ).hexdigest(),
            "catalog_generation_id": catalog_pin["generation_id"],
            "catalog_revision": catalog_pin["revision"],
            "catalog_expected_scope_count": catalog_pin["expected_scope_count"],
            "active_universe_sha256": catalog_pin["active_universe_sha256"],
            "preflight_accepted_rows": preflight["accepted_rows"],
            "preflight_quarantined_rows": preflight["quarantined_rows"],
            "preflight_quarantine_counts": preflight["quarantine_counts"],
            "preflight_excluded_symbols": preflight["excluded_symbols"],
            "preflight_decision": preflight["decision"],
        }
        await writer.create_aggregate_run(import_run_id=run_id, parameters=parameters)
        accepted = quarantined = resumed = 0
        verified_files: set[Path] = set()
        for task in tasks:
            if task.path not in verified_files:
                verify_frozen_file(task)
                verified_files.add(task.path)
            completed = await writer.completed_import_checkpoint(import_run_id=run_id, task=task)
            if completed is not None:
                accepted += completed[0]
                quarantined += completed[1]
                resumed += 1
                continue
            parsed = parse_task(
                task,
                symbol_meta=symbol_meta,
                symbols_filter=symbols_filter,
                exchanges=exchanges,
                allow_bj=allow_bj,
                halted_days=halted_days,
                pinned_symbols=pinned_symbols,
                expected_closed=expected_closed,
            )
            parsed = bind_quarantines_to_run(parsed, run_id)
            accepted += await writer.append_import_batch(
                import_run_id=run_id,
                task=task,
                parsed=parsed,
                catalog_generation_id=UUID(catalog_pin["generation_id"]),
                catalog_revision=catalog_pin["revision"],
            )
            quarantined += len(parsed.quarantines)
        # A source can be replaced while row groups are being consumed.  The
        # first-use checks above prevent starting from a changed file; these
        # tail checks make finalization refuse any in-flight replacement.
        for task in tasks:
            if task.path in verified_files:
                verify_frozen_file(task)
                verified_files.remove(task.path)
        verify_symbol_master(args.root, master_evidence)
        status = await writer.finalize_aggregate_run(
            import_run_id=run_id,
            expected_tasks=len(tasks),
            expected_parameters=parameters,
            catalog_generation_id=UUID(catalog_pin["generation_id"]),
            catalog_revision=catalog_pin["revision"],
        )
    finally:
        await writer.close()
    return {
        **summary, "import_run_id": str(run_id), "status": status,
        "accepted_rows": accepted, "quarantined_rows": quarantined, "resumed_tasks": resumed,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append approved F:data aggregate incremental Parquet files")
    parser.add_argument("--root", default=os.getenv("AGGREGATE_INCREMENT_ROOT", DEFAULT_ROOT))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("AGGREGATE_INCREMENT_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    parser.add_argument("--import-run-id", default=os.getenv("AGGREGATE_INCREMENT_RUN_ID"))
    parser.add_argument("--symbols", default=os.getenv("AGGREGATE_INCREMENT_SYMBOLS"),
                        help="Optional explicit canonical symbols for a bounded canary")
    parser.add_argument("--exchanges", default=os.getenv("AGGREGATE_INCREMENT_EXCHANGES", "SH,SZ"),
                        help="Explicit exchange allowlist; production default is SH,SZ")
    parser.add_argument("--allow-bj", action="store_true", default=os.getenv("AGGREGATE_INCREMENT_ALLOW_BJ") == "1",
                        help="Explicit future-source opt-in for BJ 5f/1d; BJ 30f remains prohibited")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("AGGREGATE_INCREMENT_DRY_RUN") == "1")
    parser.add_argument("--freshness-contract", default=os.getenv("AGGREGATE_INCREMENT_FRESHNESS_CONTRACT"),
                        help="Authoritative exact-five-level Module C freshness contract (required for writes)")
    parser.add_argument("--expected-closed-5f", default=os.getenv("AGGREGATE_INCREMENT_EXPECTED_CLOSED_5F"))
    parser.add_argument("--expected-closed-30f", default=os.getenv("AGGREGATE_INCREMENT_EXPECTED_CLOSED_30F"))
    parser.add_argument("--expected-closed-1d", default=os.getenv("AGGREGATE_INCREMENT_EXPECTED_CLOSED_1D"))
    parser.add_argument("--expected-quarantine-counts", default=os.getenv("AGGREGATE_INCREMENT_EXPECTED_QUARANTINE_COUNTS"),
                        help="Frozen JSON reason->count policy disposition manifest")
    parser.add_argument("--expected-excluded-symbols", default=os.getenv("AGGREGATE_INCREMENT_EXPECTED_EXCLUDED_SYMBOLS"),
                        help="Frozen comma-separated master/catalog symbol policy exclusions")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    return parser.parse_args(argv)


def main() -> None:
    result = asyncio.run(run_once(parse_args()))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
