"""Import a small, explicit sample from the Device B ``F:\\data`` layout.

This is deliberately a bounded, static-shard import adapter.  It converts the one-file-per-symbol intraday files plus the combined
daily file into the existing transactional native-parquet writer.  Therefore
accepted K-lines, raw quarantines and the durable checkpoint are committed in
one transaction.  Use ``--dry-run`` first; no implicit symbol discovery is
allowed when writing.

Known source contract: daily ``amount`` is thousand-yuan and ``vol`` is
hundred-shares; intraday amount is yuan and volume is shares.  The conversion
is nevertheless independently checked row-by-row by ``volume_normalization``.
BJ 30 minute source files are rejected as a class because the audit found they
are not native 30-minute bars.  A future approved resampling contract must use
a different importer/source profile.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID, uuid4, uuid5
from zoneinfo import ZoneInfo

from collector.kline_import_quarantine import QuarantineRecord
from collector.native_parquet_import import (
    NativeParquetWriter,
    ParsedBatch,
    infer_asset_type,
    normalize_symbol,
    valid_ohlc,
)
from collector.storage.postgres import amount_to_x100, price_to_x1000, source_to_code, timeframe_to_db_code
from collector.volume_normalization import decide_volume_multiplier
from trading_protocol import canonical_kline_timestamp

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_ROOT = r"F:\data"
SOURCE_NAME = "parquet_native"
SOURCE_CODE = source_to_code(SOURCE_NAME)
REQUIRED_INTRADAY = ("ts_code", "trade_date", "trade_time", "open", "high", "low", "close", "vol", "amount")
REQUIRED_DAILY = ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount")
TIMEFRAME_DIRS = {"5f": "stock_5min", "30f": "stock_30min"}
# A fixed namespace makes an omitted --import-run-id resumable while ensuring
# each static shard owns a distinct durable run/checkpoint identity.
IMPORT_RUN_NAMESPACE = UUID("e3f779a3-6146-49b9-8cfa-8bea7fbb44ef")


@dataclass(frozen=True)
class LocalTask:
    timeframe: str
    path: Path
    symbol: str
    source_ref: str
    source_checksum: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import explicit sample symbols from F:data Parquet layout")
    parser.add_argument("--root", default=os.getenv("LOCAL_PARQUET_ROOT", DEFAULT_ROOT))
    parser.add_argument("--timeframes", default=os.getenv("LOCAL_PARQUET_TIMEFRAMES", "5f,30f,1d"))
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--symbols", default=os.getenv("LOCAL_PARQUET_SYMBOLS"),
                           help="Explicit comma-separated symbols, e.g. 000001.SZ,600000.SH")
    selection.add_argument("--active-only", action="store_true", default=os.getenv("LOCAL_PARQUET_ACTIVE_ONLY") == "1",
                           help="Read the active symbol set from PostgreSQL; cannot be combined with --symbols")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("LOCAL_PARQUET_DRY_RUN") == "1")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("LOCAL_PARQUET_BATCH_SIZE", "50000")))
    parser.add_argument(
        "--import-run-id",
        default=os.getenv("LOCAL_PARQUET_IMPORT_RUN_ID"),
        help="UUID to resume the same import run after a crash; reuse it verbatim on retry.",
    )
    parser.add_argument("--shard-index", type=int, default=int(os.getenv("LOCAL_PARQUET_SHARD_INDEX", "0")),
                        help="Zero-based static shard over the sorted selected symbol list")
    parser.add_argument("--shard-count", type=int, default=int(os.getenv("LOCAL_PARQUET_SHARD_COUNT", "1")),
                        help="Total number of static, non-overlapping import shards")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"))
    return parser.parse_args(argv)


def parse_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def resolve_import_run_id(value: str | UUID | None) -> UUID:
    """Use an explicit durable run identity when resuming an interrupted run."""
    if value is None or str(value).strip() == "":
        return uuid4()
    return UUID(str(value))


def validate_shard(*, shard_index: int, shard_count: int) -> None:
    if shard_count < 1:
        raise ValueError("--shard-count must be >= 1")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"--shard-index must be in [0, {shard_count - 1}]")


def static_shard(symbols: Iterable[str], *, shard_index: int, shard_count: int) -> list[str]:
    """Return a stable, disjoint shard over canonical sorted symbols."""
    validate_shard(shard_index=shard_index, shard_count=shard_count)
    selected = sorted({normalize_symbol(symbol) for symbol in symbols})
    return selected[shard_index::shard_count]


def deterministic_import_run_id(*, root: str | Path, timeframes: Iterable[str], scope: str,
                                shard_index: int, shard_count: int) -> UUID:
    """A retry-stable run ID for one static shard when no ID is supplied."""
    identity = json.dumps({
        "adapter": "local_parquet_import", "root": str(Path(root).resolve()),
        "timeframes": list(timeframes), "scope": scope,
        "shard_index": shard_index, "shard_count": shard_count,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return uuid5(IMPORT_RUN_NAMESPACE, identity)


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return "sha256=" + digest.hexdigest()


def active_symbols_from_master(root: str | Path) -> set[str]:
    """Read the checked-in local active universe without relying on a DB seed.

    A fresh database has no ``symbols`` rows yet.  In that bootstrap state the
    local stock-basic parquet is the authoritative active universe: only
    ``list_status == 'L'`` is eligible.  Read it in batches so this remains
    bounded if the master grows.
    """
    import pyarrow.parquet as pq

    path = Path(root) / "stock_basic_data.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"active-only requires database symbols or local master: {path}")
    parquet = pq.ParquetFile(path)
    required = {"ts_code", "list_status"}
    missing = required - set(parquet.schema_arrow.names)
    if missing:
        raise ValueError(f"active symbol master missing required columns: {','.join(sorted(missing))}")
    active: set[str] = set()
    for batch in parquet.iter_batches(batch_size=65_536, columns=["ts_code", "list_status"]):
        for symbol, status in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()):
            if status == "L" and symbol:
                active.add(normalize_symbol(str(symbol)))
    return active


def discover_tasks(root: str | Path, *, timeframes: Iterable[str], symbols: Iterable[str],
                   exclude_bj_30f: bool = False) -> list[LocalTask]:
    root = Path(root)
    symbols = sorted({normalize_symbol(symbol) for symbol in symbols})
    tasks: list[LocalTask] = []
    for timeframe in timeframes:
        if timeframe not in {"5f", "30f", "1d"}:
            raise ValueError(f"unsupported local Parquet timeframe: {timeframe}")
        if timeframe == "1d":
            path = root / "stock_daily.parquet"
            if not path.is_file():
                raise FileNotFoundError(path)
            checksum = file_checksum(path)
            tasks.extend(LocalTask(timeframe, path, symbol, f"stock_daily.parquet#{symbol}", checksum + ";symbol=" + symbol) for symbol in symbols)
            continue
        folder = root / TIMEFRAME_DIRS[timeframe]
        for symbol in symbols:
            if exclude_bj_30f and timeframe == "30f" and symbol.endswith(".BJ"):
                continue
            path = folder / f"{symbol}.parquet"
            source_ref = path.relative_to(root).as_posix()
            # Missing per-symbol intraday history is a coverage exception, not
            # a fatal process error.  Keep a synthetic task so normal write
            # mode atomically persists a raw quarantine and completed
            # checkpoint; the remaining symbols/timeframes can continue.
            checksum = file_checksum(path) if path.is_file() else "missing"
            tasks.append(LocalTask(timeframe, path, symbol, source_ref, checksum))
    return tasks


def parse_task(task: LocalTask, *, batch_size: int = 50_000) -> ParsedBatch:
    """Parse one explicit local task without touching PostgreSQL."""
    import pyarrow.parquet as pq

    if not task.path.is_file():
        raw = {"path": str(task.path), "expected_symbol": task.symbol, "missing": True}
        return ParsedBatch(
            symbols={}, bars=[],
            quarantines=[_quarantine(task, 0, raw, "", task.symbol, "missing_source_file")],
            last_source_row=None,
        )

    required = REQUIRED_DAILY if task.timeframe == "1d" else REQUIRED_INTRADAY
    parquet = pq.ParquetFile(task.path)
    missing = [column for column in required if column not in parquet.schema_arrow.names]
    if missing:
        raise ValueError(f"missing required columns in {task.path}: {','.join(missing)}")
    symbols: dict[tuple[str, str], tuple] = {}
    bars: list[tuple] = []
    quarantines: list[QuarantineRecord] = []
    row_offset = 0
    if task.timeframe == "1d":
        # The combined daily parquet is materially larger than a representative
        # import.  Dataset filtering keeps this adapter sample-scoped when
        # row-group statistics permit predicate pushdown.
        import pyarrow.dataset as ds
        batches = ds.dataset(task.path, format="parquet").to_batches(
            filter=ds.field("ts_code") == task.symbol,
            columns=list(required), batch_size=batch_size,
        )
    else:
        batches = parquet.iter_batches(batch_size=batch_size, columns=list(required))
    for batch in batches:
        rows = batch.to_pydict()
        for index in range(batch.num_rows):
            source_row = row_offset + index
            raw = {column: rows[column][index] for column in required}
            raw_ts = str(raw["trade_date"] if task.timeframe == "1d" else raw["trade_time"])
            try:
                symbol = normalize_symbol(str(raw["ts_code"]))
                if symbol != task.symbol:
                    continue
                code, exchange = symbol.split(".", 1)
                open_value, high_value, low_value, close_value = (float(raw[key]) for key in ("open", "high", "low", "close"))
                amount = float(raw["amount"]) if raw["amount"] is not None else None
                volume = raw["vol"]
                if float(volume) < 0:
                    raise ValueError("negative_volume")
                bar_ts = _bar_timestamp(task.timeframe, raw)
            except (TypeError, ValueError, OverflowError) as exc:
                quarantines.append(_quarantine(task, source_row, raw, raw_ts, None, f"invalid_value:{exc}"))
                continue
            if task.timeframe == "30f" and exchange == "BJ":
                quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, "rejected_bj_30f_non_native_source"))
                continue
            if not valid_ohlc(open_value, high_value, low_value, close_value):
                quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, "invalid_ohlc"))
                continue
            if amount is not None and amount < 0:
                quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, "negative_amount"))
                continue
            # Daily source quotes amount in thousand-yuan, intraday in yuan.
            canonical_amount = None if amount is None else amount * (1000 if task.timeframe == "1d" else 1)
            # A halted/no-trade bar has zero volume and zero amount.  It has no
            # implied price, so the generic positive-volume decision function
            # correctly declines it; accept this separately only when both
            # turnover fields are exactly zero.  Any other zero-volume row is
            # forensic quarantine rather than a guessed unit conversion.
            if float(volume) == 0 and (canonical_amount is None or canonical_amount == 0):
                normalized_volume = 0
                decision = None
            elif float(volume) == 0:
                quarantines.append(_quarantine(task, source_row, raw, raw_ts, symbol, "zero_volume_with_positive_amount"))
                continue
            else:
                decision = decide_volume_multiplier(raw_volume=volume, amount=canonical_amount, low=low_value, high=high_value)
                normalized_volume = None if decision.volume_shares is None else int(decision.volume_shares)
            if decision is not None and (decision.action != "accept" or decision.volume_shares is None):
                payload = dict(raw)
                payload["volume_normalization"] = decision.provenance(raw_volume=volume)
                quarantines.append(_quarantine(task, source_row, payload, raw_ts, symbol, decision.reason or "ambiguous_volume_unit"))
                continue
            symbols[(code, exchange)] = (code, exchange, symbol, infer_asset_type(code), "A_SHARE", True)
            bars.append((
                code, exchange, timeframe_to_db_code(task.timeframe), bar_ts,
                price_to_x1000(open_value), price_to_x1000(high_value), price_to_x1000(low_value), price_to_x1000(close_value),
                normalized_volume, amount_to_x100(canonical_amount), True, 0, SOURCE_CODE,
            ))
        row_offset += batch.num_rows
    return ParsedBatch(symbols=symbols, bars=bars, quarantines=quarantines, last_source_row=row_offset - 1 if row_offset else None)


def _bar_timestamp(timeframe: str, row: dict[str, Any]) -> datetime:
    raw = row["trade_date"] if timeframe == "1d" else row["trade_time"]
    if not isinstance(raw, datetime):
        raw = datetime.fromisoformat(str(raw))
    return canonical_kline_timestamp(timeframe, raw.replace(tzinfo=SHANGHAI_TZ), date_only=timeframe == "1d")


def _quarantine(task: LocalTask, source_row: int, raw: dict[str, Any], raw_ts: str, symbol: str | None, reason: str) -> QuarantineRecord:
    return QuarantineRecord(SOURCE_NAME, task.source_ref, source_row, symbol, task.timeframe, raw_ts, reason[:500], raw)


async def run_once(args: argparse.Namespace) -> dict[str, Any]:
    timeframes = parse_csv(args.timeframes)
    validate_shard(shard_index=getattr(args, "shard_index", 0), shard_count=getattr(args, "shard_count", 1))
    requested_symbols = parse_csv(getattr(args, "symbols", None))
    active_only = bool(getattr(args, "active_only", False))
    if not requested_symbols and not active_only:
        raise ValueError("one of --symbols or --active-only is required")
    scope = "active_only" if active_only else "explicit_symbols"
    shard_index, shard_count = getattr(args, "shard_index", 0), getattr(args, "shard_count", 1)

    # Explicit dry-runs remain fully local.  Active-only needs one read-only
    # lookup from the already-initialised symbol master before it can plan.
    active_symbol_source: str | None = None
    if active_only:
        writer = NativeParquetWriter(args.database_url)
        await writer.open()
        try:
            database_symbols = await writer.fetch_active_symbols()
        finally:
            await writer.close()
        if database_symbols:
            requested_symbols = sorted(database_symbols)
            active_symbol_source = "database"
        else:
            requested_symbols = sorted(active_symbols_from_master(args.root))
            active_symbol_source = "master"
    symbols = static_shard(requested_symbols, shard_index=shard_index, shard_count=shard_count)
    tasks = discover_tasks(args.root, timeframes=timeframes, symbols=symbols, exclude_bj_30f=True)
    skipped_bj_30f = int("30f" in timeframes) * sum(symbol.endswith(".BJ") for symbol in symbols)
    summary = {
        "root": str(args.root), "timeframes": timeframes, "symbols": symbols,
        "selected_symbols": len(requested_symbols), "shard_symbols": len(symbols),
        "selection_scope": scope, "shard_index": shard_index, "shard_count": shard_count,
        "excluded_bj_30f_tasks": skipped_bj_30f, "tasks": len(tasks), "dry_run": args.dry_run,
    }
    if active_symbol_source is not None:
        summary["active_symbol_source"] = active_symbol_source
    if args.dry_run:
        summary["sources"] = [task.source_ref for task in tasks]
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return summary
    writer = NativeParquetWriter(args.database_url)
    await writer.open()
    try:
        # The emitted summary includes this ID. Pass it back with
        # --import-run-id to preserve the same checkpoint identity on retry.
        explicit_run_id = getattr(args, "import_run_id", None)
        run_id = (resolve_import_run_id(explicit_run_id) if explicit_run_id else
                  deterministic_import_run_id(root=args.root, timeframes=timeframes, scope=scope,
                                               shard_index=shard_index, shard_count=shard_count))
        await writer.create_import_run(import_run_id=run_id, parameters={**summary, "adapter": "local_parquet_import"})
        accepted = quarantined = 0
        for task in tasks:
            parsed = parse_task(task, batch_size=args.batch_size)
            accepted += await writer.upsert_import_batch(import_run_id=run_id, task=task, symbols=parsed.symbols.values(), bars=parsed.bars, quarantines=parsed.quarantines, last_source_row=parsed.last_source_row)
            quarantined += len(parsed.quarantines)
        summary.update(import_run_id=str(run_id), accepted_rows=accepted, quarantined_rows=quarantined)
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return summary
    finally:
        await writer.close()


def main() -> None:
    asyncio.run(run_once(parse_args()))


if __name__ == "__main__":
    main()
