from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from collector.market_fill import normalize_symbol, parse_csv, parse_timeframes
from collector.storage.postgres import PostgresKlineWriter
from collector.storage.tdx_csv_import_postgres import (
    PostgresTdxCsvImportTaskStore,
    TdxCsvArchiveTask,
)
from trading_protocol import Bar, SymbolInfo

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_ROOT = r"D:\BaiduNetdiskDownload\tdx数据"
DEFAULT_TIMEFRAMES = "5f"
FOLDER_TIMEFRAMES = {
    "五分钟K线数据": "5f",
    "十五分钟K线数据": "15f",
    "三十分钟K线数据": "30f",
    "六十分钟K线数据": "1h",
}
ENTRY_RE = re.compile(r"(?P<code>\d{6})_(?P<name>.+)_(?P<category>\d+)_.*\.csv$", re.IGNORECASE)
INDEX_NAME_KEYWORDS = (
    "\u6307\u6570",
    "\u4e0a\u8bc1",
    "\u6df1\u8bc1",
    "\u521b\u4e1a\u677f",
    "\u79d1\u521b",
    "\u6caa\u6df1",
    "\u4e2d\u8bc1",
    "\u56fd\u8bc1",
)
SH_INDEX_NAME_HINTS = ("\u4e0a\u8bc1", "\u6caa", "\u79d1\u521b")
SZ_INDEX_NAME_HINTS = ("\u6df1\u8bc1", "\u521b\u4e1a\u677f", "\u4e2d\u5c0f", "\u56fd\u8bc1")
FUND_PREFIXES = (
    "159",
    "160",
    "161",
    "162",
    "163",
    "164",
    "165",
    "166",
    "167",
    "168",
    "169",
    "510",
    "511",
    "512",
    "513",
    "515",
    "516",
    "517",
    "518",
    "588",
)
B_SHARE_PREFIXES = ("200", "900")
CANONICAL_HEADER_NAMES = {
    "code": {"code", "\u4ee3\u7801", "\u80a1\u7968\u4ee3\u7801"},
    "name": {"name", "\u540d\u79f0", "\u80a1\u7968\u540d\u79f0"},
    "fq": {"fq", "\u590d\u6743"},
    "tdate": {"tdate", "date", "datetime", "time", "\u65f6\u95f4", "\u65e5\u671f"},
    "open": {"open", "\u5f00", "\u5f00\u76d8", "\u5f00\u76d8\u4ef7"},
    "close": {"close", "\u6536", "\u6536\u76d8", "\u6536\u76d8\u4ef7"},
    "high": {"high", "\u9ad8", "\u6700\u9ad8", "\u6700\u9ad8\u4ef7"},
    "low": {"low", "\u4f4e", "\u6700\u4f4e", "\u6700\u4f4e\u4ef7"},
    "volume": {"cjl", "volume", "vol", "\u6210\u4ea4\u91cf"},
    "amount": {"cje", "amount", "\u6210\u4ea4\u989d"},
}


@dataclass(frozen=True)
class EntryMetadata:
    code: str
    name: str
    category: str
    symbol: str
    exchange: str
    asset_type: str = "stock"


@dataclass
class ParsedEntry:
    symbol: SymbolInfo
    bars: list[Bar]


@dataclass(frozen=True)
class CsvColumnMap:
    code: int
    tdate: int
    open: int
    close: int
    high: int
    low: int
    volume: int
    amount: int
    name: int | None = None
    fq: int | None = None


LEGACY_COLUMNS_WITH_FQ = CsvColumnMap(
    code=0,
    name=1,
    fq=3,
    tdate=4,
    open=5,
    close=6,
    high=7,
    low=8,
    volume=9,
    amount=10,
)


@dataclass
class PendingBatch:
    symbols: dict[str, SymbolInfo]
    bars: list[Bar]
    entries: int
    bars_read: int
    last_entry_index: int
    last_entry_name: str | None

    @classmethod
    def empty(cls) -> "PendingBatch":
        return cls(
            symbols={},
            bars=[],
            entries=0,
            bars_read=0,
            last_entry_index=-1,
            last_entry_name=None,
        )

    def clear(self) -> None:
        self.symbols.clear()
        self.bars.clear()
        self.entries = 0
        self.bars_read = 0
        self.last_entry_index = -1
        self.last_entry_name = None


class SymbolBudget:
    def __init__(self, symbol_limit: int) -> None:
        self.symbol_limit = max(0, symbol_limit)
        self.seen: set[str] = set()
        self._lock = asyncio.Lock()

    async def try_add(self, symbol: str) -> bool:
        async with self._lock:
            if self.symbol_limit <= 0 or symbol in self.seen:
                self.seen.add(symbol)
                return True
            if len(self.seen) >= self.symbol_limit:
                return False
            self.seen.add(symbol)
            return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import local zipped TDX CSV history into PostgreSQL")
    parser.add_argument("--root", default=os.getenv("TDX_CSV_ROOT", DEFAULT_ROOT))
    parser.add_argument("--timeframes", default=os.getenv("TDX_CSV_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument("--symbols", default=os.getenv("TDX_CSV_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("TDX_CSV_SYMBOL_LIMIT", "0")),
        help="Maximum unique symbols to import per process. Use 0 for no limit.",
    )
    parser.add_argument("--categories", default=os.getenv("TDX_CSV_CATEGORIES", "1"))
    parser.add_argument(
        "--asset-types",
        default=os.getenv("TDX_CSV_ASSET_TYPES", "stock"),
        help="Comma separated asset types to import. Default imports A-share stocks only.",
    )
    parser.add_argument(
        "--fq",
        default=os.getenv("TDX_CSV_FQ", "0"),
        help="Comma separated fq values to import. Default 0 keeps unadjusted bars.",
    )
    parser.add_argument("--task-limit", type=int, default=int(os.getenv("TDX_CSV_TASK_LIMIT", "1")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("TDX_CSV_CONCURRENCY", "1")))
    parser.add_argument("--entry-batch-size", type=int, default=int(os.getenv("TDX_CSV_ENTRY_BATCH_SIZE", "20")))
    parser.add_argument("--bar-batch-size", type=int, default=int(os.getenv("TDX_CSV_BAR_BATCH_SIZE", "20000")))
    parser.add_argument("--max-entries-per-task", type=int, default=int(os.getenv("TDX_CSV_MAX_ENTRIES_PER_TASK", "0")))
    parser.add_argument("--reset", action="store_true", default=os.getenv("TDX_CSV_RESET") == "1")
    parser.add_argument("--reset-running", action="store_true", default=os.getenv("TDX_CSV_RESET_RUNNING") == "1")
    parser.add_argument("--loop", action="store_true", default=os.getenv("TDX_CSV_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("TDX_CSV_LOOP_INTERVAL", "300")),
    )
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("TDX_CSV_DRY_RUN") == "1")
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    while True:
        await run_once(args)
        if not args.loop:
            return
        await asyncio.sleep(args.loop_interval)


async def run_once(args: argparse.Namespace) -> None:
    timeframes = parse_timeframes(args.timeframes)
    archives = discover_archives(Path(args.root), timeframes)
    symbols_filter = {normalize_symbol(symbol) for symbol in parse_csv(args.symbols)}
    categories = set(parse_csv(args.categories))
    asset_types = {item.strip().lower() for item in parse_csv(args.asset_types)}
    fq_values = {normalize_fq_value(item) for item in parse_csv(args.fq)}
    emit(
        "tdx_csv_import_started",
        root=str(Path(args.root)),
        archives=len(archives),
        timeframes=timeframes,
        symbols=len(symbols_filter),
        categories=sorted(categories),
        asset_types=sorted(asset_types),
        fq=sorted(fq_values),
        dry_run=args.dry_run,
    )
    if args.dry_run:
        for archive in archives[: min(len(archives), 20)]:
            emit(
                "tdx_csv_archive",
                zip_path=archive.zip_path,
                timeframe=archive.timeframe,
                zip_size=archive.zip_size,
            )
        emit("tdx_csv_import_finished", tasks=0, archives=len(archives), bars=0)
        return

    async with PostgresTdxCsvImportTaskStore(args.database_url) as task_store:
        ensured = await task_store.ensure_tasks(archives, reset=args.reset)
        reset_count = 0
        if args.reset_running:
            reset_count = await task_store.reset_running()
        tasks = await task_store.claim_tasks(limit=args.task_limit)
        emit(
            "tdx_csv_tasks_claimed",
            ensured=ensured,
            reset_running=reset_count,
            tasks=len(tasks),
            concurrency=max(1, args.concurrency),
        )
        async with PostgresKlineWriter(args.database_url) as writer:
            result = await process_tasks_concurrently(
                writer=writer,
                task_store=task_store,
                tasks=tasks,
                symbols_filter=symbols_filter,
                symbol_limit=args.symbol_limit,
                categories=categories,
                asset_types=asset_types,
                fq_values=fq_values,
                entry_batch_size=max(1, args.entry_batch_size),
                bar_batch_size=max(1, args.bar_batch_size),
                max_entries_per_task=args.max_entries_per_task,
                concurrency=max(1, args.concurrency),
            )
    emit(
        "tdx_csv_import_finished",
        tasks=len(tasks),
        archives=len(archives),
        bars=result["bars"],
    )


def discover_archives(root: Path, timeframes: list[str]) -> list[TdxCsvArchiveTask]:
    wanted = set(timeframes)
    archives: list[TdxCsvArchiveTask] = []
    if not root.exists():
        return archives
    for folder_name, timeframe in FOLDER_TIMEFRAMES.items():
        if timeframe not in wanted:
            continue
        folder = root / folder_name
        if not folder.exists():
            continue
        for zip_path in sorted(folder.glob("*.zip")):
            stat = zip_path.stat()
            archives.append(
                TdxCsvArchiveTask(
                    zip_path=str(zip_path.resolve()),
                    timeframe=timeframe,
                    zip_size=stat.st_size,
                    zip_mtime=datetime.fromtimestamp(stat.st_mtime, tz=SHANGHAI_TZ),
                )
            )
    return archives


async def process_tasks_concurrently(
    *,
    writer: PostgresKlineWriter,
    task_store: PostgresTdxCsvImportTaskStore,
    tasks: list[dict[str, Any]],
    symbols_filter: set[str],
    symbol_limit: int,
    categories: set[str],
    asset_types: set[str],
    fq_values: set[str],
    entry_batch_size: int,
    bar_batch_size: int,
    max_entries_per_task: int,
    concurrency: int,
) -> dict[str, int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    symbol_budget = SymbolBudget(symbol_limit=symbol_limit)

    async def run_task(task: dict[str, Any]) -> dict[str, int]:
        async with semaphore:
            return await process_task(
                writer=writer,
                task_store=task_store,
                task=task,
                symbols_filter=symbols_filter,
                symbol_limit=symbol_limit,
                categories=categories,
                asset_types=asset_types,
                fq_values=fq_values,
                entry_batch_size=entry_batch_size,
                bar_batch_size=bar_batch_size,
                max_entries_per_task=max_entries_per_task,
                symbol_budget=symbol_budget,
            )

    results = await asyncio.gather(*(run_task(task) for task in tasks))
    return {"bars": sum(item["bars"] for item in results)}


async def process_task(
    *,
    writer: PostgresKlineWriter,
    task_store: PostgresTdxCsvImportTaskStore,
    task: dict[str, Any],
    symbols_filter: set[str],
    symbol_limit: int,
    categories: set[str],
    asset_types: set[str],
    fq_values: set[str],
    entry_batch_size: int,
    bar_batch_size: int,
    max_entries_per_task: int,
    symbol_budget: SymbolBudget,
) -> dict[str, int]:
    task_id = int(task["id"])
    zip_path = str(task["zip_path"])
    timeframe = _db_timeframe_to_code(int(task["timeframe"]))
    start_index = int(task["last_entry_index"]) + 1
    batch = PendingBatch.empty()
    total_bars = 0
    entries_processed = 0
    try:
        with zipfile.ZipFile(zip_path) as archive:
            entries = [
                entry
                for entry in archive.infolist()
                if not entry.is_dir() and entry.filename.lower().endswith(".csv")
            ]
            entries_total = len(entries)
            for entry_index, entry in enumerate(entries[start_index:], start=start_index):
                if max_entries_per_task > 0 and entries_processed >= max_entries_per_task:
                    break
                parsed = parse_entry_metadata(entry.filename)
                if parsed is None or parsed.category not in categories:
                    batch.entries += 1
                    batch.last_entry_index = entry_index
                    batch.last_entry_name = entry.filename
                    entries_processed += 1
                    if batch.entries >= entry_batch_size:
                        total_bars += await flush_batch(writer, task_store, task_id, entries_total, batch)
                    continue
                entry_data = parse_csv_entry(
                    archive,
                    entry,
                    parsed,
                    timeframe,
                    symbols_filter=symbols_filter,
                    asset_types=asset_types,
                    fq_values=fq_values,
                )
                if entry_data is not None:
                    if not await symbol_budget.try_add(entry_data.symbol.symbol):
                        break
                    batch.symbols[entry_data.symbol.symbol] = entry_data.symbol
                    batch.bars.extend(entry_data.bars)
                    batch.bars_read += len(entry_data.bars)
                batch.entries += 1
                batch.last_entry_index = entry_index
                batch.last_entry_name = entry.filename
                entries_processed += 1
                if batch.entries >= entry_batch_size or len(batch.bars) >= bar_batch_size:
                    total_bars += await flush_batch(writer, task_store, task_id, entries_total, batch)
            if batch.entries:
                total_bars += await flush_batch(writer, task_store, task_id, entries_total, batch)
            if start_index + entries_processed >= entries_total:
                await task_store.record_success(task_id=task_id, entries_total=entries_total)
            else:
                await task_store.record_paused(task_id=task_id, entries_total=entries_total)
        emit(
            "tdx_csv_task_finished",
            zip_path=zip_path,
            timeframe=timeframe,
            entries=entries_processed,
            bars=total_bars,
        )
        return {"bars": total_bars}
    except Exception as exc:
        await task_store.record_failure(task_id=task_id, error=str(exc))
        emit(
            "tdx_csv_task_failed",
            zip_path=zip_path,
            timeframe=timeframe,
            error=str(exc)[:500],
        )
        return {"bars": total_bars}


async def flush_batch(
    writer: PostgresKlineWriter,
    task_store: PostgresTdxCsvImportTaskStore,
    task_id: int,
    entries_total: int,
    batch: PendingBatch,
) -> int:
    if not batch.entries:
        return 0
    bars_written = 0
    if batch.symbols:
        await writer.upsert_symbols(batch.symbols.values())
    if batch.bars:
        bars_written = await writer.upsert_bars(batch.bars)
    await task_store.record_progress(
        task_id=task_id,
        entries_total=entries_total,
        entries_done_delta=batch.entries,
        last_entry_index=batch.last_entry_index,
        last_entry_name=batch.last_entry_name,
        bars_read_delta=batch.bars_read,
        bars_written_delta=bars_written,
        symbols_seen_delta=len(batch.symbols),
    )
    batch.clear()
    return bars_written


def parse_entry_metadata(entry_name: str) -> EntryMetadata | None:
    base_name = Path(entry_name).name
    match = ENTRY_RE.match(base_name)
    if match is None:
        return None
    code = match.group("code")
    name = match.group("name")
    exchange, asset_type = infer_identity(code, name)
    if exchange is None:
        return None
    return EntryMetadata(
        code=code,
        name=name,
        category=match.group("category"),
        symbol=f"{code}.{exchange}",
        exchange=exchange,
        asset_type=asset_type,
    )


def infer_identity(code: str, name: str) -> tuple[str | None, str]:
    if is_index_name(name):
        exchange = infer_index_exchange(code, name) or infer_exchange(code)
        return exchange, "index"
    if code.startswith(B_SHARE_PREFIXES):
        return infer_exchange(code), "b_share"
    if code.startswith(FUND_PREFIXES):
        return infer_exchange(code), "fund"
    return infer_exchange(code), "stock"


def is_index_name(name: str) -> bool:
    return any(keyword in name for keyword in INDEX_NAME_KEYWORDS)


def infer_index_exchange(code: str, name: str) -> str | None:
    if code.startswith(("399", "395")) or any(hint in name for hint in SZ_INDEX_NAME_HINTS):
        return "SZ"
    if any(hint in name for hint in SH_INDEX_NAME_HINTS):
        return "SH"
    return None


def normalize_fq_value(value: str) -> str:
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return value.strip()


def infer_exchange(code: str) -> str | None:
    if code.startswith(("600", "601", "603", "605", "688", "900")):
        return "SH"
    if code.startswith(("000", "001", "002", "003", "200", "300", "301")):
        return "SZ"
    if code.startswith(("4", "8")):
        return "BJ"
    return None


def normalize_header_name(value: str) -> str:
    return value.strip().lstrip("\ufeff").lower()


def parse_header_columns(row: list[str]) -> CsvColumnMap | None:
    normalized = [normalize_header_name(value) for value in row]
    if "code" not in normalized and "\u4ee3\u7801" not in normalized:
        return None

    resolved: dict[str, int] = {}
    for index, name in enumerate(normalized):
        for canonical, aliases in CANONICAL_HEADER_NAMES.items():
            if name in aliases and canonical not in resolved:
                resolved[canonical] = index

    required = ("code", "tdate", "open", "close", "high", "low", "volume", "amount")
    missing = [name for name in required if name not in resolved]
    if missing:
        raise ValueError(
            "Unsupported TDX CSV header, missing "
            f"{','.join(missing)}: {','.join(row)}"
        )

    return CsvColumnMap(
        code=resolved["code"],
        name=resolved.get("name"),
        fq=resolved.get("fq"),
        tdate=resolved["tdate"],
        open=resolved["open"],
        close=resolved["close"],
        high=resolved["high"],
        low=resolved["low"],
        volume=resolved["volume"],
        amount=resolved["amount"],
    )


def is_non_data_row(row: list[str], columns: CsvColumnMap) -> bool:
    return len(row) <= max_required_index(columns) or not row[columns.code].strip().isdigit()


def max_required_index(columns: CsvColumnMap) -> int:
    return max(
        columns.code,
        columns.tdate,
        columns.open,
        columns.close,
        columns.high,
        columns.low,
        columns.volume,
        columns.amount,
    )


def get_cell(row: list[str], index: int | None, default: str = "") -> str:
    if index is None or index >= len(row):
        return default
    return row[index].strip()


def parse_csv_entry(
    archive: zipfile.ZipFile,
    entry: zipfile.ZipInfo,
    metadata: EntryMetadata,
    timeframe: str,
    *,
    symbols_filter: set[str] | None = None,
    asset_types: set[str] | None = None,
    fq_values: set[str] | None = None,
) -> ParsedEntry | None:
    with archive.open(entry) as raw:
        wrapper = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.reader(wrapper)
        bars: list[Bar] = []
        resolved = metadata
        columns: CsvColumnMap | None = None
        for row in reader:
            if not row:
                continue
            if columns is None:
                header_columns = parse_header_columns(row)
                if header_columns is not None:
                    columns = header_columns
                    continue
                columns = LEGACY_COLUMNS_WITH_FQ
            elif parse_header_columns(row) is not None:
                continue
            if is_non_data_row(row, columns):
                continue
            resolved = resolve_metadata_from_row(metadata, row, columns)
            if symbols_filter and resolved.symbol not in symbols_filter:
                return None
            if asset_types and resolved.asset_type not in asset_types:
                return None
            bar = parse_csv_row(
                row,
                resolved.symbol,
                timeframe,
                columns=columns,
                expected_code=resolved.code,
                fq_values=fq_values,
            )
            if bar is not None:
                bars.append(bar)
    if not bars:
        return None
    return ParsedEntry(
        symbol=SymbolInfo(
            symbol=resolved.symbol,
            code=resolved.code,
            exchange=resolved.exchange,
            name=resolved.name,
            asset_type=resolved.asset_type,
            market="A_SHARE_INDEX" if resolved.asset_type == "index" else "A_SHARE",
        ),
        bars=bars,
    )


def resolve_metadata_from_row(
    metadata: EntryMetadata,
    row: list[str],
    columns: CsvColumnMap = LEGACY_COLUMNS_WITH_FQ,
) -> EntryMetadata:
    code = get_cell(row, columns.code) or metadata.code
    name = get_cell(row, columns.name) or metadata.name
    exchange, asset_type = infer_identity(code, name)
    if exchange is None:
        exchange = metadata.exchange
    return EntryMetadata(
        code=code,
        name=name,
        category=metadata.category,
        symbol=f"{code}.{exchange}",
        exchange=exchange,
        asset_type=asset_type,
    )


def parse_csv_row(
    row: list[str],
    symbol: str,
    timeframe: str,
    *,
    columns: CsvColumnMap = LEGACY_COLUMNS_WITH_FQ,
    expected_code: str | None = None,
    fq_values: set[str] | None = None,
) -> Bar | None:
    if is_non_data_row(row, columns):
        return None
    code = get_cell(row, columns.code)
    if expected_code is not None and code != expected_code:
        return None
    fq_value = get_cell(row, columns.fq, "0")
    if fq_values and normalize_fq_value(fq_value) not in fq_values:
        return None
    try:
        ts = datetime.strptime(
            get_cell(row, columns.tdate),
            "%Y-%m-%d %H:%M:%S",
        ).replace(tzinfo=SHANGHAI_TZ)
        volume = int(round(float(get_cell(row, columns.volume)) * 100))
        amount_cell = get_cell(row, columns.amount)
        amount = float(amount_cell) if amount_cell else None
        return Bar(
            symbol=symbol,
            timeframe=timeframe,
            ts=ts,
            open=float(get_cell(row, columns.open)),
            high=float(get_cell(row, columns.high)),
            low=float(get_cell(row, columns.low)),
            close=float(get_cell(row, columns.close)),
            volume=volume,
            amount=amount,
            complete=True,
            revision=0,
            source="tdx_csv",
        )
    except (ValueError, TypeError):
        return None


def _db_timeframe_to_code(value: int) -> str:
    mapping = {
        5: "5f",
        15: "15f",
        30: "30f",
        60: "1h",
    }
    if value not in mapping:
        raise ValueError(f"Unsupported TDX CSV timeframe code: {value}")
    return mapping[value]


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
