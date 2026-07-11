from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from collector.storage.postgres import (
    amount_to_x100,
    price_to_x1000,
    source_priority_case,
    source_to_code,
    timeframe_to_db_code,
)
from collector.kline_import_quarantine import (
    ImportCheckpoint,
    QuarantineRecord,
    commit_import_batch,
    create_import_run,
)
from trading_protocol import canonical_kline_timestamp

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_ROOT = "D:\\\u5386\u53f2\u6570\u636e"
DEFAULT_TIMEFRAMES = "30f,1d"
SOURCE_NAME = "parquet_native"
SOURCE_CODE = source_to_code(SOURCE_NAME)

TIMEFRAME_PATHS = {
    "30f": ("30m_price", "trade_time"),
    "1d": ("\u65e5\u7ebf\u6570\u636e/1d_price", "date"),
}
REQUIRED_COLUMNS = {
    "30f": ("code", "trade_time", "open", "high", "low", "close", "vol", "amount"),
    "1d": ("code", "date", "open", "high", "low", "close", "vol", "amount"),
}


@dataclass(frozen=True)
class ImportTask:
    timeframe: str
    zip_path: Path
    member_path: str
    source_ref: str
    source_checksum: str


@dataclass
class ParsedBatch:
    symbols: dict[tuple[str, str], tuple]
    bars: list[tuple]
    quarantines: list[QuarantineRecord]
    last_source_row: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import native 30f/1d zipped parquet history into klines")
    parser.add_argument("--root", default=os.getenv("NATIVE_PARQUET_ROOT", DEFAULT_ROOT))
    parser.add_argument("--timeframes", default=os.getenv("NATIVE_PARQUET_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument("--years", default=os.getenv("NATIVE_PARQUET_YEARS"))
    parser.add_argument("--symbols", default=os.getenv("NATIVE_PARQUET_SYMBOLS"))
    parser.add_argument("--task-limit", type=int, default=int(os.getenv("NATIVE_PARQUET_TASK_LIMIT", "0")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("NATIVE_PARQUET_CONCURRENCY", "2")))
    parser.add_argument("--write-concurrency", type=int, default=int(os.getenv("NATIVE_PARQUET_WRITE_CONCURRENCY", "1")))
    parser.add_argument(
        "--progress-every",
        type=int,
        default=int(os.getenv("NATIVE_PARQUET_PROGRESS_EVERY", "100")),
        help="Emit aggregate progress every N completed parquet members. Use 0 to emit every member.",
    )
    parser.add_argument("--db-pool-min-size", type=int, default=int(os.getenv("NATIVE_PARQUET_DB_POOL_MIN_SIZE", "1")))
    parser.add_argument("--db-pool-max-size", type=int, default=int(os.getenv("NATIVE_PARQUET_DB_POOL_MAX_SIZE", "2")))
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        default=os.getenv("NATIVE_PARQUET_INCLUDE_INACTIVE") == "1",
        help="Import delisted/inactive symbols from stock_basic.parquet. Disabled by default.",
    )
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("NATIVE_PARQUET_DRY_RUN") == "1")
    parser.add_argument(
        "--import-run-id",
        default=os.getenv("NATIVE_PARQUET_IMPORT_RUN_ID"),
        help="UUID to resume the same import run after interruption.",
    )
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
    await run_once(args)


async def run_once(args: argparse.Namespace) -> None:
    root = Path(args.root)
    timeframes = parse_csv(args.timeframes)
    years = set(parse_csv(args.years))
    symbols_filter = {normalize_symbol(item) for item in parse_csv(args.symbols)}
    tasks = discover_tasks(root, timeframes=timeframes, years=years)
    if args.task_limit > 0:
        tasks = tasks[: args.task_limit]
    emit(
        "native_parquet_import_started",
        root=str(root),
        timeframes=timeframes,
        years=sorted(years),
        symbols=len(symbols_filter),
        tasks=len(tasks),
        dry_run=args.dry_run,
    )
    if args.dry_run:
        for task in tasks[:20]:
            emit("native_parquet_task", timeframe=task.timeframe, zip_path=str(task.zip_path), member=task.member_path)
        return

    writer = NativeParquetWriter(
        args.database_url,
        pool_min_size=max(1, args.db_pool_min_size),
        pool_max_size=max(max(1, args.db_pool_min_size), args.db_pool_max_size),
    )
    await writer.open()
    try:
        import_run_id = resolve_import_run_id(getattr(args, "import_run_id", None))
        await writer.create_import_run(
            import_run_id=import_run_id,
            parameters={"root": str(root), "timeframes": timeframes, "years": sorted(years)},
        )
        symbol_meta = load_symbol_meta(root)
        allowed_symbols = set()
        if not args.include_inactive:
            db_active_symbols = await writer.fetch_active_symbols()
            package_active_symbols = {
                symbol
                for symbol, meta in symbol_meta.items()
                if bool(meta.get("is_active", True))
            }
            allowed_symbols = db_active_symbols or package_active_symbols
            if allowed_symbols:
                symbol_meta = {
                    symbol: meta
                    for symbol, meta in symbol_meta.items()
                    if symbol in allowed_symbols
                }
            emit(
                "native_parquet_active_filter",
                source="database" if db_active_symbols else "stock_basic",
                allowed_symbols=len(allowed_symbols),
                package_active_symbols=len(package_active_symbols),
            )
        synced_symbols = await writer.upsert_symbol_rows(
            symbol_rows_from_meta(symbol_meta, symbols_filter=symbols_filter)
        )
        emit("native_parquet_symbols_synced", symbols=synced_symbols)
        result = await process_tasks(
            writer=writer,
            tasks=tasks,
            symbol_meta=symbol_meta,
            symbols_filter=symbols_filter,
            allowed_symbols=allowed_symbols,
            concurrency=max(1, args.concurrency),
            write_concurrency=max(1, args.write_concurrency),
            progress_every=max(0, args.progress_every),
            sync_symbols_per_task=not bool(symbol_meta),
            import_run_id=import_run_id,
        )
        watermarks = await writer.fetch_watermarks(timeframes=timeframes, symbols=symbols_filter)
    finally:
        await writer.close()

    emit(
        "native_parquet_import_finished",
        tasks=len(tasks),
        bars=result["bars"],
        symbols=result["symbols"],
        watermarks=watermarks[:30],
    )


def discover_tasks(root: Path, *, timeframes: list[str], years: set[str]) -> list[ImportTask]:
    tasks: list[ImportTask] = []
    for timeframe in timeframes:
        if timeframe not in TIMEFRAME_PATHS:
            raise ValueError(f"Unsupported native parquet timeframe: {timeframe}")
        folder, _ = TIMEFRAME_PATHS[timeframe]
        for zip_path in sorted((root / folder).glob("*.zip")):
            if years and zip_path.stem not in years:
                continue
            with zipfile.ZipFile(zip_path) as archive:
                for item in sorted(archive.infolist(), key=lambda info: info.filename):
                    if not item.is_dir() and item.filename.lower().endswith(".parquet"):
                        relative_zip = zip_path.relative_to(root).as_posix()
                        checksum = f"crc32={item.CRC:08x};size={item.file_size}"
                        tasks.append(
                            ImportTask(
                                timeframe=timeframe,
                                zip_path=zip_path,
                                member_path=item.filename,
                                source_ref=f"{relative_zip}!{item.filename}#{checksum}",
                                source_checksum=checksum,
                            )
                        )
    return tasks


async def process_tasks(
    *,
    writer: "NativeParquetWriter",
    tasks: list[ImportTask],
    symbol_meta: dict[str, dict[str, Any]],
    symbols_filter: set[str],
    allowed_symbols: set[str],
    concurrency: int,
    write_concurrency: int,
    progress_every: int,
    sync_symbols_per_task: bool,
    import_run_id: UUID | None = None,
) -> dict[str, int]:
    read_sem = asyncio.Semaphore(concurrency)
    write_sem = asyncio.Semaphore(write_concurrency)
    progress_lock = asyncio.Lock()
    progress = {"tasks": 0, "bars": 0, "symbols": 0}

    async def run_task(task: ImportTask) -> dict[str, int]:
        async with read_sem:
            parsed = await asyncio.to_thread(
                parse_task,
                task,
                symbol_meta,
                symbols_filter,
                allowed_symbols,
            )
        async with write_sem:
            written = await writer.upsert_import_batch(
                import_run_id=import_run_id,
                task=task,
                symbols=parsed.symbols.values() if sync_symbols_per_task else (),
                bars=parsed.bars,
                quarantines=parsed.quarantines,
                last_source_row=parsed.last_source_row,
            )
        await record_progress(
            task=task,
            bars=written,
            symbols=len(parsed.symbols),
            skipped=not parsed.bars and not parsed.quarantines,
        )
        return {"bars": written, "symbols": len(parsed.symbols)}

    async def record_progress(*, task: ImportTask, bars: int, symbols: int, skipped: bool) -> None:
        async with progress_lock:
            progress["tasks"] += 1
            progress["bars"] += bars
            progress["symbols"] += symbols
            should_emit = progress_every == 0 or progress["tasks"] % progress_every == 0
            if should_emit:
                emit(
                    "native_parquet_progress",
                    completed=progress["tasks"],
                    total=len(tasks),
                    bars=progress["bars"],
                    symbols=progress["symbols"],
                    last_timeframe=task.timeframe,
                    last_member=task.member_path,
                    last_skipped=skipped,
                )

    totals = {"bars": 0, "symbols": 0}
    task_queue: asyncio.Queue[ImportTask] = asyncio.Queue()
    for task in tasks:
        task_queue.put_nowait(task)

    async def worker_loop() -> None:
        while True:
            try:
                task = task_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                item = await run_task(task)
                async with progress_lock:
                    totals["bars"] += item["bars"]
                    totals["symbols"] += item["symbols"]
            finally:
                task_queue.task_done()
                release_memory()

    workers = [asyncio.create_task(worker_loop()) for _ in range(max(1, concurrency))]
    await asyncio.gather(*workers)
    return totals


def parse_task(
    task: ImportTask,
    symbol_meta: dict[str, dict[str, Any]],
    symbols_filter: set[str],
    allowed_symbols: set[str],
) -> ParsedBatch:
    import pyarrow.parquet as pq

    with zipfile.ZipFile(task.zip_path) as archive:
        with archive.open(task.member_path) as member:
            table = pq.read_table(member, columns=list(REQUIRED_COLUMNS[task.timeframe]), use_threads=False)
    rows = table.to_pydict()
    missing = [column for column in REQUIRED_COLUMNS[task.timeframe] if column not in rows]
    if missing:
        raise ValueError(f"Missing required columns in {task.member_path}: {','.join(missing)}")

    symbols: dict[tuple[str, str], tuple] = {}
    bars: list[tuple] = []
    quarantines: list[QuarantineRecord] = []
    row_count = len(rows["code"])
    timeframe_code = timeframe_to_db_code(task.timeframe)
    for index in range(row_count):
        raw_payload = {column: rows[column][index] for column in REQUIRED_COLUMNS[task.timeframe]}
        raw_ts = str(rows[TIMEFRAME_PATHS[task.timeframe][1]][index])
        try:
            symbol = normalize_symbol(str(rows["code"][index]))
        except (TypeError, ValueError) as exc:
            quarantines.append(
                quarantine_from_row(task, index, raw_payload, raw_ts, None, f"invalid_symbol:{exc}")
            )
            continue
        if symbols_filter and symbol not in symbols_filter:
            continue
        if allowed_symbols and symbol not in allowed_symbols:
            continue
        code, exchange = symbol.split(".", 1)
        try:
            open_value = to_float(rows["open"][index])
            high_value = to_float(rows["high"][index])
            low_value = to_float(rows["low"][index])
            close_value = to_float(rows["close"][index])
            volume = int(round(to_float(rows["vol"][index], default=0.0)))
            bar_ts = parse_bar_ts(task.timeframe, rows, index)
        except (TypeError, ValueError, OverflowError) as exc:
            quarantines.append(quarantine_from_row(task, index, raw_payload, raw_ts, symbol, f"invalid_value:{exc}"))
            continue
        if not valid_ohlc(open_value, high_value, low_value, close_value):
            quarantines.append(quarantine_from_row(task, index, raw_payload, raw_ts, symbol, "invalid_ohlc"))
            continue
        if volume < 0:
            quarantines.append(quarantine_from_row(task, index, raw_payload, raw_ts, symbol, "negative_volume"))
            continue
        meta = symbol_meta.get(symbol, {})
        symbols[(code, exchange)] = (
            code,
            exchange,
            str(meta.get("name") or symbol),
            str(meta.get("asset_type") or infer_asset_type(code)),
            "A_SHARE",
            bool(meta.get("is_active", True)),
        )
        bars.append(
            (
                code,
                exchange,
                timeframe_code,
                bar_ts,
                price_to_x1000(open_value),
                price_to_x1000(high_value),
                price_to_x1000(low_value),
                price_to_x1000(close_value),
                volume,
                amount_to_x100(to_optional_float(rows["amount"][index])),
                True,
                0,
                SOURCE_CODE,
            )
        )
    return ParsedBatch(
        symbols=symbols,
        bars=bars,
        quarantines=quarantines,
        last_source_row=row_count - 1 if row_count else None,
    )


def valid_ohlc(open_value: float, high_value: float, low_value: float, close_value: float) -> bool:
    return (
        min(open_value, high_value, low_value, close_value) > 0
        and low_value <= min(open_value, close_value)
        and high_value >= max(open_value, close_value)
        and high_value >= low_value
    )


def quarantine_from_row(
    task: ImportTask,
    source_row: int,
    raw_payload: dict[str, Any],
    raw_ts: str,
    symbol: str | None,
    reason: str,
) -> QuarantineRecord:
    return QuarantineRecord(
        source_name=SOURCE_NAME,
        source_ref=task.source_ref,
        source_row=source_row,
        symbol_text=symbol,
        timeframe=task.timeframe,
        raw_ts=raw_ts,
        reason=reason[:500],
        raw_payload=raw_payload,
    )


def release_memory() -> None:
    gc.collect()
    try:
        import pyarrow as pa

        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def parse_bar_ts(timeframe: str, rows: dict[str, list[Any]], index: int) -> datetime:
    if timeframe == "30f":
        return canonical_kline_timestamp(
            timeframe,
            datetime.strptime(str(rows["trade_time"][index]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=SHANGHAI_TZ),
        )
    if timeframe == "1d":
        return canonical_kline_timestamp(
            timeframe,
            datetime.strptime(str(rows["date"][index]), "%Y%m%d").replace(tzinfo=SHANGHAI_TZ),
            date_only=True,
        )
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def load_symbol_meta(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "\u65e5\u7ebf\u6570\u636e" / "stock_basic.parquet"
    if not path.exists():
        return {}
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["code", "name", "list_status"])
    data = table.to_pydict()
    result: dict[str, dict[str, Any]] = {}
    for code, name, status in zip(data["code"], data["name"], data["list_status"], strict=False):
        symbol = normalize_symbol(str(code))
        result[symbol] = {
            "name": name or symbol,
            "asset_type": infer_asset_type(symbol.split(".", 1)[0]),
            "is_active": status == "L",
        }
    return result


def symbol_rows_from_meta(
    symbol_meta: dict[str, dict[str, Any]],
    *,
    symbols_filter: set[str],
) -> list[tuple]:
    rows = []
    for symbol, meta in symbol_meta.items():
        if symbols_filter and symbol not in symbols_filter:
            continue
        code, exchange = symbol.split(".", 1)
        rows.append(
            (
                code,
                exchange,
                str(meta.get("name") or symbol),
                str(meta.get("asset_type") or infer_asset_type(code)),
                "A_SHARE",
                bool(meta.get("is_active", True)),
            )
        )
    return rows


class NativeParquetWriter:
    def __init__(
        self,
        database_url: str,
        *,
        pool_min_size: int = 1,
        pool_max_size: int = 2,
    ) -> None:
        self.database_url = database_url
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.pool = None

    async def open(self) -> None:
        import asyncpg

        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
        )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()

    async def create_import_run(self, *, import_run_id: UUID, parameters: dict[str, Any]) -> None:
        """Register the run before any member can advance a durable checkpoint."""
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await create_import_run(
                conn,
                import_run_id=import_run_id,
                source_name=SOURCE_NAME,
                parameters=parameters,
            )

    async def upsert(self, *, symbols: Iterable[tuple], bars: list[tuple]) -> int:
        if not bars:
            return 0
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_on_connection(conn, symbols=symbols, bars=bars)
        return len(bars)

    async def upsert_import_batch(
        self,
        *,
        import_run_id: UUID | None,
        task: ImportTask,
        symbols: Iterable[tuple],
        bars: list[tuple],
        quarantines: list[QuarantineRecord],
        last_source_row: int | None,
    ) -> int:
        """Commit a source member's canonical rows and raw failures together.

        ``import_run_id`` is optional only to preserve test callers of
        ``process_tasks``. Production ``run_once`` always supplies it.
        """
        if import_run_id is None:
            return await self.upsert(symbols=symbols, bars=bars)
        assert self.pool is not None
        symbol_rows = list(symbols)
        async with self.pool.acquire() as conn:
            return await commit_import_batch(
                conn,
                import_run_id=import_run_id,
                checkpoint=ImportCheckpoint(
                    source_ref=task.source_ref,
                    source_checksum=task.source_checksum,
                    last_source_row=last_source_row,
                ),
                quarantines=quarantines,
                write_accepted=lambda transaction_conn: self._upsert_on_connection(
                    transaction_conn,
                    symbols=symbol_rows,
                    bars=bars,
                ),
            )

    async def _upsert_on_connection(self, conn, *, symbols: Iterable[tuple], bars: list[tuple]) -> int:
        if not bars:
            return 0
        await create_symbol_stage(conn)
        symbol_rows = list(symbols)
        if symbol_rows:
            await conn.copy_records_to_table(
                "_native_parquet_symbol_stage",
                records=symbol_rows,
                columns=["code", "exchange", "name", "asset_type", "market", "is_active"],
            )
            await upsert_symbols(conn)

        await create_kline_stage(conn)
        await conn.copy_records_to_table(
            "_native_parquet_kline_stage",
            records=bars,
            columns=[
                "code",
                "exchange",
                "timeframe",
                "bar_end",
                "open_x1000",
                "high_x1000",
                "low_x1000",
                "close_x1000",
                "volume",
                "amount_x100",
                "is_complete",
                "revision",
                "source",
            ],
        )
        await register_source_coverage(conn)
        await upsert_klines(conn)
        await upsert_watermarks(conn)
        return len(bars)

    async def upsert_symbol_rows(self, rows: Iterable[tuple]) -> int:
        symbol_rows = list(rows)
        if not symbol_rows:
            return 0
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await create_symbol_stage(conn)
                await conn.copy_records_to_table(
                    "_native_parquet_symbol_stage",
                    records=symbol_rows,
                    columns=["code", "exchange", "name", "asset_type", "market", "is_active"],
                )
                await upsert_symbols(conn)
        return len(symbol_rows)

    async def fetch_watermarks(self, *, timeframes: list[str], symbols: set[str]) -> list[dict[str, Any]]:
        assert self.pool is not None
        timeframe_codes = [timeframe_to_db_code(item) for item in timeframes]
        async with self.pool.acquire() as conn:
            if symbols:
                rows = await conn.fetch(
                    """
                    select s.code || '.' || s.exchange as symbol,
                           w.timeframe,
                           w.last_bar_end,
                           w.source,
                           w.updated_at
                    from scheme2_ingest_watermarks w
                    join symbols s on s.id = w.symbol_id
                    where w.timeframe = any($1::int[])
                      and s.code || '.' || s.exchange = any($2::text[])
                    order by symbol, w.timeframe
                    """,
                    timeframe_codes,
                    sorted(symbols),
                )
            else:
                rows = await conn.fetch(
                    """
                    select s.code || '.' || s.exchange as symbol,
                           w.timeframe,
                           w.last_bar_end,
                           w.source,
                           w.updated_at
                    from scheme2_ingest_watermarks w
                    join symbols s on s.id = w.symbol_id
                    where w.timeframe = any($1::int[])
                    order by w.last_bar_end desc
                    limit 30
                    """,
                    timeframe_codes,
                )
        return [
            {
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "last_bar_end": row["last_bar_end"].isoformat() if row["last_bar_end"] else None,
                "source": row["source"],
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            }
            for row in rows
        ]

    async def fetch_active_symbols(self) -> set[str]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select code || '.' || exchange as symbol
                from symbols
                where is_active is true
                """
            )
        return {str(row["symbol"]) for row in rows}


async def create_symbol_stage(conn) -> None:
    await conn.execute(
        """
        create temp table if not exists _native_parquet_symbol_stage (
            code text not null,
            exchange text not null,
            name text not null,
            asset_type text not null,
            market text not null,
            is_active boolean not null
        ) on commit drop
        """
    )


async def upsert_symbols(conn) -> None:
    await conn.execute(
        """
        insert into symbols (code, exchange, name, asset_type, market, is_active)
        select distinct on (exchange, code)
            code, exchange, name, asset_type, market, is_active
        from _native_parquet_symbol_stage
        order by exchange, code
        on conflict (exchange, code) do update
        set name = case
                when excluded.name in (excluded.code, excluded.code || '.' || excluded.exchange)
                    then symbols.name
                else excluded.name
            end,
            asset_type = excluded.asset_type,
            market = excluded.market,
            is_active = excluded.is_active,
            updated_at = now()
        """
    )


async def create_kline_stage(conn) -> None:
    await conn.execute(
        """
        create temp table if not exists _native_parquet_kline_stage (
            code text not null,
            exchange text not null,
            timeframe integer not null,
            bar_end timestamptz not null,
            open_x1000 integer not null,
            high_x1000 integer not null,
            low_x1000 integer not null,
            close_x1000 integer not null,
            volume bigint not null,
            amount_x100 bigint,
            is_complete boolean not null,
            revision integer not null,
            source smallint not null
        ) on commit drop
        """
    )


async def upsert_klines(conn) -> None:
    await conn.execute(
        f"""
        with staged_rows as (
            select distinct on (symbols.id, stage.timeframe, stage.bar_end)
                symbols.id as symbol_id,
                stage.timeframe,
                stage.bar_end,
                stage.open_x1000,
                stage.high_x1000,
                stage.low_x1000,
                stage.close_x1000,
                stage.volume,
                stage.amount_x100,
                stage.is_complete,
                stage.revision,
                stage.source
            from _native_parquet_kline_stage stage
            join symbols
              on symbols.code = stage.code
             and symbols.exchange = stage.exchange
            order by symbols.id, stage.timeframe, stage.bar_end, stage.revision desc
        )
        insert into klines (
            symbol_id,
            timeframe,
            ts,
            open_x1000,
            high_x1000,
            low_x1000,
            close_x1000,
            volume,
            amount_x100,
            is_complete,
            revision,
            source
        )
        select
            symbol_id,
            timeframe,
            bar_end,
            open_x1000,
            high_x1000,
            low_x1000,
            close_x1000,
            volume,
            amount_x100,
            is_complete,
            revision,
            source
        from staged_rows
        on conflict (symbol_id, timeframe, ts) do update
        set open_x1000 = excluded.open_x1000,
            high_x1000 = excluded.high_x1000,
            low_x1000 = excluded.low_x1000,
            close_x1000 = excluded.close_x1000,
            volume = excluded.volume,
            amount_x100 = excluded.amount_x100,
            is_complete = excluded.is_complete,
            revision = excluded.revision,
        source = excluded.source,
        updated_at = now()
    where ({source_priority_case('excluded.source')}) > ({source_priority_case('klines.source')})
       or (
            ({source_priority_case('excluded.source')}) = ({source_priority_case('klines.source')})
            and (
                excluded.revision > klines.revision
                or (excluded.is_complete and not klines.is_complete)
            )
       )
    """
    )


async def upsert_watermarks(conn) -> None:
    await conn.execute(
        """
        insert into scheme2_ingest_watermarks (
            symbol_id,
            timeframe,
            last_bar_end,
            source,
            note,
            updated_at
        )
        select
            symbols.id,
            stage.timeframe,
            max(stage.bar_end) as last_bar_end,
            'native-parquet',
            'source=9 parquet_native historical bootstrap',
            now()
        from _native_parquet_kline_stage stage
        join symbols
          on symbols.code = stage.code
         and symbols.exchange = stage.exchange
        group by symbols.id, stage.timeframe
        on conflict (symbol_id, timeframe) do update
        set last_bar_end = greatest(
                coalesce(scheme2_ingest_watermarks.last_bar_end, excluded.last_bar_end),
                excluded.last_bar_end
            ),
            source = case
                when excluded.last_bar_end >= coalesce(
                    scheme2_ingest_watermarks.last_bar_end,
                    excluded.last_bar_end
                ) then excluded.source
                else scheme2_ingest_watermarks.source
            end,
            note = case
                when excluded.last_bar_end >= coalesce(
                    scheme2_ingest_watermarks.last_bar_end,
                    excluded.last_bar_end
                ) then excluded.note
                else scheme2_ingest_watermarks.note
            end,
            updated_at = now()
        """
    )


async def register_source_coverage(conn) -> None:
    await conn.execute(
        """
        insert into kline_source_coverage (symbol_id, timeframe, source, covered_until)
        select symbols.id, stage.timeframe, 9, max(stage.bar_end)
        from _native_parquet_kline_stage stage
        join symbols on symbols.code = stage.code and symbols.exchange = stage.exchange
        group by symbols.id, stage.timeframe
        on conflict (symbol_id, timeframe, source) do update
        set covered_until = greatest(kline_source_coverage.covered_until, excluded.covered_until),
            updated_at = now()
        """
    )


def normalize_symbol(value: str) -> str:
    raw = value.strip().upper()
    if "." in raw:
        left, right = raw.split(".", 1)
        if right in {"SH", "SZ", "BJ"}:
            return f"{left.zfill(6)}.{right}"
        if right == "SHSE":
            return f"{left.zfill(6)}.SH"
        if right == "SZSE":
            return f"{left.zfill(6)}.SZ"
        if right == "BSE":
            return f"{left.zfill(6)}.BJ"
    code = raw.zfill(6)
    if code.startswith(("600", "601", "603", "605", "688", "689", "900")):
        exchange = "SH"
    elif code.startswith(("4", "8")):
        exchange = "BJ"
    else:
        exchange = "SZ"
    return f"{code}.{exchange}"


def infer_asset_type(code: str) -> str:
    if code.startswith(("159", "160", "161", "162", "163", "164", "165", "166", "167", "168", "169", "510", "511", "512", "513", "515", "516", "517", "518", "588")):
        return "fund"
    return "stock"


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_import_run_id(value: str | UUID | None) -> UUID:
    if value is None or str(value).strip() == "":
        return uuid4()
    return UUID(str(value))


def to_float(value: Any, *, default: float | None = None) -> float:
    if value is None:
        if default is None:
            raise ValueError("missing numeric value")
        return default
    return float(value)


def to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    try:
        print(json.dumps(payload, ensure_ascii=False), flush=True)
    except (BrokenPipeError, OSError):
        pass


if __name__ == "__main__":
    asyncio.run(main())
