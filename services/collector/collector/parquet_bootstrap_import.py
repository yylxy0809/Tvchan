from __future__ import annotations

import argparse
import asyncio
import importlib
import io
import json
import os
import re
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from collector.storage.scheme2_postgres import (
    PARQUET_5F_SOURCE,
    PARQUET_5F_TIMEFRAME,
    PostgresScheme2KlineWriter,
    PostgresScheme2MemberCheckpointStore,
    Scheme2SourceMember,
)
from trading_protocol import Bar, SymbolInfo

REQUIRED_COLUMNS = (
    "code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)
DEFAULT_ROOT = "D:\\5f\u6570\u636e\\5m_price"
DEFAULT_BATCH_SIZE = 50_000
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
EXCHANGE_PREFIXES = {
    "SH": ("600", "601", "603", "605", "688", "689", "900"),
    "SZ": ("000", "001", "002", "003", "200", "300", "301"),
    "BJ": ("4", "8"),
}
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
CODE_RE = re.compile(r"(?P<code>\d{1,6})")


@dataclass
class ParsedParquetBatch:
    symbols: dict[str, SymbolInfo]
    bars: list[Bar]

    @classmethod
    def empty(cls) -> "ParsedParquetBatch":
        return cls(symbols={}, bars=[])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Scheme 2 zipped 5f parquet history into PostgreSQL"
    )
    parser.add_argument("--root", default=os.getenv("PARQUET_5F_ROOT", DEFAULT_ROOT))
    parser.add_argument("--task-limit", type=int, default=int(os.getenv("PARQUET_5F_TASK_LIMIT", "1")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("PARQUET_5F_CONCURRENCY", "1")))
    parser.add_argument(
        "--write-concurrency",
        type=int,
        default=int(os.getenv("PARQUET_5F_WRITE_CONCURRENCY", "1")),
    )
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("PARQUET_5F_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    parser.add_argument("--reset", action="store_true", default=os.getenv("PARQUET_5F_RESET") == "1")
    parser.add_argument(
        "--reset-running",
        action="store_true",
        default=os.getenv("PARQUET_5F_RESET_RUNNING") == "1",
    )
    parser.add_argument("--loop", action="store_true", default=os.getenv("PARQUET_5F_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("PARQUET_5F_LOOP_INTERVAL", "300")),
    )
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("PARQUET_5F_DRY_RUN") == "1")
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    while True:
        await run_once(args)
        if not args.loop:
            return
        await asyncio.sleep(args.loop_interval)


async def run_once(args: argparse.Namespace) -> None:
    root = Path(args.root)
    members = discover_parquet_members(root)
    emit(
        "parquet_5f_import_started",
        root=str(root),
        members=len(members),
        source=PARQUET_5F_SOURCE,
        timeframe=PARQUET_5F_TIMEFRAME,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        for member in members[: min(len(members), 20)]:
            emit(
                "parquet_5f_member",
                zip_path=member.zip_path,
                member_path=member.member_path,
                member_size_bytes=member.member_size_bytes,
            )
        emit("parquet_5f_import_finished", members=0, bars=0)
        return

    async with PostgresScheme2MemberCheckpointStore(args.database_url) as checkpoint_store:
        ensured = await checkpoint_store.ensure_member_checkpoints(members, reset=args.reset)
        reset_running = 0
        if args.reset_running:
            reset_running = await checkpoint_store.reset_running()
        tasks = await checkpoint_store.claim_member_checkpoints(limit=args.task_limit)
        emit(
            "parquet_5f_tasks_claimed",
            ensured=ensured,
            reset_running=reset_running,
            tasks=len(tasks),
            concurrency=max(1, args.concurrency),
            write_concurrency=max(1, args.write_concurrency),
        )
        async with PostgresScheme2KlineWriter(args.database_url) as writer:
            result = await process_tasks_concurrently(
                writer=writer,
                checkpoint_store=checkpoint_store,
                tasks=tasks,
                batch_size=max(1, args.batch_size),
                concurrency=max(1, args.concurrency),
                write_concurrency=max(1, args.write_concurrency),
            )
    emit("parquet_5f_import_finished", members=len(tasks), bars=result["bars"])


def discover_parquet_members(
    root: str | Path,
    *,
    source_profile: str = PARQUET_5F_SOURCE,
    timeframe: int = PARQUET_5F_TIMEFRAME,
) -> list[Scheme2SourceMember]:
    root_path = Path(root).expanduser().resolve()
    if not root_path.exists():
        return []
    if not root_path.is_dir():
        raise NotADirectoryError(f"Parquet bootstrap root is not a directory: {root_path}")

    members: list[Scheme2SourceMember] = []
    for zip_path in sorted(root_path.glob("*.zip")):
        try:
            with zipfile.ZipFile(zip_path) as archive:
                entries = [
                    entry
                    for entry in archive.infolist()
                    if not entry.is_dir() and entry.filename.lower().endswith(".parquet")
                ]
        except zipfile.BadZipFile:
            continue
        for entry in entries:
            members.append(
                Scheme2SourceMember(
                    root_path=str(root_path),
                    source_profile=source_profile,
                    zip_path=str(zip_path.resolve()),
                    member_path=entry.filename,
                    member_crc32=entry.CRC,
                    member_size_bytes=entry.file_size,
                    timeframe=timeframe,
                )
            )
    for parquet_path in sorted((root_path / "symbols").glob("**/*.parquet")):
        if not parquet_path.is_file():
            continue
        stat = parquet_path.stat()
        members.append(
            Scheme2SourceMember(
                root_path=str(root_path),
                source_profile=source_profile,
                zip_path=str(parquet_path.resolve()),
                member_path="",
                member_crc32=None,
                member_size_bytes=stat.st_size,
                timeframe=timeframe,
            )
        )
    return members


async def process_tasks_concurrently(
    *,
    writer,
    checkpoint_store,
    tasks: list[dict[str, Any]],
    batch_size: int,
    concurrency: int,
    write_concurrency: int = 1,
) -> dict[str, int]:
    read_semaphore = asyncio.Semaphore(max(1, concurrency))
    write_semaphore = asyncio.Semaphore(max(1, write_concurrency))

    async def run_task(task: dict[str, Any]) -> dict[str, int]:
        async with read_semaphore:
            return await process_member_task(
                writer=writer,
                checkpoint_store=checkpoint_store,
                task=task,
                batch_size=batch_size,
                write_semaphore=write_semaphore,
            )

    results = await asyncio.gather(*(run_task(task) for task in tasks))
    return {"bars": sum(item["bars"] for item in results)}


async def process_member_task(
    *,
    writer,
    checkpoint_store,
    task: dict[str, Any],
    batch_size: int,
    write_semaphore: asyncio.Semaphore | None = None,
) -> dict[str, int]:
    checkpoint_id = int(task["id"])
    zip_path = str(task["zip_path"])
    member_path = str(task["member_path"])
    bars_written = 0
    try:
        for rows in _iter_member_rows(zip_path, member_path, batch_size=batch_size):
            parsed = parse_parquet_rows(rows)
            if parsed.bars:
                if write_semaphore is None:
                    bars_written += await writer.upsert_5f_bars(
                        symbols=parsed.symbols.values(),
                        bars=parsed.bars,
                    )
                else:
                    async with write_semaphore:
                        bars_written += await writer.upsert_5f_bars(
                            symbols=parsed.symbols.values(),
                            bars=parsed.bars,
                        )
        await checkpoint_store.record_member_success(
            checkpoint_id=checkpoint_id,
            imported_rows=bars_written,
        )
        emit(
            "parquet_5f_member_finished",
            zip_path=zip_path,
            member_path=member_path,
            bars=bars_written,
        )
        return {"bars": bars_written}
    except Exception as exc:
        await checkpoint_store.record_member_failure(
            checkpoint_id=checkpoint_id,
            error=str(exc),
            imported_rows=bars_written,
        )
        emit(
            "parquet_5f_member_failed",
            zip_path=zip_path,
            member_path=member_path,
            bars=bars_written,
            error=str(exc)[:500],
        )
        return {"bars": bars_written}


def parse_parquet_rows(rows: Iterable[dict[str, Any]]) -> ParsedParquetBatch:
    parsed = ParsedParquetBatch.empty()
    for row in rows:
        missing = [column for column in REQUIRED_COLUMNS if column not in row]
        if missing:
            raise ValueError(f"Missing required parquet columns: {','.join(missing)}")
        symbol = parse_symbol(row["code"])
        bar_end = parse_trade_time_as_bar_end(row["trade_time"])
        parsed.symbols[symbol.symbol] = symbol
        parsed.bars.append(
            Bar(
                symbol=symbol.symbol,
                timeframe="5f",
                ts=bar_end,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(round(float(row["vol"]))),
                amount=None if row["amount"] is None else float(row["amount"]),
                complete=True,
                revision=0,
                source=PARQUET_5F_SOURCE,
            )
        )
    return parsed


def parse_symbol(value: Any) -> SymbolInfo:
    raw = str(value).strip().upper()
    exchange: str | None = None
    code_text = raw
    if "." in raw:
        left, right = [part.strip() for part in raw.split(".", 1)]
        if left in EXCHANGE_PREFIXES:
            exchange = left
            code_text = right
        else:
            code_text = left
            exchange = right if right in EXCHANGE_PREFIXES else None
    elif raw.startswith(("SH", "SZ", "BJ")):
        exchange = raw[:2]
        code_text = raw[2:]

    match = CODE_RE.search(code_text)
    if match is None:
        raise ValueError(f"Unsupported parquet code value: {value!r}")
    code = match.group("code").zfill(6)
    exchange = exchange or infer_exchange(code)
    if exchange is None:
        raise ValueError(f"Cannot infer exchange for parquet code: {code}")
    asset_type = "fund" if code.startswith(FUND_PREFIXES) else "stock"
    symbol = f"{code}.{exchange}"
    return SymbolInfo(
        symbol=symbol,
        code=code,
        exchange=exchange,
        name=code,
        asset_type=asset_type,
        market="A_SHARE",
    )


def infer_exchange(code: str) -> str | None:
    for exchange, prefixes in EXCHANGE_PREFIXES.items():
        if code.startswith(prefixes):
            return exchange
    return None


def parse_trade_time_as_bar_end(value: Any) -> datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).strip())
    # trade_time is already the 5f bar_end supplied by the parquet source.
    # Attach the A-share market timezone to naive values without adding 5m or
    # converting between zones, so the wall-clock bar_end remains unchanged.
    if dt.tzinfo is None:
        return dt.replace(tzinfo=SHANGHAI_TZ)
    return dt


def _iter_member_rows(
    zip_path: str | Path,
    member_path: str,
    *,
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    parquet = _require_parquet_module()
    zip_path = Path(zip_path)
    if zip_path.suffix.lower() == ".parquet" and member_path == "":
        parquet_file = parquet.ParquetFile(str(zip_path))
    else:
        with zipfile.ZipFile(zip_path) as archive:
            payload = archive.read(member_path)
        parquet_file = parquet.ParquetFile(io.BytesIO(payload))
    columns = [str(name) for name in parquet_file.schema_arrow.names]
    missing = [column for column in REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"Missing required parquet columns: {','.join(missing)}")
    for record_batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=list(REQUIRED_COLUMNS),
    ):
        yield record_batch.to_pylist()


def _require_parquet_module():
    try:
        return importlib.import_module("pyarrow.parquet")
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow is required for Scheme 2 parquet import.") from exc


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
