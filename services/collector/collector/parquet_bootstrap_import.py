from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import io
import json
import os
import re
import socket
import uuid
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
    LostScheme2MemberLease,
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
_MEMBER_ROWS_END = object()


def _next_member_rows(iterator: Iterator[list[dict[str, Any]]]):
    try:
        return next(iterator)
    except StopIteration:
        return _MEMBER_ROWS_END


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
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.getenv("PARQUET_5F_LEASE_SECONDS", "300")),
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.getenv("PARQUET_5F_MAX_ATTEMPTS", "5")),
    )
    parser.add_argument(
        "--max-batches-per-member",
        type=int,
        default=int(os.getenv("PARQUET_5F_MAX_BATCHES_PER_MEMBER", "0")),
    )
    parser.add_argument("--worker-id", default=os.getenv("PARQUET_5F_WORKER_ID"))
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
    args = parser.parse_args(argv)
    if args.lease_seconds <= 0:
        parser.error("--lease-seconds must be greater than zero")
    if args.max_attempts <= 0:
        parser.error("--max-attempts must be greater than zero")
    if args.max_batches_per_member < 0:
        parser.error("--max-batches-per-member cannot be negative")
    args.worker_id = str(
        args.worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    ).strip()
    if not args.worker_id or len(args.worker_id) > 160:
        parser.error("--worker-id must contain between 1 and 160 characters")
    return args


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
        tasks = await checkpoint_store.claim_member_checkpoints(
            limit=args.task_limit,
            worker_id=args.worker_id,
            lease_seconds=args.lease_seconds,
            max_attempts=args.max_attempts,
        )
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
                lease_seconds=args.lease_seconds,
                max_batches_per_member=args.max_batches_per_member,
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
        with parquet_path.open("rb") as source:
            member_size_bytes, content_sha256 = _sha256_stream(source)
        members.append(
            Scheme2SourceMember(
                root_path=str(root_path),
                source_profile=source_profile,
                zip_path=str(parquet_path.resolve()),
                member_path="",
                member_crc32=None,
                member_size_bytes=member_size_bytes,
                content_sha256=content_sha256,
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
    lease_seconds: int = 300,
    max_batches_per_member: int = 0,
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
                lease_seconds=lease_seconds,
                max_batches_per_member=max_batches_per_member,
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
    lease_seconds: int = 300,
    max_batches_per_member: int = 0,
) -> dict[str, int]:
    checkpoint_id = int(task["id"])
    zip_path = str(task["zip_path"])
    member_path = str(task["member_path"])
    bars_written = 0
    progress = int(task.get("imported_rows") or 0)
    resume_rows = progress
    batches_committed = 0
    lease_lost = asyncio.Event()
    stop_heartbeat = asyncio.Event()

    async def maintain_lease() -> None:
        interval = max(0.1, lease_seconds / 3)
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=interval)
                return
            except TimeoutError:
                try:
                    renewed = await checkpoint_store.heartbeat(
                        checkpoint_id=checkpoint_id,
                        claim_token=str(task["claim_token"]),
                        lease_version=int(task["lease_version"]),
                        lease_seconds=lease_seconds,
                    )
                except Exception:
                    renewed = False
                if not renewed:
                    lease_lost.set()
                    return

    heartbeat_task = asyncio.create_task(maintain_lease())
    try:
        iterator = _iter_member_rows(
            zip_path,
            member_path,
            batch_size=batch_size,
            expected_member_crc32=task.get("member_crc32"),
            expected_member_size_bytes=task.get("member_size_bytes"),
            expected_content_sha256=task.get("content_sha256"),
        )
        while True:
            if lease_lost.is_set():
                raise LostScheme2MemberLease(
                    f"Scheme 2 member lease lost: {checkpoint_id}"
                )
            rows = await asyncio.to_thread(_next_member_rows, iterator)
            if rows is _MEMBER_ROWS_END:
                break
            if lease_lost.is_set():
                raise LostScheme2MemberLease(
                    f"Scheme 2 member lease lost: {checkpoint_id}"
                )
            if resume_rows:
                if resume_rows >= len(rows):
                    resume_rows -= len(rows)
                    continue
                rows = rows[resume_rows:]
                resume_rows = 0
            if max_batches_per_member > 0 and batches_committed >= max_batches_per_member:
                stop_heartbeat.set()
                await heartbeat_task
                yielded = await checkpoint_store.yield_member(
                    checkpoint_id=checkpoint_id,
                    claim_token=str(task["claim_token"]),
                    lease_version=int(task["lease_version"]),
                    expected_imported_rows=progress,
                )
                if not yielded:
                    raise LostScheme2MemberLease(
                        f"Scheme 2 member lease lost before yield: {checkpoint_id}"
                    )
                emit(
                    "parquet_5f_member_yielded",
                    zip_path=zip_path,
                    member_path=member_path,
                    bars=bars_written,
                    imported_rows=progress,
                )
                return {"bars": bars_written}
            parsed = parse_parquet_rows(rows)
            if parsed.bars:
                if write_semaphore is None:
                    written = await writer.commit_member_batch(
                        task=task,
                        expected_imported_rows=progress,
                        symbols=parsed.symbols.values(),
                        bars=parsed.bars,
                        lease_seconds=lease_seconds,
                    )
                else:
                    async with write_semaphore:
                        written = await writer.commit_member_batch(
                            task=task,
                            expected_imported_rows=progress,
                            symbols=parsed.symbols.values(),
                            bars=parsed.bars,
                            lease_seconds=lease_seconds,
                        )
                bars_written += written
                progress += written
                batches_committed += 1
        if resume_rows:
            raise ValueError(
                "Scheme 2 checkpoint progress exceeds the authoritative member row count"
            )
        stop_heartbeat.set()
        await heartbeat_task
        succeeded = await checkpoint_store.record_member_success(
            checkpoint_id=checkpoint_id,
            claim_token=str(task["claim_token"]),
            lease_version=int(task["lease_version"]),
            expected_imported_rows=progress,
        )
        if not succeeded:
            raise LostScheme2MemberLease(
                f"Scheme 2 member lease lost before success: {checkpoint_id}"
            )
        emit(
            "parquet_5f_member_finished",
            zip_path=zip_path,
            member_path=member_path,
            bars=bars_written,
        )
        return {"bars": bars_written}
    except LostScheme2MemberLease as exc:
        emit(
            "parquet_5f_member_lease_lost",
            zip_path=zip_path,
            member_path=member_path,
            bars=bars_written,
            error=str(exc)[:500],
        )
        return {"bars": bars_written}
    except Exception as exc:
        recorded = await checkpoint_store.record_member_failure(
            checkpoint_id=checkpoint_id,
            claim_token=str(task["claim_token"]),
            lease_version=int(task["lease_version"]),
            error=str(exc),
            expected_imported_rows=progress,
        )
        emit(
            "parquet_5f_member_failed" if recorded else "parquet_5f_member_lease_lost",
            zip_path=zip_path,
            member_path=member_path,
            bars=bars_written,
            error=str(exc)[:500],
        )
        return {"bars": bars_written}
    finally:
        stop_heartbeat.set()
        if not heartbeat_task.done():
            await heartbeat_task


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
    expected_member_crc32: object = None,
    expected_member_size_bytes: object = None,
    expected_content_sha256: object = None,
) -> Iterator[list[dict[str, Any]]]:
    zip_path = Path(zip_path)
    if zip_path.suffix.lower() == ".parquet" and member_path == "":
        with zip_path.open("rb") as source:
            actual_size, actual_sha256 = _sha256_stream(source)
            if expected_member_crc32 is not None:
                raise ValueError("Direct parquet checkpoint must not contain a member CRC")
            _require_exact_member_value(
                field="size",
                expected=expected_member_size_bytes,
                actual=actual_size,
            )
            _require_exact_content_sha256(
                expected=expected_content_sha256,
                actual=actual_sha256,
            )
            source.seek(0)
            parquet = _require_parquet_module()
            parquet_file = parquet.ParquetFile(source)
            yield from _iter_parquet_rows(parquet_file, batch_size=batch_size)
    else:
        if expected_content_sha256 is not None:
            raise ValueError("ZIP member checkpoint must not contain a content SHA-256")
        with zipfile.ZipFile(zip_path) as archive:
            member = archive.getinfo(member_path)
            _require_exact_member_value(
                field="CRC32",
                expected=expected_member_crc32,
                actual=member.CRC,
            )
            _require_exact_member_value(
                field="size",
                expected=expected_member_size_bytes,
                actual=member.file_size,
            )
            payload = archive.read(member)
        parquet = _require_parquet_module()
        parquet_file = parquet.ParquetFile(io.BytesIO(payload))
        yield from _iter_parquet_rows(parquet_file, batch_size=batch_size)


def _iter_parquet_rows(parquet_file, *, batch_size: int) -> Iterator[list[dict[str, Any]]]:
    columns = [str(name) for name in parquet_file.schema_arrow.names]
    missing = [column for column in REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"Missing required parquet columns: {','.join(missing)}")
    for record_batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=list(REQUIRED_COLUMNS),
    ):
        yield record_batch.to_pylist()


def _sha256_stream(source, *, chunk_size: int = 1024 * 1024) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(chunk_size):
        digest.update(chunk)
        size += len(chunk)
    return size, digest.hexdigest()


def _require_exact_member_value(*, field: str, expected: object, actual: int) -> None:
    if expected is None or int(expected) != actual:
        raise ValueError(
            f"Scheme 2 source member {field} does not match its claimed checkpoint"
        )


def _require_exact_content_sha256(*, expected: object, actual: str) -> None:
    if expected is None or str(expected).lower() != actual:
        raise ValueError(
            "Scheme 2 direct parquet SHA-256 does not match its claimed checkpoint"
        )


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
