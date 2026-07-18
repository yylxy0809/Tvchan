from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import socket
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from collector.market_fill import (
    DEFAULT_TIMEFRAMES,
    create_provider,
    parse_timeframes,
    select_symbols,
    symbol_info_from_symbol,
)
from collector.storage.backfill_postgres import PostgresBackfillTaskStore
from collector.storage.postgres import LostBackfillLease, PostgresKlineWriter
from trading_protocol import Bar, SymbolInfo
from trading_protocol.timeframes import TIMEFRAMES

DB_TO_TIMEFRAME = {value.minutes: code for code, value in TIMEFRAMES.items()}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recoverable historical K-line backfill worker")
    parser.add_argument("--provider", default=os.getenv("HISTORY_BACKFILL_PROVIDER", "pytdx"), choices=["seed", "pytdx"])
    parser.add_argument("--symbols", default=os.getenv("HISTORY_BACKFILL_SYMBOLS"))
    parser.add_argument(
        "--symbols-file",
        default=os.getenv("HISTORY_BACKFILL_SYMBOLS_FILE"),
        help="UTF-8 file containing the exact symbol set, one symbol per line.",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_SYMBOL_LIMIT", "1")),
        help="Maximum symbols when --symbols is omitted. Use 0 for all provider symbols.",
    )
    parser.add_argument("--timeframes", default=os.getenv("HISTORY_BACKFILL_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument(
        "--stop-at",
        default=os.getenv("HISTORY_BACKFILL_STOP_AT"),
        help=(
            "Authoritative exclusive lower cutoff for every timeframe. Pages stop once "
            "they reach the boundary; only newer bars are written."
        ),
    )
    parser.add_argument(
        "--expected-through",
        default=os.getenv("HISTORY_BACKFILL_EXPECTED_THROUGH"),
        help="Authoritative inclusive upper freshness watermark for each scoped timeframe.",
    )
    parser.add_argument(
        "--freshness-contract-sha256",
        default=os.getenv("HISTORY_BACKFILL_FRESHNESS_CONTRACT_SHA256"),
        help="SHA-256 of the authoritative freshness contract used by a scoped run.",
    )
    parser.add_argument("--page-size", type=int, default=int(os.getenv("HISTORY_BACKFILL_PAGE_SIZE", "800")))
    parser.add_argument("--task-limit", type=int, default=int(os.getenv("HISTORY_BACKFILL_TASK_LIMIT", "3")))
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_LEASE_SECONDS", "300")),
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_MAX_ATTEMPTS", "5")),
    )
    parser.add_argument(
        "--worker-id",
        default=os.getenv("HISTORY_BACKFILL_WORKER_ID"),
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_CONCURRENCY", "1")),
        help="Maximum claimed backfill tasks to process concurrently.",
    )
    parser.add_argument(
        "--max-pages-per-task",
        type=int,
        default=int(os.getenv("HISTORY_BACKFILL_MAX_PAGES_PER_TASK", "1")),
        help="Pages to fetch for each claimed task. Use 0 to continue until the provider is exhausted.",
    )
    parser.add_argument("--sleep", type=float, default=float(os.getenv("HISTORY_BACKFILL_SLEEP", "0.25")))
    parser.add_argument("--loop", action="store_true", default=os.getenv("HISTORY_BACKFILL_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("HISTORY_BACKFILL_LOOP_INTERVAL", "30")),
    )
    parser.add_argument("--reset", action="store_true", default=os.getenv("HISTORY_BACKFILL_RESET") == "1")
    parser.add_argument(
        "--reset-running",
        action="store_true",
        default=os.getenv("HISTORY_BACKFILL_RESET_RUNNING") == "1",
    )
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("HISTORY_BACKFILL_DRY_RUN") == "1")
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
    parser.add_argument("--source-policy", default=os.getenv("HISTORY_BACKFILL_SOURCE_POLICY", "primary_failover"))
    parser.add_argument("--http-timeout", type=float, default=float(os.getenv("HISTORY_BACKFILL_HTTP_TIMEOUT", "5")))
    parser.add_argument("--pool-timeout", type=float, default=float(os.getenv("HISTORY_BACKFILL_POOL_TIMEOUT", "8")))
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    args = parser.parse_args(argv)
    if args.symbols and args.symbols_file:
        parser.error("--symbols and --symbols-file are mutually exclusive")
    if args.symbols_file and not args.stop_at:
        parser.error("--symbols-file requires --stop-at")
    if args.symbols_file and not args.expected_through:
        parser.error("--symbols-file requires --expected-through")
    if args.symbols_file and not re.fullmatch(
        r"[0-9a-f]{64}", str(args.freshness_contract_sha256 or "")
    ):
        parser.error("--symbols-file requires lowercase --freshness-contract-sha256")
    if args.stop_at and not args.symbols_file:
        parser.error("--stop-at is only supported with --symbols-file")
    if args.expected_through and not args.symbols_file:
        parser.error("--expected-through is only supported with --symbols-file")
    if args.symbols_file and args.provider != "pytdx":
        parser.error("--symbols-file scoped tail mode requires --provider pytdx")
    if args.symbols_file and (not args.tdx_host or "," in args.tdx_host):
        parser.error("--symbols-file scoped tail mode requires one explicit --tdx-host")
    if args.symbols_file and args.reset:
        parser.error("--reset is prohibited for immutable scoped tail runs")
    if args.lease_seconds <= 0:
        parser.error("--lease-seconds must be greater than zero")
    if args.max_attempts <= 0:
        parser.error("--max-attempts must be greater than zero")
    args.worker_id = str(
        args.worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    ).strip()
    if not args.worker_id or len(args.worker_id) > 160:
        parser.error("--worker-id must contain between 1 and 160 characters")
    return args


async def main() -> None:
    args = parse_args()
    while True:
        await run_once(args)
        if not args.loop:
            return
        await asyncio.sleep(args.loop_interval)


async def run_once(args: argparse.Namespace) -> None:
    provider = create_provider(args)
    timeframes = parse_timeframes(args.timeframes)
    scoped = bool(args.symbols_file)
    try:
        stop_at_by_timeframe = parse_stop_at(
            args.stop_at, timeframes, canonical_tail_labels=scoped
        )
        expected_through_by_timeframe = parse_stop_at(
            args.expected_through, timeframes, canonical_tail_labels=scoped
        )
        invalid_bounds = [
            timeframe for timeframe in timeframes
            if scoped
            and expected_through_by_timeframe[timeframe] <= stop_at_by_timeframe[timeframe]
        ]
        if invalid_bounds:
            raise ValueError(
                "scoped expected-through must be later than stop-at: "
                + ",".join(invalid_bounds)
            )
        symbols = (
            load_symbols_file(args.symbols_file)
            if args.symbols_file
            else await select_symbols(provider, args.symbols, args.symbol_limit)
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    manifest_sha256 = sha256_file(args.symbols_file) if scoped else None
    endpoint = canonical_tdx_endpoint(args.tdx_host, args.tdx_port) if scoped else None
    run_identity = (
        scoped_run_identity(
            provider=provider.name,
            manifest_sha256=manifest_sha256,
            symbols=[item.symbol for item in symbols],
            timeframes=timeframes,
            stop_at=stop_at_by_timeframe,
            expected_through=expected_through_by_timeframe,
            freshness_contract_sha256=args.freshness_contract_sha256,
            page_size=args.page_size,
            endpoint=endpoint,
            source_policy=args.source_policy,
        )
        if scoped else None
    )
    run_id = uuid5(NAMESPACE_URL, f"tvchan:history-backfill:{run_identity}") if scoped else None
    emit(
        "history_pass_started",
        provider=provider.name,
        symbols=len(symbols),
        timeframes=timeframes,
        page_size=args.page_size,
        task_limit=args.task_limit,
        concurrency=max(1, args.concurrency),
        max_pages_per_task=args.max_pages_per_task,
        dry_run=args.dry_run,
        stop_at={key: value.isoformat() for key, value in stop_at_by_timeframe.items()},
        expected_through={
            key: value.isoformat() for key, value in expected_through_by_timeframe.items()
        },
        freshness_contract_sha256=args.freshness_contract_sha256,
        run_id=None if run_id is None else str(run_id),
        run_identity=run_identity,
        manifest_sha256=manifest_sha256,
    )

    if args.dry_run:
        for symbol in symbols:
            for timeframe in timeframes:
                emit("history_dry_task", symbol=symbol.symbol, timeframe=timeframe)
        emit("history_pass_finished", tasks=0, pages=0, bars=0)
        return

    async with PostgresKlineWriter(args.database_url) as kline_writer:
        async with PostgresBackfillTaskStore(args.database_url) as task_store:
            if scoped:
                task_ids = await task_store.ensure_scoped_run_tasks(
                    run_id=run_id,
                    run_identity=run_identity,
                    manifest_sha256=manifest_sha256,
                    symbols=symbols,
                    timeframes=timeframes,
                    stop_at=stop_at_by_timeframe,
                    expected_through=expected_through_by_timeframe,
                    freshness_contract_sha256=args.freshness_contract_sha256,
                    provider=provider.name,
                    page_size=args.page_size,
                    endpoint=endpoint,
                    source_policy=args.source_policy,
                )
            else:
                await kline_writer.upsert_symbols(symbols)
                task_ids = await task_store.ensure_tasks(
                    symbols=symbols,
                    timeframes=timeframes,
                    provider=provider.name,
                    page_size=args.page_size,
                    reset=args.reset,
                )
            reset_count = 0
            if args.reset_running:
                reset_count = await task_store.reset_running(
                    provider=provider.name,
                    task_ids=task_ids,
                    run_id=run_id,
                )

            concurrency = max(1, args.concurrency)
            claim_limit = (
                min(max(1, args.task_limit), concurrency) if scoped else args.task_limit
            )
            result = {"pages": 0, "bars": 0, "failed": 0, "lease_lost": 0}
            claimed_total = 0
            durable_states: dict[str, int] = {}
            while True:
                tasks = await task_store.claim_tasks(
                    provider=provider.name,
                    limit=claim_limit,
                    worker_id=args.worker_id,
                    lease_seconds=args.lease_seconds,
                    max_attempts=args.max_attempts,
                    task_ids=task_ids,
                    run_id=run_id,
                )
                emit(
                    "history_tasks_claimed",
                    ensured=len(task_ids),
                    frozen_task_ids=task_ids if claimed_total == 0 else [],
                    run_identity=run_identity,
                    reset_running=reset_count,
                    tasks=len(tasks),
                    concurrency=concurrency,
                )
                if not tasks:
                    break
                claimed_total += len(tasks)
                batch_result = await process_tasks_concurrently(
                    provider_factory=lambda: create_provider(args),
                    kline_writer=kline_writer,
                    task_store=task_store,
                    tasks=tasks,
                    concurrency=concurrency,
                    max_pages_per_task=args.max_pages_per_task,
                    sleep=args.sleep,
                    lease_seconds=args.lease_seconds,
                    stop_at_by_timeframe=stop_at_by_timeframe,
                    expected_through_by_timeframe=expected_through_by_timeframe,
                    run_id=run_id,
                )
                for key in result:
                    result[key] += batch_result.get(key, 0)
                if not scoped:
                    break
                durable_states = await task_store.summarize_scoped_run(run_id)
                emit(
                    "history_scoped_run_status", run_id=str(run_id), states=durable_states
                )
                if (
                    batch_result.get("lease_lost", 0)
                    or durable_states.get("success", 0) == len(task_ids)
                ):
                    break
            emit(
                "history_pass_finished",
                tasks=claimed_total,
                pages=result["pages"],
                bars=result["bars"],
                failed=result.get("failed", 0),
                lease_lost=result.get("lease_lost", 0),
            )
            durable_states = await task_store.summarize_scoped_run(run_id) if scoped else {}
            if scoped:
                emit("history_scoped_run_status", run_id=str(run_id), states=durable_states)
            incomplete = sum(
                count for status, count in durable_states.items() if status != "success"
            )
            if scoped and (
                result.get("lease_lost", 0)
                or incomplete
                or durable_states.get("success", 0) != len(task_ids)
            ):
                raise RuntimeError(
                    "scoped historical backfill did not complete cleanly: "
                    f"failed={result.get('failed', 0)} "
                    f"lease_lost={result.get('lease_lost', 0)} "
                    f"states={durable_states}"
                )


async def process_tasks_concurrently(
    *,
    provider_factory,
    kline_writer: PostgresKlineWriter,
    task_store: PostgresBackfillTaskStore,
    tasks: list[dict[str, Any]],
    concurrency: int,
    max_pages_per_task: int,
    sleep: float,
    lease_seconds: int,
    stop_at_by_timeframe: dict[str, datetime] | None = None,
    expected_through_by_timeframe: dict[str, datetime] | None = None,
    run_id: UUID | None = None,
) -> dict[str, int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_task(task: dict[str, Any]) -> dict[str, int]:
        async with semaphore:
            timeframe = DB_TO_TIMEFRAME[int(task["timeframe"])]
            durable_run_id = task.get("run_id")
            durable_stop_at = task.get("stop_at")
            expected_stop_at = (stop_at_by_timeframe or {}).get(timeframe)
            durable_expected_through = task.get("expected_through")
            expected_through = (expected_through_by_timeframe or {}).get(timeframe)
            if (
                durable_run_id != run_id
                or durable_stop_at != expected_stop_at
                or durable_expected_through != expected_through
            ):
                raise RuntimeError("claimed scoped backfill identity drift")
            return await process_task(
                provider=provider_factory(),
                kline_writer=kline_writer,
                task_store=task_store,
                task=task,
                max_pages_per_task=max_pages_per_task,
                sleep=sleep,
                lease_seconds=lease_seconds,
                stop_at=durable_stop_at,
                expected_through=durable_expected_through,
                run_id=durable_run_id,
            )

    results = await asyncio.gather(*(run_task(task) for task in tasks))
    summary = {
        "pages": sum(item["pages"] for item in results),
        "bars": sum(item["bars"] for item in results),
    }
    failed = sum(item.get("failed", 0) for item in results)
    lease_lost = sum(item.get("lease_lost", 0) for item in results)
    if run_id is not None or failed or lease_lost:
        summary.update(failed=failed, lease_lost=lease_lost)
    return summary


async def process_task(
    *,
    provider,
    kline_writer: PostgresKlineWriter,
    task_store: PostgresBackfillTaskStore,
    task: dict[str, Any],
    max_pages_per_task: int,
    sleep: float,
    lease_seconds: int,
    stop_at: datetime | None = None,
    expected_through: datetime | None = None,
    run_id: UUID | None = None,
) -> dict[str, int]:
    symbol = str(task["symbol"])
    timeframe = DB_TO_TIMEFRAME[int(task["timeframe"])]
    page_size = int(task["page_size"])
    offset = int(task["next_offset"])
    pages = 0
    total_bars = 0
    failed = 0
    lost = 0
    exhausted = False
    lease_lost = asyncio.Event()
    stop_heartbeat = asyncio.Event()
    scoped_fence = (
        {"run_id": run_id, "stop_at": stop_at} if run_id is not None else {}
    )

    async def maintain_lease() -> None:
        interval = max(0.1, lease_seconds / 3)
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=interval)
                return
            except TimeoutError:
                try:
                    renewed = await task_store.heartbeat(
                        task_id=int(task["id"]),
                        claim_token=str(task["claim_token"]),
                        lease_version=int(task["lease_version"]),
                        lease_seconds=lease_seconds,
                        **scoped_fence,
                    )
                except Exception:
                    renewed = False
                if not renewed:
                    lease_lost.set()
                    return

    heartbeat_task = asyncio.create_task(maintain_lease())

    try:
        while max_pages_per_task <= 0 or pages < max_pages_per_task:
            if lease_lost.is_set():
                raise LostBackfillLease(f"historical backfill task lease lost: {task['id']}")
            bars, raw_rows_read = await get_provider_page_with_raw_count(
                provider,
                symbol=symbol,
                timeframe=timeframe,
                offset=offset,
                limit=page_size,
            )
            if lease_lost.is_set():
                raise LostBackfillLease(f"historical backfill task lease lost: {task['id']}")
            provider_exhausted = raw_rows_read < page_size
            next_offset = offset + raw_rows_read
            provider_page_newest = max((bar.ts for bar in bars), default=None)
            durable_provider_newest = task.get("provider_newest_ts")
            provider_newest = max(
                (value for value in (durable_provider_newest, provider_page_newest) if value is not None),
                default=None,
            )
            if run_id is not None and expected_through is None:
                raise RuntimeError("scoped task is missing expected-through evidence")
            if (
                run_id is not None
                and provider_newest is not None
                and provider_newest < expected_through
            ):
                raise RuntimeError("provider newest bar is earlier than expected-through")
            if run_id is not None and provider_newest is None:
                raise RuntimeError("provider returned no bars to prove expected-through")
            stop_reached = stop_at is not None and any(bar.ts <= stop_at for bar in bars)
            if run_id is not None and provider_exhausted and not stop_reached:
                raise RuntimeError("provider exhausted before stop-at boundary")
            if expected_through is not None:
                bars = [bar for bar in bars if bar.ts <= expected_through]
            if stop_at is not None:
                bars = [bar for bar in bars if bar.ts > stop_at]
            exhausted = stop_reached if run_id is not None else provider_exhausted or stop_reached
            bars_read = len(bars)
            oldest_ts = min((bar.ts for bar in bars), default=None)
            newest_ts = max((bar.ts for bar in bars), default=None)
            bars_written = await kline_writer.commit_history_backfill_page(
                task=task,
                expected_offset=offset,
                next_offset=next_offset,
                bars=bars,
                oldest_ts=oldest_ts,
                newest_ts=newest_ts,
                exhausted=exhausted,
                lease_seconds=lease_seconds,
                provider_newest_ts=provider_newest,
            )
            emit(
                "history_page_written",
                symbol=symbol,
                timeframe=timeframe,
                offset=offset,
                next_offset=next_offset,
                bars_read=bars_read,
                raw_rows_read=raw_rows_read,
                bars_written=bars_written,
                exhausted=exhausted,
            )
            pages += 1
            total_bars += bars_written
            offset = next_offset
            if exhausted:
                break
            await sleep_between_requests(sleep)

        if not exhausted:
            stop_heartbeat.set()
            await heartbeat_task
            if not await task_store.yield_task(
                task_id=int(task["id"]),
                claim_token=str(task["claim_token"]),
                lease_version=int(task["lease_version"]),
                **scoped_fence,
            ):
                raise LostBackfillLease(
                    f"historical backfill task lease lost before yield: {task['id']}"
                )

    except LostBackfillLease as exc:
        lost = 1
        emit(
            "history_task_lease_lost",
            symbol=symbol,
            timeframe=timeframe,
            offset=offset,
            error=str(exc)[:500],
        )
    except Exception as exc:
        recorded = await task_store.record_failure(
            task_id=int(task["id"]),
            claim_token=str(task["claim_token"]),
            lease_version=int(task["lease_version"]),
            error=str(exc),
            **scoped_fence,
        )
        emit(
            "history_task_failed" if recorded else "history_task_lease_lost",
            symbol=symbol,
            timeframe=timeframe,
            offset=offset,
            error=str(exc)[:500],
        )
        if recorded:
            failed = 1
        else:
            lost = 1
    finally:
        stop_heartbeat.set()
        if not heartbeat_task.done():
            await heartbeat_task
    result = {"pages": pages, "bars": total_bars}
    if run_id is not None or failed or lost:
        result.update(failed=failed, lease_lost=lost)
    return result


async def get_provider_page(
    provider,
    *,
    symbol: str,
    timeframe: str,
    offset: int,
    limit: int,
) -> list[Bar]:
    bars, _raw_count = await get_provider_page_with_raw_count(
        provider,
        symbol=symbol,
        timeframe=timeframe,
        offset=offset,
        limit=limit,
    )
    return bars


async def get_provider_page_with_raw_count(
    provider,
    *,
    symbol: str,
    timeframe: str,
    offset: int,
    limit: int,
) -> tuple[list[Bar], int]:
    if hasattr(provider, "get_bars_page_with_raw_count"):
        return await provider.get_bars_page_with_raw_count(
            symbol,
            timeframe,
            offset=offset,
            limit=limit,
        )
    if hasattr(provider, "get_bars_page"):
        bars = await provider.get_bars_page(
            symbol,
            timeframe,
            offset=offset,
            limit=limit,
        )
        return bars, len(bars)
    bars = await provider.get_bars(symbol, timeframe, limit=offset + limit)
    page = bars[offset : offset + limit]
    return page, len(page)


async def sleep_between_requests(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def load_symbols_file(path: str | os.PathLike[str]) -> list[SymbolInfo]:
    source = Path(path)
    if not source.is_file():
        raise ValueError(f"symbols file does not exist: {source}")
    values: list[SymbolInfo] = []
    seen: set[str] = set()
    for raw in source.read_text(encoding="utf-8-sig").splitlines():
        item = raw.split("#", 1)[0].strip()
        if item:
            if not re.fullmatch(r"[0-9]{6}\.(?:SH|SZ)", item):
                raise ValueError(
                    f"symbols file entry must be canonical SH/SZ: {item!r}"
                )
            if item in seen:
                raise ValueError(f"symbols file contains duplicate symbol: {item}")
            seen.add(item)
            values.append(symbol_info_from_symbol(item))
    if not values:
        raise ValueError("symbols file must contain at least one symbol")
    return sorted(values, key=lambda item: item.symbol)


def parse_stop_at(
    value: str | None,
    timeframes: list[str],
    *,
    canonical_tail_labels: bool = False,
) -> dict[str, datetime]:
    if not value:
        return {}
    parsed: dict[str, datetime] = {}
    for raw in value.split(","):
        key, separator, text_value = raw.strip().partition("=")
        if not separator or not key or not text_value:
            raise ValueError("--stop-at must use timeframe=ISO-8601 entries")
        if key in parsed:
            raise ValueError(f"duplicate --stop-at timeframe: {key}")
        try:
            cutoff = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"invalid --stop-at timestamp for {key}") from exc
        if cutoff.tzinfo is None:
            raise ValueError(f"--stop-at timestamp must include timezone for {key}")
        parsed[key] = cutoff.astimezone(UTC)
    missing = sorted(set(timeframes) - set(parsed))
    extra = sorted(set(parsed) - set(timeframes))
    if missing:
        raise ValueError(f"--stop-at missing timeframes: {','.join(missing)}")
    if extra:
        raise ValueError(f"--stop-at contains unselected timeframes: {','.join(extra)}")
    if canonical_tail_labels:
        unsupported = sorted(set(timeframes) - {"5f", "30f", "1d"})
        if unsupported:
            raise ValueError(
                "scoped PyTDX tail only supports 5f,30f,1d: " + ",".join(unsupported)
            )
        shanghai = ZoneInfo("Asia/Shanghai")
        invalid = sorted(
            key
            for key, cutoff in parsed.items()
            if (
                cutoff.astimezone(shanghai).hour,
                cutoff.astimezone(shanghai).minute,
                cutoff.astimezone(shanghai).second,
                cutoff.astimezone(shanghai).microsecond,
            ) != (15, 0, 0, 0)
        )
        if invalid:
            raise ValueError(
                "scoped --stop-at must use canonical Shanghai 15:00 labels: "
                + ",".join(invalid)
            )
    return parsed


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_tdx_endpoint(host: str, port: int) -> str:
    value = host.strip()
    if not value or any(character.isspace() for character in value):
        raise ValueError("scoped PyTDX endpoint must be one explicit host")
    return value if ":" in value else f"{value}:{int(port)}"


def scoped_run_identity(
    *,
    provider: str,
    manifest_sha256: str,
    symbols: list[str],
    timeframes: list[str],
    stop_at: dict[str, datetime],
    expected_through: dict[str, datetime],
    freshness_contract_sha256: str,
    page_size: int,
    endpoint: str,
    source_policy: str,
) -> str:
    payload = {
        "provider": provider,
        "manifest_sha256": manifest_sha256,
        "symbols": sorted(symbols),
        "timeframes": sorted(timeframes),
        "stop_at": {key: value.isoformat() for key, value in sorted(stop_at.items())},
        "expected_through": {
            key: value.isoformat() for key, value in sorted(expected_through.items())
        },
        "freshness_contract_sha256": freshness_contract_sha256,
        "page_size": int(page_size),
        "endpoint": endpoint,
        "source_policy": source_policy,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


if __name__ == "__main__":
    asyncio.run(main())
