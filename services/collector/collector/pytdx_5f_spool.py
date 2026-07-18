from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from collector.market_fill import create_provider, parse_csv, select_symbols
from collector.providers.pytdx_provider import PytdxProvider
from trading_protocol import Bar, SymbolInfo

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_OUTPUT_ROOT = "D:\\5f数据\\5m_price_incremental"
DEFAULT_START = "2026-04-18"
DEFAULT_TIMEFRAME = "5f"


@dataclass(frozen=True)
class SpoolResult:
    symbol: str
    bars: int
    pages: int
    oldest_ts: datetime | None
    newest_ts: datetime | None
    exhausted: bool
    output_path: Path | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch pytdx 5f incrementals to local parquet files without writing DB"
    )
    parser.add_argument("--provider", default="pytdx", choices=["pytdx"])
    parser.add_argument("--symbols", default=os.getenv("PYTDX_5F_SPOOL_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("PYTDX_5F_SPOOL_SYMBOL_LIMIT", "1")),
        help="Maximum symbols when --symbols is omitted. Use 0 for all provider symbols.",
    )
    parser.add_argument("--timeframe", default=os.getenv("PYTDX_5F_SPOOL_TIMEFRAME", DEFAULT_TIMEFRAME))
    parser.add_argument("--start", default=os.getenv("PYTDX_5F_SPOOL_START", DEFAULT_START))
    parser.add_argument("--end", default=os.getenv("PYTDX_5F_SPOOL_END"))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("PYTDX_5F_SPOOL_PAGE_SIZE", "800")))
    parser.add_argument(
        "--max-pages-per-symbol",
        type=int,
        default=int(os.getenv("PYTDX_5F_SPOOL_MAX_PAGES_PER_SYMBOL", "0")),
        help="Use 0 to page until older than --start or provider exhaustion.",
    )
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("PYTDX_5F_SPOOL_CONCURRENCY", "4")))
    parser.add_argument("--sleep", type=float, default=float(os.getenv("PYTDX_5F_SPOOL_SLEEP", "0.15")))
    parser.add_argument("--output-root", default=os.getenv("PYTDX_5F_SPOOL_OUTPUT_ROOT", DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--reset", action="store_true", default=os.getenv("PYTDX_5F_SPOOL_RESET") == "1")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("PYTDX_5F_SPOOL_DRY_RUN") == "1")
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    await run_once(args)


async def run_once(args: argparse.Namespace) -> None:
    provider = create_provider(args)
    if not isinstance(provider, PytdxProvider):
        raise ValueError("pytdx_5f_spool only supports the pytdx provider")
    timeframe = str(args.timeframe)
    if timeframe != DEFAULT_TIMEFRAME:
        raise ValueError("pytdx_5f_spool currently supports only 5f bars")

    start_dt = parse_boundary(args.start, end_of_day=False)
    end_dt = parse_boundary(args.end, end_of_day=True) if args.end else datetime.now(SHANGHAI_TZ)
    output_root = Path(args.output_root).expanduser().resolve()
    symbols = await select_symbols(provider, args.symbols, args.symbol_limit)
    emit(
        "pytdx_5f_spool_started",
        symbols=len(symbols),
        start=start_dt.isoformat(),
        end=end_dt.isoformat(),
        output_root=str(output_root),
        dry_run=args.dry_run,
        concurrency=max(1, args.concurrency),
    )

    if args.dry_run:
        for symbol in symbols[: min(20, len(symbols))]:
            emit("pytdx_5f_spool_symbol", symbol=symbol.symbol, name=symbol.name)
        emit("pytdx_5f_spool_finished", symbols=0, bars=0, pages=0)
        return

    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "symbols").mkdir(exist_ok=True)
    (output_root / "checkpoints").mkdir(exist_ok=True)
    semaphore = asyncio.Semaphore(max(1, args.concurrency))

    async def run_symbol(symbol: SymbolInfo) -> SpoolResult:
        async with semaphore:
            local_provider = create_provider(args)
            return await spool_symbol(
                provider=local_provider,
                symbol=symbol,
                timeframe=timeframe,
                start_dt=start_dt,
                end_dt=end_dt,
                output_root=output_root,
                page_size=max(1, args.page_size),
                max_pages_per_symbol=args.max_pages_per_symbol,
                sleep=max(0.0, args.sleep),
                reset=args.reset,
            )

    results = await asyncio.gather(*(run_symbol(symbol) for symbol in symbols))
    emit(
        "pytdx_5f_spool_finished",
        symbols=len(results),
        bars=sum(item.bars for item in results),
        pages=sum(item.pages for item in results),
    )


async def spool_symbol(
    *,
    provider,
    symbol: SymbolInfo,
    timeframe: str,
    start_dt: datetime,
    end_dt: datetime,
    output_root: Path,
    page_size: int,
    max_pages_per_symbol: int,
    sleep: float,
    reset: bool = False,
) -> SpoolResult:
    checkpoint_path = checkpoint_file(output_root, symbol.symbol)
    output_path = symbol_output_file(output_root, symbol.symbol)
    if checkpoint_path.exists() and not reset:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if checkpoint.get("status") == "success" and output_path.exists():
            emit("pytdx_5f_spool_skipped", symbol=symbol.symbol, output_path=str(output_path))
            return SpoolResult(
                symbol=symbol.symbol,
                bars=int(checkpoint.get("bars", 0)),
                pages=int(checkpoint.get("pages", 0)),
                oldest_ts=parse_optional_datetime(checkpoint.get("oldest_ts")),
                newest_ts=parse_optional_datetime(checkpoint.get("newest_ts")),
                exhausted=bool(checkpoint.get("exhausted", False)),
                output_path=output_path,
            )

    await write_checkpoint(checkpoint_path, {"status": "running", "symbol": symbol.symbol})
    offset = 0
    pages = 0
    bars_by_ts: dict[datetime, Bar] = {}
    exhausted = False
    try:
        while max_pages_per_symbol <= 0 or pages < max_pages_per_symbol:
            if hasattr(provider, "get_bars_page_with_raw_count"):
                bars, raw_rows_read = await provider.get_bars_page_with_raw_count(
                    symbol.symbol, timeframe, offset=offset, limit=page_size
                )
            else:
                bars = await provider.get_bars_page(
                    symbol.symbol, timeframe, offset=offset, limit=page_size
                )
                raw_rows_read = len(bars)
            if raw_rows_read == 0:
                exhausted = True
                break
            filtered = [bar for bar in bars if start_dt <= bar.ts <= end_dt]
            for bar in filtered:
                bars_by_ts[bar.ts] = bar
            pages += 1
            offset += raw_rows_read
            oldest = min((bar.ts for bar in bars), default=None)
            exhausted = raw_rows_read < page_size or (
                oldest is not None and oldest < start_dt
            )
            emit(
                "pytdx_5f_spool_page",
                symbol=symbol.symbol,
                page=pages,
                offset=offset,
                bars_read=len(bars),
                raw_rows_read=raw_rows_read,
                bars_kept=len(filtered),
                oldest=oldest.isoformat() if oldest else None,
                newest=max((bar.ts for bar in bars), default=None).isoformat() if bars else None,
                exhausted=exhausted,
            )
            if exhausted:
                break
            if sleep > 0:
                await asyncio.sleep(sleep)

        ordered = [bars_by_ts[key] for key in sorted(bars_by_ts)]
        if ordered:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            write_bars_parquet_atomic(output_path, ordered)
        elif output_path.exists():
            output_path.unlink()
        result = SpoolResult(
            symbol=symbol.symbol,
            bars=len(ordered),
            pages=pages,
            oldest_ts=ordered[0].ts if ordered else None,
            newest_ts=ordered[-1].ts if ordered else None,
            exhausted=exhausted,
            output_path=output_path if ordered else None,
        )
        await write_checkpoint(checkpoint_path, checkpoint_payload(result, status="success"))
        emit(
            "pytdx_5f_spool_symbol_finished",
            symbol=symbol.symbol,
            bars=result.bars,
            pages=result.pages,
            oldest_ts=result.oldest_ts.isoformat() if result.oldest_ts else None,
            newest_ts=result.newest_ts.isoformat() if result.newest_ts else None,
            output_path=str(result.output_path) if result.output_path else None,
        )
        return result
    except Exception as exc:
        await write_checkpoint(
            checkpoint_path,
            {"status": "failed", "symbol": symbol.symbol, "error": str(exc)[:2000]},
        )
        emit("pytdx_5f_spool_symbol_failed", symbol=symbol.symbol, error=str(exc)[:500])
        return SpoolResult(
            symbol=symbol.symbol,
            bars=0,
            pages=pages,
            oldest_ts=None,
            newest_ts=None,
            exhausted=False,
            output_path=None,
        )


def write_bars_parquet_atomic(path: Path, bars: Iterable[Bar]) -> None:
    rows = bars_to_parquet_rows(bars)
    parquet = importlib.import_module("pyarrow.parquet")
    pa = importlib.import_module("pyarrow")
    table = pa.Table.from_pylist(rows)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    parquet.write_table(table, temp_path, compression="zstd")
    temp_path.replace(path)


def bars_to_parquet_rows(bars: Iterable[Bar]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bar in bars:
        code, _exchange = bar.symbol.split(".", 1)
        rows.append(
            {
                "code": code,
                "trade_time": bar.ts.replace(tzinfo=None),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "vol": bar.volume,
                "amount": bar.amount,
            }
        )
    return rows


def symbol_output_file(output_root: Path, symbol: str) -> Path:
    code, exchange = symbol.split(".", 1)
    return output_root / "symbols" / exchange / f"{code}.parquet"


def checkpoint_file(output_root: Path, symbol: str) -> Path:
    code, exchange = symbol.split(".", 1)
    return output_root / "checkpoints" / exchange / f"{code}.json"


async def write_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {**payload, "updated_at": datetime.now(SHANGHAI_TZ).isoformat()}
    await asyncio.to_thread(path.write_text, json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")


def checkpoint_payload(result: SpoolResult, *, status: str) -> dict[str, Any]:
    return {
        "status": status,
        "symbol": result.symbol,
        "bars": result.bars,
        "pages": result.pages,
        "oldest_ts": result.oldest_ts.isoformat() if result.oldest_ts else None,
        "newest_ts": result.newest_ts.isoformat() if result.newest_ts else None,
        "exhausted": result.exhausted,
        "output_path": str(result.output_path) if result.output_path else None,
    }


def parse_boundary(value: str, *, end_of_day: bool) -> datetime:
    text = value.strip()
    if "T" in text or " " in text:
        parsed = datetime.fromisoformat(text)
    else:
        parsed_date = date.fromisoformat(text)
        parsed = datetime.combine(parsed_date, time.max if end_of_day else time.min)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def parse_optional_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
