from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from typing import Any

from collector.providers.factory import ProviderFactoryConfig, create_market_provider, parse_provider_names
from collector.providers.pytdx_provider import PytdxProvider
from collector.realtime_publisher import publish_bar_update
from collector.storage.postgres import PostgresKlineWriter
from trading_protocol import Bar, SymbolInfo, normalize_timeframe

DEFAULT_TIMEFRAMES = "5f,15f,30f,1h,1d,1w,1m"
DEFAULT_MODES = "confirmed,predictive"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market K-line fill worker")
    parser.add_argument(
        "--provider",
        default=os.getenv("MARKET_FILL_PROVIDER", "pytdx"),
        help="Single provider or comma-separated pool providers: seed,pytdx,mootdx,tencent,baidu.",
    )
    parser.add_argument("--symbols", default=os.getenv("MARKET_FILL_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("MARKET_FILL_SYMBOL_LIMIT", "10")),
        help="Maximum symbols per pass when --symbols is omitted. Use 0 for all provider symbols.",
    )
    parser.add_argument(
        "--symbols-from-db",
        action="store_true",
        default=os.getenv("MARKET_FILL_SYMBOLS_FROM_DB") == "1",
        help="When --symbols is omitted, use active symbols from the local symbol master instead of provider listing.",
    )
    parser.add_argument("--timeframes", default=os.getenv("MARKET_FILL_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument("--limit", type=int, default=int(os.getenv("MARKET_FILL_BAR_LIMIT", "300")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("MARKET_FILL_CONCURRENCY", "1")))
    parser.add_argument("--sleep", type=float, default=float(os.getenv("MARKET_FILL_SLEEP", "0.25")))
    parser.add_argument("--loop", action="store_true", default=os.getenv("MARKET_FILL_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("MARKET_FILL_LOOP_INTERVAL", "60")),
    )
    parser.add_argument("--skip-publish", action="store_true", default=os.getenv("MARKET_FILL_SKIP_PUBLISH") == "1")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("MARKET_FILL_DRY_RUN") == "1")
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
    parser.add_argument("--source-policy", default=os.getenv("MARKET_SOURCE_POLICY", "primary_failover"))
    parser.add_argument("--http-timeout", type=float, default=float(os.getenv("MARKET_HTTP_TIMEOUT", "5")))
    parser.add_argument("--pool-timeout", type=float, default=float(os.getenv("MARKET_POOL_TIMEOUT", "8")))
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
    provider = create_provider(args)
    timeframes = parse_timeframes(args.timeframes)
    if args.symbols_from_db and not args.symbols:
        symbols = await select_database_symbols(args.database_url, args.symbol_limit)
    else:
        symbols = await select_symbols(provider, args.symbols, args.symbol_limit)
    emit(
        "pass_started",
        provider=provider.name,
        symbols=len(symbols),
        timeframes=timeframes,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        for symbol in symbols:
            emit("dry_symbol", symbol=symbol.symbol)
        emit("pass_finished", bars=0)
        return

    async with PostgresKlineWriter(args.database_url) as kline_writer:
        if not args.symbols_from_db or args.symbols:
            await kline_writer.upsert_symbols(symbols)
        total_bars = 0
        if args.concurrency > 1:
            total_bars = await fill_bars_concurrently(
                    provider=provider,
                    kline_writer=kline_writer,
                    symbols=symbols,
                    timeframes=timeframes,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    sleep=args.sleep,
                    skip_publish=args.skip_publish,
                    redis_url=args.redis_url,
            )
        else:
            for symbol in symbols:
                for timeframe in timeframes:
                    total_bars += await fill_one_timeframe(
                            provider=provider,
                            kline_writer=kline_writer,
                            symbol=symbol,
                            timeframe=timeframe,
                            limit=args.limit,
                            sleep=args.sleep,
                            skip_publish=args.skip_publish,
                            redis_url=args.redis_url,
                    )

        emit("pass_finished", bars=total_bars)


def create_provider(args: argparse.Namespace):
    return create_market_provider(
        ProviderFactoryConfig(
            names=parse_provider_names(args.provider),
            tdx_host=args.tdx_host,
            tdx_port=args.tdx_port,
            tdx_timeout=args.tdx_timeout,
            tdx_retries=args.tdx_retries,
            http_timeout=args.http_timeout,
            pool_policy=args.source_policy,
            pool_timeout_seconds=args.pool_timeout,
        )
    )


async def select_symbols(provider, symbols: str | None, symbol_limit: int) -> list[SymbolInfo]:
    if symbols:
        requested = [normalize_symbol(value) for value in parse_csv(symbols)]
        return [symbol_info_from_symbol(item) for item in sorted(set(requested))]

    if not symbols and isinstance(provider, PytdxProvider) and symbol_limit > 0:
        provider_symbols = await provider.list_symbols(limit=symbol_limit)
    else:
        provider_symbols = await provider.list_symbols()
    if symbol_limit <= 0:
        return provider_symbols
    return provider_symbols[:symbol_limit]


async def select_database_symbols(database_url: str, symbol_limit: int) -> list[SymbolInfo]:
    import asyncpg

    limit_value = None if symbol_limit <= 0 else symbol_limit
    conn = await asyncpg.connect(database_url)
    try:
        rows = await conn.fetch(
            """
            select code, exchange, name, asset_type, market, is_active
            from symbols
            where is_active = true
            order by code, exchange
            limit coalesce($1::int, 2147483647)
            """,
            limit_value,
        )
    finally:
        await conn.close()
    return [
        SymbolInfo(
            symbol=f"{row['code']}.{row['exchange']}",
            code=row["code"],
            exchange=row["exchange"],
            name=row["name"],
            asset_type=row["asset_type"],
            market=row["market"],
            is_active=row["is_active"],
        )
        for row in rows
    ]


async def fill_bars_concurrently(
    *,
    provider,
    kline_writer: PostgresKlineWriter,
    symbols: list[SymbolInfo],
    timeframes: list[str],
    limit: int,
    concurrency: int,
    sleep: float,
    skip_publish: bool,
    redis_url: str,
) -> int:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_task(symbol: SymbolInfo, timeframe: str) -> int:
        async with semaphore:
            return await fill_one_timeframe(
                provider=provider,
                kline_writer=kline_writer,
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
                sleep=sleep,
                skip_publish=skip_publish,
                redis_url=redis_url,
            )

    results = await asyncio.gather(
        *(run_task(symbol, timeframe) for symbol in symbols for timeframe in timeframes)
    )
    return sum(results)


async def fill_one_timeframe(
    *,
    provider,
    kline_writer: PostgresKlineWriter,
    symbol: SymbolInfo,
    timeframe: str,
    limit: int,
    sleep: float,
    skip_publish: bool,
    redis_url: str,
) -> int:
    try:
        bars = await provider.get_bars(symbol.symbol, timeframe, limit=limit)
    except Exception as exc:
        emit(
            "bars_failed",
            symbol=symbol.symbol,
            timeframe=timeframe,
            error=str(exc)[:500],
        )
        await sleep_between_requests(sleep)
        return 0
    written = await kline_writer.upsert_bars(bars)
    emit("bars_written", symbol=symbol.symbol, timeframe=timeframe, bars=len(bars))
    if bars and not skip_publish:
        published = await publish_bar_update(
            redis_url=redis_url,
            symbol=symbol.symbol,
            timeframe=timeframe,
            bar=bars[-1],
        )
        emit(
            "bar_published",
            symbol=symbol.symbol,
            timeframe=timeframe,
            published=published,
        )
    await sleep_between_requests(sleep)
    return written


def symbol_info_from_symbol(symbol: str) -> SymbolInfo:
    code, exchange = symbol.split(".", 1)
    return SymbolInfo(symbol=symbol, code=code, exchange=exchange, name=symbol)


def normalize_symbol(value: str) -> str:
    normalized = value.strip().upper()
    if "." in normalized:
        return normalized
    exchange = "SH" if normalized.startswith("6") else "SZ"
    return f"{normalized}.{exchange}"


def parse_timeframes(value: str) -> list[str]:
    return [normalize_timeframe(item) for item in parse_csv(value)]


def parse_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def filter_chan_response_level(response: dict[str, Any], level: str) -> dict[str, Any]:
    return {
        **response,
        "timeframe": level,
        "strokes": [item for item in response.get("strokes", []) if item.get("level") == level],
        "segments": [item for item in response.get("segments", []) if item.get("level") == level],
        "centers": [item for item in response.get("centers", []) if item.get("level") == level],
        "signals": [item for item in response.get("signals", []) if item.get("level") == level],
    }


def bar_to_chan_payload(bar: Bar) -> dict[str, Any]:
    return {
        "time": int(bar.ts.timestamp()),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }


async def sleep_between_requests(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


def emit(event: str, **payload: Any) -> None:
    payload["event"] = event
    payload["time"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
