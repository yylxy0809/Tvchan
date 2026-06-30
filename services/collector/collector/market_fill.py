from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from typing import Any

import httpx

from collector.providers.pytdx_provider import PytdxProvider
from collector.providers.seed import SeedProvider
from collector.realtime_publisher import publish_bar_update
from collector.storage.chan_postgres import PostgresChanWriter
from collector.storage.postgres import PostgresKlineWriter
from trading_protocol import Bar, SymbolInfo, normalize_timeframe

DEFAULT_TIMEFRAMES = "5f,15f,30f,1h,1d,1w,1m"
DEFAULT_CHAN_LEVELS = "5f,30f,1d"
DEFAULT_MODES = "confirmed,predictive"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market K-line fill and Chan precompute worker")
    parser.add_argument("--provider", default=os.getenv("MARKET_FILL_PROVIDER", "pytdx"), choices=["seed", "pytdx"])
    parser.add_argument("--symbols", default=os.getenv("MARKET_FILL_SYMBOLS"))
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=int(os.getenv("MARKET_FILL_SYMBOL_LIMIT", "10")),
        help="Maximum symbols per pass when --symbols is omitted. Use 0 for all provider symbols.",
    )
    parser.add_argument("--timeframes", default=os.getenv("MARKET_FILL_TIMEFRAMES", DEFAULT_TIMEFRAMES))
    parser.add_argument("--limit", type=int, default=int(os.getenv("MARKET_FILL_BAR_LIMIT", "300")))
    parser.add_argument("--sleep", type=float, default=float(os.getenv("MARKET_FILL_SLEEP", "0.25")))
    parser.add_argument("--loop", action="store_true", default=os.getenv("MARKET_FILL_LOOP") == "1")
    parser.add_argument(
        "--loop-interval",
        type=float,
        default=float(os.getenv("MARKET_FILL_LOOP_INTERVAL", "60")),
    )
    parser.add_argument("--skip-chan", action="store_true", default=os.getenv("MARKET_FILL_SKIP_CHAN") == "1")
    parser.add_argument("--skip-publish", action="store_true", default=os.getenv("MARKET_FILL_SKIP_PUBLISH") == "1")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--chan-levels", default=os.getenv("MARKET_FILL_CHAN_LEVELS", DEFAULT_CHAN_LEVELS))
    parser.add_argument("--modes", default=os.getenv("MARKET_FILL_MODES", DEFAULT_MODES))
    parser.add_argument("--chan-service-url", default=os.getenv("CHAN_SERVICE_URL", "http://127.0.0.1:8002"))
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("MARKET_FILL_DRY_RUN") == "1")
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
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
    chan_levels = parse_timeframes(args.chan_levels)
    modes = parse_csv(args.modes)
    symbols = await select_symbols(provider, args.symbols, args.symbol_limit)
    emit(
        "pass_started",
        provider=provider.name,
        symbols=len(symbols),
        timeframes=timeframes,
        chan_levels=[] if args.skip_chan else chan_levels,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        for symbol in symbols:
            emit("dry_symbol", symbol=symbol.symbol)
        emit("pass_finished", bars=0, chan_runs=0)
        return

    async with PostgresKlineWriter(args.database_url) as kline_writer:
        chan_writer = None
        if not args.skip_chan:
            chan_writer = PostgresChanWriter(args.database_url)
            await chan_writer.__aenter__()
        try:
            await kline_writer.upsert_symbols(symbols)
            total_bars = 0
            total_chan_runs = 0
            for symbol in symbols:
                for timeframe in timeframes:
                    bars = await provider.get_bars(symbol.symbol, timeframe, limit=args.limit)
                    total_bars += await kline_writer.upsert_bars(bars)
                    emit("bars_written", symbol=symbol.symbol, timeframe=timeframe, bars=len(bars))
                    if bars and not args.skip_publish:
                        published = await publish_bar_update(
                            redis_url=args.redis_url,
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
                    await sleep_between_requests(args.sleep)

                if chan_writer is not None:
                    analysis_bars = await kline_writer.get_bars(symbol.symbol, "5f")
                    if analysis_bars:
                        response = await analyze_chan(
                            args.chan_service_url,
                            symbol.symbol,
                            "5f",
                            modes,
                            analysis_bars,
                            chan_levels=chan_levels,
                        )
                        for level in chan_levels:
                            level_response = filter_chan_response_level(response, level)
                            counts = await chan_writer.replace_analysis(
                                symbol=symbol.symbol,
                                level=level,
                                modes=modes,
                                bar_from=analysis_bars[0].ts,
                                bar_until=analysis_bars[-1].ts,
                                bar_count=len(analysis_bars),
                                response=level_response,
                            )
                            total_chan_runs += 1
                            emit(
                                "chan_written",
                                symbol=symbol.symbol,
                                level=level,
                                engine=response.get("engine"),
                                input_bars=len(analysis_bars),
                                **counts,
                            )
                        await sleep_between_requests(args.sleep)

            emit("pass_finished", bars=total_bars, chan_runs=total_chan_runs)
        finally:
            if chan_writer is not None:
                await chan_writer.__aexit__(None, None, None)


def create_provider(args: argparse.Namespace):
    if args.provider == "seed":
        return SeedProvider()
    return PytdxProvider(
        host=args.tdx_host,
        port=args.tdx_port,
        timeout=args.tdx_timeout,
        retries=args.tdx_retries,
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


async def analyze_chan(
    base_url: str,
    symbol: str,
    level: str,
    modes: list[str],
    bars: list[Bar],
    timeout: float | None = None,
    chan_levels: list[str] | None = None,
) -> dict[str, Any]:
    payload = {
        "symbol": symbol,
        "timeframe": level,
        "chan_levels": chan_levels or [level],
        "modes": modes,
        "bars": [bar_to_chan_payload(bar) for bar in bars],
    }
    request_timeout = timeout
    if request_timeout is None:
        request_timeout = float(os.getenv("CHAN_ANALYZE_TIMEOUT", "120"))
    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=request_timeout, trust_env=False) as client:
        response = await client.post("/analyze", json=payload)
        response.raise_for_status()
        return response.json()


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
