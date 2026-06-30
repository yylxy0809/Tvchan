from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime

from collector.providers.pytdx_provider import PytdxProvider
from collector.providers.seed import SeedProvider
from collector.storage.postgres import PostgresKlineWriter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 1 backfill probe")
    parser.add_argument("--provider", default="seed", choices=["seed", "pytdx"])
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument(
        "--tdx-timeout",
        type=int,
        default=int(os.getenv("TDX_TIMEOUT", "10")),
        help="pytdx quote-server handshake timeout in seconds",
    )
    parser.add_argument(
        "--tdx-retries",
        type=int,
        default=int(os.getenv("TDX_RETRIES", "3")),
        help="pytdx connection attempts per quote server",
    )
    parser.add_argument(
        "--probe-servers",
        action="store_true",
        help="Probe pytdx quote servers and print connectivity results.",
    )
    parser.add_argument("--symbols", help="Comma separated symbols")
    parser.add_argument(
        "--timeframes",
        default="5f,15f,30f,1h,1d,1w,1m",
        help="Comma separated timeframes",
    )
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--write-db", action="store_true", help="Write bars to PostgreSQL")
    parser.add_argument(
        "--replace-db",
        action="store_true",
        help="Delete selected symbol/timeframe bars before writing. Requires --write-db.",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@localhost:5432/tradingview_local",
        ),
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    provider = (
        PytdxProvider(
            host=args.tdx_host,
            port=args.tdx_port,
            timeout=args.tdx_timeout,
            retries=args.tdx_retries,
        )
        if args.provider == "pytdx"
        else SeedProvider()
    )
    if args.probe_servers:
        if not isinstance(provider, PytdxProvider):
            raise SystemExit("--probe-servers requires --provider pytdx")
        results = await provider.probe_servers()
        print(json.dumps({"provider": provider.name, "servers": results}, ensure_ascii=False))
        return
    if not args.symbols:
        raise SystemExit("--symbols is required unless --probe-servers is used")
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    timeframes = [item.strip() for item in args.timeframes.split(",") if item.strip()]
    provider_symbols = await provider.list_symbols()
    selected_symbols = [item for item in provider_symbols if item.symbol in set(symbols)]

    if args.write_db:
        async with PostgresKlineWriter(args.database_url) as writer:
            symbol_count = await writer.upsert_symbols(selected_symbols)
            deleted_count = 0
            if args.replace_db:
                deleted_count = await writer.delete_bars(symbols, timeframes)
            bar_count = 0
            for symbol in symbols:
                for timeframe in timeframes:
                    bars = await provider.get_bars(symbol, timeframe, limit=args.limit)
                    bar_count += await writer.upsert_bars(bars)
            print(
                json.dumps(
                    {
                        "provider": provider.name,
                        "symbols": symbol_count,
                        "deleted_bars": deleted_count,
                        "bars": bar_count,
                        "timeframes": timeframes,
                        "database": "written",
                    },
                    ensure_ascii=False,
                )
            )
        return

    for symbol in symbols:
        for timeframe in timeframes:
            bars = await provider.get_bars(symbol, timeframe, limit=args.limit)
            for bar in bars:
                payload = bar.as_api_dict()
                payload["symbol"] = symbol
                payload["timeframe"] = timeframe
                payload["iso_time"] = datetime.fromtimestamp(payload["time"]).isoformat()
                print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
