from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import date
from typing import Sequence

from collector.storage.symbol_master_postgres import PostgresSymbolMasterStore
from collector.symbol_sources import discover_consensus_symbols, prepare_exchanges
from trading_protocol import SymbolInfo

DEFAULT_MIN_SYMBOL_COUNT = 5000
DEFAULT_SYMBOL_SOURCES = "sse,szse,bse,tencent"
DEFAULT_MIN_CONFIRMATIONS = 2


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh A-share symbol master from market data providers")
    parser.add_argument("--providers", default=os.getenv("SYMBOL_MASTER_PROVIDERS", DEFAULT_SYMBOL_SOURCES))
    parser.add_argument("--exchanges", default=os.getenv("SYMBOL_MASTER_EXCHANGES", "SH,SZ"))
    parser.add_argument(
        "--min-confirmations",
        type=int,
        default=int(os.getenv("SYMBOL_MASTER_MIN_CONFIRMATIONS", str(DEFAULT_MIN_CONFIRMATIONS))),
    )
    parser.add_argument("--min-symbol-count", type=int, default=int(os.getenv("SYMBOL_MASTER_MIN_COUNT", str(DEFAULT_MIN_SYMBOL_COUNT))))
    parser.add_argument("--skip-provider-refresh", action="store_true", help="Only run bar-date audit; do not discover or refresh provider symbols")
    parser.add_argument("--keep-missing-active", action="store_true", help="Do not deactivate DB symbols absent from provider discovery")
    parser.add_argument("--audit-bars-date", help="After refresh, deactivate active symbols without K-line bars on this YYYY-MM-DD date")
    parser.add_argument("--audit-bars-timeframe", default=os.getenv("SYMBOL_MASTER_AUDIT_TIMEFRAME", "5f"))
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("SYMBOL_MASTER_DRY_RUN") == "1")
    parser.add_argument("--tdx-host", default=os.getenv("TDX_HOST"))
    parser.add_argument("--tdx-port", type=int, default=int(os.getenv("TDX_PORT", "7709")))
    parser.add_argument("--tdx-timeout", type=int, default=int(os.getenv("TDX_TIMEOUT", "10")))
    parser.add_argument("--tdx-retries", type=int, default=int(os.getenv("TDX_RETRIES", "3")))
    parser.add_argument("--http-timeout", type=float, default=float(os.getenv("SYMBOL_MASTER_HTTP_TIMEOUT", "5")))
    parser.add_argument("--db-pool-min-size", type=int, default=int(os.getenv("SYMBOL_MASTER_DB_POOL_MIN_SIZE", "1")))
    parser.add_argument("--db-pool-max-size", type=int, default=int(os.getenv("SYMBOL_MASTER_DB_POOL_MAX_SIZE", "2")))
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
        ),
    )
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.skip_provider_refresh and not args.audit_bars_date:
        raise RuntimeError("--skip-provider-refresh requires --audit-bars-date")
    async with PostgresSymbolMasterStore(
        args.database_url,
        pool_min_size=max(1, args.db_pool_min_size),
        pool_max_size=max(max(1, args.db_pool_min_size), args.db_pool_max_size),
    ) as store:
        if not args.skip_provider_refresh:
            exchanges = prepare_exchanges(args.exchanges)
            discovery = await discover_consensus_symbols(
                sources=parse_csv(args.providers),
                exchanges=exchanges,
                min_confirmations=max(1, args.min_confirmations),
                timeout=args.http_timeout,
                tdx_host=args.tdx_host,
                tdx_port=args.tdx_port,
                tdx_timeout=args.tdx_timeout,
                tdx_retries=args.tdx_retries,
            )
            symbols = prepare_symbol_master_symbols(discovery.symbols, exchanges=exchanges)
            emit(
                "symbol_master_consensus_discovered",
                providers=args.providers,
                exchanges=sorted(exchanges),
                symbols=len(symbols),
                candidate_count=discovery.candidate_count,
                min_confirmations=discovery.min_confirmations,
                source_counts=discovery.source_counts,
                confirmation_counts=discovery.confirmation_counts,
                source_errors=discovery.source_errors,
                min_symbol_count=args.min_symbol_count,
                dry_run=args.dry_run,
            )
            ensure_symbol_count_safe(symbols, min_symbol_count=args.min_symbol_count)
            refresh = await store.refresh_provider_symbols(
                symbols,
                deactivate_missing=not args.keep_missing_active,
                dry_run=args.dry_run,
            )
            emit("symbol_master_refresh_finished", **refresh.__dict__)
        if args.audit_bars_date:
            audit_date = date.fromisoformat(args.audit_bars_date)
            audit = await store.deactivate_symbols_without_bars_on_date(
                target_date=audit_date,
                timeframe=args.audit_bars_timeframe,
                dry_run=args.dry_run,
            )
            emit(
                "symbol_master_bar_audit_finished",
                target_date=audit.target_date.isoformat(),
                timeframe=audit.timeframe,
                active_before=audit.active_before,
                would_deactivate=audit.would_deactivate,
                deactivated=audit.deactivated,
                dry_run=audit.dry_run,
            )


def parse_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def prepare_symbol_master_symbols(
    symbols: list[SymbolInfo],
    *,
    exchanges: set[str] | None = None,
) -> list[SymbolInfo]:
    allowed_exchanges = exchanges or {"SH", "SZ"}
    deduped: dict[str, SymbolInfo] = {}
    for symbol in symbols:
        normalized_symbol = symbol.symbol.strip().upper()
        code = symbol.code.strip()
        exchange = symbol.exchange.strip().upper()
        if not normalized_symbol or not code or exchange not in allowed_exchanges:
            continue
        if symbol.asset_type != "stock" or symbol.market != "A_SHARE":
            continue
        deduped[normalized_symbol] = SymbolInfo(
            symbol=normalized_symbol,
            code=code,
            exchange=exchange,
            name=(symbol.name or normalized_symbol).strip(),
            asset_type="stock",
            market="A_SHARE",
            is_active=True,
        )
    return [deduped[key] for key in sorted(deduped)]


def ensure_symbol_count_safe(symbols: list[SymbolInfo], *, min_symbol_count: int) -> None:
    if len(symbols) < min_symbol_count:
        raise RuntimeError(
            f"Refusing to refresh symbol master with only {len(symbols)} symbols; "
            f"minimum is {min_symbol_count}. Check provider connectivity before retrying."
        )


def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
