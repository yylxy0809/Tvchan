"""Populate the compact weekly/monthly chart read model for explicit symbols.

`klines` remains authoritative. This tool only upserts rows into
`chart_period_bars` and never deletes or rewrites source K lines.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import asyncpg


ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "services" / "api"), str(ROOT / "libs" / "protocol" / "python")]

from app.repositories.postgres import get_bars_db, split_symbol  # noqa: E402


UPSERT_SQL = """
insert into chart_period_bars (
    symbol_id, timeframe, ts, open_x1000, high_x1000, low_x1000,
    close_x1000, volume, amount_x100, is_complete, revision, refreshed_at
) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
on conflict (symbol_id, timeframe, ts) do update set
    open_x1000 = excluded.open_x1000,
    high_x1000 = excluded.high_x1000,
    low_x1000 = excluded.low_x1000,
    close_x1000 = excluded.close_x1000,
    volume = excluded.volume,
    amount_x100 = excluded.amount_x100,
    is_complete = excluded.is_complete,
    revision = excluded.revision,
    refreshed_at = excluded.refreshed_at
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", nargs="+", help="Explicit symbols, for example 000001.SZ 600000.SH")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="PostgreSQL URL; defaults to DATABASE_URL",
    )
    return parser.parse_args()


async def backfill_symbol(pool: asyncpg.Pool, symbol: str) -> tuple[str, int] | None:
    code, exchange = split_symbol(symbol)
    symbol_id = await pool.fetchval(
        "select id from symbols where code = $1 and exchange = $2 and is_active = true",
        code,
        exchange,
    )
    if symbol_id is None:
        return None

    rows_to_write: list[tuple] = []
    for timeframe, timeframe_code in (("1w", 10080), ("1m", 43200)):
        # The chart contract caps its bounded viewport at 300 bars. Keeping the
        # compact projection to that same tail makes the write bounded and
        # avoids an unnecessary full-history hypertable traversal.
        bars = await get_bars_db(pool, symbol, timeframe, None, None, 300)
        for bar in bars:
            rows_to_write.append(
                (
                    symbol_id,
                    timeframe_code,
                    datetime.fromtimestamp(int(bar["time"]), tz=UTC),
                    round(float(bar["open"]) * 1000),
                    round(float(bar["high"]) * 1000),
                    round(float(bar["low"]) * 1000),
                    round(float(bar["close"]) * 1000),
                    int(bar["volume"]),
                    None if bar.get("amount") is None else round(float(bar["amount"]) * 100),
                    bool(bar.get("complete", True)),
                    int(bar.get("revision", 0)),
                )
            )
    if rows_to_write:
        await pool.executemany(UPSERT_SQL, rows_to_write)
    return symbol.upper(), len(rows_to_write)


async def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("--database-url or DATABASE_URL is required")
    pool = await asyncpg.create_pool(args.database_url, min_size=1, max_size=1)
    try:
        for raw_symbol in args.symbols:
            requested_symbol = raw_symbol.strip().upper()
            result = await backfill_symbol(pool, requested_symbol)
            if result is None:
                print(f"{requested_symbol}: skipped (not an active symbol)")
                continue
            symbol, written = result
            print(f"{symbol}: upserted {written} chart period bars")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
