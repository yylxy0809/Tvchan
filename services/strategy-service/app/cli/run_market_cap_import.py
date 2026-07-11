from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import date
from pathlib import Path

from app.db import create_pool


async def _run(args) -> int:
    csv_path = Path(args.csv)
    rows = list(csv.DictReader(csv_path.open("r", encoding="utf-8-sig", newline=""), delimiter=args.delimiter))
    payload = []
    skipped = []
    for row in rows:
        code = (row.get("code") or "").strip()
        exchange = (row.get("exchange") or "").strip().upper()
        market_cap_raw = (row.get("market_cap") or "").strip()
        if not code or not exchange or not market_cap_raw:
            skipped.append({"row": row, "reason": "missing required field"})
            continue
        try:
            market_cap = float(market_cap_raw)
        except ValueError:
            skipped.append({"row": row, "reason": "invalid market_cap"})
            continue
        payload.append((code, exchange, int(round(market_cap * 100))))
    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            symbol_rows = await conn.fetch(
                """
                select id, code, exchange
                from symbols
                where is_active = true
                """
            )
            symbol_map = {(row["code"], row["exchange"]): row["id"] for row in symbol_rows}
            upserts = []
            unresolved = []
            for code, exchange, market_cap_x100 in payload:
                symbol_id = symbol_map.get((code, exchange))
                if symbol_id is None:
                    unresolved.append({"code": code, "exchange": exchange})
                    continue
                upserts.append((symbol_id, market_cap_x100, args.source, date.fromisoformat(args.as_of)))
            await conn.executemany(
                """
                insert into symbol_fundamentals (
                    symbol_id, market_cap_x100, source, as_of_date, updated_at
                )
                values ($1, $2, $3, $4, now())
                on conflict (symbol_id)
                do update
                set market_cap_x100 = excluded.market_cap_x100,
                    source = excluded.source,
                    as_of_date = excluded.as_of_date,
                    updated_at = now()
                """,
                upserts,
            )
        summary = {
            "csv": str(csv_path),
            "rows_total": len(rows),
            "rows_valid": len(payload),
            "rows_upserted": len(upserts),
            "rows_unresolved": len(unresolved),
            "rows_skipped": len(skipped),
            "unresolved": unresolved[:50],
            "skipped": skipped[:50],
        }
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--source", default="csv_import")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--output", default="outputs/market-cap-import-summary.json")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
