from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from app.db import create_pool


async def _run(args) -> int:
    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            inserted = await conn.fetch(
                """
                with latest_history as (
                    select distinct on (symbol_id, chan_level, mode, base_timeframe)
                        symbol_id, chan_level, mode, base_timeframe,
                        new_run_id, new_base_to_bar_end
                    from scheme2_chan_c_published_head_history
                    order by symbol_id, chan_level, mode, base_timeframe, observed_at desc, id desc
                ),
                changed as (
                    select
                        h.symbol_id,
                        h.chan_level,
                        h.mode,
                        h.base_timeframe,
                        lh.new_run_id as old_run_id,
                        h.run_id as new_run_id,
                        lh.new_base_to_bar_end as old_base_to_bar_end,
                        h.base_to_bar_end as new_base_to_bar_end,
                        h.snapshot_version,
                        $1::text as source
                    from scheme2_chan_c_published_heads h
                    left join latest_history lh
                      on lh.symbol_id = h.symbol_id
                     and lh.chan_level = h.chan_level
                     and lh.mode = h.mode
                     and lh.base_timeframe = h.base_timeframe
                    where h.status = 'published'
                      and (lh.new_run_id is null or lh.new_run_id <> h.run_id or lh.new_base_to_bar_end is distinct from h.base_to_bar_end)
                )
                insert into scheme2_chan_c_published_head_history (
                    symbol_id, chan_level, mode, base_timeframe,
                    old_run_id, new_run_id, old_base_to_bar_end, new_base_to_bar_end,
                    snapshot_version, source
                )
                select
                    symbol_id, chan_level, mode, base_timeframe,
                    old_run_id, new_run_id, old_base_to_bar_end, new_base_to_bar_end,
                    snapshot_version, source
                from changed
                returning symbol_id, chan_level, mode, new_run_id, new_base_to_bar_end
                """,
                args.source,
            )
        summary = {
            "source": args.source,
            "inserted": len(inserted),
            "samples": [
                {
                    "symbol_id": row["symbol_id"],
                    "chan_level": row["chan_level"],
                    "mode": row["mode"],
                    "new_run_id": row["new_run_id"],
                    "new_base_to_bar_end": row["new_base_to_bar_end"].isoformat() if row["new_base_to_bar_end"] else None,
                }
                for row in inserted[:50]
            ],
        }
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="strategy_observer")
    parser.add_argument("--output", default="outputs/head-history-observer-summary.json")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
