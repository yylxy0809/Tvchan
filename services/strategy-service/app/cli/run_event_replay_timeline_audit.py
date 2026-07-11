from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from app.config.strategy_params import StrategyParams
from app.db import create_pool
from app.engine.event_replay_timeline_audit import (
    build_event_replay_timeline_audit,
    write_event_replay_timeline_audit,
)
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args) -> int:
    pool = await create_pool()
    try:
        params = StrategyParams.from_strategy_code(args.strategy)
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        requested_symbols = list(args.symbols or [])
        if args.symbol:
            requested_symbols.append(args.symbol)
        symbols = await module_c_repo.list_active_symbols(limit=args.limit, symbols=requested_symbols or None)
        payload = await build_event_replay_timeline_audit(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            params=params,
            symbols=symbols,
            start_time=datetime.fromisoformat(args.start),
            end_time=datetime.fromisoformat(args.end),
        )
        write_event_replay_timeline_audit(Path(args.output_dir), payload)
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="strict_explicit_b1")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--symbol")
    parser.add_argument("--output", "--output-dir", dest="output_dir", default="outputs/event-replay-timeline-audit")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
