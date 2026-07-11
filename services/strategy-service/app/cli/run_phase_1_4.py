from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from app.config.strategy_params import PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE, StrategyParams
from app.db import create_pool
from app.engine.phase_1_4 import (
    build_weekly_context_compare,
    run_historical_backtest,
    write_phase_1_4_outputs,
)
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def _run(args) -> int:
    pool = await create_pool(max_size=max(6, args.concurrency + 2))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        requested_symbols = list(args.symbols or [])
        if args.symbol:
            requested_symbols.append(args.symbol)
        symbols = await module_c_repo.list_active_symbols(limit=args.limit or None, symbols=requested_symbols or None)
        as_of_time = datetime.fromisoformat(args.as_of or args.end)
        start_time = datetime.fromisoformat(args.start)
        end_time = datetime.fromisoformat(args.end)
        weekly_context_compare = await build_weekly_context_compare(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            as_of_time=as_of_time,
            concurrency=max(1, args.concurrency),
        )
        params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
        historical_backtest = await run_historical_backtest(
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            symbols=symbols,
            params=params,
            start_time=start_time,
            end_time=end_time,
            concurrency=max(1, args.concurrency),
        )
        write_phase_1_4_outputs(
            Path(args.output_dir),
            weekly_context_compare=weekly_context_compare,
            historical_backtest=historical_backtest,
            start_time=start_time,
            end_time=end_time,
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--as-of")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--symbol")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--output-dir", default="outputs/phase-1-4")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
