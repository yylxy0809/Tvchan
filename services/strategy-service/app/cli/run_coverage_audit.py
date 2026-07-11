from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.config.strategy_params import StrategyParams
from app.db import create_pool
from app.domain.enums import MarketCapPolicy
from app.engine.coverage_audit import build_coverage_report, render_coverage_report_markdown, write_coverage_summary_csv


async def _run(args) -> int:
    pool = await create_pool()
    try:
        params = StrategyParams.default()
        if args.market_cap_policy:
            params = params.with_overrides(market_cap_policy=args.market_cap_policy)
        report = await build_coverage_report(pool, as_of_time=datetime.fromisoformat(args.as_of), params=params)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "coverage_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        (output_dir / "coverage_report.md").write_text(render_coverage_report_markdown(report), encoding="utf-8")
        write_coverage_summary_csv(report, str(output_dir / "coverage_summary.csv"))
        (output_dir / "coverage_perf.json").write_text(
            json.dumps(report["coverage_perf"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--market-cap-policy", choices=[item.value for item in MarketCapPolicy])
    parser.add_argument("--output", "--output-dir", dest="output_dir", default="outputs/coverage-audit")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
