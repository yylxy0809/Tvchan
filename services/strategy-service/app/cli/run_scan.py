from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.config.strategy_params import (
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
    StrategyParams,
)
from app.db import create_pool
from app.domain.enums import MarketCapPolicy
from app.engine.diagnostic_reporting import (
    build_candidate_samples,
    build_fail_samples,
    build_gate_waterfall,
    build_status_samples,
    diagnosis_to_dict,
    render_gate_waterfall_markdown,
    render_trace_markdown,
)
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.engine.strategy_runner import StrategyRunner
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository
from app.repositories.strategy_repo import StrategyRepository


def _strategy_meta(strategy_code: str) -> tuple[str, str]:
    if strategy_code == STRICT_EXPLICIT_B1_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Strict", "Diagnostic baseline with explicit prior weekly B1"
    if strategy_code == DIAG_TRUST_B2_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Diag Trust B2", "Diagnostic mode that trusts weekly B2 without explicit prior B1"
    if strategy_code == DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Diag Trust B2/B2s", "Diagnostic mode that trusts weekly B2 or B2s without explicit prior B1"
    if strategy_code == DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Diag Same-Bar B1/B2s", "Diagnostic mode for same-bar weekly B1 and B2s semantics"
    if strategy_code == SANITY_LOOSE_STRATEGY_CODE:
        return "Weekly-Daily B2 Resonance Sanity Loose", "Loose diagnostic variant from phase 1.2"
    return "Weekly-Daily B2 Resonance", "Module C weekly B2 plus daily setup plus 30F/5F entry confirmation"


async def _run(args) -> int:
    pool = await create_pool()
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        strategy_repo = StrategyRepository(pool)
        params = StrategyParams.from_strategy_code(args.strategy)
        if args.market_cap_policy:
            params = params.with_overrides(market_cap_policy=args.market_cap_policy)
        strategy_name, description = _strategy_meta(params.strategy_code)
        await strategy_repo.ensure_default_definition(
            strategy_code=params.strategy_code,
            version=params.strategy_version,
            strategy_name=strategy_name,
            description=description,
            rule_spec_json=params.raw,
        )
        runner = StrategyRunner(module_c_repo, kline_repo)
        diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)
        requested_symbols = list(args.symbols or [])
        if args.symbol:
            requested_symbols.append(args.symbol)
        symbols = await module_c_repo.list_active_symbols(limit=args.limit, symbols=requested_symbols or None)
        as_of_time = datetime.fromisoformat(args.as_of)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if args.diagnose or args.trace:
            diagnoses = []
            for symbol in symbols:
                diagnoses.append(await diagnoser.diagnose_symbol(symbol, as_of_time=as_of_time, params=params))
            (output_dir / "diagnoses.json").write_text(
                json.dumps([diagnosis_to_dict(item) for item in diagnoses], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            if args.diagnose:
                waterfall = build_gate_waterfall(diagnoses)
                (output_dir / "gate_waterfall.json").write_text(
                    json.dumps(waterfall, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                (output_dir / "gate_waterfall.md").write_text(
                    render_gate_waterfall_markdown(waterfall),
                    encoding="utf-8",
                )
                fail_samples = build_fail_samples(diagnoses)
                (output_dir / "fail_samples.jsonl").write_text(
                    "\n".join(json.dumps(item, ensure_ascii=False) for item in fail_samples) + ("\n" if fail_samples else ""),
                    encoding="utf-8",
                )
                candidate_samples = build_candidate_samples(diagnoses)
                (output_dir / "candidate_samples.jsonl").write_text(
                    "\n".join(json.dumps(item, ensure_ascii=False) for item in candidate_samples) + ("\n" if candidate_samples else ""),
                    encoding="utf-8",
                )
                entry_watch_samples = build_status_samples(diagnoses, status="watch")
                (output_dir / "entry_watch_samples.jsonl").write_text(
                    "\n".join(json.dumps(item, ensure_ascii=False) for item in entry_watch_samples) + ("\n" if entry_watch_samples else ""),
                    encoding="utf-8",
                )
                trigger_samples = build_status_samples(diagnoses, status="trigger")
                (output_dir / "trigger_samples.jsonl").write_text(
                    "\n".join(json.dumps(item, ensure_ascii=False) for item in trigger_samples) + ("\n" if trigger_samples else ""),
                    encoding="utf-8",
                )
            if args.trace:
                for diagnosis in diagnoses:
                    trace_dir = output_dir / diagnosis.symbol.symbol
                    trace_dir.mkdir(parents=True, exist_ok=True)
                    (trace_dir / "trace.json").write_text(
                        json.dumps(diagnosis_to_dict(diagnosis), ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    (trace_dir / "trace.md").write_text(render_trace_markdown(diagnosis), encoding="utf-8")
            return 0

        results = []
        for symbol in symbols:
            result = await runner.evaluate_symbol(symbol, as_of_time=as_of_time, params=params)
            if result is None:
                continue
            await strategy_repo.insert_scan_result(
                result,
                strategy_code=params.strategy_code,
                strategy_version=params.strategy_version,
            )
            results.append(
                {
                    "symbol": symbol.symbol,
                    "name": symbol.name,
                    "status": result.status.value,
                    "strength_score": result.daily_setup.strength_score,
                    "confidence_score": result.entry.confidence_score,
                    "entry_level": result.entry.entry_level,
                }
            )
        (output_dir / "scan-results.json").write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        markdown = [
            "# Scan Results",
            "",
            f"- Strategy: `{params.strategy_code}`",
            f"- As of: `{as_of_time.isoformat()}`",
            f"- Count: `{len(results)}`",
            "",
        ]
        for item in results:
            markdown.append(
                f"- `{item['symbol']}` `{item['name']}` `{item['status']}` strength=`{item['strength_score']:.2f}` confidence=`{item['confidence_score']:.2f}` level=`{item['entry_level']}`"
            )
        (output_dir / "scan-results.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="weekly_daily_b2_resonance_v1")
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--symbol")
    parser.add_argument("--diagnose", action="store_true")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--market-cap-policy", choices=[item.value for item in MarketCapPolicy])
    parser.add_argument("--output", "--output-dir", dest="output_dir", default="outputs/strategy-scan")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
