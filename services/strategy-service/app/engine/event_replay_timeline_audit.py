from __future__ import annotations

import json
from pathlib import Path
from statistics import median
from typing import Any

from app.config.strategy_params import StrategyParams
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


async def build_event_replay_timeline_audit(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    params: StrategyParams,
    symbols,
    start_time,
    end_time,
) -> dict[str, Any]:
    symbol_rows: list[dict[str, Any]] = []
    eval_counts: list[int] = []
    weekly_run_counts: list[int] = []
    daily_run_counts: list[int] = []
    thirty_run_counts: list[int] = []

    for symbol in symbols:
        await module_c_repo.prime_symbol_cache(symbol.symbol_id)
        await kline_repo.prime_symbol_cache(symbol.symbol_id, start_time=start_time, end_time=end_time)
        try:
            bars_30f = await kline_repo.get_klines(symbol.symbol_id, "30f", start=start_time, end=end_time)
            evaluation_points = len(bars_30f)
            weekly_run_ids: set[int] = set()
            daily_run_ids: set[int] = set()
            thirty_run_ids: set[int] = set()
            as_of_times = []
            for bar in bars_30f:
                as_of_time = bar.ts
                as_of_times.append(as_of_time)
                for level, bucket in (("1w", weekly_run_ids), ("1d", daily_run_ids), ("30f", thirty_run_ids)):
                    head = await module_c_repo.get_head(symbol.symbol_id, level, mode="predictive", as_of_time=as_of_time)
                    if head is not None:
                        bucket.add(head.run_id)

            historical_weekly_heads = await module_c_repo.list_historical_heads(symbol.symbol_id, "1w", end_time=end_time)
            weekly_b2_run_ids: list[int] = []
            for head in historical_weekly_heads:
                signals = await module_c_repo.get_signals(symbol.symbol_id, "1w", mode="predictive", as_of_time=head.bar_until)
                if any(signal.side == "buy" and signal.bsp_type in set(params.weekly_b2_types) for signal in signals):
                    weekly_b2_run_ids.append(head.run_id)
            current_weekly_signals = await module_c_repo.get_signals(symbol.symbol_id, "1w", mode="predictive", as_of_time=end_time)
            current_has_weekly_b2 = any(
                signal.side == "buy" and signal.bsp_type in set(params.weekly_b2_types)
                for signal in current_weekly_signals
            )

            row = {
                "symbol": symbol.symbol,
                "name": symbol.name,
                "evaluation_points": evaluation_points,
                "distinct_weekly_run_ids": len(weekly_run_ids),
                "distinct_daily_run_ids": len(daily_run_ids),
                "distinct_30f_run_ids": len(thirty_run_ids),
                "as_of_time_changed": evaluation_points > 1,
                "single_final_as_of_only": evaluation_points <= 1,
                "current_has_weekly_b2": current_has_weekly_b2,
                "historical_weekly_b2_run_count": len(weekly_b2_run_ids),
                "historical_weekly_b2_run_ids": weekly_b2_run_ids[:20],
                "historical_b2_missing_in_current": bool(weekly_b2_run_ids and not current_has_weekly_b2),
                "first_as_of_time": as_of_times[0].isoformat() if as_of_times else None,
                "last_as_of_time": as_of_times[-1].isoformat() if as_of_times else None,
            }
            symbol_rows.append(row)
            eval_counts.append(evaluation_points)
            weekly_run_counts.append(len(weekly_run_ids))
            daily_run_counts.append(len(daily_run_ids))
            thirty_run_counts.append(len(thirty_run_ids))
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)
            module_c_repo.release_symbol_cache(symbol.symbol_id)

    summary = {
        "strategy_code": params.strategy_code,
        "weekly_context_mode": params.weekly_context_mode,
        "symbol_count": len(symbol_rows),
        "p50_evaluation_points": _percentile(eval_counts, 0.50),
        "p90_evaluation_points": _percentile(eval_counts, 0.90),
        "p95_evaluation_points": _percentile(eval_counts, 0.95),
        "p50_distinct_weekly_run_ids": _percentile(weekly_run_counts, 0.50),
        "p50_distinct_daily_run_ids": _percentile(daily_run_counts, 0.50),
        "p50_distinct_30f_run_ids": _percentile(thirty_run_counts, 0.50),
        "single_final_as_of_only_symbols": sum(1 for row in symbol_rows if row["single_final_as_of_only"]),
        "historical_b2_missing_in_current_symbols": sum(1 for row in symbol_rows if row["historical_b2_missing_in_current"]),
        "sample_symbols_with_historical_b2_missing_in_current": [
            row["symbol"] for row in symbol_rows if row["historical_b2_missing_in_current"]
        ][:20],
    }
    return {"summary": summary, "symbols": symbol_rows}


def write_event_replay_timeline_audit(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "event_replay_timeline_audit.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "event_replay_timeline_audit.md").write_text(
        render_event_replay_timeline_audit_markdown(payload),
        encoding="utf-8",
    )


def render_event_replay_timeline_audit_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Event Replay Timeline Audit",
        "",
        f"- Strategy: `{summary['strategy_code']}`",
        f"- Weekly context mode: `{summary['weekly_context_mode']}`",
        f"- Symbols audited: `{summary['symbol_count']}`",
        f"- p50 evaluation points: `{summary['p50_evaluation_points']}`",
        f"- p90 evaluation points: `{summary['p90_evaluation_points']}`",
        f"- p95 evaluation points: `{summary['p95_evaluation_points']}`",
        f"- p50 distinct weekly run ids: `{summary['p50_distinct_weekly_run_ids']}`",
        f"- p50 distinct daily run ids: `{summary['p50_distinct_daily_run_ids']}`",
        f"- p50 distinct 30f run ids: `{summary['p50_distinct_30f_run_ids']}`",
        f"- single_final_as_of_only_symbols: `{summary['single_final_as_of_only_symbols']}`",
        f"- historical_b2_missing_in_current_symbols: `{summary['historical_b2_missing_in_current_symbols']}`",
        f"- sample_symbols_with_historical_b2_missing_in_current: `{summary['sample_symbols_with_historical_b2_missing_in_current']}`",
    ]
    return "\n".join(lines) + "\n"


def _percentile(values: list[int], ratio: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * ratio))
    return int(ordered[index])
