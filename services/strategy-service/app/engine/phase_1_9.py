from __future__ import annotations

import asyncio
import json
from collections import Counter, defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from app.analyzers.fractal_detector import latest_bottom_fractal_time
from app.config.strategy_params import PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE, StrategyParams
from app.domain.models import ChanSignal, SymbolInfo
from app.engine.module_c_history_backfill import build_backfill_dry_run, preload_symbol_bars, run_historical_backfill
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS, write_json
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PHASE_1_7_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-7-10-symbols"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-9-daily-setup-semantics"
MODE_ORDER = [
    "strict_daily_b1_after_weekly_context",
    "true_trust_daily_b2_or_b2s",
    "daily_b2_or_b2s_with_b1_score",
    "daily_buy_signal_any_observation",
]
BENCHMARK_SYMBOLS = DEFAULT_PHASE_1_7_SYMBOLS[:3]


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return serialize_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]
    return value


def load_phase_1_7_inputs() -> dict[str, Any]:
    return {
        "effective_window": json.loads(
            (PHASE_1_7_OUTPUT_DIR / "effective_backtest_window_after_10_symbols.json").read_text(encoding="utf-8")
        ),
        "backfill_summary": json.loads(
            (PHASE_1_7_OUTPUT_DIR / "backfill_10_symbols_summary.json").read_text(encoding="utf-8")
        ),
        "backfill_perf": json.loads((PHASE_1_7_OUTPUT_DIR / "backfill_perf.json").read_text(encoding="utf-8")),
        "dry_run": json.loads((PHASE_1_7_OUTPUT_DIR / "backfill_10_symbols_dry_run.json").read_text(encoding="utf-8")),
    }


def _signal_payload(signal: ChanSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "time": signal.point_time.isoformat(),
        "side": signal.side,
        "bsp_type": signal.bsp_type,
        "price": signal.price,
        "confirmed": signal.confirmed,
        "run_id": signal.run_id,
    }


def _future_window_end(daily_bars: list, as_of_time: datetime, trading_days: int) -> datetime:
    ordered = [bar.ts for bar in daily_bars if bar.ts >= as_of_time]
    if not ordered:
        return as_of_time
    index = min(len(ordered) - 1, max(0, trading_days))
    return ordered[index]


def _classify_failure_v2(
    *,
    audit,
    b1_before_context: bool,
    b1_after_as_of: bool,
    b2_or_b2s_outside_context: bool,
) -> str:
    if audit.daily_setup_accepted_by_mode:
        return "daily_setup_accepted"
    if not audit.daily_signal_any_found:
        if b1_after_as_of:
            return "daily_B1_exists_but_after_as_of"
        return "no_daily_signal_at_all"
    if audit.daily_b2_or_b2s_found and not audit.daily_prior_b1_for_b2_found:
        return "daily_B2_or_B2s_no_prior_B1"
    if b2_or_b2s_outside_context:
        return "daily_B2_or_B2s_outside_weekly_context"
    if b1_before_context:
        return "daily_B1_exists_but_before_context"
    if audit.daily_signal_any_found and not audit.daily_b1_found:
        return "only_daily_non_B1_signal"
    if b1_after_as_of:
        return "daily_B1_exists_but_after_as_of"
    return "daily_signal_exists_but_mode_rejected"


def _collect_timeline_sample(
    *,
    symbol: SymbolInfo,
    as_of_time: datetime,
    weekly_context,
    all_daily_signals: list[ChanSignal],
    future_window_end: datetime,
    failure_reason_v2: str,
) -> dict[str, Any]:
    before_as_of = [
        _signal_payload(signal)
        for signal in all_daily_signals
        if signal.side == "buy" and signal.point_time <= as_of_time
    ]
    future = [
        _signal_payload(signal)
        for signal in all_daily_signals
        if signal.side == "buy" and as_of_time < signal.point_time <= future_window_end
    ]
    nearest_daily_b1_before = next(
        (_signal_payload(signal) for signal in reversed(all_daily_signals) if signal.side == "buy" and signal.bsp_type == "1" and signal.point_time <= as_of_time),
        None,
    )
    nearest_daily_b2_or_b2s_before = next(
        (_signal_payload(signal) for signal in reversed(all_daily_signals) if signal.side == "buy" and signal.bsp_type in {"2", "2s"} and signal.point_time <= as_of_time),
        None,
    )
    nearest_daily_buy_any_before = next(
        (_signal_payload(signal) for signal in reversed(all_daily_signals) if signal.side == "buy" and signal.point_time <= as_of_time),
        None,
    )
    return {
        "symbol": symbol.symbol,
        "name": symbol.name,
        "as_of_time": as_of_time.isoformat(),
        "weekly_context_start": weekly_context.anchor_time.isoformat(),
        "weekly_context_signal_time": weekly_context.weekly_b2.point_time.isoformat(),
        "weekly_context_mode": weekly_context.context_mode,
        "daily_signals_before_as_of": before_as_of,
        "daily_signals_after_as_of_within_window": future,
        "nearest_daily_B1_before": nearest_daily_b1_before,
        "nearest_daily_B2_or_B2s_before": nearest_daily_b2_or_b2s_before,
        "nearest_daily_buy_any_before": nearest_daily_buy_any_before,
        "failure_reason_v2": failure_reason_v2,
    }


def _inspect_downstream(
    *,
    audit,
    as_of_time: datetime,
    daily_bars: list,
    signals_30f: list[ChanSignal],
    signals_5f: list[ChanSignal],
) -> dict[str, Any]:
    if not audit.daily_setup_accepted_by_mode:
        return {
            "thirty_f_b1_found": False,
            "entry_watch_active": False,
            "entry_triggered": False,
            "daily_bottom_fractal_found": False,
            "five_f_b2_confirm_found": False,
            "confidence_score": 0.0,
        }

    anchor_signal = audit.selected_daily_b2_or_b2s or audit.selected_daily_b1 or audit.selected_buy_signal_any
    if anchor_signal is None:
        return {
            "thirty_f_b1_found": False,
            "entry_watch_active": True,
            "entry_triggered": False,
            "daily_bottom_fractal_found": False,
            "five_f_b2_confirm_found": False,
            "confidence_score": 0.0,
        }

    thirty_b1 = next(
        (
            signal
            for signal in reversed(signals_30f)
            if signal.side == "buy" and signal.bsp_type == "1" and anchor_signal.point_time <= signal.point_time <= as_of_time
        ),
        None,
    )
    daily_bottom_time = latest_bottom_fractal_time([bar for bar in daily_bars if bar.ts <= as_of_time], after=anchor_signal.point_time)
    five_b2 = None
    if thirty_b1 is not None:
        five_b2 = next(
            (
                signal
                for signal in reversed(signals_5f)
                if signal.side == "buy"
                and signal.bsp_type in {"2", "2s"}
                and thirty_b1.point_time <= signal.point_time <= as_of_time
            ),
            None,
        )
    confidence = 0.0
    if thirty_b1 is not None:
        confidence += 40.0
    if daily_bottom_time is not None:
        confidence += 30.0
    if five_b2 is not None:
        confidence += 30.0
    return {
        "thirty_f_b1_found": thirty_b1 is not None,
        "entry_watch_active": True,
        "entry_triggered": thirty_b1 is not None and confidence >= 70.0,
        "daily_bottom_fractal_found": daily_bottom_time is not None,
        "five_f_b2_confirm_found": five_b2 is not None,
        "confidence_score": confidence,
        "thirty_f_b1": _signal_payload(thirty_b1),
        "five_f_b2_confirm": _signal_payload(five_b2),
        "daily_bottom_time": daily_bottom_time.isoformat() if daily_bottom_time is not None else None,
    }


async def build_daily_setup_semantics_dataset(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    start_time: datetime,
    end_time: datetime,
    concurrency: int,
) -> dict[str, Any]:
    base_params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
    mode_params = {
        "strict_daily_b1_after_weekly_context": base_params.with_overrides(daily_setup_mode="strict_daily_b1_after_weekly_context"),
        "true_trust_daily_b2_or_b2s": base_params.with_overrides(daily_setup_mode="true_trust_daily_b2_or_b2s"),
        "daily_b2_or_b2s_with_b1_score": base_params.with_overrides(daily_setup_mode="daily_b2_or_b2s_with_b1_score"),
        "daily_buy_signal_any_observation": base_params.with_overrides(daily_setup_mode="daily_buy_signal_any_observation"),
        "trust_daily_b2_or_b2s_signal": base_params.with_overrides(daily_setup_mode="trust_daily_b2_or_b2s_signal"),
    }
    diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)
    rows: list[dict[str, Any]] = []

    async def process_symbol(symbol: SymbolInfo) -> list[dict[str, Any]]:
        await kline_repo.prime_symbol_cache(symbol.symbol_id, start_time=start_time, end_time=end_time)
        await module_c_repo.prime_symbol_cache(symbol.symbol_id)
        try:
            bars_30f = await kline_repo.get_klines(symbol.symbol_id, "30f", start=start_time, end=end_time)
            daily_bars = await kline_repo.get_klines(symbol.symbol_id, "1d", end=end_time)
            daily_signals_all = await module_c_repo.get_signals(symbol.symbol_id, "1d", mode="predictive", as_of_time=end_time)
            signals_30f_all = await module_c_repo.get_signals(symbol.symbol_id, "30f", mode="predictive", as_of_time=end_time)
            signals_5f_all = await module_c_repo.get_signals(symbol.symbol_id, "5f", mode="predictive", as_of_time=end_time)
            symbol_rows: list[dict[str, Any]] = []
            for bar in bars_30f:
                strict_diagnosis = await diagnoser.diagnose_symbol(symbol, as_of_time=bar.ts, params=mode_params["strict_daily_b1_after_weekly_context"])
                gate_map = {gate.name: gate for gate in strict_diagnosis.gates}
                if not gate_map.get("weekly_macd_dif_gt_zero", None) or not gate_map["weekly_macd_dif_gt_zero"].passed:
                    continue
                if strict_diagnosis.weekly_context is None:
                    continue

                future_window_end = _future_window_end(daily_bars, bar.ts, 60)
                all_before_as_of = [signal for signal in daily_signals_all if signal.side == "buy" and signal.point_time <= bar.ts]
                all_after_as_of = [signal for signal in daily_signals_all if signal.side == "buy" and bar.ts < signal.point_time <= future_window_end]
                b1_before_context = any(signal.bsp_type == "1" and signal.point_time < strict_diagnosis.weekly_context.anchor_time for signal in all_before_as_of)
                b1_after_as_of = any(signal.bsp_type == "1" for signal in all_after_as_of)
                b2_or_b2s_outside_context = any(
                    signal.bsp_type in {"2", "2s"} and signal.point_time < strict_diagnosis.weekly_context.anchor_time
                    for signal in all_before_as_of
                )

                mode_results: dict[str, Any] = {}
                legacy_trust = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=daily_signals_all,
                    weekly_context=strict_diagnosis.weekly_context,
                    as_of_time=bar.ts,
                    params=mode_params["trust_daily_b2_or_b2s_signal"],
                    daily_bars=daily_bars,
                )
                for mode_name in MODE_ORDER:
                    audit = StrategyDiagnoser.audit_daily_setup_semantics(
                        daily_signals=daily_signals_all,
                        weekly_context=strict_diagnosis.weekly_context,
                        as_of_time=bar.ts,
                        params=mode_params[mode_name],
                        daily_bars=daily_bars,
                    )
                    downstream = _inspect_downstream(
                        audit=audit,
                        as_of_time=bar.ts,
                        daily_bars=daily_bars,
                        signals_30f=signals_30f_all,
                        signals_5f=signals_5f_all,
                    )
                    mode_results[mode_name] = {
                        "audit": serialize_value(audit),
                        "failure_reason_v2": _classify_failure_v2(
                            audit=audit,
                            b1_before_context=b1_before_context,
                            b1_after_as_of=b1_after_as_of,
                            b2_or_b2s_outside_context=b2_or_b2s_outside_context,
                        ),
                        "downstream": downstream,
                    }

                symbol_rows.append(
                    {
                        "symbol": symbol.symbol,
                        "name": symbol.name,
                        "as_of_time": bar.ts.isoformat(),
                        "weekly_context": {
                            "start": strict_diagnosis.weekly_context.anchor_time.isoformat(),
                            "signal_time": strict_diagnosis.weekly_context.weekly_b2.point_time.isoformat(),
                            "mode": strict_diagnosis.weekly_context.context_mode,
                        },
                        "legacy_trust_audit": serialize_value(legacy_trust),
                        "timeline": _collect_timeline_sample(
                            symbol=symbol,
                            as_of_time=bar.ts,
                            weekly_context=strict_diagnosis.weekly_context,
                            all_daily_signals=daily_signals_all,
                            future_window_end=future_window_end,
                            failure_reason_v2=mode_results["strict_daily_b1_after_weekly_context"]["failure_reason_v2"],
                        ),
                        "mode_results": mode_results,
                    }
                )
            return symbol_rows
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)
            module_c_repo.release_symbol_cache(symbol.symbol_id)

    results = []
    for index in range(0, len(symbols), concurrency):
        batch = symbols[index : index + concurrency]
        results.extend(await asyncio.gather(*(process_symbol(symbol) for symbol in batch)))
    for batch_rows in results:
        rows.extend(batch_rows)
    return {"rows": rows}


def build_semantics_reports(dataset: dict[str, Any]) -> dict[str, Any]:
    rows = dataset["rows"]
    strict_failures = Counter(row["mode_results"]["strict_daily_b1_after_weekly_context"]["failure_reason_v2"] for row in rows)
    symbol_distribution = defaultdict(Counter)
    asof_distribution = defaultdict(Counter)
    mode_compare_rows = []
    gate_rows = []

    for row in rows:
        symbol_distribution[row["mode_results"]["strict_daily_b1_after_weekly_context"]["failure_reason_v2"]][row["symbol"]] += 1
        asof_distribution[row["mode_results"]["strict_daily_b1_after_weekly_context"]["failure_reason_v2"]][row["as_of_time"][:10]] += 1

    for mode_name in MODE_ORDER:
        accepted = [row for row in rows if row["mode_results"][mode_name]["audit"]["daily_setup_accepted_by_mode"]]
        selected_b2 = [
            row for row in accepted if (row["mode_results"][mode_name]["audit"]["selected_daily_b2_or_b2s"] or {}).get("bsp_type") == "2"
        ]
        selected_b2s = [
            row for row in accepted if (row["mode_results"][mode_name]["audit"]["selected_daily_b2_or_b2s"] or {}).get("bsp_type") == "2s"
        ]
        prior_b1 = [row for row in rows if row["mode_results"][mode_name]["audit"]["daily_prior_b1_for_b2_found"]]
        no_prior_b1 = [
            row for row in rows
            if row["mode_results"][mode_name]["audit"]["daily_b2_or_b2s_found"]
            and not row["mode_results"][mode_name]["audit"]["daily_prior_b1_for_b2_found"]
        ]
        downstream = [row["mode_results"][mode_name]["downstream"] for row in accepted]
        mode_compare_rows.append(
            {
                "daily_setup_mode": mode_name,
                "weekly_context_count": len(rows),
                "daily_setup_count": len(accepted),
                "prior_B1_found": len(prior_b1),
                "no_prior_B1": len(no_prior_b1),
                "B2_count": len(selected_b2),
                "B2s_count": len(selected_b2s),
                "entry_watch_count": sum(1 for item in downstream if item["entry_watch_active"]),
                "thirty_f_b1_count": sum(1 for item in downstream if item["thirty_f_b1_found"]),
                "entry_trigger_count": sum(1 for item in downstream if item["entry_triggered"]),
                "trades": 0,
            }
        )
        gate_counter = Counter(row["mode_results"][mode_name]["failure_reason_v2"] for row in rows)
        for gate_name, failed in gate_counter.items():
            gate_rows.append(
                {
                    "mode": mode_name,
                    "gate": gate_name,
                    "reached": len(rows),
                    "passed": len(rows) - failed if gate_name != "daily_setup_accepted" else failed,
                    "failed": failed if gate_name != "daily_setup_accepted" else len(rows) - failed,
                }
            )

    timeline_rows = []
    by_reason = defaultdict(list)
    for row in rows:
        reason = row["mode_results"]["strict_daily_b1_after_weekly_context"]["failure_reason_v2"]
        by_reason[reason].append(row["timeline"])
    for reason, items in sorted(by_reason.items()):
        timeline_rows.extend(items[:20])

    trace_rows = []
    for row in rows:
        for mode_name in MODE_ORDER:
            mode_payload = row["mode_results"][mode_name]
            if len(trace_rows) >= 12:
                break
            if mode_payload["audit"]["daily_setup_accepted_by_mode"] or mode_payload["failure_reason_v2"] in {
                "daily_B2_or_B2s_no_prior_B1",
                "daily_B1_exists_but_before_context",
                "daily_B1_exists_but_after_as_of",
            }:
                trace_rows.append(
                    {
                        "symbol": row["symbol"],
                        "as_of_time": row["as_of_time"],
                        "mode": mode_name,
                        "failure_reason_v2": mode_payload["failure_reason_v2"],
                        "audit": mode_payload["audit"],
                        "downstream": mode_payload["downstream"],
                    }
                )

    semantics_audit = {
        "sample_count": len(rows),
        "strict_failure_reason_counts": dict(strict_failures),
        "symbol_distribution_by_failure_reason": {
            reason: dict(counter.most_common()) for reason, counter in symbol_distribution.items()
        },
        "as_of_distribution_by_failure_reason": {
            reason: dict(counter.most_common(20)) for reason, counter in asof_distribution.items()
        },
        "trust_mode_semantics": {
            "legacy_trust_daily_b2_or_b2s_signal_requires_prior_daily_B1": True,
            "legacy_trust_mode_is_window_expansion_only": True,
            "true_trust_daily_b2_or_b2s_is_self_contained": True,
        },
    }
    return {
        "semantics_audit": semantics_audit,
        "mode_compare": {"rows": mode_compare_rows},
        "gate_waterfall": {"rows": gate_rows},
        "timeline_rows": timeline_rows,
        "trace_rows": trace_rows,
        "raw_rows": rows,
    }


def render_markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(item) for item in row) + " |")
    return "\n".join(lines)


def render_daily_setup_semantics_audit_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Daily Setup Semantics Audit",
        "",
        f"- sample_count: `{payload['sample_count']}`",
        f"- strict_failure_reason_counts: `{json.dumps(payload['strict_failure_reason_counts'], ensure_ascii=False)}`",
        f"- trust_mode_semantics: `{json.dumps(payload['trust_mode_semantics'], ensure_ascii=False)}`",
        "",
        "## Failure Reasons By Symbol",
        "",
    ]
    for reason, counter in payload["symbol_distribution_by_failure_reason"].items():
        lines.append(f"- `{reason}` -> `{json.dumps(counter, ensure_ascii=False)}`")
    return "\n".join(lines) + "\n"


def render_daily_setup_mode_compare_md(payload: dict[str, Any]) -> str:
    rows = [
        [
            f"`{row['daily_setup_mode']}`",
            row["weekly_context_count"],
            row["daily_setup_count"],
            row["prior_B1_found"],
            row["no_prior_B1"],
            row["B2_count"],
            row["B2s_count"],
            row["entry_watch_count"],
            row["thirty_f_b1_count"],
            row["entry_trigger_count"],
            row["trades"],
        ]
        for row in payload["rows"]
    ]
    return "\n".join(
        [
            "# Daily Setup Mode Compare V2",
            "",
            render_markdown_table(
                [
                    "mode",
                    "weekly_context_count",
                    "daily_setup_count",
                    "prior_B1_found",
                    "no_prior_B1",
                    "B2_count",
                    "B2s_count",
                    "entry_watch_count",
                    "thirty_f_b1_count",
                    "entry_trigger_count",
                    "trades",
                ],
                rows,
            ),
            "",
        ]
    )


def render_gate_waterfall_v2_md(payload: dict[str, Any]) -> str:
    rows = [
        [f"`{row['mode']}`", f"`{row['gate']}`", row["reached"], row["passed"], row["failed"]]
        for row in payload["rows"]
    ]
    return "\n".join(
        [
            "# Gate Waterfall Daily Modes V2",
            "",
            render_markdown_table(["mode", "gate", "reached", "passed", "failed"], rows),
            "",
        ]
    )


def render_trade_analysis_v2_md(compare_payload: dict[str, Any]) -> str:
    lines = ["# Trade Analysis Daily Setup V2", ""]
    for row in compare_payload["rows"]:
        lines.append(f"## `{row['daily_setup_mode']}`")
        if row["daily_setup_count"] == 0:
            lines.append("- daily setup 仍为 0，未进入 30F gate。")
        elif row["thirty_f_b1_count"] == 0:
            lines.append("- 已进入 daily setup，但下游未发现 30F B1。")
        elif row["entry_trigger_count"] == 0:
            lines.append("- 已发现 30F B1，但未达到诊断用 entry trigger。")
        else:
            lines.append("- 已进入 30F gate 且出现诊断级 entry trigger，可考虑下一阶段 smoke。")
        lines.append("")
    return "\n".join(lines)


def render_perf_profile_md(payload: dict[str, Any], title: str) -> str:
    lines = [f"# {title}", "", f"- sample_count: `{payload['sample_count']}`", ""]
    aggregate = payload.get("aggregate", {})
    if aggregate:
        lines.append(f"- aggregate: `{json.dumps(aggregate, ensure_ascii=False)}`")
        lines.append("")
    lines.append("## Per Level")
    lines.append("")
    for row in payload.get("per_level", []):
        lines.append(f"- `{row['level']}` -> `{json.dumps(row, ensure_ascii=False)}`")
    return "\n".join(lines) + "\n"


def build_perf_scale_estimate(before_summary: dict[str, Any], after_summary: dict[str, Any]) -> dict[str, Any]:
    before_elapsed = float(before_summary["elapsed_seconds"])
    after_elapsed = float(after_summary["elapsed_seconds"])
    speedup = round((before_elapsed - after_elapsed) / before_elapsed, 6) if before_elapsed else 0.0
    avg_after_symbol = after_elapsed / max(1, int(after_summary["symbols"]))
    estimated_50 = round(avg_after_symbol * 50, 3)
    return {
        "before_elapsed_seconds": before_elapsed,
        "after_elapsed_seconds": after_elapsed,
        "relative_improvement": speedup,
        "estimated_50_symbols_seconds": estimated_50,
        "can_enter_50_symbols_backfill": speedup >= 0.3,
        "must_optimize_first": speedup < 0.3,
    }


def render_scale_estimate_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Performance Scale Estimate V2",
            "",
            f"- before_elapsed_seconds: `{payload['before_elapsed_seconds']}`",
            f"- after_elapsed_seconds: `{payload['after_elapsed_seconds']}`",
            f"- relative_improvement: `{payload['relative_improvement']}`",
            f"- estimated_50_symbols_seconds: `{payload['estimated_50_symbols_seconds']}`",
            f"- can_enter_50_symbols_backfill: `{payload['can_enter_50_symbols_backfill']}`",
            f"- must_optimize_first: `{payload['must_optimize_first']}`",
            "",
        ]
    )


async def run_phase_1_9(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_workers: int = 4,
) -> dict[str, Any]:
    started = perf_counter()
    benchmark_workers = max(1, min(max_workers, len(BENCHMARK_SYMBOLS)))
    benchmark_before_group = "phase_1_9_benchmark_before_c3"
    benchmark_after_group = "phase_1_9_benchmark_after_c3"
    output_dir.mkdir(parents=True, exist_ok=True)
    phase_1_7_inputs = load_phase_1_7_inputs()
    start_time = datetime.fromisoformat(phase_1_7_inputs["effective_window"]["strict_global_effective_start"])
    end_time = datetime.fromisoformat(phase_1_7_inputs["effective_window"]["strict_global_effective_end"])
    warmup_start = datetime.fromisoformat(phase_1_7_inputs["backfill_summary"]["warmup_start"])

    symbols = await module_c_repo.list_active_symbols(symbols=DEFAULT_PHASE_1_7_SYMBOLS)
    dataset = await build_daily_setup_semantics_dataset(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbols,
        start_time=start_time,
        end_time=end_time,
        concurrency=max_workers,
    )
    reports = build_semantics_reports(dataset)

    with (output_dir / "daily_signal_timeline_samples.jsonl").open("w", encoding="utf-8") as handle:
        for timeline_row in reports["timeline_rows"]:
            handle.write(json.dumps(timeline_row, ensure_ascii=False) + "\n")
    with (output_dir / "daily_setup_trace_samples.jsonl").open("w", encoding="utf-8") as handle:
        for row in reports["trace_rows"]:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    write_json(output_dir / "daily_setup_semantics_audit.json", reports["semantics_audit"])
    (output_dir / "daily_setup_semantics_audit.md").write_text(
        render_daily_setup_semantics_audit_md(reports["semantics_audit"]),
        encoding="utf-8",
    )
    write_json(output_dir / "daily_setup_mode_compare_v2.json", reports["mode_compare"])
    (output_dir / "daily_setup_mode_compare_v2.md").write_text(
        render_daily_setup_mode_compare_md(reports["mode_compare"]),
        encoding="utf-8",
    )
    write_json(output_dir / "gate_waterfall_daily_modes_v2.json", reports["gate_waterfall"])
    (output_dir / "gate_waterfall_daily_modes_v2.md").write_text(
        render_gate_waterfall_v2_md(reports["gate_waterfall"]),
        encoding="utf-8",
    )
    (output_dir / "trade_analysis_daily_setup_v2.md").write_text(
        render_trade_analysis_v2_md(reports["mode_compare"]),
        encoding="utf-8",
    )

    bench_symbols = await module_c_repo.list_active_symbols(symbols=BENCHMARK_SYMBOLS)
    bars_by_symbol = await preload_symbol_bars(
        kline_repo=kline_repo,
        symbols=bench_symbols,
        levels=("5f", "30f", "1d", "1w", "1m"),
        warmup_start=warmup_start,
        end_time=end_time,
    )
    before_dry_run = build_backfill_dry_run(
        symbols=bench_symbols,
        bars_by_symbol=bars_by_symbol,
        profile="research_daily_close",
        warmup_start=warmup_start,
        backtest_start=start_time,
        end_time=end_time,
        levels=("5f", "30f", "1d", "1w", "1m"),
        mode="predictive",
    )
    before_summary = await run_historical_backfill(
        pool=module_c_repo.pool,
        symbols=bench_symbols,
        bars_by_symbol=bars_by_symbol,
        profile="research_daily_close",
        warmup_start=warmup_start,
        backtest_start=start_time,
        end_time=end_time,
        levels=("5f", "30f", "1d", "1w", "1m"),
        mode="predictive",
        max_workers=benchmark_workers,
        resume=False,
        run_group_id=benchmark_before_group,
        optimization_mode="legacy",
    )
    after_summary = await run_historical_backfill(
        pool=module_c_repo.pool,
        symbols=bench_symbols,
        bars_by_symbol=bars_by_symbol,
        profile="research_daily_close",
        warmup_start=warmup_start,
        backtest_start=start_time,
        end_time=end_time,
        levels=("5f", "30f", "1d", "1w", "1m"),
        mode="predictive",
        max_workers=benchmark_workers,
        resume=False,
        run_group_id=benchmark_after_group,
        optimization_mode="optimized",
    )
    perf_scale = build_perf_scale_estimate(before_summary, after_summary)
    write_json(output_dir / "backfill_perf_profile_detailed.json", before_summary["perf_profile"])
    (output_dir / "backfill_perf_profile_detailed.md").write_text(
        render_perf_profile_md(before_summary["perf_profile"], "Backfill Perf Profile Detailed"),
        encoding="utf-8",
    )
    (output_dir / "backfill_optimization_plan.md").write_text(
        "\n".join(
            [
                "# Backfill Optimization Plan",
                "",
                "- 优化 1：resume 查询改为 symbol/level 预取已存在 cutoff，避免 per-snapshot run_exists 查询。",
                "- 优化 2：cutoff 切片改为 cursor 递增窗口，避免每个 cutoff 重建时间数组并重复 bisect。",
                "- 本轮未改 chan.py / Module C 语义，仅收口 strategy-service 历史回填链路。",
                "",
            ]
        ),
        encoding="utf-8",
    )
    write_json(output_dir / "backfill_perf_after_optimization.json", after_summary["perf_profile"])
    (output_dir / "backfill_perf_after_optimization.md").write_text(
        render_perf_profile_md(after_summary["perf_profile"], "Backfill Perf After Optimization"),
        encoding="utf-8",
    )
    write_json(output_dir / "performance_scale_estimate_v2.json", perf_scale)
    (output_dir / "performance_scale_estimate_v2.md").write_text(
        render_scale_estimate_md(perf_scale),
        encoding="utf-8",
    )

    strict_row = next(row for row in reports["mode_compare"]["rows"] if row["daily_setup_mode"] == "strict_daily_b1_after_weekly_context")
    trust_row = next(row for row in reports["mode_compare"]["rows"] if row["daily_setup_mode"] == "true_trust_daily_b2_or_b2s")
    score_row = next(row for row in reports["mode_compare"]["rows"] if row["daily_setup_mode"] == "daily_b2_or_b2s_with_b1_score")
    decision_lines = [
        "# Phase 1.9 Decision Report",
        "",
        "## 日线 setup 语义决策",
        "",
        "- official strict 保留：`true`",
        f"- true_trust_daily_b2_or_b2s 是否进入 daily setup：`{trust_row['daily_setup_count'] > 0}`",
        f"- daily_b2_or_b2s_with_b1_score 是否有研究价值：`{score_row['daily_setup_count'] > 0}`",
        f"- 是否坚持 prior daily B1 为 official 硬 gate：`{strict_row['daily_setup_count'] == 0}`",
        "",
        "## 30F 验证决策",
        "",
        f"- 是否进入 strategy_30f smoke：`{any(row['daily_setup_count'] > 0 and row['thirty_f_b1_count'] > 0 for row in reports['mode_compare']['rows'])}`",
        "",
        "## 扩样决策",
        "",
        f"- 是否进入 50 标的回填：`{perf_scale['can_enter_50_symbols_backfill']}`",
        f"- 是否仍需先优化：`{perf_scale['must_optimize_first']}`",
        "",
    ]
    (output_dir / "phase_1_9_decision_report.md").write_text("\n".join(decision_lines), encoding="utf-8")

    summary_lines = [
        "# Phase 1.9 Summary",
        "",
        f"- sample_count: `{reports['semantics_audit']['sample_count']}`",
        f"- strict_failure_reason_counts: `{json.dumps(reports['semantics_audit']['strict_failure_reason_counts'], ensure_ascii=False)}`",
        f"- strict_daily_setup_count: `{strict_row['daily_setup_count']}`",
        f"- true_trust_daily_b2_or_b2s_count: `{trust_row['daily_setup_count']}`",
        f"- daily_b2_or_b2s_with_b1_score_count: `{score_row['daily_setup_count']}`",
        f"- backfill_perf_improvement: `{perf_scale['relative_improvement']}`",
        f"- benchmark_workers: `{benchmark_workers}`",
        f"- benchmark_before_group: `{benchmark_before_group}`",
        f"- benchmark_after_group: `{benchmark_after_group}`",
        f"- elapsed_seconds_total: `{round(perf_counter() - started, 3)}`",
        "",
    ]
    (output_dir / "phase_1_9_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    deliverables = [
        "phase_1_9_summary.md",
        "phase_1_9_task_checklist_report.md",
        "daily_setup_semantics_audit.md",
        "daily_setup_semantics_audit.json",
        "daily_signal_timeline_samples.jsonl",
        "daily_setup_mode_compare_v2.md",
        "daily_setup_mode_compare_v2.json",
        "gate_waterfall_daily_modes_v2.md",
        "gate_waterfall_daily_modes_v2.json",
        "daily_setup_trace_samples.jsonl",
        "trade_analysis_daily_setup_v2.md",
        "backfill_perf_profile_detailed.md",
        "backfill_perf_profile_detailed.json",
        "backfill_optimization_plan.md",
        "backfill_perf_after_optimization.md",
        "backfill_perf_after_optimization.json",
        "performance_scale_estimate_v2.md",
        "performance_scale_estimate_v2.json",
        "phase_1_9_decision_report.md",
    ]
    checklist_lines = ["# Phase 1.9 Task Checklist Report", ""]
    for item in deliverables:
        checklist_lines.append(f"- [x] `{item}`")
    (output_dir / "phase_1_9_task_checklist_report.md").write_text("\n".join(checklist_lines), encoding="utf-8")

    return {
        "summary": {
            "sample_count": reports["semantics_audit"]["sample_count"],
            "strict_daily_setup_count": strict_row["daily_setup_count"],
            "true_trust_daily_b2_or_b2s_count": trust_row["daily_setup_count"],
            "daily_b2_or_b2s_with_b1_score_count": score_row["daily_setup_count"],
            "backfill_perf_improvement": perf_scale["relative_improvement"],
            "benchmark_workers": benchmark_workers,
            "benchmark_before_group": benchmark_before_group,
            "benchmark_after_group": benchmark_after_group,
        }
    }
