from __future__ import annotations

from collections import Counter
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from app.domain.models import ScanDiagnosis


GATE_ORDER = [
    "active_symbol",
    "market_cap_ok",
    "module_c_5f_run_available",
    "module_c_30f_run_available",
    "module_c_1d_run_available",
    "module_c_1w_run_available",
    "module_c_1m_run_available",
    "module_c_all_runs_available",
    "weekly_b1_found",
    "weekly_b2_found",
    "weekly_b2_after_weekly_b1",
    "weekly_b2_not_break_weekly_b1",
    "weekly_macd_dif_gt_zero",
    "daily_b1_found_in_weekly_context",
    "daily_previous_down_found",
    "daily_first_up_found",
    "daily_strength_score_ok",
    "nearest_daily_center_or_overlap_found",
    "daily_first_up_enter_or_exceed_center",
    "daily_b2_or_2s_area_valid",
    "entry_watch_active",
    "thirty_f_b1_found",
    "daily_bottom_fractal_found",
    "five_f_b2_confirm_found",
    "entry_confidence_40",
    "entry_confidence_70",
    "entry_confidence_100",
    "entry_triggered",
    "exit_found",
]


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


def diagnosis_to_dict(diagnosis: ScanDiagnosis) -> dict[str, Any]:
    return serialize_value(diagnosis)


def build_gate_waterfall(diagnoses: list[ScanDiagnosis]) -> list[dict[str, Any]]:
    gate_maps = [{gate.name: gate for gate in diagnosis.gates} for diagnosis in diagnoses]
    rows: list[dict[str, Any]] = []
    total = len(diagnoses) or 1
    for gate_name in GATE_ORDER:
        reached = 0
        passed = 0
        failed = 0
        for gate_map in gate_maps:
            gate = gate_map.get(gate_name)
            if gate is None:
                continue
            reached += 1
            if gate.passed:
                passed += 1
            else:
                failed += 1
        rows.append(
            {
                "gate": gate_name,
                "reached": reached,
                "passed": passed,
                "failed": failed,
                "pass_rate_from_reached": round(passed / reached, 6) if reached else 0.0,
                "pass_rate_from_total": round(passed / total, 6),
            }
        )
    return rows


def build_fail_samples(diagnoses: list[ScanDiagnosis], *, limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for diagnosis in diagnoses:
        if diagnosis.failed_gate is None:
            continue
        rows.append(
            {
                "symbol_id": diagnosis.symbol.symbol_id,
                "symbol": diagnosis.symbol.symbol,
                "code": diagnosis.symbol.code,
                "exchange": diagnosis.symbol.exchange,
                "name": diagnosis.symbol.name,
                "failed_gate": diagnosis.failed_gate,
                "reason": diagnosis.failed_reason,
                "weekly_context_mode": diagnosis.weekly_context.context_mode if diagnosis.weekly_context else diagnosis.strategy_code,
                "features": next(
                    (serialize_value(gate.features) for gate in diagnosis.gates if gate.name == diagnosis.failed_gate),
                    {},
                ),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def build_candidate_samples(diagnoses: list[ScanDiagnosis], *, limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for diagnosis in diagnoses:
        if diagnosis.result is None:
            continue
        rows.append(
            {
                "symbol_id": diagnosis.symbol.symbol_id,
                "symbol": diagnosis.symbol.symbol,
                "code": diagnosis.symbol.code,
                "exchange": diagnosis.symbol.exchange,
                "name": diagnosis.symbol.name,
                "status": diagnosis.result.status.value,
                "weekly_context_mode": diagnosis.weekly_context.context_mode if diagnosis.weekly_context else None,
                "weekly_bsp_type": diagnosis.weekly_context.weekly_bsp_type if diagnosis.weekly_context else None,
                "confidence_score": diagnosis.entry.confidence_score if diagnosis.entry else None,
                "entry_level": diagnosis.entry.entry_level if diagnosis.entry else None,
                "strength_score": diagnosis.daily_setup.strength_score if diagnosis.daily_setup else None,
                "failed_gate": diagnosis.failed_gate,
            }
        )
        if len(rows) >= limit:
            break
    return rows


def build_status_samples(diagnoses: list[ScanDiagnosis], *, status: str, limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for diagnosis in diagnoses:
        if diagnosis.result is None or diagnosis.result.status.value != status:
            continue
        rows.append(
            {
                "symbol_id": diagnosis.symbol.symbol_id,
                "symbol": diagnosis.symbol.symbol,
                "name": diagnosis.symbol.name,
                "status": diagnosis.result.status.value,
                "weekly_context_mode": diagnosis.weekly_context.context_mode if diagnosis.weekly_context else None,
                "confidence_score": diagnosis.entry.confidence_score if diagnosis.entry else None,
                "entry_level": diagnosis.entry.entry_level if diagnosis.entry else None,
                "strength_score": diagnosis.daily_setup.strength_score if diagnosis.daily_setup else None,
                "failed_gate": diagnosis.failed_gate,
            }
        )
        if len(rows) >= limit:
            break
    return rows


def top_failure_gates(diagnoses: list[ScanDiagnosis], *, limit: int = 10) -> list[dict[str, Any]]:
    counter = Counter(diagnosis.failed_gate for diagnosis in diagnoses if diagnosis.failed_gate)
    return [{"gate": gate, "count": count} for gate, count in counter.most_common(limit)]


def render_gate_waterfall_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Gate Waterfall",
        "",
        "| Gate | Reached | Passed | Failed | Pass/Reached | Pass/Total |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| `{row['gate']}` | {row['reached']} | {row['passed']} | {row['failed']} | {row['pass_rate_from_reached']:.4f} | {row['pass_rate_from_total']:.4f} |"
        )
    return "\n".join(lines) + "\n"


def render_trace_markdown(diagnosis: ScanDiagnosis) -> str:
    weekly_context_mode = _trace_weekly_context_mode(diagnosis)
    weekly_b2_after_gate = _gate_by_name(diagnosis, "weekly_b2_after_weekly_b1")
    weekly_b2_not_break_gate = _gate_by_name(diagnosis, "weekly_b2_not_break_weekly_b1")
    weekly_macd_gate = _gate_by_name(diagnosis, "weekly_macd_dif_gt_zero")
    lines = [
        f"# Symbol Trace: `{diagnosis.symbol.symbol}`",
        "",
        f"- Name: `{diagnosis.symbol.name}`",
        f"- Strategy: `{diagnosis.strategy_code}`",
        f"- As of: `{diagnosis.as_of_time.isoformat()}`",
        f"- Failed gate: `{diagnosis.failed_gate or 'none'}`",
        f"- Failed reason: `{diagnosis.failed_reason or 'none'}`",
        f"- Weekly context mode: `{weekly_context_mode}`",
        "",
        "## Heads",
        "",
    ]
    for level, head in diagnosis.heads.items():
        lines.append(f"- `{level}`: `{head.bar_until.isoformat() if head else 'missing'}`")
    lines.extend(["", "## Gates", ""])
    for gate in diagnosis.gates:
        status = "PASS" if gate.passed else "FAIL"
        lines.append(f"- `{gate.name}`: {status}")
        if gate.reason:
            lines.append(f"  - reason: `{gate.reason}`")
        if gate.features:
            lines.append(f"  - features: `{serialize_value(gate.features)}`")
    lines.extend(
        [
            "",
            "## Weekly Signals",
            "",
            f"- Recent weekly signals: `{serialize_value(_recent_signal_summaries(diagnosis.weekly_signals, limit=20))}`",
            f"- Weekly buy type counts: `{serialize_value(_signal_type_counts(diagnosis.weekly_signals))}`",
        ]
    )
    if diagnosis.weekly_context is not None:
        lines.extend(
            [
                "",
                "## Weekly Context",
                "",
                f"- Weekly B1: `{diagnosis.weekly_context.weekly_b1.point_time.isoformat() if diagnosis.weekly_context.weekly_b1 else 'none'}` @ `{diagnosis.weekly_context.weekly_b1.price if diagnosis.weekly_context.weekly_b1 else 'none'}`",
                f"- Weekly signal: `{diagnosis.weekly_context.weekly_bsp_type}` `{diagnosis.weekly_context.weekly_b2.point_time.isoformat()}` @ `{diagnosis.weekly_context.weekly_b2.price}`",
                f"- Anchor: `{diagnosis.weekly_context.anchor_time.isoformat()}` source=`{diagnosis.weekly_context.anchor_source}`",
                f"- Stop reference: `{diagnosis.weekly_context.stop_reference_price}` source=`{diagnosis.weekly_context.stop_reference_source}`",
                f"- Prior weekly B1 found: `{diagnosis.weekly_context.prior_weekly_b1_found}`",
                f"- Same bar with B1: `{diagnosis.weekly_context.same_bar_with_b1}`",
                f"- Same price with B1: `{diagnosis.weekly_context.same_price_with_b1}`",
                f"- DIF/DEA: `{diagnosis.weekly_context.dif:.4f}` / `{diagnosis.weekly_context.dea:.4f}`",
            ]
        )
    else:
        latest_weekly_buy = next((signal for signal in reversed(diagnosis.weekly_signals) if signal.side == "buy"), None)
        lines.extend(
            [
                "",
                "## Weekly Context",
                "",
                f"- Weekly B1: `none`",
                f"- Weekly signal: `{serialize_value(_recent_signal_summaries([signal for signal in diagnosis.weekly_signals if signal.side == 'buy'], limit=1))}`",
                f"- Prior weekly B1 found: `{_gate_feature(weekly_b2_after_gate, 'prior_weekly_b1_found', 'unknown')}`",
                f"- Same bar with B1: `{_gate_feature(weekly_b2_after_gate, 'same_bar_with_b1', 'unknown')}`",
                f"- Same price with B1: `{_gate_feature(weekly_b2_after_gate, 'same_price_with_b1', 'unknown')}`",
                f"- Stop reference source: `{_fallback_stop_reference_source(weekly_context_mode, latest_weekly_buy)}`",
                f"- DIF/DEA: `{_gate_feature(weekly_macd_gate, 'dif', 'unknown')}` / `{_gate_feature(weekly_macd_gate, 'dea', 'unknown')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Daily Signals",
            "",
            f"- Recent daily signals: `{serialize_value(_recent_signal_summaries(diagnosis.daily_signals, limit=20))}`",
            f"- Daily buy type counts: `{serialize_value(_signal_type_counts(diagnosis.daily_signals))}`",
        ]
    )
    if diagnosis.daily_setup is not None:
        lines.extend(
            [
                "",
                "## Daily Setup",
                "",
                f"- Daily B1: `{diagnosis.daily_setup.daily_b1.point_time.isoformat()}` @ `{diagnosis.daily_setup.daily_b1.price}`",
                f"- Daily B2: `{diagnosis.daily_setup.daily_b2.point_time.isoformat() if diagnosis.daily_setup.daily_b2 else 'none'}`",
                f"- Daily B2S: `{diagnosis.daily_setup.daily_b2s.point_time.isoformat() if diagnosis.daily_setup.daily_b2s else 'none'}`",
                f"- Previous down stroke: `{diagnosis.daily_setup.previous_down_stroke.start_time.isoformat()} -> {diagnosis.daily_setup.previous_down_stroke.end_time.isoformat()}`",
                f"- First up stroke: `{diagnosis.daily_setup.first_up_stroke.start_time.isoformat()} -> {diagnosis.daily_setup.first_up_stroke.end_time.isoformat()}`",
                f"- Center: `{diagnosis.daily_setup.center_type}` `{diagnosis.daily_setup.center_low}` `{diagnosis.daily_setup.center_high}`",
                f"- Strength: `{diagnosis.daily_setup.strength_score:.2f}`",
                f"- Strength features: `{serialize_value(diagnosis.daily_setup.features)}`",
            ]
        )
    if diagnosis.entry is not None:
        lines.extend(
            [
                "",
                "## Entry",
                "",
                f"- Confidence: `{diagnosis.entry.confidence_score:.2f}`",
                f"- Entry level: `{diagnosis.entry.entry_level}`",
                f"- 30F B1: `{diagnosis.entry.thirty_b1.point_time.isoformat() if diagnosis.entry.thirty_b1 else 'none'}`",
                f"- 5F B2 confirm: `{diagnosis.entry.five_b2_confirm.point_time.isoformat() if diagnosis.entry.five_b2_confirm else 'none'}`",
                f"- Daily bottom fractal: `{diagnosis.entry.daily_bottom_time.isoformat() if diagnosis.entry.daily_bottom_time else 'none'}`",
                f"- Reasons: `{serialize_value(diagnosis.entry.reasons)}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _recent_signal_summaries(signals, *, limit: int) -> list[dict[str, Any]]:
    rows = []
    for signal in signals[-limit:]:
        rows.append(
            {
                "point_time": signal.point_time.isoformat(),
                "base_time": signal.base_time.isoformat(),
                "side": signal.side,
                "bsp_type": signal.bsp_type,
                "price": signal.price,
                "signal_type": signal.signal_type,
                "confirmed": signal.confirmed,
                "run_id": signal.run_id,
            }
        )
    return rows


def _signal_type_counts(signals) -> dict[str, int]:
    counter = Counter()
    for signal in signals:
        if signal.side != "buy":
            continue
        key = str(signal.bsp_type or signal.signal_type)
        counter[key] += 1
    return dict(counter)


def _gate_by_name(diagnosis: ScanDiagnosis, name: str):
    for gate in diagnosis.gates:
        if gate.name == name:
            return gate
    return None


def _gate_feature(gate, key: str, default: Any) -> Any:
    if gate is None:
        return default
    return serialize_value(gate.features.get(key, default))


def _trace_weekly_context_mode(diagnosis: ScanDiagnosis) -> str:
    if diagnosis.weekly_context is not None:
        return diagnosis.weekly_context.context_mode
    for gate_name in ("weekly_b1_found", "weekly_b2_after_weekly_b1", "weekly_b2_not_break_weekly_b1"):
        gate = _gate_by_name(diagnosis, gate_name)
        if gate and "weekly_context_mode" in gate.features:
            return str(gate.features["weekly_context_mode"])
    return "none"


def _fallback_stop_reference_source(context_mode: str, latest_weekly_buy) -> str:
    if context_mode == "explicit_prior_b1":
        return "weekly_b1_price"
    if latest_weekly_buy is not None and latest_weekly_buy.bsp_type:
        return f"weekly_{latest_weekly_buy.bsp_type}_price"
    return "unknown"
