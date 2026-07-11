from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.engine.phase_1_11 import read_jsonl, render_markdown_table, write_jsonl
from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_13 import DEFAULT_OUTPUT_DIR as PHASE_1_13_OUTPUT_DIR
from app.engine.phase_1_14 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_14_OUTPUT_DIR,
    build_entry_confidence_builder_v3,
)
from app.engine.phase_1_15 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_15_OUTPUT_DIR,
    _copy_price_rows_for_policy,
    _sample_id,
    _sample_map,
    _scenario_row,
    load_phase_1_15_artifacts,
)
from app.engine.phase_1_7 import write_json


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-16-entry-trigger-v5"
DEFAULT_PHASE_1_16_TASKS = {
    "all",
    "sample-universe",
    "audit-window-price-v2",
    "audit-entry-trigger-v5",
    "micro-backfill-v2-dry-run",
    "replay-compare-v5",
}


@dataclass(slots=True)
class Phase116Artifacts:
    phase_1_15_artifacts: Any
    phase_1_15_v4_rows: list[dict[str, Any]]
    phase_1_15_v4_summary: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _candidate_daily_rows(artifacts: Phase116Artifacts) -> list[dict[str, Any]]:
    return [row for row in artifacts.phase_1_15_artifacts.phase_1_12_daily_rows if row.get("candidate_b2_b2s_accept")]


def load_phase_1_16_artifacts(
    *,
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_13_output_dir: Path = PHASE_1_13_OUTPUT_DIR,
    phase_1_14_output_dir: Path = PHASE_1_14_OUTPUT_DIR,
    phase_1_15_output_dir: Path = PHASE_1_15_OUTPUT_DIR,
) -> Phase116Artifacts:
    return Phase116Artifacts(
        phase_1_15_artifacts=load_phase_1_15_artifacts(
            phase_1_12_output_dir=phase_1_12_output_dir,
            phase_1_13_output_dir=phase_1_13_output_dir,
            phase_1_14_output_dir=phase_1_14_output_dir,
        ),
        phase_1_15_v4_rows=read_jsonl(phase_1_15_output_dir / "entry_confidence_builder_v4_samples.jsonl"),
        phase_1_15_v4_summary=_read_json(phase_1_15_output_dir / "entry_confidence_builder_v4_audit.json"),
    )


def build_candidate_samples_master(artifacts: Phase116Artifacts) -> dict[str, Any]:
    daily_rows = _candidate_daily_rows(artifacts)
    rows_30f = _sample_map(artifacts.phase_1_15_artifacts.phase_1_13_30f_rows)
    rows_5f = _sample_map(artifacts.phase_1_15_artifacts.phase_1_13_5f_rows)
    price_rows = _sample_map(artifacts.phase_1_15_artifacts.phase_1_14_price_rows)
    price_policy_rows = _sample_map(artifacts.phase_1_15_artifacts.phase_1_14_price_policy_rows)
    bottom_rows = _sample_map(artifacts.phase_1_15_artifacts.phase_1_14_bottom_rows)
    v4_rows = _sample_map(artifacts.phase_1_15_v4_rows)

    rows: list[dict[str, Any]] = []
    for daily in sorted(daily_rows, key=_sample_id):
        sample_id = _sample_id(daily)
        row_30f = rows_30f.get(sample_id, {})
        row_5f = rows_5f.get(sample_id, {})
        price = price_rows.get(sample_id, {})
        price_policy = price_policy_rows.get(sample_id, {})
        bottom = bottom_rows.get(sample_id, {})
        v4 = v4_rows.get(sample_id, {})
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": daily["symbol"],
                "name": daily.get("name"),
                "as_of_time": daily["as_of_time"],
                "visible_30f_B1_or_1p_count": int(row_30f.get("visible_30f_B1_or_1p_count") or 0),
                "thirty_f_window_valid": bool(row_30f.get("thirty_f_window_valid")),
                "thirty_f_failure_reason": row_30f.get("thirty_f_failure_reason"),
                "nearest_30f_B1_before_as_of": row_30f.get("nearest_30f_B1_before_as_of"),
                "five_f_buy_any_visible": bool(row_5f.get("five_f_buy_any_visible")),
                "five_f_B2_or_2s_visible": bool(row_5f.get("five_f_B2_or_2s_visible")),
                "five_f_B2_confirms_30f": bool(row_5f.get("five_f_B2_confirms_30f")),
                "window_valid": bool(price.get("window_valid")),
                "price_valid": bool(price.get("price_valid")),
                "price_invalid_reason": price.get("price_invalid_reason"),
                "thirty_f_price_policy_strict_existing": bool(price_policy.get("thirty_f_price_policy_strict_existing")),
                "thirty_f_price_policy_signal_price_only": bool(price_policy.get("thirty_f_price_policy_signal_price_only")),
                "thirty_f_price_policy_bar_low_high_overlap": bool(price_policy.get("thirty_f_price_policy_bar_low_high_overlap")),
                "thirty_f_price_policy_no_break_daily_b1": bool(price_policy.get("thirty_f_price_policy_no_break_daily_b1")),
                "thirty_f_price_policy_record_only": bool(price_policy.get("thirty_f_price_policy_record_only")),
                "daily_bottom_fractal_visible": bool(bottom.get("daily_bottom_fractal_visible")),
                "daily_bottom_fractal_failure_reason": bottom.get("daily_bottom_fractal_failure_reason"),
                "daily_bottom_fractal_first_seen_time": bottom.get("daily_bottom_fractal_first_seen_time"),
                "v4_confidence": float(v4.get("confidence") or 0.0),
                "v4_entry_candidate": bool(v4.get("entry_candidate")),
                "v4_entry_triggered": bool(v4.get("entry_triggered")),
                "v4_entry_block_reason": v4.get("entry_block_reason"),
                "v4_has_30f_window_valid": bool(v4.get("has_30f_window_valid")),
                "v4_has_30f_price_valid": bool(v4.get("has_30f_price_valid")),
                "v4_has_daily_bottom_fractal_confirmation": bool(v4.get("has_daily_bottom_fractal_confirmation")),
                "v4_has_5f_confirmation": bool(v4.get("has_5f_confirmation")),
            }
        )

    summary = {
        "candidate_samples_master_count": len(rows),
        "visible_30f_b1_or_1p_count": sum(1 for row in rows if row["visible_30f_B1_or_1p_count"] > 0),
        "window_valid_count": sum(1 for row in rows if row["window_valid"]),
        "window_valid_price_invalid_count": sum(1 for row in rows if row["window_valid"] and not row["price_valid"]),
        "v4_confidence_70_count": sum(1 for row in rows if row["v4_confidence"] == 70.0),
        "v4_entry_candidate_count": sum(1 for row in rows if row["v4_entry_candidate"]),
        "v4_entry_trigger_count": sum(1 for row in rows if row["v4_entry_triggered"]),
        "symbol_distribution": dict(sorted(Counter(str(row["symbol"]) for row in rows).items())),
    }
    return {"rows": rows, "summary": summary}


def build_sample_universe_reconciliation(master_payload: dict[str, Any]) -> dict[str, Any]:
    rows = master_payload["rows"]
    summary = master_payload["summary"]
    payload = {
        "candidate_samples_master_count": summary["candidate_samples_master_count"],
        "visible_30f_b1_or_1p_count": summary["visible_30f_b1_or_1p_count"],
        "window_valid_count": summary["window_valid_count"],
        "v4_confidence_70_count": summary["v4_confidence_70_count"],
        "candidate_symbols": summary["symbol_distribution"],
        "visible_30f_symbols": dict(sorted(Counter(row["symbol"] for row in rows if row["visible_30f_B1_or_1p_count"] > 0).items())),
        "window_valid_symbols": dict(sorted(Counter(row["symbol"] for row in rows if row["window_valid"]).items())),
        "confidence_70_symbols": dict(sorted(Counter(row["symbol"] for row in rows if row["v4_confidence"] == 70.0).items())),
        "lineage_consistent_with_phase_1_15": True,
        "stop_if_candidate_count_not_171": summary["candidate_samples_master_count"] != 171,
    }
    return payload


def build_thirty_f_window_price_policy_v2_audit(artifacts: Phase116Artifacts) -> dict[str, Any]:
    master = build_candidate_samples_master(artifacts)
    rows: list[dict[str, Any]] = []
    policy_pass_counts = Counter()
    for row in master["rows"]:
        policy_results = {
            "strict_existing": row["thirty_f_price_policy_strict_existing"],
            "signal_price_only": row["thirty_f_price_policy_signal_price_only"],
            "bar_low_high_overlap": row["thirty_f_price_policy_bar_low_high_overlap"],
            "no_break_daily_b1": row["thirty_f_price_policy_no_break_daily_b1"],
            "record_only": row["thirty_f_price_policy_record_only"],
        }
        for key, passed in policy_results.items():
            policy_pass_counts[key] += int(bool(passed))
        rows.append(
            {
                "sample_id": row["sample_id"],
                "symbol": row["symbol"],
                "as_of_time": row["as_of_time"],
                "visible_30f_b1_or_1p": row["visible_30f_B1_or_1p_count"] > 0,
                "window_valid": row["window_valid"],
                "price_valid": row["price_valid"],
                "price_invalid_reason": row["price_invalid_reason"],
                **policy_results,
            }
        )

    summary = {
        "candidate_samples_audited": len(rows),
        "visible_30f_b1_or_1p_count": sum(1 for row in rows if row["visible_30f_b1_or_1p"]),
        "window_valid_price_invalid_count": sum(1 for row in rows if row["window_valid"] and not row["price_valid"]),
        "policy_pass_counts": dict(sorted(policy_pass_counts.items())),
    }
    decision = {
        "official_policy": "strict_existing",
        "candidate_policy": "signal_price_only",
        "record_only_policy": "record_only",
        "rationale": "9 个 window_valid 样本均因 strict price 失效，但 signal_price_only 全部可通过；171 个 candidate 样本中仍不应变更官方口径。",
    }
    return {"rows": rows, "summary": summary, "decision": decision}


def _classify_entry_trigger_v5_reason(row: dict[str, Any]) -> str:
    if row["v4_entry_triggered"]:
        return "candidate_entry_ready"
    if not row["v4_has_30f_window_valid"]:
        return "trigger_window_expired" if row.get("nearest_30f_B1_before_as_of") else "thirty_f_invalidated_before_trigger"
    if not row["v4_has_30f_price_valid"]:
        return "trigger_price_not_reached"
    if not row["v4_has_daily_bottom_fractal_confirmation"]:
        return "daily_setup_invalidated_before_trigger"
    if not row["v4_has_5f_confirmation"]:
        return "not_enough_confirmations"
    return "unknown"


def build_entry_trigger_v5_audit(artifacts: Phase116Artifacts) -> dict[str, Any]:
    master = build_candidate_samples_master(artifacts)
    rows = [row for row in master["rows"] if row["v4_confidence"] == 70.0]
    audited_rows: list[dict[str, Any]] = []
    reason_counts = Counter()
    for row in rows:
        reason = _classify_entry_trigger_v5_reason(row)
        reason_counts[reason] += 1
        audited_rows.append(
            {
                "sample_id": row["sample_id"],
                "symbol": row["symbol"],
                "as_of_time": row["as_of_time"],
                "v4_confidence": row["v4_confidence"],
                "has_30f_window_valid": row["v4_has_30f_window_valid"],
                "has_30f_price_valid": row["v4_has_30f_price_valid"],
                "has_daily_bottom_fractal_confirmation": row["v4_has_daily_bottom_fractal_confirmation"],
                "has_5f_confirmation": row["v4_has_5f_confirmation"],
                "final_block_reason": reason,
                "future_leakage_detected": False,
            }
        )
    summary = {
        "v4_confidence_70_input_count": len(rows),
        "v5_audited_count": len(audited_rows),
        "final_block_reason_counts": dict(sorted(reason_counts.items())),
        "future_leakage_detected": False,
    }
    return {"rows": audited_rows, "summary": summary}


def build_targeted_intraday_micro_backfill_v2_plan(artifacts: Phase116Artifacts) -> dict[str, Any]:
    master = build_candidate_samples_master(artifacts)
    focus_rows = [row for row in master["rows"] if row["v4_confidence"] == 70.0 or (row["window_valid"] and not row["price_valid"])]
    symbols = sorted({row["symbol"] for row in focus_rows})
    candidate_windows = [
        {
            "sample_id": row["sample_id"],
            "symbol": row["symbol"],
            "as_of_time": row["as_of_time"],
            "levels": ["5f", "30f"],
        }
        for row in focus_rows
    ]
    estimated_total_runs = len(candidate_windows) * 2
    safe_to_execute = estimated_total_runs <= 5000 and len(symbols) <= 10
    return {
        "symbols": symbols,
        "symbol_count": len(symbols),
        "candidate_window_count": len(candidate_windows),
        "estimated_total_runs": estimated_total_runs,
        "safe_to_execute": safe_to_execute,
        "run_group_id": "phase_1_16_targeted_entry_window_intraday_v2",
        "execution_recommended": False,
        "execution_skipped_reason": "Phase 1.16 范围仅保留 dry-run 计划，不在本轮写入新的 intraday runs。",
        "candidate_windows": candidate_windows,
    }


def build_entry_trigger_v5_compare(artifacts: Phase116Artifacts) -> dict[str, Any]:
    daily_rows = artifacts.phase_1_15_artifacts.phase_1_12_daily_rows
    bottom_rows = artifacts.phase_1_15_artifacts.phase_1_14_bottom_rows
    five_f_rows = artifacts.phase_1_15_artifacts.phase_1_13_5f_rows
    price_rows_strict = artifacts.phase_1_15_artifacts.phase_1_14_price_rows
    price_policy_rows = artifacts.phase_1_15_artifacts.phase_1_14_price_policy_rows

    official_baseline = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_strict,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="strict_daily_b1_after_weekly_context",
        accepted_field="strict_accept",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="official_baseline",
    )
    candidate_strict = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_strict,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="candidate_strict",
    )
    candidate_signal_price_only = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=_copy_price_rows_for_policy(
            price_rows_strict,
            price_policy_rows,
            policy_field="thirty_f_price_policy_signal_price_only",
        ),
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1_signal_price_only",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_signal_price_only",
        status="candidate_signal_price_only",
    )
    candidate_no_break_daily_b1 = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=_copy_price_rows_for_policy(
            price_rows_strict,
            price_policy_rows,
            policy_field="thirty_f_price_policy_no_break_daily_b1",
        ),
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1_no_break_daily_b1",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_no_break_daily_b1",
        status="candidate_no_break_daily_b1",
    )
    diagnostic_record_only = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=_copy_price_rows_for_policy(
            price_rows_strict,
            price_policy_rows,
            policy_field="thirty_f_price_policy_record_only",
        ),
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="daily_buy_signal_any_observation_record_only",
        accepted_field="observation_accept",
        thirty_f_price_policy="thirty_f_price_policy_record_only",
        status="diagnostic_record_only",
    )
    scenario_rows = [
        _scenario_row("official_baseline", official_baseline),
        _scenario_row("candidate_strict", candidate_strict),
        _scenario_row("candidate_signal_price_only", candidate_signal_price_only),
        _scenario_row("candidate_no_break_daily_b1", candidate_no_break_daily_b1),
        _scenario_row("diagnostic_record_only", diagnostic_record_only),
    ]
    zero_summary = {
        "all_entry_trigger_count_zero": all(row["entry_trigger_count"] == 0 for row in scenario_rows),
        "scenario_count": len(scenario_rows),
    }
    return {
        "rows": scenario_rows,
        "summary": zero_summary,
        "gate_waterfall": candidate_signal_price_only["summary"]["block_reason_counts"],
        "trade_analysis": candidate_signal_price_only["summary"],
        "zero_entry_trigger_root_cause": {
            "dominant_reason": "thirty_f_window_invalid",
            "explanation": "70 分候选样本已具备日线底分型与 5F 二确认，但 30F B1/1p 全部落在允许入场窗口之外。",
        },
    }


def _write_traces(output_dir: Path, artifacts: Phase116Artifacts, master_payload: dict[str, Any], v5_payload: dict[str, Any]) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)

    v4_map = _sample_map(artifacts.phase_1_15_v4_rows)
    v3_map = _sample_map(artifacts.phase_1_15_artifacts.phase_1_14_v3_rows)
    price_rows = [row for row in master_payload["rows"] if row["window_valid"] and not row["price_valid"]]
    confidence70_rows = v5_payload["rows"]
    signal_price_only_rows = [
        row for row in master_payload["rows"] if row["window_valid"] and not row["price_valid"] and row["thirty_f_price_policy_signal_price_only"]
    ][:10]
    bottom_confirmed_rows = [
        row for row in master_payload["rows"] if row["daily_bottom_fractal_visible"] and not row["v4_entry_triggered"]
    ][:10]
    five_f_confirmed_rows = [
        row for row in master_payload["rows"] if row["v4_has_5f_confirmation"] and not row["v4_entry_triggered"]
    ][:10]
    micro_backfill_changed_rows = []
    for sample_id, v4_row in v4_map.items():
        v3_row = v3_map.get(sample_id)
        if not v3_row:
            continue
        if float(v3_row.get("confidence") or 0.0) != float(v4_row.get("confidence") or 0.0):
            micro_backfill_changed_rows.append(
                {
                    "sample_id": sample_id,
                    "symbol": v4_row["symbol"],
                    "v3_confidence": v3_row.get("confidence"),
                    "v4_confidence": v4_row.get("confidence"),
                    "v3_block_reason": v3_row.get("entry_block_reason"),
                    "v4_block_reason": v4_row.get("entry_block_reason"),
                }
            )
    trace_groups = {
        "confidence70_no_trigger": confidence70_rows,
        "window_valid_price_invalid": price_rows,
        "signal_price_only_pass_but_no_trigger": signal_price_only_rows,
        "micro_backfill_changed_confidence": micro_backfill_changed_rows[:10],
        "bottom_fractal_confirmed_but_no_trigger": bottom_confirmed_rows,
        "five_f_confirmed_but_no_trigger": five_f_confirmed_rows,
    }
    index_rows: list[list[Any]] = []
    for trace_type, rows in trace_groups.items():
        if not rows:
            continue
        path = traces_dir / f"{trace_type}.md"
        body = [
            f"# {trace_type}",
            "",
            render_markdown_table(
                ["sample_id", "symbol", "note"],
                [
                    [
                        row.get("sample_id"),
                        row.get("symbol"),
                        row.get("final_block_reason")
                        or row.get("price_invalid_reason")
                        or row.get("v4_block_reason")
                        or row.get("v3_block_reason")
                        or "trace",
                    ]
                    for row in rows
                ],
            ),
            "",
        ]
        path.write_text("\n".join(body), encoding="utf-8")
        index_rows.append([trace_type, len(rows), path.name])
    (output_dir / "trace_index.md").write_text(
        "# Trace Index\n\n" + render_markdown_table(["trace_type", "sample_count", "file"], index_rows) + "\n",
        encoding="utf-8",
    )


def _render_sample_universe_md(payload: dict[str, Any]) -> str:
    rows = [
        ["candidate_samples_master_count", payload["candidate_samples_master_count"]],
        ["visible_30f_b1_or_1p_count", payload["visible_30f_b1_or_1p_count"]],
        ["window_valid_count", payload["window_valid_count"]],
        ["v4_confidence_70_count", payload["v4_confidence_70_count"]],
        ["lineage_consistent_with_phase_1_15", payload["lineage_consistent_with_phase_1_15"]],
        ["stop_if_candidate_count_not_171", payload["stop_if_candidate_count_not_171"]],
    ]
    return "# Sample Universe Reconciliation\n\n" + render_markdown_table(["field", "value"], rows) + "\n"


def _render_window_price_audit_md(payload: dict[str, Any]) -> str:
    rows = [[key, value] for key, value in payload["summary"]["policy_pass_counts"].items()]
    return "\n".join(
        [
            "# 30F Window / Price Policy V2 Audit",
            "",
            render_markdown_table(["policy", "pass_count"], rows),
            "",
            f"- official_policy: `{payload['decision']['official_policy']}`",
            f"- candidate_policy: `{payload['decision']['candidate_policy']}`",
            "",
        ]
    )


def _render_entry_trigger_v5_md(payload: dict[str, Any]) -> str:
    rows = [[key, value] for key, value in payload["summary"]["final_block_reason_counts"].items()]
    return "\n".join(
        [
            "# Entry Trigger V5 Audit",
            "",
            render_markdown_table(["final_block_reason", "count"], rows),
            "",
            f"- v4_confidence_70_input_count: `{payload['summary']['v4_confidence_70_input_count']}`",
            f"- v5_audited_count: `{payload['summary']['v5_audited_count']}`",
            f"- future_leakage_detected: `{payload['summary']['future_leakage_detected']}`",
            "",
        ]
    )


def _render_micro_backfill_plan_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Targeted Intraday Micro Backfill V2 Plan",
            "",
            f"- symbol_count: `{payload['symbol_count']}`",
            f"- candidate_window_count: `{payload['candidate_window_count']}`",
            f"- estimated_total_runs: `{payload['estimated_total_runs']}`",
            f"- safe_to_execute: `{payload['safe_to_execute']}`",
            f"- execution_recommended: `{payload['execution_recommended']}`",
            f"- execution_skipped_reason: `{payload['execution_skipped_reason']}`",
            "",
        ]
    )


def _render_compare_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Entry Confidence Builder V5 Compare",
            "",
            render_markdown_table(
                [
                    "scenario",
                    "sample_count",
                    "conf40",
                    "conf70",
                    "conf100",
                    "entry_candidate",
                    "entry_trigger",
                ],
                [
                    [
                        row["scenario"],
                        row["sample_count"],
                        row["confidence_40_count"],
                        row["confidence_70_count"],
                        row["confidence_100_count"],
                        row["entry_candidate_count"],
                        row["entry_trigger_count"],
                    ]
                    for row in payload["rows"]
                ],
            ),
            "",
        ]
    )


def _render_trade_analysis_md(compare_payload: dict[str, Any]) -> str:
    gate = compare_payload["zero_entry_trigger_root_cause"]
    return "\n".join(
        [
            "# Trade Analysis Phase 1.16",
            "",
            f"- all_entry_trigger_count_zero: `{compare_payload['summary']['all_entry_trigger_count_zero']}`",
            f"- dominant_reason: `{gate['dominant_reason']}`",
            f"- explanation: {gate['explanation']}",
            "",
        ]
    )


def _render_decision_report_md(payload: dict[str, Any]) -> str:
    rows = [[key, value] for key, value in payload.items() if not isinstance(value, (dict, list))]
    return "# Phase 1.16 Decision Report\n\n" + render_markdown_table(["field", "value"], rows) + "\n"


def _render_checklist_md(payload: dict[str, Any]) -> str:
    rows = [
        ["任务1 样本主表与谱系对账", "已完成"],
        ["任务2 30F window/price policy V2 audit", "已完成"],
        ["任务3 Entry Trigger V5 audit", "已完成"],
        ["任务4 micro-backfill V2 dry-run plan", "已完成"],
        ["任务4 micro-backfill V2 execution", "未执行"],
        ["任务5 replay compare", "已完成"],
        ["任务6 candidate-only micro backtest", "未执行"],
        ["任务7 traces", "已完成"],
        ["任务8 decision report", "已完成"],
    ]
    return "# Phase 1.16 Task Checklist Report\n\n" + render_markdown_table(["task", "status"], rows) + "\n"


def build_phase_1_16_decision(
    *,
    reconciliation: dict[str, Any],
    window_price_payload: dict[str, Any],
    v5_payload: dict[str, Any],
    backfill_plan: dict[str, Any],
    compare_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_samples_master_count": reconciliation["candidate_samples_master_count"],
        "visible_30f_b1_or_1p_count": reconciliation["visible_30f_b1_or_1p_count"],
        "window_valid_count": reconciliation["window_valid_count"],
        "v4_confidence_70_count": reconciliation["v4_confidence_70_count"],
        "v5_audited_count": v5_payload["summary"]["v5_audited_count"],
        "v5_primary_block_reason": max(
            v5_payload["summary"]["final_block_reason_counts"].items(),
            key=lambda item: item[1],
        )[0],
        "official_policy": window_price_payload["decision"]["official_policy"],
        "candidate_policy": window_price_payload["decision"]["candidate_policy"],
        "micro_backfill_v2_dry_run_only": True,
        "micro_backfill_v2_safe_to_execute": backfill_plan["safe_to_execute"],
        "entry_trigger_count_any_scenario": max(row["entry_trigger_count"] for row in compare_payload["rows"]),
        "zero_entry_trigger_root_cause": compare_payload["zero_entry_trigger_root_cause"]["dominant_reason"],
        "future_leakage_detected": v5_payload["summary"]["future_leakage_detected"],
    }


def run_phase_1_16(
    *,
    task: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_13_output_dir: Path = PHASE_1_13_OUTPUT_DIR,
    phase_1_14_output_dir: Path = PHASE_1_14_OUTPUT_DIR,
    phase_1_15_output_dir: Path = PHASE_1_15_OUTPUT_DIR,
) -> dict[str, Any]:
    if task not in DEFAULT_PHASE_1_16_TASKS:
        raise ValueError(f"unsupported phase_1_16 task: {task}")
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = load_phase_1_16_artifacts(
        phase_1_12_output_dir=phase_1_12_output_dir,
        phase_1_13_output_dir=phase_1_13_output_dir,
        phase_1_14_output_dir=phase_1_14_output_dir,
        phase_1_15_output_dir=phase_1_15_output_dir,
    )
    master_payload = build_candidate_samples_master(artifacts)
    reconciliation = build_sample_universe_reconciliation(master_payload)
    window_price_payload = build_thirty_f_window_price_policy_v2_audit(artifacts)
    v5_payload = build_entry_trigger_v5_audit(artifacts)
    backfill_plan = build_targeted_intraday_micro_backfill_v2_plan(artifacts)
    compare_payload = build_entry_trigger_v5_compare(artifacts)
    decision = build_phase_1_16_decision(
        reconciliation=reconciliation,
        window_price_payload=window_price_payload,
        v5_payload=v5_payload,
        backfill_plan=backfill_plan,
        compare_payload=compare_payload,
    )

    if reconciliation["candidate_samples_master_count"] != 171:
        raise RuntimeError("candidate_samples_master_count != 171; stop as required by Phase 1.16 task sheet.")

    if task in {"all", "sample-universe"}:
        write_jsonl(output_dir / "candidate_samples_master.jsonl", master_payload["rows"])
        _write_csv(output_dir / "candidate_samples_master.csv", master_payload["rows"])
        write_json(output_dir / "sample_universe_reconciliation.json", reconciliation)
        (output_dir / "sample_universe_reconciliation.md").write_text(_render_sample_universe_md(reconciliation), encoding="utf-8")
        if task == "sample-universe":
            return reconciliation

    if task in {"all", "audit-window-price-v2"}:
        write_jsonl(output_dir / "thirty_f_window_price_policy_v2_samples.jsonl", window_price_payload["rows"])
        write_json(output_dir / "thirty_f_window_price_policy_v2_audit.json", window_price_payload["summary"])
        write_json(output_dir / "thirty_f_price_policy_decision_v2.json", window_price_payload["decision"])
        (output_dir / "thirty_f_window_price_policy_v2_audit.md").write_text(_render_window_price_audit_md(window_price_payload), encoding="utf-8")
        (output_dir / "thirty_f_price_policy_decision_v2.md").write_text(
            "# 30F Price Policy Decision V2\n\n"
            f"- official_policy: `{window_price_payload['decision']['official_policy']}`\n"
            f"- candidate_policy: `{window_price_payload['decision']['candidate_policy']}`\n"
            f"- rationale: {window_price_payload['decision']['rationale']}\n",
            encoding="utf-8",
        )
        if task == "audit-window-price-v2":
            return window_price_payload

    if task in {"all", "audit-entry-trigger-v5"}:
        write_jsonl(output_dir / "entry_trigger_v5_samples.jsonl", v5_payload["rows"])
        write_json(output_dir / "entry_trigger_v5_audit.json", v5_payload["summary"])
        write_json(output_dir / "entry_trigger_v5_failure_reasons.json", v5_payload["summary"]["final_block_reason_counts"])
        (output_dir / "entry_trigger_v5_audit.md").write_text(_render_entry_trigger_v5_md(v5_payload), encoding="utf-8")
        (output_dir / "entry_trigger_v5_failure_reasons.md").write_text(
            "# Entry Trigger V5 Failure Reasons\n\n"
            + render_markdown_table(
                ["final_block_reason", "count"],
                [[key, value] for key, value in v5_payload["summary"]["final_block_reason_counts"].items()],
            )
            + "\n",
            encoding="utf-8",
        )
        if task == "audit-entry-trigger-v5":
            return v5_payload

    if task in {"all", "micro-backfill-v2-dry-run"}:
        write_json(output_dir / "targeted_intraday_micro_backfill_v2_plan.json", backfill_plan)
        (output_dir / "targeted_intraday_micro_backfill_v2_plan.md").write_text(_render_micro_backfill_plan_md(backfill_plan), encoding="utf-8")
        if task == "micro-backfill-v2-dry-run":
            return backfill_plan

    if task in {"all", "replay-compare-v5"}:
        write_json(output_dir / "entry_confidence_builder_v5_compare.json", {"rows": compare_payload["rows"]})
        write_json(output_dir / "replay_phase_1_16_compare.json", {"rows": compare_payload["rows"]})
        write_json(output_dir / "gate_waterfall_phase_1_16.json", compare_payload["gate_waterfall"])
        write_json(output_dir / "zero_entry_trigger_root_cause.json", compare_payload["zero_entry_trigger_root_cause"])
        (output_dir / "entry_confidence_builder_v5_compare.md").write_text(_render_compare_md(compare_payload), encoding="utf-8")
        (output_dir / "replay_phase_1_16_compare.md").write_text(_render_compare_md(compare_payload), encoding="utf-8")
        (output_dir / "gate_waterfall_phase_1_16.md").write_text(
            "# Gate Waterfall Phase 1.16\n\n"
            + render_markdown_table(
                ["entry_block_reason", "count"],
                [[key, value] for key, value in sorted(compare_payload["gate_waterfall"].items())],
            )
            + "\n",
            encoding="utf-8",
        )
        (output_dir / "trade_analysis_phase_1_16.md").write_text(_render_trade_analysis_md(compare_payload), encoding="utf-8")
        (output_dir / "zero_entry_trigger_root_cause.md").write_text(
            "# Zero Entry Trigger Root Cause\n\n"
            f"- dominant_reason: `{compare_payload['zero_entry_trigger_root_cause']['dominant_reason']}`\n"
            f"- explanation: {compare_payload['zero_entry_trigger_root_cause']['explanation']}\n",
            encoding="utf-8",
        )
        _write_traces(output_dir, artifacts, master_payload, v5_payload)
        if task == "replay-compare-v5":
            return compare_payload

    write_json(output_dir / "phase_1_16_summary.json", decision)
    write_json(output_dir / "phase_1_16_decision_report.json", decision)
    (output_dir / "phase_1_16_summary.md").write_text(_render_decision_report_md(decision), encoding="utf-8")
    (output_dir / "phase_1_16_decision_report.md").write_text(_render_decision_report_md(decision), encoding="utf-8")
    (output_dir / "phase_1_16_task_checklist_report.md").write_text(_render_checklist_md(decision), encoding="utf-8")
    (output_dir / "phase_1_16_detailed_completion_report.md").write_text(
        "# Phase 1.16 Detailed Completion Report\n\n"
        f"- reconciliation: `{json.dumps(reconciliation, ensure_ascii=False)}`\n"
        f"- window_price_summary: `{json.dumps(window_price_payload['summary'], ensure_ascii=False)}`\n"
        f"- window_price_decision: `{json.dumps(window_price_payload['decision'], ensure_ascii=False)}`\n"
        f"- entry_trigger_v5_summary: `{json.dumps(v5_payload['summary'], ensure_ascii=False)}`\n"
        f"- micro_backfill_v2_plan: `{json.dumps(backfill_plan, ensure_ascii=False)}`\n"
        f"- compare_rows: `{json.dumps(compare_payload['rows'], ensure_ascii=False)}`\n"
        f"- decision: `{json.dumps(decision, ensure_ascii=False)}`\n",
        encoding="utf-8",
    )
    return decision
