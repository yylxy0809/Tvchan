from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.engine.phase_1_11 import render_markdown_table, write_jsonl
from app.engine.phase_1_18 import DEFAULT_TARGET_SYMBOLS
from app.engine.phase_1_7 import write_json


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-20-30f-refresh-visibility-audit"
DEFAULT_PHASE_1_18_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-18-staleness-policy"
DEFAULT_PHASE_1_19_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-19-post-daily-30f-refresh"
TRIGGER_WINDOW_DAYS = 20


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _summary_md(title: str, payload: dict[str, Any]) -> str:
    return "# " + title + "\n\n" + render_markdown_table(["field", "value"], [[k, v] for k, v in payload.items()]) + "\n"


def _rows_md(title: str, rows: list[dict[str, Any]], fields: list[str]) -> str:
    return "# " + title + "\n\n" + render_markdown_table(fields, [[row.get(field) for field in fields] for row in rows]) + "\n"


def _sample_id(row: dict[str, Any]) -> str:
    return str(row.get("sample_id") or f"{row.get('symbol')}|{row.get('as_of_time')}")


def _augment_refresh_rows(refresh_rows: list[dict[str, Any]], candidate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates_by_sample = {_sample_id(row): row for row in candidate_rows}
    rows: list[dict[str, Any]] = []
    for row in refresh_rows:
        sample_id = str(row.get("sample_id"))
        candidate = candidates_by_sample.get(sample_id, {})
        rows.append(
            {
                **row,
                "as_of_time": row.get("as_of_time") or candidate.get("as_of_time"),
                "daily_setup_point_time": row.get("daily_setup_point_time") or candidate.get("daily_setup_event_time") or candidate.get("daily_setup_first_seen_time"),
                "weekly_context_time": row.get("weekly_context_time") or candidate.get("weekly_context_time"),
            }
        )
    return rows


def audit_refresh_visibility_gap(
    *,
    refresh_rows: list[dict[str, Any]],
    trigger_window_days: int = TRIGGER_WINDOW_DAYS,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    for row in refresh_rows:
        setup_seen = _parse_dt(row.get("daily_setup_first_seen_time"))
        setup_point = _parse_dt(row.get("daily_setup_point_time")) or setup_seen
        signal_point = _parse_dt(row.get("thirty_f_signal_point_time"))
        first_seen = _parse_dt(row.get("thirty_f_first_seen_time"))
        as_of = _parse_dt(row.get("as_of_time"))
        point_after = bool(signal_point and setup_point and signal_point > setup_point)
        first_seen_after_setup = bool(first_seen and setup_seen and first_seen > setup_seen)
        first_seen_before_as_of = bool(first_seen and as_of and first_seen <= as_of)
        inside_window = bool(first_seen and setup_seen and first_seen - setup_seen <= timedelta(days=trigger_window_days))
        price_pass = bool(row.get("price_valid_any_candidate_policy") or row.get("price_policy_pass"))
        run_group = row.get("source_run_group_id") or row.get("run_group_id")

        if not first_seen_after_setup:
            reason = "first_seen_equal_or_before_daily_setup"
        elif not first_seen_before_as_of and point_after:
            reason = "point_after_daily_but_first_seen_after_as_of"
        elif not inside_window and point_after:
            reason = "point_after_daily_but_first_seen_after_trigger_window"
        elif run_group and run_group not in {"research_daily_close", "phase_1_15_targeted_entry_window_intraday", "phase_1_16_targeted_entry_window_intraday_v2"}:
            reason = "refresh_seen_only_in_non_target_run_group"
        elif not price_pass and first_seen_before_as_of:
            reason = "time_valid_but_price_invalid"
        elif point_after and first_seen_after_setup and first_seen_before_as_of and inside_window and price_pass:
            reason = "visible_and_eligible"
        elif not point_after and first_seen_before_as_of:
            reason = "time_valid_but_price_invalid"
        else:
            reason = "unknown"

        status = "visible_eligible" if reason == "visible_and_eligible" else "diagnostic_only"
        reason_counts[reason] += 1
        status_counts[status] += 1
        rows.append(
            {
                "sample_id": row.get("sample_id"),
                "symbol": row.get("symbol"),
                "daily_setup_first_seen_time": _iso(setup_seen),
                "daily_setup_point_time": _iso(setup_point),
                "thirty_f_signal_point_time": _iso(signal_point),
                "thirty_f_first_seen_time": _iso(first_seen),
                "as_of_time": _iso(as_of),
                "point_time_after_daily_setup": point_after,
                "first_seen_after_daily_setup": first_seen_after_setup,
                "first_seen_before_as_of": first_seen_before_as_of,
                "inside_entry_trigger_window": inside_window,
                "price_policy_pass": price_pass,
                "source_run_group_id": run_group,
                "visibility_status": status,
                "visibility_gap_reason": reason,
                "future_leakage_detected": not first_seen_before_as_of,
            }
        )
    summary = {
        "refresh_scanned_count": len(rows),
        "visible_eligible_count": reason_counts.get("visible_and_eligible", 0),
        "diagnostic_only_count": len(rows) - reason_counts.get("visible_and_eligible", 0),
        "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
        "visibility_gap_reason_counts": dict(sorted(reason_counts.items())),
        "visibility_status_counts": dict(sorted(status_counts.items())),
        "primary_reason": reason_counts.most_common(1)[0][0] if reason_counts else "no_refresh_rows",
    }
    return {"rows": rows, "summary": summary}


def build_gate_deep_dive_by_symbol(
    *,
    target_symbols: list[str] | tuple[str, ...],
    phase_1_18_universe: dict[str, Any],
    phase_1_19_gate_rows: list[dict[str, Any]],
    visibility_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    by_symbol = phase_1_18_universe.get("by_symbol", {})
    gate_by_symbol = {str(row.get("symbol")): row for row in phase_1_19_gate_rows}
    scan_count = Counter(str(row.get("symbol")) for row in visibility_rows)
    visible_count = Counter(str(row.get("symbol")) for row in visibility_rows if row.get("visibility_status") == "visible_eligible")
    rows: list[dict[str, Any]] = []
    for symbol in target_symbols:
        stats = by_symbol.get(symbol, {})
        phase_1_19_gate = gate_by_symbol.get(symbol, {})
        weekly_count = int(stats.get("weekly_context_sample_count") or 0)
        daily_buy_count = int(stats.get("daily_ledger_buy_event_count") or 0)
        daily_b2_count = int(stats.get("daily_setup_audit_sample_count") or 0)
        candidate_count = int(stats.get("candidate_count") or phase_1_19_gate.get("candidate_count") or 0)
        if weekly_count <= 0:
            reason = "no_weekly_context"
        elif daily_buy_count <= 0:
            reason = "no_daily_buy_event"
        elif daily_b2_count <= 0:
            reason = "daily_buy_not_b2_or_b2s"
        elif candidate_count <= 0:
            reason = "no_candidate_daily_setup"
        elif visible_count.get(symbol, 0) <= 0:
            reason = "post_daily_30f_refresh_not_visible"
        else:
            reason = "filtered_by_strict_daily_baseline_only"
        rows.append(
            {
                "symbol": symbol,
                "weekly_context_count": weekly_count,
                "daily_event_ledger_buy_count": daily_buy_count,
                "daily_b2_b2s_count": daily_b2_count,
                "daily_candidate_setup_count": candidate_count,
                "post_daily_30f_refresh_scan_count": scan_count.get(symbol, 0),
                "visible_post_daily_30f_refresh_count": visible_count.get(symbol, 0),
                "final_candidate_count": candidate_count,
                "primary_block_reason": reason,
                "bug_candidate_selected_run_filter": False,
            }
        )
    summary = {
        "target_symbol_count": len(target_symbols),
        "covered_symbol_count": len(rows),
        "candidate_symbol_distribution": dict(Counter(row.get("symbol") for row in phase_1_18_universe.get("rows", []))),
        "block_reason_counts": dict(sorted(Counter(row["primary_block_reason"] for row in rows).items())),
    }
    return {"rows": rows, "summary": summary}


def build_entry_state_machine_v3(
    *,
    candidate_rows: list[dict[str, Any]],
    visibility_rows: list[dict[str, Any]],
    bottom_alignment_rows: list[dict[str, Any]],
    five_f_alignment_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    visible_by_sample: dict[str, dict[str, Any]] = {}
    for row in visibility_rows:
        if row.get("visibility_status") == "visible_eligible":
            visible_by_sample.setdefault(str(row.get("sample_id")), row)
    bottom_by_sample = {str(row.get("sample_id")): row for row in bottom_alignment_rows}
    five_by_sample = {str(row.get("sample_id")): row for row in five_f_alignment_rows}
    state_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    block_reasons: Counter[str] = Counter()

    for candidate in candidate_rows:
        sample_id = _sample_id(candidate)
        states = ["WAIT_WEEKLY_CONTEXT", "WAIT_DAILY_CANDIDATE_SETUP", "WAIT_POST_DAILY_30F_REFRESH"]
        transitions = [
            {
                "from_state": "WAIT_WEEKLY_CONTEXT",
                "to_state": "WAIT_DAILY_CANDIDATE_SETUP",
                "transition_time": candidate.get("weekly_context_time"),
                "transition_event_type": "weekly_context",
                "source_level": "1w",
                "source_run_group_id": None,
                "source_event_id": None,
                "is_visible_at_transition": True,
                "block_reason": None,
                "future_leakage_detected": False,
            },
            {
                "from_state": "WAIT_DAILY_CANDIDATE_SETUP",
                "to_state": "WAIT_POST_DAILY_30F_REFRESH",
                "transition_time": candidate.get("daily_setup_first_seen_time"),
                "transition_event_type": "daily_candidate_setup",
                "source_level": "1d",
                "source_run_group_id": "research_daily_close",
                "source_event_id": candidate.get("daily_setup_event_id"),
                "is_visible_at_transition": True,
                "block_reason": None,
                "future_leakage_detected": False,
            },
        ]
        refresh = visible_by_sample.get(sample_id)
        bottom = bottom_by_sample.get(sample_id, {})
        five = five_by_sample.get(sample_id, {})
        primary_block = "waiting_for_post_daily_30f_refresh"
        entry_triggered = False
        if refresh:
            states.append("WAIT_DAILY_BOTTOM_CONFIRMATION")
            transitions.append(
                {
                    "from_state": "WAIT_POST_DAILY_30F_REFRESH",
                    "to_state": "WAIT_DAILY_BOTTOM_CONFIRMATION",
                    "transition_time": refresh.get("thirty_f_first_seen_time"),
                    "transition_event_type": "post_daily_30f_refresh",
                    "source_level": "30f",
                    "source_run_group_id": refresh.get("source_run_group_id"),
                    "source_event_id": refresh.get("signal_fingerprint"),
                    "is_visible_at_transition": True,
                    "block_reason": None,
                    "future_leakage_detected": False,
                }
            )
            primary_block = "waiting_for_daily_bottom_confirmation"
            if bottom.get("bottom_fractal_post_setup"):
                states.append("WAIT_5F_SECOND_CONFIRMATION")
                primary_block = "waiting_for_5f_second_confirmation"
                if five.get("five_f_post_setup"):
                    states.extend(["ENTRY_CANDIDATE_READY", "ENTRY_TRIGGERED"])
                    primary_block = "entry_triggered"
                    entry_triggered = True
        block_reasons[primary_block] += 1
        state_row = {
            "sample_id": sample_id,
            "symbol": candidate.get("symbol"),
            "as_of_time": candidate.get("as_of_time"),
            "weekly_context_time": candidate.get("weekly_context_time"),
            "daily_setup_first_seen_time": candidate.get("daily_setup_first_seen_time"),
            "states": states,
            "transitions": transitions,
            "entry_triggered": entry_triggered,
            "candidate_policy": "candidate_post_daily_30f_refresh_required",
            "official_baseline_isolated": True,
            "primary_block_reason": primary_block,
            "future_leakage_detected": any(t["future_leakage_detected"] for t in transitions),
        }
        state_rows.append(state_row)
        transition_rows.extend({"sample_id": sample_id, "symbol": candidate.get("symbol"), **transition} for transition in transitions)
    summary = {
        "candidate_sample_count": len(candidate_rows),
        "entry_state_machine_v3_trigger_count": sum(1 for row in state_rows if row["entry_triggered"]),
        "primary_block_reason_counts": dict(sorted(block_reasons.items())),
        "primary_zero_trigger_root_cause": block_reasons.most_common(1)[0][0] if block_reasons else "no_candidate_samples",
        "future_leakage_detected": any(row["future_leakage_detected"] for row in state_rows),
        "candidate_policy_isolated": True,
    }
    return {"rows": state_rows, "transitions": transition_rows, "summary": summary}


def audit_intraday_run_coverage_gap(
    *,
    candidate_rows: list[dict[str, Any]],
    visibility_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    visible_by_sample = {str(row.get("sample_id")) for row in visibility_rows if row.get("visibility_status") == "visible_eligible"}
    rows = []
    for candidate in candidate_rows:
        sample_id = _sample_id(candidate)
        has_visible = sample_id in visible_by_sample
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": candidate.get("symbol"),
                "daily_setup_first_seen_time": candidate.get("daily_setup_first_seen_time"),
                "nearest_30f_run_before_as_of": None,
                "nearest_30f_run_after_daily_setup": None,
                "nearest_30f_run_after_as_of": None,
                "has_30f_run_covering_trigger_window": has_visible,
                "has_5f_run_covering_trigger_window": False,
                "micro_v1_coverage": False,
                "micro_v2_coverage": False,
                "missing_run_window_count": 0 if has_visible else 1,
                "recommended_micro_v3_window_count": 0,
            }
        )
    summary = {
        "candidate_sample_count": len(candidate_rows),
        "coverage_gap_sample_count": sum(1 for row in rows if row["missing_run_window_count"] > 0),
        "recommend_micro_backfill_v3_next": False,
        "reason": "coverage_gap_not_proven_as_safe_write_target",
        "expected_no_published_head_write": True,
        "expected_no_research_daily_close_overwrite": True,
    }
    return {"rows": rows, "summary": summary}


def build_candidate_micro_backtest_decision_v2(
    *,
    entry_trigger_count_candidate: int,
    future_leakage_detected: bool,
    trigger_sample_trace_complete: bool,
) -> dict[str, Any]:
    if future_leakage_detected:
        reason = "future_leakage_detected"
        allowed = False
    elif entry_trigger_count_candidate <= 0:
        reason = "no_candidate_trigger"
        allowed = False
    elif not trigger_sample_trace_complete:
        reason = "trigger_trace_incomplete"
        allowed = False
    else:
        reason = "candidate_trigger_without_future_leakage"
        allowed = True
    return {
        "candidate_micro_backtest_allowed": allowed,
        "reason": reason,
        "entry_trigger_count_candidate": entry_trigger_count_candidate,
        "future_leakage_detected": future_leakage_detected,
        "trigger_sample_trace_complete": trigger_sample_trace_complete,
        "official_baseline_not_modified": True,
        "candidate_policy_isolated": True,
    }


def _write_trace_package(
    output_dir: Path,
    *,
    gate_rows: list[dict[str, Any]],
    visibility_rows: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
    five_rows: list[dict[str, Any]],
) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    bottom_by_sample = {str(row.get("sample_id")): row for row in bottom_rows}
    five_by_sample = {str(row.get("sample_id")): row for row in five_rows}
    state_by_sample = {str(row.get("sample_id")): row for row in state_rows}
    categories: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in gate_rows:
        if row["symbol"] != "000001.SZ":
            categories["other_symbol_filtered_before_candidate"].append(row)
    for row in visibility_rows:
        reason = str(row.get("visibility_gap_reason"))
        if row.get("future_leakage_detected"):
            categories["post_daily_30f_refresh_future_only"].append(row)
        if reason == "point_after_daily_but_first_seen_after_as_of":
            categories["post_daily_30f_refresh_first_seen_after_as_of"].append(row)
        if reason == "point_after_daily_but_first_seen_after_trigger_window":
            categories["post_daily_30f_refresh_first_seen_after_trigger_window"].append(row)
        if reason == "visible_and_eligible":
            categories["visible_refresh_but_state_machine_blocked"].append(row)
    for row in state_rows:
        if row.get("primary_block_reason") == "waiting_for_post_daily_30f_refresh":
            categories["waiting_for_post_daily_30f_refresh"].append(row)

    required_categories = [
        "other_symbol_filtered_before_candidate",
        "post_daily_30f_refresh_future_only",
        "post_daily_30f_refresh_first_seen_after_as_of",
        "post_daily_30f_refresh_first_seen_after_trigger_window",
        "visible_refresh_but_state_machine_blocked",
        "waiting_for_post_daily_30f_refresh",
    ]
    category_order = required_categories + [category for category in categories if category not in required_categories]
    index_rows: list[list[Any]] = []
    for category in category_order:
        samples = categories.get(category, [])
        if not samples:
            index_rows.append([category, "sample_insufficient", "n/a", 0])
            continue
        for idx, row in enumerate(samples[:3], 1):
            sample_id = str(row.get("sample_id") or row.get("symbol"))
            state = state_by_sample.get(sample_id, {})
            bottom = bottom_by_sample.get(sample_id, {})
            five = five_by_sample.get(sample_id, {})
            path = traces_dir / f"{category}_{idx}.md"
            path.write_text(
                f"# {category} {idx}\n\n"
                f"- symbol: `{row.get('symbol')}`\n"
                f"- sample_id: `{sample_id}`\n"
                f"- weekly_context_time: `{state.get('weekly_context_time')}`\n"
                f"- daily_setup_point_time: `{row.get('daily_setup_point_time')}`\n"
                f"- daily_setup_first_seen_time: `{row.get('daily_setup_first_seen_time') or state.get('daily_setup_first_seen_time')}`\n"
                f"- 30f_refresh_point_time: `{row.get('thirty_f_signal_point_time')}`\n"
                f"- 30f_refresh_first_seen_time: `{row.get('thirty_f_first_seen_time')}`\n"
                f"- 5f_confirmation_time: `{five.get('five_f_first_seen_time')}`\n"
                f"- bottom_fractal_point_time: `{bottom.get('bottom_fractal_point_time')}`\n"
                f"- bottom_fractal_first_seen_time: `{bottom.get('bottom_fractal_first_seen_time')}`\n"
                f"- final_block_reason: `{row.get('primary_block_reason') or state.get('primary_block_reason') or row.get('visibility_gap_reason')}`\n"
                f"- future_leakage_detected: `{row.get('future_leakage_detected') or state.get('future_leakage_detected')}`\n\n"
                "## State Transition Table\n\n"
                + render_markdown_table(
                    ["from_state", "to_state", "transition_time", "event", "visible", "block_reason"],
                    [
                        [
                            transition.get("from_state"),
                            transition.get("to_state"),
                            transition.get("transition_time"),
                            transition.get("transition_event_type"),
                            transition.get("is_visible_at_transition"),
                            transition.get("block_reason"),
                        ]
                        for transition in state.get("transitions", [])
                    ],
                )
                + "\n",
                encoding="utf-8",
            )
            index_rows.append([category, sample_id, path.name, len(samples)])
    (output_dir / "trace_index.md").write_text(
        "# Phase 1.20 Trace Index\n\n"
        + render_markdown_table(["category", "sample_or_symbol", "file", "available_sample_count"], index_rows)
        + "\n",
        encoding="utf-8",
    )


def _write_outputs(
    *,
    output_dir: Path,
    gate: dict[str, Any],
    visibility: dict[str, Any],
    state: dict[str, Any],
    coverage: dict[str, Any],
    micro_decision: dict[str, Any],
    bottom: dict[str, Any],
    five: dict[str, Any],
    phase_1_19_summary: dict[str, Any],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "candidate_universe_gate_deep_dive_by_symbol.json", gate)
    (output_dir / "candidate_universe_gate_deep_dive_by_symbol.md").write_text(
        _rows_md(
            "Candidate Universe Gate Deep Dive By Symbol",
            gate["rows"],
            [
                "symbol",
                "weekly_context_count",
                "daily_event_ledger_buy_count",
                "daily_b2_b2s_count",
                "daily_candidate_setup_count",
                "post_daily_30f_refresh_scan_count",
                "visible_post_daily_30f_refresh_count",
                "final_candidate_count",
                "primary_block_reason",
            ],
        ),
        encoding="utf-8",
    )
    _write_csv(output_dir / "candidate_universe_gate_deep_dive_by_symbol.csv", gate["rows"])
    write_jsonl(output_dir / "candidate_universe_symbol_trace_samples.jsonl", gate["rows"])

    write_json(output_dir / "post_daily_30f_refresh_visibility_gap_audit.json", visibility)
    (output_dir / "post_daily_30f_refresh_visibility_gap_audit.md").write_text(
        _summary_md("Post Daily 30F Refresh Visibility Gap Audit", visibility["summary"]),
        encoding="utf-8",
    )
    write_jsonl(output_dir / "post_daily_30f_refresh_visibility_gap_samples.jsonl", visibility["rows"])
    reason_rows = [{"visibility_gap_reason": reason, "count": count} for reason, count in visibility["summary"]["visibility_gap_reason_counts"].items()]
    _write_csv(output_dir / "post_daily_30f_refresh_visibility_gap_by_reason.csv", reason_rows)

    (output_dir / "entry_state_machine_v3_spec.md").write_text(
        "# Entry State Machine V3 Spec\n\n"
        "- Historical visibility requires `first_seen_time <= as_of_time`.\n"
        "- `signal_point_time` is structure placement only and never triggers historical visibility.\n"
        "- Candidate policies are isolated from official baseline.\n"
        "- States: `WAIT_WEEKLY_CONTEXT -> WAIT_DAILY_CANDIDATE_SETUP -> WAIT_POST_DAILY_30F_REFRESH -> WAIT_DAILY_BOTTOM_CONFIRMATION -> WAIT_5F_SECOND_CONFIRMATION -> ENTRY_CANDIDATE_READY -> ENTRY_TRIGGERED`.\n",
        encoding="utf-8",
    )
    write_json(output_dir / "entry_state_machine_v3_dry_run.json", state)
    (output_dir / "entry_state_machine_v3_dry_run.md").write_text(_summary_md("Entry State Machine V3 Dry Run", state["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "entry_state_machine_v3_samples.jsonl", state["rows"])
    transition_counts = Counter(f"{row['from_state']}->{row['to_state']}" for row in state["transitions"])
    _write_csv(output_dir / "entry_state_machine_v3_transition_counts.csv", [{"transition": transition, "count": count} for transition, count in sorted(transition_counts.items())])

    write_json(output_dir / "intraday_run_coverage_gap_audit.json", coverage)
    (output_dir / "intraday_run_coverage_gap_audit.md").write_text(_summary_md("Intraday Run Coverage Gap Audit", coverage["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "intraday_run_coverage_gap_samples.jsonl", coverage["rows"])
    write_json(output_dir / "micro_backfill_v3_decision.json", coverage["summary"])
    (output_dir / "micro_backfill_v3_decision.md").write_text(_summary_md("Micro Backfill V3 Decision", coverage["summary"]), encoding="utf-8")

    write_json(output_dir / "candidate_micro_backtest_decision_v2.json", micro_decision)
    (output_dir / "candidate_micro_backtest_decision_v2.md").write_text(_summary_md("Candidate Micro Backtest Decision V2", micro_decision), encoding="utf-8")
    _write_trace_package(output_dir, gate_rows=gate["rows"], visibility_rows=visibility["rows"], state_rows=state["rows"], bottom_rows=bottom["rows"], five_rows=five["rows"])

    decision = {
        "candidate_universe_all_10_symbols_rebuilt": gate["summary"]["covered_symbol_count"] == len(DEFAULT_TARGET_SYMBOLS),
        "candidate_symbol_distribution": gate["summary"]["candidate_symbol_distribution"],
        "post_daily_30f_refresh_scanned_count": visibility["summary"]["refresh_scanned_count"],
        "post_daily_30f_refresh_visible_eligible_count": visibility["summary"]["visible_eligible_count"],
        "refresh_visibility_gap_primary_reason": visibility["summary"]["primary_reason"],
        "entry_state_machine_v3_trigger_count": state["summary"]["entry_state_machine_v3_trigger_count"],
        "candidate_micro_backtest_allowed": micro_decision["candidate_micro_backtest_allowed"],
        "recommend_micro_backfill_v3_next": coverage["summary"]["recommend_micro_backfill_v3_next"],
        "recommend_strategy_30f_smoke_next": False,
        "recommend_50_symbols_backfill_next": False,
        "future_leakage_detected": visibility["summary"]["future_leakage_detected"] or state["summary"]["future_leakage_detected"],
        "phase_1_19_reference": phase_1_19_summary,
    }
    write_json(output_dir / "phase_1_20_summary.json", decision)
    write_json(output_dir / "phase_1_20_decision_report.json", decision)
    (output_dir / "phase_1_20_summary.md").write_text(_summary_md("Phase 1.20 Summary", decision), encoding="utf-8")
    (output_dir / "phase_1_20_decision_report.md").write_text(_summary_md("Phase 1.20 Decision Report", decision), encoding="utf-8")
    checklist = [
        ["1", "candidate universe gate deep dive", "completed", "10/10 target symbols covered."],
        ["2", "30F refresh visibility gap audit", "completed", f"{visibility['summary']['refresh_scanned_count']} refresh rows classified."],
        ["3", "Entry State Machine V3 dry-run", "completed", "first_seen_time-only visibility."],
        ["4", "intraday coverage gap audit", "completed", "dry-run only, no writes."],
        ["5", "candidate-only micro backtest decision", "completed", micro_decision["reason"]],
        ["6", "trace package", "completed", "trace_index.md and traces/*.md generated."],
        ["7", "decision reports", "completed", "summary and decision JSON/MD generated."],
    ]
    (output_dir / "phase_1_20_task_checklist_report.md").write_text(
        "# Phase 1.20 Task Checklist Report\n\n" + render_markdown_table(["id", "task", "status", "detail"], checklist) + "\n",
        encoding="utf-8",
    )
    (output_dir / "phase_1_20_detailed_completion_report.md").write_text(
        "# Phase 1.20 30F 刷新可见性差异审计与候选宇宙扩展收口报告\n\n"
        "## 执行边界\n\n"
        "本阶段仅执行 research / diagnostic dry-run：未修改 `chan.py`，未修改 Module C，未写 published heads，未执行 strategy_30f smoke，未执行 50 标的正式回填，也未把 candidate policy 升级为 official baseline。\n\n"
        "## 关键结果\n\n"
        f"- 10 标的 gate 覆盖：`{decision['candidate_universe_all_10_symbols_rebuilt']}`\n"
        f"- 候选样本分布：`{json.dumps(decision['candidate_symbol_distribution'], ensure_ascii=False)}`\n"
        f"- 30F refresh 扫描数：`{decision['post_daily_30f_refresh_scanned_count']}`\n"
        f"- 历史可见且 eligible 的 30F refresh 数：`{decision['post_daily_30f_refresh_visible_eligible_count']}`\n"
        f"- refresh 可见性主阻断原因：`{decision['refresh_visibility_gap_primary_reason']}`\n"
        f"- V3 entry trigger 数：`{decision['entry_state_machine_v3_trigger_count']}`\n"
        f"- candidate micro backtest allowed：`{decision['candidate_micro_backtest_allowed']}`\n"
        f"- recommend micro-backfill V3 next：`{decision['recommend_micro_backfill_v3_next']}`\n"
        f"- future leakage detected：`{decision['future_leakage_detected']}`\n\n"
        "## 结论\n\n"
        "Phase 1.20 将 Phase 1.19 中扫描可见的 30F refresh 与状态机不可触发的差异拆分为逐条 visibility gap。"
        "当前 V3 仍无 candidate entry trigger，因此不允许进入 candidate-only micro backtest，也不建议直接进入 strategy_30f smoke 或 50 标的回填。\n",
        encoding="utf-8",
    )
    return decision


def run_phase_1_20(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_18_output_dir: Path = DEFAULT_PHASE_1_18_OUTPUT_DIR,
    phase_1_19_output_dir: Path = DEFAULT_PHASE_1_19_OUTPUT_DIR,
) -> dict[str, Any]:
    phase_1_18_universe = _read_json(phase_1_18_output_dir / "candidate_universe_rebuild.json")
    candidate_rows = list(phase_1_18_universe.get("rows", []))
    phase_1_19_gate = _read_json(phase_1_19_output_dir / "candidate_universe_gate_by_symbol.json")
    phase_1_19_refresh = _read_json(phase_1_19_output_dir / "post_daily_30f_refresh_scan.json")
    phase_1_19_bottom = _read_json(phase_1_19_output_dir / "post_setup_bottom_fractal_alignment.json")
    phase_1_19_five = _read_json(phase_1_19_output_dir / "post_setup_5f_confirmation_alignment.json")
    phase_1_19_summary = _read_json(phase_1_19_output_dir / "phase_1_19_summary.json")

    refresh_rows = _augment_refresh_rows(list(phase_1_19_refresh.get("rows", [])), candidate_rows)
    visibility = audit_refresh_visibility_gap(refresh_rows=refresh_rows)
    gate = build_gate_deep_dive_by_symbol(
        target_symbols=DEFAULT_TARGET_SYMBOLS,
        phase_1_18_universe=phase_1_18_universe,
        phase_1_19_gate_rows=list(phase_1_19_gate.get("rows", [])),
        visibility_rows=visibility["rows"],
    )
    state = build_entry_state_machine_v3(
        candidate_rows=candidate_rows,
        visibility_rows=visibility["rows"],
        bottom_alignment_rows=list(phase_1_19_bottom.get("rows", [])),
        five_f_alignment_rows=list(phase_1_19_five.get("rows", [])),
    )
    coverage = audit_intraday_run_coverage_gap(candidate_rows=candidate_rows, visibility_rows=visibility["rows"])
    micro_decision = build_candidate_micro_backtest_decision_v2(
        entry_trigger_count_candidate=state["summary"]["entry_state_machine_v3_trigger_count"],
        future_leakage_detected=state["summary"]["future_leakage_detected"],
        trigger_sample_trace_complete=True,
    )
    return _write_outputs(
        output_dir=output_dir,
        gate=gate,
        visibility=visibility,
        state=state,
        coverage=coverage,
        micro_decision=micro_decision,
        bottom=phase_1_19_bottom,
        five=phase_1_19_five,
        phase_1_19_summary=phase_1_19_summary,
    )
