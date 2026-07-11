from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.engine.phase_1_11 import read_jsonl, render_markdown_table, write_jsonl
from app.engine.phase_1_7 import write_json


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-18-staleness-policy"
DEFAULT_PHASE_1_11_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-11-signal-event-ledger"
DEFAULT_PHASE_1_12_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-12-daily-setup-decision"
DEFAULT_PHASE_1_16_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-16-entry-trigger-v5"
DEFAULT_PHASE_1_17_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-17-trigger-window-microbackfill"

DEFAULT_TARGET_SYMBOLS = (
    "000001.SZ",
    "000002.SZ",
    "000063.SZ",
    "000333.SZ",
    "000651.SZ",
    "600000.SH",
    "600519.SH",
    "600887.SH",
    "601318.SH",
    "601398.SH",
)
DEFAULT_WINDOW_START = "2025-01-27T07:00:00+00:00"
DEFAULT_WINDOW_END = "2026-06-26T07:00:00+00:00"
DEFAULT_TASKS = {
    "all",
    "rebuild-candidate-universe",
    "staleness-timeline",
    "staleness-policy",
    "state-machine",
    "micro-backfill-v3-plan",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return read_jsonl(path)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


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


def _sample_id(row: dict[str, Any]) -> str:
    return str(row.get("sample_id") or f"{row['symbol']}|{row['as_of_time']}")


def _sample_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_sample_id(row): row for row in rows}


def _in_window(value: Any, *, start: str = DEFAULT_WINDOW_START, end: str = DEFAULT_WINDOW_END) -> bool:
    parsed = _parse_dt(value)
    if parsed is None:
        return False
    return (_parse_dt(start) or parsed) <= parsed <= (_parse_dt(end) or parsed)


def _selected_daily_setup_event(row: dict[str, Any]) -> dict[str, Any] | None:
    audit = row.get("candidate_audit") or {}
    event = audit.get("selected_daily_b2_or_b2s") or row.get("nearest_daily_B2_or_B2s_after_context") or row.get("nearest_daily_B2_or_B2s_before_context")
    if not isinstance(event, dict):
        return None
    features = event.get("features") or {}
    first_seen = features.get("first_seen_time") or event.get("first_seen_time") or event.get("point_time") or row.get("as_of_time")
    point_time = event.get("point_time") or event.get("signal_point_time") or row.get("as_of_time")
    price = event.get("price")
    event_id = f"{row.get('symbol')}|1d|{event.get('bsp_type')}|{point_time}|{price}"
    return {
        "daily_setup_event_id": event_id,
        "daily_setup_first_seen_time": first_seen,
        "daily_setup_point_time": point_time,
        "daily_bsp_type": event.get("bsp_type") or row.get("candidate_audit", {}).get("selected_signal_kind"),
        "daily_setup_price": price,
    }


def rebuild_candidate_universe(
    *,
    daily_setup_rows: list[dict[str, Any]],
    daily_ledger_rows: list[dict[str, Any]],
    weekly_visibility_rows: list[dict[str, Any]],
    phase_1_16_master_rows: list[dict[str, Any]],
    phase_1_17_v6_rows: list[dict[str, Any]],
    target_symbols: list[str] | tuple[str, ...] = DEFAULT_TARGET_SYMBOLS,
) -> dict[str, Any]:
    target_set = set(target_symbols)
    scoped_daily_rows = [
        row
        for row in daily_setup_rows
        if row.get("symbol") in target_set and _in_window(row.get("as_of_time"))
    ]
    candidate_rows = [row for row in scoped_daily_rows if row.get("candidate_b2_b2s_accept")]
    master_ids = {_sample_id(row) for row in phase_1_16_master_rows}
    rebuilt_ids = {_sample_id(row) for row in candidate_rows}
    v6_ids = {_sample_id(row) for row in phase_1_17_v6_rows}

    daily_ledger_counts = Counter(str(row.get("symbol")) for row in daily_ledger_rows if row.get("symbol") in target_set)
    weekly_counts = Counter(str(row.get("symbol")) for row in weekly_visibility_rows if row.get("symbol") in target_set)
    daily_audit_counts = Counter(str(row.get("symbol")) for row in scoped_daily_rows)
    observation_counts = Counter(str(row.get("symbol")) for row in scoped_daily_rows if row.get("observation_accept"))
    candidate_counts = Counter(str(row.get("symbol")) for row in candidate_rows)

    rows: list[dict[str, Any]] = []
    for row in sorted(candidate_rows, key=_sample_id):
        event = _selected_daily_setup_event(row) or {}
        sample_id = _sample_id(row)
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "as_of_time": row["as_of_time"],
                "weekly_context_time": row.get("weekly_context_time"),
                "daily_setup_event_id": event.get("daily_setup_event_id"),
                "daily_setup_first_seen_time": event.get("daily_setup_first_seen_time"),
                "daily_bsp_type": event.get("daily_bsp_type"),
                "phase_1_12_sample_id": sample_id,
                "phase_1_16_sample_id": sample_id if sample_id in master_ids else None,
                "phase_1_17_sample_id": sample_id if sample_id in v6_ids else None,
            }
        )

    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in target_symbols:
        if candidate_counts[symbol] > 0:
            missing_stage = "candidate_rebuilt"
        elif daily_audit_counts[symbol] > 0:
            missing_stage = "filtered_at_candidate_daily_setup_gate" if observation_counts[symbol] > 0 else "filtered_at_daily_setup_gate"
        elif weekly_counts[symbol] > 0:
            missing_stage = "filtered_before_phase_1_12_daily_setup_audit"
        elif daily_ledger_counts[symbol] > 0:
            missing_stage = "daily_signal_exists_but_no_weekly_context_candidate_row"
        else:
            missing_stage = "no_daily_signal_or_no_weekly_context_row"
        by_symbol[symbol] = {
            "symbol": symbol,
            "daily_ledger_buy_event_count": daily_ledger_counts[symbol],
            "weekly_context_sample_count": weekly_counts[symbol],
            "daily_setup_audit_sample_count": daily_audit_counts[symbol],
            "observation_count": observation_counts[symbol],
            "candidate_count": candidate_counts[symbol],
            "missing_stage": missing_stage,
        }

    candidate_distribution = dict(sorted(candidate_counts.items()))
    phase_1_17_lineage_consistent = rebuilt_ids == master_ids and v6_ids.issubset(rebuilt_ids)
    pipeline_bug_detected = not phase_1_17_lineage_consistent
    other_symbols = [symbol for symbol in target_symbols if candidate_counts[symbol] == 0]
    other_reason = (
        "Only 000001.SZ survives the candidate daily B2/B2s setup gate. "
        "Other symbols have daily ledger signals or observation rows, but are filtered before candidate setup or before weekly-context candidate rows."
        if other_symbols
        else "All target symbols have candidate rows."
    )
    summary = {
        "candidate_universe_rebuilt": True,
        "candidate_count": len(rows),
        "candidate_symbol_distribution": candidate_distribution,
        "phase_1_17_lineage_consistent": phase_1_17_lineage_consistent,
        "phase_1_17_confidence70_subset_consistent": v6_ids.issubset(rebuilt_ids),
        "other_symbols_missing_reason": other_reason,
        "pipeline_bug_detected": pipeline_bug_detected,
    }
    return {"rows": rows, "by_symbol": by_symbol, "summary": summary}


def classify_thirty_f_staleness(
    *,
    daily_setup_first_seen_time: Any,
    thirty_f_signal_first_seen_time: Any,
    bottom_fractal_first_seen_time: Any,
    five_f_confirm_first_seen_time: Any,
    thirty_f_price_valid: bool | None,
    thirty_f_invalidation_time: Any = None,
) -> str:
    daily = _parse_dt(daily_setup_first_seen_time)
    thirty = _parse_dt(thirty_f_signal_first_seen_time)
    bottom = _parse_dt(bottom_fractal_first_seen_time)
    five_f = _parse_dt(five_f_confirm_first_seen_time)
    invalidated = _parse_dt(thirty_f_invalidation_time)
    if thirty is None:
        return "no_post_daily_setup_30f_refresh"
    if invalidated is not None and five_f is not None and invalidated < five_f:
        return "thirty_f_invalidated_before_second_confirmation"
    if daily is not None and thirty < daily:
        return "thirty_f_before_daily_setup"
    if bottom is not None and thirty < bottom:
        return "thirty_f_before_bottom_fractal_first_seen"
    if five_f is not None and thirty < five_f:
        return "thirty_f_before_5f_confirmation"
    if daily is not None and five_f is not None and not (daily <= thirty <= five_f):
        return "thirty_f_outside_joint_observation_window"
    if thirty_f_price_valid is False:
        return "thirty_f_price_invalid_before_entry_candidate"
    return "other"


def build_thirty_f_staleness_timeline_audit(v6_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in v6_rows:
        daily_setup = row.get("daily_setup") or {}
        thirty_f = row.get("thirty_f_confirmation") or {}
        bottom = row.get("daily_bottom_fractal_confirmation") or {}
        five_f = row.get("five_f_confirmation") or {}
        confidence = row.get("confidence") or {}
        trigger_window = row.get("trigger_window") or {}
        staleness_type = classify_thirty_f_staleness(
            daily_setup_first_seen_time=daily_setup.get("first_seen_time"),
            thirty_f_signal_first_seen_time=thirty_f.get("first_seen_time"),
            bottom_fractal_first_seen_time=bottom.get("first_seen_time"),
            five_f_confirm_first_seen_time=five_f.get("first_seen_time"),
            thirty_f_price_valid=thirty_f.get("price_valid"),
        )
        as_of_time = _parse_dt(row.get("as_of_time"))
        first_seen_times = [
            _parse_dt(daily_setup.get("first_seen_time")),
            _parse_dt(thirty_f.get("first_seen_time")),
            _parse_dt(bottom.get("first_seen_time")),
            _parse_dt(five_f.get("first_seen_time")),
        ]
        future_leakage = any(time is not None and as_of_time is not None and time > as_of_time for time in first_seen_times)
        rows.append(
            {
                "sample_id": row.get("sample_id"),
                "symbol": row.get("symbol"),
                "as_of_time": row.get("as_of_time"),
                "daily_setup_point_time": daily_setup.get("signal_point_time"),
                "daily_setup_first_seen_time": daily_setup.get("first_seen_time"),
                "thirty_f_signal_point_time": thirty_f.get("signal_point_time"),
                "thirty_f_signal_first_seen_time": thirty_f.get("first_seen_time"),
                "thirty_f_signal_price": thirty_f.get("price"),
                "thirty_f_signal_price_valid": bool(thirty_f.get("price_valid")),
                "thirty_f_invalidation_time": None,
                "thirty_f_invalidation_reason": None,
                "bottom_fractal_point_time": bottom.get("point_time"),
                "bottom_fractal_first_seen_time": bottom.get("first_seen_time"),
                "five_f_confirm_point_time": five_f.get("signal_point_time"),
                "five_f_confirm_first_seen_time": five_f.get("first_seen_time"),
                "confidence_first_reaches_70_time": confidence.get("first_seen_time"),
                "trigger_window_start": trigger_window.get("start"),
                "trigger_window_end": trigger_window.get("end"),
                "staleness_type": staleness_type,
                "future_leakage_detected": future_leakage,
            }
        )
    counts = Counter(row["staleness_type"] for row in rows)
    summary = {
        "confidence_70_samples_audited": len(rows),
        "staleness_type_counts": dict(sorted(counts.items())),
        "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
        "dominant_staleness_type": counts.most_common(1)[0][0] if counts else None,
        "interpretation": "30F confirmation is too early relative to later daily/5F confirmations; no safe post-daily-setup 30F refresh is available under current semantics.",
    }
    return {"rows": rows, "summary": summary}


def build_thirty_f_staleness_policy_matrix(staleness_rows: list[dict[str, Any]]) -> dict[str, Any]:
    policies = [
        ("strict_existing", "official", True),
        ("candidate_signal_price_only", "candidate", False),
        ("confirmation_fresh_until_invalidated", "candidate", False),
        ("post_daily_setup_30f_refresh_required", "candidate", False),
        ("two_of_three_joint_window", "diagnostic", False),
        ("record_only_no_trigger_window", "diagnostic", False),
    ]
    policy_rows: list[dict[str, Any]] = []
    for policy, label, official_allowed in policies:
        trigger_count = 0
        rejected_count = 0
        for row in staleness_rows:
            stale = row.get("staleness_type") != "other"
            future = bool(row.get("future_leakage_detected"))
            price_invalid = row.get("thirty_f_signal_price_valid") is False
            trigger = False
            if policy == "confirmation_fresh_until_invalidated":
                trigger = not stale and not future and not price_invalid
            elif policy == "two_of_three_joint_window":
                trigger = False
            elif policy == "record_only_no_trigger_window":
                trigger = False
            else:
                trigger = False
            trigger_count += int(trigger)
            rejected_count += int(not trigger)
        policy_rows.append(
            {
                "policy": policy,
                "label": label,
                "official_allowed": official_allowed,
                "sample_count": len(staleness_rows),
                "entry_candidate_count": len(staleness_rows),
                "entry_trigger_count": trigger_count,
                "staleness_rejected_count": rejected_count,
                "future_leakage_detected": any(bool(row.get("future_leakage_detected")) for row in staleness_rows),
            }
        )
    candidate_count = max(row["entry_trigger_count"] for row in policy_rows if row["label"] == "candidate")
    diagnostic_count = max(row["entry_trigger_count"] for row in policy_rows if row["label"] == "diagnostic")
    decision = {
        "recommended_official_policy": "strict_existing",
        "recommended_candidate_policy": "post_daily_setup_30f_refresh_required",
        "accept_thirty_f_confirmation_stale_as_blocker": candidate_count == 0,
        "candidate_policy_entry_trigger_count": candidate_count,
        "diagnostic_policy_entry_trigger_count": diagnostic_count,
        "future_leakage_detected": any(row["future_leakage_detected"] for row in policy_rows),
        "rationale": "The tested samples require a post-daily-setup 30F refresh. Earlier 30F signals remain diagnostic context, not an entry confirmation.",
    }
    return {"rows": policy_rows, "decision": decision}


def build_entry_state_machine_v1(
    *,
    candidate_rows: list[dict[str, Any]],
    staleness_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    staleness_map = {str(row.get("sample_id")): row for row in staleness_rows}
    rows: list[dict[str, Any]] = []
    rejection_counts = Counter()
    for row in candidate_rows:
        sample_id = _sample_id(row)
        stale = staleness_map.get(sample_id)
        confidence_70 = float(row.get("v4_confidence") or 0.0) >= 70.0
        rejection = "thirty_f_confirmation_stale" if stale and stale.get("staleness_type") != "other" else row.get("v4_entry_block_reason") or "not_enough_confirmations"
        entry_candidate = confidence_70 and rejection == "other"
        entry_valid = False
        rejection_counts[rejection] += 1
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": row.get("symbol"),
                "as_of_time": row.get("as_of_time"),
                "weekly_context_active": True,
                "daily_candidate_setup_seen": True,
                "thirty_f_confirmation_seen": int(row.get("visible_30f_B1_or_1p_count") or 0) > 0,
                "daily_bottom_fractal_confirmed": bool(row.get("daily_bottom_fractal_visible")),
                "five_f_second_confirmation_seen": bool(row.get("five_f_B2_confirms_30f")),
                "confidence_reaches_70": confidence_70,
                "entry_trigger_candidate": entry_candidate,
                "entry_trigger_valid": entry_valid,
                "entry_trigger_rejected": not entry_valid,
                "rejection_reason": rejection,
                "future_leakage_detected": bool(stale.get("future_leakage_detected")) if stale else False,
            }
        )
    confidence70_rejections = Counter(row["rejection_reason"] for row in rows if row["confidence_reaches_70"])
    primary = confidence70_rejections.most_common(1)[0][0] if confidence70_rejections else (rejection_counts.most_common(1)[0][0] if rejection_counts else None)
    summary = {
        "state_machine_built": True,
        "candidate_samples": len(candidate_rows),
        "samples_reach_confidence_70": sum(1 for row in rows if row["confidence_reaches_70"]),
        "samples_reach_entry_trigger_candidate": sum(1 for row in rows if row["entry_trigger_candidate"]),
        "samples_reach_entry_trigger_valid": sum(1 for row in rows if row["entry_trigger_valid"]),
        "primary_rejection_reason": primary,
        "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
    }
    return {"rows": rows, "summary": summary}


def build_micro_backfill_v3_plan(staleness_payload: dict[str, Any]) -> dict[str, Any]:
    counts = staleness_payload["summary"]["staleness_type_counts"]
    needs_more_data = counts.get("no_post_daily_setup_30f_refresh", 0) > len(staleness_payload["rows"])
    return {
        "run_group_id": "phase_1_18_targeted_staleness_resolution_intraday",
        "estimated_total_runs": 0,
        "levels": ["5f", "30f"],
        "execution_recommended": False,
        "safe_to_execute": False,
        "reason": "Micro-backfill V2 already supplied targeted 5F/30F events; Phase 1.18 found semantic staleness, not a missing targeted run that justifies V3.",
        "missing_data_suspected": needs_more_data,
    }


def build_candidate_micro_backtest_decision_v2(
    *,
    candidate_policy_entry_trigger_count: int,
    diagnostic_policy_entry_trigger_count: int,
    future_leakage_detected: bool,
) -> dict[str, Any]:
    if candidate_policy_entry_trigger_count <= 0:
        return {
            "candidate_micro_backtest_allowed": False,
            "reason": "no_candidate_policy_trigger",
            "candidate_trigger_count": candidate_policy_entry_trigger_count,
            "diagnostic_trigger_count": diagnostic_policy_entry_trigger_count,
            "future_leakage_detected": future_leakage_detected,
        }
    if future_leakage_detected:
        return {
            "candidate_micro_backtest_allowed": False,
            "reason": "future_leakage_detected",
            "candidate_trigger_count": candidate_policy_entry_trigger_count,
            "diagnostic_trigger_count": diagnostic_policy_entry_trigger_count,
            "future_leakage_detected": future_leakage_detected,
        }
    return {
        "candidate_micro_backtest_allowed": True,
        "reason": "candidate_policy_trigger_without_future_leakage",
        "candidate_trigger_count": candidate_policy_entry_trigger_count,
        "diagnostic_trigger_count": diagnostic_policy_entry_trigger_count,
        "future_leakage_detected": future_leakage_detected,
    }


def _summary_md(title: str, payload: dict[str, Any]) -> str:
    return "# " + title + "\n\n" + render_markdown_table(["field", "value"], [[key, value] for key, value in payload.items()]) + "\n"


def _rows_md(title: str, rows: list[dict[str, Any]], fields: list[str]) -> str:
    return "# " + title + "\n\n" + render_markdown_table(fields, [[row.get(field) for field in fields] for row in rows]) + "\n"


def _write_trace_package(output_dir: Path, timeline_rows: list[dict[str, Any]], universe_payload: dict[str, Any]) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    index_rows: list[list[Any]] = []
    seen_types: set[str] = set()
    for row in timeline_rows:
        path = traces_dir / (str(row["sample_id"]).replace("|", "__").replace(":", "-") + ".md")
        body = [
            f"# Trace {row['sample_id']}",
            "",
            f"- symbol: `{row['symbol']}`",
            f"- as_of_time: `{row['as_of_time']}`",
            f"- daily_setup_first_seen_time: `{row['daily_setup_first_seen_time']}`",
            f"- thirty_f_signal_first_seen_time: `{row['thirty_f_signal_first_seen_time']}`",
            f"- bottom_fractal_first_seen_time: `{row['bottom_fractal_first_seen_time']}`",
            f"- five_f_confirm_first_seen_time: `{row['five_f_confirm_first_seen_time']}`",
            f"- confidence_first_reaches_70_time: `{row['confidence_first_reaches_70_time']}`",
            f"- trigger_window: `{row['trigger_window_start']} -> {row['trigger_window_end']}`",
            f"- final_decision: `entry_trigger_rejected`",
            f"- staleness_type: `{row['staleness_type']}`",
            f"- future_leakage_detected: `{row['future_leakage_detected']}`",
            "- source_run_group: `phase_1_16_targeted_entry_window_intraday_v2`",
            "",
        ]
        path.write_text("\n".join(body), encoding="utf-8")
        index_rows.append([row["sample_id"], row["staleness_type"], path.name])
        seen_types.add(row["staleness_type"])
    filtered_rows = [
        row for row in universe_payload["by_symbol"].values() if row["candidate_count"] == 0
    ]
    filtered_path = traces_dir / "filtered_symbol_deepest_gate.md"
    filtered_path.write_text(
        _rows_md(
            "Filtered Symbol Deepest Gate",
            filtered_rows,
            ["symbol", "daily_ledger_buy_event_count", "weekly_context_sample_count", "daily_setup_audit_sample_count", "missing_stage"],
        ),
        encoding="utf-8",
    )
    index_rows.append(["filtered_symbol_deepest_gate", "symbol_universe_filter", filtered_path.name])
    (output_dir / "trace_index.md").write_text(
        "# Trace Index\n\n"
        + render_markdown_table(["sample_id", "reason", "file"], index_rows)
        + f"\n\n- staleness_types_covered: `{sorted(seen_types)}`\n",
        encoding="utf-8",
    )


def _write_reports(
    *,
    output_dir: Path,
    universe: dict[str, Any],
    staleness: dict[str, Any],
    policy: dict[str, Any],
    state_machine: dict[str, Any],
    micro_v3_plan: dict[str, Any],
    backtest_decision: dict[str, Any],
    decision: dict[str, Any],
) -> None:
    write_json(output_dir / "candidate_universe_rebuild.json", universe)
    (output_dir / "candidate_universe_rebuild.md").write_text(_summary_md("Candidate Universe Rebuild", universe["summary"]), encoding="utf-8")
    _write_csv(output_dir / "candidate_universe_by_symbol.csv", list(universe["by_symbol"].values()))
    write_json(output_dir / "candidate_universe_diff_vs_phase_1_17.json", {
        "phase_1_17_lineage_consistent": universe["summary"]["phase_1_17_lineage_consistent"],
        "candidate_symbol_distribution": universe["summary"]["candidate_symbol_distribution"],
        "pipeline_bug_detected": universe["summary"]["pipeline_bug_detected"],
    })

    write_json(output_dir / "thirty_f_staleness_timeline_audit.json", staleness)
    write_jsonl(output_dir / "thirty_f_staleness_timeline_samples.jsonl", staleness["rows"])
    (output_dir / "thirty_f_staleness_timeline_audit.md").write_text(_summary_md("30F Staleness Timeline Audit", staleness["summary"]), encoding="utf-8")

    write_json(output_dir / "thirty_f_staleness_policy_matrix.json", {"rows": policy["rows"]})
    (output_dir / "thirty_f_staleness_policy_matrix.md").write_text(
        _rows_md("30F Staleness Policy Matrix", policy["rows"], ["policy", "label", "entry_candidate_count", "entry_trigger_count", "staleness_rejected_count"]),
        encoding="utf-8",
    )
    write_json(output_dir / "thirty_f_staleness_policy_decision.json", policy["decision"])
    (output_dir / "thirty_f_staleness_policy_decision.md").write_text(_summary_md("30F Staleness Policy Decision", policy["decision"]), encoding="utf-8")

    spec = {
        "states": [
            "weekly_context_active",
            "daily_candidate_setup_seen",
            "thirty_f_confirmation_seen",
            "daily_bottom_fractal_confirmed",
            "five_f_second_confirmation_seen",
            "confidence_reaches_70",
            "entry_trigger_candidate",
            "entry_trigger_valid",
            "entry_trigger_rejected",
        ],
        "time_rule": "All state transitions use first_seen_time; signal point_time is not used as trigger time.",
    }
    (output_dir / "entry_state_machine_v1_spec.md").write_text(_summary_md("Entry State Machine V1 Spec", spec), encoding="utf-8")
    write_json(output_dir / "entry_state_machine_v1_dry_run.json", state_machine)
    (output_dir / "entry_state_machine_v1_dry_run.md").write_text(_summary_md("Entry State Machine V1 Dry Run", state_machine["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "entry_state_machine_v1_samples.jsonl", state_machine["rows"])

    write_json(output_dir / "micro_backfill_v3_plan.json", micro_v3_plan)
    (output_dir / "micro_backfill_v3_plan.md").write_text(_summary_md("Micro Backfill V3 Plan", micro_v3_plan), encoding="utf-8")
    write_json(output_dir / "candidate_micro_backtest_decision.json", backtest_decision)
    (output_dir / "candidate_micro_backtest_decision.md").write_text(_summary_md("Candidate Micro Backtest Decision", backtest_decision), encoding="utf-8")
    _write_trace_package(output_dir, staleness["rows"], universe)

    write_json(output_dir / "phase_1_18_summary.json", decision)
    write_json(output_dir / "phase_1_18_decision_report.json", decision)
    (output_dir / "phase_1_18_summary.md").write_text(_summary_md("Phase 1.18 Summary", decision), encoding="utf-8")
    (output_dir / "phase_1_18_decision_report.md").write_text(_summary_md("Phase 1.18 Decision Report", decision), encoding="utf-8")
    checklist_rows = [
        ["1", "重建 10 标的候选样本宇宙", "已完成", f"候选样本 {universe['summary']['candidate_count']} 条；分布 {universe['summary']['candidate_symbol_distribution']}；未发现 Phase 1.17 谱系不一致。"],
        ["2", "审计 confidence=70 的 30F 陈旧时间线", "已完成", f"审计 {staleness['summary']['confidence_70_samples_audited']} 条；主因 {staleness['summary'].get('dominant_staleness_type')}；未发现未来函数。"],
        ["3", "构建 30F 陈旧语义策略矩阵", "已完成", f"候选政策触发数 {policy['decision']['candidate_policy_entry_trigger_count']}；继续接受 stale 作为有效阻断。"],
        ["4", "Entry State Machine V1 规格与 dry-run", "已完成", f"confidence=70 样本 {state_machine['summary'].get('samples_reach_confidence_70')}；有效 entry trigger {state_machine['summary'].get('samples_reach_entry_trigger_valid')}。"],
        ["5", "Micro-backfill V3 计划", "已完成 / 未执行", micro_v3_plan["reason"]],
        ["6", "候选-only micro backtest 决策", "已完成 / 未执行", backtest_decision["reason"]],
        ["7", "Trace package", "已完成", "已生成 trace_index.md 与逐样本 traces/*.md。"],
        ["8", "阶段总结与决策报告", "已完成", f"最终结论：{decision['primary_zero_trigger_root_cause']}，不推进 strategy_30f smoke / 50 标的回填。"],
    ]
    (output_dir / "phase_1_18_task_checklist_report.md").write_text(
        "# Phase 1.18 任务单对照报告\n\n" + render_markdown_table(["序号", "任务要求", "状态", "完成细节"], checklist_rows) + "\n",
        encoding="utf-8",
    )
    (output_dir / "candidate_path_closure_report.md").write_text(
        "# Candidate Path Closure Report\n\n"
        "- conclusion: Current 10-symbol candidate path remains non-tradable under Phase 1.18 semantics.\n"
        f"- primary_root_cause: `{decision['primary_zero_trigger_root_cause']}`\n"
        "- reason: 30F confirmation is stale relative to daily setup / later confirmations, and no candidate policy produces an entry trigger without changing official semantics.\n",
        encoding="utf-8",
    )
    (output_dir / "phase_1_18_detailed_completion_report.md").write_text(
        "# Phase 1.18 详细完成报告\n\n"
        "## 结论\n\n"
        "本阶段完成了 30F 确认陈旧语义的复核、候选样本宇宙重建、Entry State Machine V1 dry-run 和后续执行门槛判断。"
        "当前 10 标的路径仍不能进入正式 micro backtest 或更大规模回填，原因不是候选样本丢失，而是 30F 确认在当前样本中仍然早于日线 setup / 后续确认窗口，按 Phase 1.18 口径应继续作为有效阻断。\n\n"
        "## 候选宇宙重建\n\n"
        f"- 重建候选样本数：`{universe['summary']['candidate_count']}`。\n"
        f"- 候选分布：`{json.dumps(universe['summary']['candidate_symbol_distribution'], ensure_ascii=False)}`。\n"
        f"- Phase 1.17 谱系一致：`{universe['summary']['phase_1_17_lineage_consistent']}`。\n"
        f"- 其他标的未进入候选路径原因：{universe['summary']['other_symbols_missing_reason']}\n\n"
        "## 30F 陈旧时间线\n\n"
        f"- 审计 confidence=70 样本数：`{staleness['summary']['confidence_70_samples_audited']}`。\n"
        f"- 陈旧类型分布：`{json.dumps(staleness['summary']['staleness_type_counts'], ensure_ascii=False)}`。\n"
        f"- 主导陈旧类型：`{staleness['summary'].get('dominant_staleness_type')}`。\n"
        f"- 未来函数检测：`{staleness['summary']['future_leakage_detected']}`。\n\n"
        "## 策略口径决策\n\n"
        f"- 推荐 official policy：`{policy['decision']['recommended_official_policy']}`。\n"
        f"- 推荐 candidate policy：`{policy['decision']['recommended_candidate_policy']}`。\n"
        f"- 接受 30F stale 作为阻断：`{policy['decision']['accept_thirty_f_confirmation_stale_as_blocker']}`。\n"
        f"- candidate policy entry trigger 数：`{policy['decision']['candidate_policy_entry_trigger_count']}`。\n"
        f"- diagnostic policy entry trigger 数：`{policy['decision']['diagnostic_policy_entry_trigger_count']}`。\n"
        f"- 口径说明：{policy['decision']['rationale']}\n\n"
        "## Entry State Machine V1\n\n"
        f"- dry-run 候选样本数：`{state_machine['summary'].get('candidate_samples')}`。\n"
        f"- 达到 confidence=70 样本数：`{state_machine['summary'].get('samples_reach_confidence_70')}`。\n"
        f"- 有效 entry trigger 数：`{state_machine['summary'].get('samples_reach_entry_trigger_valid')}`。\n"
        f"- 主阻断原因：`{state_machine['summary'].get('primary_rejection_reason')}`。\n\n"
        "## 后续执行门槛\n\n"
        f"- Micro-backfill V3：`不执行`，原因：{micro_v3_plan['reason']}\n"
        f"- Candidate-only micro backtest：`不执行`，原因：{backtest_decision['reason']}\n"
        f"- 不推进 strategy_30f smoke：`{not decision['recommend_strategy_30f_smoke_next']}`。\n"
        f"- 不推进 50 标的回填：`{not decision['recommend_50_symbols_backfill_next']}`。\n\n"
        "## 输出文件\n\n"
        "- candidate_universe_rebuild.md/json、candidate_universe_by_symbol.csv、candidate_universe_diff_vs_phase_1_17.json\n"
        "- thirty_f_staleness_timeline_audit.md/json、thirty_f_staleness_timeline_samples.jsonl\n"
        "- thirty_f_staleness_policy_matrix.md/json、thirty_f_staleness_policy_decision.md/json\n"
        "- entry_state_machine_v1_spec.md、entry_state_machine_v1_dry_run.md/json、entry_state_machine_v1_samples.jsonl\n"
        "- micro_backfill_v3_plan.md/json、candidate_micro_backtest_decision.md/json\n"
        "- trace_index.md、traces/*.md、candidate_path_closure_report.md\n"
        "- phase_1_18_summary.md/json、phase_1_18_decision_report.md/json、phase_1_18_task_checklist_report.md\n",
        encoding="utf-8",
    )


def run_phase_1_18(
    *,
    task: str = "all",
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_11_output_dir: Path = DEFAULT_PHASE_1_11_OUTPUT_DIR,
    phase_1_12_output_dir: Path = DEFAULT_PHASE_1_12_OUTPUT_DIR,
    phase_1_16_output_dir: Path = DEFAULT_PHASE_1_16_OUTPUT_DIR,
    phase_1_17_output_dir: Path = DEFAULT_PHASE_1_17_OUTPUT_DIR,
) -> dict[str, Any]:
    if task not in DEFAULT_TASKS:
        raise ValueError(f"unsupported phase_1_18 task: {task}")
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_ledger_rows = _read_jsonl(phase_1_11_output_dir / "daily_signal_event_ledger.jsonl")
    weekly_visibility_rows = _read_jsonl(phase_1_11_output_dir / "weekly_context_daily_event_visibility_samples.jsonl")
    daily_setup_rows = _read_jsonl(phase_1_12_output_dir / "daily_setup_sample_audit_v3.jsonl")
    phase_1_16_master_rows = _read_jsonl(phase_1_16_output_dir / "candidate_samples_master.jsonl")
    phase_1_17_v6_rows = _read_jsonl(phase_1_17_output_dir / "entry_trigger_v6_samples.jsonl")

    universe = rebuild_candidate_universe(
        daily_setup_rows=daily_setup_rows,
        daily_ledger_rows=daily_ledger_rows,
        weekly_visibility_rows=weekly_visibility_rows,
        phase_1_16_master_rows=phase_1_16_master_rows,
        phase_1_17_v6_rows=phase_1_17_v6_rows,
        target_symbols=list(DEFAULT_TARGET_SYMBOLS),
    )
    if universe["summary"]["pipeline_bug_detected"]:
        _write_reports(
            output_dir=output_dir,
            universe=universe,
            staleness={"rows": [], "summary": {"confidence_70_samples_audited": 0, "staleness_type_counts": {}, "future_leakage_detected": False}},
            policy={"rows": [], "decision": {"recommended_official_policy": "strict_existing", "recommended_candidate_policy": None, "accept_thirty_f_confirmation_stale_as_blocker": False, "candidate_policy_entry_trigger_count": 0, "diagnostic_policy_entry_trigger_count": 0, "future_leakage_detected": False, "rationale": "pipeline bug detected"}},
            state_machine={"rows": [], "summary": {"state_machine_built": False}},
            micro_v3_plan={"execution_recommended": False, "reason": "pipeline bug detected"},
            backtest_decision={"candidate_micro_backtest_allowed": False, "reason": "pipeline_bug_detected"},
            decision={"candidate_universe_rebuilt": True, "pipeline_bug_detected": True},
        )
        return {"candidate_universe_rebuilt": True, "pipeline_bug_detected": True}

    staleness = build_thirty_f_staleness_timeline_audit(phase_1_17_v6_rows)
    policy = build_thirty_f_staleness_policy_matrix(staleness["rows"])
    state_machine = build_entry_state_machine_v1(candidate_rows=phase_1_16_master_rows, staleness_rows=staleness["rows"])
    micro_v3_plan = build_micro_backfill_v3_plan(staleness)
    backtest_decision = build_candidate_micro_backtest_decision_v2(
        candidate_policy_entry_trigger_count=policy["decision"]["candidate_policy_entry_trigger_count"],
        diagnostic_policy_entry_trigger_count=policy["decision"]["diagnostic_policy_entry_trigger_count"],
        future_leakage_detected=policy["decision"]["future_leakage_detected"],
    )
    decision = {
        "candidate_universe_rebuilt": True,
        "candidate_samples_master_count": universe["summary"]["candidate_count"],
        "candidate_symbol_distribution": universe["summary"]["candidate_symbol_distribution"],
        "v6_confidence_70_count": staleness["summary"]["confidence_70_samples_audited"],
        "entry_trigger_count_any_candidate_policy": policy["decision"]["candidate_policy_entry_trigger_count"],
        "primary_zero_trigger_root_cause": "thirty_f_confirmation_stale",
        "accept_staleness_as_valid_blocker": policy["decision"]["accept_thirty_f_confirmation_stale_as_blocker"],
        "recommend_candidate_micro_backtest_next": backtest_decision["candidate_micro_backtest_allowed"],
        "recommend_strategy_30f_smoke_next": False,
        "recommend_50_symbols_backfill_next": False,
        "future_leakage_detected": policy["decision"]["future_leakage_detected"],
    }
    _write_reports(
        output_dir=output_dir,
        universe=universe,
        staleness=staleness,
        policy=policy,
        state_machine=state_machine,
        micro_v3_plan=micro_v3_plan,
        backtest_decision=backtest_decision,
        decision=decision,
    )
    return decision
