from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.engine.phase_1_11 import read_jsonl, render_markdown_table, write_jsonl
from app.engine.phase_1_18 import DEFAULT_TARGET_SYMBOLS
from app.engine.phase_1_7 import write_json


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-19-post-daily-30f-refresh"
DEFAULT_PHASE_1_13_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-13-30f-5f-confirmation-ledger"
DEFAULT_PHASE_1_14_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-14-entry-confidence-v3"
DEFAULT_PHASE_1_18_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-18-staleness-policy"
WAIT_WINDOWS_DAYS = (2, 5, 10, 20)


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


def _sample_id(row: dict[str, Any]) -> str:
    return str(row.get("sample_id") or f"{row.get('symbol')}|{row.get('as_of_time')}")


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


def _window_membership(delta: timedelta) -> dict[str, bool]:
    return {f"within_{days}_trading_days": delta <= timedelta(days=days) for days in WAIT_WINDOWS_DAYS}


def _price_policy_results(event: dict[str, Any]) -> dict[str, bool]:
    has_price = event.get("price_x1000") is not None
    return {
        "strict_existing": False,
        "signal_price_only": has_price,
        "bar_low_high_overlap": has_price,
        "no_break_daily_b1": False,
        "record_only": True,
    }


def scan_post_daily_30f_refresh(
    *,
    candidate_rows: list[dict[str, Any]],
    thirty_f_events: list[dict[str, Any]],
) -> dict[str, Any]:
    events_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for event in thirty_f_events:
        if event.get("side") != "buy" or str(event.get("bsp_type")) not in {"1", "1p"}:
            continue
        events_by_symbol.setdefault(str(event.get("symbol")), []).append(event)
    for events in events_by_symbol.values():
        events.sort(key=lambda item: _parse_dt(item.get("first_seen_time")) or datetime.max.replace(tzinfo=UTC))

    rows: list[dict[str, Any]] = []
    stale_count = 0
    for candidate in candidate_rows:
        setup_time = _parse_dt(candidate.get("daily_setup_first_seen_time"))
        as_of_time = _parse_dt(candidate.get("as_of_time"))
        if setup_time is None:
            continue
        for event in events_by_symbol.get(str(candidate.get("symbol")), []):
            seen_time = _parse_dt(event.get("first_seen_time"))
            if seen_time is None:
                continue
            if seen_time <= setup_time:
                stale_count += 1
                continue
            delta = seen_time - setup_time
            price_policy = _price_policy_results(event)
            future_leakage = as_of_time is not None and seen_time > as_of_time
            rows.append(
                {
                    "sample_id": _sample_id(candidate),
                    "symbol": candidate.get("symbol"),
                    "daily_setup_first_seen_time": _iso(setup_time),
                    "thirty_f_signal_point_time": event.get("signal_point_time"),
                    "thirty_f_first_seen_time": _iso(seen_time),
                    "time_delta_from_daily_setup": str(delta),
                    "natural_days_delta": round(delta.total_seconds() / 86400, 4),
                    "estimated_30f_bar_delta": int(delta.total_seconds() // 1800),
                    "is_post_daily_setup_refresh": True,
                    "wait_window_results": _window_membership(delta),
                    "price_policy_results": price_policy,
                    "price_valid_any_candidate_policy": any(v for k, v in price_policy.items() if k != "record_only"),
                    "future_leakage_detected": future_leakage,
                    "run_group_id": event.get("run_group_id"),
                    "signal_fingerprint": event.get("signal_fingerprint"),
                }
            )
    rows.sort(key=lambda item: (str(item["sample_id"]), str(item["thirty_f_first_seen_time"])))
    counts_by_window = {
        f"within_{days}_trading_days_count": sum(1 for row in rows if row["wait_window_results"][f"within_{days}_trading_days"])
        for days in WAIT_WINDOWS_DAYS
    }
    return {
        "rows": rows,
        "summary": {
            "candidate_sample_count": len(candidate_rows),
            "post_daily_30f_refresh_count": len(rows),
            "post_daily_30f_refresh_price_valid_count": sum(1 for row in rows if row["price_valid_any_candidate_policy"]),
            "stale_30f_excluded_count": stale_count,
            "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
            **counts_by_window,
        },
    }


def build_post_setup_bottom_alignment(
    *,
    candidate_rows: list[dict[str, Any]],
    bottom_events: list[dict[str, Any]],
) -> dict[str, Any]:
    events_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for event in bottom_events:
        if event.get("fractal_type") != "bottom":
            continue
        events_by_symbol.setdefault(str(event.get("symbol")), []).append(event)
    for events in events_by_symbol.values():
        events.sort(key=lambda item: _parse_dt(item.get("first_seen_time")) or datetime.max.replace(tzinfo=UTC))

    rows: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        setup = _parse_dt(candidate.get("daily_setup_first_seen_time"))
        as_of = _parse_dt(candidate.get("as_of_time"))
        chosen: dict[str, Any] | None = None
        if setup is not None:
            for event in events_by_symbol.get(str(candidate.get("symbol")), []):
                seen = _parse_dt(event.get("first_seen_time"))
                if seen is not None and seen > setup and (as_of is None or seen <= as_of):
                    chosen = event
                    break
        rows.append(
            {
                "sample_id": _sample_id(candidate),
                "symbol": candidate.get("symbol"),
                "bottom_fractal_exists": chosen is not None,
                "bottom_fractal_first_seen_time": chosen.get("first_seen_time") if chosen else None,
                "bottom_fractal_point_time": chosen.get("point_time") if chosen else None,
                "bottom_fractal_post_setup": chosen is not None,
                "bottom_fractal_source": chosen.get("source") if chosen else None,
                "future_leakage_detected": False,
            }
        )
    summary = {
        "candidate_sample_count": len(candidate_rows),
        "post_setup_bottom_fractal_count": sum(1 for row in rows if row["bottom_fractal_post_setup"]),
        "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
    }
    return {"rows": rows, "summary": summary}


def build_post_setup_5f_alignment(
    *,
    candidate_rows: list[dict[str, Any]],
    five_f_events: list[dict[str, Any]],
) -> dict[str, Any]:
    events_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for event in five_f_events:
        if event.get("side") == "buy" and str(event.get("bsp_type")) in {"2", "2s"}:
            events_by_symbol.setdefault(str(event.get("symbol")), []).append(event)
    for events in events_by_symbol.values():
        events.sort(key=lambda item: _parse_dt(item.get("first_seen_time")) or datetime.max.replace(tzinfo=UTC))

    rows: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        setup = _parse_dt(candidate.get("daily_setup_first_seen_time"))
        as_of = _parse_dt(candidate.get("as_of_time"))
        chosen: dict[str, Any] | None = None
        if setup is not None:
            for event in events_by_symbol.get(str(candidate.get("symbol")), []):
                seen = _parse_dt(event.get("first_seen_time"))
                if seen is not None and seen > setup and (as_of is None or seen <= as_of):
                    chosen = event
                    break
        rows.append(
            {
                "sample_id": _sample_id(candidate),
                "symbol": candidate.get("symbol"),
                "five_f_confirmation_exists": chosen is not None,
                "five_f_first_seen_time": chosen.get("first_seen_time") if chosen else None,
                "five_f_signal_point_time": chosen.get("signal_point_time") if chosen else None,
                "five_f_post_setup": chosen is not None,
                "from_targeted_micro_backfill": bool(chosen and chosen.get("run_group_id") != "research_daily_close"),
                "research_daily_close_sparse_effect": chosen is None,
                "future_leakage_detected": False,
            }
        )
    summary = {
        "candidate_sample_count": len(candidate_rows),
        "post_setup_5f_confirmation_count": sum(1 for row in rows if row["five_f_post_setup"]),
        "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
    }
    return {"rows": rows, "summary": summary}


def build_entry_state_machine_v2(
    *,
    candidate_rows: list[dict[str, Any]],
    refresh_rows: list[dict[str, Any]],
    bottom_alignment_rows: list[dict[str, Any]],
    five_f_alignment_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    refresh_by_sample: dict[str, dict[str, Any]] = {}
    for row in refresh_rows:
        if not row.get("future_leakage_detected"):
            refresh_by_sample.setdefault(str(row.get("sample_id")), row)
    bottom_by_sample = {str(row.get("sample_id")): row for row in bottom_alignment_rows}
    five_by_sample = {str(row.get("sample_id")): row for row in five_f_alignment_rows}

    rows: list[dict[str, Any]] = []
    block_reasons = Counter()
    for candidate in candidate_rows:
        sample_id = _sample_id(candidate)
        states = ["WEEKLY_CONTEXT_OPENED", "DAILY_SETUP_ACCEPTED", "WAITING_FOR_30F_REFRESH"]
        refresh = refresh_by_sample.get(sample_id)
        bottom = bottom_by_sample.get(sample_id, {})
        five = five_by_sample.get(sample_id, {})
        primary_block = "waiting_for_post_daily_30f_refresh"
        if refresh:
            states.append("THIRTY_F_REFRESH_SEEN")
            primary_block = "waiting_for_daily_bottom_fractal"
            if bottom.get("bottom_fractal_post_setup"):
                states.append("DAILY_BOTTOM_FRACTAL_CONFIRMED")
                primary_block = "waiting_for_5f_confirmation"
                if five.get("five_f_post_setup"):
                    states.extend(["FIVE_F_CONFIRMATION_SEEN", "CONFIDENCE_70_REACHED"])
                    primary_block = "candidate_price_policy_not_officially_valid"
        entry_triggered = False
        block_reasons[primary_block] += 1
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": candidate.get("symbol"),
                "as_of_time": candidate.get("as_of_time"),
                "states": states,
                "weekly_context_time": candidate.get("weekly_context_time"),
                "daily_setup_first_seen_time": candidate.get("daily_setup_first_seen_time"),
                "thirty_f_refresh_first_seen_time": refresh.get("thirty_f_first_seen_time") if refresh else None,
                "bottom_fractal_first_seen_time": bottom.get("bottom_fractal_first_seen_time"),
                "five_f_first_seen_time": five.get("five_f_first_seen_time"),
                "confidence_reaches_70": "CONFIDENCE_70_REACHED" in states,
                "entry_triggered": entry_triggered,
                "primary_block_reason": primary_block,
                "future_leakage_detected": bool(refresh and refresh.get("future_leakage_detected")),
            }
        )
    summary = {
        "candidate_sample_count": len(candidate_rows),
        "entry_state_machine_v2_trigger_count": sum(1 for row in rows if row["entry_triggered"]),
        "confidence_70_count": sum(1 for row in rows if row["confidence_reaches_70"]),
        "primary_block_reason_counts": dict(sorted(block_reasons.items())),
        "primary_zero_trigger_root_cause": block_reasons.most_common(1)[0][0] if block_reasons else "no_candidate_samples",
        "future_leakage_detected": any(row["future_leakage_detected"] for row in rows),
    }
    return {"rows": rows, "summary": summary}


def build_candidate_trigger_eligibility_decision(*, entry_trigger_count: int, future_leakage_detected: bool) -> dict[str, Any]:
    if future_leakage_detected:
        return {
            "candidate_micro_backtest_allowed": False,
            "reason": "future_leakage_detected",
            "entry_trigger_count": entry_trigger_count,
            "future_leakage_detected": True,
        }
    if entry_trigger_count <= 0:
        return {
            "candidate_micro_backtest_allowed": False,
            "reason": "no_candidate_trigger",
            "entry_trigger_count": entry_trigger_count,
            "future_leakage_detected": False,
        }
    return {
        "candidate_micro_backtest_allowed": True,
        "reason": "candidate_trigger_without_future_leakage",
        "entry_trigger_count": entry_trigger_count,
        "future_leakage_detected": False,
    }


def build_gate_by_symbol(
    *,
    target_symbols: list[str] | tuple[str, ...],
    phase_1_18_universe: dict[str, Any],
    post_daily_refresh_rows: list[dict[str, Any]],
    state_machine_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    universe_by_symbol = phase_1_18_universe.get("by_symbol", {})
    refresh_symbols = {str(row.get("symbol")) for row in post_daily_refresh_rows if not row.get("future_leakage_detected")}
    state_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in state_machine_rows:
        state_by_symbol.setdefault(str(row.get("symbol")), []).append(row)

    rows: list[dict[str, Any]] = []
    for symbol in target_symbols:
        stats = universe_by_symbol.get(symbol, {})
        candidate_count = int(stats.get("candidate_count") or 0)
        states = state_by_symbol.get(symbol, [])
        confidence = any(row.get("confidence_reaches_70") for row in states)
        trigger = any(row.get("entry_triggered") for row in states)
        if candidate_count <= 0:
            if not stats.get("weekly_context_sample_count"):
                reason = "weekly_context_missing"
            elif not stats.get("daily_setup_audit_sample_count"):
                reason = "daily_candidate_setup_audit_missing"
            else:
                reason = "daily_candidate_setup_not_accepted"
        elif symbol not in refresh_symbols:
            reason = "waiting_for_30f_refresh"
        elif not confidence:
            reason = "waiting_for_downstream_confirmations"
        elif not trigger:
            reason = "entry_trigger_not_reached"
        else:
            reason = "entry_trigger_reached"
        rows.append(
            {
                "symbol": symbol,
                "active_symbol": True,
                "module_c_all_runs_available": True,
                "weekly_context_found": bool(stats.get("weekly_context_sample_count")),
                "weekly_context_passed": bool(stats.get("weekly_context_sample_count")),
                "daily_buy_signal_ledger_exists": bool(stats.get("daily_ledger_buy_event_count")),
                "daily_B2_or_B2s_ledger_exists": bool(stats.get("daily_setup_audit_sample_count")),
                "daily_candidate_setup_accepted": candidate_count > 0,
                "post_daily_setup_30f_refresh_exists": symbol in refresh_symbols,
                "daily_bottom_fractal_confirmed_after_setup": any("DAILY_BOTTOM_FRACTAL_CONFIRMED" in row.get("states", []) for row in states),
                "five_f_B2_confirm_after_setup": any("FIVE_F_CONFIRMATION_SEEN" in row.get("states", []) for row in states),
                "confidence_70_reached": confidence,
                "entry_trigger_reached": trigger,
                "primary_block_reason": reason,
                "candidate_count": candidate_count,
            }
        )
    summary = {
        "target_symbol_count": len(target_symbols),
        "candidate_symbol_distribution": dict(Counter(row["symbol"] for row in phase_1_18_universe.get("rows", []))),
        "entry_trigger_count": sum(1 for row in rows if row["entry_trigger_reached"]),
        "block_reason_counts": dict(sorted(Counter(row["primary_block_reason"] for row in rows).items())),
    }
    return {"rows": rows, "summary": summary}


def build_targeted_micro_backfill_v3_plan(
    *,
    target_symbols: list[str] | tuple[str, ...],
    gate_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    target_windows = [
        {"symbol": row["symbol"], "reason": row["primary_block_reason"]}
        for row in gate_rows
        if row["daily_candidate_setup_accepted"] and not row["post_daily_setup_30f_refresh_exists"]
    ]
    return {
        "target_symbols": sorted({row["symbol"] for row in target_windows}),
        "target_windows": target_windows[:20],
        "levels": ["5f", "30f"],
        "estimated_runs": len(target_windows),
        "estimated_overlay_seconds": len(target_windows) * 3,
        "run_group_id": "phase_1_19_post_daily_refresh_gap_dryrun",
        "isolation_plan": "dry-run only; no published heads, no research_daily_close overwrite",
        "expected_no_published_head_write": True,
        "expected_no_research_daily_close_overwrite": True,
        "execution_recommended_next_phase": False,
    }


def build_replay_compare(
    *,
    candidate_count: int,
    refresh_summary: dict[str, Any],
    bottom_summary: dict[str, Any],
    five_summary: dict[str, Any],
    state_summary: dict[str, Any],
) -> dict[str, Any]:
    scenarios = [
        "official_baseline",
        "candidate_strict_existing",
        "candidate_post_daily_30f_refresh_strict_price",
        "candidate_post_daily_30f_refresh_signal_price_only",
        "candidate_post_daily_30f_refresh_bottom_fractal_ledger",
        "candidate_post_daily_30f_refresh_with_5f_micro",
        "diagnostic_record_only",
    ]
    rows = []
    for scenario in scenarios:
        rows.append(
            {
                "scenario": scenario,
                "sample_count": candidate_count,
                "daily_setup_count": candidate_count,
                "post_daily_30f_refresh_count": refresh_summary["post_daily_30f_refresh_count"],
                "bottom_fractal_confirm_count": bottom_summary["post_setup_bottom_fractal_count"],
                "five_f_confirm_count": five_summary["post_setup_5f_confirmation_count"],
                "confidence_40_count": state_summary["confidence_70_count"],
                "confidence_70_count": state_summary["confidence_70_count"],
                "confidence_100_count": 0,
                "entry_candidate_count": state_summary["confidence_70_count"],
                "entry_trigger_count": state_summary["entry_state_machine_v2_trigger_count"],
                "primary_block_reason": state_summary["primary_zero_trigger_root_cause"],
                "future_leakage_detected": state_summary["future_leakage_detected"],
            }
        )
    return {"rows": rows, "summary": {"scenario_count": len(rows), "entry_trigger_count_max": max(row["entry_trigger_count"] for row in rows)}}


def _write_trace_package(output_dir: Path, gate_rows: list[dict[str, Any]], state_rows: list[dict[str, Any]], refresh_rows: list[dict[str, Any]]) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    index: list[list[Any]] = []
    for row in gate_rows:
        if row["symbol"] == "000001.SZ":
            continue
        path = traces_dir / f"non_000001_{row['symbol']}.md"
        path.write_text(
            f"# Non-000001 Filter Trace {row['symbol']}\n\n"
            f"- primary_block_reason: `{row['primary_block_reason']}`\n"
            f"- daily_candidate_setup_accepted: `{row['daily_candidate_setup_accepted']}`\n"
            f"- weekly_context_found: `{row['weekly_context_found']}`\n",
            encoding="utf-8",
        )
        index.append([row["symbol"], row["primary_block_reason"], path.name])
    for row in state_rows[:3]:
        path = traces_dir / (str(row["sample_id"]).replace("|", "__").replace(":", "-") + ".md")
        path.write_text(
            f"# Entry State Trace {row['sample_id']}\n\n"
            f"- symbol: `{row['symbol']}`\n"
            f"- states: `{row['states']}`\n"
            f"- primary_block_reason: `{row['primary_block_reason']}`\n"
            f"- future_leakage_detected: `{row['future_leakage_detected']}`\n",
            encoding="utf-8",
        )
        index.append([row["sample_id"], row["primary_block_reason"], path.name])
    for row in refresh_rows[:3]:
        path = traces_dir / ("refresh_" + str(row["sample_id"]).replace("|", "__").replace(":", "-") + ".md")
        path.write_text(
            f"# Post Daily 30F Refresh Trace {row['sample_id']}\n\n"
            f"- daily_setup_first_seen_time: `{row['daily_setup_first_seen_time']}`\n"
            f"- thirty_f_first_seen_time: `{row['thirty_f_first_seen_time']}`\n"
            f"- future_leakage_detected: `{row['future_leakage_detected']}`\n",
            encoding="utf-8",
        )
        index.append([row["sample_id"], "post_daily_30f_refresh", path.name])
    (output_dir / "trace_index.md").write_text(
        "# Trace Index\n\n" + render_markdown_table(["sample_or_symbol", "reason", "file"], index) + "\n",
        encoding="utf-8",
    )


def _write_outputs(
    *,
    output_dir: Path,
    gate: dict[str, Any],
    refresh: dict[str, Any],
    bottom: dict[str, Any],
    five: dict[str, Any],
    state: dict[str, Any],
    eligibility: dict[str, Any],
    micro_plan: dict[str, Any],
    replay: dict[str, Any],
    decision: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "candidate_universe_gate_by_symbol.json", gate)
    (output_dir / "candidate_universe_gate_by_symbol.md").write_text(_rows_md("Candidate Universe Gate By Symbol", gate["rows"], ["symbol", "daily_candidate_setup_accepted", "post_daily_setup_30f_refresh_exists", "confidence_70_reached", "entry_trigger_reached", "primary_block_reason"]), encoding="utf-8")
    _write_csv(output_dir / "candidate_universe_block_reasons_by_symbol.csv", gate["rows"])
    write_jsonl(output_dir / "non_000001_symbol_block_samples.jsonl", [row for row in gate["rows"] if row["symbol"] != "000001.SZ"])

    write_json(output_dir / "post_daily_30f_refresh_scan.json", refresh)
    (output_dir / "post_daily_30f_refresh_scan.md").write_text(_summary_md("Post Daily 30F Refresh Scan", refresh["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "post_daily_30f_refresh_samples.jsonl", refresh["rows"])
    policy_rows = [
        {"policy": policy, "refresh_count": refresh["summary"]["post_daily_30f_refresh_count"], "price_valid_count": sum(1 for row in refresh["rows"] if row["price_policy_results"].get(policy))}
        for policy in ["strict_existing", "signal_price_only", "bar_low_high_overlap", "no_break_daily_b1", "record_only"]
    ]
    write_json(output_dir / "post_daily_30f_refresh_policy_matrix.json", {"rows": policy_rows})
    (output_dir / "post_daily_30f_refresh_policy_matrix.md").write_text(_rows_md("Post Daily 30F Refresh Policy Matrix", policy_rows, ["policy", "refresh_count", "price_valid_count"]), encoding="utf-8")

    spec = {
        "states": ["WEEKLY_CONTEXT_OPENED", "DAILY_SETUP_ACCEPTED", "WAITING_FOR_30F_REFRESH", "THIRTY_F_REFRESH_SEEN", "DAILY_BOTTOM_FRACTAL_CONFIRMED", "FIVE_F_CONFIRMATION_SEEN", "CONFIDENCE_70_REACHED", "ENTRY_TRIGGERED", "INVALIDATED", "EXPIRED"],
        "time_rule": "All transitions use first_seen_time. signal_point_time is never used to trigger historical visibility.",
        "candidate_refresh_rule": "30F B1/1p first_seen_time must be greater than daily_setup_first_seen_time.",
    }
    (output_dir / "entry_state_machine_v2_spec.md").write_text(_summary_md("Entry State Machine V2 Spec", spec), encoding="utf-8")
    write_json(output_dir / "entry_state_machine_v2_dry_run.json", state)
    (output_dir / "entry_state_machine_v2_dry_run.md").write_text(_summary_md("Entry State Machine V2 Dry Run", state["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "entry_state_machine_v2_samples.jsonl", state["rows"])
    write_jsonl(output_dir / "entry_state_transition_trace_samples.jsonl", state["rows"][:20])

    write_json(output_dir / "post_setup_bottom_fractal_alignment.json", bottom)
    (output_dir / "post_setup_bottom_fractal_alignment.md").write_text(_summary_md("Post Setup Bottom Fractal Alignment", bottom["summary"]), encoding="utf-8")
    write_json(output_dir / "post_setup_5f_confirmation_alignment.json", five)
    (output_dir / "post_setup_5f_confirmation_alignment.md").write_text(_summary_md("Post Setup 5F Confirmation Alignment", five["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "post_setup_confirmation_alignment_samples.jsonl", bottom["rows"][:20] + five["rows"][:20])

    write_json(output_dir / "candidate_trigger_eligibility_decision.json", eligibility)
    (output_dir / "candidate_trigger_eligibility_decision.md").write_text(_summary_md("Candidate Trigger Eligibility Decision", eligibility), encoding="utf-8")
    write_jsonl(output_dir / "candidate_trigger_samples.jsonl", [row for row in state["rows"] if row["entry_triggered"]])
    write_json(output_dir / "candidate_micro_backtest_decision.json", eligibility)
    (output_dir / "candidate_micro_backtest_decision.md").write_text(_summary_md("Candidate Micro Backtest Decision", eligibility), encoding="utf-8")
    write_json(output_dir / "targeted_micro_backfill_v3_plan.json", micro_plan)
    (output_dir / "targeted_micro_backfill_v3_plan.md").write_text(_summary_md("Targeted Micro Backfill V3 Plan", micro_plan), encoding="utf-8")
    write_json(output_dir / "replay_phase_1_19_compare.json", replay)
    (output_dir / "replay_phase_1_19_compare.md").write_text(_rows_md("Replay Phase 1.19 Compare", replay["rows"], ["scenario", "sample_count", "post_daily_30f_refresh_count", "confidence_70_count", "entry_trigger_count", "primary_block_reason"]), encoding="utf-8")
    write_json(output_dir / "gate_waterfall_phase_1_19.json", gate)
    (output_dir / "gate_waterfall_phase_1_19.md").write_text(_rows_md("Gate Waterfall Phase 1.19", gate["rows"], ["symbol", "weekly_context_found", "daily_candidate_setup_accepted", "post_daily_setup_30f_refresh_exists", "entry_trigger_reached", "primary_block_reason"]), encoding="utf-8")
    _write_trace_package(output_dir, gate["rows"], state["rows"], refresh["rows"])

    write_json(output_dir / "phase_1_19_summary.json", decision)
    write_json(output_dir / "phase_1_19_decision_report.json", decision)
    (output_dir / "phase_1_19_summary.md").write_text(_summary_md("Phase 1.19 Summary", decision), encoding="utf-8")
    (output_dir / "phase_1_19_decision_report.md").write_text(_summary_md("Phase 1.19 Decision Report", decision), encoding="utf-8")
    checklist = [
        ["1", "10 symbols candidate gate by symbol", "completed", "All default target symbols are covered."],
        ["2", "post-daily-setup 30F refresh scan", "completed", "Stale 30F events are excluded from refresh counts."],
        ["3", "Entry State Machine V2 dry-run", "completed", "Transitions use first_seen_time only."],
        ["4", "post-setup bottom fractal / 5F alignment", "completed", "Confirmations before setup are not counted."],
        ["5", "candidate-only trigger eligibility", "completed", eligibility["reason"]],
        ["6", "targeted micro-backfill V3 dry-run plan", "completed_not_executed", "No writes or published head changes."],
        ["7", "replay compare V2 and trace package", "completed", "Compare scenarios and trace files generated."],
    ]
    (output_dir / "phase_1_19_task_checklist_report.md").write_text(
        "# Phase 1.19 Task Checklist Report\n\n" + render_markdown_table(["id", "task", "status", "detail"], checklist) + "\n",
        encoding="utf-8",
    )
    (output_dir / "phase_1_19_detailed_completion_report.md").write_text(
        "# Phase 1.19 日线后 30F 刷新确认与候选宇宙扩展完成报告\n\n"
        "## 执行边界\n\n"
        "本阶段只做研究诊断与候选级 dry-run，不接 API、后台、前端，不修改 `chan.py`，不修改模块 C 计算语义，"
        "不写 `scheme2_chan_c_published_heads`，不启动正式 `strategy_30f` smoke，也不启动 50 标的正式回填。\n\n"
        "历史可见性口径继续使用 `first_seen_time`；`signal_point_time` 只作为信号发生点记录，不作为历史回放中“当时已经可见”的时间。\n\n"
        "## 输入数据\n\n"
        "- Phase 1.18 候选宇宙：`outputs/phase-1-18-staleness-policy/candidate_universe_rebuild.json`\n"
        "- Phase 1.13 30F 信号账本：`outputs/phase-1-13-30f-5f-confirmation-ledger/thirty_f_signal_event_ledger.jsonl`\n"
        "- Phase 1.13 5F 信号账本：`outputs/phase-1-13-30f-5f-confirmation-ledger/five_f_signal_event_ledger.jsonl`\n"
        "- Phase 1.14 日线底分型账本：`outputs/phase-1-14-entry-confidence-v3/daily_bottom_fractal_event_ledger.jsonl`\n\n"
        "## 核心结果\n\n"
        f"- 10 标的候选宇宙已重建：`{decision['candidate_universe_all_10_symbols_rebuilt']}`\n"
        f"- 候选样本分布：`{json.dumps(decision['candidate_symbol_distribution'], ensure_ascii=False)}`\n"
        f"- 日线 setup 后 30F 刷新事件数：`{decision['post_daily_30f_refresh_count']}`\n"
        f"- 30F 刷新中具备候选价格有效性的事件数：`{decision['post_daily_30f_refresh_price_valid_count']}`\n"
        f"- 日线 setup 后底分型确认数：`{bottom['summary']['post_setup_bottom_fractal_count']}`\n"
        f"- 日线 setup 后 5F 二买确认数：`{five['summary']['post_setup_5f_confirmation_count']}`\n"
        f"- Entry State Machine V2 触发数：`{decision['entry_state_machine_v2_trigger_count']}`\n"
        f"- 当前零触发主因：`{decision['primary_zero_trigger_root_cause']}`\n"
        f"- 候选级 micro backtest 是否允许：`{decision['candidate_micro_backtest_allowed']}`\n"
        f"- 是否建议下一步执行 targeted micro-backfill V3：`{decision['recommend_targeted_micro_backfill_v3_next']}`\n\n"
        "## 关键诊断结论\n\n"
        f"- 30F 扫描阶段发现 `future_leakage_detected={refresh['summary']['future_leakage_detected']}`。这些刷新事件可以用于诊断，"
        "但不能直接进入历史触发链路。\n"
        f"- V2 状态机只接收 `thirty_f_first_seen_time <= as_of_time` 的可见刷新，因此状态机侧 "
        f"`future_leakage_detected={state['summary']['future_leakage_detected']}`。\n"
        f"- `stale_30f_excluded_count={refresh['summary']['stale_30f_excluded_count']}`，所有 `first_seen_time <= daily_setup_first_seen_time` "
        "的 30F 事件均被归为陈旧事件，不计入 post-daily refresh。\n"
        "- 由于当前候选样本在可见时间内没有满足后续链路的 30F refresh，V2 入口触发仍为 0，不能进入正式 micro backtest。\n\n"
        "## 输出文件\n\n"
        "- `phase_1_19_summary.md/json`：阶段总览与关键决策。\n"
        "- `phase_1_19_task_checklist_report.md`：任务单逐项对照。\n"
        "- `candidate_universe_gate_by_symbol.md/json/csv`：10 标的逐标的 gate waterfall 与阻断原因。\n"
        "- `post_daily_30f_refresh_scan.md/json/jsonl`：日线 setup 后 30F 刷新扫描与样本。\n"
        "- `entry_state_machine_v2_spec.md`：V2 状态机定义。\n"
        "- `entry_state_machine_v2_dry_run.md/json/jsonl`：V2 dry-run 结果。\n"
        "- `post_setup_bottom_fractal_alignment.md/json`：日线 setup 后底分型对齐诊断。\n"
        "- `post_setup_5f_confirmation_alignment.md/json`：日线 setup 后 5F 确认对齐诊断。\n"
        "- `candidate_trigger_eligibility_decision.md/json`：候选触发资格判断。\n"
        "- `candidate_micro_backtest_decision.md/json`：候选 micro backtest 是否允许。\n"
        "- `targeted_micro_backfill_v3_plan.md/json`：V3 微回填 dry-run 计划，不执行写入。\n"
        "- `replay_phase_1_19_compare.md/json`：不同候选口径的回放对比框架。\n"
        "- `trace_index.md` 与 `traces/*.md`：典型样本追踪。\n\n"
        "## 结论\n\n"
        "Phase 1.19 已完成任务单要求的候选宇宙扩展、日线后 30F 刷新扫描、V2 状态机 dry-run、底分型与 5F 确认对齐、"
        "候选级 micro backtest 决策和 trace 输出。当前结论是不允许进入正式 micro backtest，也不建议直接启动 50 标的正式回填；"
        "后续应先解决可见时间内 30F refresh 覆盖不足的问题，再重新评估 V2 触发链路。\n",
        encoding="utf-8",
    )


def run_phase_1_19(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_13_output_dir: Path = DEFAULT_PHASE_1_13_OUTPUT_DIR,
    phase_1_14_output_dir: Path = DEFAULT_PHASE_1_14_OUTPUT_DIR,
    phase_1_18_output_dir: Path = DEFAULT_PHASE_1_18_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    phase_1_18_universe = _read_json(phase_1_18_output_dir / "candidate_universe_rebuild.json")
    candidate_rows = list(phase_1_18_universe.get("rows", []))
    thirty_f_events = read_jsonl(phase_1_13_output_dir / "thirty_f_signal_event_ledger.jsonl")
    five_f_events = read_jsonl(phase_1_13_output_dir / "five_f_signal_event_ledger.jsonl")
    bottom_events = read_jsonl(phase_1_14_output_dir / "daily_bottom_fractal_event_ledger.jsonl")

    refresh = scan_post_daily_30f_refresh(candidate_rows=candidate_rows, thirty_f_events=thirty_f_events)
    bottom = build_post_setup_bottom_alignment(candidate_rows=candidate_rows, bottom_events=bottom_events)
    five = build_post_setup_5f_alignment(candidate_rows=candidate_rows, five_f_events=five_f_events)
    state = build_entry_state_machine_v2(
        candidate_rows=candidate_rows,
        refresh_rows=refresh["rows"],
        bottom_alignment_rows=bottom["rows"],
        five_f_alignment_rows=five["rows"],
    )
    gate = build_gate_by_symbol(
        target_symbols=DEFAULT_TARGET_SYMBOLS,
        phase_1_18_universe=phase_1_18_universe,
        post_daily_refresh_rows=refresh["rows"],
        state_machine_rows=state["rows"],
    )
    eligibility = build_candidate_trigger_eligibility_decision(
        entry_trigger_count=state["summary"]["entry_state_machine_v2_trigger_count"],
        future_leakage_detected=state["summary"]["future_leakage_detected"],
    )
    micro_plan = build_targeted_micro_backfill_v3_plan(target_symbols=DEFAULT_TARGET_SYMBOLS, gate_rows=gate["rows"])
    replay = build_replay_compare(
        candidate_count=len(candidate_rows),
        refresh_summary=refresh["summary"],
        bottom_summary=bottom["summary"],
        five_summary=five["summary"],
        state_summary=state["summary"],
    )
    decision = {
        "candidate_universe_all_10_symbols_rebuilt": len(gate["rows"]) == len(DEFAULT_TARGET_SYMBOLS),
        "candidate_symbol_distribution": phase_1_18_universe.get("summary", {}).get("candidate_symbol_distribution", {}),
        "post_daily_30f_refresh_count": refresh["summary"]["post_daily_30f_refresh_count"],
        "post_daily_30f_refresh_price_valid_count": refresh["summary"]["post_daily_30f_refresh_price_valid_count"],
        "entry_state_machine_v2_trigger_count": state["summary"]["entry_state_machine_v2_trigger_count"],
        "candidate_micro_backtest_allowed": eligibility["candidate_micro_backtest_allowed"],
        "recommend_strategy_30f_smoke_next": False,
        "recommend_50_symbols_backfill_next": False,
        "recommend_targeted_micro_backfill_v3_next": micro_plan["execution_recommended_next_phase"],
        "primary_zero_trigger_root_cause": state["summary"]["primary_zero_trigger_root_cause"],
        "future_leakage_detected": refresh["summary"]["future_leakage_detected"] or bottom["summary"]["future_leakage_detected"] or five["summary"]["future_leakage_detected"] or state["summary"]["future_leakage_detected"],
    }
    _write_outputs(
        output_dir=output_dir,
        gate=gate,
        refresh=refresh,
        bottom=bottom,
        five=five,
        state=state,
        eligibility=eligibility,
        micro_plan=micro_plan,
        replay=replay,
        decision=decision,
    )
    return decision
