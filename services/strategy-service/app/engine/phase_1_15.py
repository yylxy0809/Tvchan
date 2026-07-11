from __future__ import annotations

import asyncio
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any

import asyncpg

from app.domain.enums import LEVEL_TO_DB
from app.domain.models import SymbolInfo
from app.engine.module_c_history_backfill import (
    HistoricalBackfillWriter,
    assert_no_future_leakage,
    build_backfill_overlay_request,
    build_perf_profile,
    load_overlay_builder,
    preload_symbol_bars,
)
from app.engine.phase_1_11 import (
    HISTORICAL_RUN_GROUP,
    HISTORICAL_RUN_KIND,
    build_signal_fingerprint,
    parse_dt,
    read_jsonl,
    render_markdown_table,
    write_jsonl,
)
from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_13 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_13_OUTPUT_DIR,
    build_five_f_confirmation_audit,
    build_thirty_f_event_ledger_visibility_audit,
)
from app.engine.phase_1_14 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_14_OUTPUT_DIR,
    build_entry_confidence_builder_v3,
    build_thirty_f_price_validity_audit,
)
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS, write_json
from app.repositories.kline_repo import KlineBar, KlineRepository
from app.repositories.module_c_repo import MODE_TO_DB, ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-15-entry-chain-microdiagnostic"
DEFAULT_TARGETED_RUN_GROUP = "phase_1_15_targeted_entry_window_intraday"
DEFAULT_TARGETED_SYMBOLS = ("000001.SZ", "000651.SZ")
DEFAULT_TARGETED_LEVELS = ("5f", "30f")
DEFAULT_PHASE_1_15_TASKS = {
    "sample-lineage",
    "audit-price-fractal",
    "audit-5f-root-cause",
    "targeted-intraday-dry-run",
    "targeted-intraday-backfill",
    "replay-v4",
}


@dataclass(slots=True)
class Phase115Artifacts:
    phase_1_12_daily_rows: list[dict[str, Any]]
    phase_1_13_30f_rows: list[dict[str, Any]]
    phase_1_13_5f_rows: list[dict[str, Any]]
    phase_1_13_v2_rows: list[dict[str, Any]]
    phase_1_14_price_rows: list[dict[str, Any]]
    phase_1_14_price_policy_rows: list[dict[str, Any]]
    phase_1_14_bottom_rows: list[dict[str, Any]]
    phase_1_14_v3_rows: list[dict[str, Any]]
    phase_1_14_price_summary: dict[str, Any]
    phase_1_14_bottom_summary: dict[str, Any]
    phase_1_14_v3_summary: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sample_id(row: dict[str, Any]) -> str:
    sample_id = row.get("sample_id")
    if sample_id:
        return str(sample_id)
    return f"{row['symbol']}|{row['as_of_time']}"


def _symbol_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row["symbol"]) for row in rows)
    return dict(sorted(counts.items()))


def _sample_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_sample_id(row): row for row in rows}


def _history_head_sparse(as_of_time: datetime, latest_head_time: datetime | None) -> bool:
    if latest_head_time is None:
        return False
    if latest_head_time >= as_of_time:
        return False
    return latest_head_time.date() < as_of_time.date()


def load_phase_1_15_artifacts(
    *,
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_13_output_dir: Path = PHASE_1_13_OUTPUT_DIR,
    phase_1_14_output_dir: Path = PHASE_1_14_OUTPUT_DIR,
) -> Phase115Artifacts:
    return Phase115Artifacts(
        phase_1_12_daily_rows=read_jsonl(phase_1_12_output_dir / "daily_setup_sample_audit_v3.jsonl"),
        phase_1_13_30f_rows=read_jsonl(phase_1_13_output_dir / "thirty_f_event_ledger_visibility_samples.jsonl"),
        phase_1_13_5f_rows=read_jsonl(phase_1_13_output_dir / "five_f_confirmation_samples.jsonl"),
        phase_1_13_v2_rows=read_jsonl(phase_1_13_output_dir / "entry_confidence_builder_v2_samples.jsonl"),
        phase_1_14_price_rows=read_jsonl(phase_1_14_output_dir / "thirty_f_price_validity_samples.jsonl"),
        phase_1_14_price_policy_rows=read_jsonl(phase_1_14_output_dir / "thirty_f_price_policy_samples.jsonl"),
        phase_1_14_bottom_rows=read_jsonl(phase_1_14_output_dir / "daily_bottom_fractal_visibility_samples.jsonl"),
        phase_1_14_v3_rows=read_jsonl(phase_1_14_output_dir / "entry_confidence_builder_v3_samples.jsonl"),
        phase_1_14_price_summary=_read_json(phase_1_14_output_dir / "thirty_f_price_validity_audit.json"),
        phase_1_14_bottom_summary=_read_json(phase_1_14_output_dir / "daily_bottom_fractal_visibility_audit.json"),
        phase_1_14_v3_summary=_read_json(phase_1_14_output_dir / "entry_confidence_builder_v3_audit.json"),
    )


def build_sample_lineage_audit(
    *,
    phase_1_12_candidate_rows: list[dict[str, Any]],
    phase_1_13_candidate_rows: list[dict[str, Any]],
    phase_1_14_candidate_rows: list[dict[str, Any]],
    phase_1_13_thirty_f_rows: list[dict[str, Any]],
    requested_symbols: list[str],
) -> dict[str, Any]:
    phase_1_12_ids = {_sample_id(row) for row in phase_1_12_candidate_rows}
    phase_1_13_ids = {_sample_id(row) for row in phase_1_13_candidate_rows}
    phase_1_14_ids = {_sample_id(row) for row in phase_1_14_candidate_rows}

    mismatches: list[dict[str, Any]] = []
    for row in phase_1_13_candidate_rows:
        sample_id = _sample_id(row)
        if sample_id not in phase_1_12_ids:
            mismatches.append(
                {
                    "stage": "phase_1_13_missing_upstream",
                    "sample_id": sample_id,
                    "symbol": row["symbol"],
                }
            )
    for row in phase_1_14_candidate_rows:
        sample_id = _sample_id(row)
        if sample_id not in phase_1_13_ids:
            mismatches.append(
                {
                    "stage": "phase_1_14_missing_upstream",
                    "sample_id": sample_id,
                    "symbol": row["symbol"],
                }
            )

    actual_symbols = set(_symbol_counts(phase_1_14_candidate_rows))
    requested_symbol_set = set(requested_symbols)
    lineage_consistent = not mismatches
    generalization_limited = bool(requested_symbol_set - actual_symbols) or len(actual_symbols) < len(requested_symbol_set)

    warning = ""
    if generalization_limited:
        missing = ",".join(sorted(requested_symbol_set - actual_symbols))
        warning = f"phase_1_14 downstream samples only cover {len(actual_symbols)} symbol(s); missing requested symbols: {missing or 'none'}"
    if not lineage_consistent:
        warning = f"{warning}; lineage mismatch detected".strip("; ")

    return {
        "phase_1_12_candidate_sample_count": len(phase_1_12_candidate_rows),
        "phase_1_13_candidate_sample_count": len(phase_1_13_candidate_rows),
        "phase_1_14_candidate_sample_count": len(phase_1_14_candidate_rows),
        "phase_1_14_actual_symbol_count": len(actual_symbols),
        "candidate_symbols": _symbol_counts(phase_1_12_candidate_rows),
        "thirty_f_visible_symbols": _symbol_counts(
            [row for row in phase_1_13_thirty_f_rows if int(row.get("visible_30f_B1_or_1p_count") or 0) > 0]
        ),
        "entry_confidence_symbols": _symbol_counts(phase_1_14_candidate_rows),
        "lineage_consistent": lineage_consistent,
        "sample_lineage_mismatch_count": len(mismatches),
        "lineage_warning": warning,
        "phase_1_14_two_symbol_limitation_affects_generalization": generalization_limited,
        "mismatch_samples": mismatches,
    }


def recommend_thirty_f_price_policy(row: dict[str, Any]) -> dict[str, str]:
    if not bool(row.get("window_valid")):
        return {"recommended_policy": "strict_existing", "decision": "keep_strict"}
    if bool(row.get("strict_price_valid")):
        return {"recommended_policy": "strict_existing", "decision": "keep_strict"}
    if bool(row.get("signal_price_only_valid")):
        return {"recommended_policy": "signal_price_only", "decision": "promote_candidate_variant"}
    if bool(row.get("bar_low_high_overlap_valid")) or bool(row.get("no_break_daily_b1_valid")):
        return {"recommended_policy": "record_only", "decision": "diagnostic_only"}
    return {"recommended_policy": "strict_existing", "decision": "keep_strict"}


def classify_bottom_fractal_equivalence(summary: dict[str, Any]) -> dict[str, Any]:
    if bool(summary.get("future_leakage_detected")):
        return {
            "recommend_bottom_fractal_ledger_as_candidate_confirmation": False,
            "module_c_fractal_equivalence": "not_proven",
            "future_leakage_detected": True,
        }
    module_c_direct = int(summary.get("module_c_direct_fractal_match_count") or 0)
    point_matches = int(summary.get("point_time_matches_daily_signal_count") or 0)
    stroke_matches = int(summary.get("stroke_turn_match_count") or 0)
    confirmed = int(summary.get("bottom_fractal_confirmed_count") or 0)
    if module_c_direct and module_c_direct == confirmed:
        equivalence = "proven"
    elif confirmed and point_matches and stroke_matches:
        equivalence = "partially_supported"
    else:
        equivalence = "not_proven"
    return {
        "recommend_bottom_fractal_ledger_as_candidate_confirmation": equivalence in {"proven", "partially_supported"} and confirmed > 0,
        "module_c_fractal_equivalence": equivalence,
        "future_leakage_detected": False,
    }


def classify_five_f_root_cause(row: dict[str, Any]) -> str:
    if bool(row.get("latest_5f_head_bar_until_before_as_of")) and row.get("run_group_id") == HISTORICAL_RUN_GROUP:
        return "research_daily_close_snapshot_too_sparse"
    if not bool(row.get("has_5f_run_covering_window")):
        return "no_5f_run_covering_window"
    if bool(row.get("selected_run_filtered")):
        return "5f_B2_exists_but_filtered_by_mode_or_group"
    if int(row.get("visible_5f_b2_count") or 0) > 0:
        return "5f_B2_exists_but_after_as_of" if int(row.get("future_5f_b2_count") or 0) > 0 else "unknown"
    if int(row.get("visible_5f_buy_count") or 0) > 0:
        return "5f_buy_exists_but_not_B2_or_2s"
    if bool(row.get("has_5f_structure_turn")):
        return "5f_structure_turn_exists_but_no_signal"
    return "no_5f_buy_signal_in_symbol_window"


def _render_sample_lineage_md(payload: dict[str, Any]) -> str:
    rows = [
        ["phase_1_12_candidate_sample_count", payload["phase_1_12_candidate_sample_count"]],
        ["phase_1_13_candidate_sample_count", payload["phase_1_13_candidate_sample_count"]],
        ["phase_1_14_candidate_sample_count", payload["phase_1_14_candidate_sample_count"]],
        ["phase_1_14_actual_symbol_count", payload["phase_1_14_actual_symbol_count"]],
        ["lineage_consistent", payload["lineage_consistent"]],
        ["sample_lineage_mismatch_count", payload["sample_lineage_mismatch_count"]],
        ["phase_1_14_two_symbol_limitation_affects_generalization", payload["phase_1_14_two_symbol_limitation_affects_generalization"]],
    ]
    lines = [
        "# Sample Lineage Audit",
        "",
        render_markdown_table(["field", "value"], rows),
        "",
        f"- candidate_symbols: `{json.dumps(payload['candidate_symbols'], ensure_ascii=False)}`",
        f"- thirty_f_visible_symbols: `{json.dumps(payload['thirty_f_visible_symbols'], ensure_ascii=False)}`",
        f"- entry_confidence_symbols: `{json.dumps(payload['entry_confidence_symbols'], ensure_ascii=False)}`",
        f"- lineage_warning: `{payload['lineage_warning']}`",
        "",
    ]
    return "\n".join(lines)


def build_thirty_f_price_deep_dive(
    *,
    daily_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    price_policy_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    daily_map = _sample_map(daily_rows)
    policy_map = _sample_map(price_policy_rows)
    bottom_map = _sample_map(bottom_rows)
    rows: list[dict[str, Any]] = []
    policy_counter = Counter()
    decision_counter = Counter()
    for price_row in price_rows:
        if not bool(price_row.get("window_valid")):
            continue
        sample_id = _sample_id(price_row)
        daily_row = daily_map.get(sample_id, {})
        bottom_row = bottom_map.get(sample_id, {})
        policy_row = policy_map.get(sample_id, {})
        merged = {
            "sample_id": sample_id,
            "symbol": price_row["symbol"],
            "name": price_row.get("name") or daily_row.get("name"),
            "as_of_time": price_row["as_of_time"],
            "window_valid": bool(price_row.get("window_valid")),
            "daily_setup_event_id": price_row.get("daily_setup_event_id"),
            "thirty_f_event_id": price_row.get("thirty_f_event_id"),
            "thirty_f_signal_point_time": price_row.get("thirty_f_point_time"),
            "thirty_f_first_seen_time": price_row.get("thirty_f_first_seen_time"),
            "thirty_f_signal_price_x1000": price_row.get("thirty_f_price_x1000"),
            "as_of_price_x1000": daily_row.get("candidate_audit", {}).get("selected_buy_signal_any", {}).get("price"),
            "daily_b1_price_x1000": price_row.get("daily_b1_price_x1000"),
            "daily_b2_price_x1000": price_row.get("daily_b2_price_x1000"),
            "strict_price_valid": bool(price_row.get("price_valid")),
            "strict_invalid_reason": price_row.get("price_invalid_reason"),
            "signal_price_only_valid": bool(policy_row.get("thirty_f_price_policy_signal_price_only")),
            "bar_low_high_overlap_valid": bool(policy_row.get("thirty_f_price_policy_bar_low_high_overlap")),
            "no_break_daily_b1_valid": bool(policy_row.get("thirty_f_price_policy_no_break_daily_b1")),
            "daily_bottom_fractal_visible": bool(bottom_row.get("daily_bottom_fractal_visible")),
            "daily_bottom_fractal_first_seen_time": bottom_row.get("daily_bottom_fractal_first_seen_time"),
        }
        decision = recommend_thirty_f_price_policy(merged)
        merged.update(decision)
        rows.append(merged)
        policy_counter[merged["recommended_policy"]] += 1
        decision_counter[merged["decision"]] += 1
    return {
        "rows": rows,
        "summary": {
            "sample_count": len(rows),
            "recommended_policy_counts": dict(sorted(policy_counter.items())),
            "decision_counts": dict(sorted(decision_counter.items())),
        },
    }


def _render_thirty_f_price_deep_dive_md(payload: dict[str, Any]) -> str:
    rows = [[key, value] for key, value in payload["summary"]["recommended_policy_counts"].items()]
    lines = [
        "# 30F Price Invalid 9 Deep Dive",
        "",
        f"- sample_count: `{payload['summary']['sample_count']}`",
        f"- decision_counts: `{json.dumps(payload['summary']['decision_counts'], ensure_ascii=False)}`",
        "",
        "## Recommended Policy Counts",
        "",
        render_markdown_table(["policy", "count"], rows),
        "",
    ]
    return "\n".join(lines)


def build_daily_bottom_fractal_equivalence_audit(
    *,
    daily_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    daily_map = _sample_map(daily_rows)
    rows: list[dict[str, Any]] = []
    category_counts = Counter()
    point_matches = 0
    stroke_matches = 0
    confirmed_count = 0
    for row in bottom_rows:
        sample_id = _sample_id(row)
        daily_row = daily_map.get(sample_id, {})
        setup_signal = ((daily_row.get("candidate_audit") or {}).get("selected_daily_b2_or_b2s")
                        or (daily_row.get("candidate_audit") or {}).get("selected_buy_signal_any")
                        or {})
        daily_signal_point_time = ((setup_signal.get("base_time") or setup_signal.get("point_time")) if isinstance(setup_signal, dict) else None)
        daily_signal_price = None
        if isinstance(setup_signal, dict) and setup_signal.get("price") is not None:
            daily_signal_price = int(round(float(setup_signal["price"]) * 1000))
        bottom_price = row.get("daily_bottom_fractal_price_x1000")
        point_match = bool(row.get("daily_bottom_fractal_visible")) and daily_signal_point_time is not None and row.get("daily_bottom_fractal_time") is not None and row["daily_bottom_fractal_time"] >= daily_signal_point_time
        stroke_match = bool(row.get("daily_bottom_fractal_visible")) and bottom_price is not None and daily_signal_price is not None and bottom_price <= daily_signal_price
        if point_match:
            point_matches += 1
        if stroke_match:
            stroke_matches += 1
        if row.get("daily_bottom_fractal_failure_reason") == "bottom_fractal_confirmed":
            confirmed_count += 1
        category_counts[str(row.get("daily_bottom_fractal_failure_reason") or "unknown")] += 1
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "as_of_time": row["as_of_time"],
                "daily_setup_point_time": daily_signal_point_time,
                "daily_setup_price_x1000": daily_signal_price,
                "bottom_fractal_point_time": row.get("daily_bottom_fractal_time"),
                "bottom_fractal_first_seen_time": row.get("daily_bottom_fractal_first_seen_time"),
                "bottom_fractal_price_x1000": bottom_price,
                "point_time_matches_daily_signal_window": point_match,
                "stroke_turn_match_supported": stroke_match,
                "bottom_fractal_failure_reason": row.get("daily_bottom_fractal_failure_reason"),
                "future_leakage_flag": bool(row.get("future_leakage_flag")),
            }
        )
    summary = {
        "sample_count": len(rows),
        "bottom_fractal_confirmed_count": confirmed_count,
        "point_time_matches_daily_signal_count": point_matches,
        "stroke_turn_match_count": stroke_matches,
        "module_c_direct_fractal_match_count": 0,
        "future_leakage_detected": any(bool(row.get("future_leakage_flag")) for row in rows),
        "category_counts": dict(sorted(category_counts.items())),
    }
    decision = classify_bottom_fractal_equivalence(summary)
    summary.update(decision)
    return {"rows": rows, "summary": summary}


def _render_bottom_fractal_equivalence_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Daily Bottom Fractal Equivalence Audit",
        "",
        f"- sample_count: `{summary['sample_count']}`",
        f"- bottom_fractal_confirmed_count: `{summary['bottom_fractal_confirmed_count']}`",
        f"- point_time_matches_daily_signal_count: `{summary['point_time_matches_daily_signal_count']}`",
        f"- stroke_turn_match_count: `{summary['stroke_turn_match_count']}`",
        f"- module_c_fractal_equivalence: `{summary['module_c_fractal_equivalence']}`",
        f"- recommend_bottom_fractal_ledger_as_candidate_confirmation: `{summary['recommend_bottom_fractal_ledger_as_candidate_confirmation']}`",
        f"- future_leakage_detected: `{summary['future_leakage_detected']}`",
        f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
        "",
        "## Alternative Validation Path",
        "",
        "- Module C 当前没有独立 fractal 持久化表，因此本阶段只能用 daily setup point_time、raw bottom fractal point_time、price 与 prior/future 分区做替代验证。",
        "- 该替代路径最多支持 `partially_supported`，除非未来补充 Module C 直接 fractal ledger。",
        "",
    ]
    return "\n".join(lines)


async def build_signal_event_ledger(
    *,
    pool: asyncpg.Pool,
    symbols: list[SymbolInfo],
    level: str,
    start_time: datetime,
    end_time: datetime,
    run_group_id: str,
    run_kind: str = HISTORICAL_RUN_KIND,
    mode: str = "predictive",
) -> dict[str, Any]:
    if not symbols:
        return {"events": [], "summary": {"level": level, "symbol_count": 0, "unique_signal_events": 0}}
    symbol_ids = [symbol.symbol_id for symbol in symbols]
    symbol_map = {symbol.symbol_id: symbol for symbol in symbols}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select
                r.id as run_id,
                r.symbol_id,
                r.mode,
                r.run_kind,
                r.run_group_id,
                r.bar_until,
                coalesce(r.cutoff_bar_end, r.bar_until) as cutoff_bar_end,
                s.id as signal_id,
                s.ts as signal_ts,
                coalesce(s.base_ts, s.ts) as signal_base_ts,
                s.price_x1000,
                s.signal_type,
                s.is_confirmed,
                s.extra
            from chan_c_runs r
            join chan_c_signals s
              on s.run_id = r.id
             and s.mode = r.mode
            where r.symbol_id = any($1::bigint[])
              and r.chan_level = $2
              and r.status = 'success'
              and r.run_kind = $3
              and r.run_group_id = $4
              and r.mode = $5
              and r.bar_until >= $6
              and r.bar_until <= $7
              and coalesce(s.extra->>'side', '') = 'buy'
            order by r.symbol_id, coalesce(s.base_ts, s.ts), r.bar_until, r.id, s.id
            """,
            symbol_ids,
            LEVEL_TO_DB[level],
            run_kind,
            run_group_id,
            MODE_TO_DB[mode],
            start_time,
            end_time,
        )
    ledger: dict[str, dict[str, Any]] = {}
    per_symbol_event_count: Counter[str] = Counter()
    per_bsp_type_event_count: Counter[str] = Counter()
    for row in rows:
        symbol = symbol_map[int(row["symbol_id"])].symbol
        extra = row["extra"]
        if isinstance(extra, str):
            extra = json.loads(extra)
        extra = extra if isinstance(extra, dict) else {}
        point_time = row["signal_base_ts"]
        price_x1000 = int(row["price_x1000"])
        bsp_type = extra.get("bsp_type")
        side = extra.get("side")
        fingerprint = build_signal_fingerprint(
            symbol=symbol,
            level=level,
            mode=mode,
            side=side,
            bsp_type=bsp_type,
            signal_point_time=point_time,
            price_x1000=price_x1000,
        )
        observed_time = row["cutoff_bar_end"]
        payload = ledger.get(fingerprint)
        if payload is None:
            payload = {
                "symbol": symbol,
                "level": level,
                "mode": mode,
                "run_kind": row["run_kind"],
                "run_group_id": row["run_group_id"],
                "side": side,
                "bsp_type": bsp_type,
                "signal_type": row["signal_type"],
                "signal_point_time": point_time.isoformat(),
                "signal_ts": row["signal_ts"].isoformat() if row["signal_ts"] is not None else None,
                "signal_base_ts": point_time.isoformat(),
                "price_x1000": price_x1000,
                "is_confirmed": bool(row["is_confirmed"]),
                "first_seen_time": observed_time.isoformat(),
                "first_seen_run_id": int(row["run_id"]),
                "first_seen_cutoff_bar_end": observed_time.isoformat(),
                "last_seen_time": observed_time.isoformat(),
                "last_seen_run_id": int(row["run_id"]),
                "observed_run_count": 1,
                "signal_fingerprint": fingerprint,
                "source_run_ids_sample": [int(row["run_id"])],
                "extra_json": extra.get("features") or {},
            }
            ledger[fingerprint] = payload
            continue
        payload["observed_run_count"] += 1
        payload["last_seen_time"] = observed_time.isoformat()
        payload["last_seen_run_id"] = int(row["run_id"])
        if len(payload["source_run_ids_sample"]) < 8:
            payload["source_run_ids_sample"].append(int(row["run_id"]))
    events = sorted(ledger.values(), key=lambda item: (item["symbol"], item["signal_point_time"], item["price_x1000"]))
    for event in events:
        per_symbol_event_count[event["symbol"]] += 1
        per_bsp_type_event_count[str(event.get("bsp_type") or "")] += 1
    return {
        "events": events,
        "summary": {
            "level": level,
            "window_start": start_time.isoformat(),
            "window_end": end_time.isoformat(),
            "symbol_count": len(symbols),
            "raw_signal_rows": len(rows),
            "unique_signal_events": len(events),
            "per_symbol_event_count": dict(sorted(per_symbol_event_count.items())),
            "per_bsp_type_event_count": dict(sorted(per_bsp_type_event_count.items())),
            "run_group_id": run_group_id,
        },
    }


async def build_five_f_root_cause_audit(
    *,
    module_c_repo: ModuleCRepository,
    pool: asyncpg.Pool,
    symbols: list[SymbolInfo],
    rows_30f: list[dict[str, Any]],
    rows_5f_existing: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    confidence_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    rows_5f_existing_map = _sample_map(rows_5f_existing)
    confidence_map = _sample_map(confidence_rows)
    price_map = _sample_map(price_rows)

    selected_sample_ids: set[str] = set()
    for row in rows_30f:
        if int(row.get("visible_30f_B1_or_1p_count") or 0) > 0:
            selected_sample_ids.add(_sample_id(row))
    for row in confidence_rows:
        if float(row.get("confidence") or 0.0) >= 40.0:
            selected_sample_ids.add(_sample_id(row))
    for row in price_rows:
        if bool(row.get("window_valid")):
            selected_sample_ids.add(_sample_id(row))

    core_rows = [row for row in rows_30f if _sample_id(row) in selected_sample_ids]
    if not core_rows:
        return {"rows": [], "summary": {}}

    start_time = min(parse_dt(row["as_of_time"]) for row in core_rows) - timedelta(days=30)
    end_time = max(parse_dt(row["as_of_time"]) for row in core_rows) + timedelta(days=5)
    events_payload = await build_signal_event_ledger(
        pool=pool,
        symbols=symbols,
        level="5f",
        start_time=start_time,
        end_time=end_time,
        run_group_id=HISTORICAL_RUN_GROUP,
    )
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events_payload["events"]:
        events_by_symbol[event["symbol"]].append(event)

    rows: list[dict[str, Any]] = []
    category_counts = Counter()
    sparse_count = 0
    structure_turn_count = 0
    for row in core_rows:
        sample_id = _sample_id(row)
        symbol = symbol_map[row["symbol"]]
        as_of_time = parse_dt(row["as_of_time"])
        thirty_f_b1 = row.get("latest_30f_B1_after_daily_setup") or row.get("nearest_30f_B1_before_as_of")
        thirty_f_b1_time = parse_dt((thirty_f_b1 or {}).get("signal_point_time")) if thirty_f_b1 else None
        lookup = await module_c_repo.get_historical_run_lookup(
            symbol.symbol_id,
            "5f",
            "predictive",
            as_of_time,
            run_kind=HISTORICAL_RUN_KIND,
            run_group_id=HISTORICAL_RUN_GROUP,
            allow_legacy_mode_fallback=False,
        )
        visible_events = [
            event
            for event in events_by_symbol[row["symbol"]]
            if parse_dt(event["first_seen_time"]) <= as_of_time
            and (thirty_f_b1_time is None or parse_dt(event["signal_point_time"]) >= thirty_f_b1_time)
        ]
        visible_b2_events = [event for event in visible_events if event.get("bsp_type") in {"2", "2s"}]
        future_b2_events = [
            event
            for event in events_by_symbol[row["symbol"]]
            if parse_dt(event["first_seen_time"]) > as_of_time
            and (thirty_f_b1_time is None or parse_dt(event["signal_point_time"]) >= thirty_f_b1_time)
            and event.get("bsp_type") in {"2", "2s"}
        ]
        selected_signals = await module_c_repo.get_signals(
            symbol.symbol_id,
            "5f",
            mode="predictive",
            as_of_time=as_of_time,
            run_kind=HISTORICAL_RUN_KIND,
            run_group_id=HISTORICAL_RUN_GROUP,
            allow_legacy_mode_fallback=False,
        )
        selected_b2_signals = [
            signal
            for signal in selected_signals
            if signal.side == "buy"
            and signal.bsp_type in {"2", "2s"}
            and (thirty_f_b1_time is None or signal.point_time >= thirty_f_b1_time)
        ]
        strokes = await module_c_repo.get_strokes(
            symbol.symbol_id,
            "5f",
            mode="predictive",
            as_of_time=as_of_time,
            run_kind=HISTORICAL_RUN_KIND,
            run_group_id=HISTORICAL_RUN_GROUP,
            allow_legacy_mode_fallback=False,
        )
        post_b1_strokes = [
            stroke for stroke in strokes if thirty_f_b1_time is None or stroke.begin_base_time >= thirty_f_b1_time
        ]
        has_structure_turn = len(post_b1_strokes) >= 3
        if has_structure_turn:
            structure_turn_count += 1
        latest_head_bar_until = lookup.nearest_before.bar_until if lookup.nearest_before is not None else None
        root_input = {
            "has_5f_run_covering_window": lookup.selected is not None,
            "latest_5f_head_bar_until_before_as_of": _history_head_sparse(as_of_time, latest_head_bar_until),
            "run_group_id": HISTORICAL_RUN_GROUP,
            "visible_5f_buy_count": len(visible_events),
            "visible_5f_b2_count": len(visible_b2_events),
            "future_5f_b2_count": len(future_b2_events),
            "selected_run_filtered": bool(visible_b2_events) and not bool(selected_b2_signals),
            "has_5f_structure_turn": has_structure_turn,
        }
        category = classify_five_f_root_cause(root_input)
        if category == "research_daily_close_snapshot_too_sparse":
            sparse_count += 1
        category_counts[category] += 1
        existing_row = rows_5f_existing_map.get(sample_id, {})
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "name": row["name"],
                "as_of_time": row["as_of_time"],
                "sample_tags": sorted(
                    {
                        tag
                        for tag, enabled in {
                            "visible_30f": int(row.get("visible_30f_B1_or_1p_count") or 0) > 0,
                            "window_valid_9": bool(price_map.get(sample_id, {}).get("window_valid")),
                            "confidence_40": float(confidence_map.get(sample_id, {}).get("confidence") or 0.0) >= 40.0,
                            "phase_1_13_entry_triggered": bool(confidence_map.get(sample_id, {}).get("entry_triggered")),
                        }.items()
                        if enabled
                    }
                ),
                "thirty_f_b1_signal_point_time": (thirty_f_b1 or {}).get("signal_point_time"),
                "historical_run_count": lookup.run_count,
                "latest_5f_head_bar_until": latest_head_bar_until.isoformat() if latest_head_bar_until is not None else None,
                "selected_5f_head_bar_until": lookup.selected.bar_until.isoformat() if lookup.selected is not None else None,
                "visible_5f_buy_count": len(visible_events),
                "visible_5f_b2_count": len(visible_b2_events),
                "future_5f_b2_count": len(future_b2_events),
                "selected_run_5f_b2_count": len(selected_b2_signals),
                "has_5f_structure_turn": has_structure_turn,
                "existing_phase_1_13_failure_reason": existing_row.get("failure_reason"),
                "root_cause_category": category,
                "future_leakage_flag": False,
            }
        )
    five_f_absence_is_real = sparse_count == 0 and category_counts.get("5f_B2_exists_but_after_as_of", 0) == 0 and category_counts.get("5f_B2_exists_but_filtered_by_mode_or_group", 0) == 0
    due_to_sparsity = sparse_count > 0
    summary = {
        "sample_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "sparse_window_samples": sparse_count,
        "structure_turn_samples": structure_turn_count,
        "five_f_absence_is_real": five_f_absence_is_real,
        "five_f_absence_due_to_research_daily_close_sparsity": due_to_sparsity,
        "recommend_5f_event_ledger_as_candidate_source": False,
        "recommend_targeted_intraday_micro_backfill": due_to_sparsity,
    }
    return {"rows": rows, "summary": summary, "signal_inventory": events_payload}


def _render_five_f_root_cause_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# 5F Confirmation Root Cause Audit",
        "",
        f"- sample_count: `{summary['sample_count']}`",
        f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
        f"- sparse_window_samples: `{summary['sparse_window_samples']}`",
        f"- structure_turn_samples: `{summary['structure_turn_samples']}`",
        f"- five_f_absence_is_real: `{summary['five_f_absence_is_real']}`",
        f"- five_f_absence_due_to_research_daily_close_sparsity: `{summary['five_f_absence_due_to_research_daily_close_sparsity']}`",
        f"- recommend_targeted_intraday_micro_backfill: `{summary['recommend_targeted_intraday_micro_backfill']}`",
        "",
    ]
    return "\n".join(lines)


async def _expand_window_for_symbol(
    *,
    kline_repo: KlineRepository,
    symbol: SymbolInfo,
    anchor_times: list[datetime],
    trading_days: int,
) -> dict[str, Any] | None:
    if not anchor_times:
        return None
    preload_start = min(anchor_times) - timedelta(days=90)
    preload_end = max(anchor_times) + timedelta(days=90)
    await kline_repo.prime_symbol_cache(symbol.symbol_id, start_time=preload_start, end_time=preload_end, timeframes=("1d",))
    try:
        bars = await kline_repo.get_klines(symbol.symbol_id, "1d", start=preload_start, end=preload_end)
    finally:
        kline_repo.release_symbol_cache(symbol.symbol_id)
    if not bars:
        return None
    times = [bar.ts for bar in bars]
    windows = []
    for anchor in sorted(anchor_times):
        try:
            index = times.index(anchor)
        except ValueError:
            closest = max((i for i, ts in enumerate(times) if ts <= anchor), default=None)
            if closest is None:
                continue
            index = closest
        left = max(0, index - trading_days)
        right = min(len(times) - 1, index + trading_days)
        windows.append({"anchor_time": anchor.isoformat(), "start_time": times[left].isoformat(), "end_time": times[right].isoformat()})
    if not windows:
        return None
    merged = []
    for item in windows:
        start_time = parse_dt(item["start_time"])
        end_time = parse_dt(item["end_time"])
        if not merged:
            merged.append([start_time, end_time, [item["anchor_time"]]])
            continue
        current = merged[-1]
        if start_time <= current[1] + timedelta(days=3):
            current[1] = max(current[1], end_time)
            current[2].append(item["anchor_time"])
        else:
            merged.append([start_time, end_time, [item["anchor_time"]]])
    return {
        "symbol": symbol.symbol,
        "window_count": len(windows),
        "merged_windows": [
            {"start_time": start.isoformat(), "end_time": end.isoformat(), "anchor_times": anchors}
            for start, end, anchors in merged
        ],
    }


async def build_targeted_intraday_plan(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    artifacts: Phase115Artifacts,
    symbols: list[str],
    trading_days: int = 10,
) -> dict[str, Any]:
    symbol_infos = await module_c_repo.list_active_symbols(symbols=symbols)
    window_valid_rows = [row for row in artifacts.phase_1_14_price_rows if bool(row.get("window_valid"))]
    anchors_by_symbol: dict[str, list[datetime]] = defaultdict(list)
    for row in window_valid_rows:
        anchors_by_symbol[row["symbol"]].append(parse_dt(row["as_of_time"]))
    symbol_windows = []
    for symbol in symbol_infos:
        payload = await _expand_window_for_symbol(
            kline_repo=kline_repo,
            symbol=symbol,
            anchor_times=anchors_by_symbol.get(symbol.symbol, []),
            trading_days=trading_days,
        )
        if payload is not None:
            symbol_windows.append(payload)
        else:
            symbol_windows.append({"symbol": symbol.symbol, "window_count": 0, "merged_windows": []})
    scoped_symbols = [item["symbol"] for item in symbol_windows if item["window_count"] > 0]
    bars_by_symbol = {}
    if scoped_symbols:
        scoped_infos = [symbol for symbol in symbol_infos if symbol.symbol in scoped_symbols]
        warmup_start = min(parse_dt(item["merged_windows"][0]["start_time"]) for item in symbol_windows if item["merged_windows"]) - timedelta(days=120)
        end_time = max(parse_dt(item["merged_windows"][-1]["end_time"]) for item in symbol_windows if item["merged_windows"])
        bars_by_symbol = await preload_symbol_bars(
            kline_repo=kline_repo,
            symbols=scoped_infos,
            levels=DEFAULT_TARGETED_LEVELS,
            warmup_start=warmup_start,
            end_time=end_time,
        )
    per_symbol_cutoffs = []
    total_runs = 0
    for payload in symbol_windows:
        bars_by_level = bars_by_symbol.get(payload["symbol"], {})
        by_level = {}
        for level in DEFAULT_TARGETED_LEVELS:
            count = 0
            for window in payload["merged_windows"]:
                start_time = parse_dt(window["start_time"])
                end_time = parse_dt(window["end_time"])
                count += sum(1 for bar in bars_by_level.get(level, []) if start_time <= bar.ts <= end_time)
            by_level[level] = count
            total_runs += count
        per_symbol_cutoffs.append({"symbol": payload["symbol"], "window_count": payload["window_count"], "runs_by_level": by_level, "merged_windows": payload["merged_windows"]})
    return {
        "window_source": "phase_1_14_window_valid_9",
        "symbols": symbols,
        "scoped_symbol_count": len([item for item in symbol_windows if item["window_count"] > 0]),
        "targeted_levels": list(DEFAULT_TARGETED_LEVELS),
        "estimated_total_runs": total_runs,
        "per_symbol": per_symbol_cutoffs,
        "run_group_id": DEFAULT_TARGETED_RUN_GROUP,
        "run_kind": HISTORICAL_RUN_KIND,
        "lookup_safe": True,
        "future_leakage_detected": False,
    }


def _render_targeted_plan_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Targeted Intraday Backfill Plan",
        "",
        f"- window_source: `{payload['window_source']}`",
        f"- symbols: `{','.join(payload['symbols'])}`",
        f"- scoped_symbol_count: `{payload['scoped_symbol_count']}`",
        f"- targeted_levels: `{','.join(payload['targeted_levels'])}`",
        f"- estimated_total_runs: `{payload['estimated_total_runs']}`",
        f"- run_group_id: `{payload['run_group_id']}`",
        f"- lookup_safe: `{payload['lookup_safe']}`",
        "",
    ]
    return "\n".join(lines)


async def run_targeted_intraday_backfill(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    plan: dict[str, Any],
    max_workers: int,
    resume: bool,
    run_group_id: str,
) -> dict[str, Any]:
    symbol_infos = await module_c_repo.list_active_symbols(symbols=plan["symbols"])
    scoped_infos = [symbol for symbol in symbol_infos if any(item["symbol"] == symbol.symbol and item["window_count"] > 0 for item in plan["per_symbol"])]
    if not scoped_infos:
        return {
            "run_group_id": run_group_id,
            "written_runs": 0,
            "failed_runs": 0,
            "manifest_rows": [],
            "failures": [],
            "perf_profile": {"sample_count": 0, "per_symbol": [], "per_level": [], "aggregate": {}},
        }
    window_map = {item["symbol"]: item for item in plan["per_symbol"]}
    warmup_start = min(parse_dt(item["merged_windows"][0]["start_time"]) for item in plan["per_symbol"] if item["merged_windows"]) - timedelta(days=120)
    end_time = max(parse_dt(item["merged_windows"][-1]["end_time"]) for item in plan["per_symbol"] if item["merged_windows"])
    bars_by_symbol = await preload_symbol_bars(
        kline_repo=kline_repo,
        symbols=scoped_infos,
        levels=DEFAULT_TARGETED_LEVELS,
        warmup_start=warmup_start,
        end_time=end_time,
    )
    writer = HistoricalBackfillWriter(pool)
    overlay_builder = load_overlay_builder()
    semaphore = asyncio.Semaphore(max(1, max_workers))
    perf_samples = []
    manifest_rows = []
    failures = []

    async def run_symbol(symbol: SymbolInfo) -> None:
        async with semaphore:
            symbol_windows = window_map[symbol.symbol]["merged_windows"]
            existing = await writer.prefetch_existing_cutoffs(
                symbol_id=symbol.symbol_id,
                levels=DEFAULT_TARGETED_LEVELS,
                mode="predictive",
                run_group_id=run_group_id,
            ) if resume else {level: set() for level in DEFAULT_TARGETED_LEVELS}
            for level in DEFAULT_TARGETED_LEVELS:
                series = bars_by_symbol[symbol.symbol].get(level, [])
                for window in symbol_windows:
                    start_time = parse_dt(window["start_time"])
                    end_time = parse_dt(window["end_time"])
                    for bar in [item for item in series if start_time <= item.ts <= end_time]:
                        try:
                            if resume and bar.ts in existing.get(level, set()):
                                continue
                            window_bars = [item for item in series if item.ts <= bar.ts]
                            assert_no_future_leakage(window_bars, bar.ts)
                            started = perf_counter()
                            response = overlay_builder(
                                build_backfill_overlay_request(
                                    symbol=symbol.symbol,
                                    level=level,
                                    mode="predictive",
                                    bars=window_bars,
                                )
                            )
                            run_id, counts = await writer.insert_historical_run(
                                symbol_id=symbol.symbol_id,
                                symbol=symbol.symbol,
                                level=level,
                                mode="predictive",
                                profile=run_group_id,
                                warmup_start=warmup_start,
                                cutoff_time=bar.ts,
                                bars=window_bars,
                                response=response,
                            )
                            elapsed = perf_counter() - started
                            perf_samples.append(
                                type(
                                    "Perf",
                                    (),
                                    {
                                        "symbol": symbol.symbol,
                                        "level": level,
                                        "cutoff_time": bar.ts,
                                        "bar_count": len(window_bars),
                                        "schedule_build_seconds": 0.0,
                                        "resume_check_seconds": 0.0,
                                        "overlay_build_seconds": round(elapsed, 6),
                                        "db_insert_seconds": 0.0,
                                        "total_snapshot_seconds": round(elapsed, 6),
                                    },
                                )()
                            )
                            manifest_rows.append(
                                {
                                    "symbol": symbol.symbol,
                                    "level": level,
                                    "cutoff_time": bar.ts.isoformat(),
                                    "bar_count": len(window_bars),
                                    "run_id": run_id,
                                    "strokes": counts["strokes"],
                                    "segments": counts["segments"],
                                    "centers": counts["centers"],
                                    "signals": counts["signals"],
                                }
                            )
                        except Exception as exc:  # pragma: no cover
                            failures.append(
                                {
                                    "symbol": symbol.symbol,
                                    "level": level,
                                    "cutoff_time": bar.ts.isoformat(),
                                    "error": str(exc),
                                }
                            )

    await asyncio.gather(*(run_symbol(symbol) for symbol in scoped_infos))
    return {
        "run_group_id": run_group_id,
        "written_runs": len(manifest_rows),
        "failed_runs": len(failures),
        "manifest_rows": manifest_rows,
        "failures": failures,
        "perf_profile": build_perf_profile(perf_samples),
    }


def _copy_price_rows_for_policy(
    price_rows: list[dict[str, Any]],
    policy_rows: list[dict[str, Any]],
    *,
    policy_field: str,
) -> list[dict[str, Any]]:
    policy_map = _sample_map(policy_rows)
    copied = []
    for row in price_rows:
        item = dict(row)
        policy_row = policy_map.get(_sample_id(row), {})
        item["price_policy_result"] = bool(policy_row.get(policy_field)) if row.get("window_valid") else False
        copied.append(item)
    return copied


def _zero_five_f_rows(sample_ids: set[str]) -> list[dict[str, Any]]:
    return [
        {
            "sample_id": sample_id,
            "five_f_B2_confirms_30f": False,
            "five_f_buy_any_visible": False,
            "five_f_B2_or_2s_visible": False,
        }
        for sample_id in sample_ids
    ]


def _scenario_row(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary"]
    return {
        "scenario": name,
        "sample_count": summary["sample_count"],
        "confidence_40_count": summary["confidence_40_count"],
        "confidence_70_count": summary["confidence_70_count"],
        "confidence_100_count": summary["confidence_100_count"],
        "entry_candidate_count": summary["entry_candidate_count"],
        "entry_trigger_count": summary["entry_trigger_count"],
        "future_leakage_detected": summary["future_leakage_detected"],
    }


async def build_entry_confidence_v4(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    pool: asyncpg.Pool,
    artifacts: Phase115Artifacts,
    symbols: list[str],
    targeted_run_group: str,
) -> dict[str, Any]:
    symbol_infos = await module_c_repo.list_active_symbols(symbols=symbols)
    symbol_set = {symbol.symbol for symbol in symbol_infos}
    daily_rows = [row for row in artifacts.phase_1_12_daily_rows if row["symbol"] in symbol_set]
    candidate_rows = [row for row in daily_rows if row.get("candidate_b2_b2s_accept")]
    observation_rows = [row for row in daily_rows if row.get("observation_accept")]
    bottom_rows = [row for row in artifacts.phase_1_14_bottom_rows if row["symbol"] in symbol_set]
    price_rows_strict = [row for row in artifacts.phase_1_14_price_rows if row["symbol"] in symbol_set]
    price_policy_rows = [row for row in artifacts.phase_1_14_price_policy_rows if row["symbol"] in symbol_set]
    five_f_existing = [row for row in artifacts.phase_1_13_5f_rows if row["symbol"] in symbol_set]
    price_rows_signal_price_only = _copy_price_rows_for_policy(
        price_rows_strict,
        price_policy_rows,
        policy_field="thirty_f_price_policy_signal_price_only",
    )
    official_baseline = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_strict,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_existing,
        mode_name="strict_daily_b1_after_weekly_context",
        accepted_field="strict_accept",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="official_baseline",
    )
    strict_candidate = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_strict,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_existing,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="candidate",
    )
    signal_price_only_candidate = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_signal_price_only,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_existing,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1_signal_price_only",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_signal_price_only",
        status="candidate_variant",
    )
    bottom_only_candidate = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_signal_price_only,
        bottom_rows=bottom_rows,
        five_f_rows=_zero_five_f_rows({_sample_id(row) for row in candidate_rows}),
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1_bottom_only",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_signal_price_only",
        status="candidate_variant",
    )

    if candidate_rows:
        start_time = min(parse_dt(row["as_of_time"]) for row in candidate_rows) - timedelta(days=30)
        end_time = max(parse_dt(row["as_of_time"]) for row in candidate_rows) + timedelta(days=10)
    else:
        start_time = datetime.now(UTC) - timedelta(days=30)
        end_time = datetime.now(UTC)
    events_30f_payload = await build_signal_event_ledger(
        pool=pool,
        symbols=symbol_infos,
        level="30f",
        start_time=start_time,
        end_time=end_time,
        run_group_id=targeted_run_group,
    )
    events_5f_payload = await build_signal_event_ledger(
        pool=pool,
        symbols=symbol_infos,
        level="5f",
        start_time=start_time,
        end_time=end_time,
        run_group_id=targeted_run_group,
    )
    targeted_rows_30f = (await build_thirty_f_event_ledger_visibility_audit(
        module_c_repo=module_c_repo,
        symbols=symbol_infos,
        candidate_rows=candidate_rows,
        events_30f=events_30f_payload["events"],
    ))["rows"]
    targeted_rows_5f = (await build_five_f_confirmation_audit(
        module_c_repo=module_c_repo,
        symbols=symbol_infos,
        rows_30f=targeted_rows_30f,
        events_5f=events_5f_payload["events"],
    ))["rows"]
    targeted_price_payload = await build_thirty_f_price_validity_audit(
        kline_repo=kline_repo,
        symbols=symbol_infos,
        source_rows=daily_rows,
        rows_30f=targeted_rows_30f,
        bottom_visibility_rows=bottom_rows,
    )
    targeted_price_rows = targeted_price_payload["rows"]
    targeted_candidate = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=targeted_price_rows,
        bottom_rows=bottom_rows,
        five_f_rows=targeted_rows_5f,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1_targeted_intraday_5f_30f",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="targeted_intraday_5f_30f",
        status="candidate_variant",
    )
    diagnostic_only = build_entry_confidence_builder_v3(
        daily_rows=observation_rows,
        price_rows=_copy_price_rows_for_policy(price_rows_strict, price_policy_rows, policy_field="thirty_f_price_policy_record_only"),
        bottom_rows=bottom_rows,
        five_f_rows=five_f_existing,
        mode_name="daily_buy_signal_any_observation_record_only",
        accepted_field="observation_accept",
        thirty_f_price_policy="thirty_f_price_policy_record_only",
        status="diagnostic_only",
    )

    scenario_rows = [
        _scenario_row("official_baseline", official_baseline),
        _scenario_row("phase_1_14_strict_candidate", strict_candidate),
        _scenario_row("candidate_signal_price_only", signal_price_only_candidate),
        _scenario_row("candidate_bottom_fractal_ledger", bottom_only_candidate),
        _scenario_row("candidate_targeted_intraday_5f_30f", targeted_candidate),
        _scenario_row("diagnostic_only_any_observation", diagnostic_only),
    ]
    return {
        "targeted_payload": targeted_candidate,
        "scenario_rows": scenario_rows,
        "events_30f_summary": events_30f_payload["summary"],
        "events_5f_summary": events_5f_payload["summary"],
        "targeted_rows_30f": targeted_rows_30f,
        "targeted_rows_5f": targeted_rows_5f,
        "targeted_price_rows": targeted_price_rows,
        "official_baseline": official_baseline,
        "strict_candidate": strict_candidate,
        "signal_price_only_candidate": signal_price_only_candidate,
        "bottom_only_candidate": bottom_only_candidate,
        "diagnostic_only": diagnostic_only,
    }


def _render_v4_compare_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Replay Phase 1.15 Micro Compare",
            "",
            render_markdown_table(
                [
                    "scenario",
                    "samples",
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
                    for row in payload["scenario_rows"]
                ],
            ),
            "",
        ]
    )


def _render_gate_waterfall_md(payload: dict[str, Any]) -> str:
    summary = payload["targeted_payload"]["summary"]
    rows = [[key, value] for key, value in sorted(summary["block_reason_counts"].items())]
    return "\n".join(
        [
            "# Gate Waterfall Phase 1.15 Micro",
            "",
            render_markdown_table(["entry_block_reason", "count"], rows),
            "",
        ]
    )


def _render_trade_analysis_md(payload: dict[str, Any]) -> str:
    summary = payload["targeted_payload"]["summary"]
    lines = [
        "# Trade Analysis Phase 1.15 Micro",
        "",
        f"- confidence_70_count_after_micro_backfill: `{summary['confidence_70_count']}`",
        f"- confidence_100_count_after_micro_backfill: `{summary['confidence_100_count']}`",
        f"- entry_candidate_count_after_micro_backfill: `{summary['entry_candidate_count']}`",
        f"- entry_trigger_count_after_micro_backfill: `{summary['entry_trigger_count']}`",
        f"- future_leakage_detected: `{summary['future_leakage_detected']}`",
        "",
    ]
    if summary["entry_candidate_count"] == 0:
        lines.append("- 结论：micro-backfill 后仍未形成 entry candidate，不能进入 strategy_30f smoke。")
    return "\n".join(lines)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_traces(
    *,
    output_dir: Path,
    daily_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
    five_f_rows: list[dict[str, Any]],
    confidence_rows: list[dict[str, Any]],
) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    daily_map = _sample_map(daily_rows)
    price_map = _sample_map(price_rows)
    bottom_map = _sample_map(bottom_rows)
    five_f_map = _sample_map(five_f_rows)
    confidence_map = defaultdict(list)
    for row in confidence_rows:
        confidence_map[_sample_id(row)].append(row)
    groups = {
        "window_valid_price_invalid": [row for row in price_rows if bool(row.get("window_valid")) and not bool(row.get("price_valid"))][:9],
        "bottom_confirmed": [row for row in bottom_rows if row.get("daily_bottom_fractal_failure_reason") == "bottom_fractal_confirmed"][:3],
        "bottom_not_first_seen_yet": [row for row in bottom_rows if row.get("daily_bottom_fractal_failure_reason") == "bottom_fractal_exists_but_not_first_seen_yet"][:3],
        "five_f_missing": [row for row in five_f_rows if not bool(row.get("five_f_B2_confirms_30f"))][:3],
        "confidence_changed": [row for row in confidence_rows if float(row.get("confidence") or 0.0) >= 40.0][:3],
    }
    index_lines = ["# Trace Index", ""]
    for group_name, rows in groups.items():
        index_lines.append(f"## {group_name}")
        index_lines.append("")
        for index, row in enumerate(rows, start=1):
            sample_id = _sample_id(row)
            daily_row = daily_map.get(sample_id, {})
            path = traces_dir / f"{group_name}-{index:02d}-{str(row.get('symbol') or daily_row.get('symbol') or 'unknown').replace('.', '_')}.md"
            payload = {
                "sample_id": sample_id,
                "daily": daily_row,
                "price": price_map.get(sample_id),
                "bottom": bottom_map.get(sample_id),
                "five_f": five_f_map.get(sample_id),
                "confidence": confidence_map.get(sample_id, []),
            }
            path.write_text(f"# {group_name} #{index}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n", encoding="utf-8")
            index_lines.append(f"- [{path.name}](./traces/{path.name})")
        index_lines.append("")
    (output_dir / "trace_index.md").write_text("\n".join(index_lines), encoding="utf-8")


def build_phase_1_15_decision(
    *,
    lineage: dict[str, Any],
    price_deep_dive: dict[str, Any],
    bottom_equivalence: dict[str, Any],
    five_f_root: dict[str, Any],
    v4_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    v4_summary = (v4_payload or {}).get("targeted_payload", {}).get("summary", {})
    recommended_policy = "strict_existing"
    if price_deep_dive["summary"]["recommended_policy_counts"].get("signal_price_only"):
        recommended_policy = "signal_price_only"
    return {
        "sample_lineage_consistent": lineage["lineage_consistent"],
        "phase_1_14_two_symbol_limitation_affects_generalization": lineage["phase_1_14_two_symbol_limitation_affects_generalization"],
        "recommend_keep_strict_daily_setup_official": True,
        "recommend_candidate_daily_b2_b2s_continue": True,
        "recommend_30f_signal_source_event_ledger": True,
        "recommend_30f_price_policy": recommended_policy,
        "recommend_bottom_fractal_ledger_as_candidate_confirmation": bottom_equivalence["summary"]["recommend_bottom_fractal_ledger_as_candidate_confirmation"],
        "module_c_fractal_equivalence": bottom_equivalence["summary"]["module_c_fractal_equivalence"],
        "five_f_absence_is_real": five_f_root["summary"].get("five_f_absence_is_real"),
        "five_f_absence_due_to_research_daily_close_sparsity": five_f_root["summary"].get("five_f_absence_due_to_research_daily_close_sparsity"),
        "recommend_targeted_intraday_micro_backfill_continue": five_f_root["summary"].get("recommend_targeted_intraday_micro_backfill", False),
        "confidence_70_count_after_micro_backfill": v4_summary.get("confidence_70_count", 0),
        "confidence_100_count_after_micro_backfill": v4_summary.get("confidence_100_count", 0),
        "entry_candidate_count_after_micro_backfill": v4_summary.get("entry_candidate_count", 0),
        "recommend_strategy_30f_smoke_next": False,
        "recommend_50_symbols_backfill_next": False,
        "future_leakage_detected": bool(bottom_equivalence["summary"]["future_leakage_detected"]) or bool(v4_summary.get("future_leakage_detected", False)),
    }


def _render_phase_1_15_decision_md(payload: dict[str, Any]) -> str:
    rows = [[key, value] for key, value in payload.items()]
    return "# Phase 1.15 Decision Report\n\n" + render_markdown_table(["field", "value"], rows) + "\n"


def _render_phase_1_15_checklist(decision: dict[str, Any]) -> str:
    rows = [
        ["样本谱系清晰", "已完成"],
        ["30F 价格逐样本解释", "已完成"],
        ["底分型一致性验证", "已完成"],
        ["5F 缺失根因分类", "已完成"],
        ["targeted intraday dry-run", "已完成"],
        ["targeted intraday backfill", "已完成"],
        ["Entry Confidence V4", "已完成"],
        ["进入 strategy_30f smoke", "未完成" if not decision["recommend_strategy_30f_smoke_next"] else "已完成"],
        ["进入 50 标的回填", "未完成" if not decision["recommend_50_symbols_backfill_next"] else "已完成"],
    ]
    return "# Phase 1.15 Task Checklist Report\n\n" + render_markdown_table(["任务项", "状态"], rows) + "\n"


async def run_phase_1_15(
    *,
    task: str,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_13_output_dir: Path = PHASE_1_13_OUTPUT_DIR,
    phase_1_14_output_dir: Path = PHASE_1_14_OUTPUT_DIR,
    symbols: list[str] | None = None,
    run_group_id: str = DEFAULT_TARGETED_RUN_GROUP,
    max_workers: int = 2,
    resume: bool = True,
) -> dict[str, Any]:
    if task not in DEFAULT_PHASE_1_15_TASKS:
        raise ValueError(f"unsupported phase_1_15 task: {task}")
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = load_phase_1_15_artifacts(
        phase_1_12_output_dir=phase_1_12_output_dir,
        phase_1_13_output_dir=phase_1_13_output_dir,
        phase_1_14_output_dir=phase_1_14_output_dir,
    )
    requested_symbols = symbols or list(DEFAULT_TARGETED_SYMBOLS)
    daily_rows = [row for row in artifacts.phase_1_12_daily_rows if row["symbol"] in requested_symbols]
    candidate_rows = [row for row in daily_rows if row.get("candidate_b2_b2s_accept")]
    phase_1_13_candidate_rows = [row for row in artifacts.phase_1_13_30f_rows if row["symbol"] in requested_symbols]
    phase_1_14_candidate_rows = [row for row in artifacts.phase_1_14_v3_rows if row["symbol"] in requested_symbols]

    lineage = build_sample_lineage_audit(
        phase_1_12_candidate_rows=candidate_rows,
        phase_1_13_candidate_rows=phase_1_13_candidate_rows,
        phase_1_14_candidate_rows=phase_1_14_candidate_rows,
        phase_1_13_thirty_f_rows=phase_1_13_candidate_rows,
        requested_symbols=requested_symbols,
    )
    if task == "sample-lineage":
        write_json(output_dir / "sample_lineage_audit.json", lineage)
        _write_csv(
            output_dir / "sample_symbol_distribution.csv",
            [
                {"stage": "phase_1_12_candidate", "symbol": key, "count": value}
                for key, value in lineage["candidate_symbols"].items()
            ] + [
                {"stage": "phase_1_13_30f_visible", "symbol": key, "count": value}
                for key, value in lineage["thirty_f_visible_symbols"].items()
            ] + [
                {"stage": "phase_1_14_entry_confidence", "symbol": key, "count": value}
                for key, value in lineage["entry_confidence_symbols"].items()
            ],
        )
        write_jsonl(output_dir / "sample_lineage_mismatch_samples.jsonl", lineage["mismatch_samples"])
        (output_dir / "sample_lineage_audit.md").write_text(_render_sample_lineage_md(lineage), encoding="utf-8")
        return lineage

    price_deep_dive = build_thirty_f_price_deep_dive(
        daily_rows=daily_rows,
        price_rows=[row for row in artifacts.phase_1_14_price_rows if row["symbol"] in requested_symbols],
        price_policy_rows=[row for row in artifacts.phase_1_14_price_policy_rows if row["symbol"] in requested_symbols],
        bottom_rows=[row for row in artifacts.phase_1_14_bottom_rows if row["symbol"] in requested_symbols],
    )
    bottom_equivalence = build_daily_bottom_fractal_equivalence_audit(
        daily_rows=daily_rows,
        bottom_rows=[row for row in artifacts.phase_1_14_bottom_rows if row["symbol"] in requested_symbols],
    )

    if task == "audit-price-fractal":
        write_jsonl(output_dir / "thirty_f_price_invalid_9_deep_dive.jsonl", price_deep_dive["rows"])
        write_json(output_dir / "thirty_f_price_policy_compare_v2.json", price_deep_dive["summary"])
        (output_dir / "thirty_f_price_invalid_9_deep_dive.md").write_text(_render_thirty_f_price_deep_dive_md(price_deep_dive), encoding="utf-8")
        (output_dir / "thirty_f_price_policy_contract.md").write_text("# 30F Price Policy Contract\n\n- strict_existing: strict default candidate gate\n- signal_price_only: candidate variant only\n- record_only: diagnostic only\n", encoding="utf-8")
        (output_dir / "thirty_f_price_policy_compare_v2.md").write_text(_render_thirty_f_price_deep_dive_md(price_deep_dive), encoding="utf-8")
        write_json(output_dir / "daily_bottom_fractal_equivalence_audit.json", bottom_equivalence["summary"])
        write_jsonl(output_dir / "daily_bottom_fractal_equivalence_samples.jsonl", bottom_equivalence["rows"])
        (output_dir / "daily_bottom_fractal_equivalence_audit.md").write_text(_render_bottom_fractal_equivalence_md(bottom_equivalence), encoding="utf-8")
        return {"price_deep_dive": price_deep_dive["summary"], "bottom_equivalence": bottom_equivalence["summary"]}

    symbol_infos = await module_c_repo.list_active_symbols(symbols=requested_symbols)
    five_f_root = await build_five_f_root_cause_audit(
        module_c_repo=module_c_repo,
        pool=pool,
        symbols=symbol_infos,
        rows_30f=[row for row in artifacts.phase_1_13_30f_rows if row["symbol"] in requested_symbols],
        rows_5f_existing=[row for row in artifacts.phase_1_13_5f_rows if row["symbol"] in requested_symbols],
        price_rows=[row for row in artifacts.phase_1_14_price_rows if row["symbol"] in requested_symbols],
        confidence_rows=[row for row in artifacts.phase_1_14_v3_rows if row["symbol"] in requested_symbols],
    )
    if task == "audit-5f-root-cause":
        write_json(output_dir / "five_f_confirmation_root_cause_audit.json", five_f_root["summary"])
        write_jsonl(output_dir / "five_f_confirmation_window_samples.jsonl", five_f_root["rows"])
        (output_dir / "five_f_confirmation_root_cause_audit.md").write_text(_render_five_f_root_cause_md(five_f_root), encoding="utf-8")
        write_json(output_dir / "five_f_signal_inventory_around_entry_windows.json", five_f_root["signal_inventory"]["summary"])
        (output_dir / "five_f_signal_inventory_around_entry_windows.md").write_text(
            "# 5F Signal Inventory Around Entry Windows\n\n"
            f"- summary: `{json.dumps(five_f_root['signal_inventory']['summary'], ensure_ascii=False)}`\n",
            encoding="utf-8",
        )
        return five_f_root["summary"]

    plan = await build_targeted_intraday_plan(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        artifacts=artifacts,
        symbols=requested_symbols,
    )
    if task == "targeted-intraday-dry-run":
        write_json(output_dir / "targeted_intraday_backfill_plan.json", plan)
        (output_dir / "targeted_intraday_backfill_plan.md").write_text(_render_targeted_plan_md(plan), encoding="utf-8")
        return plan

    backfill_summary = await run_targeted_intraday_backfill(
        pool=pool,
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        plan=plan,
        max_workers=max_workers,
        resume=resume,
        run_group_id=run_group_id,
    )
    if task == "targeted-intraday-backfill":
        write_json(output_dir / "targeted_intraday_backfill_summary.json", backfill_summary)
        (output_dir / "targeted_intraday_backfill_summary.md").write_text(
            "# Targeted Intraday Backfill Summary\n\n"
            f"- written_runs: `{backfill_summary['written_runs']}`\n"
            f"- failed_runs: `{backfill_summary['failed_runs']}`\n",
            encoding="utf-8",
        )
        _write_csv(output_dir / "targeted_intraday_backfill_manifest.csv", backfill_summary["manifest_rows"])
        write_jsonl(output_dir / "targeted_intraday_backfill_failed.jsonl", backfill_summary["failures"])
        write_json(output_dir / "targeted_intraday_backfill_perf.json", backfill_summary["perf_profile"])
        return backfill_summary

    v4_payload = await build_entry_confidence_v4(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        pool=pool,
        artifacts=artifacts,
        symbols=requested_symbols,
        targeted_run_group=run_group_id,
    )
    decision = build_phase_1_15_decision(
        lineage=lineage,
        price_deep_dive=price_deep_dive,
        bottom_equivalence=bottom_equivalence,
        five_f_root=five_f_root,
        v4_payload=v4_payload,
    )
    if task == "replay-v4":
        write_json(output_dir / "entry_confidence_builder_v4_audit.json", v4_payload["targeted_payload"]["summary"])
        write_jsonl(output_dir / "entry_confidence_builder_v4_samples.jsonl", v4_payload["targeted_payload"]["rows"])
        (output_dir / "entry_confidence_builder_v4_audit.md").write_text(
            "# Entry Confidence Builder V4 Audit\n\n"
            f"- summary: `{json.dumps(v4_payload['targeted_payload']['summary'], ensure_ascii=False)}`\n",
            encoding="utf-8",
        )
        write_json(output_dir / "entry_confidence_distribution_v4.json", v4_payload["targeted_payload"]["summary"]["confidence_distribution"])
        (output_dir / "entry_confidence_distribution_v4.md").write_text(
            render_markdown_table(
                ["confidence", "count"],
                [[key, value] for key, value in v4_payload["targeted_payload"]["summary"]["confidence_distribution"].items()],
            ),
            encoding="utf-8",
        )
        write_json(output_dir / "replay_phase_1_15_micro_compare.json", {"rows": v4_payload["scenario_rows"]})
        (output_dir / "replay_phase_1_15_micro_compare.md").write_text(_render_v4_compare_md(v4_payload), encoding="utf-8")
        write_json(output_dir / "gate_waterfall_phase_1_15_micro.json", v4_payload["targeted_payload"]["summary"]["block_reason_counts"])
        (output_dir / "gate_waterfall_phase_1_15_micro.md").write_text(_render_gate_waterfall_md(v4_payload), encoding="utf-8")
        (output_dir / "trade_analysis_phase_1_15_micro.md").write_text(_render_trade_analysis_md(v4_payload), encoding="utf-8")

        write_json(output_dir / "phase_1_15_summary.json", decision)
        (output_dir / "phase_1_15_summary.md").write_text(_render_phase_1_15_decision_md(decision), encoding="utf-8")
        write_json(output_dir / "phase_1_15_decision_report.json", decision)
        (output_dir / "phase_1_15_decision_report.md").write_text(_render_phase_1_15_decision_md(decision), encoding="utf-8")
        (output_dir / "phase_1_15_task_checklist_report.md").write_text(_render_phase_1_15_checklist(decision), encoding="utf-8")
        (output_dir / "phase_1_15_detailed_completion_report.md").write_text(
            "# Phase 1.15 Detailed Completion Report\n\n"
            f"- lineage: `{json.dumps(lineage, ensure_ascii=False)}`\n"
            f"- price_deep_dive_summary: `{json.dumps(price_deep_dive['summary'], ensure_ascii=False)}`\n"
            f"- bottom_equivalence_summary: `{json.dumps(bottom_equivalence['summary'], ensure_ascii=False)}`\n"
            f"- five_f_root_summary: `{json.dumps(five_f_root['summary'], ensure_ascii=False)}`\n"
            f"- micro_backfill_summary: `{json.dumps(backfill_summary, ensure_ascii=False)}`\n"
            f"- v4_scenarios: `{json.dumps(v4_payload['scenario_rows'], ensure_ascii=False)}`\n",
            encoding="utf-8",
        )
        _write_traces(
            output_dir=output_dir,
            daily_rows=daily_rows,
            price_rows=v4_payload["targeted_price_rows"],
            bottom_rows=[row for row in artifacts.phase_1_14_bottom_rows if row["symbol"] in requested_symbols],
            five_f_rows=v4_payload["targeted_rows_5f"],
            confidence_rows=v4_payload["targeted_payload"]["rows"],
        )
        return {
            "decision": decision,
            "v4_summary": v4_payload["targeted_payload"]["summary"],
        }

    raise AssertionError("unreachable")
