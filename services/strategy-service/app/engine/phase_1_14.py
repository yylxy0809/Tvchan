from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.domain.models import SymbolInfo
from app.engine.phase_1_11 import parse_dt, read_jsonl, render_markdown_table, write_json, write_jsonl
from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_13 import DEFAULT_OUTPUT_DIR as PHASE_1_13_OUTPUT_DIR
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS
from app.repositories.kline_repo import KlineBar, KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-14-entry-confidence-v3"
WINDOW_DAYS = (5, 10, 20)
PRICE_POLICIES = (
    "thirty_f_price_policy_strict_existing",
    "thirty_f_price_policy_signal_price_only",
    "thirty_f_price_policy_bar_low_high_overlap",
    "thirty_f_price_policy_no_break_daily_b1",
    "thirty_f_price_policy_record_only",
)


@dataclass(slots=True)
class Phase114Artifacts:
    phase_1_12_daily_rows: list[dict[str, Any]]
    phase_1_12_replay_compare: dict[str, Any]
    phase_1_13_summary: dict[str, Any]
    phase_1_13_30f_rows: list[dict[str, Any]]
    phase_1_13_30f_summary: dict[str, Any]
    phase_1_13_5f_rows: list[dict[str, Any]]
    phase_1_13_5f_summary: dict[str, Any]
    phase_1_13_confidence_v2_rows: list[dict[str, Any]]
    phase_1_13_confidence_v2_summary: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any] | list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sample_id(row: dict[str, Any]) -> str:
    return f"{row['symbol']}|{row['as_of_time']}"


def _signal_first_seen(signal_payload: dict[str, Any] | None) -> datetime | None:
    if signal_payload is None:
        return None
    features = signal_payload.get("features") or {}
    return parse_dt(features.get("first_seen_time")) or parse_dt(signal_payload.get("point_time"))


def _signal_point_time(signal_payload: dict[str, Any] | None) -> datetime | None:
    if signal_payload is None:
        return None
    return parse_dt(signal_payload.get("point_time"))


def _price_x1000(value: float | str | int | None) -> int | None:
    if value is None:
        return None
    return int(round(float(value) * 1000))


def _event_payload(event: dict[str, Any] | None) -> dict[str, Any] | None:
    if event is None:
        return None
    return {
        "event_id": event.get("event_id"),
        "point_time": event.get("point_time"),
        "first_seen_time": event.get("first_seen_time"),
        "price_x1000": event.get("price_x1000"),
        "middle_low_x1000": event.get("middle_low_x1000"),
        "signal_point_time": event.get("signal_point_time"),
        "bsp_type": event.get("bsp_type"),
        "signal_fingerprint": event.get("signal_fingerprint"),
    }


def _find_bar_by_time(bars: list[KlineBar], ts: datetime | None) -> KlineBar | None:
    if ts is None:
        return None
    for bar in bars:
        if bar.ts == ts:
            return bar
    return None


def _future_leakage_detected(rows: list[dict[str, Any]], *, first_seen_key: str, as_of_key: str) -> bool:
    for row in rows:
        first_seen = parse_dt(row.get(first_seen_key))
        as_of_time = parse_dt(row.get(as_of_key))
        if first_seen is not None and as_of_time is not None and first_seen > as_of_time:
            return True
    return False


def load_phase_1_14_artifacts(
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_13_output_dir: Path = PHASE_1_13_OUTPUT_DIR,
) -> Phase114Artifacts:
    return Phase114Artifacts(
        phase_1_12_daily_rows=read_jsonl(phase_1_12_output_dir / "daily_setup_sample_audit_v3.jsonl"),
        phase_1_12_replay_compare=dict(_read_json(phase_1_12_output_dir / "replay_phase_1_12_compare.json")),
        phase_1_13_summary=dict(_read_json(phase_1_13_output_dir / "phase_1_13_summary.json")),
        phase_1_13_30f_rows=read_jsonl(phase_1_13_output_dir / "thirty_f_event_ledger_visibility_samples.jsonl"),
        phase_1_13_30f_summary=dict(_read_json(phase_1_13_output_dir / "thirty_f_event_ledger_visibility_audit.json")),
        phase_1_13_5f_rows=read_jsonl(phase_1_13_output_dir / "five_f_confirmation_samples.jsonl"),
        phase_1_13_5f_summary=dict(_read_json(phase_1_13_output_dir / "five_f_confirmation_audit.json")),
        phase_1_13_confidence_v2_rows=read_jsonl(phase_1_13_output_dir / "entry_confidence_builder_v2_samples.jsonl"),
        phase_1_13_confidence_v2_summary=dict(_read_json(phase_1_13_output_dir / "entry_confidence_builder_v2_audit.json")),
    )


def _build_bottom_fractal_events(symbol: str, bars: list[KlineBar]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for index in range(1, len(bars) - 1):
        left, middle, right = bars[index - 1], bars[index], bars[index + 1]
        if middle.low < left.low and middle.low < right.low:
            price_x1000 = _price_x1000(middle.low)
            events.append(
                {
                    "event_id": f"{symbol}|1d|bottom|{middle.ts.isoformat()}|{price_x1000}",
                    "symbol": symbol,
                    "level": "1d",
                    "source": "raw_kline_derived",
                    "fractal_type": "bottom",
                    "point_time": middle.ts.isoformat(),
                    "first_seen_time": right.ts.isoformat(),
                    "left_bar_time": left.ts.isoformat(),
                    "middle_bar_time": middle.ts.isoformat(),
                    "right_bar_time": right.ts.isoformat(),
                    "price_x1000": price_x1000,
                    "middle_low_x1000": price_x1000,
                    "left_low_x1000": _price_x1000(left.low),
                    "right_low_x1000": _price_x1000(right.low),
                    "confirmed": True,
                    "future_leakage_flag": False,
                }
            )
    return events


def _visible_bottom_events(
    events: list[dict[str, Any]],
    *,
    as_of_time: datetime,
    after_time: datetime | None,
) -> list[dict[str, Any]]:
    visible: list[dict[str, Any]] = []
    for event in events:
        point_time = parse_dt(event["point_time"])
        first_seen = parse_dt(event["first_seen_time"])
        if point_time is None or first_seen is None:
            continue
        if point_time > as_of_time:
            continue
        if first_seen > as_of_time:
            continue
        if after_time is not None and first_seen <= after_time:
            continue
        visible.append(event)
    return visible


def _future_bottom_events(
    events: list[dict[str, Any]],
    *,
    as_of_time: datetime,
    after_time: datetime | None,
) -> list[dict[str, Any]]:
    future: list[dict[str, Any]] = []
    for event in events:
        first_seen = parse_dt(event["first_seen_time"])
        if first_seen is None:
            continue
        if after_time is not None and first_seen <= after_time:
            continue
        if first_seen > as_of_time:
            future.append(event)
    return future


def _latest_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    return events[-1] if events else None


def _window_flags(day_order: list[str], left: datetime | None, right: datetime | None) -> dict[str, bool]:
    ordered = [parse_dt(item) for item in day_order if parse_dt(item) is not None]
    if left is None or right is None or not ordered:
        return {str(days): False for days in WINDOW_DAYS}
    left_index = None
    right_index = None
    for index, ts in enumerate(ordered):
        if ts <= left:
            left_index = index
        if ts <= right:
            right_index = index
    if left_index is None or right_index is None:
        return {str(days): False for days in WINDOW_DAYS}
    distance = abs(left_index - right_index)
    return {str(days): distance <= days for days in WINDOW_DAYS}


def _strict_existing_price_valid(thirty_f_price_x1000: int | None, daily_setup_price_x1000: int | None) -> bool:
    return (
        thirty_f_price_x1000 is not None
        and daily_setup_price_x1000 is not None
        and thirty_f_price_x1000 > daily_setup_price_x1000
    )


def _price_policy_result(
    *,
    policy: str,
    window_valid: bool,
    thirty_f_price_x1000: int | None,
    daily_setup_price_x1000: int | None,
    daily_b1_price_x1000: int | None,
    signal_bar: KlineBar | None,
) -> bool:
    if not window_valid:
        return False
    if policy == "thirty_f_price_policy_record_only":
        return True
    if policy == "thirty_f_price_policy_strict_existing":
        return _strict_existing_price_valid(thirty_f_price_x1000, daily_setup_price_x1000)
    if policy == "thirty_f_price_policy_signal_price_only":
        return (
            thirty_f_price_x1000 is not None
            and daily_setup_price_x1000 is not None
            and thirty_f_price_x1000 >= daily_setup_price_x1000
        )
    if policy == "thirty_f_price_policy_bar_low_high_overlap":
        if signal_bar is None or daily_setup_price_x1000 is None:
            return False
        return _price_x1000(signal_bar.low) <= daily_setup_price_x1000 <= _price_x1000(signal_bar.high)
    if policy == "thirty_f_price_policy_no_break_daily_b1":
        return (
            thirty_f_price_x1000 is not None
            and daily_b1_price_x1000 is not None
            and thirty_f_price_x1000 > daily_b1_price_x1000
        )
    return False


def _strict_price_invalid_reason(
    *,
    has_visible_30f_b1: bool,
    window_valid: bool,
    thirty_f_price_x1000: int | None,
    daily_setup_price_x1000: int | None,
    daily_b1_price_x1000: int | None,
    signal_bar: KlineBar | None,
    future_signal_only: bool,
) -> str:
    if not has_visible_30f_b1:
        return "future_30f_signal_only" if future_signal_only else "signal_point_time_bar_not_found"
    if not window_valid:
        return "stale_30f_signal_after_daily_setup"
    if signal_bar is None:
        return "signal_point_time_bar_not_found"
    if thirty_f_price_x1000 is None:
        return "price_rule_not_explicit_enough"
    if not (_price_x1000(signal_bar.low) <= thirty_f_price_x1000 <= _price_x1000(signal_bar.high)):
        return "signal_price_vs_bar_price_mismatch"
    if daily_setup_price_x1000 is None:
        return "daily_b2_reference_missing"
    if daily_b1_price_x1000 is None:
        return "daily_b1_reference_missing"
    if thirty_f_price_x1000 <= daily_b1_price_x1000:
        return "thirty_f_price_below_daily_b1_fail"
    if thirty_f_price_x1000 <= daily_setup_price_x1000:
        return "thirty_f_signal_already_invalidated"
    return "price_rule_unknown"


async def build_daily_bottom_fractal_event_ledger(
    *,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    rows_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    per_symbol = Counter()
    for symbol in symbols:
        symbol_rows = rows_by_symbol.get(symbol.symbol, [])
        if not symbol_rows:
            continue
        max_as_of = max(parse_dt(row["as_of_time"]) for row in symbol_rows)
        await kline_repo.prime_symbol_cache(
            symbol.symbol_id,
            start_time=max_as_of,
            end_time=max_as_of,
            timeframes=("1d",),
        )
        try:
            bars = await kline_repo.get_klines(symbol.symbol_id, "1d", end=max_as_of)
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)
        symbol_events = _build_bottom_fractal_events(symbol.symbol, bars)
        per_symbol[symbol.symbol] = len(symbol_events)
        events.extend(symbol_events)
    summary = {
        "symbol_count": len(symbols),
        "event_count": len(events),
        "per_symbol_event_count": dict(sorted(per_symbol.items())),
        "future_leakage_detected": any(
            parse_dt(event["first_seen_time"]) <= parse_dt(event["point_time"])
            for event in events
        ),
        "source": "raw_kline_derived",
        "module_c_fractal_equivalence": "not_proven",
    }
    return {"events": events, "summary": summary}


def build_daily_bottom_fractal_visibility_audit(
    *,
    daily_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
    bottom_events: list[dict[str, Any]],
) -> dict[str, Any]:
    rows_30f_map = {row["sample_id"]: row for row in rows_30f}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in bottom_events:
        events_by_symbol[event["symbol"]].append(event)

    rows: list[dict[str, Any]] = []
    category_counts = Counter()
    for row in daily_rows:
        sample_id = _sample_id(row)
        audit_30f = rows_30f_map.get(sample_id)
        if audit_30f is None or audit_30f["visible_30f_B1_or_1p_count"] <= 0:
            continue
        as_of_time = parse_dt(row["as_of_time"])
        setup_signal = row["candidate_audit"]["selected_daily_b2_or_b2s"] or row["candidate_audit"]["selected_buy_signal_any"]
        setup_first_seen = _signal_first_seen(setup_signal)
        symbol_events = events_by_symbol[row["symbol"]]
        prior = _latest_event(
            [event for event in symbol_events if parse_dt(event["first_seen_time"]) <= (setup_first_seen or as_of_time)]
        )
        visible = _visible_bottom_events(symbol_events, as_of_time=as_of_time, after_time=setup_first_seen)
        future = _future_bottom_events(symbol_events, as_of_time=as_of_time, after_time=setup_first_seen)
        confirmed = _latest_event(visible)
        future_event = future[0] if future else None
        if confirmed is not None:
            category = "bottom_fractal_confirmed"
        elif future_event is not None:
            category = "bottom_fractal_exists_but_not_first_seen_yet"
        else:
            category = "bottom_fractal_not_found"
        category_counts[category] += 1
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "name": row["name"],
                "as_of_time": row["as_of_time"],
                "daily_setup_first_seen_time": setup_first_seen.isoformat() if setup_first_seen else None,
                "daily_bottom_fractal_visible": confirmed is not None,
                "daily_bottom_fractal_time": confirmed["point_time"] if confirmed else None,
                "daily_bottom_fractal_first_seen_time": confirmed["first_seen_time"] if confirmed else None,
                "daily_bottom_fractal_price_x1000": confirmed["price_x1000"] if confirmed else None,
                "daily_bottom_fractal_within_window": confirmed is not None,
                "daily_bottom_fractal_after_daily_setup": confirmed is not None,
                "daily_bottom_fractal_before_entry_eval": confirmed is not None,
                "daily_bottom_fractal_failure_reason": category,
                "prior_bottom_fractal": _event_payload(prior),
                "same_window_bottom_fractal": _event_payload(confirmed),
                "future_bottom_fractal": _event_payload(future_event),
                "source": "raw_kline_derived",
                "future_leakage_flag": False,
            }
        )

    summary = {
        "sample_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "source": "raw_kline_derived",
        "recommend_as_candidate_source": bool(category_counts.get("bottom_fractal_confirmed", 0)),
        "future_leakage_detected": _future_leakage_detected(
            [row for row in rows if row["daily_bottom_fractal_visible"]],
            first_seen_key="daily_bottom_fractal_first_seen_time",
            as_of_key="as_of_time",
        ),
    }
    return {"rows": rows, "summary": summary}


async def build_thirty_f_price_validity_audit(
    *,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    source_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
    bottom_visibility_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    rows_30f_map = {row["sample_id"]: row for row in rows_30f}
    bottom_map = {row["sample_id"]: row for row in bottom_visibility_rows}
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_rows:
        rows_by_symbol[row["symbol"]].append(row)

    samples: list[dict[str, Any]] = []
    policy_samples: list[dict[str, Any]] = []
    reason_counts = Counter()

    for symbol_code, symbol_rows in rows_by_symbol.items():
        symbol = symbol_map[symbol_code]
        min_as_of = min(parse_dt(row["as_of_time"]) for row in symbol_rows)
        max_as_of = max(parse_dt(row["as_of_time"]) for row in symbol_rows)
        await kline_repo.prime_symbol_cache(
            symbol.symbol_id,
            start_time=min_as_of,
            end_time=max_as_of,
            timeframes=("30f",),
        )
        try:
            bars_30f = await kline_repo.get_klines(symbol.symbol_id, "30f", start=min_as_of, end=max_as_of)
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)

        for row in symbol_rows:
            sample_id = _sample_id(row)
            audit_30f = rows_30f_map.get(sample_id)
            if audit_30f is None:
                continue
            setup_signal = row["candidate_audit"]["selected_daily_b2_or_b2s"] or row["candidate_audit"]["selected_buy_signal_any"]
            daily_b1_signal = row["candidate_audit"]["selected_daily_b1"]
            bottom_row = bottom_map.get(sample_id)
            thirty_f_event = audit_30f["latest_30f_B1_after_daily_setup"] or audit_30f["nearest_30f_B1_before_as_of"]

            setup_first_seen = _signal_first_seen(setup_signal)
            setup_point_time = _signal_point_time(setup_signal)
            signal_point_time = parse_dt(thirty_f_event["signal_point_time"]) if thirty_f_event else None
            signal_bar = _find_bar_by_time(bars_30f, signal_point_time)

            daily_setup_price_x1000 = _price_x1000(setup_signal["price"]) if setup_signal else None
            daily_b1_price_x1000 = _price_x1000(daily_b1_signal["price"]) if daily_b1_signal else None
            thirty_f_price_x1000 = _price_x1000(thirty_f_event["price"]) if thirty_f_event else None
            bottom_price_x1000 = bottom_row["daily_bottom_fractal_price_x1000"] if bottom_row else None
            future_signal_only = audit_30f["thirty_f_failure_reason"] == "future_30f_signal_only"
            strict_valid = _price_policy_result(
                policy="thirty_f_price_policy_strict_existing",
                window_valid=audit_30f["thirty_f_window_valid"],
                thirty_f_price_x1000=thirty_f_price_x1000,
                daily_setup_price_x1000=daily_setup_price_x1000,
                daily_b1_price_x1000=daily_b1_price_x1000,
                signal_bar=signal_bar,
            )
            invalid_reason = None if strict_valid else _strict_price_invalid_reason(
                has_visible_30f_b1=audit_30f["visible_30f_B1_or_1p_count"] > 0,
                window_valid=audit_30f["thirty_f_window_valid"],
                thirty_f_price_x1000=thirty_f_price_x1000,
                daily_setup_price_x1000=daily_setup_price_x1000,
                daily_b1_price_x1000=daily_b1_price_x1000,
                signal_bar=signal_bar,
                future_signal_only=future_signal_only,
            )
            if invalid_reason is not None:
                reason_counts[invalid_reason] += 1

            sample = {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "name": row["name"],
                "as_of_time": row["as_of_time"],
                "daily_setup_event_id": f"{row['symbol']}|1d|{row['candidate_audit'].get('selected_signal_kind') or 'na'}|{setup_point_time.isoformat() if setup_point_time else 'na'}|{daily_setup_price_x1000}",
                "daily_setup_bsp_type": row["candidate_audit"].get("selected_signal_kind"),
                "daily_setup_first_seen_time": setup_first_seen.isoformat() if setup_first_seen else None,
                "daily_setup_point_time": setup_point_time.isoformat() if setup_point_time else None,
                "daily_setup_price_x1000": daily_setup_price_x1000,
                "thirty_f_event_id": thirty_f_event["signal_fingerprint"] if thirty_f_event else None,
                "thirty_f_bsp_type": thirty_f_event["bsp_type"] if thirty_f_event else None,
                "thirty_f_first_seen_time": thirty_f_event["first_seen_time"] if thirty_f_event else None,
                "thirty_f_point_time": thirty_f_event["signal_point_time"] if thirty_f_event else None,
                "thirty_f_price_x1000": thirty_f_price_x1000,
                "entry_window_start": setup_first_seen.isoformat() if setup_first_seen else None,
                "entry_window_end": row["as_of_time"],
                "window_valid": bool(audit_30f["thirty_f_window_valid"]),
                "window_flags": audit_30f.get("window_flags") or _window_flags(row.get("day_order") or [], setup_first_seen, parse_dt(row["as_of_time"])),
                "price_reference_policy": "thirty_f_price_policy_strict_existing",
                "price_valid": strict_valid,
                "price_invalid_reason": invalid_reason,
                "daily_b1_price_x1000": daily_b1_price_x1000,
                "daily_b2_price_x1000": daily_setup_price_x1000,
                "daily_bottom_fractal_price_x1000": bottom_price_x1000,
                "thirty_f_signal_bar_low_x1000": _price_x1000(signal_bar.low) if signal_bar else None,
                "thirty_f_signal_bar_high_x1000": _price_x1000(signal_bar.high) if signal_bar else None,
                "selected_run_underestimate": bool(audit_30f["selected_run_underestimates_30f"]),
                "future_leakage_flag": False,
            }
            samples.append(sample)

            policy_row = {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "as_of_time": row["as_of_time"],
                "window_valid": sample["window_valid"],
            }
            for policy in PRICE_POLICIES:
                policy_row[policy] = _price_policy_result(
                    policy=policy,
                    window_valid=sample["window_valid"],
                    thirty_f_price_x1000=thirty_f_price_x1000,
                    daily_setup_price_x1000=daily_setup_price_x1000,
                    daily_b1_price_x1000=daily_b1_price_x1000,
                    signal_bar=signal_bar,
                )
            policy_samples.append(policy_row)

    summary = {
        "sample_count": len(samples),
        "window_valid_samples": sum(1 for row in samples if row["window_valid"]),
        "window_valid_and_price_valid_samples": sum(1 for row in samples if row["window_valid"] and row["price_valid"]),
        "price_invalid_reason_counts": dict(sorted(reason_counts.items())),
    }
    return {
        "rows": samples,
        "summary": summary,
        "window_valid_rows": [row for row in samples if row["window_valid"]],
        "policy_rows": policy_samples,
    }


def build_thirty_f_price_policy_compare(price_policy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    window_valid_rows = [row for row in price_policy_rows if row["window_valid"]]
    markdown_rows: list[list[Any]] = []
    policy_pass_counts: dict[str, dict[str, int]] = {}
    for policy in PRICE_POLICIES:
        visible_scope = sum(1 for row in price_policy_rows if row[policy])
        window_scope = sum(1 for row in window_valid_rows if row[policy])
        policy_pass_counts[policy] = {
            "visible_b1_scope_pass_count": visible_scope,
            "window_valid_scope_pass_count": window_scope,
        }
        markdown_rows.append([policy, visible_scope, window_scope])
    return {
        "summary": {
            "visible_b1_scope_sample_count": len(price_policy_rows),
            "window_valid_scope_sample_count": len(window_valid_rows),
            "policy_pass_counts": policy_pass_counts,
        },
        "rows": price_policy_rows,
        "markdown_rows": markdown_rows,
    }


def _select_candidate_price_policy(compare_payload: dict[str, Any]) -> str | None:
    strict = compare_payload["summary"]["policy_pass_counts"]["thirty_f_price_policy_strict_existing"]
    strict_score = (strict["window_valid_scope_pass_count"], strict["visible_b1_scope_pass_count"])
    best_policy = None
    best_score = strict_score
    for policy, counts in compare_payload["summary"]["policy_pass_counts"].items():
        if policy in {"thirty_f_price_policy_strict_existing", "thirty_f_price_policy_record_only"}:
            continue
        score = (counts["window_valid_scope_pass_count"], counts["visible_b1_scope_pass_count"])
        if score > best_score:
            best_policy = policy
            best_score = score
    return best_policy


def _build_entry_block_reason(
    *,
    window_valid: bool,
    price_valid: bool,
    bottom_confirmed: bool,
    bottom_not_first_seen_yet: bool,
    five_f_buy_visible: bool,
    five_f_b2_visible: bool,
    five_f_confirmed: bool,
    confidence: float,
    entry_candidate: bool,
) -> str:
    if not window_valid:
        return "thirty_f_window_invalid"
    if not price_valid and bottom_confirmed and not five_f_confirmed:
        return "missing_30f_price_validity_only"
    if not price_valid:
        return "thirty_f_price_invalid"
    if bottom_not_first_seen_yet:
        return "bottom_fractal_not_first_seen_yet"
    if five_f_confirmed and not bottom_confirmed:
        return "missing_bottom_fractal_only"
    if bottom_confirmed and not five_f_confirmed:
        return "missing_5f_only"
    if not bottom_confirmed and not five_f_confirmed and price_valid:
        return "only_30f_confirmation"
    if not five_f_buy_visible:
        return "five_f_not_visible"
    if five_f_buy_visible and not five_f_b2_visible:
        return "five_f_visible_but_not_b2"
    if entry_candidate:
        return "candidate_entry_ready"
    if confidence < 70.0:
        return "confidence_below_70"
    return "unknown"


def build_entry_confidence_builder_v3(
    *,
    daily_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
    five_f_rows: list[dict[str, Any]],
    mode_name: str,
    accepted_field: str,
    thirty_f_price_policy: str,
    status: str,
) -> dict[str, Any]:
    price_map = {row["sample_id"]: row for row in price_rows}
    bottom_map = {row["sample_id"]: row for row in bottom_rows}
    five_f_map = {row["sample_id"]: row for row in five_f_rows}
    rows: list[dict[str, Any]] = []
    distribution = Counter()
    reason_counts = Counter()

    for row in daily_rows:
        if not row[accepted_field]:
            continue
        sample_id = _sample_id(row)
        price_row = price_map.get(sample_id)
        if price_row is None:
            continue
        bottom_row = bottom_map.get(sample_id)
        five_f_row = five_f_map.get(sample_id)
        has_30f_window_valid = bool(price_row["window_valid"])
        has_30f_price_valid = bool(price_row.get("price_policy_result", price_row["price_valid"]))
        has_30f_confirmation = has_30f_window_valid and has_30f_price_valid
        has_bottom_confirmation = bool(bottom_row and bottom_row["daily_bottom_fractal_visible"])
        bottom_not_first_seen_yet = bool(
            bottom_row and bottom_row["daily_bottom_fractal_failure_reason"] == "bottom_fractal_exists_but_not_first_seen_yet"
        )
        five_f_confirmed = bool(five_f_row and five_f_row["five_f_B2_confirms_30f"])
        five_f_buy_visible = bool(five_f_row and five_f_row["five_f_buy_any_visible"])
        five_f_b2_visible = bool(five_f_row and five_f_row["five_f_B2_or_2s_visible"])

        confirmation_count = int(has_30f_confirmation) + int(has_bottom_confirmation) + int(five_f_confirmed)
        confidence = 40.0 * int(confirmation_count >= 1) + 30.0 * int(confirmation_count >= 2) + 30.0 * int(confirmation_count >= 3)
        entry_candidate = confidence >= 70.0
        entry_triggered = entry_candidate and has_30f_confirmation and not bottom_not_first_seen_yet
        block_reason = _build_entry_block_reason(
            window_valid=has_30f_window_valid,
            price_valid=has_30f_price_valid,
            bottom_confirmed=has_bottom_confirmation,
            bottom_not_first_seen_yet=bottom_not_first_seen_yet,
            five_f_buy_visible=five_f_buy_visible,
            five_f_b2_visible=five_f_b2_visible,
            five_f_confirmed=five_f_confirmed,
            confidence=confidence,
            entry_candidate=entry_candidate and not entry_triggered,
        )

        rows.append(
            {
                "sample_id": sample_id,
                "symbol": row["symbol"],
                "name": row["name"],
                "as_of_time": row["as_of_time"],
                "daily_setup_mode": mode_name,
                "status": status,
                "daily_signal_source": "event_ledger",
                "thirty_f_signal_source": "event_ledger",
                "bottom_fractal_source": "kline_derived_event_ledger",
                "thirty_f_price_policy": thirty_f_price_policy,
                "has_30f_confirmation": has_30f_confirmation,
                "has_30f_window_valid": has_30f_window_valid,
                "has_30f_price_valid": has_30f_price_valid,
                "has_daily_bottom_fractal_confirmation": has_bottom_confirmation,
                "has_5f_confirmation": five_f_confirmed,
                "confidence": confidence,
                "entry_candidate": entry_candidate,
                "entry_triggered": entry_triggered,
                "entry_block_reason": "candidate_entry_ready" if entry_triggered else block_reason,
                "future_leakage_flag": False,
            }
        )
        distribution[str(int(confidence))] += 1
        reason_counts["candidate_entry_ready" if entry_triggered else block_reason] += 1

    summary = {
        "sample_count": len(rows),
        "confidence_40_count": sum(1 for row in rows if row["confidence"] >= 40.0),
        "confidence_70_count": sum(1 for row in rows if row["confidence"] >= 70.0),
        "confidence_100_count": sum(1 for row in rows if row["confidence"] >= 100.0),
        "entry_candidate_count": sum(1 for row in rows if row["entry_candidate"]),
        "entry_trigger_count": sum(1 for row in rows if row["entry_triggered"]),
        "block_reason_counts": dict(sorted(reason_counts.items())),
        "confidence_distribution": dict(sorted(distribution.items())),
        "future_leakage_detected": False,
    }
    return {"rows": rows, "summary": summary}


def build_replay_phase_1_14_compare(
    *,
    phase_1_12_replay_compare: dict[str, Any],
    daily_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
    five_f_rows: list[dict[str, Any]],
    candidate_v3: dict[str, Any],
    candidate_variant_v3: dict[str, Any] | None,
    diagnostic_v3: dict[str, Any],
) -> dict[str, Any]:
    phase_1_12_rows = {(row["daily_signal_source"], row["daily_setup_mode"]): row for row in phase_1_12_replay_compare["rows"]}
    official_reference = dict(phase_1_12_rows[("selected_run", "strict_daily_b1_after_weekly_context")])
    rows_30f_map = {row["sample_id"]: row for row in rows_30f}
    price_map = {row["sample_id"]: row for row in price_rows}
    bottom_map = {row["sample_id"]: row for row in bottom_rows}
    five_f_map = {row["sample_id"]: row for row in five_f_rows}

    def _compare_row(*, mode_name: str, status: str, accepted_field: str, payload: dict[str, Any]) -> dict[str, Any]:
        accepted = [row for row in daily_rows if row[accepted_field]]
        visible_30f_b1_count = 0
        window_valid_count = 0
        price_valid_count = 0
        bottom_confirmed_count = 0
        five_f_confirm_count = 0
        for row in accepted:
            sample_id = _sample_id(row)
            row_30f = rows_30f_map.get(sample_id)
            row_price = price_map.get(sample_id)
            row_bottom = bottom_map.get(sample_id)
            row_5f = five_f_map.get(sample_id)
            visible_30f_b1_count += int(bool(row_30f and row_30f["visible_30f_B1_or_1p_count"] > 0))
            window_valid_count += int(bool(row_price and row_price["window_valid"]))
            price_valid_count += int(bool(row_price and row_price.get("price_policy_result", row_price["price_valid"])))
            bottom_confirmed_count += int(bool(row_bottom and row_bottom["daily_bottom_fractal_visible"]))
            five_f_confirm_count += int(bool(row_5f and row_5f["five_f_B2_confirms_30f"]))

        summary = payload["summary"]
        return {
            "daily_signal_source": "selected_run" if status == "official" else "event_ledger",
            "daily_setup_mode": mode_name,
            "mode_class": status,
            "weekly_context_count": official_reference["weekly_context_count"],
            "daily_setup_count": len(accepted),
            "visible_30f_b1_count": visible_30f_b1_count,
            "30f_window_valid_count": window_valid_count,
            "30f_price_valid_count": price_valid_count,
            "bottom_fractal_confirmed_count": bottom_confirmed_count,
            "five_f_confirm_count": five_f_confirm_count,
            "confidence_40_count": summary["confidence_40_count"],
            "confidence_70_count": summary["confidence_70_count"],
            "confidence_100_count": summary["confidence_100_count"],
            "entry_candidate_count": summary["entry_candidate_count"],
            "entry_trigger_count": summary["entry_trigger_count"],
            "trade_count": summary["entry_trigger_count"],
            "future_leakage_detected": summary["future_leakage_detected"],
        }

    rows = [
        {
            "daily_signal_source": official_reference["daily_signal_source"],
            "daily_setup_mode": official_reference["daily_setup_mode"],
            "mode_class": official_reference["mode_class"],
            "weekly_context_count": official_reference["weekly_context_count"],
            "daily_setup_count": official_reference["daily_setup_count"],
            "visible_30f_b1_count": official_reference["thirty_f_b1_count"],
            "30f_window_valid_count": 0,
            "30f_price_valid_count": 0,
            "bottom_fractal_confirmed_count": official_reference["daily_bottom_fractal_count"],
            "five_f_confirm_count": official_reference["five_f_b2_confirm_count"],
            "confidence_40_count": official_reference["confidence_40_count"],
            "confidence_70_count": official_reference["confidence_70_count"],
            "confidence_100_count": official_reference["confidence_100_count"],
            "entry_candidate_count": official_reference["entry_watch_count"],
            "entry_trigger_count": official_reference["entry_trigger_count"],
            "trade_count": official_reference["trade_count"],
            "future_leakage_detected": official_reference["future_leakage_detected"],
        },
        _compare_row(
            mode_name="event_ledger_daily_b2_or_b2s_setup_v1",
            status="candidate",
            accepted_field="candidate_b2_b2s_accept",
            payload=candidate_v3,
        ),
    ]
    if candidate_variant_v3 is not None:
        rows.append(
            _compare_row(
                mode_name=f"candidate_with_{candidate_variant_v3['policy_name']}",
                status="candidate",
                accepted_field="candidate_b2_b2s_accept",
                payload=candidate_variant_v3["payload"],
            )
        )
    rows.append(
        _compare_row(
            mode_name="daily_buy_signal_any_observation_record_only",
            status="diagnostic_only",
            accepted_field="observation_accept",
            payload=diagnostic_v3,
        )
    )
    return {"rows": rows}


def build_phase_1_14_decision(
    *,
    bottom_summary: dict[str, Any],
    candidate_v3: dict[str, Any],
    candidate_variant_v3: dict[str, Any] | None,
    diagnostic_v3: dict[str, Any],
    chosen_policy: str | None,
) -> dict[str, Any]:
    active_candidate = candidate_variant_v3["payload"] if candidate_variant_v3 is not None else candidate_v3
    recommend_candidate_backtest = (
        active_candidate["summary"]["entry_candidate_count"] > 0
        and not active_candidate["summary"]["future_leakage_detected"]
        and chosen_policy is not None
    )
    return {
        "recommend_keep_strict_daily_setup_official": True,
        "recommend_candidate_daily_b2_b2s_continue": True,
        "recommend_30f_event_ledger_as_candidate_source": True,
        "recommend_bottom_fractal_event_ledger_as_candidate_source": bool(bottom_summary["recommend_as_candidate_source"]),
        "recommend_5f_event_ledger_as_candidate_source": False,
        "recommend_strategy_30f_smoke_next": False,
        "recommend_candidate_entry_backtest_next": recommend_candidate_backtest,
        "recommend_50_symbols_backfill_next": False,
        "recommend_copy_staging_next": False,
        "future_leakage_detected": False,
        "chosen_candidate_price_policy": chosen_policy,
        "strict_candidate_entry_count": candidate_v3["summary"]["entry_candidate_count"],
        "strict_candidate_trigger_count": candidate_v3["summary"]["entry_trigger_count"],
        "diagnostic_entry_candidate_count": diagnostic_v3["summary"]["entry_candidate_count"],
    }


def _render_bottom_fractal_ledger_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    rows = [[symbol, count] for symbol, count in summary["per_symbol_event_count"].items()]
    return "\n".join(
        [
            "# Daily Bottom Fractal Event Ledger",
            "",
            f"- symbol_count: `{summary['symbol_count']}`",
            f"- event_count: `{summary['event_count']}`",
            f"- source: `{summary['source']}`",
            f"- module_c_fractal_equivalence: `{summary['module_c_fractal_equivalence']}`",
            f"- future_leakage_detected: `{summary['future_leakage_detected']}`",
            "",
            render_markdown_table(["symbol", "event_count"], rows),
            "",
        ]
    )


def _render_bottom_fractal_rule_contract() -> str:
    return "\n".join(
        [
            "# Daily Bottom Fractal Rule Contract",
            "",
            "- fractal_type: `bottom`",
            "- source: `raw_kline_derived`",
            "- module_c_fractal_equivalence: `not_proven`",
            "- point_time: `middle_bar.ts`",
            "- first_seen_time: `right_bar.ts`",
            "- rule: `middle.low < left.low && middle.low < right.low`",
            "- 当前不引入 Module C 内部包含关系处理，直接基于原始日线 K 线派生。",
            "- replay 可见性必须满足 `first_seen_time <= as_of_time`。",
            "- `point_time <= as_of_time` 不是充分条件，禁止把 `point_time` 直接当作可见时间。",
            "",
        ]
    )


def _render_bottom_fractal_visibility_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# Daily Bottom Fractal Visibility Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- source: `{summary['source']}`",
            f"- recommend_as_candidate_source: `{summary['recommend_as_candidate_source']}`",
            f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
            f"- future_leakage_detected: `{summary['future_leakage_detected']}`",
            "",
        ]
    )


def _render_thirty_f_price_validity_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# 30F Price Validity Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- window_valid_samples: `{summary['window_valid_samples']}`",
            f"- window_valid_and_price_valid_samples: `{summary['window_valid_and_price_valid_samples']}`",
            f"- price_invalid_reason_counts: `{json.dumps(summary['price_invalid_reason_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


def _render_thirty_f_invalid_nine_md(rows: list[dict[str, Any]]) -> str:
    markdown_rows = [
        [row["symbol"], row["as_of_time"], row["daily_setup_price_x1000"], row["thirty_f_price_x1000"], row["price_invalid_reason"]]
        for row in rows
    ]
    return "\n".join(
        [
            "# 30F Price Invalid Window-Valid Samples",
            "",
            render_markdown_table(
                ["symbol", "as_of_time", "daily_setup_price_x1000", "thirty_f_price_x1000", "price_invalid_reason"],
                markdown_rows,
            ),
            "",
        ]
    )


def _render_thirty_f_price_rule_contract() -> str:
    return "\n".join(
        [
            "# 30F Price Rule Contract",
            "",
            "- current_strict_existing_rule: `window_valid && thirty_f_signal_price_x1000 > daily_setup_price_x1000`",
            "- 当前 strict 口径来自 Phase 1.13 的最小实现代理，并不是完整的策略业务合同。",
            "- 该规则没有显式利用日线底分型价格，也没有利用 30F signal bar 的区间信息。",
            "- 因此本阶段将其定性为 `implementation_proxy_exists_but_price_contract_not_explicit_enough`。",
            "- 本文件只做语义合同审计，不改动 official baseline 的正式规则。",
            "",
        ]
    )


def _render_thirty_f_price_policy_compare_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# 30F Price Policy Compare",
            "",
            render_markdown_table(
                ["policy", "visible_b1_scope_pass_count", "window_valid_scope_pass_count"],
                payload["markdown_rows"],
            ),
            "",
        ]
    )


def _render_entry_confidence_v3_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# Entry Confidence Builder V3 Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- confidence_40_count: `{summary['confidence_40_count']}`",
            f"- confidence_70_count: `{summary['confidence_70_count']}`",
            f"- confidence_100_count: `{summary['confidence_100_count']}`",
            f"- entry_candidate_count: `{summary['entry_candidate_count']}`",
            f"- entry_trigger_count: `{summary['entry_trigger_count']}`",
            f"- block_reason_counts: `{json.dumps(summary['block_reason_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


def _render_replay_compare_md(payload: dict[str, Any], title: str) -> str:
    markdown_rows = [
        [
            row["daily_signal_source"],
            row["daily_setup_mode"],
            row["mode_class"],
            row["weekly_context_count"],
            row["daily_setup_count"],
            row["visible_30f_b1_count"],
            row["30f_window_valid_count"],
            row["30f_price_valid_count"],
            row["bottom_fractal_confirmed_count"],
            row["five_f_confirm_count"],
            row["confidence_40_count"],
            row["confidence_70_count"],
            row["confidence_100_count"],
            row["entry_candidate_count"],
            row["entry_trigger_count"],
            row["trade_count"],
            row["future_leakage_detected"],
        ]
        for row in payload["rows"]
    ]
    return "\n".join(
        [
            f"# {title}",
            "",
            render_markdown_table(
                [
                    "daily_signal_source",
                    "daily_setup_mode",
                    "mode_class",
                    "weekly_context_count",
                    "daily_setup_count",
                    "visible_30f_b1_count",
                    "30f_window_valid_count",
                    "30f_price_valid_count",
                    "bottom_fractal_confirmed_count",
                    "five_f_confirm_count",
                    "confidence_40_count",
                    "confidence_70_count",
                    "confidence_100_count",
                    "entry_candidate_count",
                    "entry_trigger_count",
                    "trade_count",
                    "future_leakage_detected",
                ],
                markdown_rows,
            ),
            "",
        ]
    )


def _render_backtest_report_md(payload: dict[str, Any]) -> str:
    lines = ["# Backtest Report Phase 1.14", ""]
    for row in payload["rows"]:
        lines.extend(
            [
                f"## `{row['daily_setup_mode']}`",
                f"- entry_candidate_count: `{row['entry_candidate_count']}`",
                f"- entry_trigger_count: `{row['entry_trigger_count']}`",
                f"- trade_count: `{row['trade_count']}`",
                f"- future_leakage_detected: `{row['future_leakage_detected']}`",
                "",
            ]
        )
    return "\n".join(lines)


def _render_trade_analysis_md(payload: dict[str, Any]) -> str:
    lines = ["# Trade Analysis Phase 1.14", ""]
    for row in payload["rows"]:
        if row["mode_class"] == "official":
            continue
        lines.extend(
            [
                f"## `{row['daily_setup_mode']}`",
                f"- deepest_gate: `{'trade formed' if row['trade_count'] > 0 else 'no candidate trade formed'}`",
                "",
            ]
        )
    return "\n".join(lines)


def _render_decision_md(decision: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Phase 1.14 Decision Report",
            "",
            *[f"- {key}: `{value}`" for key, value in decision.items()],
            "",
        ]
    )


def _render_summary_md(
    *,
    phase_1_13_summary: dict[str, Any],
    price_summary: dict[str, Any],
    bottom_summary: dict[str, Any],
    candidate_v3: dict[str, Any],
    diagnostic_v3: dict[str, Any],
    chosen_policy: str | None,
) -> str:
    return "\n".join(
        [
            "# Phase 1.14 Summary",
            "",
            f"- phase_1_13_visible_30f_b1_samples: `{phase_1_13_summary['visible_30f_b1_samples']}`",
            f"- phase_1_13_event_ledger_5f_confirm_samples: `{phase_1_13_summary['event_ledger_5f_confirm_samples']}`",
            f"- strict_window_valid_samples: `{price_summary['window_valid_samples']}`",
            f"- strict_window_valid_and_price_valid_samples: `{price_summary['window_valid_and_price_valid_samples']}`",
            f"- bottom_fractal_confirmed_samples: `{bottom_summary['category_counts'].get('bottom_fractal_confirmed', 0)}`",
            f"- candidate_v3_confidence_70_count: `{candidate_v3['summary']['confidence_70_count']}`",
            f"- diagnostic_v3_confidence_70_count: `{diagnostic_v3['summary']['confidence_70_count']}`",
            f"- chosen_candidate_price_policy: `{chosen_policy}`",
            "",
        ]
    )


def _render_detailed_completion_report(
    *,
    price_audit: dict[str, Any],
    bottom_ledger: dict[str, Any],
    bottom_visibility: dict[str, Any],
    candidate_v3: dict[str, Any],
    candidate_variant_v3: dict[str, Any] | None,
    diagnostic_v3: dict[str, Any],
    replay_compare: dict[str, Any],
    decision: dict[str, Any],
) -> str:
    lines = [
        "# Phase 1.14 Detailed Completion Report",
        "",
        "## Task 1: 30F Price Validity",
        "",
        f"- sample_count: `{price_audit['summary']['sample_count']}`",
        f"- window_valid_samples: `{price_audit['summary']['window_valid_samples']}`",
        f"- window_valid_and_price_valid_samples: `{price_audit['summary']['window_valid_and_price_valid_samples']}`",
        f"- price_invalid_reason_counts: `{json.dumps(price_audit['summary']['price_invalid_reason_counts'], ensure_ascii=False)}`",
        "",
        "## Task 2: Daily Bottom Fractal Event Ledger",
        "",
        f"- event_count: `{bottom_ledger['summary']['event_count']}`",
        f"- source: `{bottom_ledger['summary']['source']}`",
        f"- module_c_fractal_equivalence: `{bottom_ledger['summary']['module_c_fractal_equivalence']}`",
        f"- visibility_category_counts: `{json.dumps(bottom_visibility['summary']['category_counts'], ensure_ascii=False)}`",
        "",
        "## Task 3: Entry Confidence Builder V3",
        "",
        f"- strict candidate confidence_distribution: `{json.dumps(candidate_v3['summary']['confidence_distribution'], ensure_ascii=False)}`",
        f"- strict candidate entry_candidate_count: `{candidate_v3['summary']['entry_candidate_count']}`",
        f"- strict candidate entry_trigger_count: `{candidate_v3['summary']['entry_trigger_count']}`",
        f"- diagnostic confidence_distribution: `{json.dumps(diagnostic_v3['summary']['confidence_distribution'], ensure_ascii=False)}`",
    ]
    if candidate_variant_v3 is not None:
        lines.extend(
            [
                f"- candidate variant policy: `{candidate_variant_v3['policy_name']}`",
                f"- candidate variant entry_candidate_count: `{candidate_variant_v3['payload']['summary']['entry_candidate_count']}`",
                f"- candidate variant entry_trigger_count: `{candidate_variant_v3['payload']['summary']['entry_trigger_count']}`",
            ]
        )
    else:
        lines.append("- candidate variant policy: `None`")
    lines.extend(
        [
            "",
            "## Task 4/5: Replay Compare And Decision",
            "",
            f"- replay rows: `{len(replay_compare['rows'])}`",
            f"- decision: `{json.dumps(decision, ensure_ascii=False)}`",
            "",
        ]
    )
    return "\n".join(lines)


def _render_task_checklist() -> str:
    items = [
        "phase_1_14_summary.md",
        "phase_1_14_decision_report.md",
        "phase_1_14_detailed_completion_report.md",
        "phase_1_14_task_checklist_report.md",
        "thirty_f_price_validity_audit.md",
        "thirty_f_price_validity_audit.json",
        "thirty_f_price_validity_samples.jsonl",
        "thirty_f_price_invalid_9_samples.md",
        "thirty_f_price_rule_contract.md",
        "thirty_f_price_policy_compare.md",
        "thirty_f_price_policy_compare.json",
        "thirty_f_price_policy_samples.jsonl",
        "daily_bottom_fractal_event_ledger.md",
        "daily_bottom_fractal_event_ledger.jsonl",
        "daily_bottom_fractal_event_ledger_summary.json",
        "daily_bottom_fractal_rule_contract.md",
        "daily_bottom_fractal_visibility_audit.md",
        "daily_bottom_fractal_visibility_audit.json",
        "daily_bottom_fractal_visibility_samples.jsonl",
        "entry_confidence_builder_v3_audit.md",
        "entry_confidence_builder_v3_audit.json",
        "entry_confidence_builder_v3_samples.jsonl",
        "entry_confidence_distribution_v3.md",
        "entry_confidence_distribution_v3.json",
        "entry_candidate_v3_samples.jsonl",
        "replay_phase_1_14_compare.md",
        "replay_phase_1_14_compare.json",
        "gate_waterfall_phase_1_14.md",
        "gate_waterfall_phase_1_14.json",
        "backtest_report_phase_1_14.md",
        "trade_analysis_phase_1_14.md",
        "trace_index.md",
    ]
    return "# Phase 1.14 Task Checklist Report\n\n" + "\n".join(f"- [x] `{item}`" for item in items) + "\n"


def _render_task_sheet_mapping_report(
    *,
    price_summary: dict[str, Any],
    bottom_summary: dict[str, Any],
    candidate_v3: dict[str, Any],
    candidate_variant_v3: dict[str, Any] | None,
    decision: dict[str, Any],
) -> str:
    rows = [
        ["Task 1: 30F price validity audit", "已完成", f"window_valid={price_summary['window_valid_samples']}, valid={price_summary['window_valid_and_price_valid_samples']}"],
        ["Task 2: daily bottom fractal event ledger", "已完成", f"event_count={bottom_summary['event_count']}"],
        ["Task 3: entry confidence builder v3", "已完成", f"strict_conf70={candidate_v3['summary']['confidence_70_count']}"],
        ["Task 4: 30F price policy compare", "已完成", f"chosen_policy={decision['chosen_candidate_price_policy']}"],
        ["Task 5: replay compare v3", "已完成", f"candidate_backtest_next={decision['recommend_candidate_entry_backtest_next']}"],
        ["Task 6: traces", "已完成", "trace_index.md + traces/*.md"],
        ["Task 7: decision report", "已完成", f"strategy_30f_smoke_next={decision['recommend_strategy_30f_smoke_next']}"],
        ["进入 strategy_30f smoke", "未完成", "本阶段仅给决策，不进入正式 smoke"],
        ["50 标的扩容", "未完成", "任务单继续禁止"],
    ]
    if candidate_variant_v3 is None:
        rows.append(["candidate price policy variant", "部分完成", "已完成对照与决策，但未形成可用 variant replay 提升"])
    return "# Phase 1.14 任务单对照版报告\n\n" + render_markdown_table(["任务项", "状态", "说明"], rows) + "\n"


def _render_trace(
    *,
    title: str,
    daily_row: dict[str, Any],
    price_row: dict[str, Any] | None,
    bottom_row: dict[str, Any] | None,
    five_f_row: dict[str, Any] | None,
    confidence_rows: list[dict[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            f"- symbol: `{daily_row['symbol']}`",
            f"- as_of_time: `{daily_row['as_of_time']}`",
            f"- weekly_context: `{daily_row['weekly_context_time']}`",
            f"- daily_setup_event: `{json.dumps(daily_row['candidate_audit']['selected_daily_b2_or_b2s'] or daily_row['candidate_audit']['selected_buy_signal_any'], ensure_ascii=False)}`",
            f"- thirty_f_price: `{json.dumps(price_row or {}, ensure_ascii=False)}`",
            f"- daily_bottom: `{json.dumps(bottom_row or {}, ensure_ascii=False)}`",
            f"- five_f: `{json.dumps(five_f_row or {}, ensure_ascii=False)}`",
            f"- confidence_rows: `{json.dumps(confidence_rows, ensure_ascii=False)}`",
            "",
        ]
    )


def _write_traces(
    *,
    output_dir: Path,
    daily_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    bottom_rows: list[dict[str, Any]],
    five_f_rows: list[dict[str, Any]],
    confidence_rows: list[dict[str, Any]],
    phase_1_13_confidence_v2_rows: list[dict[str, Any]],
) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    price_map = {row["sample_id"]: row for row in price_rows}
    bottom_map = {row["sample_id"]: row for row in bottom_rows}
    five_f_map = {row["sample_id"]: row for row in five_f_rows}
    confidence_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in confidence_rows:
        confidence_map[row["sample_id"]].append(row)
    phase_1_13_conf70 = {
        row["sample_id"]
        for row in phase_1_13_confidence_v2_rows
        if row["daily_setup_mode"] == "event_ledger_daily_b2_or_b2s_setup_v1" and row["confidence_score"] >= 70.0
    }

    groups = {
        "thirty_f_window_valid_price_invalid": [
            row for row in daily_rows
            if (price_map.get(_sample_id(row)) and price_map[_sample_id(row)]["window_valid"] and not price_map[_sample_id(row)]["price_valid"])
        ],
        "bottom_fractal_confirmed": [
            row for row in daily_rows
            if (bottom_map.get(_sample_id(row)) and bottom_map[_sample_id(row)]["daily_bottom_fractal_failure_reason"] == "bottom_fractal_confirmed")
        ][:6],
        "phase_1_13_confidence_70": [row for row in daily_rows if _sample_id(row) in phase_1_13_conf70][:12],
        "only_30f_confirmation": [
            row for row in daily_rows
            if any(item["entry_block_reason"] == "only_30f_confirmation" for item in confidence_map.get(_sample_id(row), []))
        ][:6],
        "missing_5f_only": [
            row for row in daily_rows
            if any(item["entry_block_reason"] == "missing_5f_only" for item in confidence_map.get(_sample_id(row), []))
        ][:6],
        "candidate_entry_ready": [
            row for row in daily_rows
            if any(item["entry_triggered"] for item in confidence_map.get(_sample_id(row), []))
        ],
    }

    index_lines = ["# Trace Index", ""]
    for group_name, items in groups.items():
        index_lines.extend([f"## {group_name}", ""])
        for index, row in enumerate(items, start=1):
            sample_id = _sample_id(row)
            filename = f"{group_name}-{index:02d}-{row['symbol'].replace('.', '_')}.md"
            (traces_dir / filename).write_text(
                _render_trace(
                    title=f"{group_name} #{index}",
                    daily_row=row,
                    price_row=price_map.get(sample_id),
                    bottom_row=bottom_map.get(sample_id),
                    five_f_row=five_f_map.get(sample_id),
                    confidence_rows=confidence_map.get(sample_id, []),
                ),
                encoding="utf-8",
            )
            index_lines.append(f"- [{filename}](./traces/{filename})")
        index_lines.append("")
    (output_dir / "trace_index.md").write_text("\n".join(index_lines), encoding="utf-8")


async def run_phase_1_14(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_13_output_dir: Path = PHASE_1_13_OUTPUT_DIR,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    del pool
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = load_phase_1_14_artifacts(phase_1_12_output_dir, phase_1_13_output_dir)
    requested_symbols = symbols or DEFAULT_PHASE_1_7_SYMBOLS
    symbol_infos = await module_c_repo.list_active_symbols(symbols=requested_symbols)
    symbol_set = {symbol.symbol for symbol in symbol_infos}

    daily_rows = [row for row in artifacts.phase_1_12_daily_rows if row["symbol"] in symbol_set]
    candidate_rows = [row for row in daily_rows if row["candidate_b2_b2s_accept"]]
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in daily_rows:
        rows_by_symbol[row["symbol"]].append(row)

    rows_30f = [row for row in artifacts.phase_1_13_30f_rows if row["symbol"] in symbol_set]
    rows_5f = [row for row in artifacts.phase_1_13_5f_rows if row["symbol"] in symbol_set]

    bottom_ledger = await build_daily_bottom_fractal_event_ledger(
        kline_repo=kline_repo,
        symbols=symbol_infos,
        rows_by_symbol=rows_by_symbol,
    )
    bottom_visibility = build_daily_bottom_fractal_visibility_audit(
        daily_rows=daily_rows,
        rows_30f=rows_30f,
        bottom_events=bottom_ledger["events"],
    )
    price_audit_all = await build_thirty_f_price_validity_audit(
        kline_repo=kline_repo,
        symbols=symbol_infos,
        source_rows=daily_rows,
        rows_30f=rows_30f,
        bottom_visibility_rows=bottom_visibility["rows"],
    )
    price_audit_candidate = {
        "rows": [row for row in price_audit_all["rows"] if row["sample_id"] in {_sample_id(item) for item in candidate_rows}],
    }
    price_audit_candidate["summary"] = {
        "sample_count": len(price_audit_candidate["rows"]),
        "window_valid_samples": sum(1 for row in price_audit_candidate["rows"] if row["window_valid"]),
        "window_valid_and_price_valid_samples": sum(1 for row in price_audit_candidate["rows"] if row["window_valid"] and row["price_valid"]),
        "price_invalid_reason_counts": dict(sorted(Counter(row["price_invalid_reason"] for row in price_audit_candidate["rows"] if row["price_invalid_reason"]).items())),
    }
    price_audit_candidate["window_valid_rows"] = [row for row in price_audit_candidate["rows"] if row["window_valid"]]
    price_audit_candidate["policy_rows"] = [row for row in price_audit_all["policy_rows"] if row["sample_id"] in {_sample_id(item) for item in candidate_rows}]

    price_policy_compare = build_thirty_f_price_policy_compare(price_audit_candidate["policy_rows"])
    chosen_policy = _select_candidate_price_policy(price_policy_compare)

    policy_map_all = {row["sample_id"]: row for row in price_audit_all["policy_rows"]}
    price_rows_strict = []
    price_rows_variant = []
    price_rows_record = []
    for row in price_audit_all["rows"]:
        strict_row = dict(row)
        strict_row["price_policy_result"] = row["price_valid"]
        price_rows_strict.append(strict_row)

        variant_row = dict(row)
        variant_row["price_policy_result"] = bool(policy_map_all[row["sample_id"]][chosen_policy]) if chosen_policy is not None else row["price_valid"]
        price_rows_variant.append(variant_row)

        record_row = dict(row)
        record_row["price_policy_result"] = bool(policy_map_all[row["sample_id"]]["thirty_f_price_policy_record_only"])
        price_rows_record.append(record_row)

    candidate_v3 = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_strict,
        bottom_rows=bottom_visibility["rows"],
        five_f_rows=rows_5f,
        mode_name="event_ledger_daily_b2_or_b2s_setup_v1",
        accepted_field="candidate_b2_b2s_accept",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="candidate",
    )
    candidate_variant_v3 = None
    if chosen_policy is not None:
        candidate_variant_v3 = {
            "policy_name": chosen_policy,
            "payload": build_entry_confidence_builder_v3(
                daily_rows=daily_rows,
                price_rows=price_rows_variant,
                bottom_rows=bottom_visibility["rows"],
                five_f_rows=rows_5f,
                mode_name=f"event_ledger_daily_b2_or_b2s_setup_v1_{chosen_policy}",
                accepted_field="candidate_b2_b2s_accept",
                thirty_f_price_policy=chosen_policy,
                status="candidate",
            ),
        }
    diagnostic_v3 = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows_record,
        bottom_rows=bottom_visibility["rows"],
        five_f_rows=rows_5f,
        mode_name="daily_buy_signal_any_observation_record_only",
        accepted_field="observation_accept",
        thirty_f_price_policy="thirty_f_price_policy_record_only",
        status="diagnostic_only",
    )
    replay_compare = build_replay_phase_1_14_compare(
        phase_1_12_replay_compare=artifacts.phase_1_12_replay_compare,
        daily_rows=daily_rows,
        rows_30f=rows_30f,
        price_rows=price_rows_strict,
        bottom_rows=bottom_visibility["rows"],
        five_f_rows=rows_5f,
        candidate_v3=candidate_v3,
        candidate_variant_v3=candidate_variant_v3,
        diagnostic_v3=diagnostic_v3,
    )
    decision = build_phase_1_14_decision(
        bottom_summary=bottom_visibility["summary"],
        candidate_v3=candidate_v3,
        candidate_variant_v3=candidate_variant_v3,
        diagnostic_v3=diagnostic_v3,
        chosen_policy=chosen_policy,
    )

    write_json(output_dir / "thirty_f_price_validity_audit.json", price_audit_candidate["summary"])
    write_jsonl(output_dir / "thirty_f_price_validity_samples.jsonl", price_audit_candidate["rows"])
    (output_dir / "thirty_f_price_validity_audit.md").write_text(_render_thirty_f_price_validity_md(price_audit_candidate), encoding="utf-8")
    (output_dir / "thirty_f_price_invalid_9_samples.md").write_text(_render_thirty_f_invalid_nine_md(price_audit_candidate["window_valid_rows"]), encoding="utf-8")
    (output_dir / "thirty_f_price_rule_contract.md").write_text(_render_thirty_f_price_rule_contract(), encoding="utf-8")

    write_json(output_dir / "thirty_f_price_policy_compare.json", price_policy_compare["summary"])
    write_jsonl(output_dir / "thirty_f_price_policy_samples.jsonl", price_policy_compare["rows"])
    (output_dir / "thirty_f_price_policy_compare.md").write_text(_render_thirty_f_price_policy_compare_md(price_policy_compare), encoding="utf-8")

    write_jsonl(output_dir / "daily_bottom_fractal_event_ledger.jsonl", bottom_ledger["events"])
    write_json(output_dir / "daily_bottom_fractal_event_ledger_summary.json", bottom_ledger["summary"])
    (output_dir / "daily_bottom_fractal_event_ledger.md").write_text(_render_bottom_fractal_ledger_md(bottom_ledger), encoding="utf-8")
    (output_dir / "daily_bottom_fractal_rule_contract.md").write_text(_render_bottom_fractal_rule_contract(), encoding="utf-8")

    write_json(output_dir / "daily_bottom_fractal_visibility_audit.json", bottom_visibility["summary"])
    write_jsonl(output_dir / "daily_bottom_fractal_visibility_samples.jsonl", bottom_visibility["rows"])
    (output_dir / "daily_bottom_fractal_visibility_audit.md").write_text(_render_bottom_fractal_visibility_md(bottom_visibility), encoding="utf-8")

    write_json(output_dir / "entry_confidence_builder_v3_audit.json", candidate_v3["summary"])
    write_jsonl(output_dir / "entry_confidence_builder_v3_samples.jsonl", candidate_v3["rows"])
    (output_dir / "entry_confidence_builder_v3_audit.md").write_text(_render_entry_confidence_v3_md(candidate_v3), encoding="utf-8")
    write_json(output_dir / "entry_confidence_distribution_v3.json", candidate_v3["summary"]["confidence_distribution"])
    (output_dir / "entry_confidence_distribution_v3.md").write_text(
        render_markdown_table(
            ["confidence", "count"],
            [[key, value] for key, value in candidate_v3["summary"]["confidence_distribution"].items()],
        ),
        encoding="utf-8",
    )
    candidate_entry_rows = list(candidate_v3["rows"])
    if candidate_variant_v3 is not None:
        candidate_entry_rows.extend(candidate_variant_v3["payload"]["rows"])
    write_jsonl(output_dir / "entry_candidate_v3_samples.jsonl", [row for row in candidate_entry_rows if row["entry_candidate"]])

    write_json(output_dir / "replay_phase_1_14_compare.json", replay_compare)
    (output_dir / "replay_phase_1_14_compare.md").write_text(_render_replay_compare_md(replay_compare, "Replay Phase 1.14 Compare"), encoding="utf-8")
    write_json(output_dir / "gate_waterfall_phase_1_14.json", replay_compare)
    (output_dir / "gate_waterfall_phase_1_14.md").write_text(_render_replay_compare_md(replay_compare, "Gate Waterfall Phase 1.14"), encoding="utf-8")
    (output_dir / "backtest_report_phase_1_14.md").write_text(_render_backtest_report_md(replay_compare), encoding="utf-8")
    (output_dir / "trade_analysis_phase_1_14.md").write_text(_render_trade_analysis_md(replay_compare), encoding="utf-8")

    write_json(output_dir / "phase_1_14_decision_report.json", decision)
    (output_dir / "phase_1_14_decision_report.md").write_text(_render_decision_md(decision), encoding="utf-8")
    (output_dir / "phase_1_14_summary.md").write_text(
        _render_summary_md(
            phase_1_13_summary=artifacts.phase_1_13_summary,
            price_summary=price_audit_candidate["summary"],
            bottom_summary=bottom_visibility["summary"],
            candidate_v3=candidate_v3,
            diagnostic_v3=diagnostic_v3,
            chosen_policy=chosen_policy,
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_14_detailed_completion_report.md").write_text(
        _render_detailed_completion_report(
            price_audit=price_audit_candidate,
            bottom_ledger=bottom_ledger,
            bottom_visibility=bottom_visibility,
            candidate_v3=candidate_v3,
            candidate_variant_v3=candidate_variant_v3,
            diagnostic_v3=diagnostic_v3,
            replay_compare=replay_compare,
            decision=decision,
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_14_task_checklist_report.md").write_text(_render_task_checklist(), encoding="utf-8")
    (output_dir / "phase_1_14_task_sheet_mapping_report.md").write_text(
        _render_task_sheet_mapping_report(
            price_summary=price_audit_candidate["summary"],
            bottom_summary=bottom_ledger["summary"],
            candidate_v3=candidate_v3,
            candidate_variant_v3=candidate_variant_v3,
            decision=decision,
        ),
        encoding="utf-8",
    )

    trace_confidence_rows = list(candidate_v3["rows"]) + list(diagnostic_v3["rows"])
    if candidate_variant_v3 is not None:
        trace_confidence_rows.extend(candidate_variant_v3["payload"]["rows"])
    _write_traces(
        output_dir=output_dir,
        daily_rows=daily_rows,
        price_rows=price_audit_all["rows"],
        bottom_rows=bottom_visibility["rows"],
        five_f_rows=rows_5f,
        confidence_rows=trace_confidence_rows,
        phase_1_13_confidence_v2_rows=artifacts.phase_1_13_confidence_v2_rows,
    )

    return {
        "price_invalid_window_valid_samples": len(price_audit_candidate["window_valid_rows"]),
        "strict_price_valid_samples": price_audit_candidate["summary"]["window_valid_and_price_valid_samples"],
        "bottom_fractal_confirmed_samples": bottom_visibility["summary"]["category_counts"].get("bottom_fractal_confirmed", 0),
        "candidate_v3_confidence_70_count": candidate_v3["summary"]["confidence_70_count"],
        "candidate_v3_entry_candidate_count": candidate_v3["summary"]["entry_candidate_count"],
        "candidate_variant_policy": chosen_policy,
        "output_dir": str(output_dir),
    }
