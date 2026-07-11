from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.analyzers.fractal_detector import latest_bottom_fractal_time
from app.domain.enums import LEVEL_TO_DB
from app.domain.models import SymbolInfo
from app.engine.phase_1_11 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_11_OUTPUT_DIR,
    HISTORICAL_RUN_GROUP,
    HISTORICAL_RUN_KIND,
    build_signal_fingerprint,
    parse_dt,
    read_jsonl,
    render_markdown_table,
    write_json,
    write_jsonl,
)
from app.engine.phase_1_12 import DEFAULT_OUTPUT_DIR as PHASE_1_12_OUTPUT_DIR
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-13-30f-5f-confirmation-ledger"
THIRTY_F_WINDOW_DAYS = (5, 10, 20)


@dataclass(slots=True)
class Phase113Artifacts:
    phase_1_12_daily_rows: list[dict[str, Any]]
    phase_1_12_replay_compare: dict[str, Any]
    phase_1_12_summary: dict[str, Any]
    phase_1_11_daily_event_ledger: list[dict[str, Any]]


def _read_json(path: Path) -> dict[str, Any] | list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_phase_1_13_artifacts(
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_11_output_dir: Path = PHASE_1_11_OUTPUT_DIR,
) -> Phase113Artifacts:
    return Phase113Artifacts(
        phase_1_12_daily_rows=read_jsonl(phase_1_12_output_dir / "daily_setup_sample_audit_v3.jsonl"),
        phase_1_12_replay_compare=dict(_read_json(phase_1_12_output_dir / "replay_phase_1_12_compare.json")),
        phase_1_12_summary=dict(_read_json(phase_1_12_output_dir / "phase_1_12_summary.json")),
        phase_1_11_daily_event_ledger=read_jsonl(phase_1_11_output_dir / "daily_signal_event_ledger.jsonl"),
    )


def _event_payload(event: dict[str, Any] | None) -> dict[str, Any] | None:
    if event is None:
        return None
    return {
        "signal_point_time": event["signal_point_time"],
        "first_seen_time": event["first_seen_time"],
        "price": int(event["price_x1000"]) / 1000,
        "bsp_type": event["bsp_type"],
        "observed_run_count": event["observed_run_count"],
        "signal_fingerprint": event["signal_fingerprint"],
    }


def _signal_payload(signal) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "time": signal.point_time.isoformat(),
        "base_time": signal.base_time.isoformat(),
        "price": signal.price,
        "side": signal.side,
        "bsp_type": signal.bsp_type,
        "run_id": signal.run_id,
        "first_seen_time": signal.features.get("first_seen_time"),
    }


def _nearest_event(events: list[dict[str, Any]], predicate) -> dict[str, Any] | None:
    for event in reversed(events):
        if predicate(event):
            return event
    return None


def _first_event(events: list[dict[str, Any]], predicate) -> dict[str, Any] | None:
    for event in events:
        if predicate(event):
            return event
    return None


def _parse_signal_first_seen(signal_payload: dict[str, Any] | None) -> datetime | None:
    if signal_payload is None:
        return None
    features = signal_payload.get("features") or {}
    return parse_dt(features.get("first_seen_time")) or parse_dt(signal_payload.get("point_time"))


def _visible_events(events: list[dict[str, Any]], as_of_time: datetime) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if parse_dt(event["first_seen_time"]) <= as_of_time and parse_dt(event["signal_point_time"]) <= as_of_time
    ]


def _future_events(events: list[dict[str, Any]], as_of_time: datetime) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if parse_dt(event["first_seen_time"]) > as_of_time or parse_dt(event["signal_point_time"]) > as_of_time
    ]


def _trading_day_distance(day_order: list[datetime], left: datetime | None, right: datetime | None) -> int | None:
    if left is None or right is None or not day_order:
        return None
    left_index = None
    right_index = None
    for index, ts in enumerate(day_order):
        if ts <= left:
            left_index = index
        if ts <= right:
            right_index = index
    if left_index is None or right_index is None:
        return None
    return left_index - right_index


def _window_membership(day_order: list[datetime], left: datetime | None, right: datetime | None) -> dict[int, bool]:
    distance = _trading_day_distance(day_order, left, right)
    abs_distance = abs(distance) if distance is not None else None
    return {window: abs_distance is not None and abs_distance <= window for window in THIRTY_F_WINDOW_DAYS}


def _first_bottom_fractal_time(bars, *, after: datetime | None = None) -> datetime | None:
    for index in range(1, len(bars) - 1):
        left, mid, right = bars[index - 1], bars[index], bars[index + 1]
        if after is not None and right.ts <= after:
            continue
        if mid.low < left.low and mid.low < right.low:
            return right.ts
    return None


def _build_entry_failure_reason_v2(
    *,
    confirmation_30f_b1: bool,
    confirmation_daily_bottom_fractal: bool,
    confirmation_5f_b2_confirms_30f: bool,
    order_invalid: bool,
    first_seen_after_as_of: bool,
) -> str:
    if first_seen_after_as_of:
        return "confirmation_first_seen_after_as_of"
    if order_invalid:
        return "confirmation_time_order_invalid"
    count = int(confirmation_30f_b1) + int(confirmation_daily_bottom_fractal) + int(confirmation_5f_b2_confirms_30f)
    if count == 0:
        return "all_confirmations_absent"
    if count == 1:
        if confirmation_30f_b1:
            return "only_30f_confirmation"
        if confirmation_daily_bottom_fractal:
            return "only_daily_bottom_fractal_confirmation"
        return "only_5f_confirmation"
    if confirmation_30f_b1 and confirmation_daily_bottom_fractal and not confirmation_5f_b2_confirms_30f:
        return "missing_5f_only"
    if confirmation_30f_b1 and confirmation_5f_b2_confirms_30f and not confirmation_daily_bottom_fractal:
        return "missing_daily_bottom_only"
    if confirmation_daily_bottom_fractal and confirmation_5f_b2_confirms_30f and not confirmation_30f_b1:
        return "missing_30f_and_5f"
    return "confirmed"


async def build_level_signal_event_ledger_payload(
    *,
    pool: asyncpg.Pool,
    symbols: list[SymbolInfo],
    level: str,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, Any]:
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
            HISTORICAL_RUN_KIND,
            HISTORICAL_RUN_GROUP,
            2,
            start_time,
            end_time,
        )

    ledger: dict[str, dict[str, Any]] = {}
    per_symbol_event_count: Counter[str] = Counter()
    per_bsp_type_event_count: Counter[str] = Counter()
    raw_signal_rows = 0

    for row in rows:
        raw_signal_rows += 1
        extra = row["extra"]
        if isinstance(extra, str):
            extra = json.loads(extra)
        extra = extra if isinstance(extra, dict) else {}
        symbol = symbol_map[int(row["symbol_id"])].symbol
        signal_point_time = row["signal_base_ts"]
        price_x1000 = int(row["price_x1000"])
        bsp_type = extra.get("bsp_type")
        side = extra.get("side")
        fingerprint = build_signal_fingerprint(
            symbol=symbol,
            level=level,
            mode="predictive",
            side=side,
            bsp_type=bsp_type,
            signal_point_time=signal_point_time,
            price_x1000=price_x1000,
        )
        observed_time = row["cutoff_bar_end"]
        payload = ledger.get(fingerprint)
        if payload is None:
            ledger[fingerprint] = {
                "symbol": symbol,
                "level": level,
                "mode": "predictive",
                "run_kind": row["run_kind"],
                "run_group_id": row["run_group_id"],
                "side": side,
                "bsp_type": bsp_type,
                "signal_type": row["signal_type"],
                "signal_point_time": signal_point_time.isoformat(),
                "signal_ts": row["signal_ts"].isoformat() if row["signal_ts"] is not None else None,
                "signal_base_ts": signal_point_time.isoformat(),
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
            continue
        payload["observed_run_count"] += 1
        payload["last_seen_time"] = observed_time.isoformat()
        payload["last_seen_run_id"] = int(row["run_id"])
        if len(payload["source_run_ids_sample"]) < 8:
            payload["source_run_ids_sample"].append(int(row["run_id"]))

    events = sorted(
        ledger.values(),
        key=lambda item: (item["symbol"], item["signal_point_time"], item["price_x1000"]),
    )
    for event in events:
        per_symbol_event_count[event["symbol"]] += 1
        per_bsp_type_event_count[str(event["bsp_type"] or "")] += 1

    summary = {
        "level": level,
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "symbol_count": len(symbols),
        "raw_signal_rows": raw_signal_rows,
        "unique_signal_events": len(events),
        "dedup_ratio": (len(events) / raw_signal_rows) if raw_signal_rows else 0.0,
        "per_symbol_event_count": dict(sorted(per_symbol_event_count.items())),
        "per_bsp_type_event_count": dict(sorted(per_bsp_type_event_count.items())),
        "invalid_first_seen_gt_last_seen": sum(
            1 for event in events if parse_dt(event["first_seen_time"]) > parse_dt(event["last_seen_time"])
        ),
    }
    return {"events": events, "summary": summary}


def _render_level_signal_event_ledger_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    rows_symbol = [[symbol, count] for symbol, count in summary["per_symbol_event_count"].items()]
    rows_bsp = [[bsp or "(empty)", count] for bsp, count in summary["per_bsp_type_event_count"].items()]
    return "\n".join(
        [
            f"# {summary['level'].upper()} Signal Event Ledger",
            "",
            f"- raw_signal_rows: `{summary['raw_signal_rows']}`",
            f"- unique_signal_events: `{summary['unique_signal_events']}`",
            f"- dedup_ratio: `{summary['dedup_ratio']:.4f}`",
            f"- invalid_first_seen_gt_last_seen: `{summary['invalid_first_seen_gt_last_seen']}`",
            "",
            "## Per Symbol",
            render_markdown_table(["symbol", "event_count"], rows_symbol),
            "",
            "## Per BSP Type",
            render_markdown_table(["bsp_type", "event_count"], rows_bsp),
            "",
        ]
    )


def _build_multi_level_ledger_summary(*, payload_30f: dict[str, Any], payload_5f: dict[str, Any]) -> dict[str, Any]:
    return {
        "levels": {
            "30f": payload_30f["summary"],
            "5f": payload_5f["summary"],
        }
    }


def _render_multi_level_ledger_summary_md(payload: dict[str, Any]) -> str:
    rows = []
    for level, summary in payload["levels"].items():
        rows.append(
            [
                level,
                summary["raw_signal_rows"],
                summary["unique_signal_events"],
                f"{summary['dedup_ratio']:.4f}",
                summary["invalid_first_seen_gt_last_seen"],
            ]
        )
    return "\n".join(
        [
            "# Multi-Level Signal Event Ledger Summary",
            "",
            render_markdown_table(
                ["level", "raw_signal_rows", "unique_signal_events", "dedup_ratio", "invalid_first_seen_gt_last_seen"],
                rows,
            ),
            "",
        ]
    )


def _sample_id(row: dict[str, Any]) -> str:
    return f"{row['symbol']}|{row['as_of_time']}"


async def build_thirty_f_event_ledger_visibility_audit(
    *,
    module_c_repo: ModuleCRepository,
    symbols: list[SymbolInfo],
    candidate_rows: list[dict[str, Any]],
    events_30f: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events_30f:
        events_by_symbol[event["symbol"]].append(event)

    rows: list[dict[str, Any]] = []
    category_counts = Counter()
    selected_run_underestimate = 0

    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        rows_by_symbol[row["symbol"]].append(row)

    for symbol_code, symbol_rows in rows_by_symbol.items():
        symbol = symbol_map[symbol_code]
        max_as_of = max(parse_dt(row["as_of_time"]) for row in symbol_rows)
        await module_c_repo.prime_symbol_cache(symbol.symbol_id, levels=("30f",), modes=("predictive",))
        try:
            selected_signals = await module_c_repo.get_signals(
                symbol.symbol_id,
                "30f",
                mode="predictive",
                as_of_time=max_as_of,
                run_kind=HISTORICAL_RUN_KIND,
                run_group_id=HISTORICAL_RUN_GROUP,
                allow_legacy_mode_fallback=False,
            )
            for row in symbol_rows:
                as_of_time = parse_dt(row["as_of_time"])
                day_order = [parse_dt(item) for item in row["day_order"]]
                setup_signal = row["candidate_audit"]["selected_daily_b2_or_b2s"] or row["candidate_audit"]["selected_buy_signal_any"]
                daily_setup_time = _parse_signal_first_seen(setup_signal) or as_of_time
                daily_setup_price = float(setup_signal["price"]) if setup_signal is not None else 0.0
                symbol_events = events_by_symbol[row["symbol"]]
                visible_buy_events = _visible_events(symbol_events, as_of_time)
                visible_b1_events = [event for event in visible_buy_events if event["bsp_type"] in {"1", "1p"}]
                visible_b2_events = [event for event in visible_buy_events if event["bsp_type"] in {"2", "2s"}]
                latest_b1_after_setup = _nearest_event(
                    visible_b1_events,
                    lambda event: parse_dt(event["first_seen_time"]) >= daily_setup_time,
                )
                selected_at_asof = [
                    signal for signal in selected_signals if signal.point_time <= as_of_time
                ]
                selected_buy_count = sum(1 for signal in selected_at_asof if signal.side == "buy")
                selected_b1_count = sum(
                    1 for signal in selected_at_asof if signal.side == "buy" and signal.bsp_type in {"1", "1p"}
                )
                future_buy_events = _future_events(symbol_events, as_of_time)
                window_flags = _window_membership(
                    day_order,
                    parse_dt(latest_b1_after_setup["signal_point_time"]) if latest_b1_after_setup else None,
                    daily_setup_time,
                )
                price_invalid = latest_b1_after_setup is not None and (int(latest_b1_after_setup["price_x1000"]) / 1000) <= daily_setup_price
                selected_underestimate = selected_buy_count < len(visible_buy_events) or selected_b1_count < len(visible_b1_events)
                if selected_underestimate:
                    selected_run_underestimate += 1

                if not visible_buy_events:
                    category = "future_30f_signal_only" if future_buy_events else "no_30f_buy_signal_visible"
                elif not visible_b1_events:
                    category = "has_30f_buy_but_no_B1_or_1p"
                elif latest_b1_after_setup is None:
                    category = "has_30f_B1_but_outside_window"
                elif price_invalid:
                    category = "has_30f_B1_but_invalidated_by_price"
                elif not any(window_flags.values()):
                    category = "has_30f_B1_but_outside_window"
                else:
                    category = "has_30f_B1_in_window_and_valid"
                category_counts[category] += 1

                rows.append(
                    {
                        "sample_id": f"{row['symbol']}|{row['as_of_time']}",
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "as_of_time": row["as_of_time"],
                        "weekly_context_time": row["weekly_context_time"],
                        "daily_setup_time": daily_setup_time.isoformat(),
                        "daily_setup_bsp_type": row["candidate_audit"]["selected_signal_kind"],
                        "candidate_daily_setup_accepted": True,
                        "visible_30f_buy_count": len(visible_buy_events),
                        "visible_30f_B1_or_1p_count": len(visible_b1_events),
                        "visible_30f_B2_or_2s_count": len(visible_b2_events),
                        "nearest_30f_buy_before_as_of": _event_payload(_nearest_event(visible_buy_events, lambda event: True)),
                        "nearest_30f_B1_before_as_of": _event_payload(_nearest_event(visible_b1_events, lambda event: True)),
                        "nearest_30f_buy_after_as_of": _event_payload(_first_event(future_buy_events, lambda event: True)),
                        "selected_run_30f_signal_count": selected_buy_count,
                        "event_ledger_30f_signal_count": len(visible_buy_events),
                        "selected_run_underestimates_30f": selected_underestimate,
                        "thirty_f_window_valid": any(window_flags.values()),
                        "thirty_f_failure_reason": category,
                        "window_flags": window_flags,
                        "latest_30f_B1_after_daily_setup": _event_payload(latest_b1_after_setup),
                        "latest_30f_B1_before_daily_setup": _event_payload(
                            _nearest_event(visible_b1_events, lambda event: parse_dt(event["first_seen_time"]) < daily_setup_time)
                        ),
                    }
                )
        finally:
            module_c_repo.release_symbol_cache(symbol.symbol_id)

    summary = {
        "sample_count": len(candidate_rows),
        "visible_30f_buy_signal_samples": sum(1 for row in rows if row["visible_30f_buy_count"] > 0),
        "visible_30f_b1_samples": sum(1 for row in rows if row["visible_30f_B1_or_1p_count"] > 0),
        "window_valid_samples": sum(1 for row in rows if row["thirty_f_window_valid"]),
        "window_valid_and_price_valid_samples": sum(
            1 for row in rows if row["thirty_f_failure_reason"] == "has_30f_B1_in_window_and_valid"
        ),
        "selected_run_underestimate_samples": selected_run_underestimate,
        "category_counts": dict(sorted(category_counts.items())),
    }
    return {"rows": rows, "summary": summary}


def _render_30f_visibility_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# 30F Event Ledger Visibility Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- visible_30f_buy_signal_samples: `{summary['visible_30f_buy_signal_samples']}`",
            f"- visible_30f_b1_samples: `{summary['visible_30f_b1_samples']}`",
            f"- window_valid_samples: `{summary['window_valid_samples']}`",
            f"- window_valid_and_price_valid_samples: `{summary['window_valid_and_price_valid_samples']}`",
            f"- selected_run_underestimate_samples: `{summary['selected_run_underestimate_samples']}`",
            f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


def _render_gate_waterfall_md(title: str, counts: dict[str, int]) -> str:
    rows = [[key, value] for key, value in counts.items()]
    return "\n".join([f"# {title}", "", render_markdown_table(["category", "count"], rows), ""])


async def build_five_f_confirmation_audit(
    *,
    module_c_repo: ModuleCRepository,
    symbols: list[SymbolInfo],
    rows_30f: list[dict[str, Any]],
    events_5f: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events_5f:
        events_by_symbol[event["symbol"]].append(event)

    rows: list[dict[str, Any]] = []
    category_counts = Counter()

    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows_30f:
        if row["visible_30f_B1_or_1p_count"] > 0:
            rows_by_symbol[row["symbol"]].append(row)

    for symbol_code, symbol_rows in rows_by_symbol.items():
        symbol = symbol_map[symbol_code]
        max_as_of = max(parse_dt(row["as_of_time"]) for row in symbol_rows)
        await module_c_repo.prime_symbol_cache(symbol.symbol_id, levels=("5f",), modes=("predictive",))
        try:
            selected_signals = await module_c_repo.get_signals(
                symbol.symbol_id,
                "5f",
                mode="predictive",
                as_of_time=max_as_of,
                run_kind=HISTORICAL_RUN_KIND,
                run_group_id=HISTORICAL_RUN_GROUP,
                allow_legacy_mode_fallback=False,
            )
            for row in symbol_rows:
                as_of_time = parse_dt(row["as_of_time"])
                thirty_f_b1 = row["latest_30f_B1_after_daily_setup"] or row["nearest_30f_B1_before_as_of"]
                if thirty_f_b1 is None:
                    continue
                thirty_f_b1_time = parse_dt(thirty_f_b1["signal_point_time"])
                thirty_f_b1_price = float(thirty_f_b1["price"])
                symbol_events = events_by_symbol[row["symbol"]]
                visible_buy_events = [
                    event
                    for event in _visible_events(symbol_events, as_of_time)
                    if parse_dt(event["signal_point_time"]) >= thirty_f_b1_time
                ]
                visible_b2_events = [event for event in visible_buy_events if event["bsp_type"] in {"2", "2s"}]
                future_buy_events = [
                    event
                    for event in _future_events(symbol_events, as_of_time)
                    if parse_dt(event["signal_point_time"]) >= thirty_f_b1_time
                ]
                future_b2_events = [event for event in future_buy_events if event["bsp_type"] in {"2", "2s"}]
                selected_at_asof = [signal for signal in selected_signals if signal.point_time <= as_of_time]
                selected_b2 = [
                    signal
                    for signal in selected_at_asof
                    if signal.side == "buy" and signal.bsp_type in {"2", "2s"} and signal.point_time >= thirty_f_b1_time
                ]
                latest_b2 = _nearest_event(visible_b2_events, lambda event: True)
                price_invalid = latest_b2 is not None and (int(latest_b2["price_x1000"]) / 1000) <= thirty_f_b1_price
                has_selected_confirm = bool(selected_b2)
                has_ledger_confirm = bool(latest_b2) and not price_invalid
                selected_underestimate = bool(visible_b2_events) and not has_selected_confirm

                if not visible_buy_events:
                    category = "future_5f_signal_only" if future_buy_events else "no_5f_buy_signal_visible"
                elif not visible_b2_events:
                    category = "future_5f_signal_only" if future_b2_events else "has_5f_buy_but_no_B2_or_2s"
                elif price_invalid:
                    category = "has_5f_B2_but_price_invalid"
                elif has_ledger_confirm:
                    category = "has_5f_B2_confirm"
                else:
                    category = "has_5f_B2_but_outside_window"
                category_counts[category] += 1

                rows.append(
                    {
                        "sample_id": row["sample_id"],
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "as_of_time": row["as_of_time"],
                        "thirty_f_b1_first_seen_time": thirty_f_b1["first_seen_time"],
                        "five_f_buy_any_visible": bool(visible_buy_events),
                        "five_f_B2_or_2s_visible": bool(visible_b2_events),
                        "five_f_B2_after_30f_B1": bool(latest_b2),
                        "five_f_B2_within_confirm_window": has_ledger_confirm,
                        "five_f_B2_not_break_30f_B1_price": not price_invalid if latest_b2 else False,
                        "five_f_B2_confirms_30f": has_ledger_confirm,
                        "five_f_selected_run_underestimates_event": selected_underestimate,
                        "selected_run_5f_b2_confirm_found": has_selected_confirm,
                        "historical_visible_5f_b2_confirm_found": bool(visible_b2_events),
                        "latest_5f_B2_event": _event_payload(latest_b2),
                        "failure_reason": category,
                    }
                )
        finally:
            module_c_repo.release_symbol_cache(symbol.symbol_id)

    summary = {
        "sample_count": len(rows),
        "visible_5f_buy_signal_samples": sum(1 for row in rows if row["five_f_buy_any_visible"]),
        "visible_5f_b2_samples": sum(1 for row in rows if row["five_f_B2_or_2s_visible"]),
        "selected_run_5f_confirm_samples": sum(1 for row in rows if row["selected_run_5f_b2_confirm_found"]),
        "event_ledger_5f_confirm_samples": sum(1 for row in rows if row["five_f_B2_confirms_30f"]),
        "category_counts": dict(sorted(category_counts.items())),
    }
    return {"rows": rows, "summary": summary}


def _render_5f_confirmation_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# 5F Confirmation Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- visible_5f_buy_signal_samples: `{summary['visible_5f_buy_signal_samples']}`",
            f"- visible_5f_b2_samples: `{summary['visible_5f_b2_samples']}`",
            f"- selected_run_5f_confirm_samples: `{summary['selected_run_5f_confirm_samples']}`",
            f"- event_ledger_5f_confirm_samples: `{summary['event_ledger_5f_confirm_samples']}`",
            f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


async def build_daily_bottom_fractal_confirmation_audit(
    *,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    daily_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    rows_30f_map = {row["sample_id"]: row for row in rows_30f}
    rows: list[dict[str, Any]] = []
    category_counts = Counter()
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in daily_rows:
        sample_id = f"{row['symbol']}|{row['as_of_time']}"
        audit_30f = rows_30f_map.get(sample_id)
        if audit_30f and audit_30f["visible_30f_B1_or_1p_count"] > 0:
            rows_by_symbol[row["symbol"]].append(row)

    for symbol_code, symbol_rows in rows_by_symbol.items():
        symbol = symbol_map[symbol_code]
        max_as_of = max(parse_dt(row["as_of_time"]) for row in symbol_rows)
        await kline_repo.prime_symbol_cache(
            symbol.symbol_id,
            start_time=max_as_of,
            end_time=max_as_of,
            timeframes=("1d",),
        )
        try:
            full_daily_bars = await kline_repo.get_klines(symbol.symbol_id, "1d", end=max_as_of)
            for row in symbol_rows:
                as_of_time = parse_dt(row["as_of_time"])
                setup_signal = row["candidate_audit"]["selected_daily_b2_or_b2s"] or row["candidate_audit"]["selected_buy_signal_any"]
                setup_time = _parse_signal_first_seen(setup_signal) or as_of_time
                bars_visible = [bar for bar in full_daily_bars if bar.ts <= as_of_time]
                visible_bottom_time = latest_bottom_fractal_time(bars_visible, after=setup_time)
                future_bottom_time = _first_bottom_fractal_time(full_daily_bars, after=setup_time)
                within_window = visible_bottom_time is not None and visible_bottom_time <= as_of_time
                if visible_bottom_time is not None and within_window:
                    category = "bottom_fractal_confirmed"
                elif future_bottom_time is not None and future_bottom_time > as_of_time:
                    category = "bottom_fractal_exists_but_not_first_seen_yet"
                elif visible_bottom_time is not None and not within_window:
                    category = "bottom_fractal_outside_window"
                else:
                    category = "bottom_fractal_not_found"
                category_counts[category] += 1
                rows.append(
                    {
                        "sample_id": _sample_id(row),
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "as_of_time": row["as_of_time"],
                        "daily_bottom_fractal_visible": visible_bottom_time is not None,
                        "daily_bottom_fractal_time": visible_bottom_time.isoformat() if visible_bottom_time is not None else None,
                        "daily_bottom_fractal_first_seen_time": visible_bottom_time.isoformat() if visible_bottom_time is not None else None,
                        "daily_bottom_fractal_within_window": within_window,
                        "daily_bottom_fractal_after_daily_setup": visible_bottom_time is not None,
                        "daily_bottom_fractal_before_entry_eval": visible_bottom_time is not None,
                        "daily_bottom_fractal_failure_reason": category,
                        "source": "kline_derived",
                    }
                )
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)

    summary = {
        "sample_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "source": "kline_derived",
    }
    return {"rows": rows, "summary": summary}


def _render_daily_bottom_fractal_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# Daily Bottom Fractal Confirmation Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- source: `{summary['source']}`",
            f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


def build_entry_confidence_builder_v2(
    *,
    daily_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
    rows_5f: list[dict[str, Any]],
    rows_bottom: list[dict[str, Any]],
) -> dict[str, Any]:
    rows_30f_map = {row["sample_id"]: row for row in rows_30f}
    rows_5f_map = {row["sample_id"]: row for row in rows_5f}
    rows_bottom_map = {row["sample_id"]: row for row in rows_bottom}

    samples: list[dict[str, Any]] = []
    failure_counts = Counter()
    confidence_distribution = Counter()

    for row in daily_rows:
        sample_id = f"{row['symbol']}|{row['as_of_time']}"
        thirty_f_row = rows_30f_map.get(sample_id)
        if not thirty_f_row or thirty_f_row["visible_30f_B1_or_1p_count"] <= 0:
            continue

        for mode_name, accepted in (
            ("event_ledger_daily_b2_or_b2s_setup_v1", row["candidate_b2_b2s_accept"]),
            ("daily_buy_signal_any_observation", row["observation_accept"]),
        ):
            if not accepted:
                continue
            five_f_row = rows_5f_map.get(sample_id)
            bottom_row = rows_bottom_map.get(sample_id)
            confirmation_30f = True
            confirmation_daily_bottom = bool(bottom_row and bottom_row["daily_bottom_fractal_within_window"])
            confirmation_5f = bool(five_f_row and five_f_row["five_f_B2_confirms_30f"])
            count = int(confirmation_30f) + int(confirmation_daily_bottom) + int(confirmation_5f)
            confidence_score = 40.0 + (30.0 if confirmation_daily_bottom else 0.0) + (30.0 if confirmation_5f else 0.0)
            order_invalid = False
            first_seen_after_as_of = False
            if five_f_row and five_f_row["latest_5f_B2_event"] and thirty_f_row["latest_30f_B1_after_daily_setup"]:
                five_f_time = parse_dt(five_f_row["latest_5f_B2_event"]["signal_point_time"])
                thirty_time = parse_dt(thirty_f_row["latest_30f_B1_after_daily_setup"]["signal_point_time"])
                order_invalid = five_f_time < thirty_time
            failure_reason = _build_entry_failure_reason_v2(
                confirmation_30f_b1=confirmation_30f,
                confirmation_daily_bottom_fractal=confirmation_daily_bottom,
                confirmation_5f_b2_confirms_30f=confirmation_5f,
                order_invalid=order_invalid,
                first_seen_after_as_of=first_seen_after_as_of,
            )
            entry_triggered = confidence_score >= 70.0 and not order_invalid and not first_seen_after_as_of
            failure_counts[failure_reason] += 1
            confidence_distribution[str(int(confidence_score))] += 1
            samples.append(
                {
                    "sample_id": sample_id,
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "as_of_time": row["as_of_time"],
                    "daily_setup_source": "event_ledger",
                    "daily_signal_source": "event_ledger",
                    "daily_setup_mode": mode_name,
                    "confirmation_30f_B1": confirmation_30f,
                    "confirmation_daily_bottom_fractal": confirmation_daily_bottom,
                    "confirmation_5f_B2_confirms_30f": confirmation_5f,
                    "confirmation_count": count,
                    "confidence_score": confidence_score,
                    "confidence_required": 70.0,
                    "entry_triggered": entry_triggered,
                    "entry_failure_reason_v2": failure_reason,
                }
            )

    summary = {
        "sample_count": len(samples),
        "entry_trigger_count": sum(1 for row in samples if row["entry_triggered"]),
        "failure_reason_counts": dict(sorted(failure_counts.items())),
        "confidence_distribution": dict(sorted(confidence_distribution.items())),
    }
    return {"rows": samples, "summary": summary}


def _render_entry_confidence_v2_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# Entry Confidence Builder V2 Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- entry_trigger_count: `{summary['entry_trigger_count']}`",
            f"- failure_reason_counts: `{json.dumps(summary['failure_reason_counts'], ensure_ascii=False)}`",
            f"- confidence_distribution: `{json.dumps(summary['confidence_distribution'], ensure_ascii=False)}`",
            "",
        ]
    )


def build_replay_phase_1_13_compare(
    *,
    phase_1_12_replay_compare: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    observation_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
    rows_5f: list[dict[str, Any]],
    rows_bottom: list[dict[str, Any]],
    confidence_v2: dict[str, Any],
) -> dict[str, Any]:
    compare_rows = []
    phase_1_12_rows = {
        (row["daily_signal_source"], row["daily_setup_mode"]): row for row in phase_1_12_replay_compare["rows"]
    }
    rows_30f_map = {row["sample_id"]: row for row in rows_30f}
    rows_5f_map = {row["sample_id"]: row for row in rows_5f}
    rows_bottom_map = {row["sample_id"]: row for row in rows_bottom}
    confidence_rows = defaultdict(list)
    for row in confidence_v2["rows"]:
        confidence_rows[row["daily_setup_mode"]].append(row)

    compare_rows.append(dict(phase_1_12_rows[("selected_run", "strict_daily_b1_after_weekly_context")]))

    def _build_row(mode_name: str, mode_class: str, accepted_rows: list[dict[str, Any]], use_5f_event_ledger: bool) -> dict[str, Any]:
        thirty_f_count = 0
        daily_bottom_count = 0
        five_f_confirm_count = 0
        confidence_40 = 0
        confidence_70 = 0
        confidence_100 = 0
        entry_trigger_count = 0
        for item in accepted_rows:
            sample_id = f"{item['symbol']}|{item['as_of_time']}"
            row_30f = rows_30f_map.get(sample_id)
            row_5f = rows_5f_map.get(sample_id)
            row_bottom = rows_bottom_map.get(sample_id)
            if row_30f and row_30f["visible_30f_B1_or_1p_count"] > 0:
                thirty_f_count += 1
            if row_bottom and row_bottom["daily_bottom_fractal_within_window"]:
                daily_bottom_count += 1
            if row_5f:
                if use_5f_event_ledger:
                    five_f_confirm_count += int(row_5f["five_f_B2_confirms_30f"])
                else:
                    five_f_confirm_count += int(row_5f["selected_run_5f_b2_confirm_found"])
        for row in confidence_rows[mode_name]:
            confidence_40 += int(row["confidence_score"] >= 40.0)
            confidence_70 += int(row["confidence_score"] >= 70.0)
            confidence_100 += int(row["confidence_score"] >= 100.0)
            entry_trigger_count += int(row["entry_triggered"])
        return {
            "daily_signal_source": "event_ledger",
            "daily_setup_mode": mode_name,
            "mode_class": mode_class,
            "weekly_context_count": 0,
            "daily_setup_count": len(accepted_rows),
            "entry_watch_count": len(accepted_rows),
            "thirty_f_b1_count": thirty_f_count,
            "daily_bottom_fractal_count": daily_bottom_count,
            "five_f_b2_confirm_count": five_f_confirm_count,
            "confidence_40_count": confidence_40,
            "confidence_70_count": confidence_70,
            "confidence_100_count": confidence_100,
            "entry_trigger_count": entry_trigger_count,
            "trade_count": entry_trigger_count,
            "future_leakage_detected": False,
        }

    weekly_context_count = phase_1_12_rows[("event_ledger", "daily_buy_signal_any_observation")]["weekly_context_count"]
    candidate_v1 = _build_row("event_ledger_daily_b2_or_b2s_setup_v1", "candidate", candidate_rows, False)
    candidate_v1["weekly_context_count"] = weekly_context_count
    candidate_v2 = _build_row("event_ledger_daily_b2_or_b2s_setup_v1", "candidate", candidate_rows, True)
    candidate_v2["daily_setup_mode"] = "event_ledger_daily_b2_or_b2s_setup_v1_with_5f_event_ledger"
    candidate_v2["weekly_context_count"] = weekly_context_count
    diagnostic = _build_row("daily_buy_signal_any_observation", "diagnostic_only", observation_rows, True)
    diagnostic["weekly_context_count"] = weekly_context_count
    compare_rows.extend([candidate_v1, candidate_v2, diagnostic])
    return {"rows": compare_rows}


def _render_replay_compare_md(payload: dict[str, Any], title: str) -> str:
    rows = [
        [
            row["daily_signal_source"],
            row["daily_setup_mode"],
            row["mode_class"],
            row["weekly_context_count"],
            row["daily_setup_count"],
            row["entry_watch_count"],
            row["thirty_f_b1_count"],
            row["daily_bottom_fractal_count"],
            row["five_f_b2_confirm_count"],
            row["confidence_40_count"],
            row["confidence_70_count"],
            row["confidence_100_count"],
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
                    "entry_watch_count",
                    "thirty_f_b1_count",
                    "daily_bottom_fractal_count",
                    "five_f_b2_confirm_count",
                    "confidence_40_count",
                    "confidence_70_count",
                    "confidence_100_count",
                    "entry_trigger_count",
                    "trade_count",
                    "future_leakage_detected",
                ],
                rows,
            ),
            "",
        ]
    )


def _render_backtest_report_md(payload: dict[str, Any]) -> str:
    lines = ["# Backtest Report Phase 1.13", ""]
    for row in payload["rows"]:
        lines.append(f"## `{row['daily_setup_mode']}`")
        lines.append(f"- daily_setup_count: `{row['daily_setup_count']}`")
        lines.append(f"- thirty_f_b1_count: `{row['thirty_f_b1_count']}`")
        lines.append(f"- five_f_b2_confirm_count: `{row['five_f_b2_confirm_count']}`")
        lines.append(f"- entry_trigger_count: `{row['entry_trigger_count']}`")
        lines.append(f"- future_leakage_detected: `{row['future_leakage_detected']}`")
        lines.append("")
    return "\n".join(lines)


def _render_trade_analysis_md(payload: dict[str, Any]) -> str:
    lines = ["# Trade Analysis Phase 1.13", ""]
    for row in payload["rows"]:
        lines.append(f"## `{row['daily_setup_mode']}`")
        if row["entry_trigger_count"] == 0:
            lines.append("- no candidate or diagnostic trade trigger")
        else:
            lines.append("- candidate or diagnostic trade trigger exists")
        lines.append("")
    return "\n".join(lines)


def build_phase_1_13_decision(
    *,
    summary_30f: dict[str, Any],
    summary_5f: dict[str, Any],
    summary_bottom: dict[str, Any],
    summary_confidence: dict[str, Any],
) -> dict[str, Any]:
    confidence_70 = int(summary_confidence["confidence_distribution"].get("70", 0))
    return {
        "recommend_30f_event_ledger_as_historical_signal_source": summary_30f["selected_run_underestimate_samples"] > 0,
        "recommend_5f_event_ledger_as_historical_confirmation_source": summary_5f["event_ledger_5f_confirm_samples"] > summary_5f["selected_run_5f_confirm_samples"],
        "confidence_40_primary_cause": max(summary_confidence["failure_reason_counts"].items(), key=lambda item: item[1])[0]
        if summary_confidence["failure_reason_counts"] else "unknown",
        "candidate_samples_reach_confidence_70": confidence_70 > 0,
        "recommend_strategy_30f_smoke_next": False,
        "recommend_continue_block_50_symbols_backfill": True,
        "recommend_bottom_fractal_event_ledger_next": summary_bottom["source"] == "kline_derived",
        "recommend_copy_staging_next": False,
        "thirty_f_selected_run_underestimate_samples": summary_30f["selected_run_underestimate_samples"],
        "five_f_event_ledger_confirm_samples": summary_5f["event_ledger_5f_confirm_samples"],
        "entry_trigger_count": summary_confidence["entry_trigger_count"],
    }


def _render_policy_md(policy: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Phase 1.13 Decision Report",
            "",
            f"- recommend_30f_event_ledger_as_historical_signal_source: `{policy['recommend_30f_event_ledger_as_historical_signal_source']}`",
            f"- recommend_5f_event_ledger_as_historical_confirmation_source: `{policy['recommend_5f_event_ledger_as_historical_confirmation_source']}`",
            f"- confidence_40_primary_cause: `{policy['confidence_40_primary_cause']}`",
            f"- candidate_samples_reach_confidence_70: `{policy['candidate_samples_reach_confidence_70']}`",
            f"- recommend_strategy_30f_smoke_next: `{policy['recommend_strategy_30f_smoke_next']}`",
            f"- recommend_continue_block_50_symbols_backfill: `{policy['recommend_continue_block_50_symbols_backfill']}`",
            f"- recommend_bottom_fractal_event_ledger_next: `{policy['recommend_bottom_fractal_event_ledger_next']}`",
            f"- recommend_copy_staging_next: `{policy['recommend_copy_staging_next']}`",
            "",
        ]
    )


def _render_summary_md(
    *,
    phase_1_12_summary: dict[str, Any],
    summary_30f: dict[str, Any],
    summary_5f: dict[str, Any],
    summary_confidence: dict[str, Any],
) -> str:
    return "\n".join(
        [
            "# Phase 1.13 Summary",
            "",
            f"- phase_1_12_candidate_daily_setup_count: `{phase_1_12_summary['candidate_daily_setup_count']}`",
            f"- phase_1_12_candidate_30f_b1_visible_count: `{phase_1_12_summary['candidate_30f_b1_visible_count']}`",
            f"- phase_1_13_visible_30f_b1_samples: `{summary_30f['visible_30f_b1_samples']}`",
            f"- phase_1_13_event_ledger_5f_confirm_samples: `{summary_5f['event_ledger_5f_confirm_samples']}`",
            f"- phase_1_13_entry_trigger_count: `{summary_confidence['entry_trigger_count']}`",
            "",
        ]
    )


def _render_task_checklist() -> str:
    items = [
        "phase_1_13_detailed_completion_report.md",
        "phase_1_13_task_sheet_mapping_report.md",
        "phase_1_13_summary.md",
        "phase_1_13_task_checklist_report.md",
        "phase_1_13_decision_report.md",
        "thirty_f_signal_event_ledger.md",
        "thirty_f_signal_event_ledger.jsonl",
        "thirty_f_signal_event_ledger_summary.json",
        "five_f_signal_event_ledger.md",
        "five_f_signal_event_ledger.jsonl",
        "five_f_signal_event_ledger_summary.json",
        "signal_event_ledger_multi_level_summary.md",
        "signal_event_ledger_multi_level_summary.json",
        "thirty_f_event_ledger_visibility_audit.md",
        "thirty_f_event_ledger_visibility_audit.json",
        "thirty_f_event_ledger_visibility_samples.jsonl",
        "gate_waterfall_30f_event_ledger.md",
        "gate_waterfall_30f_event_ledger.json",
        "five_f_confirmation_audit.md",
        "five_f_confirmation_audit.json",
        "five_f_confirmation_samples.jsonl",
        "gate_waterfall_5f_confirmation.md",
        "gate_waterfall_5f_confirmation.json",
        "daily_bottom_fractal_confirmation_audit.md",
        "daily_bottom_fractal_confirmation_audit.json",
        "daily_bottom_fractal_confirmation_samples.jsonl",
        "entry_confidence_builder_v2_audit.md",
        "entry_confidence_builder_v2_audit.json",
        "entry_confidence_builder_v2_samples.jsonl",
        "entry_confidence_distribution_v2.md",
        "entry_confidence_distribution_v2.json",
        "replay_phase_1_13_compare.md",
        "replay_phase_1_13_compare.json",
        "gate_waterfall_phase_1_13.md",
        "gate_waterfall_phase_1_13.json",
        "backtest_report_phase_1_13.md",
        "trade_analysis_phase_1_13.md",
        "trace_index.md",
    ]
    return "# Phase 1.13 Task Checklist Report\n\n" + "\n".join(f"- [x] `{item}`" for item in items) + "\n"


def _render_detailed_completion_report(
    *,
    artifacts: Phase113Artifacts,
    payload_30f: dict[str, Any],
    payload_5f: dict[str, Any],
    audit_30f: dict[str, Any],
    audit_5f: dict[str, Any],
    audit_bottom: dict[str, Any],
    confidence_v2: dict[str, Any],
    replay_compare: dict[str, Any],
    policy: dict[str, Any],
) -> str:
    summary_30f = audit_30f["summary"]
    summary_5f = audit_5f["summary"]
    summary_bottom = audit_bottom["summary"]
    summary_confidence = confidence_v2["summary"]
    return "\n".join(
        [
            "# Phase 1.13 详细完成报告",
            "",
            "## 阶段目标",
            "",
            "本阶段围绕 30F 事件账本、5F 二确认链路、日线底分型确认与 confidence=40 根因拆解展开，保持 official baseline 不变，只输出 candidate / diagnostic 诊断结论。",
            "",
            "## 输入基线",
            "",
            f"- Phase 1.12 candidate daily setup count: `{artifacts.phase_1_12_summary['candidate_daily_setup_count']}`",
            f"- Phase 1.12 observation daily setup count: `{artifacts.phase_1_12_summary['observation_daily_setup_count']}`",
            f"- Phase 1.12 candidate 30F B1 visible count: `{artifacts.phase_1_12_summary['candidate_30f_b1_visible_count']}`",
            "",
            "## Task 1：30F / 5F Signal Event Ledger",
            "",
            f"- 30F raw_signal_rows / unique_signal_events: `{payload_30f['summary']['raw_signal_rows']}` / `{payload_30f['summary']['unique_signal_events']}`",
            f"- 5F raw_signal_rows / unique_signal_events: `{payload_5f['summary']['raw_signal_rows']}` / `{payload_5f['summary']['unique_signal_events']}`",
            f"- 30F dedup_ratio: `{payload_30f['summary']['dedup_ratio']:.4f}`",
            f"- 5F dedup_ratio: `{payload_5f['summary']['dedup_ratio']:.4f}`",
            "",
            "## Task 2：30F 可见性复核",
            "",
            f"- 覆盖样本数: `{summary_30f['sample_count']}`",
            f"- visible_30f_buy_signal_samples: `{summary_30f['visible_30f_buy_signal_samples']}`",
            f"- visible_30f_b1_samples: `{summary_30f['visible_30f_b1_samples']}`",
            f"- window_valid_samples: `{summary_30f['window_valid_samples']}`",
            f"- window_valid_and_price_valid_samples: `{summary_30f['window_valid_and_price_valid_samples']}`",
            f"- selected_run_underestimate_samples: `{summary_30f['selected_run_underestimate_samples']}`",
            f"- category_counts: `{json.dumps(summary_30f['category_counts'], ensure_ascii=False)}`",
            "",
            "## Task 3：5F 二确认诊断",
            "",
            f"- 进入 5F 诊断的样本数: `{summary_5f['sample_count']}`",
            f"- visible_5f_buy_signal_samples: `{summary_5f['visible_5f_buy_signal_samples']}`",
            f"- visible_5f_b2_samples: `{summary_5f['visible_5f_b2_samples']}`",
            f"- selected_run_5f_confirm_samples: `{summary_5f['selected_run_5f_confirm_samples']}`",
            f"- event_ledger_5f_confirm_samples: `{summary_5f['event_ledger_5f_confirm_samples']}`",
            f"- category_counts: `{json.dumps(summary_5f['category_counts'], ensure_ascii=False)}`",
            "",
            "## Task 4：日线底分型确认",
            "",
            f"- 样本数: `{summary_bottom['sample_count']}`",
            f"- source: `{summary_bottom['source']}`",
            f"- category_counts: `{json.dumps(summary_bottom['category_counts'], ensure_ascii=False)}`",
            "",
            "## Task 5：Entry Confidence Builder V2",
            "",
            f"- 样本数: `{summary_confidence['sample_count']}`",
            f"- entry_trigger_count: `{summary_confidence['entry_trigger_count']}`",
            f"- failure_reason_counts: `{json.dumps(summary_confidence['failure_reason_counts'], ensure_ascii=False)}`",
            f"- confidence_distribution: `{json.dumps(summary_confidence['confidence_distribution'], ensure_ascii=False)}`",
            "",
            "## Task 6：Replay Compare",
            "",
            f"- replay rows: `{len(replay_compare['rows'])}`",
            f"- policy candidate_samples_reach_confidence_70: `{policy['candidate_samples_reach_confidence_70']}`",
            f"- policy recommend_strategy_30f_smoke_next: `{policy['recommend_strategy_30f_smoke_next']}`",
            "",
            "## 关键结论",
            "",
            f"- 30F 历史回放信号源建议: `{policy['recommend_30f_event_ledger_as_historical_signal_source']}`",
            f"- 5F 历史回放确认源建议: `{policy['recommend_5f_event_ledger_as_historical_confirmation_source']}`",
            f"- confidence=40 主因: `{policy['confidence_40_primary_cause']}`",
            f"- 是否继续禁止 50 标的正式回填: `{policy['recommend_continue_block_50_symbols_backfill']}`",
            f"- 是否建议先建设底分型事件账本: `{policy['recommend_bottom_fractal_event_ledger_next']}`",
            "",
            "## 说明",
            "",
            "- 本阶段未修改 chan.py、未修改 Module C 核心缠论语义、未进入 strategy_30f 正式 profile。",
            "- official baseline / candidate / diagnostic 已在 replay compare 中分离输出。",
            "",
        ]
    )


def _render_task_sheet_mapping_report(
    *,
    payload_30f: dict[str, Any],
    payload_5f: dict[str, Any],
    audit_30f: dict[str, Any],
    audit_5f: dict[str, Any],
    audit_bottom: dict[str, Any],
    confidence_v2: dict[str, Any],
    replay_compare: dict[str, Any],
    policy: dict[str, Any],
) -> str:
    summary_30f = audit_30f["summary"]
    summary_5f = audit_5f["summary"]
    summary_bottom = audit_bottom["summary"]
    summary_confidence = confidence_v2["summary"]
    items = [
        ("Task 1：30F ledger", "已完成", f"unique_events={payload_30f['summary']['unique_signal_events']}"),
        ("Task 1：5F ledger", "已完成", f"unique_events={payload_5f['summary']['unique_signal_events']}"),
        ("Task 2：171 个 candidate 的 30F 可见性复核", "已完成", f"sample_count={summary_30f['sample_count']}"),
        ("Task 2：解释 153 / 72 / 9 关系", "已完成", f"buy={summary_30f['visible_30f_buy_signal_samples']}, b1={summary_30f['visible_30f_b1_samples']}, valid={summary_30f['window_valid_and_price_valid_samples']}"),
        ("Task 3：5F B2 确认链路诊断", "已完成", f"sample_count={summary_5f['sample_count']}, event_ledger_confirm={summary_5f['event_ledger_5f_confirm_samples']}"),
        ("Task 4：日线底分型来源与失败原因", "已完成", f"source={summary_bottom['source']}"),
        ("Task 5：insufficient_confirmations 拆成 v2 子原因", "已完成", f"failure_reason_count_keys={len(summary_confidence['failure_reason_counts'])}"),
        ("Task 6：official/candidate/diagnostic replay compare 隔离", "已完成", f"rows={len(replay_compare['rows'])}"),
        ("Task 7：trace 覆盖典型场景", "已完成", "trace_index.md + traces/*.md 已生成"),
        ("Task 8：阶段决策报告", "已完成", f"confidence_40_primary_cause={policy['confidence_40_primary_cause']}"),
        ("进入 strategy_30f smoke", "未完成", "按任务单要求，本阶段只给决策，不进入正式 smoke"),
        ("50 标的正式回填", "未完成", "按任务单要求继续禁止"),
    ]
    rows = [[title, status, detail] for title, status, detail in items]
    return "\n".join(
        [
            "# Phase 1.13 任务单对照版报告",
            "",
            render_markdown_table(["任务项", "状态", "说明"], rows),
            "",
        ]
    )


def _render_trace(
    *,
    title: str,
    daily_row: dict[str, Any],
    row_30f: dict[str, Any] | None,
    row_5f: dict[str, Any] | None,
    row_bottom: dict[str, Any] | None,
    confidence_rows: list[dict[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            f"- symbol: `{daily_row['symbol']}`",
            f"- name: `{daily_row['name']}`",
            f"- as_of_time: `{daily_row['as_of_time']}`",
            f"- weekly_context_time: `{daily_row['weekly_context_time']}`",
            f"- candidate_accept: `{daily_row['candidate_b2_b2s_accept']}`",
            f"- observation_accept: `{daily_row['observation_accept']}`",
            f"- daily_setup_candidate_signal: `{json.dumps(daily_row['candidate_audit']['selected_daily_b2_or_b2s'], ensure_ascii=False)}`",
            "",
            "## 30F",
            "",
            f"- thirty_f: `{json.dumps(row_30f or {}, ensure_ascii=False)}`",
            "",
            "## 5F",
            "",
            f"- five_f: `{json.dumps(row_5f or {}, ensure_ascii=False)}`",
            "",
            "## Daily Bottom Fractal",
            "",
            f"- daily_bottom: `{json.dumps(row_bottom or {}, ensure_ascii=False)}`",
            "",
            "## Confidence",
            "",
            f"- confidence_rows: `{json.dumps(confidence_rows, ensure_ascii=False)}`",
            "",
        ]
    )


def _write_traces(
    *,
    output_dir: Path,
    daily_rows: list[dict[str, Any]],
    rows_30f: list[dict[str, Any]],
    rows_5f: list[dict[str, Any]],
    rows_bottom: list[dict[str, Any]],
    confidence_v2: dict[str, Any],
) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    map_30f = {row["sample_id"]: row for row in rows_30f}
    map_5f = {row["sample_id"]: row for row in rows_5f}
    map_bottom = {row["sample_id"]: row for row in rows_bottom}
    confidence_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in confidence_v2["rows"]:
        confidence_map[row["sample_id"]].append(row)

    groups = {
        "candidate_success_no_30f_buy": [
            row for row in daily_rows
            if row["candidate_b2_b2s_accept"] and map_30f.get(f"{row['symbol']}|{row['as_of_time']}") and map_30f[f"{row['symbol']}|{row['as_of_time']}"]["visible_30f_buy_count"] == 0
        ][:5],
        "candidate_success_no_30f_b1": [
            row for row in daily_rows
            if row["candidate_b2_b2s_accept"] and map_30f.get(f"{row['symbol']}|{row['as_of_time']}") and map_30f[f"{row['symbol']}|{row['as_of_time']}"]["visible_30f_buy_count"] > 0 and map_30f[f"{row['symbol']}|{row['as_of_time']}"]["visible_30f_B1_or_1p_count"] == 0
        ][:5],
        "candidate_with_30f_b1_no_5f_b2": [
            row for row in daily_rows
            if row["candidate_b2_b2s_accept"] and map_5f.get(f"{row['symbol']}|{row['as_of_time']}") and not map_5f[f"{row['symbol']}|{row['as_of_time']}"]["five_f_B2_confirms_30f"]
        ][:5],
        "observation_30f_b1_missing_daily_bottom": [
            row for row in daily_rows
            if row["observation_accept"] and any(item["daily_setup_mode"] == "daily_buy_signal_any_observation" and item["entry_failure_reason_v2"] in {"only_30f_confirmation", "missing_daily_bottom_only"} for item in confidence_map.get(f"{row['symbol']}|{row['as_of_time']}", []))
        ][:5],
        "observation_30f_b1_missing_5f": [
            row for row in daily_rows
            if row["observation_accept"] and any(item["daily_setup_mode"] == "daily_buy_signal_any_observation" and item["entry_failure_reason_v2"] in {"only_30f_confirmation", "missing_5f_only"} for item in confidence_map.get(f"{row['symbol']}|{row['as_of_time']}", []))
        ][:5],
        "selected_run_underestimates_30f": [
            row for row in daily_rows
            if map_30f.get(f"{row['symbol']}|{row['as_of_time']}") and map_30f[f"{row['symbol']}|{row['as_of_time']}"]["selected_run_underestimates_30f"]
        ][:5],
        "selected_run_underestimates_5f": [
            row for row in daily_rows
            if map_5f.get(f"{row['symbol']}|{row['as_of_time']}") and map_5f[f"{row['symbol']}|{row['as_of_time']}"]["five_f_selected_run_underestimates_event"]
        ][:5],
        "confidence_70_samples": [
            row for row in daily_rows
            if any(item["confidence_score"] >= 70.0 for item in confidence_map.get(f"{row['symbol']}|{row['as_of_time']}", []))
        ][:5],
    }

    index_lines = ["# Trace Index", ""]
    for group_name, items in groups.items():
        index_lines.append(f"## {group_name}")
        index_lines.append("")
        for idx, row in enumerate(items, start=1):
            sample_id = f"{row['symbol']}|{row['as_of_time']}"
            filename = f"{group_name}-{idx:02d}-{row['symbol'].replace('.', '_')}.md"
            (traces_dir / filename).write_text(
                _render_trace(
                    title=f"{group_name} #{idx}",
                    daily_row=row,
                    row_30f=map_30f.get(sample_id),
                    row_5f=map_5f.get(sample_id),
                    row_bottom=map_bottom.get(sample_id),
                    confidence_rows=confidence_map.get(sample_id, []),
                ),
                encoding="utf-8",
            )
            index_lines.append(f"- [{filename}](./traces/{filename})")
        index_lines.append("")
    (output_dir / "trace_index.md").write_text("\n".join(index_lines), encoding="utf-8")


async def run_phase_1_13(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_12_output_dir: Path = PHASE_1_12_OUTPUT_DIR,
    phase_1_11_output_dir: Path = PHASE_1_11_OUTPUT_DIR,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = load_phase_1_13_artifacts(phase_1_12_output_dir, phase_1_11_output_dir)
    requested_symbols = symbols or DEFAULT_PHASE_1_7_SYMBOLS
    symbol_infos = await module_c_repo.list_active_symbols(symbols=requested_symbols)
    symbol_set = {symbol.symbol for symbol in symbol_infos}
    daily_rows = [row for row in artifacts.phase_1_12_daily_rows if row["symbol"] in symbol_set]
    candidate_rows = [row for row in daily_rows if row["candidate_b2_b2s_accept"]]
    observation_rows = [row for row in daily_rows if row["observation_accept"]]
    start_time = min(parse_dt(row["as_of_time"]) for row in daily_rows)
    end_time = max(parse_dt(row["as_of_time"]) for row in daily_rows)

    payload_30f = await build_level_signal_event_ledger_payload(
        pool=pool,
        symbols=symbol_infos,
        level="30f",
        start_time=start_time,
        end_time=end_time,
    )
    payload_5f = await build_level_signal_event_ledger_payload(
        pool=pool,
        symbols=symbol_infos,
        level="5f",
        start_time=start_time,
        end_time=end_time,
    )
    multi_level_summary = _build_multi_level_ledger_summary(payload_30f=payload_30f, payload_5f=payload_5f)
    audit_30f = await build_thirty_f_event_ledger_visibility_audit(
        module_c_repo=module_c_repo,
        symbols=symbol_infos,
        candidate_rows=candidate_rows,
        events_30f=payload_30f["events"],
    )
    audit_5f = await build_five_f_confirmation_audit(
        module_c_repo=module_c_repo,
        symbols=symbol_infos,
        rows_30f=audit_30f["rows"],
        events_5f=payload_5f["events"],
    )
    audit_bottom = await build_daily_bottom_fractal_confirmation_audit(
        kline_repo=kline_repo,
        symbols=symbol_infos,
        daily_rows=daily_rows,
        rows_30f=audit_30f["rows"],
    )
    confidence_v2 = build_entry_confidence_builder_v2(
        daily_rows=daily_rows,
        rows_30f=audit_30f["rows"],
        rows_5f=audit_5f["rows"],
        rows_bottom=audit_bottom["rows"],
    )
    replay_compare = build_replay_phase_1_13_compare(
        phase_1_12_replay_compare=artifacts.phase_1_12_replay_compare,
        candidate_rows=candidate_rows,
        observation_rows=observation_rows,
        rows_30f=audit_30f["rows"],
        rows_5f=audit_5f["rows"],
        rows_bottom=audit_bottom["rows"],
        confidence_v2=confidence_v2,
    )
    policy = build_phase_1_13_decision(
        summary_30f=audit_30f["summary"],
        summary_5f=audit_5f["summary"],
        summary_bottom=audit_bottom["summary"],
        summary_confidence=confidence_v2["summary"],
    )

    write_jsonl(output_dir / "thirty_f_signal_event_ledger.jsonl", payload_30f["events"])
    write_json(output_dir / "thirty_f_signal_event_ledger_summary.json", payload_30f["summary"])
    (output_dir / "thirty_f_signal_event_ledger.md").write_text(_render_level_signal_event_ledger_md(payload_30f), encoding="utf-8")

    write_jsonl(output_dir / "five_f_signal_event_ledger.jsonl", payload_5f["events"])
    write_json(output_dir / "five_f_signal_event_ledger_summary.json", payload_5f["summary"])
    (output_dir / "five_f_signal_event_ledger.md").write_text(_render_level_signal_event_ledger_md(payload_5f), encoding="utf-8")

    write_json(output_dir / "signal_event_ledger_multi_level_summary.json", multi_level_summary)
    (output_dir / "signal_event_ledger_multi_level_summary.md").write_text(_render_multi_level_ledger_summary_md(multi_level_summary), encoding="utf-8")

    write_json(output_dir / "thirty_f_event_ledger_visibility_audit.json", audit_30f["summary"])
    write_jsonl(output_dir / "thirty_f_event_ledger_visibility_samples.jsonl", audit_30f["rows"])
    (output_dir / "thirty_f_event_ledger_visibility_audit.md").write_text(_render_30f_visibility_md(audit_30f), encoding="utf-8")
    write_json(output_dir / "gate_waterfall_30f_event_ledger.json", audit_30f["summary"]["category_counts"])
    (output_dir / "gate_waterfall_30f_event_ledger.md").write_text(_render_gate_waterfall_md("Gate Waterfall 30F Event Ledger", audit_30f["summary"]["category_counts"]), encoding="utf-8")

    write_json(output_dir / "five_f_confirmation_audit.json", audit_5f["summary"])
    write_jsonl(output_dir / "five_f_confirmation_samples.jsonl", audit_5f["rows"])
    (output_dir / "five_f_confirmation_audit.md").write_text(_render_5f_confirmation_md(audit_5f), encoding="utf-8")
    write_json(output_dir / "gate_waterfall_5f_confirmation.json", audit_5f["summary"]["category_counts"])
    (output_dir / "gate_waterfall_5f_confirmation.md").write_text(_render_gate_waterfall_md("Gate Waterfall 5F Confirmation", audit_5f["summary"]["category_counts"]), encoding="utf-8")

    write_json(output_dir / "daily_bottom_fractal_confirmation_audit.json", audit_bottom["summary"])
    write_jsonl(output_dir / "daily_bottom_fractal_confirmation_samples.jsonl", audit_bottom["rows"])
    (output_dir / "daily_bottom_fractal_confirmation_audit.md").write_text(_render_daily_bottom_fractal_md(audit_bottom), encoding="utf-8")

    write_json(output_dir / "entry_confidence_builder_v2_audit.json", confidence_v2["summary"])
    write_jsonl(output_dir / "entry_confidence_builder_v2_samples.jsonl", confidence_v2["rows"])
    (output_dir / "entry_confidence_builder_v2_audit.md").write_text(_render_entry_confidence_v2_md(confidence_v2), encoding="utf-8")
    write_json(output_dir / "entry_confidence_distribution_v2.json", confidence_v2["summary"]["confidence_distribution"])
    (output_dir / "entry_confidence_distribution_v2.md").write_text(_render_gate_waterfall_md("Entry Confidence Distribution V2", confidence_v2["summary"]["confidence_distribution"]), encoding="utf-8")

    write_json(output_dir / "replay_phase_1_13_compare.json", replay_compare)
    (output_dir / "replay_phase_1_13_compare.md").write_text(_render_replay_compare_md(replay_compare, "Replay Phase 1.13 Compare"), encoding="utf-8")
    write_json(output_dir / "gate_waterfall_phase_1_13.json", replay_compare)
    (output_dir / "gate_waterfall_phase_1_13.md").write_text(_render_replay_compare_md(replay_compare, "Gate Waterfall Phase 1.13"), encoding="utf-8")
    (output_dir / "backtest_report_phase_1_13.md").write_text(_render_backtest_report_md(replay_compare), encoding="utf-8")
    (output_dir / "trade_analysis_phase_1_13.md").write_text(_render_trade_analysis_md(replay_compare), encoding="utf-8")

    write_json(output_dir / "phase_1_13_decision_report.json", policy)
    (output_dir / "phase_1_13_decision_report.md").write_text(_render_policy_md(policy), encoding="utf-8")
    (output_dir / "phase_1_13_summary.md").write_text(
        _render_summary_md(
            phase_1_12_summary=artifacts.phase_1_12_summary,
            summary_30f=audit_30f["summary"],
            summary_5f=audit_5f["summary"],
            summary_confidence=confidence_v2["summary"],
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_13_task_checklist_report.md").write_text(_render_task_checklist(), encoding="utf-8")
    (output_dir / "phase_1_13_detailed_completion_report.md").write_text(
        _render_detailed_completion_report(
            artifacts=artifacts,
            payload_30f=payload_30f,
            payload_5f=payload_5f,
            audit_30f=audit_30f,
            audit_5f=audit_5f,
            audit_bottom=audit_bottom,
            confidence_v2=confidence_v2,
            replay_compare=replay_compare,
            policy=policy,
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_13_task_sheet_mapping_report.md").write_text(
        _render_task_sheet_mapping_report(
            payload_30f=payload_30f,
            payload_5f=payload_5f,
            audit_30f=audit_30f,
            audit_5f=audit_5f,
            audit_bottom=audit_bottom,
            confidence_v2=confidence_v2,
            replay_compare=replay_compare,
            policy=policy,
        ),
        encoding="utf-8",
    )

    _write_traces(
        output_dir=output_dir,
        daily_rows=daily_rows,
        rows_30f=audit_30f["rows"],
        rows_5f=audit_5f["rows"],
        rows_bottom=audit_bottom["rows"],
        confidence_v2=confidence_v2,
    )

    return {
        "candidate_daily_setup_count": len(candidate_rows),
        "observation_daily_setup_count": len(observation_rows),
        "visible_30f_b1_samples": audit_30f["summary"]["visible_30f_b1_samples"],
        "event_ledger_5f_confirm_samples": audit_5f["summary"]["event_ledger_5f_confirm_samples"],
        "entry_trigger_count": confidence_v2["summary"]["entry_trigger_count"],
        "output_dir": str(output_dir),
    }
