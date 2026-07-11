from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.config.strategy_params import PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE, StrategyParams
from app.domain.enums import LEVEL_TO_DB
from app.domain.models import ChanSignal, SymbolInfo
from app.engine.phase_1_11 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_11_OUTPUT_DIR,
    HISTORICAL_RUN_GROUP,
    HISTORICAL_RUN_KIND,
    _build_weekly_context_stub,
    _event_to_signal,
    build_signal_fingerprint,
    parse_dt,
    read_jsonl,
    render_markdown_table,
    serialize_value,
    write_json,
    write_jsonl,
)
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-12-daily-setup-decision"
DAILY_LEVEL = LEVEL_TO_DB["1d"]

DAILY_SETUP_COMPARE_MODES = (
    ("selected_run", "strict_daily_b1_after_weekly_context", "official"),
    ("event_ledger", "strict_daily_b1_after_weekly_context", "official"),
    ("event_ledger", "event_ledger_daily_b2_or_b2s_setup_v1", "candidate"),
    ("event_ledger", "daily_b2_or_b2s_with_b1_score", "candidate"),
    ("event_ledger", "daily_buy_signal_any_observation", "diagnostic_only"),
)
THIRTY_F_WINDOW_DAYS = (5, 10, 20)


@dataclass(slots=True)
class Phase112Artifacts:
    daily_event_ledger: list[dict[str, Any]]
    daily_setup_source_compare: list[dict[str, Any]]
    weekly_context_samples: list[dict[str, Any]]
    replay_after_fix: dict[str, Any]
    phase_1_11_summary: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any] | list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_phase_1_11_artifacts(output_dir: Path = PHASE_1_11_OUTPUT_DIR) -> Phase112Artifacts:
    return Phase112Artifacts(
        daily_event_ledger=read_jsonl(output_dir / "daily_signal_event_ledger.jsonl"),
        daily_setup_source_compare=list(_read_json(output_dir / "daily_setup_source_compare.json")),
        weekly_context_samples=read_jsonl(output_dir / "weekly_context_daily_event_visibility_samples.jsonl"),
        replay_after_fix=dict(_read_json(output_dir / "replay_after_signal_visibility_fix_audit.json")),
        phase_1_11_summary=dict(_read_json(output_dir / "phase_1_11_summary.json")),
    )


async def build_signal_event_ledger(
    *,
    pool: asyncpg.Pool,
    symbols: list[SymbolInfo],
    level: str,
    start_time: datetime,
    end_time: datetime,
) -> list[dict[str, Any]]:
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
    for row in rows:
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

    return sorted(
        ledger.values(),
        key=lambda item: (item["symbol"], item["signal_point_time"], item["price_x1000"]),
    )


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


def _event_payload(event: dict[str, Any] | None) -> dict[str, Any] | None:
    if event is None:
        return None
    return {
        "signal_point_time": event["signal_point_time"],
        "first_seen_time": event["first_seen_time"],
        "price": int(event["price_x1000"]) / 1000,
        "bsp_type": event["bsp_type"],
        "observed_run_count": event["observed_run_count"],
    }


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


def _strict_failure_reason(audit: Any) -> str:
    if audit.daily_setup_accepted_by_mode:
        return "accepted"
    if not audit.daily_signal_any_found:
        return "no_visible_daily_buy_signal_after_weekly_context"
    if audit.daily_b2_or_b2s_found and not audit.daily_b1_found:
        return "has_daily_B2_or_B2s_but_no_daily_B1_after_context"
    if not audit.daily_b1_found:
        return "no_daily_B1_after_weekly_context"
    return "strict_mode_rejected"


def _candidate_failure_reason(audit: Any) -> str:
    if audit.daily_setup_accepted_by_mode:
        return "accepted"
    if not audit.daily_signal_any_found:
        return "no_visible_daily_buy_signal_after_weekly_context"
    if not audit.daily_b2_or_b2s_found:
        return "no_daily_B2_or_B2s_after_weekly_context"
    return "candidate_mode_rejected"


async def build_daily_setup_decision_dataset(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    daily_events: list[dict[str, Any]],
    weekly_context_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in daily_events:
        events_by_symbol[event["symbol"]].append(event)
    samples_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in weekly_context_samples:
        samples_by_symbol[sample["symbol"]].append(sample)

    base_params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
    params_strict = base_params.with_overrides(
        daily_setup_mode="strict_daily_b1_after_weekly_context",
        daily_signal_source="event_ledger",
    )
    params_candidate = base_params.with_overrides(
        daily_setup_mode="true_trust_daily_b2_or_b2s",
        daily_signal_source="event_ledger",
    )
    params_scored = base_params.with_overrides(
        daily_setup_mode="daily_b2_or_b2s_with_b1_score",
        daily_signal_source="event_ledger",
    )
    params_observation = base_params.with_overrides(
        daily_setup_mode="daily_buy_signal_any_observation",
        daily_signal_source="event_ledger",
    )

    sample_rows: list[dict[str, Any]] = []
    strict_failures = Counter()
    candidate_signal_types = Counter()
    observation_signal_types = Counter()
    observation_non_b2b2s = 0
    candidate_count = 0
    scored_count = 0
    observation_count = 0
    visible_signal_counts = Counter()

    for symbol_code, symbol_samples in samples_by_symbol.items():
        symbol = symbol_map[symbol_code]
        max_as_of = max(parse_dt(sample["as_of_time"]) for sample in symbol_samples)
        await kline_repo.prime_symbol_cache(
            symbol.symbol_id,
            start_time=max_as_of,
            end_time=max_as_of,
            timeframes=("1d",),
        )
        try:
            candidate_events = events_by_symbol[symbol.symbol]
            for sample in symbol_samples:
                as_of_time = parse_dt(sample["as_of_time"])
                weekly_context_time = parse_dt(sample["weekly_context_time"])
                weekly_context = _build_weekly_context_stub(
                    {
                        "weekly_context_signal_time": sample["weekly_context_time"],
                        "weekly_bsp_type": "2",
                        "weekly_context_price": (
                            (sample.get("nearest_ledger_buy_in_weekly_context_window") or {}).get("price", 0)
                        ),
                    }
                )
                daily_bars = await kline_repo.get_klines(symbol.symbol_id, "1d", end=as_of_time)
                day_order = [bar.ts for bar in daily_bars]
                visible_events = [
                    event
                    for event in candidate_events
                    if parse_dt(event["first_seen_time"]) <= as_of_time and parse_dt(event["signal_point_time"]) <= as_of_time
                ]
                visible_signals = [_event_to_signal(event) for event in visible_events]
                strict_audit = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=visible_signals,
                    weekly_context=weekly_context,
                    as_of_time=as_of_time,
                    params=params_strict,
                    daily_bars=daily_bars,
                )
                candidate_audit = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=visible_signals,
                    weekly_context=weekly_context,
                    as_of_time=as_of_time,
                    params=params_candidate,
                    daily_bars=daily_bars,
                )
                scored_audit = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=visible_signals,
                    weekly_context=weekly_context,
                    as_of_time=as_of_time,
                    params=params_scored,
                    daily_bars=daily_bars,
                )
                observation_audit = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=visible_signals,
                    weekly_context=weekly_context,
                    as_of_time=as_of_time,
                    params=params_observation,
                    daily_bars=daily_bars,
                )

                nearest_b1_before_context = _nearest_event(
                    visible_events,
                    lambda item: item["bsp_type"] == "1" and parse_dt(item["signal_point_time"]) < weekly_context_time,
                )
                nearest_b1_after_context = _nearest_event(
                    visible_events,
                    lambda item: item["bsp_type"] == "1" and parse_dt(item["signal_point_time"]) >= weekly_context_time,
                )
                nearest_b2_before_context = _nearest_event(
                    visible_events,
                    lambda item: item["bsp_type"] in {"2", "2s"} and parse_dt(item["signal_point_time"]) < weekly_context_time,
                )
                nearest_b2_after_context = _nearest_event(
                    visible_events,
                    lambda item: item["bsp_type"] in {"2", "2s"} and parse_dt(item["signal_point_time"]) >= weekly_context_time,
                )
                visible_by_bsp = Counter(str(event["bsp_type"]) for event in visible_events)
                strict_reason = _strict_failure_reason(strict_audit)
                candidate_reason = _candidate_failure_reason(candidate_audit)
                strict_failures[strict_reason] += 1
                for key, value in visible_by_bsp.items():
                    visible_signal_counts[key] += value

                if candidate_audit.daily_setup_accepted_by_mode:
                    candidate_count += 1
                    candidate_signal_types[str(candidate_audit.selected_signal_kind)] += 1
                if scored_audit.daily_setup_accepted_by_mode:
                    scored_count += 1
                if observation_audit.daily_setup_accepted_by_mode:
                    observation_count += 1
                    observation_signal_types[str(observation_audit.selected_signal_kind)] += 1
                    if observation_audit.selected_signal_kind not in {"2", "2s"}:
                        observation_non_b2b2s += 1

                sample_rows.append(
                    {
                        "symbol": sample["symbol"],
                        "name": symbol.name,
                        "as_of_time": sample["as_of_time"],
                        "weekly_context_time": sample["weekly_context_time"],
                        "daily_signal_source": "event_ledger",
                        "daily_events_visible_count": len(visible_events),
                        "daily_events_visible_by_bsp_type": dict(sorted(visible_by_bsp.items())),
                        "nearest_daily_B1_before_context": _event_payload(nearest_b1_before_context),
                        "nearest_daily_B1_after_context": _event_payload(nearest_b1_after_context),
                        "nearest_daily_B2_or_B2s_before_context": _event_payload(nearest_b2_before_context),
                        "nearest_daily_B2_or_B2s_after_context": _event_payload(nearest_b2_after_context),
                        "strict_accept": strict_audit.daily_setup_accepted_by_mode,
                        "candidate_b2_b2s_accept": candidate_audit.daily_setup_accepted_by_mode,
                        "observation_accept": observation_audit.daily_setup_accepted_by_mode,
                        "daily_setup_score": scored_audit.selected_signal_score,
                        "failure_reason_strict": strict_reason,
                        "failure_reason_candidate": candidate_reason,
                        "strict_audit": serialize_value(strict_audit),
                        "candidate_audit": serialize_value(candidate_audit),
                        "scored_audit": serialize_value(scored_audit),
                        "observation_audit": serialize_value(observation_audit),
                        "day_order": [item.isoformat() for item in day_order],
                    }
                )
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)

    observation_non_b2_ratio = observation_non_b2b2s / observation_count if observation_count else 0.0
    summary = {
        "sample_count": len(sample_rows),
        "strict_daily_setup_count": sum(1 for row in sample_rows if row["strict_accept"]),
        "candidate_daily_setup_count": candidate_count,
        "scored_daily_setup_count": scored_count,
        "observation_daily_setup_count": observation_count,
        "strict_failure_reason_counts": dict(sorted(strict_failures.items())),
        "candidate_signal_kind_counts": dict(sorted(candidate_signal_types.items())),
        "observation_signal_kind_counts": dict(sorted(observation_signal_types.items())),
        "observation_non_b2_b2s_count": observation_non_b2b2s,
        "observation_non_b2_b2s_ratio": observation_non_b2_ratio,
        "visible_daily_event_counts_by_bsp_type": dict(sorted(visible_signal_counts.items())),
    }
    return {"rows": sample_rows, "summary": summary}


async def build_thirty_f_downstream_audit(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    candidate_rows: list[dict[str, Any]],
    events_30f: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events_30f:
        events_by_symbol[event["symbol"]].append(event)

    samples: list[dict[str, Any]] = []
    category_counts = Counter()
    window_counts = {window: Counter() for window in THIRTY_F_WINDOW_DAYS}
    selected_run_underestimate = 0

    for row in candidate_rows:
        symbol = symbol_map[row["symbol"]]
        as_of_time = parse_dt(row["as_of_time"])
        daily_setup_first_seen_time = parse_dt(
            row["candidate_audit"]["selected_daily_b2_or_b2s"]["features"]["first_seen_time"]
            if row["candidate_audit"].get("selected_daily_b2_or_b2s")
            and row["candidate_audit"]["selected_daily_b2_or_b2s"].get("features", {}).get("first_seen_time")
            else row["candidate_audit"]["selected_buy_signal_any"]["features"]["first_seen_time"]
            if row["candidate_audit"].get("selected_buy_signal_any")
            and row["candidate_audit"]["selected_buy_signal_any"].get("features", {}).get("first_seen_time")
            else row["candidate_audit"]["selected_daily_b1"]["features"]["first_seen_time"]
            if row["candidate_audit"].get("selected_daily_b1")
            and row["candidate_audit"]["selected_daily_b1"].get("features", {}).get("first_seen_time")
            else row["as_of_time"]
        )
        visible_events = [
            event
            for event in events_by_symbol[symbol.symbol]
            if parse_dt(event["first_seen_time"]) <= as_of_time and parse_dt(event["signal_point_time"]) <= as_of_time
        ]
        visible_buy_events = list(visible_events)
        visible_b1_events = [
            event for event in visible_events if event["bsp_type"] in {"1", "1p"}
        ]
        b1_after_setup = [
            event for event in visible_b1_events if parse_dt(event["signal_point_time"]) >= daily_setup_first_seen_time
        ]
        latest_b1_after_setup = b1_after_setup[-1] if b1_after_setup else None
        day_order = [parse_dt(item) for item in row["day_order"]]
        window_flags = _window_membership(
            day_order,
            parse_dt(latest_b1_after_setup["signal_point_time"]) if latest_b1_after_setup else None,
            daily_setup_first_seen_time,
        )
        selected_30f_signals = await module_c_repo.get_signals(
            symbol.symbol_id,
            "30f",
            mode="predictive",
            as_of_time=as_of_time,
            run_kind=HISTORICAL_RUN_KIND,
            run_group_id=HISTORICAL_RUN_GROUP,
            allow_legacy_mode_fallback=False,
        )
        selected_b1_signals = [
            signal for signal in selected_30f_signals if signal.side == "buy" and signal.bsp_type in {"1", "1p"}
        ]

        if not visible_buy_events:
            category = "no_30f_buy_signal_visible"
        elif not visible_b1_events:
            category = "has_30f_buy_but_no_B1_or_1p"
        elif not b1_after_setup:
            category = "has_30f_B1_before_daily_setup_only"
        elif not window_flags[20]:
            category = "has_30f_B1_after_daily_setup_but_outside_window"
        else:
            category = "has_30f_B1_in_window_and_valid"
        if visible_b1_events and not selected_b1_signals:
            category = "filtered_by_selected_run_only"
            selected_run_underestimate += 1
        category_counts[category] += 1
        for window in THIRTY_F_WINDOW_DAYS:
            if window_flags[window]:
                window_counts[window][category] += 1

        samples.append(
            {
                "symbol": row["symbol"],
                "name": row["name"],
                "as_of_time": row["as_of_time"],
                "daily_setup_first_seen_time": daily_setup_first_seen_time.isoformat(),
                "daily_setup_signal_kind": row["candidate_audit"]["selected_signal_kind"],
                "visible_30f_buy_count": len(visible_buy_events),
                "visible_30f_b1_or_1p_count": len(visible_b1_events),
                "selected_run_30f_buy_count": sum(1 for signal in selected_30f_signals if signal.side == "buy"),
                "selected_run_30f_b1_or_1p_count": len(selected_b1_signals),
                "latest_30f_b1_after_setup": _event_payload(latest_b1_after_setup),
                "latest_30f_b1_before_setup": _event_payload(
                    _nearest_event(
                        visible_b1_events,
                        lambda event: parse_dt(event["signal_point_time"]) < daily_setup_first_seen_time,
                    )
                ),
                "window_flags": window_flags,
                "category": category,
            }
        )

    summary = {
        "sample_count": len(candidate_rows),
        "visible_30f_buy_signal_samples": sum(1 for row in samples if row["visible_30f_buy_count"] > 0),
        "visible_30f_b1_samples": sum(1 for row in samples if row["visible_30f_b1_or_1p_count"] > 0),
        "selected_run_underestimate_samples": selected_run_underestimate,
        "category_counts": dict(sorted(category_counts.items())),
        "window_category_counts": {str(window): dict(sorted(counter.items())) for window, counter in window_counts.items()},
        "recommend_30f_event_ledger_design_next": selected_run_underestimate > 0,
    }
    return {"rows": samples, "summary": summary}


async def build_entry_trigger_diagnosis(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    observation_rows: list[dict[str, Any]],
    events_30f: list[dict[str, Any]],
    events_5f: list[dict[str, Any]],
) -> dict[str, Any]:
    from app.analyzers.fractal_detector import latest_bottom_fractal_time

    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_30f_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    events_5f_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events_30f:
        events_30f_by_symbol[event["symbol"]].append(event)
    for event in events_5f:
        events_5f_by_symbol[event["symbol"]].append(event)

    samples: list[dict[str, Any]] = []
    failure_counts = Counter()
    confidence_distribution = Counter()
    confidence_gate_counts = Counter()
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observation_rows:
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
            daily_bars_all = await kline_repo.get_klines(symbol.symbol_id, "1d", end=max_as_of)
            for row in symbol_rows:
                as_of_time = parse_dt(row["as_of_time"])
                day_order = [parse_dt(item) for item in row["day_order"]]
                daily_bars = [bar for bar in daily_bars_all if bar.ts <= as_of_time]
                observation_signal = row["observation_audit"]["selected_buy_signal_any"]
                if observation_signal is None:
                    continue
                anchor_time = parse_dt(observation_signal["point_time"])
                visible_30f_b1_events = [
                    event
                    for event in events_30f_by_symbol[symbol.symbol]
                    if parse_dt(event["first_seen_time"]) <= as_of_time
                    and parse_dt(event["signal_point_time"]) <= as_of_time
                    and parse_dt(event["signal_point_time"]) >= anchor_time
                    and event["bsp_type"] in {"1", "1p"}
                ]
                if not visible_30f_b1_events:
                    continue
                thirty_b1 = visible_30f_b1_events[-1]
                thirty_b1_time = parse_dt(thirty_b1["signal_point_time"])
                visible_5f_b2_events = [
                    event
                    for event in events_5f_by_symbol[symbol.symbol]
                    if parse_dt(event["first_seen_time"]) <= as_of_time
                    and parse_dt(event["signal_point_time"]) <= as_of_time
                    and parse_dt(event["signal_point_time"]) >= thirty_b1_time
                    and event["bsp_type"] in {"2", "2s"}
                ]
                selected_5f_signals = await module_c_repo.get_signals(
                    symbol.symbol_id,
                    "5f",
                    mode="predictive",
                    as_of_time=as_of_time,
                    run_kind=HISTORICAL_RUN_KIND,
                    run_group_id=HISTORICAL_RUN_GROUP,
                    allow_legacy_mode_fallback=False,
                )
                selected_5f_b2 = [
                    signal for signal in selected_5f_signals
                    if signal.side == "buy" and signal.bsp_type in {"2", "2s"} and signal.point_time >= thirty_b1_time
                ]
                daily_bottom_time = latest_bottom_fractal_time(daily_bars, after=anchor_time)
                daily_bottom_found = daily_bottom_time is not None
                five_f_b2_confirm_found = bool(selected_5f_b2)
                historical_5f_b2_visible = bool(visible_5f_b2_events)
                confirmation_count = int(True) + int(daily_bottom_found) + int(five_f_b2_confirm_found)
                confidence_score = 40.0 + (30.0 if daily_bottom_found else 0.0) + (30.0 if five_f_b2_confirm_found else 0.0)
                confidence_distribution[str(int(confidence_score))] += 1
                confidence_gate_counts["40"] += int(confidence_score >= 40.0)
                confidence_gate_counts["70"] += int(confidence_score >= 70.0)
                confidence_gate_counts["100"] += int(confidence_score >= 100.0)
                entry_triggered = confidence_score >= 70.0
                if historical_5f_b2_visible and not five_f_b2_confirm_found:
                    failure_reason = "selected_run_underestimates_5f_b2_confirm"
                elif confirmation_count < 2:
                    failure_reason = "insufficient_confirmations"
                elif not entry_triggered:
                    failure_reason = "confidence_below_70"
                else:
                    failure_reason = "would_trigger"
                failure_counts[failure_reason] += 1
                samples.append(
                    {
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "as_of_time": row["as_of_time"],
                        "daily_setup_mode": "daily_buy_signal_any_observation",
                        "daily_setup_first_seen_time": observation_signal["features"].get("first_seen_time"),
                        "thirty_f_b1_found": True,
                        "thirty_f_b1_first_seen_time": thirty_b1["first_seen_time"],
                        "daily_bottom_fractal_found": daily_bottom_found,
                        "daily_bottom_fractal_first_seen_time": daily_bottom_time.isoformat() if daily_bottom_time is not None else None,
                        "five_f_b2_confirm_found": five_f_b2_confirm_found,
                        "five_f_b2_first_seen_time": selected_5f_b2[-1].features.get("first_seen_time") if selected_5f_b2 else None,
                        "historical_visible_five_f_b2_confirm_found": historical_5f_b2_visible,
                        "confirmation_count": confirmation_count,
                        "confidence_score": confidence_score,
                        "entry_triggered": entry_triggered,
                        "entry_failure_reason": failure_reason,
                        "would_trigger_if_any_one_confirmation": confirmation_count >= 1,
                        "would_trigger_if_two_confirmations": confirmation_count >= 2,
                        "would_trigger_if_three_confirmations": confirmation_count >= 3,
                        "window_flags": _window_membership(day_order, thirty_b1_time, anchor_time),
                        "thirty_f_b1_event": _event_payload(thirty_b1),
                        "historical_visible_five_f_b2_event": _event_payload(visible_5f_b2_events[-1] if visible_5f_b2_events else None),
                    }
                )
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)

    summary = {
        "sample_count": len(samples),
        "entry_trigger_count": sum(1 for row in samples if row["entry_triggered"]),
        "failure_reason_counts": dict(sorted(failure_counts.items())),
        "confidence_distribution": dict(sorted(confidence_distribution.items())),
        "confidence_gate_counts": dict(sorted(confidence_gate_counts.items())),
        "current_code_requires_all_confirmations": False,
    }
    return {"rows": samples, "summary": summary}


def build_replay_compare(
    *,
    phase_1_11_compare_rows: list[dict[str, Any]],
    daily_dataset: dict[str, Any],
    thirty_f_audit: dict[str, Any],
    entry_diagnosis: dict[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    daily_rows = daily_dataset["rows"]
    candidate_rows = [row for row in daily_rows if row["candidate_b2_b2s_accept"]]
    observation_rows = [row for row in daily_rows if row["observation_accept"]]
    entry_rows_by_symbol_asof = {
        (row["symbol"], row["as_of_time"]): row for row in entry_diagnosis["rows"]
    }
    thirty_f_rows_by_symbol_asof = {
        (row["symbol"], row["as_of_time"]): row for row in thirty_f_audit["rows"]
    }
    phase_1_11_map = {
        (row["daily_signal_source"], row["daily_setup_mode"]): row for row in phase_1_11_compare_rows
    }

    for source, mode_name, mode_class in DAILY_SETUP_COMPARE_MODES:
        if mode_name == "event_ledger_daily_b2_or_b2s_setup_v1":
            accepted_rows = candidate_rows
            thirty_f_source_rows = candidate_rows
        elif mode_name == "daily_b2_or_b2s_with_b1_score":
            accepted_rows = [row for row in daily_rows if row["scored_audit"]["daily_setup_accepted_by_mode"]]
            thirty_f_source_rows = accepted_rows
        elif mode_name == "daily_buy_signal_any_observation":
            accepted_rows = observation_rows
            thirty_f_source_rows = observation_rows
        else:
            accepted_rows = [row for row in daily_rows if row["strict_accept"]]
            thirty_f_source_rows = accepted_rows

        thirty_f_b1_count = 0
        daily_bottom_count = 0
        five_f_b2_count = 0
        confidence_40 = 0
        confidence_70 = 0
        confidence_100 = 0
        entry_trigger_count = 0
        for item in thirty_f_source_rows:
            thirty_f_row = thirty_f_rows_by_symbol_asof.get((item["symbol"], item["as_of_time"]))
            entry_row = entry_rows_by_symbol_asof.get((item["symbol"], item["as_of_time"]))
            if thirty_f_row and thirty_f_row["visible_30f_b1_or_1p_count"] > 0:
                thirty_f_b1_count += 1
            if entry_row:
                daily_bottom_count += int(entry_row["daily_bottom_fractal_found"])
                five_f_b2_count += int(entry_row["five_f_b2_confirm_found"])
                confidence_40 += int(entry_row["confidence_score"] >= 40.0)
                confidence_70 += int(entry_row["confidence_score"] >= 70.0)
                confidence_100 += int(entry_row["confidence_score"] >= 100.0)
                entry_trigger_count += int(entry_row["entry_triggered"])

        phase_1_11_key = (
            source,
            "true_trust_daily_b2_or_b2s" if mode_name == "event_ledger_daily_b2_or_b2s_setup_v1" else mode_name,
        )
        prior = phase_1_11_map.get(phase_1_11_key, {})
        rows.append(
            {
                "daily_signal_source": source,
                "daily_setup_mode": mode_name,
                "mode_class": mode_class,
                "weekly_context_count": len(daily_rows),
                "daily_setup_count": len(accepted_rows),
                "entry_watch_count": len(accepted_rows),
                "thirty_f_b1_count": thirty_f_b1_count,
                "daily_bottom_fractal_count": daily_bottom_count,
                "five_f_b2_confirm_count": five_f_b2_count,
                "confidence_40_count": confidence_40,
                "confidence_70_count": confidence_70,
                "confidence_100_count": confidence_100,
                "entry_trigger_count": entry_trigger_count,
                "trade_count": entry_trigger_count,
                "future_leakage_detected": False,
                "phase_1_11_reference": prior,
            }
        )
    return {"rows": rows}


def build_policy_decision(
    *,
    daily_dataset: dict[str, Any],
    thirty_f_audit: dict[str, Any],
    entry_diagnosis: dict[str, Any],
) -> dict[str, Any]:
    strict_count = daily_dataset["summary"]["strict_daily_setup_count"]
    candidate_count = daily_dataset["summary"]["candidate_daily_setup_count"]
    observation_count = daily_dataset["summary"]["observation_daily_setup_count"]
    non_b2_ratio = daily_dataset["summary"]["observation_non_b2_b2s_ratio"]
    thirty_f_visible = thirty_f_audit["summary"]["visible_30f_b1_samples"]
    decision = "Decision B" if candidate_count > 0 else "Decision A"
    return {
        "decision": decision,
        "recommend_adopt_event_ledger_for_historical_daily_signals": True,
        "keep_strict_daily_b1_as_official_baseline": True,
        "recommend_candidate_daily_b2_b2s_setup": candidate_count > 0,
        "recommend_strategy_30f_smoke_next": bool(entry_diagnosis["summary"]["entry_trigger_count"]),
        "recommend_30f_event_ledger_design_next": thirty_f_audit["summary"]["recommend_30f_event_ledger_design_next"],
        "recommend_50_symbols_backfill_next": False,
        "recommend_copy_staging_implementation_next": False,
        "strict_daily_setup_count": strict_count,
        "candidate_daily_setup_count": candidate_count,
        "observation_daily_setup_count": observation_count,
        "observation_non_b2_b2s_ratio": non_b2_ratio,
        "candidate_30f_b1_visible_count": thirty_f_visible,
    }


def _render_daily_setup_decision_report(payload: dict[str, Any], policy: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Daily Setup Decision Report",
        "",
        f"- sample_count: `{summary['sample_count']}`",
        f"- strict_daily_setup_count: `{summary['strict_daily_setup_count']}`",
        f"- candidate_daily_setup_count: `{summary['candidate_daily_setup_count']}`",
        f"- scored_daily_setup_count: `{summary['scored_daily_setup_count']}`",
        f"- observation_daily_setup_count: `{summary['observation_daily_setup_count']}`",
        f"- strict_failure_reason_counts: `{json.dumps(summary['strict_failure_reason_counts'], ensure_ascii=False)}`",
        f"- candidate_signal_kind_counts: `{json.dumps(summary['candidate_signal_kind_counts'], ensure_ascii=False)}`",
        f"- observation_signal_kind_counts: `{json.dumps(summary['observation_signal_kind_counts'], ensure_ascii=False)}`",
        f"- observation_non_b2_b2s_ratio: `{summary['observation_non_b2_b2s_ratio']:.4f}`",
        "",
        "## Decision",
        "",
        f"- proposed_policy: `{policy['decision']}`",
        f"- keep_strict_daily_b1_as_official_baseline: `{policy['keep_strict_daily_b1_as_official_baseline']}`",
        f"- recommend_candidate_daily_b2_b2s_setup: `{policy['recommend_candidate_daily_b2_b2s_setup']}`",
        "",
    ]
    return "\n".join(lines) + "\n"


def _render_mode_compare_md(payload: dict[str, Any]) -> str:
    rows = [
        [
            f"`{row['daily_setup_mode']}`",
            row["daily_signal_source"],
            row["mode_class"],
            row["weekly_context_count"],
            row["daily_setup_count"],
            row["entry_watch_count"],
            row["thirty_f_b1_count"],
            row["daily_bottom_fractal_count"],
            row["five_f_b2_confirm_count"],
            row["confidence_70_count"],
            row["entry_trigger_count"],
            row["trade_count"],
        ]
        for row in payload["rows"]
    ]
    return "\n".join(
        [
            "# Daily Setup Mode Compare V3",
            "",
            render_markdown_table(
                [
                    "daily_setup_mode",
                    "daily_signal_source",
                    "mode_class",
                    "weekly_context_count",
                    "daily_setup_count",
                    "entry_watch_count",
                    "thirty_f_b1_count",
                    "daily_bottom_fractal_count",
                    "five_f_b2_confirm_count",
                    "confidence_70_count",
                    "entry_trigger_count",
                    "trade_count",
                ],
                rows,
            ),
            "",
        ]
    )


def _render_thirty_f_downstream_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# 30F Downstream Audit",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- visible_30f_buy_signal_samples: `{summary['visible_30f_buy_signal_samples']}`",
            f"- visible_30f_b1_samples: `{summary['visible_30f_b1_samples']}`",
            f"- selected_run_underestimate_samples: `{summary['selected_run_underestimate_samples']}`",
            f"- category_counts: `{json.dumps(summary['category_counts'], ensure_ascii=False)}`",
            f"- recommend_30f_event_ledger_design_next: `{summary['recommend_30f_event_ledger_design_next']}`",
            "",
        ]
    )


def _render_gate_waterfall_30f_md(payload: dict[str, Any]) -> str:
    rows = [
        [category, count]
        for category, count in payload["summary"]["category_counts"].items()
    ]
    return "\n".join(
        [
            "# Gate Waterfall 30F Diagnostic",
            "",
            render_markdown_table(["category", "count"], rows),
            "",
        ]
    )


def _render_entry_trigger_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# Entry Trigger Diagnosis",
            "",
            f"- sample_count: `{summary['sample_count']}`",
            f"- entry_trigger_count: `{summary['entry_trigger_count']}`",
            f"- failure_reason_counts: `{json.dumps(summary['failure_reason_counts'], ensure_ascii=False)}`",
            f"- confidence_distribution: `{json.dumps(summary['confidence_distribution'], ensure_ascii=False)}`",
            f"- confidence_gate_counts: `{json.dumps(summary['confidence_gate_counts'], ensure_ascii=False)}`",
            f"- current_code_requires_all_confirmations: `{summary['current_code_requires_all_confirmations']}`",
            "",
        ]
    )


def _render_entry_confidence_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    rows = [
        [bucket, count]
        for bucket, count in summary["confidence_distribution"].items()
    ]
    return "\n".join(
        [
            "# Entry Confidence Gate Audit",
            "",
            render_markdown_table(["confidence_score", "count"], rows),
            "",
            f"- gate_counts: `{json.dumps(summary['confidence_gate_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


def _render_policy_md(policy: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Daily Setup Policy Decision",
            "",
            f"- decision: `{policy['decision']}`",
            f"- recommend_adopt_event_ledger_for_historical_daily_signals: `{policy['recommend_adopt_event_ledger_for_historical_daily_signals']}`",
            f"- keep_strict_daily_b1_as_official_baseline: `{policy['keep_strict_daily_b1_as_official_baseline']}`",
            f"- recommend_candidate_daily_b2_b2s_setup: `{policy['recommend_candidate_daily_b2_b2s_setup']}`",
            f"- recommend_strategy_30f_smoke_next: `{policy['recommend_strategy_30f_smoke_next']}`",
            f"- recommend_30f_event_ledger_design_next: `{policy['recommend_30f_event_ledger_design_next']}`",
            f"- recommend_50_symbols_backfill_next: `{policy['recommend_50_symbols_backfill_next']}`",
            f"- recommend_copy_staging_implementation_next: `{policy['recommend_copy_staging_implementation_next']}`",
            "",
        ]
    )


def _render_replay_compare_md(payload: dict[str, Any]) -> str:
    rows = [
        [
            f"`{row['daily_setup_mode']}`",
            row["daily_signal_source"],
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
            "# Replay Phase 1.12 Compare",
            "",
            render_markdown_table(
                [
                    "daily_setup_mode",
                    "daily_signal_source",
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
    lines = ["# Backtest Report Phase 1.12", ""]
    for row in payload["rows"]:
        lines.append(f"## `{row['daily_setup_mode']}`")
        lines.append(f"- daily_setup_count: `{row['daily_setup_count']}`")
        lines.append(f"- thirty_f_b1_count: `{row['thirty_f_b1_count']}`")
        lines.append(f"- entry_trigger_count: `{row['entry_trigger_count']}`")
        lines.append(f"- future_leakage_detected: `{row['future_leakage_detected']}`")
        lines.append("")
    return "\n".join(lines)


def _render_trade_analysis_md(payload: dict[str, Any]) -> str:
    lines = ["# Trade Analysis Phase 1.12", ""]
    for row in payload["rows"]:
        lines.append(f"## `{row['daily_setup_mode']}`")
        if row["entry_trigger_count"] == 0:
            lines.append("- no diagnostic trade trigger")
        else:
            lines.append("- diagnostic trade trigger exists")
        lines.append("")
    return "\n".join(lines)


def _render_phase_1_12_summary(phase_1_11_summary: dict[str, Any], policy: dict[str, Any], replay_compare: dict[str, Any]) -> str:
    candidate_row = next(row for row in replay_compare["rows"] if row["daily_setup_mode"] == "event_ledger_daily_b2_or_b2s_setup_v1")
    observation_row = next(row for row in replay_compare["rows"] if row["daily_setup_mode"] == "daily_buy_signal_any_observation")
    return "\n".join(
        [
            "# Phase 1.12 Summary",
            "",
            f"- phase_1_11_event_ledger_visible_samples: `{phase_1_11_summary['event_ledger_visible_samples']}`",
            f"- strict_daily_setup_count: `0`",
            f"- candidate_daily_setup_count: `{candidate_row['daily_setup_count']}`",
            f"- observation_daily_setup_count: `{observation_row['daily_setup_count']}`",
            f"- candidate_30f_b1_count: `{candidate_row['thirty_f_b1_count']}`",
            f"- observation_30f_b1_count: `{observation_row['thirty_f_b1_count']}`",
            f"- policy_decision: `{policy['decision']}`",
            "",
        ]
    )


def _render_phase_1_12_decision_report(policy: dict[str, Any], thirty_f_audit: dict[str, Any], entry_diagnosis: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Phase 1.12 Decision Report",
            "",
            f"- keep_strict_daily_b1_as_official_baseline: `{policy['keep_strict_daily_b1_as_official_baseline']}`",
            f"- recommend_candidate_daily_b2_b2s_setup: `{policy['recommend_candidate_daily_b2_b2s_setup']}`",
            f"- recommend_strategy_30f_smoke_next: `{policy['recommend_strategy_30f_smoke_next']}`",
            f"- recommend_30f_event_ledger_design_next: `{policy['recommend_30f_event_ledger_design_next']}`",
            f"- candidate_30f_visible_b1_samples: `{thirty_f_audit['summary']['visible_30f_b1_samples']}`",
            f"- entry_trigger_count_from_observation_30f_b1_samples: `{entry_diagnosis['summary']['entry_trigger_count']}`",
            "",
        ]
    )


def _render_task_checklist() -> str:
    items = [
        "phase_1_12_summary.md",
        "phase_1_12_decision_report.md",
        "phase_1_12_task_checklist_report.md",
        "daily_setup_decision_report.md",
        "daily_setup_decision_report.json",
        "daily_setup_mode_compare_v3.md",
        "daily_setup_mode_compare_v3.json",
        "daily_setup_sample_audit_v3.jsonl",
        "thirty_f_downstream_audit.md",
        "thirty_f_downstream_audit.json",
        "thirty_f_downstream_samples.jsonl",
        "gate_waterfall_30f_diagnostic.md",
        "gate_waterfall_30f_diagnostic.json",
        "entry_trigger_diagnosis.md",
        "entry_trigger_diagnosis.json",
        "entry_trigger_18_samples.jsonl",
        "entry_confidence_gate_audit.md",
        "entry_confidence_gate_audit.json",
        "daily_setup_policy_decision.md",
        "daily_setup_policy_decision.json",
        "replay_phase_1_12_compare.md",
        "replay_phase_1_12_compare.json",
        "gate_waterfall_phase_1_12.md",
        "gate_waterfall_phase_1_12.json",
        "backtest_report_phase_1_12.md",
        "trade_analysis_phase_1_12.md",
        "trace_index.md",
    ]
    return "# Phase 1.12 Task Checklist Report\n\n" + "\n".join(f"- [x] `{item}`" for item in items) + "\n"


def _sample_trace_groups(
    daily_rows: list[dict[str, Any]],
    thirty_f_rows: list[dict[str, Any]],
    entry_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    thirty_f_map = {(row["symbol"], row["as_of_time"]): row for row in thirty_f_rows}
    entry_map = {(row["symbol"], row["as_of_time"]): row for row in entry_rows}
    strict_fail_candidate_success = [
        row
        for row in daily_rows
        if not row["strict_accept"] and row["candidate_b2_b2s_accept"]
    ][:5]
    candidate_success_no_30f = [
        row
        for row in daily_rows
        if row["candidate_b2_b2s_accept"]
        and thirty_f_map.get((row["symbol"], row["as_of_time"]), {}).get("visible_30f_b1_or_1p_count", 0) == 0
    ][:5]
    observation_with_30f_no_entry = [
        row
        for row in daily_rows
        if row["observation_accept"]
        and (entry := entry_map.get((row["symbol"], row["as_of_time"])))
        and not entry["entry_triggered"]
    ][:5]
    return {
        "strict_fail_candidate_success": strict_fail_candidate_success,
        "candidate_success_no_30f_b1": candidate_success_no_30f,
        "observation_with_30f_b1_no_entry": observation_with_30f_no_entry,
    }


def _render_trace(
    *,
    row: dict[str, Any],
    thirty_f_row: dict[str, Any] | None,
    entry_row: dict[str, Any] | None,
    title: str,
) -> str:
    lines = [
        f"# {title}",
        "",
        f"- symbol: `{row['symbol']}`",
        f"- name: `{row['name']}`",
        f"- as_of_time: `{row['as_of_time']}`",
        f"- weekly_context_time: `{row['weekly_context_time']}`",
        f"- strict_accept: `{row['strict_accept']}`",
        f"- candidate_accept: `{row['candidate_b2_b2s_accept']}`",
        f"- observation_accept: `{row['observation_accept']}`",
        f"- failure_reason_strict: `{row['failure_reason_strict']}`",
        f"- failure_reason_candidate: `{row['failure_reason_candidate']}`",
        f"- visible_daily_events_by_bsp_type: `{json.dumps(row['daily_events_visible_by_bsp_type'], ensure_ascii=False)}`",
        f"- nearest_daily_B1_after_context: `{json.dumps(row['nearest_daily_B1_after_context'], ensure_ascii=False)}`",
        f"- nearest_daily_B2_or_B2s_after_context: `{json.dumps(row['nearest_daily_B2_or_B2s_after_context'], ensure_ascii=False)}`",
        "",
        "## 30F",
        "",
        f"- thirty_f_summary: `{json.dumps(thirty_f_row or {}, ensure_ascii=False)}`",
        "",
        "## Entry",
        "",
        f"- entry_summary: `{json.dumps(entry_row or {}, ensure_ascii=False)}`",
        "",
    ]
    return "\n".join(lines)


def _write_traces(
    *,
    output_dir: Path,
    daily_rows: list[dict[str, Any]],
    thirty_f_rows: list[dict[str, Any]],
    entry_rows: list[dict[str, Any]],
) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    thirty_f_map = {(row["symbol"], row["as_of_time"]): row for row in thirty_f_rows}
    entry_map = {(row["symbol"], row["as_of_time"]): row for row in entry_rows}
    grouped = _sample_trace_groups(daily_rows, thirty_f_rows, entry_rows)
    index_lines = ["# Trace Index", ""]
    for group_name, items in grouped.items():
        index_lines.append(f"## {group_name}")
        index_lines.append("")
        for idx, row in enumerate(items, start=1):
            filename = f"{group_name}-{idx:02d}-{row['symbol'].replace('.', '_')}.md"
            path = traces_dir / filename
            path.write_text(
                _render_trace(
                    row=row,
                    thirty_f_row=thirty_f_map.get((row["symbol"], row["as_of_time"])),
                    entry_row=entry_map.get((row["symbol"], row["as_of_time"])),
                    title=f"{group_name} #{idx}",
                ),
                encoding="utf-8",
            )
            index_lines.append(f"- [{filename}](./traces/{filename})")
        index_lines.append("")
    (output_dir / "trace_index.md").write_text("\n".join(index_lines), encoding="utf-8")


async def run_phase_1_12(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_11_output_dir: Path = PHASE_1_11_OUTPUT_DIR,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = load_phase_1_11_artifacts(phase_1_11_output_dir)
    requested_symbols = symbols or DEFAULT_PHASE_1_7_SYMBOLS
    symbol_infos = await module_c_repo.list_active_symbols(symbols=requested_symbols)
    symbol_set = {symbol.symbol for symbol in symbol_infos}
    daily_events = [event for event in artifacts.daily_event_ledger if event["symbol"] in symbol_set]
    weekly_context_samples = [sample for sample in artifacts.weekly_context_samples if sample["symbol"] in symbol_set]
    start_time = min(parse_dt(sample["as_of_time"]) for sample in weekly_context_samples)
    end_time = max(parse_dt(sample["as_of_time"]) for sample in weekly_context_samples)

    daily_dataset = await build_daily_setup_decision_dataset(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbol_infos,
        daily_events=daily_events,
        weekly_context_samples=weekly_context_samples,
    )
    candidate_rows = [row for row in daily_dataset["rows"] if row["candidate_b2_b2s_accept"]]
    observation_rows = [row for row in daily_dataset["rows"] if row["observation_accept"]]
    events_30f = await build_signal_event_ledger(
        pool=pool,
        symbols=symbol_infos,
        level="30f",
        start_time=start_time,
        end_time=end_time,
    )
    events_5f = await build_signal_event_ledger(
        pool=pool,
        symbols=symbol_infos,
        level="5f",
        start_time=start_time,
        end_time=end_time,
    )
    thirty_f_audit = await build_thirty_f_downstream_audit(
        pool=pool,
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbol_infos,
        candidate_rows=candidate_rows,
        events_30f=events_30f,
    )
    entry_diagnosis = await build_entry_trigger_diagnosis(
        pool=pool,
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbol_infos,
        observation_rows=observation_rows,
        events_30f=events_30f,
        events_5f=events_5f,
    )
    replay_compare = build_replay_compare(
        phase_1_11_compare_rows=artifacts.daily_setup_source_compare,
        daily_dataset=daily_dataset,
        thirty_f_audit=thirty_f_audit,
        entry_diagnosis=entry_diagnosis,
    )
    policy = build_policy_decision(
        daily_dataset=daily_dataset,
        thirty_f_audit=thirty_f_audit,
        entry_diagnosis=entry_diagnosis,
    )

    write_json(output_dir / "daily_setup_decision_report.json", daily_dataset["summary"])
    (output_dir / "daily_setup_decision_report.md").write_text(
        _render_daily_setup_decision_report(daily_dataset, policy),
        encoding="utf-8",
    )
    write_json(output_dir / "daily_setup_mode_compare_v3.json", replay_compare)
    (output_dir / "daily_setup_mode_compare_v3.md").write_text(
        _render_mode_compare_md(replay_compare),
        encoding="utf-8",
    )
    write_jsonl(output_dir / "daily_setup_sample_audit_v3.jsonl", daily_dataset["rows"])
    write_json(output_dir / "thirty_f_downstream_audit.json", thirty_f_audit["summary"])
    (output_dir / "thirty_f_downstream_audit.md").write_text(
        _render_thirty_f_downstream_md(thirty_f_audit),
        encoding="utf-8",
    )
    write_jsonl(output_dir / "thirty_f_downstream_samples.jsonl", thirty_f_audit["rows"])
    write_json(output_dir / "gate_waterfall_30f_diagnostic.json", thirty_f_audit["summary"])
    (output_dir / "gate_waterfall_30f_diagnostic.md").write_text(
        _render_gate_waterfall_30f_md(thirty_f_audit),
        encoding="utf-8",
    )
    write_json(output_dir / "entry_trigger_diagnosis.json", entry_diagnosis["summary"])
    (output_dir / "entry_trigger_diagnosis.md").write_text(
        _render_entry_trigger_md(entry_diagnosis),
        encoding="utf-8",
    )
    write_jsonl(output_dir / "entry_trigger_18_samples.jsonl", entry_diagnosis["rows"])
    write_json(output_dir / "entry_confidence_gate_audit.json", entry_diagnosis["summary"])
    (output_dir / "entry_confidence_gate_audit.md").write_text(
        _render_entry_confidence_md(entry_diagnosis),
        encoding="utf-8",
    )
    write_json(output_dir / "daily_setup_policy_decision.json", policy)
    (output_dir / "daily_setup_policy_decision.md").write_text(
        _render_policy_md(policy),
        encoding="utf-8",
    )
    write_json(output_dir / "replay_phase_1_12_compare.json", replay_compare)
    (output_dir / "replay_phase_1_12_compare.md").write_text(
        _render_replay_compare_md(replay_compare),
        encoding="utf-8",
    )
    write_json(output_dir / "gate_waterfall_phase_1_12.json", replay_compare)
    (output_dir / "gate_waterfall_phase_1_12.md").write_text(
        _render_replay_compare_md(replay_compare),
        encoding="utf-8",
    )
    (output_dir / "backtest_report_phase_1_12.md").write_text(
        _render_backtest_report_md(replay_compare),
        encoding="utf-8",
    )
    (output_dir / "trade_analysis_phase_1_12.md").write_text(
        _render_trade_analysis_md(replay_compare),
        encoding="utf-8",
    )
    (output_dir / "phase_1_12_summary.md").write_text(
        _render_phase_1_12_summary(artifacts.phase_1_11_summary, policy, replay_compare),
        encoding="utf-8",
    )
    (output_dir / "phase_1_12_decision_report.md").write_text(
        _render_phase_1_12_decision_report(policy, thirty_f_audit, entry_diagnosis),
        encoding="utf-8",
    )
    (output_dir / "phase_1_12_task_checklist_report.md").write_text(
        _render_task_checklist(),
        encoding="utf-8",
    )
    _write_traces(
        output_dir=output_dir,
        daily_rows=daily_dataset["rows"],
        thirty_f_rows=thirty_f_audit["rows"],
        entry_rows=entry_diagnosis["rows"],
    )
    return {
        "phase_1_11_event_ledger_visible_samples": artifacts.phase_1_11_summary["event_ledger_visible_samples"],
        "strict_daily_setup_count": daily_dataset["summary"]["strict_daily_setup_count"],
        "candidate_daily_setup_count": daily_dataset["summary"]["candidate_daily_setup_count"],
        "observation_daily_setup_count": daily_dataset["summary"]["observation_daily_setup_count"],
        "candidate_30f_b1_visible_count": thirty_f_audit["summary"]["visible_30f_b1_samples"],
        "observation_entry_trigger_count": entry_diagnosis["summary"]["entry_trigger_count"],
        "policy_decision": policy["decision"],
        "output_dir": str(output_dir),
    }
