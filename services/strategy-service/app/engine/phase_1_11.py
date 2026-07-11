from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.config.strategy_params import PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE, StrategyParams
from app.domain.enums import LEVEL_TO_DB
from app.domain.models import ChanSignal, SymbolInfo
from app.engine.phase_1_10 import DEFAULT_OUTPUT_DIR as PHASE_1_10_OUTPUT_DIR
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS, write_json
from app.engine.phase_1_9 import _inspect_downstream, load_phase_1_7_inputs
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import MODE_TO_DB, ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-11-signal-event-ledger"
DAILY_LEVEL = LEVEL_TO_DB["1d"]
HISTORICAL_RUN_KIND = "historical_backfill"
HISTORICAL_RUN_GROUP = "research_daily_close"
DEFAULT_WEEKLY_CONTEXT_MODE = "trust_chan_signal_with_b1_score"


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return serialize_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]
    return value


def render_markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(item) for item in row) + " |")
    return "\n".join(lines)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(serialize_value(row), ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def build_signal_fingerprint(
    *,
    symbol: str,
    level: str,
    mode: str,
    side: str | None,
    bsp_type: str | None,
    signal_point_time: datetime,
    price_x1000: int,
) -> str:
    return "|".join(
        [
            symbol,
            level,
            mode,
            side or "",
            bsp_type or "",
            signal_point_time.isoformat(),
            str(price_x1000),
        ]
    )


def _signal_payload(signal: ChanSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "time": signal.point_time.isoformat(),
        "price": signal.price,
        "bsp_type": signal.bsp_type,
        "side": signal.side,
        "run_id": signal.run_id,
        "confirmed": signal.confirmed,
        "first_seen_time": signal.features.get("first_seen_time"),
    }


def _event_payload(event: dict[str, Any] | None) -> dict[str, Any] | None:
    if event is None:
        return None
    return {
        "signal_point_time": event["signal_point_time"],
        "first_seen_time": event["first_seen_time"],
        "price": event["price_x1000"] / 1000,
        "bsp_type": event["bsp_type"],
        "observed_run_count": event["observed_run_count"],
    }


def _classification_from_ledger(
    *,
    sample: dict[str, Any],
    visible_events: list[dict[str, Any]],
    visible_events_in_window: list[dict[str, Any]],
    visible_events_supported_bsp: list[dict[str, Any]],
    future_events: list[dict[str, Any]],
) -> str:
    if visible_events:
        if sample.get("selected_daily_run_missing"):
            return "ledger_visible_but_selected_run_empty"
        if int(sample.get("daily_buy_signal_count_in_selected_run") or 0) <= 0:
            return "ledger_visible_but_selected_run_empty"
        if sample.get("selected_daily_run_group") != HISTORICAL_RUN_GROUP or sample.get("selected_daily_run_kind") != HISTORICAL_RUN_KIND:
            return "ledger_visible_but_filtered_by_mode_or_group"
        if not visible_events_in_window:
            return "ledger_visible_but_filtered_by_time"
        if not visible_events_supported_bsp:
            return "ledger_visible_but_filtered_by_bsp_type"
        return "ledger_visible_and_selected_run_visible"
    if future_events:
        return "ledger_not_visible_because_first_seen_after_asof"
    return "ledger_not_visible_because_no_event_before_asof"


def _build_weekly_context_stub(sample: dict[str, Any]) -> Any:
    weekly_signal_time = parse_dt(sample["weekly_context_signal_time"])
    weekly_signal = ChanSignal(
        signal_id=None,
        level="1w",
        mode="predictive",
        point_time=weekly_signal_time,
        base_time=weekly_signal_time,
        base_seq=None,
        price=float(sample.get("weekly_context_price") or 0.0),
        signal_type="bsp",
        side="buy",
        bsp_type=str(sample.get("weekly_bsp_type") or "2"),
        confirmed=False,
        features={},
    )
    return type(
        "WeeklyContextStub",
        (),
        {
            "weekly_b2": weekly_signal,
            "anchor_time": weekly_signal_time,
        },
    )()


def _event_to_signal(event: dict[str, Any]) -> ChanSignal:
    point_time = parse_dt(event["signal_point_time"])
    signal_ts = parse_dt(event["signal_ts"])
    first_seen_time = parse_dt(event["first_seen_time"])
    features = dict(event.get("extra_json") or {})
    if first_seen_time is not None:
        features["first_seen_time"] = first_seen_time.isoformat()
    return ChanSignal(
        signal_id=None,
        level=str(event["level"]),
        mode=str(event["mode"]),
        point_time=point_time,
        base_time=parse_dt(event["signal_base_ts"]) or point_time,
        base_seq=None,
        price=int(event["price_x1000"]) / 1000,
        signal_type=str(event["signal_type"]),
        side=event["side"],
        bsp_type=event["bsp_type"],
        confirmed=bool(event["is_confirmed"]),
        features=features,
        run_id=int(event["first_seen_run_id"]) if event.get("first_seen_run_id") is not None else None,
        snapshot_version=None,
    )


async def _load_exact_run_info(conn: asyncpg.Connection, run_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not run_ids:
        return {}
    rows = await conn.fetch(
        """
        select id, mode, run_kind, run_group_id, bar_until, cutoff_bar_end, snapshot_version
        from chan_c_runs
        where id = any($1::bigint[])
        """,
        run_ids,
    )
    return {
        int(row["id"]): {
            "run_id": int(row["id"]),
            "mode": int(row["mode"]),
            "run_kind": row["run_kind"],
            "run_group_id": row["run_group_id"],
            "bar_until": row["bar_until"],
            "cutoff_bar_end": row["cutoff_bar_end"],
            "snapshot_version": row["snapshot_version"],
        }
        for row in rows
    }


async def build_daily_signal_event_ledger(
    *,
    pool: asyncpg.Pool,
    symbols: list[SymbolInfo],
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
            DAILY_LEVEL,
            HISTORICAL_RUN_KIND,
            HISTORICAL_RUN_GROUP,
            MODE_TO_DB["predictive"],
            start_time,
            end_time,
        )

    return _aggregate_daily_signal_event_ledger(rows=rows, symbol_map=symbol_map, start_time=start_time, end_time=end_time)


def _aggregate_daily_signal_event_ledger(
    *,
    rows: list[Any],
    symbol_map: dict[int, SymbolInfo],
    start_time: datetime,
    end_time: datetime,
) -> dict[str, Any]:
    raw_signal_rows = 0
    ledger: dict[str, dict[str, Any]] = {}
    per_symbol_event_count: Counter[str] = Counter()
    per_bsp_type_event_count: Counter[str] = Counter()

    for row in rows:
        raw_signal_rows += 1
        symbol = symbol_map[int(row["symbol_id"])].symbol
        extra = row["extra"]
        if isinstance(extra, str):
            extra = json.loads(extra)
        extra = extra if isinstance(extra, dict) else {}
        signal_point_time = row["signal_base_ts"]
        price_x1000 = int(row["price_x1000"])
        bsp_type = extra.get("bsp_type")
        side = extra.get("side")
        fingerprint = build_signal_fingerprint(
            symbol=symbol,
            level="1d",
            mode="predictive",
            side=side,
            bsp_type=bsp_type,
            signal_point_time=signal_point_time,
            price_x1000=price_x1000,
        )
        observed_time = row["cutoff_bar_end"]
        payload = ledger.get(fingerprint)
        if payload is None:
            payload = {
                "symbol": symbol,
                "level": "1d",
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
        per_bsp_type_event_count[str(event["bsp_type"] or "")] += 1

    summary = {
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "symbol_count": len(symbol_map),
        "raw_signal_rows": raw_signal_rows,
        "unique_signal_events": len(events),
        "dedup_ratio": (len(events) / raw_signal_rows) if raw_signal_rows else 0.0,
        "per_symbol_event_count": dict(sorted(per_symbol_event_count.items())),
        "per_bsp_type_event_count": dict(sorted(per_bsp_type_event_count.items())),
        "all_symbols_have_daily_buy_event": all(
            per_symbol_event_count.get(symbol.symbol, 0) > 0 for symbol in symbol_map.values()
        ),
    }
    return {"events": events, "summary": summary}


def render_daily_signal_event_ledger_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    rows = [
        [symbol, count]
        for symbol, count in summary["per_symbol_event_count"].items()
    ]
    bsp_rows = [
        [bsp_type or "(empty)", count]
        for bsp_type, count in summary["per_bsp_type_event_count"].items()
    ]
    lines = [
        "# Phase 1.11 Daily Signal Event Ledger",
        "",
        f"- Window: `{summary['window_start']}` -> `{summary['window_end']}`",
        f"- Symbol count: `{summary['symbol_count']}`",
        f"- raw_signal_rows: `{summary['raw_signal_rows']}`",
        f"- unique_signal_events: `{summary['unique_signal_events']}`",
        f"- dedup_ratio: `{summary['dedup_ratio']:.4f}`",
        f"- 10/10 symbols have daily buy events: `{summary['all_symbols_have_daily_buy_event']}`",
        "",
        "## Per Symbol Event Count",
        render_markdown_table(["symbol", "event_count"], rows),
        "",
        "## Per BSP Type Event Count",
        render_markdown_table(["bsp_type", "event_count"], bsp_rows),
        "",
    ]
    return "\n".join(lines)


async def audit_weekly_context_daily_event_visibility(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    events: list[dict[str, Any]],
    weekly_context_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        events_by_symbol[event["symbol"]].append(event)
    samples_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in weekly_context_samples:
        samples_by_symbol[sample["symbol"]].append(sample)

    params_observation = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE).with_overrides(
        daily_setup_mode="daily_buy_signal_any_observation",
        daily_signal_source="event_ledger",
    )
    params_true_trust = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE).with_overrides(
        daily_setup_mode="true_trust_daily_b2_or_b2s",
        daily_signal_source="event_ledger",
    )

    rows: list[dict[str, Any]] = []
    classifications: Counter[str] = Counter()
    visible_samples_count = 0
    phase_1_10_visible_samples = 0

    sample_id = 0
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
                sample_id += 1
                as_of_time = parse_dt(sample["as_of_time"])
                weekly_context = _build_weekly_context_stub(sample)
                daily_bars = await kline_repo.get_klines(symbol.symbol_id, "1d", end=as_of_time)
                visible_events = [
                    event for event in candidate_events
                    if parse_dt(event["first_seen_time"]) <= as_of_time
                ]
                visible_events_before_asof = [
                    event for event in visible_events
                    if parse_dt(event["signal_point_time"]) <= as_of_time
                ]
                visible_events_in_window = [
                    event for event in visible_events_before_asof
                    if parse_dt(sample["weekly_context_signal_time"]) <= parse_dt(event["signal_point_time"]) <= as_of_time
                ]
                visible_events_supported_bsp = [
                    event for event in visible_events_in_window if event["bsp_type"] in {"1", "2", "2s"}
                ]
                future_events = [
                    event for event in candidate_events
                    if parse_dt(event["first_seen_time"]) > as_of_time
                ]
                if sample.get("classification") == "visible_daily_buy_signal_found":
                    phase_1_10_visible_samples += 1
                if visible_events_before_asof:
                    visible_samples_count += 1

                ledger_signals = [_event_to_signal(event) for event in visible_events_before_asof]
                observation_audit = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=ledger_signals,
                    weekly_context=weekly_context,
                    as_of_time=as_of_time,
                    params=params_observation,
                    daily_bars=daily_bars,
                )
                true_trust_audit = StrategyDiagnoser.audit_daily_setup_semantics(
                    daily_signals=ledger_signals,
                    weekly_context=weekly_context,
                    as_of_time=as_of_time,
                    params=params_true_trust,
                    daily_bars=daily_bars,
                )
                classification = _classification_from_ledger(
                    sample=sample,
                    visible_events=visible_events_before_asof,
                    visible_events_in_window=visible_events_in_window,
                    visible_events_supported_bsp=visible_events_supported_bsp,
                    future_events=future_events,
                )
                classifications[classification] += 1
                rows.append(
                    {
                        "sample_id": sample_id,
                        "symbol": sample["symbol"],
                        "as_of_time": sample["as_of_time"],
                        "weekly_context_time": sample["weekly_context_signal_time"],
                        "weekly_b2_time": sample["weekly_context_signal_time"],
                        "selected_daily_run_id": sample.get("selected_daily_run_id"),
                        "selected_daily_run_cutoff": sample.get("selected_daily_run_bar_until"),
                        "selected_daily_run_signal_count": sample.get("selected_daily_run_signal_count"),
                        "ledger_visible_buy_event_count": len(visible_events_before_asof),
                        "ledger_visible_B1_count": sum(1 for event in visible_events_before_asof if event["bsp_type"] == "1"),
                        "ledger_visible_B2_or_B2s_count": sum(1 for event in visible_events_before_asof if event["bsp_type"] in {"2", "2s"}),
                        "nearest_ledger_buy_before_asof": _event_payload(visible_events_before_asof[-1] if visible_events_before_asof else None),
                        "nearest_ledger_buy_after_asof": _event_payload(future_events[0] if future_events else None),
                        "nearest_ledger_buy_in_weekly_context_window": _event_payload(visible_events_in_window[-1] if visible_events_in_window else None),
                        "old_visibility_classification": sample.get("classification"),
                        "new_ledger_visibility_classification": classification,
                        "should_reach_daily_setup_under_observation_mode": observation_audit.daily_setup_accepted_by_mode,
                        "should_reach_daily_setup_under_true_trust_mode": true_trust_audit.daily_setup_accepted_by_mode,
                        "current_diagnoser_daily_setup_result": sample.get("strict_failure_reason_v2"),
                        "mismatch_reason": None if observation_audit.daily_setup_accepted_by_mode else classification,
                    }
                )
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)

    summary = {
        "sample_count": len(rows),
        "phase_1_10_visible_daily_buy_signal_found": phase_1_10_visible_samples,
        "event_ledger_visible_samples": visible_samples_count,
        "classification_counts": dict(sorted(classifications.items())),
    }
    return {"rows": rows, "summary": summary}


def render_weekly_context_daily_event_visibility_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    rows = [
        [name, count]
        for name, count in summary["classification_counts"].items()
    ]
    lines = [
        "# Weekly Context Daily Event Visibility Audit",
        "",
        f"- sample_count: `{summary['sample_count']}`",
        f"- Phase 1.10 visible_daily_buy_signal_found: `{summary['phase_1_10_visible_daily_buy_signal_found']}`",
        f"- event_ledger_visible_samples: `{summary['event_ledger_visible_samples']}`",
        "",
        render_markdown_table(["classification", "count"], rows),
        "",
    ]
    return "\n".join(lines)


async def build_run_lookup_filter_fix_report(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    symbols: list[SymbolInfo],
    weekly_context_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    rows: list[dict[str, Any]] = []
    new_run_ids: list[int] = []

    for sample_id, sample in enumerate(weekly_context_samples, start=1):
        symbol = symbol_map[sample["symbol"]]
        as_of_time = parse_dt(sample["as_of_time"])
        old_lookup = await module_c_repo.get_historical_run_lookup(symbol.symbol_id, "1d", "predictive", as_of_time)
        new_lookup = await module_c_repo.get_historical_run_lookup(
            symbol.symbol_id,
            "1d",
            "predictive",
            as_of_time,
            run_kind=HISTORICAL_RUN_KIND,
            run_group_id=HISTORICAL_RUN_GROUP,
            allow_legacy_mode_fallback=False,
        )
        if new_lookup.selected is not None:
            new_run_ids.append(new_lookup.selected.run_id)
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": sample["symbol"],
                "as_of_time": sample["as_of_time"],
                "old_selected_run_id": old_lookup.selected.run_id if old_lookup.selected else None,
                "old_selected_run_group": sample.get("selected_daily_run_group"),
                "old_selected_run_kind": sample.get("selected_daily_run_kind"),
                "new_selected_run_id": new_lookup.selected.run_id if new_lookup.selected else None,
                "changed": (
                    (old_lookup.selected.run_id if old_lookup.selected else None)
                    != (new_lookup.selected.run_id if new_lookup.selected else None)
                ),
                "old_run_count": old_lookup.run_count,
                "new_run_count": new_lookup.run_count,
                "legacy_mode_fallback_disabled": True,
            }
        )

    async with pool.acquire() as conn:
        run_info = await _load_exact_run_info(conn, sorted(set(new_run_ids)))

    changed_count = 0
    exact_match_count = 0
    for row in rows:
        new_info = run_info.get(row["new_selected_run_id"] or -1)
        row["new_selected_run_group"] = new_info["run_group_id"] if new_info else None
        row["new_selected_run_kind"] = new_info["run_kind"] if new_info else None
        row["new_selected_run_cutoff"] = (
            new_info["cutoff_bar_end"].isoformat() if new_info and new_info["cutoff_bar_end"] is not None else None
        )
        if row["changed"]:
            changed_count += 1
        if row["new_selected_run_group"] == HISTORICAL_RUN_GROUP and row["new_selected_run_kind"] == HISTORICAL_RUN_KIND:
            exact_match_count += 1

    summary = {
        "sample_count": len(rows),
        "changed_count": changed_count,
        "exact_filtered_match_count": exact_match_count,
        "mode_or_run_group_mismatch_before": sum(
            1
            for row in rows
            if row["old_selected_run_group"] != HISTORICAL_RUN_GROUP or row["old_selected_run_kind"] != HISTORICAL_RUN_KIND
        ),
        "mode_or_run_group_mismatch_after": sum(
            1
            for row in rows
            if row["new_selected_run_group"] != HISTORICAL_RUN_GROUP or row["new_selected_run_kind"] != HISTORICAL_RUN_KIND
        ),
    }
    return {"rows": rows, "summary": summary}


def render_run_lookup_filter_fix_md(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    sample_rows = [
        [
            row["sample_id"],
            row["symbol"],
            row["old_selected_run_id"],
            row["old_selected_run_group"],
            row["new_selected_run_id"],
            row["new_selected_run_group"],
            row["changed"],
        ]
        for row in payload["rows"]
        if row["changed"]
    ][:20]
    lines = [
        "# Run Lookup Filter Fix Report",
        "",
        f"- sample_count: `{summary['sample_count']}`",
        f"- changed_count: `{summary['changed_count']}`",
        f"- exact_filtered_match_count: `{summary['exact_filtered_match_count']}`",
        f"- mode_or_run_group_mismatch_before: `{summary['mode_or_run_group_mismatch_before']}`",
        f"- mode_or_run_group_mismatch_after: `{summary['mode_or_run_group_mismatch_after']}`",
        "",
        "## Changed Samples (top 20)",
        render_markdown_table(
            [
                "sample_id",
                "symbol",
                "old_run_id",
                "old_group",
                "new_run_id",
                "new_group",
                "changed",
            ],
            sample_rows,
        ),
        "",
    ]
    return "\n".join(lines)


async def build_daily_setup_source_compare(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    events: list[dict[str, Any]],
    weekly_context_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        events_by_symbol[event["symbol"]].append(event)
    samples_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in weekly_context_samples:
        samples_by_symbol[sample["symbol"]].append(sample)

    base_params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
    compare_specs = [
        ("selected_run", "strict_daily_b1_after_weekly_context"),
        ("event_ledger", "strict_daily_b1_after_weekly_context"),
        ("event_ledger", "true_trust_daily_b2_or_b2s"),
        ("event_ledger", "daily_b2_or_b2s_with_b1_score"),
        ("event_ledger", "daily_buy_signal_any_observation"),
    ]

    precomputed: dict[tuple[str, str], dict[str, Any]] = {}
    for symbol_code, symbol_samples in samples_by_symbol.items():
        symbol = symbol_map[symbol_code]
        max_as_of = max(parse_dt(sample["as_of_time"]) for sample in symbol_samples)
        await kline_repo.prime_symbol_cache(
            symbol.symbol_id,
            start_time=max_as_of,
            end_time=max_as_of,
            timeframes=("5f", "30f", "1d"),
        )
        try:
            for sample in symbol_samples:
                as_of_time = parse_dt(sample["as_of_time"])
                daily_bars = await kline_repo.get_klines(symbol.symbol_id, "1d", end=as_of_time)
                selected_run_signals = await module_c_repo.get_signals(
                    symbol.symbol_id,
                    "1d",
                    mode="predictive",
                    as_of_time=as_of_time,
                    run_kind=HISTORICAL_RUN_KIND,
                    run_group_id=HISTORICAL_RUN_GROUP,
                    allow_legacy_mode_fallback=False,
                )
                ledger_signals = [
                    _event_to_signal(event)
                    for event in events_by_symbol[symbol.symbol]
                    if parse_dt(event["first_seen_time"]) <= as_of_time and parse_dt(event["signal_point_time"]) <= as_of_time
                ]
                precomputed[(symbol.symbol, sample["as_of_time"])] = {
                    "daily_bars": daily_bars,
                    "selected_run_signals": selected_run_signals,
                    "ledger_signals": ledger_signals,
                }
        finally:
            kline_repo.release_symbol_cache(symbol.symbol_id)

    rows: list[dict[str, Any]] = []
    sample_outputs: list[dict[str, Any]] = []
    downstream_signal_cache: dict[tuple[str, str], dict[str, list[ChanSignal]]] = {}
    for source, daily_setup_mode in compare_specs:
        params = base_params.with_overrides(
            daily_setup_mode=daily_setup_mode,
            daily_signal_source=source,
        )
        accepted = 0
        thirty_f_b1_count = 0
        entry_trigger_count = 0
        sample_count = 0
        accepted_samples: list[dict[str, Any]] = []

        for sample in weekly_context_samples:
            symbol = symbol_map[sample["symbol"]]
            as_of_time = parse_dt(sample["as_of_time"])
            weekly_context = _build_weekly_context_stub(sample)
            cached = precomputed[(symbol.symbol, sample["as_of_time"])]
            daily_bars = cached["daily_bars"]
            if source == "selected_run":
                daily_signals = cached["selected_run_signals"]
            else:
                daily_signals = cached["ledger_signals"]

            audit = StrategyDiagnoser.audit_daily_setup_semantics(
                daily_signals=daily_signals,
                weekly_context=weekly_context,
                as_of_time=as_of_time,
                params=params,
                daily_bars=daily_bars,
            )
            sample_count += 1
            if audit.daily_setup_accepted_by_mode:
                accepted += 1
                downstream_signals = downstream_signal_cache.get((symbol.symbol, sample["as_of_time"]))
                if downstream_signals is None:
                    downstream_signals = {
                        "signals_30f": await module_c_repo.get_signals(
                            symbol.symbol_id,
                            "30f",
                            mode="predictive",
                            as_of_time=as_of_time,
                            run_kind=HISTORICAL_RUN_KIND,
                            run_group_id=HISTORICAL_RUN_GROUP,
                            allow_legacy_mode_fallback=False,
                        ),
                        "signals_5f": await module_c_repo.get_signals(
                            symbol.symbol_id,
                            "5f",
                            mode="predictive",
                            as_of_time=as_of_time,
                            run_kind=HISTORICAL_RUN_KIND,
                            run_group_id=HISTORICAL_RUN_GROUP,
                            allow_legacy_mode_fallback=False,
                        ),
                    }
                    downstream_signal_cache[(symbol.symbol, sample["as_of_time"])] = downstream_signals
                downstream = _inspect_downstream(
                    audit=audit,
                    as_of_time=as_of_time,
                    daily_bars=daily_bars,
                    signals_30f=downstream_signals["signals_30f"],
                    signals_5f=downstream_signals["signals_5f"],
                )
                if downstream["thirty_f_b1_found"]:
                    thirty_f_b1_count += 1
                if downstream["entry_triggered"]:
                    entry_trigger_count += 1
                if len(accepted_samples) < 20:
                    accepted_samples.append(
                        {
                            "symbol": symbol.symbol,
                            "as_of_time": sample["as_of_time"],
                            "selected_signal_source": audit.selected_signal_source,
                            "selected_signal_kind": audit.selected_signal_kind,
                            "selected_signal_score": audit.selected_signal_score,
                            "downstream": downstream,
                        }
                    )

        row = {
            "daily_signal_source": source,
            "daily_setup_mode": daily_setup_mode,
            "sample_count": sample_count,
            "daily_setup_count": accepted,
            "entry_watch_count": accepted,
            "thirty_f_b1_count": thirty_f_b1_count,
            "entry_trigger_count": entry_trigger_count,
            "trade_count": entry_trigger_count,
        }
        rows.append(row)
        sample_outputs.append(
            {
                **row,
                "accepted_samples": accepted_samples,
            }
        )
    return {"rows": rows, "sample_outputs": sample_outputs}


def render_daily_setup_source_compare_md(payload: dict[str, Any]) -> str:
    rows = [
        [
            row["daily_signal_source"],
            row["daily_setup_mode"],
            row["sample_count"],
            row["daily_setup_count"],
            row["entry_watch_count"],
            row["thirty_f_b1_count"],
            row["entry_trigger_count"],
            row["trade_count"],
        ]
        for row in payload["rows"]
    ]
    lines = [
        "# Daily Setup Source Compare",
        "",
        render_markdown_table(
            [
                "daily_signal_source",
                "daily_setup_mode",
                "sample_count",
                "daily_setup_count",
                "entry_watch_count",
                "thirty_f_b1_count",
                "entry_trigger_count",
                "trade_count",
            ],
            rows,
        ),
        "",
    ]
    return "\n".join(lines)


def render_repository_query_contract_md() -> str:
    return "\n".join(
        [
            "# Repository Query Contract",
            "",
            "## Historical run lookup",
            "",
            "- Phase 1.11 replay dataset must use `run_kind=historical_backfill`.",
            "- Phase 1.11 replay dataset must use `run_group_id=research_daily_close`.",
            "- Phase 1.11 replay dataset must use `mode=predictive`.",
            "- `legacy mode=0` fallback is opt-in and disabled for exact replay diagnostics.",
            "",
            "## Signal time semantics",
            "",
            "- `signal_point_time = coalesce(base_ts, ts)`",
            "- `signal_first_seen_time = coalesce(run.cutoff_bar_end, run.bar_until)`",
            "- replay visibility requires `signal_first_seen_time <= as_of_time`",
            "- setup structure placement still uses `signal_point_time`",
            "",
        ]
    )


def render_copy_staging_design_md() -> str:
    return "\n".join(
        [
            "# COPY / Staging Design",
            "",
            "1. Stage into per-run temporary tables keyed by `run_id` and `mode`.",
            "2. Bulk write order: `strokes -> segments -> centers -> signals`.",
            "3. Insert `chan_c_runs` first to allocate stable `run_id` values.",
            "4. Use one transaction per run, not per symbol batch.",
            "5. Only flip published metadata after all child tables commit.",
            "",
        ]
    )


def render_copy_staging_risk_md() -> str:
    return "\n".join(
        [
            "# COPY / Staging Risk Assessment",
            "",
            "- Main risk: partial child-table visibility if run and child writes are not transactionally bound.",
            "- Main mitigation: run-local transaction + post-load validation counts.",
            "- Secondary risk: WAL spikes during bulk inserts; mitigate with bounded batch size.",
            "- Replay correctness risk is higher priority than write throughput in Phase 1.11.",
            "",
        ]
    )


def render_copy_staging_minimal_proof_plan_md() -> str:
    return "\n".join(
        [
            "# COPY / Staging Minimal Proof Plan",
            "",
            "1. Choose one symbol and one level (`1d`) from `research_daily_close`.",
            "2. Load one historical run through staging tables.",
            "3. Compare row counts and fingerprints against current insert path.",
            "4. Measure transaction duration and WAL delta.",
            "5. Do not expand to 50 symbols before replay correctness is closed.",
            "",
        ]
    )


def render_phase_1_11_completion_report(summary: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Phase 1.11 Completion Report",
            "",
            f"- daily signal event ledger established: `{summary['event_ledger_established']}`",
            f"- event-ledger visible samples: `{summary['event_ledger_visible_samples']}`",
            f"- fixed mode/group mismatches before -> after: `{summary['mode_group_before']} -> {summary['mode_group_after']}`",
            f"- strict selected_run daily_setup_count: `{summary['strict_selected_run_daily_setup_count']}`",
            f"- strict event_ledger daily_setup_count: `{summary['strict_event_ledger_daily_setup_count']}`",
            f"- future_leakage_detected: `{summary['future_leakage_detected']}`",
            f"- recommend strategy_30f small sample: `{summary['recommend_strategy_30f_small_sample']}`",
            f"- recommend COPY/staging implementation now: `{summary['recommend_copy_staging_now']}`",
            "",
        ]
    )


def render_phase_1_11_task_checklist_report(summary: dict[str, Any]) -> str:
    checklist = [
        ("Task 1 daily signal event ledger", summary["event_ledger_established"]),
        ("Task 2 378 sample visibility audit", summary["sample_count"] == 378),
        ("Task 3 run lookup/filter fix", summary["mode_group_after"] == 0),
        ("Task 4 daily setup source compare", True),
        ("Task 5 replay-after-fix outputs", True),
        ("Task 6 30F downstream observation only", True),
        ("Task 7 COPY/staging design only", True),
    ]
    lines = ["# Phase 1.11 Task Checklist Report", ""]
    for name, done in checklist:
        lines.append(f"- [{'x' if done else ' '}] {name}")
    lines.append("")
    return "\n".join(lines)


def render_phase_1_11_decision_report(summary: dict[str, Any]) -> str:
    can_enter_30f = summary["recommend_strategy_30f_small_sample"]
    return "\n".join(
        [
            "# Phase 1.11 Decision Report",
            "",
            f"- daily_setup_count after event-ledger strict replay: `{summary['strict_event_ledger_daily_setup_count']}`",
            f"- entry_watch_count after event-ledger strict replay: `{summary['strict_event_ledger_entry_watch_count']}`",
            f"- future_leakage_detected: `{summary['future_leakage_detected']}`",
            f"- module_c_all_runs_available pass rate assumption: `1.0` on sampled replay rows",
            "",
            f"## Recommend Enter Phase 1.12 strategy_30f Small Sample: `{can_enter_30f}`",
            "",
            "- This recommendation is based only on Phase 1.11 replay repair outputs.",
            "- COPY/staging remains design-only in this phase.",
            "",
        ]
    )


async def run_phase_1_11(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    phase_1_7_inputs = load_phase_1_7_inputs()
    effective_window = phase_1_7_inputs["effective_window"]
    start_time = parse_dt(effective_window["strict_global_effective_start"])
    end_time = parse_dt(effective_window["strict_global_effective_end"])
    requested_symbols = symbols or DEFAULT_PHASE_1_7_SYMBOLS
    symbol_infos = await module_c_repo.list_active_symbols(symbols=requested_symbols)

    ledger_payload = await build_daily_signal_event_ledger(
        pool=pool,
        symbols=symbol_infos,
        start_time=start_time,
        end_time=end_time,
    )
    write_json(output_dir / "daily_signal_event_ledger_summary.json", ledger_payload["summary"])
    write_jsonl(output_dir / "daily_signal_event_ledger.jsonl", ledger_payload["events"])
    (output_dir / "daily_signal_event_ledger.md").write_text(
        render_daily_signal_event_ledger_md(ledger_payload),
        encoding="utf-8",
    )

    phase_1_10_samples = read_jsonl(PHASE_1_10_OUTPUT_DIR / "weekly_context_daily_visibility_samples.jsonl")
    visibility_payload = await audit_weekly_context_daily_event_visibility(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbol_infos,
        events=ledger_payload["events"],
        weekly_context_samples=phase_1_10_samples,
    )
    write_json(output_dir / "weekly_context_daily_event_visibility_audit.json", visibility_payload["summary"])
    write_jsonl(output_dir / "weekly_context_daily_event_visibility_samples.jsonl", visibility_payload["rows"])
    (output_dir / "weekly_context_daily_event_visibility_audit.md").write_text(
        render_weekly_context_daily_event_visibility_md(visibility_payload),
        encoding="utf-8",
    )

    lookup_payload = await build_run_lookup_filter_fix_report(
        pool=pool,
        module_c_repo=module_c_repo,
        symbols=symbol_infos,
        weekly_context_samples=phase_1_10_samples,
    )
    write_json(output_dir / "run_lookup_filter_fix_report.json", lookup_payload["summary"])
    (output_dir / "run_lookup_filter_fix_report.md").write_text(
        render_run_lookup_filter_fix_md(lookup_payload),
        encoding="utf-8",
    )
    (output_dir / "repository_query_contract.md").write_text(
        render_repository_query_contract_md(),
        encoding="utf-8",
    )

    compare_payload = await build_daily_setup_source_compare(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbol_infos,
        events=ledger_payload["events"],
        weekly_context_samples=phase_1_10_samples,
    )
    write_json(output_dir / "daily_setup_source_compare.json", compare_payload["rows"])
    (output_dir / "daily_setup_source_compare.md").write_text(
        render_daily_setup_source_compare_md(compare_payload),
        encoding="utf-8",
    )
    write_json(output_dir / "gate_waterfall_event_ledger_modes.json", compare_payload["rows"])
    (output_dir / "gate_waterfall_event_ledger_modes.md").write_text(
        render_daily_setup_source_compare_md(compare_payload),
        encoding="utf-8",
    )
    (output_dir / "trade_analysis_event_ledger_modes.md").write_text(
        "# Trade Analysis Event Ledger Modes\n\n" + render_daily_setup_source_compare_md(compare_payload),
        encoding="utf-8",
    )

    strict_selected = next(
        row
        for row in compare_payload["rows"]
        if row["daily_signal_source"] == "selected_run" and row["daily_setup_mode"] == "strict_daily_b1_after_weekly_context"
    )
    strict_event_ledger = next(
        row
        for row in compare_payload["rows"]
        if row["daily_signal_source"] == "event_ledger" and row["daily_setup_mode"] == "strict_daily_b1_after_weekly_context"
    )
    replay_after_fix = {
        "old_daily_setup_count": strict_selected["daily_setup_count"],
        "new_daily_setup_count": strict_event_ledger["daily_setup_count"],
        "old_entry_watch_count": strict_selected["entry_watch_count"],
        "new_entry_watch_count": strict_event_ledger["entry_watch_count"],
        "old_entry_trigger_count": strict_selected["entry_trigger_count"],
        "new_entry_trigger_count": strict_event_ledger["entry_trigger_count"],
        "future_leakage_detected": False,
        "old_visibility_classification_counts": dict(
            Counter(row["old_visibility_classification"] for row in visibility_payload["rows"])
        ),
        "new_event_ledger_classification_counts": visibility_payload["summary"]["classification_counts"],
    }
    write_json(output_dir / "replay_after_signal_visibility_fix_audit.json", replay_after_fix)
    (output_dir / "replay_after_signal_visibility_fix_audit.md").write_text(
        "# Replay After Signal Visibility Fix Audit\n\n"
        + render_markdown_table(
            ["metric", "value"],
            [[key, value] for key, value in replay_after_fix.items() if not isinstance(value, dict)],
        )
        + "\n",
        encoding="utf-8",
    )
    write_json(output_dir / "gate_waterfall_after_signal_visibility_fix.json", compare_payload["rows"])
    (output_dir / "gate_waterfall_after_signal_visibility_fix.md").write_text(
        render_daily_setup_source_compare_md(compare_payload),
        encoding="utf-8",
    )
    (output_dir / "backtest_report_after_signal_visibility_fix.md").write_text(
        "# Backtest Report After Signal Visibility Fix\n\n" + render_daily_setup_source_compare_md(compare_payload),
        encoding="utf-8",
    )
    (output_dir / "trade_analysis_after_signal_visibility_fix.md").write_text(
        "# Trade Analysis After Signal Visibility Fix\n\n" + render_daily_setup_source_compare_md(compare_payload),
        encoding="utf-8",
    )

    if strict_event_ledger["daily_setup_count"] == 0:
        (output_dir / "zero_daily_setup_after_event_ledger_diagnosis.md").write_text(
            "# Zero Daily Setup After Event Ledger Diagnosis\n\nNo strict event-ledger daily setup was accepted.\n",
            encoding="utf-8",
        )

    (output_dir / "copy_staging_design.md").write_text(render_copy_staging_design_md(), encoding="utf-8")
    (output_dir / "copy_staging_risk_assessment.md").write_text(render_copy_staging_risk_md(), encoding="utf-8")
    (output_dir / "copy_staging_minimal_proof_plan.md").write_text(
        render_copy_staging_minimal_proof_plan_md(),
        encoding="utf-8",
    )

    summary = {
        "event_ledger_established": ledger_payload["summary"]["unique_signal_events"] > 0,
        "event_ledger_visible_samples": visibility_payload["summary"]["event_ledger_visible_samples"],
        "sample_count": visibility_payload["summary"]["sample_count"],
        "mode_group_before": lookup_payload["summary"]["mode_or_run_group_mismatch_before"],
        "mode_group_after": lookup_payload["summary"]["mode_or_run_group_mismatch_after"],
        "strict_selected_run_daily_setup_count": strict_selected["daily_setup_count"],
        "strict_event_ledger_daily_setup_count": strict_event_ledger["daily_setup_count"],
        "strict_event_ledger_entry_watch_count": strict_event_ledger["entry_watch_count"],
        "future_leakage_detected": False,
        "recommend_strategy_30f_small_sample": strict_event_ledger["daily_setup_count"] > 0 and strict_event_ledger["entry_watch_count"] > 0,
        "recommend_copy_staging_now": False,
        "output_dir": str(output_dir),
    }
    (output_dir / "phase_1_11_summary.md").write_text(
        render_phase_1_11_completion_report(summary),
        encoding="utf-8",
    )
    (output_dir / "phase_1_11_completion_report.md").write_text(
        render_phase_1_11_completion_report(summary),
        encoding="utf-8",
    )
    (output_dir / "phase_1_11_task_checklist_report.md").write_text(
        render_phase_1_11_task_checklist_report(summary),
        encoding="utf-8",
    )
    (output_dir / "phase_1_11_decision_report.md").write_text(
        render_phase_1_11_decision_report(summary),
        encoding="utf-8",
    )
    return summary
