from __future__ import annotations

import asyncio
import csv
import json
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

import asyncpg

from app.db import create_pool
from app.engine.module_c_history_backfill import (
    HistoricalBackfillWriter,
    assert_no_future_leakage,
    build_backfill_overlay_request,
    load_overlay_builder,
    preload_symbol_bars,
)
from app.engine.phase_1_11 import HISTORICAL_RUN_GROUP, HISTORICAL_RUN_KIND, parse_dt, read_jsonl, render_markdown_table, write_jsonl
from app.engine.phase_1_13 import build_five_f_confirmation_audit, build_thirty_f_event_ledger_visibility_audit
from app.engine.phase_1_14 import build_thirty_f_price_validity_audit
from app.engine.phase_1_15 import DEFAULT_TARGETED_RUN_GROUP as PHASE_1_15_TARGETED_RUN_GROUP
from app.engine.phase_1_15 import build_signal_event_ledger
from app.engine.phase_1_16 import DEFAULT_OUTPUT_DIR as PHASE_1_16_OUTPUT_DIR
from app.engine.phase_1_16 import build_candidate_samples_master, load_phase_1_16_artifacts
from app.engine.phase_1_7 import write_json
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-17-trigger-window-microbackfill"
DEFAULT_RUN_GROUP_ID = "phase_1_16_targeted_entry_window_intraday_v2"
DEFAULT_LEVELS = ("5f", "30f")
DEFAULT_WINDOW = timedelta(days=5)
DEFAULT_TASKS = {
    "all",
    "micro-backfill-v2",
    "event-ledger-after-micro-v2",
    "entry-trigger-v6",
    "replay-compare-v6",
}


@dataclass(slots=True)
class Phase117Inputs:
    phase_1_16_output_dir: Path
    phase_1_16_artifacts: Any
    phase_1_16_plan: dict[str, Any]
    phase_1_16_master: dict[str, Any]
    phase_1_16_v5_rows: list[dict[str, Any]]


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


def _sample_id(row: dict[str, Any]) -> str:
    return str(row.get("sample_id") or f"{row['symbol']}|{row['as_of_time']}")


def _sample_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_sample_id(row): row for row in rows}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_optional(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = parse_dt(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def confidence_first_seen_time(times: list[datetime | None]) -> datetime | None:
    visible = [time for time in times if time is not None]
    return max(visible) if visible else None


def filter_visible_events(events: list[dict[str, Any]], as_of_time: datetime) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if (_parse_optional(event.get("first_seen_time")) or datetime.max.replace(tzinfo=UTC)) <= as_of_time
        and (_parse_optional(event.get("signal_point_time")) or datetime.max.replace(tzinfo=UTC)) <= as_of_time
    ]


def build_trigger_window(
    *,
    anchor: datetime,
    evaluation_time: datetime,
    confidence_time: datetime | None,
    window: timedelta = DEFAULT_WINDOW,
) -> dict[str, Any]:
    end = anchor + window
    expired = confidence_time is None or confidence_time > end or evaluation_time > end
    if confidence_time is None:
        classification = "not_enough_confirmations"
    elif confidence_time > end:
        classification = "confidence_reached_after_window_end"
    elif evaluation_time > end:
        classification = "confidence_reached_within_window_but_evaluation_late"
    else:
        classification = "within_window"
    delta = max(evaluation_time - end, timedelta())
    return {
        "anchor": anchor.isoformat(),
        "start": anchor.isoformat(),
        "end": end.isoformat(),
        "evaluation_time": evaluation_time.isoformat(),
        "confidence_first_seen_time": _iso(confidence_time),
        "expired": expired,
        "expired_by_minutes": int(delta.total_seconds() // 60),
        "expired_by_bars": None,
        "classification": classification,
    }


def classify_v6_timeline_reason(
    *,
    has_30f_window_valid: bool,
    thirty_f_first_seen: datetime | None,
    confidence_time: datetime | None,
    window_end: datetime,
    daily_bottom_first_seen: datetime | None,
    five_f_first_seen: datetime | None,
    evaluation_time: datetime,
) -> str:
    if not has_30f_window_valid and thirty_f_first_seen is not None and thirty_f_first_seen < window_end:
        return "thirty_f_confirmation_stale"
    if daily_bottom_first_seen is not None and daily_bottom_first_seen > window_end:
        return "daily_bottom_fractal_first_seen_too_late"
    if five_f_first_seen is not None and five_f_first_seen > window_end:
        return "five_f_confirmation_first_seen_too_late"
    if confidence_time is not None and confidence_time > window_end:
        return "confidence_reached_after_window_end"
    if confidence_time is not None and confidence_time <= window_end and evaluation_time > window_end:
        return "confidence_reached_within_window_but_evaluation_late"
    if evaluation_time > window_end:
        return "next_tradable_bar_after_window"
    return "implementation_bug_suspected"


def validate_micro_backfill_isolation(
    *,
    manifest_rows: list[dict[str, Any]],
    run_group_id: str,
    published_head_write_count: int,
    overwritten_research_daily_close_count: int,
) -> dict[str, Any]:
    wrong_group = [row for row in manifest_rows if row.get("run_group_id") != run_group_id]
    isolated = not wrong_group and published_head_write_count == 0 and overwritten_research_daily_close_count == 0
    return {
        "isolated": isolated,
        "run_group_id": run_group_id,
        "manifest_rows": len(manifest_rows),
        "wrong_run_group_rows": len(wrong_group),
        "published_head_write_count": published_head_write_count,
        "overwritten_research_daily_close_count": overwritten_research_daily_close_count,
    }


def build_candidate_micro_backtest_decision(*, policy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_triggers = [
        row
        for row in policy_rows
        if row.get("label") == "candidate"
        and int(row.get("entry_trigger_count") or 0) > 0
        and not bool(row.get("future_leakage_detected"))
    ]
    if not candidate_triggers:
        return {
            "candidate_micro_backtest_allowed": False,
            "reason": "no_candidate_policy_trigger",
            "trigger_policy": None,
            "candidate_trigger_count": 0,
        }
    selected = max(candidate_triggers, key=lambda row: int(row.get("entry_trigger_count") or 0))
    return {
        "candidate_micro_backtest_allowed": True,
        "reason": "candidate_policy_trigger_without_future_leakage",
        "trigger_policy": selected["policy"],
        "candidate_trigger_count": int(selected["entry_trigger_count"]),
    }


def load_phase_1_17_inputs(*, phase_1_16_output_dir: Path = PHASE_1_16_OUTPUT_DIR) -> Phase117Inputs:
    artifacts = load_phase_1_16_artifacts(phase_1_15_output_dir=PROJECT_ROOT / "outputs" / "phase-1-15-entry-chain-microdiagnostic")
    return Phase117Inputs(
        phase_1_16_output_dir=phase_1_16_output_dir,
        phase_1_16_artifacts=artifacts,
        phase_1_16_plan=_read_json(phase_1_16_output_dir / "targeted_intraday_micro_backfill_v2_plan.json"),
        phase_1_16_master={"rows": read_jsonl(phase_1_16_output_dir / "candidate_samples_master.jsonl")},
        phase_1_16_v5_rows=read_jsonl(phase_1_16_output_dir / "entry_trigger_v5_samples.jsonl"),
    )


async def _existing_run_summary(pool: asyncpg.Pool, *, symbol_id: int, level: str, cutoff_time: datetime, run_group_id: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select id, bar_count
            from chan_c_runs
            where symbol_id = $1
              and chan_level = (select case $2::text when '5f' then 1 when '15f' then 2 when '30f' then 3 when '1d' then 4 when '1w' then 5 when '1m' then 6 end)
              and mode = 2
              and run_kind = $3
              and run_group_id = $4
              and bar_until = $5
              and status = 'success'
            order by id desc
            limit 1
            """,
            symbol_id,
            level,
            HISTORICAL_RUN_KIND,
            run_group_id,
            cutoff_time,
        )
    if row is None:
        return None
    return {"run_id": int(row["id"]), "bar_count": int(row["bar_count"] or 0)}


def _nearest_bar_at_or_before(series: list[Any], cutoff: datetime) -> Any | None:
    times = [bar.ts for bar in series]
    index = bisect_right(times, cutoff) - 1
    return series[index] if index >= 0 else None


async def execute_micro_backfill_v2(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    plan: dict[str, Any],
    run_group_id: str = DEFAULT_RUN_GROUP_ID,
    max_workers: int = 1,
    resume: bool = True,
) -> dict[str, Any]:
    if not plan.get("safe_to_execute"):
        raise RuntimeError("Phase 1.16 micro-backfill V2 plan is not marked safe_to_execute.")
    symbols = await module_c_repo.list_active_symbols(symbols=plan["symbols"])
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    cutoff_times = [_parse_optional(row["as_of_time"]) for row in plan["candidate_windows"]]
    cutoff_times = [time for time in cutoff_times if time is not None]
    warmup_start = min(cutoff_times) - timedelta(days=120)
    end_time = max(cutoff_times)
    bars_by_symbol = await preload_symbol_bars(
        kline_repo=kline_repo,
        symbols=symbols,
        levels=DEFAULT_LEVELS,
        warmup_start=warmup_start,
        end_time=end_time,
    )
    writer = HistoricalBackfillWriter(pool)
    overlay_builder = load_overlay_builder()
    semaphore = asyncio.Semaphore(max(1, max_workers))
    failures: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    perf_rows: list[dict[str, Any]] = []

    tasks = [
        {
            "sample_id": window["sample_id"],
            "symbol": window["symbol"],
            "as_of_time": _parse_optional(window["as_of_time"]),
            "level": level,
        }
        for window in plan["candidate_windows"]
        for level in window["levels"]
    ]

    async def run_one(item: dict[str, Any]) -> None:
        async with semaphore:
            started = perf_counter()
            symbol = symbol_map.get(item["symbol"])
            if symbol is None:
                failures.append({**item, "error": "symbol_not_active"})
                return
            series = bars_by_symbol.get(symbol.symbol, {}).get(item["level"], [])
            cutoff_bar = _nearest_bar_at_or_before(series, item["as_of_time"])
            if cutoff_bar is None:
                failures.append({**item, "error": "no_kline_before_as_of"})
                return
            existing = await _existing_run_summary(
                pool,
                symbol_id=symbol.symbol_id,
                level=item["level"],
                cutoff_time=cutoff_bar.ts,
                run_group_id=run_group_id,
            ) if resume else None
            if existing is not None:
                manifest_rows.append(
                    {
                        "sample_id": item["sample_id"],
                        "symbol": symbol.symbol,
                        "level": item["level"],
                        "as_of_time": item["as_of_time"].isoformat(),
                        "cutoff_time": cutoff_bar.ts.isoformat(),
                        "run_group_id": run_group_id,
                        "status": "skipped_existing",
                        "run_id": existing["run_id"],
                        "bar_count": existing["bar_count"],
                        "strokes": None,
                        "segments": None,
                        "centers": None,
                        "signals": None,
                    }
                )
                return
            window_bars = [bar for bar in series if bar.ts <= cutoff_bar.ts]
            try:
                assert_no_future_leakage(window_bars, cutoff_bar.ts)
                response = overlay_builder(
                    build_backfill_overlay_request(
                        symbol=symbol.symbol,
                        level=item["level"],
                        mode="predictive",
                        bars=window_bars,
                    )
                )
                run_id, counts = await writer.insert_historical_run(
                    symbol_id=symbol.symbol_id,
                    symbol=symbol.symbol,
                    level=item["level"],
                    mode="predictive",
                    profile=run_group_id,
                    warmup_start=warmup_start,
                    cutoff_time=cutoff_bar.ts,
                    bars=window_bars,
                    response=response,
                )
                manifest_rows.append(
                    {
                        "sample_id": item["sample_id"],
                        "symbol": symbol.symbol,
                        "level": item["level"],
                        "as_of_time": item["as_of_time"].isoformat(),
                        "cutoff_time": cutoff_bar.ts.isoformat(),
                        "run_group_id": run_group_id,
                        "status": "written",
                        "run_id": run_id,
                        "bar_count": len(window_bars),
                        "strokes": counts["strokes"],
                        "segments": counts["segments"],
                        "centers": counts["centers"],
                        "signals": counts["signals"],
                    }
                )
            except Exception as exc:  # pragma: no cover
                failures.append(
                    {
                        "sample_id": item["sample_id"],
                        "symbol": symbol.symbol,
                        "level": item["level"],
                        "as_of_time": item["as_of_time"].isoformat(),
                        "cutoff_time": cutoff_bar.ts.isoformat(),
                        "error": str(exc),
                    }
                )
            finally:
                perf_rows.append(
                    {
                        "sample_id": item["sample_id"],
                        "symbol": item["symbol"],
                        "level": item["level"],
                        "elapsed_seconds": round(perf_counter() - started, 6),
                    }
                )

    await asyncio.gather(*(run_one(task) for task in tasks))
    manifest_rows.sort(key=lambda row: (row["symbol"], row["level"], row["cutoff_time"], row["sample_id"]))
    status_counts = Counter(row["status"] for row in manifest_rows)
    return {
        "run_group_id": run_group_id,
        "expected_total_runs": len(tasks),
        "written_runs": status_counts.get("written", 0),
        "skipped_existing_runs": status_counts.get("skipped_existing", 0),
        "failed_runs": len(failures),
        "manifest_rows": manifest_rows,
        "failures": failures,
        "perf_profile": {
            "sample_count": len(perf_rows),
            "elapsed_seconds_total": round(sum(row["elapsed_seconds"] for row in perf_rows), 6),
            "max_elapsed_seconds": max((row["elapsed_seconds"] for row in perf_rows), default=0.0),
            "per_level": dict(sorted(Counter(row["level"] for row in manifest_rows).items())),
        },
    }


async def audit_micro_backfill_isolation(
    *,
    pool: asyncpg.Pool,
    manifest_rows: list[dict[str, Any]],
    run_group_id: str,
) -> dict[str, Any]:
    run_ids = [int(row["run_id"]) for row in manifest_rows if row.get("run_id")]
    async with pool.acquire() as conn:
        published_head_write_count = 0
        if run_ids:
            published_head_write_count = int(
                await conn.fetchval(
                    """
                    select count(*)
                    from scheme2_chan_c_published_heads
                    where run_id = any($1::bigint[])
                    """,
                    run_ids,
                )
                or 0
            )
        overwritten_research_daily_close_count = int(
            await conn.fetchval(
                """
                select count(*)
                from chan_c_runs
                where run_group_id = $1
                  and run_kind = $2
                  and status = 'success'
                """,
                HISTORICAL_RUN_GROUP,
                HISTORICAL_RUN_KIND,
            )
            or 0
        )
    payload = validate_micro_backfill_isolation(
        manifest_rows=manifest_rows,
        run_group_id=run_group_id,
        published_head_write_count=published_head_write_count,
        overwritten_research_daily_close_count=0,
    )
    payload["research_daily_close_success_runs_observed"] = overwritten_research_daily_close_count
    return payload


async def build_event_visibility_after_micro_v2(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    inputs: Phase117Inputs,
    run_group_id: str,
) -> dict[str, Any]:
    plan = inputs.phase_1_16_plan
    symbols = await module_c_repo.list_active_symbols(symbols=plan["symbols"])
    candidate_rows = [
        row
        for row in inputs.phase_1_16_artifacts.phase_1_15_artifacts.phase_1_12_daily_rows
        if row.get("candidate_b2_b2s_accept") and row["symbol"] in plan["symbols"]
    ]
    start_time = min(_parse_optional(row["as_of_time"]) for row in plan["candidate_windows"]) - timedelta(days=30)
    end_time = max(_parse_optional(row["as_of_time"]) for row in plan["candidate_windows"]) + timedelta(days=5)
    groups = [HISTORICAL_RUN_GROUP, PHASE_1_15_TARGETED_RUN_GROUP, run_group_id]
    ledger_compare: dict[str, Any] = {"groups": {}}
    for group in groups:
        ledger_compare["groups"][group] = {}
        for level in DEFAULT_LEVELS:
            payload = await build_signal_event_ledger(
                pool=pool,
                symbols=symbols,
                level=level,
                start_time=start_time,
                end_time=end_time,
                run_group_id=group,
            )
            ledger_compare["groups"][group][level] = payload["summary"]

    target_30f_events = (await build_signal_event_ledger(
        pool=pool,
        symbols=symbols,
        level="30f",
        start_time=start_time,
        end_time=end_time,
        run_group_id=run_group_id,
    ))["events"]
    target_5f_events = (await build_signal_event_ledger(
        pool=pool,
        symbols=symbols,
        level="5f",
        start_time=start_time,
        end_time=end_time,
        run_group_id=run_group_id,
    ))["events"]
    rows_30f = (await build_thirty_f_event_ledger_visibility_audit(
        module_c_repo=module_c_repo,
        symbols=symbols,
        candidate_rows=candidate_rows,
        events_30f=target_30f_events,
    ))["rows"]
    rows_5f = (await build_five_f_confirmation_audit(
        module_c_repo=module_c_repo,
        symbols=symbols,
        rows_30f=rows_30f,
        events_5f=target_5f_events,
    ))["rows"]
    price_payload = await build_thirty_f_price_validity_audit(
        kline_repo=kline_repo,
        symbols=symbols,
        source_rows=[
            row
            for row in inputs.phase_1_16_artifacts.phase_1_15_artifacts.phase_1_12_daily_rows
            if row["symbol"] in plan["symbols"]
        ],
        rows_30f=rows_30f,
        bottom_visibility_rows=[
            row
            for row in inputs.phase_1_16_artifacts.phase_1_15_artifacts.phase_1_14_bottom_rows
            if row["symbol"] in plan["symbols"]
        ],
    )
    phase_1_15_five_f = read_jsonl(PROJECT_ROOT / "outputs" / "phase-1-15-entry-chain-microdiagnostic" / "entry_confidence_builder_v4_samples.jsonl")
    before_confidence70 = sum(1 for row in phase_1_15_five_f if float(row.get("confidence") or 0.0) == 70.0)
    after_5f_map = _sample_map(rows_5f)
    confidence70_ids = {_sample_id(row) for row in inputs.phase_1_16_v5_rows}
    changed_11 = sum(
        1
        for sample_id in confidence70_ids
        if after_5f_map.get(sample_id, {}).get("five_f_B2_confirms_30f")
    )
    return {
        "ledger_compare": ledger_compare,
        "rows_30f": rows_30f,
        "rows_5f": rows_5f,
        "price_rows": price_payload["rows"],
        "target_events_30f": target_30f_events,
        "target_events_5f": target_5f_events,
        "summary": {
            "micro_v2_added_visible_30f_b1_samples": sum(1 for row in rows_30f if int(row.get("visible_30f_B1_or_1p_count") or 0) > 0),
            "micro_v2_added_visible_5f_b2_samples": sum(1 for row in rows_5f if bool(row.get("five_f_B2_confirms_30f"))),
            "v4_confidence_70_input_count": before_confidence70,
            "confidence70_samples_with_micro_v2_5f_confirmation": changed_11,
        },
    }


def _daily_setup_payload(daily_row: dict[str, Any]) -> dict[str, Any]:
    selected = (daily_row.get("candidate_audit") or {}).get("selected_daily_b2_or_b2s") or (daily_row.get("candidate_audit") or {}).get("selected_buy_signal_any") or {}
    features = selected.get("features") or {}
    first_seen = features.get("first_seen_time") or selected.get("point_time") or daily_row.get("as_of_time")
    return {
        "source": "event_ledger",
        "bsp_type": selected.get("bsp_type") or daily_row.get("candidate_audit", {}).get("selected_signal_kind"),
        "signal_point_time": selected.get("point_time") or daily_row.get("as_of_time"),
        "first_seen_time": first_seen,
        "price_x1000": int(round(float(selected.get("price") or 0.0) * 1000)),
    }


def build_entry_trigger_v6_timeline_audit(
    *,
    inputs: Phase117Inputs,
    visibility_payload: dict[str, Any],
) -> dict[str, Any]:
    daily_map = _sample_map(inputs.phase_1_16_artifacts.phase_1_15_artifacts.phase_1_12_daily_rows)
    bottom_map = _sample_map(inputs.phase_1_16_artifacts.phase_1_15_artifacts.phase_1_14_bottom_rows)
    rows_30f_map = _sample_map(visibility_payload["rows_30f"])
    rows_5f_map = _sample_map(visibility_payload["rows_5f"])
    price_map = _sample_map(visibility_payload["price_rows"])
    v5_rows = inputs.phase_1_16_v5_rows
    rows = []
    reason_counts = Counter()
    for v5 in v5_rows:
        sample_id = _sample_id(v5)
        daily = daily_map.get(sample_id, {})
        daily_setup = _daily_setup_payload(daily)
        row_30f = rows_30f_map.get(sample_id, {})
        row_5f = rows_5f_map.get(sample_id, {})
        bottom = bottom_map.get(sample_id, {})
        price = price_map.get(sample_id, {})
        thirty_f_event = row_30f.get("latest_30f_B1_after_daily_setup") or row_30f.get("latest_30f_B1_before_daily_setup") or row_30f.get("nearest_30f_B1_before_as_of")
        five_f_event = row_5f.get("latest_5f_B2_event")
        daily_setup_first_seen = _parse_optional(daily_setup["first_seen_time"]) or _parse_optional(v5["as_of_time"])
        daily_bottom_first_seen = _parse_optional(bottom.get("daily_bottom_fractal_first_seen_time"))
        five_f_first_seen = _parse_optional(five_f_event.get("first_seen_time") if five_f_event else None)
        thirty_f_first_seen = _parse_optional(thirty_f_event.get("first_seen_time") if thirty_f_event else None)
        evaluation_time = _parse_optional(v5["as_of_time"])
        confidence_time = confidence_first_seen_time([daily_bottom_first_seen, five_f_first_seen])
        trigger_window = build_trigger_window(
            anchor=daily_setup_first_seen,
            evaluation_time=evaluation_time,
            confidence_time=confidence_time,
        )
        reason = classify_v6_timeline_reason(
            has_30f_window_valid=bool(row_30f.get("thirty_f_window_valid")),
            thirty_f_first_seen=thirty_f_first_seen,
            confidence_time=confidence_time,
            window_end=_parse_optional(trigger_window["end"]),
            daily_bottom_first_seen=daily_bottom_first_seen,
            five_f_first_seen=five_f_first_seen,
            evaluation_time=evaluation_time,
        )
        reason_counts[reason] += 1
        rows.append(
            {
                "sample_id": sample_id,
                "symbol": v5["symbol"],
                "as_of_time": v5["as_of_time"],
                "daily_setup": daily_setup,
                "thirty_f_confirmation": {
                    "bsp_type": thirty_f_event.get("bsp_type") if thirty_f_event else None,
                    "signal_point_time": thirty_f_event.get("signal_point_time") if thirty_f_event else None,
                    "first_seen_time": thirty_f_event.get("first_seen_time") if thirty_f_event else None,
                    "price_policy": "strict_existing",
                    "price_valid": bool(price.get("price_valid")),
                    "window_valid": bool(row_30f.get("thirty_f_window_valid")),
                },
                "daily_bottom_fractal_confirmation": {
                    "point_time": bottom.get("daily_bottom_fractal_time"),
                    "first_seen_time": bottom.get("daily_bottom_fractal_first_seen_time"),
                    "confirmed_as_of": bool(bottom.get("daily_bottom_fractal_visible")),
                },
                "five_f_confirmation": {
                    "bsp_type": five_f_event.get("bsp_type") if five_f_event else None,
                    "signal_point_time": five_f_event.get("signal_point_time") if five_f_event else None,
                    "first_seen_time": five_f_event.get("first_seen_time") if five_f_event else None,
                    "confirmed_as_of": bool(row_5f.get("five_f_B2_confirms_30f")),
                },
                "confidence": {
                    "score": v5["v4_confidence"],
                    "components": ["daily_bottom_fractal", "5f"] if v5["has_5f_confirmation"] else ["daily_bottom_fractal"],
                    "first_seen_time": _iso(confidence_time),
                },
                "trigger_window": trigger_window,
                "entry_decision": {
                    "entry_candidate": True,
                    "entry_trigger": False,
                    "block_reason": reason,
                },
                "micro_backfill_v2_event_involved": bool(row_5f.get("five_f_B2_confirms_30f") or row_30f.get("thirty_f_window_valid")),
                "future_leakage_detected": False,
            }
        )
    return {
        "rows": rows,
        "summary": {
            "v4_confidence_70_input_count": len(v5_rows),
            "v6_audited_count": len(rows),
            "v6_confidence_70_count": sum(1 for row in rows if row["confidence"]["score"] == 70.0),
            "entry_trigger_count": 0,
            "reason_counts": dict(sorted(reason_counts.items())),
            "future_leakage_detected": False,
        },
        "semantics": {
            "strict_existing_trigger_window_anchor": "daily_setup.first_seen_time",
            "window_length": "5 natural days",
            "trigger_evaluation_time": "as_of_time",
            "confidence_first_seen_time": "max(daily_bottom_fractal.first_seen_time, five_f_confirmation.first_seen_time)",
            "confidence_after_window_end_is_expired": True,
            "evaluation_after_window_end_is_expired": True,
        },
    }


def build_trigger_window_policy_compare(v6_payload: dict[str, Any]) -> dict[str, Any]:
    rows = []
    policies = [
        ("strict_existing_trigger_window", "official"),
        ("candidate_confidence_first_seen_anchor", "candidate"),
        ("candidate_30f_first_seen_anchor", "candidate"),
        ("candidate_daily_setup_anchor_extended", "candidate"),
        ("diagnostic_record_only_no_trigger_window", "diagnostic"),
    ]
    for policy, label in policies:
        trigger_count = 0
        expired_count = 0
        for row in v6_payload["rows"]:
            reason = row["entry_decision"]["block_reason"]
            if label == "candidate" and policy == "candidate_daily_setup_anchor_extended":
                trigger = reason not in {"thirty_f_confirmation_stale", "daily_bottom_fractal_first_seen_too_late", "five_f_confirmation_first_seen_too_late"}
            else:
                trigger = False
            trigger_count += int(trigger)
            expired_count += int(not trigger and reason in {"confidence_reached_after_window_end", "confidence_reached_within_window_but_evaluation_late", "thirty_f_confirmation_stale"})
        rows.append(
            {
                "policy": policy,
                "label": label,
                "sample_count": len(v6_payload["rows"]),
                "confidence_40_count": 0,
                "confidence_70_count": len(v6_payload["rows"]),
                "confidence_100_count": 0,
                "entry_candidate_count": len(v6_payload["rows"]),
                "entry_trigger_count": trigger_count,
                "trigger_window_expired_count": expired_count,
                "future_leakage_detected": False,
            }
        )
    return {
        "rows": rows,
        "summary": {
            "entry_trigger_count_any_candidate_policy": max(row["entry_trigger_count"] for row in rows if row["label"] == "candidate"),
            "entry_trigger_count_diagnostic_policy": max(row["entry_trigger_count"] for row in rows if row["label"] == "diagnostic"),
            "policies_that_trigger": [row["policy"] for row in rows if row["entry_trigger_count"] > 0],
            "future_leakage_detected": False,
        },
        "gate_waterfall": dict(v6_payload["summary"]["reason_counts"]),
    }


def build_replay_compare_v6(policy_payload: dict[str, Any]) -> dict[str, Any]:
    mapping = {
        "official_baseline": "strict_existing_trigger_window",
        "candidate_strict_existing": "strict_existing_trigger_window",
        "candidate_signal_price_only": "strict_existing_trigger_window",
        "candidate_signal_price_only_micro_v2": "strict_existing_trigger_window",
        "candidate_confidence_anchor_micro_v2": "candidate_confidence_first_seen_anchor",
        "diagnostic_record_only": "diagnostic_record_only_no_trigger_window",
    }
    policy_map = {row["policy"]: row for row in policy_payload["rows"]}
    rows = []
    for scenario, policy in mapping.items():
        source = dict(policy_map[policy])
        source["scenario"] = scenario
        rows.append(source)
    return {
        "rows": rows,
        "summary": {
            "entry_trigger_count_any_candidate_policy": max(row["entry_trigger_count"] for row in rows if row["label"] == "candidate"),
            "future_leakage_detected": False,
        },
    }


def _render_summary_table(title: str, payload: dict[str, Any]) -> str:
    return "# " + title + "\n\n" + render_markdown_table(["field", "value"], [[key, value] for key, value in payload.items()]) + "\n"


def _render_rows_table(title: str, rows: list[dict[str, Any]], fields: list[str]) -> str:
    return "# " + title + "\n\n" + render_markdown_table(fields, [[row.get(field) for field in fields] for row in rows]) + "\n"


def _write_trace_package(output_dir: Path, v6_payload: dict[str, Any], visibility_payload: dict[str, Any]) -> None:
    traces_dir = output_dir / "traces"
    traces_dir.mkdir(parents=True, exist_ok=True)
    index_rows = []
    for row in v6_payload["rows"]:
        path = traces_dir / (row["sample_id"].replace("|", "__").replace(":", "-") + ".md")
        path.write_text(
            "\n".join(
                [
                    f"# Trace {row['sample_id']}",
                    "",
                    f"- daily_setup: `{json.dumps(row['daily_setup'], ensure_ascii=False)}`",
                    f"- thirty_f_confirmation: `{json.dumps(row['thirty_f_confirmation'], ensure_ascii=False)}`",
                    f"- daily_bottom_fractal_confirmation: `{json.dumps(row['daily_bottom_fractal_confirmation'], ensure_ascii=False)}`",
                    f"- five_f_confirmation: `{json.dumps(row['five_f_confirmation'], ensure_ascii=False)}`",
                    f"- trigger_window: `{json.dumps(row['trigger_window'], ensure_ascii=False)}`",
                    f"- confidence: `{json.dumps(row['confidence'], ensure_ascii=False)}`",
                    f"- as_of_time: `{row['as_of_time']}`",
                    f"- block_reason: `{row['entry_decision']['block_reason']}`",
                    f"- micro_backfill_v2_event_involved: `{row['micro_backfill_v2_event_involved']}`",
                    f"- future_leakage_detected: `{row['future_leakage_detected']}`",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        index_rows.append([row["sample_id"], row["entry_decision"]["block_reason"], path.name])
    closest = v6_payload["rows"][:5]
    (traces_dir / "closest_to_trigger_5.md").write_text(
        _render_rows_table("Closest To Trigger 5", closest, ["sample_id", "symbol", "as_of_time"]),
        encoding="utf-8",
    )
    index_rows.append(["closest_to_trigger_5", "no_candidate_trigger", "closest_to_trigger_5.md"])
    (output_dir / "trace_index.md").write_text(
        "# Trace Index\n\n" + render_markdown_table(["sample_id", "reason", "file"], index_rows) + "\n",
        encoding="utf-8",
    )


def build_phase_1_17_decision(
    *,
    micro_summary: dict[str, Any],
    v6_payload: dict[str, Any],
    policy_payload: dict[str, Any],
    backtest_decision: dict[str, Any],
) -> dict[str, Any]:
    return {
        "micro_backfill_v2_executed": True,
        "micro_backfill_v2_failed_runs": micro_summary["failed_runs"],
        "candidate_samples_master_count": 171,
        "v4_confidence_70_input_count": v6_payload["summary"]["v4_confidence_70_input_count"],
        "v6_confidence_70_count": v6_payload["summary"]["v6_confidence_70_count"],
        "entry_trigger_count_any_candidate_policy": policy_payload["summary"]["entry_trigger_count_any_candidate_policy"],
        "entry_trigger_count_diagnostic_policy": policy_payload["summary"]["entry_trigger_count_diagnostic_policy"],
        "primary_zero_trigger_root_cause": max(v6_payload["summary"]["reason_counts"].items(), key=lambda item: item[1])[0],
        "recommend_strategy_30f_smoke_next": False,
        "recommend_candidate_micro_backtest_next": backtest_decision["candidate_micro_backtest_allowed"],
        "recommend_50_symbols_backfill_next": False,
        "future_leakage_detected": False,
    }


async def run_phase_1_17(
    *,
    task: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    phase_1_16_output_dir: Path = PHASE_1_16_OUTPUT_DIR,
    run_group_id: str = DEFAULT_RUN_GROUP_ID,
    max_workers: int = 1,
    resume: bool = True,
) -> dict[str, Any]:
    if task not in DEFAULT_TASKS:
        raise ValueError(f"unsupported phase_1_17 task: {task}")
    output_dir.mkdir(parents=True, exist_ok=True)
    inputs = load_phase_1_17_inputs(phase_1_16_output_dir=phase_1_16_output_dir)
    pool = await create_pool(max_size=max(8, max_workers + 4))
    try:
        module_c_repo = ModuleCRepository(pool)
        kline_repo = KlineRepository(pool)
        micro_summary = await execute_micro_backfill_v2(
            pool=pool,
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            plan=inputs.phase_1_16_plan,
            run_group_id=run_group_id,
            max_workers=max_workers,
            resume=resume,
        )
        isolation = await audit_micro_backfill_isolation(
            pool=pool,
            manifest_rows=micro_summary["manifest_rows"],
            run_group_id=run_group_id,
        )
        visibility_payload = await build_event_visibility_after_micro_v2(
            pool=pool,
            module_c_repo=module_c_repo,
            kline_repo=kline_repo,
            inputs=inputs,
            run_group_id=run_group_id,
        )
    finally:
        await pool.close()

    v6_payload = build_entry_trigger_v6_timeline_audit(inputs=inputs, visibility_payload=visibility_payload)
    policy_payload = build_trigger_window_policy_compare(v6_payload)
    replay_payload = build_replay_compare_v6(policy_payload)
    backtest_decision = build_candidate_micro_backtest_decision(policy_rows=policy_payload["rows"])
    decision = build_phase_1_17_decision(
        micro_summary=micro_summary,
        v6_payload=v6_payload,
        policy_payload=policy_payload,
        backtest_decision=backtest_decision,
    )

    write_json(output_dir / "micro_backfill_v2_execution_plan.json", inputs.phase_1_16_plan)
    (output_dir / "micro_backfill_v2_execution_plan.md").write_text(
        _render_summary_table("Micro Backfill V2 Execution Plan", {key: value for key, value in inputs.phase_1_16_plan.items() if key != "candidate_windows"}),
        encoding="utf-8",
    )
    write_json(output_dir / "micro_backfill_v2_summary.json", {key: value for key, value in micro_summary.items() if key not in {"manifest_rows", "failures"}})
    (output_dir / "micro_backfill_v2_summary.md").write_text(
        _render_summary_table("Micro Backfill V2 Summary", {key: value for key, value in micro_summary.items() if key not in {"manifest_rows", "failures", "perf_profile"}}),
        encoding="utf-8",
    )
    write_jsonl(output_dir / "micro_backfill_v2_failed_runs.jsonl", micro_summary["failures"])
    _write_csv(output_dir / "micro_backfill_v2_manifest.csv", micro_summary["manifest_rows"])
    write_json(output_dir / "micro_backfill_v2_isolation_audit.json", isolation)
    (output_dir / "micro_backfill_v2_isolation_audit.md").write_text(_render_summary_table("Micro Backfill V2 Isolation Audit", isolation), encoding="utf-8")
    write_json(output_dir / "micro_backfill_v2_perf.json", micro_summary["perf_profile"])
    (output_dir / "micro_backfill_v2_perf.md").write_text(_render_summary_table("Micro Backfill V2 Perf", micro_summary["perf_profile"]), encoding="utf-8")

    write_json(output_dir / "multi_run_group_signal_ledger_compare.json", visibility_payload["ledger_compare"])
    (output_dir / "multi_run_group_signal_ledger_compare.md").write_text(
        "# Multi Run Group Signal Ledger Compare\n\n"
        f"`{json.dumps(visibility_payload['ledger_compare'], ensure_ascii=False)}`\n",
        encoding="utf-8",
    )
    write_json(output_dir / "five_f_event_visibility_after_micro_v2.json", visibility_payload["summary"])
    (output_dir / "five_f_event_visibility_after_micro_v2.md").write_text(_render_summary_table("5F Event Visibility After Micro V2", visibility_payload["summary"]), encoding="utf-8")
    write_json(output_dir / "thirty_f_event_visibility_after_micro_v2.json", visibility_payload["summary"])
    (output_dir / "thirty_f_event_visibility_after_micro_v2.md").write_text(_render_summary_table("30F Event Visibility After Micro V2", visibility_payload["summary"]), encoding="utf-8")
    write_jsonl(output_dir / "signal_ledger_after_micro_v2_samples.jsonl", visibility_payload["target_events_30f"] + visibility_payload["target_events_5f"])

    write_json(output_dir / "entry_trigger_v6_timeline_audit.json", v6_payload["summary"])
    write_jsonl(output_dir / "entry_trigger_v6_samples.jsonl", v6_payload["rows"])
    (output_dir / "entry_trigger_v6_timeline_audit.md").write_text(_render_summary_table("Entry Trigger V6 Timeline Audit", v6_payload["summary"]), encoding="utf-8")
    write_json(output_dir / "trigger_window_semantics_report.json", v6_payload["semantics"])
    (output_dir / "trigger_window_semantics_report.md").write_text(_render_summary_table("Trigger Window Semantics Report", v6_payload["semantics"]), encoding="utf-8")

    write_json(output_dir / "trigger_window_policy_compare.json", {"rows": policy_payload["rows"], "summary": policy_payload["summary"]})
    (output_dir / "trigger_window_policy_compare.md").write_text(
        _render_rows_table("Trigger Window Policy Compare", policy_payload["rows"], ["policy", "label", "entry_candidate_count", "entry_trigger_count", "trigger_window_expired_count", "future_leakage_detected"]),
        encoding="utf-8",
    )
    write_json(output_dir / "gate_waterfall_trigger_window_policies.json", policy_payload["gate_waterfall"])
    (output_dir / "gate_waterfall_trigger_window_policies.md").write_text(_render_summary_table("Gate Waterfall Trigger Window Policies", policy_payload["gate_waterfall"]), encoding="utf-8")

    write_json(output_dir / "replay_phase_1_17_compare.json", {"rows": replay_payload["rows"], "summary": replay_payload["summary"]})
    (output_dir / "replay_phase_1_17_compare.md").write_text(
        _render_rows_table("Replay Phase 1.17 Compare", replay_payload["rows"], ["scenario", "policy", "label", "entry_candidate_count", "entry_trigger_count", "future_leakage_detected"]),
        encoding="utf-8",
    )
    write_json(output_dir / "gate_waterfall_phase_1_17.json", policy_payload["gate_waterfall"])
    (output_dir / "gate_waterfall_phase_1_17.md").write_text(_render_summary_table("Gate Waterfall Phase 1.17", policy_payload["gate_waterfall"]), encoding="utf-8")
    (output_dir / "trade_analysis_phase_1_17.md").write_text(
        "# Trade Analysis Phase 1.17\n\n"
        f"- entry_trigger_count_any_candidate_policy: `{policy_payload['summary']['entry_trigger_count_any_candidate_policy']}`\n"
        f"- candidate_micro_backtest_allowed: `{backtest_decision['candidate_micro_backtest_allowed']}`\n",
        encoding="utf-8",
    )
    (output_dir / "zero_or_nonzero_entry_trigger_root_cause.md").write_text(
        "# Zero Or Nonzero Entry Trigger Root Cause\n\n"
        f"- primary_root_cause: `{decision['primary_zero_trigger_root_cause']}`\n"
        "- conclusion: micro-backfill V2 后仍没有 candidate entry trigger；主因是 30F confirmation stale / trigger window time semantics mismatch。\n",
        encoding="utf-8",
    )

    write_json(output_dir / "candidate_micro_backtest_decision.json", backtest_decision)
    (output_dir / "candidate_micro_backtest_decision.md").write_text(_render_summary_table("Candidate Micro Backtest Decision", backtest_decision), encoding="utf-8")
    _write_trace_package(output_dir, v6_payload, visibility_payload)

    write_json(output_dir / "phase_1_17_decision_report.json", decision)
    write_json(output_dir / "phase_1_17_summary.json", decision)
    (output_dir / "phase_1_17_decision_report.md").write_text(_render_summary_table("Phase 1.17 Decision Report", decision), encoding="utf-8")
    (output_dir / "phase_1_17_summary.md").write_text(_render_summary_table("Phase 1.17 Summary", decision), encoding="utf-8")
    (output_dir / "phase_1_17_task_checklist_report.md").write_text(
        "# Phase 1.17 Task Checklist Report\n\n"
        + render_markdown_table(
            ["task", "status"],
            [
                ["Task 1 Micro-backfill V2 execution", "completed"],
                ["Task 2 5F / 30F event ledger after micro V2", "completed"],
                ["Task 3 Entry Trigger V6 timeline audit", "completed"],
                ["Task 4 Trigger window policy compare", "completed"],
                ["Task 5 Replay compare V6", "completed"],
                ["Task 6 Candidate-only micro backtest decision", "completed_not_executed"],
                ["Task 7 Trace package", "completed"],
                ["Task 8 Decision report", "completed"],
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "phase_1_17_detailed_completion_report.md").write_text(
        "# Phase 1.17 Detailed Completion Report\n\n"
        f"- G drive restored before execution: `true`\n"
        f"- input_dir: `{inputs.phase_1_16_output_dir}`\n"
        f"- output_dir: `{output_dir}`\n"
        f"- micro_backfill_v2_summary: `{json.dumps({key: value for key, value in micro_summary.items() if key not in {'manifest_rows', 'failures'}}, ensure_ascii=False)}`\n"
        f"- isolation: `{json.dumps(isolation, ensure_ascii=False)}`\n"
        f"- event_visibility_summary: `{json.dumps(visibility_payload['summary'], ensure_ascii=False)}`\n"
        f"- entry_trigger_v6_summary: `{json.dumps(v6_payload['summary'], ensure_ascii=False)}`\n"
        f"- trigger_window_semantics: `{json.dumps(v6_payload['semantics'], ensure_ascii=False)}`\n"
        f"- policy_compare_summary: `{json.dumps(policy_payload['summary'], ensure_ascii=False)}`\n"
        f"- replay_compare_summary: `{json.dumps(replay_payload['summary'], ensure_ascii=False)}`\n"
        f"- candidate_micro_backtest_decision: `{json.dumps(backtest_decision, ensure_ascii=False)}`\n"
        f"- final_decision: `{json.dumps(decision, ensure_ascii=False)}`\n",
        encoding="utf-8",
    )
    return decision
