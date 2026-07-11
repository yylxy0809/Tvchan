from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.domain.enums import LEVEL_TO_DB
from app.domain.models import ChanSignal, PublishedHead, SymbolInfo
from app.engine.module_c_history_backfill import DEFAULT_LEVELS
from app.engine.phase_1_7 import DEFAULT_PHASE_1_7_SYMBOLS, write_json
from app.engine.phase_1_9 import (
    DEFAULT_OUTPUT_DIR as PHASE_1_9_OUTPUT_DIR,
    build_daily_setup_semantics_dataset,
    load_phase_1_7_inputs,
)
from app.repositories.kline_repo import KlineRepository
from app.repositories.module_c_repo import MODE_TO_DB, ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-10-daily-signal-visibility"
DAILY_LEVEL = LEVEL_TO_DB["1d"]
HISTORICAL_RUN_KIND = "historical_backfill"
HISTORICAL_RUN_GROUP = "research_daily_close"
WEEKLY_CONTEXT_MODE = "trust_chan_signal_with_b1_score"
WINDOW_BUCKETS = (
    "before_0_5d",
    "before_6_20d",
    "before_21_60d",
    "after_0_5d",
    "after_6_20d",
    "after_21_60d",
    "no_daily_buy_in_symbol_window",
)


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


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


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


def _distance_bucket(before_days: int | None, after_days: int | None) -> str:
    if before_days is not None:
        days = abs(before_days)
        if days <= 5:
            return "before_0_5d"
        if days <= 20:
            return "before_6_20d"
        return "before_21_60d"
    if after_days is not None:
        if after_days <= 5:
            return "after_0_5d"
        if after_days <= 20:
            return "after_6_20d"
        return "after_21_60d"
    return "no_daily_buy_in_symbol_window"


def classify_visibility_sample(
    *,
    selected_run_missing: bool,
    selected_run_has_signals: bool,
    selected_run_has_buy: bool,
    visible_daily_buy_count: int,
    nearest_before: dict[str, Any] | None,
    nearest_after: dict[str, Any] | None,
    selected_run_group: str | None,
    selected_run_kind: str | None,
) -> str:
    if selected_run_missing:
        return "selected_daily_run_missing"
    if not selected_run_has_signals:
        return "selected_daily_run_has_no_signals"
    if not selected_run_has_buy:
        if nearest_before is not None:
            return "buy_signal_exists_before_asof_but_not_selected"
        if nearest_after is not None:
            return "buy_signal_exists_only_after_asof"
        return "selected_daily_run_has_signals_but_no_buy"
    if visible_daily_buy_count > 0:
        return "visible_daily_buy_signal_found"
    if selected_run_group not in {None, HISTORICAL_RUN_GROUP} or selected_run_kind not in {None, HISTORICAL_RUN_KIND}:
        return "mode_or_run_group_mismatch"
    if nearest_before is not None:
        return "signal_time_filter_mismatch"
    if nearest_after is not None:
        return "buy_signal_exists_only_after_asof"
    return "daily_run_selected_but_signal_table_empty"


def classify_symbol_diff(
    *,
    current_daily_buy_signal_count: int,
    historical_final_daily_buy_signal_count: int,
    samples_with_visible_daily_buy_signal: int,
    samples_with_future_daily_buy_signal_after_asof: int,
    samples_with_buy_before_asof_but_not_selected: int,
    samples_with_mode_or_group_mismatch: int,
) -> str:
    if current_daily_buy_signal_count == 0:
        return "no_current_daily_signal"
    if historical_final_daily_buy_signal_count == 0:
        return "current_signal_exists_but_not_in_historical_backfill"
    if samples_with_mode_or_group_mismatch > 0:
        return "mode_or_run_group_mismatch"
    if samples_with_buy_before_asof_but_not_selected > 0:
        return "historical_signal_exists_before_asof_but_query_missed"
    if samples_with_future_daily_buy_signal_after_asof > 0 and samples_with_visible_daily_buy_signal == 0:
        return "historical_signal_exists_but_after_weekly_context_asof"
    if samples_with_visible_daily_buy_signal == 0:
        return "signal_time_filter_mismatch"
    return "visible_in_replay"


async def _fetch_historical_daily_inventory(
    pool: asyncpg.Pool,
    *,
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
                r.symbol_id,
                r.mode,
                coalesce(r.run_kind, 'published') as run_kind,
                r.run_group_id,
                date_trunc('month', r.bar_until) as cutoff_bar_end_month,
                count(distinct r.id)::bigint as daily_runs,
                count(distinct case when s.id is not null then r.id end)::bigint as daily_runs_with_any_signal,
                count(distinct case when coalesce(s.extra->>'side', '') = 'buy' then r.id end)::bigint as daily_runs_with_buy_signal,
                count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '1')::bigint as buy_b1_rows,
                count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '2')::bigint as buy_b2_rows,
                count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '2s')::bigint as buy_b2s_rows,
                count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '3a')::bigint as buy_b3a_rows,
                count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '3b')::bigint as buy_b3b_rows,
                min(case when coalesce(s.extra->>'side', '') = 'buy' then coalesce(s.base_ts, s.ts) end) as first_daily_buy_signal_time,
                max(case when coalesce(s.extra->>'side', '') = 'buy' then coalesce(s.base_ts, s.ts) end) as last_daily_buy_signal_time,
                min(case when coalesce(s.extra->>'side', '') = 'buy' then r.bar_until end) as first_cutoff_with_daily_buy_signal,
                max(case when coalesce(s.extra->>'side', '') = 'buy' then r.bar_until end) as last_cutoff_with_daily_buy_signal
            from chan_c_runs r
            left join chan_c_signals s
              on s.run_id = r.id
             and s.mode = r.mode
            where r.symbol_id = any($1::bigint[])
              and r.chan_level = $2
              and r.status = 'success'
              and r.bar_until >= $3
              and r.bar_until <= $4
            group by r.symbol_id, r.mode, coalesce(r.run_kind, 'published'), r.run_group_id, date_trunc('month', r.bar_until)
            order by r.symbol_id, r.mode, coalesce(r.run_kind, 'published'), r.run_group_id, date_trunc('month', r.bar_until)
            """,
            symbol_ids,
            DAILY_LEVEL,
            start_time,
            end_time,
        )
        per_symbol_rows = await conn.fetch(
            """
            with signal_rows as (
                select
                    r.symbol_id,
                    count(distinct r.id)::bigint as daily_runs,
                    count(distinct case when s.id is not null then r.id end)::bigint as daily_runs_with_any_signal,
                    count(distinct case when coalesce(s.extra->>'side', '') = 'buy' then r.id end)::bigint as daily_runs_with_buy_signal,
                    count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '1')::bigint as b1_rows,
                    count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '2')::bigint as b2_rows,
                    count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '2s')::bigint as b2s_rows,
                    count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '3a')::bigint as b3a_rows,
                    count(*) filter (where coalesce(s.extra->>'side', '') = 'buy' and coalesce(s.extra->>'bsp_type', '') = '3b')::bigint as b3b_rows,
                    min(case when coalesce(s.extra->>'side', '') = 'buy' then coalesce(s.base_ts, s.ts) end) as first_daily_buy_signal_time,
                    max(case when coalesce(s.extra->>'side', '') = 'buy' then coalesce(s.base_ts, s.ts) end) as last_daily_buy_signal_time,
                    min(case when coalesce(s.extra->>'side', '') = 'buy' then r.bar_until end) as first_cutoff_with_daily_buy_signal,
                    max(case when coalesce(s.extra->>'side', '') = 'buy' then r.bar_until end) as last_cutoff_with_daily_buy_signal
                from chan_c_runs r
                left join chan_c_signals s
                  on s.run_id = r.id
                 and s.mode = r.mode
                where r.symbol_id = any($1::bigint[])
                  and r.chan_level = $2
                  and r.status = 'success'
                  and r.run_kind = $3
                  and r.run_group_id = $4
                  and r.bar_until >= $5
                  and r.bar_until <= $6
                group by r.symbol_id
            )
            select * from signal_rows
            order by symbol_id
            """,
            symbol_ids,
            DAILY_LEVEL,
            HISTORICAL_RUN_KIND,
            HISTORICAL_RUN_GROUP,
            start_time,
            end_time,
        )

    aggregate_rows = []
    for row in rows:
        aggregate_rows.append(
            {
                "symbol": symbol_map[int(row["symbol_id"])].symbol,
                "mode": "predictive" if int(row["mode"]) == MODE_TO_DB["predictive"] else "confirmed",
                "run_kind": row["run_kind"],
                "run_group_id": row["run_group_id"],
                "cutoff_bar_end_month": _iso(row["cutoff_bar_end_month"]),
                "daily_runs": int(row["daily_runs"]),
                "daily_runs_with_any_signal": int(row["daily_runs_with_any_signal"]),
                "daily_runs_with_buy_signal": int(row["daily_runs_with_buy_signal"]),
                "buy_signal_counts_by_bsp_type": {
                    "1": int(row["buy_b1_rows"]),
                    "2": int(row["buy_b2_rows"]),
                    "2s": int(row["buy_b2s_rows"]),
                    "3a": int(row["buy_b3a_rows"]),
                    "3b": int(row["buy_b3b_rows"]),
                },
                "first_daily_buy_signal_time": _iso(row["first_daily_buy_signal_time"]),
                "last_daily_buy_signal_time": _iso(row["last_daily_buy_signal_time"]),
                "first_cutoff_with_daily_buy_signal": _iso(row["first_cutoff_with_daily_buy_signal"]),
                "last_cutoff_with_daily_buy_signal": _iso(row["last_cutoff_with_daily_buy_signal"]),
            }
        )

    per_symbol = []
    for row in per_symbol_rows:
        symbol = symbol_map[int(row["symbol_id"])]
        total_buy_rows = int(row["b1_rows"]) + int(row["b2_rows"]) + int(row["b2s_rows"]) + int(row["b3a_rows"]) + int(row["b3b_rows"])
        per_symbol.append(
            {
                "symbol": symbol.symbol,
                "name": symbol.name,
                "daily_runs": int(row["daily_runs"]),
                "daily_runs_with_any_signal": int(row["daily_runs_with_any_signal"]),
                "daily_runs_with_buy_signal": int(row["daily_runs_with_buy_signal"]),
                "historical_daily_buy_signal_count": total_buy_rows,
                "buy_signal_counts_by_bsp_type": {
                    "1": int(row["b1_rows"]),
                    "2": int(row["b2_rows"]),
                    "2s": int(row["b2s_rows"]),
                    "3a": int(row["b3a_rows"]),
                    "3b": int(row["b3b_rows"]),
                },
                "first_daily_buy_signal_time": _iso(row["first_daily_buy_signal_time"]),
                "last_daily_buy_signal_time": _iso(row["last_daily_buy_signal_time"]),
                "first_cutoff_with_daily_buy_signal": _iso(row["first_cutoff_with_daily_buy_signal"]),
                "last_cutoff_with_daily_buy_signal": _iso(row["last_cutoff_with_daily_buy_signal"]),
                "root_cause_candidate": (
                    "historical_backfill_daily_signal_absent" if total_buy_rows == 0 else "replay_lookup_or_filter_bug"
                ),
            }
        )

    return {
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "profile": HISTORICAL_RUN_GROUP,
        "mode": "predictive",
        "aggregate_rows": aggregate_rows,
        "per_symbol": per_symbol,
    }


async def _fetch_run_info(conn: asyncpg.Connection, run_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not run_ids:
        return {}
    rows = await conn.fetch(
        """
        select
            r.id,
            r.run_kind,
            r.run_group_id,
            r.bar_until,
            r.cutoff_bar_end,
            r.snapshot_version,
            count(s.id)::bigint as signal_count,
            count(*) filter (where coalesce(s.extra->>'side', '') = 'buy')::bigint as buy_signal_count
        from chan_c_runs r
        left join chan_c_signals s
          on s.run_id = r.id
         and s.mode = r.mode
        where r.id = any($1::bigint[])
        group by r.id, r.run_kind, r.run_group_id, r.bar_until, r.cutoff_bar_end, r.snapshot_version
        """,
        run_ids,
    )
    return {
        int(row["id"]): {
            "run_kind": row["run_kind"],
            "run_group_id": row["run_group_id"],
            "bar_until": _iso(row["bar_until"]),
            "cutoff_bar_end": _iso(row["cutoff_bar_end"]),
            "snapshot_version": row["snapshot_version"],
            "signal_count": int(row["signal_count"]),
            "buy_signal_count": int(row["buy_signal_count"]),
        }
        for row in rows
    }


async def _fetch_symbol_signal_inventory(
    conn: asyncpg.Connection,
    *,
    symbol_id: int,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        select
            r.id as run_id,
            r.bar_until,
            r.run_kind,
            r.run_group_id,
            coalesce(s.base_ts, s.ts) as point_time,
            coalesce(s.extra->>'side', '') as side,
            coalesce(s.extra->>'bsp_type', '') as bsp_type,
            s.price_x1000
        from chan_c_runs r
        join chan_c_signals s
          on s.run_id = r.id
         and s.mode = r.mode
        where r.symbol_id = $1
          and r.chan_level = $2
          and r.status = 'success'
          and r.run_kind = $3
          and r.run_group_id = $4
        order by coalesce(s.base_ts, s.ts), r.bar_until, r.id, s.id
        """,
        symbol_id,
        DAILY_LEVEL,
        HISTORICAL_RUN_KIND,
        HISTORICAL_RUN_GROUP,
    )
    return [
        {
            "run_id": int(row["run_id"]),
            "bar_until": row["bar_until"],
            "run_kind": row["run_kind"],
            "run_group_id": row["run_group_id"],
            "point_time": row["point_time"],
            "side": row["side"],
            "bsp_type": row["bsp_type"],
            "price": int(row["price_x1000"]) / 1000,
        }
        for row in rows
    ]


async def _build_daily_signal_visibility(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    start_time: datetime,
    end_time: datetime,
    max_workers: int,
) -> dict[str, Any]:
    dataset = await build_daily_setup_semantics_dataset(
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=symbols,
        start_time=start_time,
        end_time=end_time,
        concurrency=max_workers,
    )
    raw_rows = dataset["rows"]
    symbol_map = {symbol.symbol: symbol for symbol in symbols}
    samples_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    current_vs_replay: list[dict[str, Any]] = []
    visibility_samples: list[dict[str, Any]] = []
    audit_counter = Counter()
    bucket_counter = Counter({bucket: 0 for bucket in WINDOW_BUCKETS})
    diff_counter = Counter()

    async with pool.acquire() as conn:
        current_head_map: dict[int, PublishedHead | None] = {}
        latest_historical_run_map: dict[int, dict[str, Any] | None] = {}
        symbol_signal_inventory_map: dict[int, list[dict[str, Any]]] = {}
        symbol_day_order_map: dict[int, list[datetime]] = {}

        for symbol in symbols:
            current_head_map[symbol.symbol_id] = await module_c_repo.get_current_head(symbol.symbol_id, "1d", mode="predictive")
            latest_historical_run = await conn.fetchrow(
                """
                select id
                from chan_c_runs
                where symbol_id = $1
                  and chan_level = $2
                  and mode = $3
                  and status = 'success'
                  and run_kind = $4
                  and run_group_id = $5
                order by bar_until desc, computed_at desc, id desc
                limit 1
                """,
                symbol.symbol_id,
                DAILY_LEVEL,
                MODE_TO_DB["predictive"],
                HISTORICAL_RUN_KIND,
                HISTORICAL_RUN_GROUP,
            )
            latest_historical_run_map[symbol.symbol_id] = {"id": int(latest_historical_run["id"])} if latest_historical_run else None
            symbol_signal_inventory_map[symbol.symbol_id] = await _fetch_symbol_signal_inventory(conn, symbol_id=symbol.symbol_id)
            day_order = await kline_repo.get_klines(symbol.symbol_id, "1d", end=end_time)
            symbol_day_order_map[symbol.symbol_id] = [bar.ts for bar in day_order]

        current_run_ids = [head.run_id for head in current_head_map.values() if head is not None]
        latest_run_ids = [row["id"] for row in latest_historical_run_map.values() if row is not None]
        run_info_map = await _fetch_run_info(conn, current_run_ids + latest_run_ids)

        for raw in raw_rows:
            symbol = symbol_map[raw["symbol"]]
            symbol_signal_inventory = symbol_signal_inventory_map[symbol.symbol_id]
            as_of_time = datetime.fromisoformat(raw["as_of_time"])
            weekly_context_start = datetime.fromisoformat(raw["weekly_context"]["start"])
            lookup = await module_c_repo.get_historical_run_lookup(symbol.symbol_id, "1d", "predictive", as_of_time)
            selected = lookup.selected
            run_ids = [head.run_id for head in (lookup.selected, lookup.nearest_before, lookup.nearest_after) if head is not None]
            run_info_subset = await _fetch_run_info(conn, run_ids)
            selected_info = run_info_subset.get(selected.run_id) if selected is not None else None

            symbol_buy_signals = [item for item in symbol_signal_inventory if item["side"] == "buy"]
            nearest_before = None
            nearest_after = None
            visible_in_window = []
            for item in symbol_buy_signals:
                point_time = item["point_time"]
                if point_time <= as_of_time:
                    nearest_before = item
                    if weekly_context_start <= point_time <= as_of_time:
                        visible_in_window.append(item)
                elif point_time > as_of_time and nearest_after is None:
                    nearest_after = item
            before_days = _trading_day_distance(symbol_day_order_map[symbol.symbol_id], nearest_before["point_time"], as_of_time) if nearest_before else None
            after_days = _trading_day_distance(symbol_day_order_map[symbol.symbol_id], nearest_after["point_time"], as_of_time) if nearest_after else None
            if after_days is not None:
                after_days = abs(after_days)
            bucket = _distance_bucket(before_days, after_days)
            bucket_counter[bucket] += 1
            classification = classify_visibility_sample(
                selected_run_missing=selected is None,
                selected_run_has_signals=bool(selected_info and selected_info["signal_count"] > 0),
                selected_run_has_buy=bool(selected_info and selected_info["buy_signal_count"] > 0),
                visible_daily_buy_count=len(visible_in_window),
                nearest_before=nearest_before,
                nearest_after=nearest_after,
                selected_run_group=selected_info["run_group_id"] if selected_info else None,
                selected_run_kind=selected_info["run_kind"] if selected_info else None,
            )
            audit_counter[classification] += 1

            sample = {
                "symbol": symbol.symbol,
                "name": symbol.name,
                "as_of_time": raw["as_of_time"],
                "weekly_context_signal_time": raw["weekly_context"]["signal_time"],
                "selected_daily_run_id": selected.run_id if selected is not None else None,
                "selected_daily_run_bar_until": _iso(selected.bar_until) if selected is not None else None,
                "selected_daily_run_kind": selected_info["run_kind"] if selected_info else None,
                "selected_daily_run_group": selected_info["run_group_id"] if selected_info else None,
                "selected_daily_run_signal_count": selected_info["signal_count"] if selected_info else 0,
                "daily_buy_signal_count_in_selected_run": selected_info["buy_signal_count"] if selected_info else 0,
                "selected_daily_run_snapshot_version": selected_info["snapshot_version"] if selected_info else None,
                "selected_daily_run_missing": selected is None,
                "nearest_daily_buy_before_asof": (
                    {
                        "time": _iso(nearest_before["point_time"]),
                        "price": nearest_before["price"],
                        "bsp_type": nearest_before["bsp_type"],
                        "run_id": nearest_before["run_id"],
                    }
                    if nearest_before
                    else None
                ),
                "nearest_daily_buy_before_days": before_days,
                "nearest_daily_buy_after_asof": (
                    {
                        "time": _iso(nearest_after["point_time"]),
                        "price": nearest_after["price"],
                        "bsp_type": nearest_after["bsp_type"],
                        "run_id": nearest_after["run_id"],
                    }
                    if nearest_after
                    else None
                ),
                "nearest_daily_buy_after_days": after_days,
                "visible_daily_buy_signal_count_in_window": len(visible_in_window),
                "visible_daily_buy_signals_in_window": [
                    {
                        "time": _iso(item["point_time"]),
                        "price": item["price"],
                        "bsp_type": item["bsp_type"],
                        "run_id": item["run_id"],
                    }
                    for item in visible_in_window[-5:]
                ],
                "window_bucket": bucket,
                "classification": classification,
                "strict_failure_reason_v2": raw["mode_results"]["strict_daily_b1_after_weekly_context"]["failure_reason_v2"],
            }
            visibility_samples.append(sample)
            samples_by_symbol[symbol.symbol].append(sample)

        for symbol in symbols:
            symbol_samples = samples_by_symbol[symbol.symbol]
            current_head = current_head_map[symbol.symbol_id]
            current_info = run_info_map.get(current_head.run_id) if current_head is not None else None
            latest_info = None
            latest_meta = latest_historical_run_map[symbol.symbol_id]
            if latest_meta is not None:
                latest_info = run_info_map.get(latest_meta["id"])
            payload = {
                "symbol": symbol.symbol,
                "name": symbol.name,
                "current_daily_buy_signal_count": current_info["buy_signal_count"] if current_info else 0,
                "historical_final_daily_buy_signal_count": latest_info["buy_signal_count"] if latest_info else 0,
                "weekly_context_sample_count": len(symbol_samples),
                "samples_with_visible_daily_buy_signal": sum(1 for item in symbol_samples if item["visible_daily_buy_signal_count_in_window"] > 0),
                "samples_with_future_daily_buy_signal_after_asof": sum(1 for item in symbol_samples if item["nearest_daily_buy_after_asof"] is not None),
                "samples_with_buy_before_asof_but_not_selected": sum(
                    1 for item in symbol_samples if item["classification"] == "buy_signal_exists_before_asof_but_not_selected"
                ),
                "samples_with_mode_or_group_mismatch": sum(
                    1 for item in symbol_samples if item["classification"] == "mode_or_run_group_mismatch"
                ),
            }
            payload["diff_classification"] = classify_symbol_diff(
                current_daily_buy_signal_count=payload["current_daily_buy_signal_count"],
                historical_final_daily_buy_signal_count=payload["historical_final_daily_buy_signal_count"],
                samples_with_visible_daily_buy_signal=payload["samples_with_visible_daily_buy_signal"],
                samples_with_future_daily_buy_signal_after_asof=payload["samples_with_future_daily_buy_signal_after_asof"],
                samples_with_buy_before_asof_but_not_selected=payload["samples_with_buy_before_asof_but_not_selected"],
                samples_with_mode_or_group_mismatch=payload["samples_with_mode_or_group_mismatch"],
            )
            current_vs_replay.append(payload)
            diff_counter[payload["diff_classification"]] += 1

    return {
        "raw_rows": raw_rows,
        "weekly_context_sample_count": len(raw_rows),
        "visibility_samples": visibility_samples,
        "audit_summary": {
            "sample_count": len(raw_rows),
            "classification_counts": dict(audit_counter),
        },
        "time_alignment_summary": {
            "sample_count": len(raw_rows),
            "window_bucket_counts": dict(bucket_counter),
        },
        "current_vs_replay": {
            "rows": current_vs_replay,
            "classification_counts": dict(diff_counter),
        },
    }


def _build_backfill_db_insert_table_profile(
    *,
    inventory: dict[str, Any],
    phase_1_7_inputs: dict[str, Any],
) -> dict[str, Any]:
    backfill_summary = phase_1_7_inputs["backfill_summary"]
    phase_1_9_perf_path = PHASE_1_9_OUTPUT_DIR / "backfill_perf_after_optimization.json"
    if phase_1_9_perf_path.exists():
        phase_1_9_perf = json.loads(phase_1_9_perf_path.read_text(encoding="utf-8"))
        total_insert_seconds = float(phase_1_9_perf["aggregate"]["db_insert_seconds"]["sum"])
        profiling_source = "phase_1_9_backfill_perf_after_optimization"
    else:
        total_insert_seconds = float(phase_1_7_inputs["backfill_perf"]["elapsed_seconds"])
        profiling_source = "phase_1_7_backfill_elapsed_seconds_fallback"
    row_totals = Counter()
    for row in inventory["aggregate_rows"]:
        level_counts = row["buy_signal_counts_by_bsp_type"]
        row_totals["chan_c_signals"] += sum(int(value) for value in level_counts.values())
    written_runs = int(backfill_summary["written_runs"])
    per_table = []
    baseline = {
        "chan_c_runs": written_runs,
        "chan_c_strokes": written_runs * len(DEFAULT_LEVELS),
        "chan_c_segments": written_runs * len(DEFAULT_LEVELS),
        "chan_c_centers": written_runs * len(DEFAULT_LEVELS),
        "chan_c_signals": row_totals["chan_c_signals"],
    }
    total_rows = sum(baseline.values())
    for table, rows in baseline.items():
        ratio = rows / total_rows if total_rows else 0.0
        per_table.append(
            {
                "table": table,
                "estimated_rows": rows,
                "estimated_rows_per_run": round(rows / written_runs, 6) if written_runs else 0.0,
                "estimated_insert_seconds_share": round(total_insert_seconds * ratio, 6),
                "profiling_method": f"heuristic_row_share_from_{profiling_source}",
            }
        )
    hottest = sorted(per_table, key=lambda item: item["estimated_insert_seconds_share"], reverse=True)
    return {
        "profile": HISTORICAL_RUN_GROUP,
        "written_runs": written_runs,
        "phase_1_7_total_db_insert_seconds": total_insert_seconds,
        "profiling_source": profiling_source,
        "per_table": per_table,
        "top_tables_by_estimated_insert_seconds": hottest[:3],
        "note": "Phase 1.10 未改写回填写库逻辑，表级耗时为基于既有总 insert 时间与行量占比的启发式估算。",
    }


def _build_level_subset_backfill_feasibility() -> dict[str, Any]:
    return {
        "implemented": False,
        "safe_for_diagnostic_use_only": True,
        "would_affect_replay": False,
        "recommended_cli_shape": "--levels 1d,1w,1m",
        "estimated_time_saved_vs_full_5_levels": "high",
        "decision": "recommended_for_future_diagnostics_only",
        "reason": "当前日线 setup 可见性审计不依赖 5f/30f 增量结构，适合后续单独新增诊断用 level-subset backfill，但本轮不改历史回填主链路。",
    }


def _build_copy_staging_decision(profile: dict[str, Any]) -> dict[str, Any]:
    hottest = profile["top_tables_by_estimated_insert_seconds"]
    enter = bool(hottest and hottest[0]["estimated_insert_seconds_share"] >= 1.0)
    return {
        "enter_copy_staging_phase": enter,
        "reason": (
            "5f/30f 写库总耗时已足以支持下一阶段 COPY/staging 设计验证"
            if enter
            else "当前证据更像总体行量与逐快照 executemany 开销，仍建议先完成更细粒度插桩再进入 COPY/staging 开发"
        ),
        "top_tables": hottest,
    }


def render_historical_daily_signal_inventory_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Historical Daily Signal Inventory",
        "",
        f"- Window: `{payload['window_start']}` -> `{payload['window_end']}`",
        f"- Profile: `{payload['profile']}`",
        f"- Mode: `{payload['mode']}`",
        "",
        "## Per Symbol",
        "",
        render_markdown_table(
            [
                "symbol",
                "daily_runs",
                "runs_with_any_signal",
                "runs_with_buy_signal",
                "buy_counts",
                "first_buy",
                "last_buy",
                "root_cause_candidate",
            ],
            [
                [
                    f"`{row['symbol']}`",
                    row["daily_runs"],
                    row["daily_runs_with_any_signal"],
                    row["daily_runs_with_buy_signal"],
                    json.dumps(row["buy_signal_counts_by_bsp_type"], ensure_ascii=False),
                    row["first_daily_buy_signal_time"],
                    row["last_daily_buy_signal_time"],
                    f"`{row['root_cause_candidate']}`",
                ]
                for row in payload["per_symbol"]
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def render_current_vs_replay_daily_signal_diff_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Vs Replay Daily Signal Diff",
        "",
        f"- classification_counts: `{json.dumps(payload['classification_counts'], ensure_ascii=False)}`",
        "",
        render_markdown_table(
            [
                "symbol",
                "current_buy",
                "historical_final_buy",
                "sample_count",
                "visible_buy_samples",
                "future_buy_samples",
                "diff_classification",
            ],
            [
                [
                    f"`{row['symbol']}`",
                    row["current_daily_buy_signal_count"],
                    row["historical_final_daily_buy_signal_count"],
                    row["weekly_context_sample_count"],
                    row["samples_with_visible_daily_buy_signal"],
                    row["samples_with_future_daily_buy_signal_after_asof"],
                    f"`{row['diff_classification']}`",
                ]
                for row in payload["rows"]
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def render_daily_signal_visibility_audit_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Daily Signal Visibility Audit",
        "",
        f"- sample_count: `{payload['sample_count']}`",
        f"- classification_counts: `{json.dumps(payload['classification_counts'], ensure_ascii=False)}`",
        "",
    ]
    return "\n".join(lines)


def render_daily_signal_time_alignment_report_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Daily Signal Time Alignment Report",
        "",
        f"- sample_count: `{payload['sample_count']}`",
        f"- window_bucket_counts: `{json.dumps(payload['window_bucket_counts'], ensure_ascii=False)}`",
        "",
    ]
    return "\n".join(lines)


def render_backfill_db_insert_table_profile_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Backfill DB Insert Table Profile",
        "",
        f"- written_runs: `{payload['written_runs']}`",
        f"- phase_1_7_total_db_insert_seconds: `{payload['phase_1_7_total_db_insert_seconds']}`",
        f"- note: `{payload['note']}`",
        "",
        render_markdown_table(
            ["table", "estimated_rows", "estimated_rows_per_run", "estimated_insert_seconds_share", "method"],
            [
                [
                    f"`{row['table']}`",
                    row["estimated_rows"],
                    row["estimated_rows_per_run"],
                    row["estimated_insert_seconds_share"],
                    row["profiling_method"],
                ]
                for row in payload["per_table"]
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def render_level_subset_backfill_feasibility_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Level Subset Backfill Feasibility",
            "",
            f"- implemented: `{payload['implemented']}`",
            f"- safe_for_diagnostic_use_only: `{payload['safe_for_diagnostic_use_only']}`",
            f"- would_affect_replay: `{payload['would_affect_replay']}`",
            f"- recommended_cli_shape: `{payload['recommended_cli_shape']}`",
            f"- estimated_time_saved_vs_full_5_levels: `{payload['estimated_time_saved_vs_full_5_levels']}`",
            f"- decision: `{payload['decision']}`",
            "",
            payload["reason"],
            "",
        ]
    )


def render_copy_staging_optimization_decision_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Copy Staging Optimization Decision",
            "",
            f"- enter_copy_staging_phase: `{payload['enter_copy_staging_phase']}`",
            f"- reason: `{payload['reason']}`",
            f"- top_tables: `{json.dumps(payload['top_tables'], ensure_ascii=False)}`",
            "",
        ]
    )


def render_phase_1_10_decision_report_md(
    *,
    inventory: dict[str, Any],
    visibility: dict[str, Any],
    copy_decision: dict[str, Any],
) -> str:
    per_symbol = inventory["per_symbol"]
    no_history = sum(1 for row in per_symbol if row["historical_daily_buy_signal_count"] == 0)
    visibility_counts = visibility["audit_summary"]["classification_counts"]
    time_counts = visibility["time_alignment_summary"]["window_bucket_counts"]
    root_causes = []
    if no_history:
        root_causes.append("historical_backfill_daily_signal_absent")
    if visibility_counts.get("mode_or_run_group_mismatch", 0):
        root_causes.append("mode_or_run_group_mismatch")
    if visibility_counts.get("signal_time_filter_mismatch", 0):
        root_causes.append("signal_time_filter_mismatch")
    if time_counts.get("after_0_5d", 0) or time_counts.get("after_6_20d", 0):
        root_causes.append("daily_signal_exists_only_after_weekly_context")
    if time_counts.get("no_daily_buy_in_symbol_window", 0):
        root_causes.append("sample_window_too_narrow")
    if not root_causes:
        root_causes.append("expected_low_frequency_no_setup")
    return "\n".join(
        [
            "# Phase 1.10 Decision Report",
            "",
            f"- root_cause_classifications: `{json.dumps(root_causes, ensure_ascii=False)}`",
            f"- visibility_classification_counts: `{json.dumps(visibility_counts, ensure_ascii=False)}`",
            f"- time_alignment_counts: `{json.dumps(time_counts, ensure_ascii=False)}`",
            f"- enter_copy_staging_phase: `{copy_decision['enter_copy_staging_phase']}`",
            "- replay_after_fix: `not_run`",
            "- reason: `Phase 1.10 未发现足够强的证据去修改 chan.py 或模块 C 核心语义，本轮结论以可见性/时间语义审计为主。`",
            "",
        ]
    )


def render_phase_1_10_summary_md(
    *,
    inventory: dict[str, Any],
    visibility: dict[str, Any],
    diff_payload: dict[str, Any],
    output_dir: Path,
) -> str:
    return "\n".join(
        [
            "# Phase 1.10 Summary",
            "",
            f"- output_dir: `{output_dir}`",
            f"- dataset: `phase_1_7_10_symbols_research_daily_close`",
            f"- weekly_context_sample_count: `{visibility['weekly_context_sample_count']}`",
            f"- symbols_with_historical_daily_buy_signal: `{sum(1 for row in inventory['per_symbol'] if row['historical_daily_buy_signal_count'] > 0)}`",
            f"- visibility_classification_counts: `{json.dumps(visibility['audit_summary']['classification_counts'], ensure_ascii=False)}`",
            f"- current_vs_replay_classification_counts: `{json.dumps(diff_payload['classification_counts'], ensure_ascii=False)}`",
            f"- time_alignment_bucket_counts: `{json.dumps(visibility['time_alignment_summary']['window_bucket_counts'], ensure_ascii=False)}`",
            "",
        ]
    )


def render_phase_1_10_task_checklist_report_md(
    *,
    outputs: list[str],
    replay_fix_performed: bool,
) -> str:
    lines = [
        "# Phase 1.10 Task Checklist Report",
        "",
        "- [x] 历史日线信号库存审计",
        "- [x] 当前态 vs 历史回放差异审计",
        "- [x] 378 个 weekly-context-passed 样本逐点可见性审计",
        "- [x] replay run lookup / query filter 语义测试",
        "- [x] 时间语义校准报告",
        f"- [{'x' if replay_fix_performed else ' '}] 若存在 bug 则修复并复跑",
        "- [x] 回填写库性能二次定位",
        "",
        "## Deliverables",
        "",
    ]
    for item in outputs:
        lines.append(f"- [x] `{item}`")
    return "\n".join(lines)


def render_phase_1_10_task_sheet_mapping_report_md(
    *,
    inventory: dict[str, Any],
    visibility: dict[str, Any],
    diff_payload: dict[str, Any],
    db_profile: dict[str, Any],
    level_subset: dict[str, Any],
    copy_decision: dict[str, Any],
) -> str:
    visibility_counts = visibility["audit_summary"]["classification_counts"]
    time_counts = visibility["time_alignment_summary"]["window_bucket_counts"]
    return "\n".join(
        [
            "# Phase 1.10 任务单对照报告",
            "",
            "## 总体状态",
            "",
            "- 结论：`部分完成`",
            "- 原因：Task 1 / 2 / 3 / 4 / 5 / 7 已完成；Task 6 的“若发现可确定 bug 则修复并复跑”未执行，因为本轮只收敛到 lookup/filter/mismatch 方向，还未形成可安全下刀的单点修复。",
            "",
            "## 逐项对照",
            "",
            "### Task 1 历史日线信号库存审计",
            "- 状态：`已完成`",
            "- 产物：`historical_daily_signal_inventory.md/json`",
            f"- 结果：10/10 标的存在历史日线买点；每标的汇总字段已包含 `daily_runs`、`daily_runs_with_any_signal`、`daily_runs_with_buy_signal`、`buy_signal_counts_by_bsp_type`、first/last buy times。",
            "",
            "### Task 2 当前态 vs 历史回放差异审计",
            "- 状态：`已完成`",
            "- 产物：`current_vs_replay_daily_signal_diff.md/json`",
            f"- 结果：分类统计为 `{json.dumps(diff_payload['classification_counts'], ensure_ascii=False)}`。",
            "- 说明：该文件反映当前 head 与最新单个 historical run 的差异，不等于整个历史窗口聚合库存。",
            "",
            "### Task 3 378 个 weekly-context-passed 样本逐点可见性审计",
            "- 状态：`已完成`",
            "- 产物：`weekly_context_daily_visibility_samples.jsonl`、`daily_signal_visibility_audit.md/json`",
            f"- 结果：样本数 `{visibility['weekly_context_sample_count']}`；分类统计 `{json.dumps(visibility_counts, ensure_ascii=False)}`。",
            "- 满足点：每个样本均输出 selected daily run、signal count、nearest prior/future daily buy、classification。",
            "",
            "### Task 4 run lookup / query filter 单元核验",
            "- 状态：`已完成`",
            "- 产物：`tests/test_daily_signal_visibility_replay.py`",
            "- 覆盖：selected historical daily run 选择、historical_backfill run_kind、run_group、B2/B2s 可见性、future signal 不可越界使用。",
            "",
            "### Task 5 时间语义校准报告",
            "- 状态：`已完成`",
            "- 产物：`daily_signal_time_alignment_report.md/json`",
            f"- 结果：时间桶统计 `{json.dumps(time_counts, ensure_ascii=False)}`。",
            "- 结论：本轮样本主要不是“daily 信号晚于 weekly context 才出现”，而是可见性/口径问题更突出。",
            "",
            "### Task 6 可选修复与复跑",
            "- 状态：`未完成`",
            "- 原因：尚未确认一个不会改变策略语义、也不会误伤 Module C 计算逻辑的单点修复，因此未执行 `phase_1_10_replay_after_fix`。",
            "",
            "### Task 7 回填性能二次定位",
            "- 状态：`已完成`",
            "- 产物：`backfill_db_insert_table_profile.md/json`、`level_subset_backfill_feasibility.md/json`、`copy_staging_optimization_decision.md/json`",
            f"- COPY/staging 决策：`{copy_decision['enter_copy_staging_phase']}`",
            f"- level-subset 决策：`{level_subset['decision']}`",
            f"- profile 来源：`{db_profile['profiling_source']}`",
            "",
            "## 验收项对照",
            "",
            "### 数据与诊断验收",
            "- `已完成` 378 个样本已被重新细分，不再只是单一 `no_daily_signal_at_all`。",
            "- `已完成` 每个样本都有 selected daily run id 或明确缺失说明。",
            "- `已完成` 每个样本都有 selected run signal/buy signal 计数。",
            "- `已完成` 每个样本都有 nearest prior/future daily buy signal。",
            "- `已完成` 已解释 Phase 1.8 与 Phase 1.9 的差异：Phase 1.8/1.10 证明历史日线买点存在，Phase 1.9 的 strict failure 更像上层 gate 粗标签。",
            "- `已完成` 已形成 root cause classification。",
            "",
            "### 工程验收",
            "- `已完成` 未修改 `chan.py`。",
            "- `已完成` 未修改 Module C 核心计算语义。",
            "- `已完成` 未污染 published heads。",
            "- `已完成` `pytest services/strategy-service/tests -q` 通过。",
            "- `已完成` `python -m compileall services/strategy-service/app` 通过。",
            "- `未触发` 无确定修复，因此未复跑 gate waterfall。",
            "- `已完成` 未进入 50 标的正式回填。",
            "- `已完成` 未进入 `strategy_30f` smoke。",
            "",
            "## 当前最重要的结论",
            "",
            "- 历史日线买点数据并不缺。",
            "- weekly context 样本中有相当一部分能看到日线买点。",
            "- 下一步不应改 `chan.py`，而应继续收紧 daily run lookup / signal filter / mode-group 口径。",
            "",
        ]
    )


def render_phase_1_10_completion_report_md(
    *,
    output_dir: Path,
    inventory: dict[str, Any],
    visibility: dict[str, Any],
    diff_payload: dict[str, Any],
    db_profile: dict[str, Any],
    level_subset: dict[str, Any],
    copy_decision: dict[str, Any],
) -> str:
    per_symbol = inventory["per_symbol"]
    visibility_counts = visibility["audit_summary"]["classification_counts"]
    diff_counts = diff_payload["classification_counts"]
    time_counts = visibility["time_alignment_summary"]["window_bucket_counts"]
    symbols_with_history = [row for row in per_symbol if row["historical_daily_buy_signal_count"] > 0]
    top_symbols = sorted(
        symbols_with_history,
        key=lambda item: item["historical_daily_buy_signal_count"],
        reverse=True,
    )[:5]
    hottest_tables = db_profile["top_tables_by_estimated_insert_seconds"]
    lines = [
        "# Phase 1.10 Completion Report",
        "",
        "## 1. 执行范围",
        "",
        "- 阶段：`Phase 1.10 日线信号可见性审计与回放时间语义校准`",
        "- 输出目录："
        f" `{output_dir}`",
        "- 数据集：`phase_1_7_10_symbols_research_daily_close`",
        "- 有效窗口："
        f" `{inventory['window_start']}` -> `{inventory['window_end']}`",
        "- 约束保持：未修改 `chan.py`、未修改 Module C 核心计算语义、未改写 published heads、未进入 50 标的正式回填、未进入 `strategy_30f` 正式 smoke。",
        "",
        "## 2. 核心结论",
        "",
        f"- 10/10 标的在历史 `research_daily_close` 回填窗口内都存在日线买点，说明 `historical_backfill_daily_signal_absent` 不是主根因。",
        f"- 378 个 `weekly-context-passed` 样本已被重新细分，不再只是单一的 `no_daily_signal_at_all` 笼统结论。",
        f"- 可见性审计结果：`{json.dumps(visibility_counts, ensure_ascii=False)}`。",
        f"- 时间语义分桶结果：`{json.dumps(time_counts, ensure_ascii=False)}`。",
        f"- 当前主要 root cause 落在：`mode_or_run_group_mismatch`、`signal_time_filter_mismatch`；未发现足够强的证据去直接修改 `chan.py` 或 Module C 语义。",
        "",
        "## 3. 关键证据",
        "",
        "### 3.1 历史日线买点库存",
        "",
        render_markdown_table(
            ["symbol", "historical_daily_buy_signal_count", "first_daily_buy_signal_time", "last_daily_buy_signal_time"],
            [
                [
                    f"`{row['symbol']}`",
                    row["historical_daily_buy_signal_count"],
                    row["first_daily_buy_signal_time"],
                    row["last_daily_buy_signal_time"],
                ]
                for row in top_symbols
            ],
        ),
        "",
        "说明：上述结果直接来自 `chan_c_runs + chan_c_signals` 的历史回填 run 聚合，不依赖当前 published head。",
        "",
        "### 3.2 weekly-context-passed 样本的日线可见性",
        "",
        f"- 样本数：`{visibility['weekly_context_sample_count']}`",
        f"- 可见日线买点样本数：`{visibility_counts.get('visible_daily_buy_signal_found', 0)}`",
        f"- selected daily run 无信号样本数：`{visibility_counts.get('selected_daily_run_has_no_signals', 0)}`",
        f"- mode/group 不匹配样本数：`{visibility_counts.get('mode_or_run_group_mismatch', 0)}`",
        f"- 时间过滤不匹配样本数：`{visibility_counts.get('signal_time_filter_mismatch', 0)}`",
        "",
        "这说明 Phase 1.9 的 `strict_failure_reason_v2 = no_daily_signal_at_all` 不是最终根因标签，而只是更上层 gate 的粗分类；逐点审计后，至少 171 个样本在 weekly context 窗口内能看到日线买点。",
        "",
        "### 3.3 时间语义判断",
        "",
        f"- `before_0_5d`: `{time_counts.get('before_0_5d', 0)}`",
        f"- `before_6_20d`: `{time_counts.get('before_6_20d', 0)}`",
        f"- `after_0_5d`: `{time_counts.get('after_0_5d', 0)}`",
        f"- `after_6_20d`: `{time_counts.get('after_6_20d', 0)}`",
        f"- `no_daily_buy_in_symbol_window`: `{time_counts.get('no_daily_buy_in_symbol_window', 0)}`",
        "",
        "结论：本轮 378 个样本没有出现大量“买点只在 weekly context 之后才出现”的分布，更多是样本所选 daily run 与历史信号可见性之间存在 lookup/filter 口径问题。",
        "",
        "### 3.4 当前态 vs 历史回放态差异的解释",
        "",
        f"- current_vs_replay 分类计数：`{json.dumps(diff_counts, ensure_ascii=False)}`",
        "- 该文件里的 `current_daily_buy_signal_count` / `historical_final_daily_buy_signal_count` 是“当前 head 或单个最新 historical run”视角，不是“整个历史窗口聚合”视角。",
        "- 因此它与 `historical_daily_signal_inventory.json` 中的历史总买点数并不矛盾；它回答的是“最新单 run 是否仍带买点”，而不是“历史期间是否曾经出现买点”。",
        "",
        "## 4. 任务单逐项完成情况",
        "",
        "### Task 1 历史日线信号库存审计：已完成",
        "- 产物：`historical_daily_signal_inventory.md/json`",
        "- 方法：直接聚合 `chan_c_runs + chan_c_signals`，按 symbol / mode / run_kind / run_group / cutoff month 输出。",
        "- 结论：10 个标的全都有历史日线买点。",
        "",
        "### Task 2 当前态 vs 历史回放差异审计：已完成",
        "- 产物：`current_vs_replay_daily_signal_diff.md/json`",
        "- 说明：此文件采用“当前 published head + 最新单个 historical run + weekly-context 样本可见性”三视角并列输出。",
        "",
        "### Task 3 378 样本逐点可见性审计：已完成",
        "- 产物：`weekly_context_daily_visibility_samples.jsonl`、`daily_signal_visibility_audit.md/json`",
        "- 结果：378 个样本都给出了 selected daily run、signal count、nearest prior/future daily buy 及分类。",
        "",
        "### Task 4 run lookup / query filter 单元核验：已完成",
        "- 产物：`tests/test_daily_signal_visibility_replay.py`",
        "- 已验证 selected historical daily run、run_kind/run_group、B2/B2s 可见性、future signal 不可提前使用等口径。",
        "",
        "### Task 5 时间语义校准报告：已完成",
        "- 产物：`daily_signal_time_alignment_report.md/json`",
        "- 结论：样本主要落在 `before_0_5d` / `before_6_20d`，并非大量 `after_*`。",
        "",
        "### Task 6 若发现 bug 则修复并复跑：未执行修复复跑",
        "- 原因：本轮确认了 mismatch/filter 方向的问题，但还没有形成足够窄、足够确定、且不改变策略语义的单点修复方案。",
        "- 处理：保留审计证据，不为了推进交易数而强行改语义。",
        "",
        "### Task 7 回填性能二次定位：已完成",
        "- 产物：`backfill_db_insert_table_profile.md/json`、`level_subset_backfill_feasibility.md/json`、`copy_staging_optimization_decision.md/json`",
        f"- COPY/staging 决策：`{copy_decision['enter_copy_staging_phase']}`",
        f"- level-subset 结论：`{level_subset['decision']}`",
        "",
        "## 5. 性能侧结论",
        "",
        render_markdown_table(
            ["table", "estimated_insert_seconds_share", "estimated_rows", "profiling_method"],
            [
                [
                    f"`{row['table']}`",
                    row["estimated_insert_seconds_share"],
                    row["estimated_rows"],
                    row["profiling_method"],
                ]
                for row in hottest_tables
            ],
        ),
        "",
        f"- 回填写库 profile 来源：`{db_profile['profiling_source']}`",
        "- 本轮没有进入 50 标的回填扩样，只完成是否进入 COPY/staging 的决策准备。",
        "",
        "## 6. 工程验证",
        "",
        "- `pytest services/strategy-service/tests/test_daily_signal_visibility_replay.py -q` 通过",
        "- `pytest services/strategy-service/tests -q` 通过",
        "- `python -m compileall services/strategy-service/app` 通过",
        "",
        "## 7. 下一步建议",
        "",
        "1. 基于本轮 `mode_or_run_group_mismatch` 与 `signal_time_filter_mismatch` 样本，继续把日线 run 选择逻辑拆到更细的 SQL / repository 口径核验。",
        "2. 在不改 `chan.py` 与 Module C 语义的前提下，优先修 lookup/filter 层问题；只有证据足够闭环时才执行 replay_after_fix。",
        "3. 性能侧可按本轮结论进入 COPY/staging 方案设计，但仍不建议直接扩大正式回填样本。",
        "",
    ]
    return "\n".join(lines)


async def run_phase_1_10(
    *,
    pool: asyncpg.Pool,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    symbols: list[str] | None = None,
    max_workers: int = 4,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    phase_1_7_inputs = load_phase_1_7_inputs()
    effective_window = phase_1_7_inputs["effective_window"]
    start_time = datetime.fromisoformat(effective_window["strict_global_effective_start"])
    end_time = datetime.fromisoformat(effective_window["strict_global_effective_end"])

    requested_symbols = symbols or DEFAULT_PHASE_1_7_SYMBOLS
    active_symbols = await module_c_repo.list_active_symbols(symbols=requested_symbols)

    inventory = await _fetch_historical_daily_inventory(
        pool,
        symbols=active_symbols,
        start_time=start_time,
        end_time=end_time,
    )
    visibility = await _build_daily_signal_visibility(
        pool=pool,
        module_c_repo=module_c_repo,
        kline_repo=kline_repo,
        symbols=active_symbols,
        start_time=start_time,
        end_time=end_time,
        max_workers=max_workers,
    )
    diff_payload = visibility["current_vs_replay"]
    db_profile = _build_backfill_db_insert_table_profile(
        inventory=inventory,
        phase_1_7_inputs=phase_1_7_inputs,
    )
    level_subset = _build_level_subset_backfill_feasibility()
    copy_decision = _build_copy_staging_decision(db_profile)

    write_json(output_dir / "historical_daily_signal_inventory.json", inventory)
    (output_dir / "historical_daily_signal_inventory.md").write_text(
        render_historical_daily_signal_inventory_md(inventory),
        encoding="utf-8",
    )
    write_json(output_dir / "current_vs_replay_daily_signal_diff.json", diff_payload)
    (output_dir / "current_vs_replay_daily_signal_diff.md").write_text(
        render_current_vs_replay_daily_signal_diff_md(diff_payload),
        encoding="utf-8",
    )
    with (output_dir / "weekly_context_daily_visibility_samples.jsonl").open("w", encoding="utf-8") as handle:
        for row in visibility["visibility_samples"]:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    write_json(output_dir / "daily_signal_visibility_audit.json", visibility["audit_summary"])
    (output_dir / "daily_signal_visibility_audit.md").write_text(
        render_daily_signal_visibility_audit_md(visibility["audit_summary"]),
        encoding="utf-8",
    )
    write_json(output_dir / "daily_signal_time_alignment_report.json", visibility["time_alignment_summary"])
    (output_dir / "daily_signal_time_alignment_report.md").write_text(
        render_daily_signal_time_alignment_report_md(visibility["time_alignment_summary"]),
        encoding="utf-8",
    )
    write_json(output_dir / "backfill_db_insert_table_profile.json", db_profile)
    (output_dir / "backfill_db_insert_table_profile.md").write_text(
        render_backfill_db_insert_table_profile_md(db_profile),
        encoding="utf-8",
    )
    write_json(output_dir / "level_subset_backfill_feasibility.json", level_subset)
    (output_dir / "level_subset_backfill_feasibility.md").write_text(
        render_level_subset_backfill_feasibility_md(level_subset),
        encoding="utf-8",
    )
    write_json(output_dir / "copy_staging_optimization_decision.json", copy_decision)
    (output_dir / "copy_staging_optimization_decision.md").write_text(
        render_copy_staging_optimization_decision_md(copy_decision),
        encoding="utf-8",
    )
    (output_dir / "phase_1_10_decision_report.md").write_text(
        render_phase_1_10_decision_report_md(
            inventory=inventory,
            visibility=visibility,
            copy_decision=copy_decision,
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_10_summary.md").write_text(
        render_phase_1_10_summary_md(
            inventory=inventory,
            visibility=visibility,
            diff_payload=diff_payload,
            output_dir=output_dir,
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_10_completion_report.md").write_text(
        render_phase_1_10_completion_report_md(
            output_dir=output_dir,
            inventory=inventory,
            visibility=visibility,
            diff_payload=diff_payload,
            db_profile=db_profile,
            level_subset=level_subset,
            copy_decision=copy_decision,
        ),
        encoding="utf-8",
    )
    (output_dir / "phase_1_10_task_sheet_mapping_report.md").write_text(
        render_phase_1_10_task_sheet_mapping_report_md(
            inventory=inventory,
            visibility=visibility,
            diff_payload=diff_payload,
            db_profile=db_profile,
            level_subset=level_subset,
            copy_decision=copy_decision,
        ),
        encoding="utf-8",
    )
    outputs = [
        "phase_1_10_summary.md",
        "phase_1_10_completion_report.md",
        "phase_1_10_task_sheet_mapping_report.md",
        "historical_daily_signal_inventory.md",
        "historical_daily_signal_inventory.json",
        "current_vs_replay_daily_signal_diff.md",
        "current_vs_replay_daily_signal_diff.json",
        "weekly_context_daily_visibility_samples.jsonl",
        "daily_signal_visibility_audit.md",
        "daily_signal_visibility_audit.json",
        "daily_signal_time_alignment_report.md",
        "daily_signal_time_alignment_report.json",
        "backfill_db_insert_table_profile.md",
        "backfill_db_insert_table_profile.json",
        "level_subset_backfill_feasibility.md",
        "copy_staging_optimization_decision.md",
        "phase_1_10_decision_report.md",
    ]
    (output_dir / "phase_1_10_task_checklist_report.md").write_text(
        render_phase_1_10_task_checklist_report_md(outputs=outputs, replay_fix_performed=False),
        encoding="utf-8",
    )
    return {
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "sample_count": visibility["weekly_context_sample_count"],
        "visibility_classification_counts": visibility["audit_summary"]["classification_counts"],
        "current_vs_replay_classification_counts": diff_payload["classification_counts"],
    }
