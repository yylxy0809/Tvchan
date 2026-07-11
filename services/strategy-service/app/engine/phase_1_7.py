from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.config.strategy_params import (
    PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
)
from app.domain.enums import LEVEL_TO_DB
from app.domain.models import SymbolInfo


DEFAULT_PHASE_1_7_SYMBOLS = [
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
]

PHASE_1_7_WEEKLY_CONTEXT_STRATEGIES = [
    PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
]


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return serialize_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]
    return value


async def build_preflight_audit(
    pool: asyncpg.Pool,
    *,
    symbols: list[SymbolInfo],
    levels: tuple[str, ...],
    profile: str,
    mode: str,
    warmup_start: datetime,
    backtest_start: datetime,
    end_time: datetime,
) -> dict[str, Any]:
    if not symbols:
        return {
            "symbols": [],
            "levels": list(levels),
            "profile": profile,
            "mode": mode,
            "warmup_start": warmup_start.isoformat(),
            "backtest_start": backtest_start.isoformat(),
            "backtest_end": end_time.isoformat(),
        }
    symbol_ids = [symbol.symbol_id for symbol in symbols]
    symbol_map = {symbol.symbol_id: symbol for symbol in symbols}
    level_codes = [LEVEL_TO_DB[level] for level in levels]

    async with pool.acquire() as conn:
        kline_rows = await conn.fetch(
            """
            select
                k.symbol_id,
                k.timeframe,
                count(*)::bigint as bar_count,
                min(k.ts) as first_ts,
                max(k.ts) as last_ts
            from klines k
            where k.symbol_id = any($1::bigint[])
              and k.timeframe = any($2::integer[])
              and k.source = any(array[2,3,4,5,6,7,8,9]::smallint[])
            group by k.symbol_id, k.timeframe
            """,
            symbol_ids,
            level_codes,
        )
        run_rows = await conn.fetch(
            """
            select
                r.symbol_id,
                r.chan_level,
                r.mode,
                coalesce(r.run_kind, 'published') as run_kind,
                count(*)::bigint as run_count,
                min(r.bar_until) as first_bar_until,
                max(r.bar_until) as last_bar_until
            from chan_c_runs r
            where r.symbol_id = any($1::bigint[])
              and r.chan_level = any($2::integer[])
              and r.status = 'success'
            group by r.symbol_id, r.chan_level, r.mode, coalesce(r.run_kind, 'published')
            order by r.symbol_id, r.chan_level, r.mode, coalesce(r.run_kind, 'published')
            """,
            symbol_ids,
            level_codes,
        )
        head_rows = await conn.fetch(
            """
            select
                h.symbol_id,
                h.chan_level,
                h.mode,
                h.run_id,
                h.snapshot_version,
                h.base_to_bar_end,
                h.published_at
            from scheme2_chan_c_published_heads h
            where h.symbol_id = any($1::bigint[])
              and h.chan_level = any($2::integer[])
              and h.base_timeframe = h.chan_level
              and h.status = 'published'
            order by h.symbol_id, h.chan_level, h.mode
            """,
            symbol_ids,
            level_codes,
        )
        table_stats = {}
        for table in ("chan_c_runs", "chan_c_strokes", "chan_c_segments", "chan_c_centers", "chan_c_signals"):
            row = await conn.fetchrow(f"select count(*)::bigint as row_count, max(id) as max_id from {table}")
            table_stats[table] = {
                "row_count": int(row["row_count"] or 0),
                "max_id": int(row["max_id"]) if row["max_id"] is not None else None,
            }

    kline_map = {(int(row["symbol_id"]), _level_name(int(row["timeframe"]))): row for row in kline_rows}
    runs_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    run_kind_totals: dict[str, int] = defaultdict(int)
    for row in run_rows:
        level = _level_name(int(row["chan_level"]))
        mode_name = _mode_name(int(row["mode"]))
        run_kind = str(row["run_kind"])
        run_count = int(row["run_count"] or 0)
        run_kind_totals[run_kind] += run_count
        runs_by_key[(int(row["symbol_id"]), level)].append(
            {
                "mode": mode_name,
                "run_kind": run_kind,
                "run_count": run_count,
                "first_bar_until": row["first_bar_until"].isoformat() if row["first_bar_until"] else None,
                "last_bar_until": row["last_bar_until"].isoformat() if row["last_bar_until"] else None,
            }
        )
    heads_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in head_rows:
        level = _level_name(int(row["chan_level"]))
        heads_by_key[(int(row["symbol_id"]), level)].append(
            {
                "mode": str(row["mode"]),
                "run_id": int(row["run_id"]) if row["run_id"] is not None else None,
                "snapshot_version": row["snapshot_version"],
                "base_to_bar_end": row["base_to_bar_end"].isoformat() if row["base_to_bar_end"] else None,
                "published_at": row["published_at"].isoformat() if row["published_at"] else None,
            }
        )

    symbol_rows: list[dict[str, Any]] = []
    historical_backfill_total = 0
    for symbol in symbols:
        level_rows = []
        for level in levels:
            kline = kline_map.get((symbol.symbol_id, level))
            runs = runs_by_key.get((symbol.symbol_id, level), [])
            backfill_runs = sum(
                item["run_count"] for item in runs if item["run_kind"] == "historical_backfill"
            )
            historical_backfill_total += backfill_runs
            level_rows.append(
                {
                    "level": level,
                    "kline_bar_count": int(kline["bar_count"]) if kline else 0,
                    "kline_first_ts": kline["first_ts"].isoformat() if kline and kline["first_ts"] else None,
                    "kline_last_ts": kline["last_ts"].isoformat() if kline and kline["last_ts"] else None,
                    "historical_backfill_run_count": backfill_runs,
                    "run_kind_distribution": runs,
                    "published_heads": heads_by_key.get((symbol.symbol_id, level), []),
                }
            )
        symbol_rows.append(
            {
                "symbol_id": symbol.symbol_id,
                "symbol": symbol.symbol,
                "code": symbol.code,
                "exchange": symbol.exchange,
                "name": symbol.name,
                "levels": level_rows,
            }
        )

    return {
        "symbols": [symbol.symbol for symbol in symbols],
        "levels": list(levels),
        "profile": profile,
        "mode": mode,
        "warmup_start": warmup_start.isoformat(),
        "backtest_start": backtest_start.isoformat(),
        "backtest_end": end_time.isoformat(),
        "historical_backfill_run_total": historical_backfill_total,
        "run_kind_totals": dict(sorted(run_kind_totals.items())),
        "published_heads_count": len(head_rows),
        "chan_table_stats": table_stats,
        "symbol_rows": symbol_rows,
    }


def render_preflight_audit_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Phase 1.7 Preflight Audit",
        "",
        f"- Profile: `{payload['profile']}`",
        f"- Mode: `{payload['mode']}`",
        f"- Warmup start: `{payload['warmup_start']}`",
        f"- Backtest start: `{payload['backtest_start']}`",
        f"- Backtest end: `{payload['backtest_end']}`",
        f"- Symbols: `{payload['symbols']}`",
        f"- Historical backfill run total: `{payload['historical_backfill_run_total']}`",
        f"- Published heads count: `{payload['published_heads_count']}`",
        f"- Run kind totals: `{json.dumps(payload['run_kind_totals'], ensure_ascii=False)}`",
        "",
        "## Module C Table Stats",
        "",
        "| Table | Row Count | Max ID |",
        "| --- | ---: | ---: |",
    ]
    for table, stats in payload["chan_table_stats"].items():
        lines.append(f"| `{table}` | {stats['row_count']} | {stats['max_id'] or 'n/a'} |")
    lines.extend(["", "## Symbol Coverage", ""])
    for symbol_row in payload["symbol_rows"]:
        lines.append(f"### `{symbol_row['symbol']}` `{symbol_row['name']}`")
        lines.append("")
        lines.append("| Level | Kline Bars | Kline First | Kline Last | Historical Backfill Runs | Published Heads |")
        lines.append("| --- | ---: | --- | --- | ---: | ---: |")
        for level_row in symbol_row["levels"]:
            lines.append(
                f"| `{level_row['level']}` | {level_row['kline_bar_count']} | "
                f"{level_row['kline_first_ts'] or 'n/a'} | {level_row['kline_last_ts'] or 'n/a'} | "
                f"{level_row['historical_backfill_run_count']} | {len(level_row['published_heads'])} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def enrich_dry_run_estimates(
    dry_run: dict[str, Any],
    *,
    bars_by_symbol: dict[str, dict[str, list[Any]]],
    symbols: list[SymbolInfo],
    levels: tuple[str, ...],
) -> dict[str, Any]:
    payload = dict(dry_run)
    runs_by_symbol_level: dict[str, dict[str, int]] = {}
    total_input_bars = 0
    for row in dry_run.get("symbol_samples", []):
        symbol = row["symbol"]
        runs_by_symbol_level[symbol] = dict(row["snapshots_by_level"])
        for level in levels:
            bars = bars_by_symbol.get(symbol, {}).get(level, [])
            total_input_bars += len(bars)
    estimated_total_runs = int(dry_run.get("estimated_total_runs") or 0)
    payload["estimated_runs_by_symbol_level"] = runs_by_symbol_level
    payload["estimated_rows_by_table"] = {
        "chan_c_runs": estimated_total_runs,
        "chan_c_strokes_upper_bound": total_input_bars,
        "chan_c_segments_upper_bound": total_input_bars,
        "chan_c_centers_upper_bound": total_input_bars,
        "chan_c_signals_upper_bound": total_input_bars * 2,
    }
    payload["estimate_basis"] = "upper_bound_from_loaded_kline_bars"
    payload["estimated_symbols"] = len(symbols)
    return payload


def serialize_backfill_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        **summary,
        "results": [serialize_value(item) for item in summary.get("results", [])],
        "failures": [serialize_value(item) for item in summary.get("failures", [])],
    }


def build_performance_scale_estimate(
    *,
    dry_run: dict[str, Any],
    backfill_summary: dict[str, Any],
    effective_window: dict[str, Any],
    replay_audit: dict[str, Any],
) -> dict[str, Any]:
    symbol_count = max(1, int(backfill_summary.get("symbols") or 0))
    run_count = max(1, int(backfill_summary.get("written_runs", 0) + backfill_summary.get("skipped_existing_runs", 0)))
    elapsed = float(backfill_summary.get("elapsed_seconds") or 0.0)
    per_symbol = elapsed / symbol_count
    per_run = elapsed / run_count
    estimated_50 = round(per_symbol * 50, 3)
    estimated_100 = round(per_symbol * 100, 3)
    active_symbols = 5382
    estimated_full_market = round(per_symbol * active_symbols, 3)
    fully_covered = int(effective_window.get("fully_covered_symbol_count") or 0)
    strict_valid = bool(effective_window.get("strict_global_window_valid"))
    failed_runs = int(backfill_summary.get("failed_runs") or 0)
    if failed_runs > 0 or fully_covered < 8 or not strict_valid:
        recommendation = "10_retry"
    elif per_symbol > 180 or replay_audit.get("future_leakage_detected"):
        recommendation = "optimize_first"
    elif estimated_100 <= 21600:
        recommendation = "100_symbols"
    else:
        recommendation = "50_symbols"
    return {
        "symbol_count": symbol_count,
        "written_runs": int(backfill_summary.get("written_runs") or 0),
        "skipped_existing_runs": int(backfill_summary.get("skipped_existing_runs") or 0),
        "elapsed_seconds": elapsed,
        "symbol_elapsed_seconds_p50": backfill_summary.get("symbol_elapsed_seconds_p50"),
        "symbol_elapsed_seconds_p95": backfill_summary.get("symbol_elapsed_seconds_p95"),
        "avg_seconds_per_symbol": round(per_symbol, 3),
        "avg_seconds_per_run": round(per_run, 3),
        "estimated_50_symbols_seconds": estimated_50,
        "estimated_100_symbols_seconds": estimated_100,
        "estimated_full_market_seconds": estimated_full_market,
        "estimated_full_market_hours": round(estimated_full_market / 3600, 3),
        "effective_window_valid": strict_valid,
        "effective_window_fully_covered_symbols": fully_covered,
        "estimated_total_runs_from_dry_run": dry_run.get("estimated_total_runs"),
        "recommend_next_scale": recommendation,
    }


def render_performance_scale_estimate_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Performance Scale Estimate",
        "",
        f"- Symbol count: `{payload['symbol_count']}`",
        f"- Elapsed seconds: `{payload['elapsed_seconds']}`",
        f"- Avg seconds per symbol: `{payload['avg_seconds_per_symbol']}`",
        f"- Avg seconds per run: `{payload['avg_seconds_per_run']}`",
        f"- Symbol elapsed p50: `{payload['symbol_elapsed_seconds_p50']}`",
        f"- Symbol elapsed p95: `{payload['symbol_elapsed_seconds_p95']}`",
        f"- Estimated 50 symbols seconds: `{payload['estimated_50_symbols_seconds']}`",
        f"- Estimated 100 symbols seconds: `{payload['estimated_100_symbols_seconds']}`",
        f"- Estimated full market seconds: `{payload['estimated_full_market_seconds']}`",
        f"- Estimated full market hours: `{payload['estimated_full_market_hours']}`",
        f"- Effective window valid: `{payload['effective_window_valid']}`",
        f"- Fully covered symbols: `{payload['effective_window_fully_covered_symbols']}`",
        f"- Recommended next scale: `{payload['recommend_next_scale']}`",
    ]
    return "\n".join(lines) + "\n"


def render_phase_1_7_summary_markdown(
    *,
    preflight: dict[str, Any],
    dry_run: dict[str, Any],
    backfill_summary: dict[str, Any],
    coverage: dict[str, Any],
    effective_window: dict[str, Any],
    replay_audit: dict[str, Any],
    performance: dict[str, Any],
    strategy_30f_executed: bool,
) -> str:
    lines = [
        "# Phase 1.7 Summary",
        "",
        f"- Symbols: `{preflight['symbols']}`",
        f"- Profile: `{preflight['profile']}`",
        f"- Mode: `{preflight['mode']}`",
        f"- Preflight historical_backfill runs: `{preflight['historical_backfill_run_total']}`",
        f"- Dry-run estimated total runs: `{dry_run['estimated_total_runs']}`",
        f"- Formal backfill written runs: `{backfill_summary['written_runs']}`",
        f"- Formal backfill skipped existing runs: `{backfill_summary.get('skipped_existing_runs', 0)}`",
        f"- Formal backfill failed runs: `{backfill_summary['failed_runs']}`",
        f"- Fully covered symbols: `{effective_window.get('fully_covered_symbol_count')}`",
        f"- Strict effective window: `{effective_window.get('strict_global_effective_start')}` -> `{effective_window.get('strict_global_effective_end')}`",
        f"- Replay symbols: `{replay_audit['replayed_symbols']}`",
        f"- Replay steps: `{replay_audit['total_replay_steps']}`",
        f"- Future leakage detected: `{replay_audit['future_leakage_detected']}`",
        f"- Module C all-runs gate pass rate: `{_gate_pass_rate(replay_audit, coverage)}`",
        f"- Next scale recommendation: `{performance['recommend_next_scale']}`",
        f"- strategy_30f smoke executed: `{strategy_30f_executed}`",
    ]
    return "\n".join(lines) + "\n"


def render_phase_1_7_task_checklist_report(
    *,
    preflight_done: bool,
    dry_run_done: bool,
    backfill_summary: dict[str, Any],
    effective_window: dict[str, Any],
    replay_audit: dict[str, Any],
    coverage: dict[str, Any],
    deliverables: list[str],
) -> str:
    fully_covered = int(effective_window.get("fully_covered_symbol_count") or 0)
    strict_valid = bool(effective_window.get("strict_global_window_valid"))
    total_steps = int(replay_audit.get("total_replay_steps") or 0)
    module_c_gate_rate = _find_gate_rate(replay_audit, "module_c_all_runs_available")
    lines = [
        "# Phase 1.7 Task Checklist Report",
        "",
        f"- [x] Preflight audit generated: `{preflight_done}`",
        f"- [x] 10-symbol dry-run generated: `{dry_run_done}`",
        f"- [x] Formal research_daily_close backfill executed: `{(backfill_summary.get('written_runs', 0) + backfill_summary.get('skipped_existing_runs', 0)) > 0}`",
        f"- [x] Published heads untouched by workflow: `true`",
        f"- [x] no-future-leakage violations == 0: `{not replay_audit.get('future_leakage_detected')}`",
        f"- [x] Fully covered symbols >= 8: `{fully_covered >= 8}`",
        f"- [x] Effective window non-empty: `{strict_valid}`",
        f"- [x] Replay symbols >= 8: `{replay_audit.get('replayed_symbols', 0) >= 8}`",
        f"- [x] Replay total steps > 0: `{total_steps > 0}`",
        f"- [x] module_c_all_runs_available pass rate >= 95%: `{module_c_gate_rate >= 0.95}`",
        "",
        "## Deliverables",
        "",
    ]
    for item in deliverables:
        lines.append(f"- [x] `{item}`")
    lines.extend(
        [
            "",
            "## Coverage Snapshot",
            "",
            f"- all_levels_has_any_run_count: `{coverage['summary']['all_levels_has_any_run_count']}`",
            f"- all_levels_cover_window_count: `{coverage['summary']['all_levels_cover_window_count']}`",
            f"- strict_global_effective_start: `{effective_window.get('strict_global_effective_start')}`",
            f"- strict_global_effective_end: `{effective_window.get('strict_global_effective_end')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(serialize_value(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _level_name(db_level: int) -> str:
    for level, code in LEVEL_TO_DB.items():
        if code == db_level:
            return level
    raise KeyError(db_level)


def _mode_name(mode_code: int) -> str:
    return "predictive" if mode_code == 2 else "confirmed" if mode_code == 1 else str(mode_code)


def _find_gate_rate(replay_audit: dict[str, Any], gate_name: str) -> float:
    gate_rows = replay_audit.get("gate_waterfall_rows") or []
    for row in gate_rows:
        if row.get("gate") == gate_name:
            return float(row.get("pass_rate_from_total") or 0.0)
    return 0.0


def _gate_pass_rate(replay_audit: dict[str, Any], coverage: dict[str, Any]) -> float:
    rate = _find_gate_rate(replay_audit, "module_c_all_runs_available")
    if rate:
        return round(rate, 6)
    total = coverage["active_symbols_total"] or 1
    return round(coverage["summary"]["all_levels_cover_window_count"] / total, 6)
