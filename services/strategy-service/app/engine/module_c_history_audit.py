from __future__ import annotations

import csv
from collections import Counter
from datetime import datetime
from typing import Any

import asyncpg

from app.domain.enums import LEVEL_TO_DB
from app.repositories.module_c_repo import LEGACY_SHARED_MODE, ModuleCRepository, MODE_TO_DB


DEFAULT_LEVELS = ("5f", "30f", "1d", "1w", "1m")
DEFAULT_LOOKUP_SYMBOLS = ("000001.SZ", "600519.SH", "000698.SZ", "300818.SZ", "300898.SZ")
DEFAULT_LOOKUP_TIMES = (
    "2026-01-05T07:00:00+00:00",
    "2026-03-01T07:00:00+00:00",
    "2026-07-01T00:00:00+00:00",
)


async def build_module_c_history_coverage(
    pool: asyncpg.Pool,
    *,
    start_time: datetime,
    end_time: datetime,
    levels: tuple[str, ...],
    mode: str,
    symbols: list[str] | None = None,
    limit: int = 0,
) -> dict[str, Any]:
    selected_symbols = await _load_active_symbols(pool, symbols=symbols, limit=limit)
    if not selected_symbols:
        return {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "mode": mode,
            "levels": list(levels),
            "active_symbols_total": 0,
            "rows": [],
            "summary": {},
        }
    symbol_ids = [row["symbol_id"] for row in selected_symbols]
    level_ids = [LEVEL_TO_DB[level] for level in levels]
    mode_id = MODE_TO_DB[mode]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select
                r.symbol_id,
                r.chan_level,
                count(*)::bigint as run_count,
                min(r.bar_until) as first_bar_until,
                max(r.bar_until) as last_bar_until,
                min(r.computed_at) as first_computed_at,
                max(r.computed_at) as last_computed_at,
                max(r.bar_until) filter (where r.bar_until <= $4) as latest_before_start,
                max(r.bar_until) filter (where r.bar_until <= $5) as latest_before_end
            from chan_c_runs r
            where r.symbol_id = any($1::int[])
              and r.chan_level = any($2::int[])
              and r.mode = any($3::smallint[])
              and r.status = 'success'
            group by r.symbol_id, r.chan_level
            order by r.symbol_id, r.chan_level
            """,
            symbol_ids,
            level_ids,
            [mode_id, LEGACY_SHARED_MODE],
            start_time,
            end_time,
        )
    symbol_meta = {
        row["symbol_id"]: {
            "symbol": row["symbol"],
            "code": row["code"],
            "exchange": row["exchange"],
            "name": row["name"],
        }
        for row in selected_symbols
    }
    row_map = {
        (int(row["symbol_id"]), _level_name(int(row["chan_level"]))): row
        for row in rows
    }
    detail_rows: list[dict[str, Any]] = []
    per_level: dict[str, dict[str, Any]] = {
        level: {
            "has_any_run": 0,
            "covers_backtest_start": 0,
            "covers_backtest_end": 0,
            "missing_symbols": [],
            "start_missing_symbols": [],
            "end_missing_symbols": [],
            "first_bar_untils": [],
            "last_bar_untils": [],
        }
        for level in levels
    }
    all_levels_any = 0
    all_levels_start = 0
    all_levels_end = 0
    all_levels_window = 0
    effective_symbol_windows: list[dict[str, Any]] = []

    for symbol_id, meta in symbol_meta.items():
        level_rows: dict[str, dict[str, Any]] = {}
        for level in levels:
            row = row_map.get((symbol_id, level))
            payload = {
                "symbol_id": symbol_id,
                "symbol": meta["symbol"],
                "code": meta["code"],
                "exchange": meta["exchange"],
                "name": meta["name"],
                "level": level,
                "mode": mode,
                "run_count": int(row["run_count"]) if row else 0,
                "first_bar_until": row["first_bar_until"].isoformat() if row and row["first_bar_until"] else None,
                "last_bar_until": row["last_bar_until"].isoformat() if row and row["last_bar_until"] else None,
                "first_computed_at": row["first_computed_at"].isoformat() if row and row["first_computed_at"] else None,
                "last_computed_at": row["last_computed_at"].isoformat() if row and row["last_computed_at"] else None,
                "has_any_run": bool(row),
                "covers_backtest_start": bool(row and row["latest_before_start"] is not None),
                "covers_backtest_end": bool(row and row["latest_before_end"] is not None),
                "latest_before_start": row["latest_before_start"].isoformat() if row and row["latest_before_start"] else None,
                "latest_before_end": row["latest_before_end"].isoformat() if row and row["latest_before_end"] else None,
            }
            detail_rows.append(payload)
            level_rows[level] = payload
            bucket = per_level[level]
            if payload["has_any_run"]:
                bucket["has_any_run"] += 1
                bucket["first_bar_untils"].append(payload["first_bar_until"])
                bucket["last_bar_untils"].append(payload["last_bar_until"])
            else:
                bucket["missing_symbols"].append(meta["symbol"])
            if payload["covers_backtest_start"]:
                bucket["covers_backtest_start"] += 1
            else:
                bucket["start_missing_symbols"].append(meta["symbol"])
            if payload["covers_backtest_end"]:
                bucket["covers_backtest_end"] += 1
            else:
                bucket["end_missing_symbols"].append(meta["symbol"])

        if all(level_rows[level]["has_any_run"] for level in levels):
            all_levels_any += 1
            effective_start = max(level_rows[level]["first_bar_until"] for level in levels)
            effective_end = min(level_rows[level]["last_bar_until"] for level in levels)
            effective_symbol_windows.append(
                {
                    "symbol": meta["symbol"],
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                }
            )
        if all(level_rows[level]["covers_backtest_start"] for level in levels):
            all_levels_start += 1
        if all(level_rows[level]["covers_backtest_end"] for level in levels):
            all_levels_end += 1
        if all(level_rows[level]["covers_backtest_start"] and level_rows[level]["covers_backtest_end"] for level in levels):
            all_levels_window += 1

    total = len(symbol_meta)
    summary = {
        "active_symbols_total": total,
        "levels": {
            level: {
                "has_any_run_count": per_level[level]["has_any_run"],
                "has_any_run_ratio": round(per_level[level]["has_any_run"] / total, 6),
                "covers_backtest_start_count": per_level[level]["covers_backtest_start"],
                "covers_backtest_start_ratio": round(per_level[level]["covers_backtest_start"] / total, 6),
                "covers_backtest_end_count": per_level[level]["covers_backtest_end"],
                "covers_backtest_end_ratio": round(per_level[level]["covers_backtest_end"] / total, 6),
                "first_bar_until_min": _safe_min(per_level[level]["first_bar_untils"]),
                "first_bar_until_max": _safe_max(per_level[level]["first_bar_untils"]),
                "last_bar_until_min": _safe_min(per_level[level]["last_bar_untils"]),
                "last_bar_until_max": _safe_max(per_level[level]["last_bar_untils"]),
                "missing_samples": per_level[level]["missing_symbols"][:20],
                "start_missing_samples": per_level[level]["start_missing_symbols"][:20],
                "end_missing_samples": per_level[level]["end_missing_symbols"][:20],
            }
            for level in levels
        },
        "all_levels_has_any_run_count": all_levels_any,
        "all_levels_has_any_run_ratio": round(all_levels_any / total, 6),
        "all_levels_cover_start_count": all_levels_start,
        "all_levels_cover_start_ratio": round(all_levels_start / total, 6),
        "all_levels_cover_end_count": all_levels_end,
        "all_levels_cover_end_ratio": round(all_levels_end / total, 6),
        "all_levels_cover_window_count": all_levels_window,
        "all_levels_cover_window_ratio": round(all_levels_window / total, 6),
        "effective_symbol_windows": effective_symbol_windows,
    }
    return {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "mode": mode,
        "levels": list(levels),
        "active_symbols_total": total,
        "rows": detail_rows,
        "summary": summary,
    }


async def build_historical_run_lookup_audit(
    module_c_repo: ModuleCRepository,
    *,
    symbols: list[str] | None = None,
    as_of_times: list[datetime] | None = None,
    levels: tuple[str, ...] = DEFAULT_LEVELS,
    mode: str = "predictive",
) -> dict[str, Any]:
    symbol_rows = await module_c_repo.list_active_symbols(symbols=symbols or list(DEFAULT_LOOKUP_SYMBOLS))
    audit_times = as_of_times or [datetime.fromisoformat(value) for value in DEFAULT_LOOKUP_TIMES]
    rows: list[dict[str, Any]] = []
    matrix: list[dict[str, Any]] = []
    for symbol in symbol_rows:
        for as_of_time in audit_times:
            row = {
                "symbol": symbol.symbol,
                "name": symbol.name,
                "as_of_time": as_of_time.isoformat(),
                "mode": mode,
                "levels": {},
            }
            matrix_row = {"symbol": symbol.symbol, "as_of_time": as_of_time.isoformat()}
            for level in levels:
                lookup = await module_c_repo.get_historical_run_lookup(symbol.symbol_id, level, mode, as_of_time)
                available = lookup.selected is not None
                row["levels"][level] = {
                    "available": available,
                    "run_count": lookup.run_count,
                    "selected_run_id": lookup.selected.run_id if lookup.selected else None,
                    "selected_bar_until": lookup.selected.bar_until.isoformat() if lookup.selected else None,
                    "selected_computed_at": lookup.selected.published_at.isoformat() if lookup.selected and lookup.selected.published_at else None,
                    "nearest_before_run_id": lookup.nearest_before.run_id if lookup.nearest_before else None,
                    "nearest_before_bar_until": lookup.nearest_before.bar_until.isoformat() if lookup.nearest_before else None,
                    "nearest_after_run_id": lookup.nearest_after.run_id if lookup.nearest_after else None,
                    "nearest_after_bar_until": lookup.nearest_after.bar_until.isoformat() if lookup.nearest_after else None,
                }
                matrix_row[level] = "PASS" if available else "FAIL"
            rows.append(row)
            matrix.append(matrix_row)
    pass_counter = Counter({level: 0 for level in levels})
    fail_counter = Counter({level: 0 for level in levels})
    for row in rows:
        for level, detail in row["levels"].items():
            if detail["available"]:
                pass_counter[level] += 1
            else:
                fail_counter[level] += 1
    return {
        "mode": mode,
        "levels": list(levels),
        "sample_symbols": [row.symbol for row in symbol_rows],
        "sample_times": [value.isoformat() for value in audit_times],
        "matrix": matrix,
        "rows": rows,
        "summary": {
            "pass_by_level": dict(pass_counter),
            "fail_by_level": dict(fail_counter),
        },
    }


def build_effective_backtest_window(coverage: dict[str, Any]) -> dict[str, Any]:
    effective_windows = coverage["summary"]["effective_symbol_windows"]
    if not effective_windows:
        return {
            "strict_global_effective_start": None,
            "strict_global_effective_end": None,
            "fully_covered_symbol_count": 0,
            "note": "no symbol has all requested levels available",
        }
    starts = [row["effective_start"] for row in effective_windows]
    ends = [row["effective_end"] for row in effective_windows]
    requested_start = coverage["start_time"]
    requested_end = coverage["end_time"]
    symbols_covering_requested_window = [
        row["symbol"]
        for row in effective_windows
        if row["effective_start"] <= requested_start and row["effective_end"] >= requested_end
    ]
    strict_start = max(starts)
    strict_end = min(ends)
    window_valid = strict_start <= strict_end
    return {
        "requested_start": requested_start,
        "requested_end": requested_end,
        "strict_global_effective_start": strict_start,
        "strict_global_effective_end": strict_end,
        "strict_global_window_valid": window_valid,
        "fully_covered_symbol_count": len(effective_windows),
        "symbols_covering_requested_window_count": len(symbols_covering_requested_window),
        "symbols_covering_requested_window_samples": symbols_covering_requested_window[:20],
        "sample_effective_windows": effective_windows[:20],
        "note": (
            "requested replay window is unsupported by current Module C history coverage"
            if not window_valid
            else "historical replay can only be considered fully supported for a symbol after all requested levels "
            "have at least one successful run with bar_until <= replay as_of_time"
        ),
    }


def write_module_c_history_coverage_csv(coverage: dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol_id",
                "symbol",
                "code",
                "exchange",
                "name",
                "level",
                "mode",
                "run_count",
                "first_bar_until",
                "last_bar_until",
                "first_computed_at",
                "last_computed_at",
                "has_any_run",
                "covers_backtest_start",
                "covers_backtest_end",
                "latest_before_start",
                "latest_before_end",
            ],
        )
        writer.writeheader()
        writer.writerows(coverage["rows"])


def render_module_c_history_coverage_markdown(coverage: dict[str, Any]) -> str:
    summary = coverage["summary"]
    lines = [
        "# Module C History Coverage",
        "",
        f"- Mode: `{coverage['mode']}`",
        f"- Levels: `{coverage['levels']}`",
        f"- Requested window: `{coverage['start_time']}` -> `{coverage['end_time']}`",
        f"- Active symbols: `{coverage['active_symbols_total']}`",
        f"- All levels have any run: `{summary['all_levels_has_any_run_count']}`",
        f"- All levels cover requested start: `{summary['all_levels_cover_start_count']}`",
        f"- All levels cover requested end: `{summary['all_levels_cover_end_count']}`",
        f"- All levels cover requested window: `{summary['all_levels_cover_window_count']}`",
        "",
        "| Level | Has Any Run | Cover Start | Cover End | First Run Min | Last Run Max | Missing Samples |",
        "| --- | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for level in coverage["levels"]:
        row = summary["levels"][level]
        lines.append(
            f"| `{level}` | {row['has_any_run_count']} ({row['has_any_run_ratio']:.4f}) | "
            f"{row['covers_backtest_start_count']} ({row['covers_backtest_start_ratio']:.4f}) | "
            f"{row['covers_backtest_end_count']} ({row['covers_backtest_end_ratio']:.4f}) | "
            f"{row['first_bar_until_min'] or 'n/a'} | {row['last_bar_until_max'] or 'n/a'} | "
            f"{', '.join(row['missing_samples']) or 'none'} |"
        )
    return "\n".join(lines) + "\n"


def render_historical_run_lookup_audit_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Historical Run Lookup Audit",
        "",
        f"- Mode: `{audit['mode']}`",
        f"- Symbols: `{audit['sample_symbols']}`",
        f"- As-of times: `{audit['sample_times']}`",
        "",
        "| Symbol | As Of | 5f | 30f | 1d | 1w | 1m |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in audit["matrix"]:
        lines.append(
            f"| `{row['symbol']}` | `{row['as_of_time']}` | `{row.get('5f', 'n/a')}` | `{row.get('30f', 'n/a')}` | "
            f"`{row.get('1d', 'n/a')}` | `{row.get('1w', 'n/a')}` | `{row.get('1m', 'n/a')}` |"
        )
    return "\n".join(lines) + "\n"


def render_effective_backtest_window_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Effective Backtest Window",
        "",
        f"- Requested start: `{payload.get('requested_start')}`",
        f"- Requested end: `{payload.get('requested_end')}`",
        f"- Strict global effective start: `{payload.get('strict_global_effective_start')}`",
        f"- Strict global effective end: `{payload.get('strict_global_effective_end')}`",
        f"- Strict global window valid: `{payload.get('strict_global_window_valid')}`",
        f"- Fully covered symbol count: `{payload.get('fully_covered_symbol_count')}`",
        f"- Symbols covering requested window: `{payload.get('symbols_covering_requested_window_count')}`",
        f"- Samples: `{payload.get('symbols_covering_requested_window_samples')}`",
        "",
        payload.get("note", ""),
    ]
    return "\n".join(lines) + "\n"


def render_phase_1_5_summary_markdown(
    *,
    coverage: dict[str, Any],
    lookup_audit: dict[str, Any],
    effective_window: dict[str, Any],
    replay_audit: dict[str, Any],
) -> str:
    lines = [
        "# Phase 1.5 Summary",
        "",
        f"- Coverage mode: `{coverage['mode']}`",
        f"- Coverage active symbols: `{coverage['active_symbols_total']}`",
        f"- All-level requested-window coverage: `{coverage['summary']['all_levels_cover_window_count']}` / `{coverage['active_symbols_total']}`",
        f"- Strict global effective start: `{effective_window.get('strict_global_effective_start')}`",
        f"- Strict global effective end: `{effective_window.get('strict_global_effective_end')}`",
        f"- Replay symbols: `{replay_audit['replayed_symbols']}`",
        f"- Replay total steps: `{replay_audit['total_replay_steps']}`",
        f"- Future leakage detected: `{replay_audit['future_leakage_detected']}`",
        f"- Lookup pass-by-level: `{lookup_audit['summary']['pass_by_level']}`",
        f"- Lookup fail-by-level: `{lookup_audit['summary']['fail_by_level']}`",
    ]
    return "\n".join(lines) + "\n"


async def _load_active_symbols(
    pool: asyncpg.Pool,
    *,
    symbols: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    where_clause = "where is_active = true"
    args: list[Any] = []
    if symbols:
        where_clause += " and (code || '.' || exchange) = any($1::text[])"
        args.append(symbols)
    limit_sql = ""
    if limit > 0:
        limit_sql = f" limit ${len(args) + 1}"
        args.append(limit)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            select id as symbol_id, code, exchange, name, (code || '.' || exchange) as symbol
            from symbols
            {where_clause}
            order by id
            {limit_sql}
            """,
            *args,
        )
    return [dict(row) for row in rows]


def _level_name(db_level: int) -> str:
    for key, value in LEVEL_TO_DB.items():
        if value == db_level:
            return key
    raise KeyError(db_level)


def _safe_min(values: list[str]) -> str | None:
    return min(values) if values else None


def _safe_max(values: list[str]) -> str | None:
    return max(values) if values else None
