from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.models import (
    ChanCenterResponse,
    ChanChannelResponse,
    ChanOverlayResponse,
    ChanPointResponse,
    ChanSignalResponse,
    ChanStrokeResponse,
)
from app.repositories.postgres import TIMEFRAME_TO_DB, split_symbol
from trading_protocol import MODULE_C_CONFIG_HASH

MODE_TO_DB = {
    "confirmed": 1,
    "predictive": 2,
}

DB_TO_MODE = {value: key for key, value in MODE_TO_DB.items()}
DB_TO_LEVEL = {value: key for key, value in TIMEFRAME_TO_DB.items()}
DB_TO_DIRECTION = {
    1: "up",
    -1: "down",
}
MAX_OVERLAY_ITEMS_PER_KIND = 2_000
CHINA_TZ = ZoneInfo("Asia/Shanghai")

MODULE_C_CHAN_TABLES = {
    "strokes": "chan_c_strokes",
    "segments": "chan_c_segments",
    "centers": "chan_c_centers",
    "signals": "chan_c_signals",
    "published_heads": "scheme2_chan_c_published_heads",
}
SUPPORTED_MODULE_C_CONFIG_HASHES = (
    MODULE_C_CONFIG_HASH,
    "module-c:native-5lvl-v3-bi-strict-false",
)


def _tables() -> dict[str, str]:
    return MODULE_C_CHAN_TABLES


async def get_available_precomputed_chan_levels_db(
    pool,
    *,
    symbol: str,
    levels: list[str],
    storage_namespace: str = "c",
) -> list[str]:
    requested_codes = [TIMEFRAME_TO_DB[level] for level in levels]
    if not requested_codes:
        return []
    code, exchange = split_symbol(symbol)
    table = _tables()["published_heads"]
    try:
        rows = await pool.fetch(
            f"""
            select distinct head.chan_level
            from {table} head
            join symbols symbol on symbol.id = head.symbol_id
            where symbol.code = $1
              and symbol.exchange = $2
              and symbol.is_active = true
              and head.status = 'published'
              and head.run_id is not null
              and head.chan_level = any($3::integer[])
            """,
            code,
            exchange,
            requested_codes,
        )
    except Exception as exc:
        if exc.__class__.__name__ in {"UndefinedColumnError", "UndefinedTableError"}:
            return []
        raise
    available = {int(row["chan_level"]) for row in rows}
    return [level for level in levels if TIMEFRAME_TO_DB[level] in available]


async def get_module_c_published_head_coverage_db(pool) -> dict[str, Any]:
    """Report published Module C coverage separately from the legacy chan service."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select run.config_hash, head.chan_level, head.mode,
                       count(*) as heads,
                       count(distinct head.symbol_id) as active_symbols,
                       max(head.base_to_bar_end) as latest_base_to_bar_end,
                       max(head.published_at) as latest_published_at
                from scheme2_chan_c_published_heads head
                join symbols symbol on symbol.id = head.symbol_id
                join chan_c_runs run on run.id = head.run_id
                where symbol.is_active
                  and head.status = 'published'
                  and head.run_id is not null
                  and head.base_timeframe = head.chan_level
                  and run.status = 'success'
                  and run.config_hash = any($1::varchar[])
                group by run.config_hash, head.chan_level, head.mode
                order by run.config_hash, head.chan_level, head.mode
                """
            , list(SUPPORTED_MODULE_C_CONFIG_HASHES))
            candidate = await conn.fetchrow(
                """
                select symbol.code || '.' || symbol.exchange as symbol,
                       head.chan_level, head.mode, run.config_hash,
                       head.base_to_bar_end, head.published_at
                from scheme2_chan_c_published_heads head
                join symbols symbol on symbol.id = head.symbol_id
                join chan_c_runs run on run.id = head.run_id
                where symbol.is_active
                  and head.status = 'published'
                  and head.run_id is not null
                  and head.base_timeframe = head.chan_level
                  and run.status = 'success'
                  and run.config_hash = any($1::varchar[])
                  and exists (
                      select 1 from chan_c_strokes stroke
                      where stroke.run_id = head.run_id
                        and stroke.mode = case head.mode
                            when 'confirmed' then 1
                            when 'predictive' then 2
                        end
                  )
                order by head.published_at desc nulls last, head.id desc
                limit 1
                """
            , list(SUPPORTED_MODULE_C_CONFIG_HASHES))
    except Exception as exc:
        if exc.__class__.__name__ in {"UndefinedColumnError", "UndefinedTableError"}:
            return {"ready": False, "reason": "module_c_schema_unavailable", "coverage": []}
        raise

    coverage = [dict(row) for row in rows]
    current_config_heads = sum(
        int(row["heads"])
        for row in coverage
        if row["config_hash"] == MODULE_C_CONFIG_HASH
    )
    compatible_heads = sum(
        int(row["heads"])
        for row in coverage
        if row["config_hash"] in SUPPORTED_MODULE_C_CONFIG_HASHES
    )
    return {
        "ready": compatible_heads > 0,
        "configured_config_hash": MODULE_C_CONFIG_HASH,
        "compatible_config_hashes": list(SUPPORTED_MODULE_C_CONFIG_HASHES),
        "current_config_heads": current_config_heads,
        "current_config_ready": current_config_heads > 0,
        "compatible_heads": compatible_heads,
        "coverage": coverage,
        "published_smoke_candidate": dict(candidate) if candidate is not None else None,
    }


async def get_precomputed_chan_overlay_db(
    pool,
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
    storage_namespace: str = "c",
) -> ChanOverlayResponse | None:
    try:
        return await _get_precomputed_chan_overlay_db(
            pool,
            symbol=symbol,
            chart_timeframe=chart_timeframe,
            levels=levels,
            modes=modes,
            requested_bar_count=requested_bar_count,
            bars_by_level=bars_by_level,
            storage_namespace=storage_namespace,
        )
    except Exception as exc:
        if exc.__class__.__name__ in {"UndefinedColumnError", "UndefinedTableError"}:
            return None
        raise


class OverlayTooLargeError(RuntimeError):
    """The bounded query found more detail than the API response contract allows."""


class _OutputBudget:
    def __init__(self, maximum: int) -> None:
        self.remaining = maximum

    def consume(self, rows: list[Any]) -> list[Any]:
        if len(rows) > self.remaining:
            raise OverlayTooLargeError("Overlay contains too many items")
        self.remaining -= len(rows)
        return rows


async def get_windowed_module_c_overlay_db(
    pool,
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    first_ts: datetime,
    last_ts: datetime,
    requested_bar_count: int,
) -> ChanOverlayResponse | None:
    """Read authoritative Module C detail without loading K-lines or full runs."""
    try:
        return await _get_windowed_module_c_overlay_db(
            pool, symbol=symbol, chart_timeframe=chart_timeframe, levels=levels,
            modes=modes, first_ts=first_ts, last_ts=last_ts,
            requested_bar_count=requested_bar_count,
        )
    except Exception as exc:
        if exc.__class__.__name__ in {"UndefinedColumnError", "UndefinedTableError"}:
            return None
        raise


async def _get_windowed_module_c_overlay_db(
    pool,
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    first_ts: datetime,
    last_ts: datetime,
    requested_bar_count: int,
) -> ChanOverlayResponse | None:
    code, exchange = split_symbol(symbol)
    async with pool.acquire() as conn:
        symbol_id = await conn.fetchval(
            "select id from symbols where code = $1 and exchange = $2 and is_active = true",
            code, exchange,
        )
        if symbol_id is None:
            return None
        runs = await _select_windowed_module_c_runs(
            conn, symbol_id=symbol_id, levels=levels, modes=modes,
            first_ts=first_ts, last_ts=last_ts,
        )
        if runs is None:
            return None

        strokes: list[ChanStrokeResponse] = []
        segments: list[ChanStrokeResponse] = []
        centers: list[ChanCenterResponse] = []
        signals: list[ChanSignalResponse] = []
        budget = _OutputBudget(MAX_OVERLAY_ITEMS_PER_KIND)
        for level in levels:
            for mode in modes:
                run = runs[(level, mode)]
                mode_code = MODE_TO_DB[mode]
                strokes.extend(
                    _stroke_row_to_response(row, level)
                    for row in await _fetch_windowed_stroke_like(
                        conn, MODULE_C_CHAN_TABLES["strokes"], run["run_id"], mode_code,
                        first_ts, last_ts, budget,
                    )
                )
                segments.extend(
                    _stroke_row_to_response(row, level)
                    for row in await _fetch_windowed_stroke_like(
                        conn, MODULE_C_CHAN_TABLES["segments"], run["run_id"], mode_code,
                        first_ts, last_ts, budget,
                    )
                )
                centers.extend(
                    _center_row_to_response(row, level)
                    for row in await _fetch_windowed_centers(
                        conn, run["run_id"], mode_code, first_ts, last_ts, budget,
                    )
                )
                signals.extend(
                    _signal_row_to_response(row, level)
                    for row in await _fetch_windowed_signals(
                        conn, run["run_id"], mode_code, first_ts, last_ts, budget,
                    )
                )

    return ChanOverlayResponse(
        symbol=symbol, chart_timeframe=chart_timeframe, levels=levels, modes=modes,
        snapshot_version=_windowed_snapshot_version(symbol, runs), base_timeframe="native",
        base_ts_semantics="bar_end", engine="database:chan-module-c-windowed",
        requested_bar_count=requested_bar_count, bars_by_level={level: 0 for level in levels},
        strokes=_sort_detail(strokes), segments=_sort_detail(segments),
        centers=_sort_detail(centers), signals=_sort_detail(signals), channels=[],
    )


async def _select_windowed_module_c_runs(
    conn, *, symbol_id: int, levels: list[str], modes: list[str], first_ts: datetime, last_ts: datetime,
) -> dict[tuple[str, str], dict[str, Any]] | None:
    rows = await conn.fetch(
        """
        select head.chan_level, head.mode, head.run_id, head.snapshot_version,
               head.base_from_bar_end, head.base_to_bar_end,
               run.bar_from, run.bar_until, run.computed_at
        from scheme2_chan_c_published_heads head
        join chan_c_runs run on run.id = head.run_id
        where head.symbol_id = $1
          and head.chan_level = any($2::integer[])
          and head.mode = any($3::varchar[])
          and head.base_timeframe = head.chan_level
          and head.status = 'published'
          and head.run_id is not null
          and head.base_from_bar_end <= $4 and head.base_to_bar_end >= $5
          and run.status = 'success' and run.config_hash = any($6::varchar[])
          and run.bar_from <= $4 and run.bar_until >= $5
        order by head.chan_level, head.mode, coalesce(head.published_at, head.updated_at) desc, head.id desc
        """,
        symbol_id, [TIMEFRAME_TO_DB[level] for level in levels], modes,
        first_ts, last_ts, list(SUPPORTED_MODULE_C_CONFIG_HASHES),
    )
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        level = DB_TO_LEVEL.get(int(row["chan_level"]))
        mode = str(row["mode"])
        if level in levels and mode in modes:
            selected.setdefault((level, mode), dict(row))
    if len(selected) != len(levels) * len(modes):
        return None
    return selected


async def _fetch_windowed_stroke_like(
    conn, table: str, run_id: int, mode_code: int, first_ts: datetime, last_ts: datetime,
    budget: _OutputBudget,
):
    intersecting = await conn.fetch(
        f"""
        select id, mode, seq, start_ts, end_ts,
               coalesce(begin_base_ts, start_ts) as begin_base_ts,
               coalesce(end_base_ts, end_ts) as end_base_ts,
               begin_base_seq, end_base_seq, start_price_x1000, end_price_x1000,
               direction, is_confirmed, extra
        from {table}
        where run_id = $1 and mode = $2
          and tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')
              && tstzrange($3, $4, '[]')
        order by coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), seq, id
        limit $5
        """,
        run_id, mode_code, first_ts, last_ts, budget.remaining + 1,
    )
    rows = budget.consume(intersecting)
    for predicate, order, boundary_ts in (("< $3", "desc", first_ts), ("> $3", "asc", last_ts)):
        if budget.remaining == 0:
            raise OverlayTooLargeError("Overlay contains too many items")
        boundary = "end_base_ts" if predicate.startswith("<") else "begin_base_ts"
        adjacent = await conn.fetch(
            f"""
            select id, mode, seq, start_ts, end_ts,
                   coalesce(begin_base_ts, start_ts) as begin_base_ts,
                   coalesce(end_base_ts, end_ts) as end_base_ts,
                   begin_base_seq, end_base_seq, start_price_x1000, end_price_x1000,
                   direction, is_confirmed, extra
            from {table}
            where run_id = $1 and mode = $2
              and coalesce({boundary}, {'end_ts' if boundary == 'end_base_ts' else 'start_ts'}) {predicate}
            order by coalesce({boundary}, {'end_ts' if boundary == 'end_base_ts' else 'start_ts'}) {order}, seq {order}, id {order}
            limit 1
            """,
            run_id, mode_code, boundary_ts,
        )
        rows.extend(budget.consume(adjacent))
    return rows


async def _fetch_windowed_centers(
    conn, run_id: int, mode_code: int, first_ts: datetime, last_ts: datetime, budget: _OutputBudget,
):
    rows = await conn.fetch(
        """
        select id, mode, seq, start_ts, end_ts,
               coalesce(begin_base_ts, start_ts) as begin_base_ts,
               coalesce(end_base_ts, end_ts) as end_base_ts,
               begin_base_seq, end_base_seq, low_x1000, high_x1000, is_confirmed, extra
        from chan_c_centers
        where run_id = $1 and mode = $2
          and tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')
              && tstzrange($3, $4, '[]')
        order by coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), seq, id
        limit $5
        """, run_id, mode_code, first_ts, last_ts, budget.remaining + 1,
    )
    return budget.consume(rows)


async def _fetch_windowed_signals(
    conn, run_id: int, mode_code: int, first_ts: datetime, last_ts: datetime, budget: _OutputBudget,
):
    rows = await conn.fetch(
        """
        select id, mode, ts, coalesce(base_ts, ts) as base_ts, base_seq,
               price_x1000, signal_type, is_confirmed, extra
        from chan_c_signals
        where run_id = $1 and mode = $2
          and coalesce(base_ts, ts) >= $3 and coalesce(base_ts, ts) <= $4
        order by coalesce(base_ts, ts), id
        limit $5
        """, run_id, mode_code, first_ts, last_ts, budget.remaining + 1,
    )
    return budget.consume(rows)


def _windowed_snapshot_version(symbol: str, runs: dict[tuple[str, str], dict[str, Any]]) -> str:
    parts = [
        f"{level}:{mode}:{run['run_id']}:{run.get('snapshot_version') or ''}"
        for (level, mode), run in sorted(runs.items())
    ]
    return f"{symbol}:module-c:{'|'.join(parts)}"


def _sort_detail(items: list[Any]) -> list[Any]:
    return sorted(items, key=lambda item: (item.level, item.mode, item.seq or -1, item.id))


async def _get_precomputed_chan_overlay_db(
    pool,
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
    storage_namespace: str,
) -> ChanOverlayResponse | None:
    tables = _tables()
    mode_codes = [MODE_TO_DB[mode] for mode in modes if mode in MODE_TO_DB]
    if not mode_codes:
        return None
    if MODE_TO_DB["predictive"] in mode_codes and MODE_TO_DB["confirmed"] not in mode_codes:
        # Module C keeps confirmed structures available for predictive-only requests.
        mode_codes.append(MODE_TO_DB["confirmed"])
    window_bars = bars_by_level.get(chart_timeframe) or bars_by_level.get("5f")
    if not window_bars:
        return None
    first_ts = _epoch_to_datetime(window_bars[0]["time"])
    last_ts = _epoch_to_datetime(window_bars[-1]["time"])

    code, exchange = split_symbol(symbol)
    runs: dict[str, dict] = {}
    async with pool.acquire() as conn:
        symbol_id = await conn.fetchval(
            """
            select id
            from symbols
            where code = $1 and exchange = $2 and is_active = true
            """,
            code,
            exchange,
        )
        if symbol_id is None:
            return None

        runs = await _select_runs(
            conn,
            symbol_id=symbol_id,
            levels=levels,
            first_ts=first_ts,
            last_ts=last_ts,
            tables=tables,
        )
        if runs is None:
            return None

        strokes: list[ChanStrokeResponse] = []
        segments: list[ChanStrokeResponse] = []
        centers: list[ChanCenterResponse] = []
        signals: list[ChanSignalResponse] = []
        channels: list[ChanChannelResponse] = []
        projection_bars = _projection_bars_for_chart(bars_by_level, chart_timeframe)
        for level in levels:
            run = runs[level]
            native_bars = _native_bars_for_level(bars_by_level, level)
            strokes.extend(
                [
                    _stroke_row_to_response(
                        row,
                        level,
                        chart_timeframe=chart_timeframe,
                        projection_bars=projection_bars,
                        native_bars=native_bars,
                    )
                    for row in await _fetch_stroke_like(
                        conn,
                        tables["strokes"],
                        run["id"],
                        mode_codes,
                        run["first_ts"],
                        run["last_ts"],
                    )
                ]
            )
            segments.extend(
                [
                    _stroke_row_to_response(
                        row,
                        level,
                        chart_timeframe=chart_timeframe,
                        projection_bars=projection_bars,
                        native_bars=native_bars,
                    )
                    for row in await _fetch_stroke_like(
                        conn,
                        tables["segments"],
                        run["id"],
                        mode_codes,
                        run["first_ts"],
                        run["last_ts"],
                    )
                ]
            )
            centers.extend(
                [
                    _center_row_to_response(row, level)
                    for row in await _fetch_centers(
                        conn,
                        tables["centers"],
                        run["id"],
                        mode_codes,
                        run["first_ts"],
                        run["last_ts"],
                    )
                ]
            )
            signals.extend(
                [
                    _signal_row_to_response(
                        row,
                        level,
                        chart_timeframe=chart_timeframe,
                        projection_bars=projection_bars,
                        native_bars=native_bars,
                    )
                    for row in await _fetch_signals(
                        conn,
                        tables["signals"],
                        run["id"],
                        mode_codes,
                        run["first_ts"],
                        run["last_ts"],
                    )
                ]
            )

    return ChanOverlayResponse(
        symbol=symbol,
        chart_timeframe=chart_timeframe,
        levels=levels,
        modes=modes,
        snapshot_version=_snapshot_version(symbol, runs),
        base_timeframe="native",
        base_ts_semantics="bar_end",
        engine="database:chan-module-c-precomputed",
        requested_bar_count=requested_bar_count,
        bars_by_level={level: len(bars_by_level.get(level, [])) for level in levels},
        strokes=strokes,
        segments=segments,
        centers=centers,
        signals=signals,
        channels=channels,
    )


async def _select_runs(
    conn,
    *,
    symbol_id: int,
    levels: list[str],
    first_ts: datetime,
    last_ts: datetime,
    tables: dict[str, str],
) -> dict[str, dict[str, Any]] | None:
    requested_codes = [TIMEFRAME_TO_DB[level] for level in levels]
    published_heads_table = tables["published_heads"]
    published_rows = await conn.fetch(
        f"""
        select
            chan_level,
            mode,
            run_id,
            snapshot_version,
            base_from_bar_end,
            base_to_bar_end,
            published_at,
            updated_at
        from {published_heads_table}
        where symbol_id = $1
          and chan_level = any($2::integer[])
          and status = 'published'
          and run_id is not null
          and base_from_bar_end is not null
          and base_to_bar_end is not null
          and base_from_bar_end <= $3
          and base_to_bar_end >= $4
        order by coalesce(published_at, updated_at) desc, id desc
        """,
        symbol_id,
        requested_codes,
        last_ts,
        first_ts,
    )
    published = _group_published_runs(
        published_rows,
        requested_codes=requested_codes,
        first_ts=first_ts,
        last_ts=last_ts,
    )
    if published is not None:
        return published

    return _latest_published_runs_by_level(
        published_rows,
        requested_codes=requested_codes,
        first_ts=first_ts,
        last_ts=last_ts,
    )


def _group_published_runs(
    rows: list[Any],
    *,
    requested_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
) -> dict[str, dict[str, Any]] | None:
    requested_set = set(requested_codes)
    grouped: dict[str, dict[int, Any]] = {}
    ordered_versions: list[str] = []
    for row in rows:
        snapshot_version = str(row.get("snapshot_version") or "").strip()
        run_id = row.get("run_id")
        if not snapshot_version or run_id is None:
            continue
        if snapshot_version not in grouped:
            grouped[snapshot_version] = {}
            ordered_versions.append(snapshot_version)
        grouped[snapshot_version].setdefault(int(row["chan_level"]), row)

    for snapshot_version in ordered_versions:
        group = grouped[snapshot_version]
        if not requested_set.issubset(group.keys()):
            continue
        return {
            DB_TO_LEVEL[code]: {
                "id": int(group[code]["run_id"]),
                "first_ts": first_ts,
                "last_ts": last_ts,
                "snapshot_version": snapshot_version,
            }
            for code in requested_codes
        }
    return None


def _latest_published_runs_by_level(
    rows: list[Any],
    *,
    requested_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
) -> dict[str, dict[str, Any]] | None:
    by_level: dict[int, Any] = {}
    for row in rows:
        level_code = int(row["chan_level"])
        snapshot_version = str(row.get("snapshot_version") or "").strip()
        run_id = row.get("run_id")
        if not snapshot_version or run_id is None or level_code in by_level:
            continue
        by_level[level_code] = row

    if not set(requested_codes).issubset(by_level.keys()):
        return None

    return {
        DB_TO_LEVEL[code]: {
            "id": int(by_level[code]["run_id"]),
            "first_ts": first_ts,
            "last_ts": last_ts,
            "snapshot_version": str(by_level[code]["snapshot_version"]).strip(),
        }
        for code in requested_codes
    }


async def _fetch_stroke_like(
    conn,
    table: str,
    run_id: int,
    mode_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
):
    return await conn.fetch(
        f"""
        select
            id,
            mode,
            seq,
            start_ts,
            end_ts,
            coalesce(begin_base_ts, start_ts) as begin_base_ts,
            coalesce(end_base_ts, end_ts) as end_base_ts,
            begin_base_seq,
            end_base_seq,
            start_price_x1000,
            end_price_x1000,
            direction,
            is_confirmed,
            extra
        from {table}
        where run_id = $1
          and mode = any($2::smallint[])
          and coalesce(begin_base_ts, start_ts) <= $4
          and coalesce(end_base_ts, end_ts) >= $3
        order by begin_base_ts, end_base_ts, seq
        """,
        run_id,
        mode_codes,
        first_ts,
        last_ts,
    )


async def _fetch_centers(
    conn,
    table: str,
    run_id: int,
    mode_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
):
    return await conn.fetch(
        f"""
        select
            id,
            mode,
            seq,
            start_ts,
            end_ts,
            coalesce(begin_base_ts, start_ts) as begin_base_ts,
            coalesce(end_base_ts, end_ts) as end_base_ts,
            begin_base_seq,
            end_base_seq,
            low_x1000,
            high_x1000,
            is_confirmed,
            extra
        from {table}
        where run_id = $1
          and mode = any($2::smallint[])
          and coalesce(begin_base_ts, start_ts) <= $4
          and coalesce(end_base_ts, end_ts) >= $3
        order by begin_base_ts, end_base_ts, seq
        """,
        run_id,
        mode_codes,
        first_ts,
        last_ts,
    )


async def _fetch_signals(
    conn,
    table: str,
    run_id: int,
    mode_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
):
    return await conn.fetch(
        f"""
        select
            id,
            mode,
            ts,
            coalesce(base_ts, ts) as base_ts,
            base_seq,
            price_x1000,
            signal_type,
            is_confirmed,
            extra
        from {table}
        where run_id = $1
          and mode = any($2::smallint[])
          and coalesce(base_ts, ts) >= $3
          and coalesce(base_ts, ts) <= $4
        order by base_ts, id
        """,
        run_id,
        mode_codes,
        first_ts,
        last_ts,
    )


def _stroke_row_to_response(
    row,
    level: str,
    *,
    chart_timeframe: str | None = None,
    projection_bars: list[dict] | None = None,
    native_bars: list[dict] | None = None,
) -> ChanStrokeResponse:
    begin_base_dt = row.get("begin_base_ts") or row["start_ts"]
    end_base_dt = row.get("end_base_ts") or row["end_ts"]
    begin_base_ts = int(begin_base_dt.timestamp())
    end_base_ts = int(end_base_dt.timestamp())
    start_price = row["start_price_x1000"] / 1000
    end_price = row["end_price_x1000"] / 1000
    direction = DB_TO_DIRECTION.get(row["direction"], "up")
    begin_base_ts = _project_point_timestamp(
        level=level,
        chart_timeframe=chart_timeframe,
        original_ts=begin_base_ts,
        price=start_price,
        projection_bars=projection_bars,
        native_bars=native_bars,
        preferred_price_field="low" if direction == "up" else "high",
    )
    end_base_ts = _project_point_timestamp(
        level=level,
        chart_timeframe=chart_timeframe,
        original_ts=end_base_ts,
        price=end_price,
        projection_bars=projection_bars,
        native_bars=native_bars,
        preferred_price_field="high" if direction == "up" else "low",
    )
    return ChanStrokeResponse(
        id=_read_extra_id(row, f"{level}:stroke:{row['id']}"),
        seq=row.get("seq"),
        level=level,
        mode=DB_TO_MODE.get(row["mode"], "confirmed"),
        start=ChanPointResponse(
            time=begin_base_ts,
            price=start_price,
            base_ts=begin_base_ts,
            base_seq=row.get("begin_base_seq"),
        ),
        end=ChanPointResponse(
            time=end_base_ts,
            price=end_price,
            base_ts=end_base_ts,
            base_seq=row.get("end_base_seq"),
        ),
        begin_base_ts=begin_base_ts,
        end_base_ts=end_base_ts,
        begin_base_seq=row.get("begin_base_seq"),
        end_base_seq=row.get("end_base_seq"),
        direction=direction,
        confirmed=row["is_confirmed"],
    )


def _center_row_to_response(row, level: str) -> ChanCenterResponse:
    begin_base_dt = row.get("begin_base_ts") or row["start_ts"]
    end_base_dt = row.get("end_base_ts") or row["end_ts"]
    begin_base_ts = int(begin_base_dt.timestamp())
    end_base_ts = int(end_base_dt.timestamp())
    return ChanCenterResponse(
        id=_read_extra_id(row, f"{level}:center:{row['id']}"),
        seq=row.get("seq"),
        level=level,
        mode=DB_TO_MODE.get(row["mode"], "confirmed"),
        start_time=begin_base_ts,
        end_time=end_base_ts,
        begin_base_ts=begin_base_ts,
        end_base_ts=end_base_ts,
        begin_base_seq=row.get("begin_base_seq"),
        end_base_seq=row.get("end_base_seq"),
        low=row["low_x1000"] / 1000,
        high=row["high_x1000"] / 1000,
        confirmed=row["is_confirmed"],
    )


def _signal_row_to_response(
    row,
    level: str,
    *,
    chart_timeframe: str | None = None,
    projection_bars: list[dict] | None = None,
    native_bars: list[dict] | None = None,
) -> ChanSignalResponse:
    base_dt = row.get("base_ts") or row["ts"]
    base_ts = int(base_dt.timestamp())
    extra = _read_extra(row)
    price = row["price_x1000"] / 1000
    base_ts = _project_point_timestamp(
        level=level,
        chart_timeframe=chart_timeframe,
        original_ts=base_ts,
        price=price,
        projection_bars=projection_bars,
        native_bars=native_bars,
    )
    return ChanSignalResponse(
        id=_read_extra_id(row, f"{level}:signal:{row['id']}"),
        seq=row.get("id"),
        level=level,
        mode=DB_TO_MODE.get(row["mode"], "confirmed"),
        time=base_ts,
        base_ts=base_ts,
        base_seq=row.get("base_seq"),
        price=price,
        signal_type=row["signal_type"],
        side=extra.get("side"),
        bsp_type=extra.get("bsp_type"),
        features=extra.get("features") if isinstance(extra.get("features"), dict) else {},
        confirmed=row["is_confirmed"],
    )


def _read_extra_id(row, fallback: str) -> str:
    extra = _read_extra(row)
    if isinstance(extra, dict) and extra.get("id"):
        return str(extra["id"])
    return fallback


def _read_extra(row) -> dict[str, Any]:
    extra = row["extra"]
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except json.JSONDecodeError:
            extra = None
    return extra if isinstance(extra, dict) else {}


def _projection_bars_for_chart(
    bars_by_level: dict[str, list[dict]],
    chart_timeframe: str,
) -> list[dict]:
    bars = bars_by_level.get(chart_timeframe) or []
    if not bars and chart_timeframe != "5f":
        bars = bars_by_level.get("5f") or []
    return sorted(
        [bar for bar in bars if isinstance(bar.get("time"), int)],
        key=lambda item: int(item["time"]),
    )


def _native_bars_for_level(
    bars_by_level: dict[str, list[dict]],
    level: str,
) -> list[dict]:
    return sorted(
        [
            bar
            for bar in bars_by_level.get(level, [])
            if isinstance(bar.get("time"), int)
        ],
        key=lambda item: int(item["time"]),
    )


def _project_point_timestamp(
    *,
    level: str,
    chart_timeframe: str | None,
    original_ts: int,
    price: float,
    projection_bars: list[dict] | None,
    native_bars: list[dict] | None = None,
    preferred_price_field: str | None = None,
) -> int:
    if not chart_timeframe or not projection_bars:
        return original_ts
    level_minutes = TIMEFRAME_TO_DB.get(level)
    chart_minutes = TIMEFRAME_TO_DB.get(chart_timeframe)
    if level_minutes is None or chart_minutes is None or level_minutes <= chart_minutes:
        return original_ts

    candidates = _candidate_projection_bars(
        level=level,
        original_ts=original_ts,
        projection_bars=projection_bars,
        native_bars=native_bars,
    )
    if not candidates:
        return original_ts

    target = float(price)
    exact = _last_exact_price_match(
        candidates,
        target,
        preferred_price_field=preferred_price_field,
    )
    if exact is not None:
        return int(exact["time"])

    def score(bar: dict) -> tuple[float, float, int]:
        high = _float_or_none(bar.get("high"))
        low = _float_or_none(bar.get("low"))
        preferred = _float_or_none(bar.get(preferred_price_field)) if preferred_price_field else None
        preferred_diff = abs(preferred - target) if preferred is not None else float("inf")
        high_low_diffs = [abs(value - target) for value in (high, low) if value is not None]
        best_high_low_diff = min(high_low_diffs) if high_low_diffs else float("inf")
        return (preferred_diff, best_high_low_diff, abs(int(bar["time"]) - original_ts))

    best = min(candidates, key=score)
    return int(best["time"])


def _last_exact_price_match(
    candidates: list[dict],
    target: float,
    *,
    preferred_price_field: str | None,
) -> dict | None:
    field_groups: list[list[str]] = []
    if preferred_price_field:
        field_groups.append([preferred_price_field])
    field_groups.append(["high", "low"])
    field_groups.append(["open", "close"])

    for fields in field_groups:
        matches = [
            bar
            for bar in candidates
            if any(_price_equal(_float_or_none(bar.get(field)), target) for field in fields)
        ]
        if matches:
            return max(matches, key=lambda item: int(item["time"]))
    return None


def _price_equal(value: float | None, target: float) -> bool:
    if value is None:
        return False
    return abs(value - target) <= 0.0005


def _candidate_projection_bars(
    *,
    level: str,
    original_ts: int,
    projection_bars: list[dict],
    native_bars: list[dict] | None = None,
) -> list[dict]:
    native_window = _native_projection_window(
        level=level,
        original_ts=original_ts,
        native_bars=native_bars,
    )
    if native_window is not None:
        start_ts, end_ts = native_window
        return [
            bar
            for bar in projection_bars
            if start_ts < int(bar["time"]) <= end_ts
        ]

    if level == "1d":
        target_day = datetime.fromtimestamp(original_ts, tz=UTC).astimezone(CHINA_TZ).date()
        return [
            bar
            for bar in projection_bars
            if datetime.fromtimestamp(int(bar["time"]), tz=UTC).astimezone(CHINA_TZ).date()
            == target_day
        ]

    level_minutes = TIMEFRAME_TO_DB.get(level)
    if level_minutes is None:
        return []
    start_ts = original_ts - level_minutes * 60
    if level_minutes >= TIMEFRAME_TO_DB["1d"]:
        return [
            bar
            for bar in projection_bars
            if start_ts < int(bar["time"]) <= original_ts
        ]
    target_day = datetime.fromtimestamp(original_ts, tz=UTC).astimezone(CHINA_TZ).date()
    return [
        bar
        for bar in projection_bars
        if start_ts < int(bar["time"]) <= original_ts
        and datetime.fromtimestamp(int(bar["time"]), tz=UTC).astimezone(CHINA_TZ).date()
        == target_day
    ]


def _native_projection_window(
    *,
    level: str,
    original_ts: int,
    native_bars: list[dict] | None,
) -> tuple[int, int] | None:
    if not native_bars:
        return None
    previous_ts: int | None = None
    for bar in native_bars:
        bar_ts = int(bar["time"])
        if bar_ts == original_ts:
            return (previous_ts or _native_window_fallback_start(level, original_ts), bar_ts)
        if bar_ts > original_ts:
            return None
        previous_ts = bar_ts
    return None


def _native_window_fallback_start(level: str, original_ts: int) -> int:
    level_minutes = TIMEFRAME_TO_DB.get(level)
    if level_minutes is not None and level_minutes != TIMEFRAME_TO_DB["1d"]:
        return original_ts - level_minutes * 60
    target_day = datetime.fromtimestamp(original_ts, tz=UTC).astimezone(CHINA_TZ).date()
    return int(
        datetime(
            target_day.year,
            target_day.month,
            target_day.day,
            tzinfo=CHINA_TZ,
        )
        .astimezone(UTC)
        .timestamp()
    )


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _epoch_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value, tz=UTC)


def _snapshot_version(symbol: str, runs: dict[str, dict[str, Any]]) -> str:
    versions = {
        str(run.get("snapshot_version") or "").strip()
        for run in runs.values()
        if str(run.get("snapshot_version") or "").strip()
    }
    if len(versions) == 1:
        return versions.pop()
    parts = []
    for level in sorted(runs):
        run = runs[level]
        parts.append(
            f"{level}:{run['id']}:{int(run['first_ts'].timestamp())}:{int(run['last_ts'].timestamp())}"
        )
    return f"{symbol}:db:{'|'.join(parts)}"
