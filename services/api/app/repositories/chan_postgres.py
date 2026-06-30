from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app.models import (
    ChanCenterResponse,
    ChanChannelResponse,
    ChanOverlayResponse,
    ChanPointResponse,
    ChanSignalResponse,
    ChanStrokeResponse,
)
from app.repositories.postgres import TIMEFRAME_TO_DB, split_symbol

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


async def get_precomputed_chan_overlay_db(
    pool,
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
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
        )
    except Exception as exc:
        if exc.__class__.__name__ in {"UndefinedColumnError", "UndefinedTableError"}:
            return None
        raise


async def _get_precomputed_chan_overlay_db(
    pool,
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
) -> ChanOverlayResponse | None:
    mode_codes = [MODE_TO_DB[mode] for mode in modes if mode in MODE_TO_DB]
    if not mode_codes:
        return None
    if MODE_TO_DB["predictive"] in mode_codes and MODE_TO_DB["confirmed"] not in mode_codes:
        # The current module-b chan.py integration publishes predictive heads for API compatibility,
        # but persists confirmed structures only. Fall back to confirmed rows until a real
        # predictive structure extractor is added.
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
        )
        if runs is None:
            return None

        strokes: list[ChanStrokeResponse] = []
        segments: list[ChanStrokeResponse] = []
        centers: list[ChanCenterResponse] = []
        signals: list[ChanSignalResponse] = []
        channels: list[ChanChannelResponse] = []
        for level in levels:
            run = runs[level]
            strokes.extend(
                [
                    _stroke_row_to_response(row, level)
                    for row in await _fetch_stroke_like(
                        conn,
                        "chan_strokes",
                        run["id"],
                        mode_codes,
                        run["first_ts"],
                        run["last_ts"],
                    )
                ]
            )
            segments.extend(
                [
                    _stroke_row_to_response(row, level)
                    for row in await _fetch_stroke_like(
                        conn,
                        "chan_segments",
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
                        run["id"],
                        mode_codes,
                        run["first_ts"],
                        run["last_ts"],
                    )
                ]
            )
            signals.extend(
                [
                    _signal_row_to_response(row, level)
                    for row in await _fetch_signals(
                        conn,
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
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="database:chan-precomputed",
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
) -> dict[str, dict[str, Any]] | None:
    requested_codes = [TIMEFRAME_TO_DB[level] for level in levels]
    published_rows = await conn.fetch(
        """
        select
            chan_level,
            mode,
            run_id,
            snapshot_version,
            base_from_bar_end,
            base_to_bar_end,
            published_at,
            updated_at
        from scheme2_chan_published_heads
        where symbol_id = $1
          and chan_level = any($2::smallint[])
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
        first_ts,
        last_ts,
    )
    published = _group_published_runs(
        published_rows,
        requested_codes=requested_codes,
        first_ts=first_ts,
        last_ts=last_ts,
    )
    if published is not None:
        return published

    candidate_rows = await conn.fetch(
        """
        select
            id,
            chan_level,
            snapshot_version,
            computed_at,
            bar_from,
            bar_until
        from chan_runs
        where symbol_id = $1
          and chan_level = any($2::smallint[])
          and status = 'success'
          and bar_from is not null
          and bar_until is not null
          and bar_from <= $3
          and bar_until >= $4
        order by computed_at desc nulls last, id desc
        """,
        symbol_id,
        requested_codes,
        first_ts,
        last_ts,
    )

    requested_set = set(requested_codes)
    grouped: dict[str, dict[int, Any]] = {}
    ordered_versions: list[str] = []
    for row in candidate_rows:
        snapshot_version = str(row.get("snapshot_version") or "").strip()
        if not snapshot_version:
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
                "id": group[code]["id"],
                "first_ts": first_ts,
                "last_ts": last_ts,
                "snapshot_version": snapshot_version,
            }
            for code in requested_codes
        }

    runs: dict[str, dict[str, Any]] = {}
    for level in levels:
        run = await conn.fetchrow(
            """
            select
                id,
                bar_from,
                bar_until,
                snapshot_version
            from chan_runs
            where symbol_id = $1
              and chan_level = $2
              and status = 'success'
              and bar_from is not null
              and bar_until is not null
              and bar_from <= $3
              and bar_until >= $4
            order by computed_at desc nulls last, id desc
            limit 1
            """,
            symbol_id,
            TIMEFRAME_TO_DB[level],
            first_ts,
            last_ts,
        )
        if run is None:
            return None
        runs[level] = {
            "id": run["id"],
            "first_ts": first_ts,
            "last_ts": last_ts,
            "snapshot_version": str(run.get("snapshot_version") or "").strip(),
        }
    return runs


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
    run_id: int,
    mode_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
):
    return await conn.fetch(
        """
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
        from chan_centers
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
    run_id: int,
    mode_codes: list[int],
    first_ts: datetime,
    last_ts: datetime,
):
    return await conn.fetch(
        """
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
        from chan_signals
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


def _stroke_row_to_response(row, level: str) -> ChanStrokeResponse:
    begin_base_dt = row.get("begin_base_ts") or row["start_ts"]
    end_base_dt = row.get("end_base_ts") or row["end_ts"]
    begin_base_ts = int(begin_base_dt.timestamp())
    end_base_ts = int(end_base_dt.timestamp())
    return ChanStrokeResponse(
        id=_read_extra_id(row, f"{level}:stroke:{row['id']}"),
        level=level,
        mode=DB_TO_MODE.get(row["mode"], "confirmed"),
        start=ChanPointResponse(
            time=begin_base_ts,
            price=row["start_price_x1000"] / 1000,
            base_ts=begin_base_ts,
            base_seq=row.get("begin_base_seq"),
        ),
        end=ChanPointResponse(
            time=end_base_ts,
            price=row["end_price_x1000"] / 1000,
            base_ts=end_base_ts,
            base_seq=row.get("end_base_seq"),
        ),
        begin_base_ts=begin_base_ts,
        end_base_ts=end_base_ts,
        begin_base_seq=row.get("begin_base_seq"),
        end_base_seq=row.get("end_base_seq"),
        direction=DB_TO_DIRECTION.get(row["direction"], "up"),
        confirmed=row["is_confirmed"],
    )


def _center_row_to_response(row, level: str) -> ChanCenterResponse:
    begin_base_dt = row.get("begin_base_ts") or row["start_ts"]
    end_base_dt = row.get("end_base_ts") or row["end_ts"]
    begin_base_ts = int(begin_base_dt.timestamp())
    end_base_ts = int(end_base_dt.timestamp())
    return ChanCenterResponse(
        id=_read_extra_id(row, f"{level}:center:{row['id']}"),
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


def _signal_row_to_response(row, level: str) -> ChanSignalResponse:
    base_dt = row.get("base_ts") or row["ts"]
    base_ts = int(base_dt.timestamp())
    extra = _read_extra(row)
    return ChanSignalResponse(
        id=_read_extra_id(row, f"{level}:signal:{row['id']}"),
        level=level,
        mode=DB_TO_MODE.get(row["mode"], "confirmed"),
        time=base_ts,
        base_ts=base_ts,
        base_seq=row.get("base_seq"),
        price=row["price_x1000"] / 1000,
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
