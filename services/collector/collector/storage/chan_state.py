from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any


DEFINITION_VERSION = "chan-state-v1"
BASE_TIMEFRAME_CODE = 5
MODE_TO_CODE = {
    "confirmed": 1,
    "predictive": 2,
}


@dataclass(frozen=True)
class StrokeLike:
    seq: int
    direction: int | None
    confirmed: bool | None
    begin_base_ts: datetime | None
    end_base_ts: datetime | None


@dataclass(frozen=True)
class Center:
    seq: int
    low_x1000: int
    high_x1000: int
    confirmed: bool | None
    begin_base_ts: datetime | None
    end_base_ts: datetime | None


@dataclass(frozen=True)
class Signal:
    signal_type: str | None
    side: str | None
    bsp_type: str | None
    base_ts: datetime | None


def derive_level_state(
    *,
    strokes: list[StrokeLike],
    segments: list[StrokeLike],
    centers: list[Center],
    signals: list[Signal],
    source_bar_until: datetime | None,
) -> dict[str, Any]:
    latest_stroke = _latest_stroke_like(strokes)
    latest_segment = _latest_stroke_like(segments)
    latest_signal = signals[-1] if signals else None
    current_range = _structure_range(latest_segment or latest_stroke)
    scoped_centers = _centers_in_range(centers, current_range) if current_range else list(centers)
    active_center = scoped_centers[-1] if scoped_centers else None
    structure_state, structure_direction = _structure_status(
        scoped_centers=scoped_centers,
        fallback_direction=_direction(latest_segment or latest_stroke),
    )
    asof = _max_dt(
        [
            source_bar_until,
            _end(latest_stroke),
            _end(latest_segment),
            active_center.end_base_ts if active_center else None,
            latest_signal.base_ts if latest_signal else None,
        ]
    )
    return {
        "asof_base_ts": asof,
        "latest_stroke_seq": latest_stroke.seq if latest_stroke else None,
        "latest_stroke_direction": _direction(latest_stroke),
        "latest_stroke_confirmed": latest_stroke.confirmed if latest_stroke else None,
        "latest_stroke_begin_base_ts": latest_stroke.begin_base_ts if latest_stroke else None,
        "latest_stroke_end_base_ts": latest_stroke.end_base_ts if latest_stroke else None,
        "latest_segment_seq": latest_segment.seq if latest_segment else None,
        "latest_segment_direction": _direction(latest_segment),
        "latest_segment_confirmed": latest_segment.confirmed if latest_segment else None,
        "latest_segment_begin_base_ts": latest_segment.begin_base_ts if latest_segment else None,
        "latest_segment_end_base_ts": latest_segment.end_base_ts if latest_segment else None,
        "has_active_center": active_center is not None,
        "active_center_seq": active_center.seq if active_center else None,
        "center_low_x1000": active_center.low_x1000 if active_center else None,
        "center_high_x1000": active_center.high_x1000 if active_center else None,
        "center_count": len(scoped_centers),
        "structure_state": structure_state,
        "structure_direction": structure_direction,
        "last_signal_type": latest_signal.signal_type if latest_signal else None,
        "last_signal_side": latest_signal.side if latest_signal else None,
        "last_signal_bsp_type": latest_signal.bsp_type if latest_signal else None,
        "last_signal_base_ts": latest_signal.base_ts if latest_signal else None,
        "is_complete": bool((latest_segment or latest_stroke) and (latest_segment or latest_stroke).confirmed),
        "warnings": {},
        "definition_version": DEFINITION_VERSION,
    }


async def refresh_symbol_chan_states(
    conn,
    *,
    symbol_id: int,
    snapshot_version: str | None = None,
) -> None:
    heads = await _fetch_heads(conn, symbol_id=symbol_id, snapshot_version=snapshot_version)
    if not heads:
        return
    for head in heads:
        mode = str(head["mode"])
        run_id = int(head["run_id"])
        detail_mode = MODE_TO_CODE.get(mode, MODE_TO_CODE["confirmed"])
        strokes = await _fetch_stroke_like(conn, "chan_strokes", run_id, detail_mode)
        segments = await _fetch_stroke_like(conn, "chan_segments", run_id, detail_mode)
        if not strokes and mode == "predictive":
            detail_mode = MODE_TO_CODE["confirmed"]
            strokes = await _fetch_stroke_like(conn, "chan_strokes", run_id, detail_mode)
            segments = await _fetch_stroke_like(conn, "chan_segments", run_id, detail_mode)
        centers = await _fetch_centers(conn, run_id, detail_mode)
        signals = await _fetch_signals(conn, run_id, detail_mode)
        state = derive_level_state(
            strokes=strokes,
            segments=segments,
            centers=centers,
            signals=signals,
            source_bar_until=_row_get(head, "base_to_bar_end"),
        )
        await _upsert_level_state(conn, head=head, state=state)
    await _refresh_cross_level_states(conn, symbol_id=symbol_id, snapshot_version=snapshot_version)


async def _fetch_heads(conn, *, symbol_id: int, snapshot_version: str | None):
    if snapshot_version:
        return await conn.fetch(
            """
            select
                symbol_id,
                chan_level,
                mode,
                base_timeframe,
                base_to_bar_end,
                bar_count,
                snapshot_version,
                run_id
            from scheme2_chan_published_heads
            where symbol_id = $1
              and snapshot_version = $2
              and status = 'published'
              and run_id is not null
            order by chan_level, mode
            """,
            symbol_id,
            snapshot_version,
        )
    return await conn.fetch(
        """
        select
            symbol_id,
            chan_level,
            mode,
            base_timeframe,
            base_to_bar_end,
            bar_count,
            snapshot_version,
            run_id
        from scheme2_chan_published_heads
        where symbol_id = $1
          and status = 'published'
          and run_id is not null
        order by chan_level, mode
        """,
        symbol_id,
    )


async def _fetch_stroke_like(conn, table: str, run_id: int, mode: int) -> list[StrokeLike]:
    rows = await conn.fetch(
        f"""
        select
            seq,
            direction,
            is_confirmed,
            coalesce(begin_base_ts, start_ts) as begin_base_ts,
            coalesce(end_base_ts, end_ts) as end_base_ts
        from {table}
        where run_id = $1 and mode = $2
        order by seq
        """,
        run_id,
        mode,
    )
    return [
        StrokeLike(
            seq=int(row["seq"]),
            direction=row["direction"],
            confirmed=row["is_confirmed"],
            begin_base_ts=row["begin_base_ts"],
            end_base_ts=row["end_base_ts"],
        )
        for row in rows
    ]


async def _fetch_centers(conn, run_id: int, mode: int) -> list[Center]:
    rows = await conn.fetch(
        """
        select
            seq,
            low_x1000,
            high_x1000,
            is_confirmed,
            coalesce(begin_base_ts, start_ts) as begin_base_ts,
            coalesce(end_base_ts, end_ts) as end_base_ts
        from chan_centers
        where run_id = $1 and mode = $2
        order by seq
        """,
        run_id,
        mode,
    )
    return [
        Center(
            seq=int(row["seq"]),
            low_x1000=int(row["low_x1000"]),
            high_x1000=int(row["high_x1000"]),
            confirmed=row["is_confirmed"],
            begin_base_ts=row["begin_base_ts"],
            end_base_ts=row["end_base_ts"],
        )
        for row in rows
    ]


async def _fetch_signals(conn, run_id: int, mode: int) -> list[Signal]:
    rows = await conn.fetch(
        """
        select
            signal_type,
            coalesce(base_ts, ts) as base_ts,
            extra
        from chan_signals
        where run_id = $1 and mode = $2
        order by coalesce(base_ts, ts), id
        """,
        run_id,
        mode,
    )
    return [
        Signal(
            signal_type=row["signal_type"],
            side=_read_extra(row["extra"]).get("side"),
            bsp_type=_read_extra(row["extra"]).get("bsp_type"),
            base_ts=row["base_ts"],
        )
        for row in rows
    ]


async def _upsert_level_state(conn, *, head, state: dict[str, Any]) -> None:
    await conn.execute(
        """
        insert into chan_level_state_snapshots (
            symbol_id,
            chan_level,
            mode,
            base_timeframe,
            snapshot_version,
            run_id,
            asof_base_ts,
            source_bar_until,
            bar_count,
            latest_stroke_seq,
            latest_stroke_direction,
            latest_stroke_confirmed,
            latest_stroke_begin_base_ts,
            latest_stroke_end_base_ts,
            latest_segment_seq,
            latest_segment_direction,
            latest_segment_confirmed,
            latest_segment_begin_base_ts,
            latest_segment_end_base_ts,
            has_active_center,
            active_center_seq,
            center_low_x1000,
            center_high_x1000,
            center_count,
            structure_state,
            structure_direction,
            last_signal_type,
            last_signal_side,
            last_signal_bsp_type,
            last_signal_base_ts,
            is_complete,
            warnings,
            definition_version,
            computed_at
        )
        values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23, $24, $25, $26, $27, $28, $29,
            $30, $31, $32::jsonb, $33, now()
        )
        on conflict (symbol_id, chan_level, mode, base_timeframe)
        do update
        set snapshot_version = excluded.snapshot_version,
            run_id = excluded.run_id,
            asof_base_ts = excluded.asof_base_ts,
            source_bar_until = excluded.source_bar_until,
            bar_count = excluded.bar_count,
            latest_stroke_seq = excluded.latest_stroke_seq,
            latest_stroke_direction = excluded.latest_stroke_direction,
            latest_stroke_confirmed = excluded.latest_stroke_confirmed,
            latest_stroke_begin_base_ts = excluded.latest_stroke_begin_base_ts,
            latest_stroke_end_base_ts = excluded.latest_stroke_end_base_ts,
            latest_segment_seq = excluded.latest_segment_seq,
            latest_segment_direction = excluded.latest_segment_direction,
            latest_segment_confirmed = excluded.latest_segment_confirmed,
            latest_segment_begin_base_ts = excluded.latest_segment_begin_base_ts,
            latest_segment_end_base_ts = excluded.latest_segment_end_base_ts,
            has_active_center = excluded.has_active_center,
            active_center_seq = excluded.active_center_seq,
            center_low_x1000 = excluded.center_low_x1000,
            center_high_x1000 = excluded.center_high_x1000,
            center_count = excluded.center_count,
            structure_state = excluded.structure_state,
            structure_direction = excluded.structure_direction,
            last_signal_type = excluded.last_signal_type,
            last_signal_side = excluded.last_signal_side,
            last_signal_bsp_type = excluded.last_signal_bsp_type,
            last_signal_base_ts = excluded.last_signal_base_ts,
            is_complete = excluded.is_complete,
            warnings = excluded.warnings,
            definition_version = excluded.definition_version,
            computed_at = now()
        """,
        head["symbol_id"],
        int(head["chan_level"]),
        str(head["mode"]),
        int(head["base_timeframe"] or BASE_TIMEFRAME_CODE),
        str(head["snapshot_version"]),
        int(head["run_id"]),
        state["asof_base_ts"],
        _row_get(head, "base_to_bar_end"),
        _row_get(head, "bar_count"),
        state["latest_stroke_seq"],
        state["latest_stroke_direction"],
        state["latest_stroke_confirmed"],
        state["latest_stroke_begin_base_ts"],
        state["latest_stroke_end_base_ts"],
        state["latest_segment_seq"],
        state["latest_segment_direction"],
        state["latest_segment_confirmed"],
        state["latest_segment_begin_base_ts"],
        state["latest_segment_end_base_ts"],
        state["has_active_center"],
        state["active_center_seq"],
        state["center_low_x1000"],
        state["center_high_x1000"],
        state["center_count"],
        state["structure_state"],
        state["structure_direction"],
        state["last_signal_type"],
        state["last_signal_side"],
        state["last_signal_bsp_type"],
        state["last_signal_base_ts"],
        state["is_complete"],
        json.dumps(state["warnings"], ensure_ascii=False),
        state["definition_version"],
    )


async def _refresh_cross_level_states(
    conn,
    *,
    symbol_id: int,
    snapshot_version: str | None,
) -> None:
    states = await conn.fetch(
        """
        select *
        from chan_level_state_snapshots
        where symbol_id = $1
          and ($2::varchar is null or snapshot_version = $2)
          and definition_version = $3
        order by chan_level
        """,
        symbol_id,
        snapshot_version,
        DEFINITION_VERSION,
    )
    if not states:
        return
    await conn.execute(
        """
        delete from chan_cross_level_states
        where symbol_id = $1
          and ($2::varchar is null or snapshot_version = $2)
          and definition_version = $3
        """,
        symbol_id,
        snapshot_version,
        DEFINITION_VERSION,
    )
    by_level = {int(row["chan_level"]): row for row in states}
    for parent_level, child_level in ((10080, 1440), (1440, 30), (30, 5)):
        parent = by_level.get(parent_level)
        child = by_level.get(child_level)
        if parent is None or child is None:
            continue
        await _insert_cross_level_state(
            conn,
            parent=parent,
            child=child,
            parent_structure_type="stroke",
        )
        if parent["latest_segment_seq"] is not None:
            await _insert_cross_level_state(
                conn,
                parent=parent,
                child=child,
                parent_structure_type="segment",
            )


async def _insert_cross_level_state(conn, *, parent, child, parent_structure_type: str) -> None:
    parent_seq_key = "latest_stroke_seq" if parent_structure_type == "stroke" else "latest_segment_seq"
    parent_direction_key = (
        "latest_stroke_direction" if parent_structure_type == "stroke" else "latest_segment_direction"
    )
    parent_begin_key = (
        "latest_stroke_begin_base_ts"
        if parent_structure_type == "stroke"
        else "latest_segment_begin_base_ts"
    )
    parent_end_key = (
        "latest_stroke_end_base_ts"
        if parent_structure_type == "stroke"
        else "latest_segment_end_base_ts"
    )
    parent_seq = _row_get(parent, parent_seq_key)
    parent_begin = _row_get(parent, parent_begin_key)
    parent_end = _row_get(parent, parent_end_key)
    if parent_seq is None or parent_begin is None or parent_end is None:
        return
    child_run_id = int(child["run_id"])
    child_counts = await _child_counts(
        conn,
        run_id=child_run_id,
        mode=MODE_TO_CODE.get(str(child["mode"]), MODE_TO_CODE["confirmed"]),
        begin_ts=parent_begin,
        end_ts=parent_end,
    )
    await conn.execute(
        """
        insert into chan_cross_level_states (
            symbol_id,
            snapshot_version,
            mode,
            parent_level,
            parent_structure_type,
            parent_seq,
            parent_run_id,
            parent_direction,
            parent_begin_base_ts,
            parent_end_base_ts,
            child_level,
            child_run_id,
            child_stroke_count,
            child_segment_count,
            child_center_count,
            child_latest_stroke_direction,
            child_latest_segment_direction,
            child_last_signal_type,
            is_current,
            definition_version,
            computed_at
        )
        values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, true, $19, now()
        )
        on conflict (
            symbol_id,
            snapshot_version,
            mode,
            parent_level,
            parent_structure_type,
            parent_seq,
            child_level,
            definition_version
        )
        do update
        set parent_run_id = excluded.parent_run_id,
            parent_direction = excluded.parent_direction,
            parent_begin_base_ts = excluded.parent_begin_base_ts,
            parent_end_base_ts = excluded.parent_end_base_ts,
            child_run_id = excluded.child_run_id,
            child_stroke_count = excluded.child_stroke_count,
            child_segment_count = excluded.child_segment_count,
            child_center_count = excluded.child_center_count,
            child_latest_stroke_direction = excluded.child_latest_stroke_direction,
            child_latest_segment_direction = excluded.child_latest_segment_direction,
            child_last_signal_type = excluded.child_last_signal_type,
            is_current = true,
            computed_at = now()
        """,
        parent["symbol_id"],
        parent["snapshot_version"],
        parent["mode"],
        int(parent["chan_level"]),
        parent_structure_type,
        int(parent_seq),
        int(parent["run_id"]),
        _row_get(parent, parent_direction_key),
        parent_begin,
        parent_end,
        int(child["chan_level"]),
        child_run_id,
        child_counts["strokes"],
        child_counts["segments"],
        child_counts["centers"],
        child["latest_stroke_direction"],
        child["latest_segment_direction"],
        child["last_signal_type"],
        DEFINITION_VERSION,
    )


async def _child_counts(conn, *, run_id: int, mode: int, begin_ts, end_ts) -> dict[str, int]:
    if begin_ts is None or end_ts is None:
        return {"strokes": 0, "segments": 0, "centers": 0}
    left, right = sorted([begin_ts, end_ts])
    rows = await conn.fetchrow(
        """
        select
            (
                select count(*)
                from chan_strokes
                where run_id = $1 and mode = $2
                  and coalesce(begin_base_ts, start_ts) >= $3
                  and coalesce(end_base_ts, end_ts) <= $4
            ) as strokes,
            (
                select count(*)
                from chan_segments
                where run_id = $1 and mode = $2
                  and coalesce(begin_base_ts, start_ts) >= $3
                  and coalesce(end_base_ts, end_ts) <= $4
            ) as segments,
            (
                select count(*)
                from chan_centers
                where run_id = $1 and mode = $2
                  and coalesce(begin_base_ts, start_ts) >= $3
                  and coalesce(end_base_ts, end_ts) <= $4
            ) as centers
        """,
        run_id,
        mode,
        left,
        right,
    )
    return {
        "strokes": int(rows["strokes"] or 0),
        "segments": int(rows["segments"] or 0),
        "centers": int(rows["centers"] or 0),
    }


def _structure_status(
    *,
    scoped_centers: list[Center],
    fallback_direction: int | None,
) -> tuple[str, int | None]:
    if not scoped_centers:
        return "no_center", fallback_direction
    if len(scoped_centers) >= 2:
        previous, current = scoped_centers[-2], scoped_centers[-1]
        if current.low_x1000 > previous.high_x1000:
            return "trend", 1
        if current.high_x1000 < previous.low_x1000:
            return "trend", -1
    return "consolidation", fallback_direction


def _centers_in_range(centers: list[Center], current_range: tuple[datetime, datetime]) -> list[Center]:
    left, right = current_range
    return [
        center
        for center in centers
        if center.begin_base_ts is not None
        and center.end_base_ts is not None
        and min(center.begin_base_ts, center.end_base_ts) >= left
        and max(center.begin_base_ts, center.end_base_ts) <= right
    ]


def _structure_range(item: StrokeLike | None) -> tuple[datetime, datetime] | None:
    if item is None or item.begin_base_ts is None or item.end_base_ts is None:
        return None
    return tuple(sorted([item.begin_base_ts, item.end_base_ts]))


def _latest_stroke_like(items: list[StrokeLike]) -> StrokeLike | None:
    return items[-1] if items else None


def _direction(item: StrokeLike | None) -> int | None:
    return item.direction if item is not None else None


def _end(item: StrokeLike | None) -> datetime | None:
    if item is None:
        return None
    return item.end_base_ts


def _max_dt(values: list[datetime | None]) -> datetime | None:
    items = [value for value in values if value is not None]
    return max(items) if items else None


def _read_extra(extra: Any) -> dict[str, Any]:
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except json.JSONDecodeError:
            return {}
    return extra if isinstance(extra, dict) else {}


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, TypeError):
        return default
