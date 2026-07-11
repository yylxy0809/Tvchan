from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import ChanOverlayResponse
from app.repositories.chan_postgres import (
    OverlayTooLargeError,
    get_windowed_module_c_overlay_db,
)
from trading_protocol import normalize_timeframe

DEFAULT_LEVELS = ("5f", "30f", "1d", "1w", "1m")
DEFAULT_MODES = ("confirmed", "predictive")
DISPLAY_LEVELS = {
    "5f": ("5f", "30f", "1d"),
    "15f": ("5f", "30f", "1d"),
    "30f": ("30f", "1d"),
    "1h": ("30f", "1d"),
    "1d": ("1d", "1w"),
    "1w": ("1w", "1m"),
    "1m": ("1m",),
}
MAX_OVERLAY_WINDOW_SECONDS = 366 * 24 * 60 * 60
MAX_OVERLAY_WINDOW_BARS = 360
OVERLAY_WINDOW_GUARD_BARS = 12
TIMEFRAME_SECONDS = {
    "5f": 5 * 60,
    "15f": 15 * 60,
    "30f": 30 * 60,
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
    # Calendar months vary; 31 days keeps the cap conservative.
    "1m": 31 * 24 * 60 * 60,
}
# Stored daily and monthly bars follow trading calendars, not evenly spaced
# wall-clock intervals. Keep the viewport cap bounded by bar count while
# allowing ordinary cold 300-bar chart windows across holidays and suspensions.
CALENDAR_WINDOW_MULTIPLIERS = {
    "1d": 1.8,
    "1w": 1.3,
    "1m": 1.5,
}

router = APIRouter(prefix="/chan", tags=["chan"], dependencies=[Depends(require_token)])


@router.get("/overlay", response_model=ChanOverlayResponse)
async def get_chan_overlay(
    request: Request,
    symbol: str = Query(..., min_length=6, max_length=16),
    timeframe: str = Query(default="5f"),
    levels: str = Query(default=""),
    modes: str = Query(default="confirmed,predictive"),
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=300, ge=1, le=5000),
    settings: Settings = Depends(get_settings),
) -> ChanOverlayResponse:
    return await build_chan_overlay(
        request=request,
        symbol=symbol,
        timeframe=timeframe,
        levels=levels,
        modes=modes,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        settings=settings,
    )


async def build_chan_overlay(
    request: Request,
    symbol: str,
    timeframe: str,
    levels: str,
    modes: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    settings: Settings,
    authoritative_window: bool = False,
    legacy_bundle: bool = False,
) -> ChanOverlayResponse:
    try:
        chart_timeframe = normalize_timeframe(timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if legacy_bundle:
        # Compatibility bundles retain their caller-selected legacy levels.
        chan_levels = _parse_levels(levels) if levels.strip() else list(DEFAULT_LEVELS)
        requested_levels = chan_levels
    else:
        chan_levels = _display_levels_for_chart(chart_timeframe)
        requested_levels = _parse_levels(levels) if levels.strip() else chan_levels
    if authoritative_window and requested_levels != chan_levels:
        raise HTTPException(
            status_code=400,
            detail=f"Levels for {chart_timeframe} must be: {', '.join(chan_levels)}",
        )
    chan_modes = _parse_modes(modes)
    if authoritative_window:
        first_ts, last_ts = _validate_window(from_ts, to_ts, chart_timeframe, limit)
        pool = getattr(request.app.state, "db_pool", None)
        if pool is None:
            if settings.use_seed_data:
                return _empty_published_overlay(
                    symbol=symbol.upper(), chart_timeframe=chart_timeframe, levels=chan_levels,
                    modes=chan_modes, requested_bar_count=limit,
                    bars_by_level={level: 0 for level in chan_levels}, level_bars={},
                )
            raise HTTPException(status_code=503, detail="Database pool is not ready")
        try:
            precomputed = await get_windowed_module_c_overlay_db(
                pool,
                symbol=symbol,
                chart_timeframe=chart_timeframe,
                levels=chan_levels,
                modes=chan_modes,
                first_ts=first_ts,
                last_ts=last_ts,
                requested_bar_count=limit,
            )
        except OverlayTooLargeError as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        if precomputed is not None:
            return precomputed
        return _empty_published_overlay(
            symbol=symbol.upper(), chart_timeframe=chart_timeframe, levels=chan_levels,
            modes=chan_modes, requested_bar_count=limit,
            bars_by_level={level: 0 for level in chan_levels}, level_bars={},
        )

    return _empty_published_overlay(
        symbol=symbol.upper(),
        chart_timeframe=chart_timeframe,
        levels=chan_levels,
        modes=chan_modes,
        requested_bar_count=limit,
        bars_by_level={level: 0 for level in chan_levels},
        level_bars={},
    )


def _empty_published_overlay(
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, int],
    level_bars: dict[str, list[dict]],
) -> ChanOverlayResponse:
    window_bars = level_bars.get(chart_timeframe) or level_bars.get("5f") or []
    first = window_bars[0].get("time") if window_bars else ""
    last = window_bars[-1].get("time") if window_bars else ""
    return ChanOverlayResponse(
        symbol=symbol,
        chart_timeframe=chart_timeframe,
        levels=levels,
        modes=modes,
        snapshot_version=f"{symbol}:published-empty:{chart_timeframe}:{first}:{last}:{requested_bar_count}",
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="database:chan-published-empty",
        requested_bar_count=requested_bar_count,
        bars_by_level=bars_by_level,
        strokes=[],
        segments=[],
        centers=[],
        signals=[],
        channels=[],
    )


def _unique_levels(levels: list[str]) -> list[str]:
    seen = set()
    result = []
    for level in levels:
        if level in seen:
            continue
        seen.add(level)
        result.append(level)
    return result


def _parse_levels(value: str) -> list[str]:
    requested = [item.strip() for item in value.split(",") if item.strip()]
    levels = requested or list(DEFAULT_LEVELS)
    normalized: list[str] = []
    for item in levels:
        try:
            normalized.append(normalize_timeframe(item))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    invalid = [item for item in normalized if item not in DEFAULT_LEVELS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported chan levels: {', '.join(invalid)}",
        )
    return normalized


def _display_levels_for_chart(chart_timeframe: str) -> list[str]:
    try:
        return list(DISPLAY_LEVELS[chart_timeframe])
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"Unsupported chart timeframe: {chart_timeframe}") from exc


def _validate_window(
    from_ts: datetime | None,
    to_ts: datetime | None,
    chart_timeframe: str,
    limit: int,
) -> tuple[datetime, datetime]:
    if from_ts is None or to_ts is None:
        raise HTTPException(status_code=422, detail="Both from and to are required for a chart overlay")
    if from_ts.tzinfo is None or from_ts.utcoffset() is None:
        raise HTTPException(status_code=422, detail="from must include a UTC offset")
    if to_ts.tzinfo is None or to_ts.utcoffset() is None:
        raise HTTPException(status_code=422, detail="to must include a UTC offset")
    from_ts = from_ts.astimezone(UTC)
    to_ts = to_ts.astimezone(UTC)
    if from_ts > to_ts:
        raise HTTPException(status_code=400, detail="from must be less than or equal to to")
    # A bounded monthly 300-bar viewport naturally spans about 25 years. Cap
    # the request by a bar-count horizon instead of a fixed wall-clock ceiling,
    # retaining a small trading-calendar guard while rejecting arbitrary spans.
    bounded_bars = min(limit, MAX_OVERLAY_WINDOW_BARS) + OVERLAY_WINDOW_GUARD_BARS
    requested_window_seconds = int(
        TIMEFRAME_SECONDS[chart_timeframe]
        * bounded_bars
        * CALENDAR_WINDOW_MULTIPLIERS.get(chart_timeframe, 1.0)
    )
    maximum_window_seconds = max(MAX_OVERLAY_WINDOW_SECONDS, requested_window_seconds)
    if (to_ts - from_ts).total_seconds() > maximum_window_seconds:
        raise HTTPException(
            status_code=413,
            detail={
                "code": "overlay_window_too_large",
                "message": "Overlay window exceeds the bounded request allowance",
                "maximum_seconds": maximum_window_seconds,
            },
        )
    return from_ts, to_ts


def _parse_modes(value: str) -> list[str]:
    requested = [item.strip().lower() for item in value.split(",") if item.strip()]
    modes = requested or list(DEFAULT_MODES)
    invalid = [item for item in modes if item not in DEFAULT_MODES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported chan modes: {', '.join(invalid)}",
        )
    if len(set(modes)) != len(modes):
        raise HTTPException(status_code=400, detail="Duplicate chan modes are not allowed")
    return modes
