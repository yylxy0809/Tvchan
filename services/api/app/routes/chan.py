from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from time import monotonic

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import ChanOverlayResponse
from app.repositories.bars import generate_seed_bars, resolve_symbol
from app.repositories.chan_postgres import get_precomputed_chan_overlay_db
from app.repositories.postgres import get_bars_db, resolve_symbol_db
from app.services.chan_client import ChanServiceError, analyze_with_chan_service
from trading_protocol import normalize_timeframe

DEFAULT_LEVELS = ("5f", "30f", "1d")
DEFAULT_MODES = ("confirmed", "predictive")
_ANALYSIS_CACHE_MAX_ITEMS = 32
_ANALYSIS_FAST_CACHE_TTL_SECONDS = 300
_analysis_overlay_cache: OrderedDict[tuple, ChanOverlayResponse] = OrderedDict()
_analysis_overlay_fast_cache: OrderedDict[tuple, tuple[float, ChanOverlayResponse]] = OrderedDict()

router = APIRouter(prefix="/chan", tags=["chan"], dependencies=[Depends(require_token)])


@router.get("/overlay", response_model=ChanOverlayResponse)
async def get_chan_overlay(
    request: Request,
    symbol: str = Query(..., min_length=6, max_length=16),
    timeframe: str = Query(default="5f"),
    levels: str = Query(default="5f,30f,1d"),
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
) -> ChanOverlayResponse:
    try:
        chart_timeframe = normalize_timeframe(timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    chan_levels = _parse_levels(levels)
    chan_modes = _parse_modes(modes)

    symbol_name, level_bars = await _load_level_bars(
        request=request,
        symbol=symbol,
        chart_timeframe=chart_timeframe,
        levels=[],
        start=from_ts,
        end=to_ts,
        limit=limit,
        settings=settings,
    )
    bars_by_level_count = _bar_counts(level_bars, chan_levels)
    if not settings.use_seed_data and settings.use_precomputed_chan:
        pool = getattr(request.app.state, "db_pool", None)
        if pool is not None:
            precomputed = await get_precomputed_chan_overlay_db(
                pool,
                symbol=symbol_name,
                chart_timeframe=chart_timeframe,
                levels=chan_levels,
                modes=chan_modes,
                requested_bar_count=limit,
                bars_by_level=level_bars,
            )
            if precomputed is not None:
                return precomputed

    if not all(level in level_bars for level in chan_levels):
        symbol_name, level_bars = await _load_level_bars(
            request=request,
            symbol=symbol_name,
            chart_timeframe=chart_timeframe,
            levels=chan_levels,
            start=from_ts,
            end=to_ts,
            limit=limit,
            settings=settings,
        )
        bars_by_level_count = _bar_counts(level_bars, chan_levels)

    if settings.chan_service_url:
        try:
            if not settings.use_seed_data:
                cached = _load_fast_analysis_overlay_cache(
                    symbol=symbol_name,
                    service_url=settings.chan_service_url,
                    chart_timeframe=chart_timeframe,
                    levels=chan_levels,
                    modes=chan_modes,
                    requested_bar_count=limit,
                    bars_by_level=level_bars,
                )
                if cached is not None:
                    return cached
            analysis_bars = await _load_chan_analysis_bars(
                request=request,
                settings=settings,
                symbol=symbol_name,
                visible_bars=level_bars,
                end=to_ts,
            )
            cached = _load_analysis_overlay_cache(
                symbol=symbol_name,
                chart_timeframe=chart_timeframe,
                levels=chan_levels,
                modes=chan_modes,
                requested_bar_count=limit,
                bars_by_level=level_bars,
                analysis_bars=analysis_bars,
            )
            if cached is not None:
                return cached
            analyzed = await analyze_with_chan_service(
                base_url=settings.chan_service_url,
                symbol=symbol_name,
                chart_timeframe=chart_timeframe,
                levels=chan_levels,
                modes=chan_modes,
                requested_bar_count=limit,
                bars_by_level=level_bars,
                analysis_bars=analysis_bars,
            )
            _publish_analysis_overlay_cache(
                symbol=symbol_name,
                levels=chan_levels,
                modes=chan_modes,
                analysis_bars=analysis_bars,
                overlay=analyzed,
            )
            if not settings.use_seed_data:
                _publish_fast_analysis_overlay_cache(
                    symbol=symbol_name,
                    service_url=settings.chan_service_url,
                    levels=chan_levels,
                    modes=chan_modes,
                    overlay=analyzed,
                )
            return analyzed
        except ChanServiceError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    raise HTTPException(
        status_code=503,
        detail="Chan overlay requires precomputed data or CHAN_SERVICE_URL module-b service",
    )


async def _load_level_bars(
    request: Request,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    start: datetime | None,
    end: datetime | None,
    limit: int,
    settings: Settings,
) -> tuple[str, dict[str, list[dict]]]:
    if not settings.use_seed_data:
        pool = getattr(request.app.state, "db_pool", None)
        if pool is None:
            raise HTTPException(status_code=503, detail="Database pool is not ready")
        symbol_row = await resolve_symbol_db(pool, symbol)
        if symbol_row is None:
            raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")
        load_levels = _unique_levels([*levels, chart_timeframe, "5f"])
        bars_by_level = {
            level: await get_bars_db(pool, symbol_row["symbol"], level, start, end, limit)
            for level in load_levels
        }
        return symbol_row["symbol"], bars_by_level

    symbol_info = resolve_symbol(symbol)
    if symbol_info is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")
    load_levels = _unique_levels([*levels, chart_timeframe, "5f"])
    bars_by_level = {
        level: [
            bar.as_api_dict()
            for bar in generate_seed_bars(
                symbol_info.symbol,
                level,
                start=start,
                end=end,
                limit=limit,
            )
        ]
        for level in load_levels
    }
    return symbol_info.symbol, bars_by_level


def _bar_counts(bars_by_level: dict[str, list[dict]], levels: list[str]) -> dict[str, int]:
    return {level: len(bars_by_level.get(level, [])) for level in levels}


def _load_analysis_overlay_cache(
    *,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
    analysis_bars: list[dict],
) -> ChanOverlayResponse | None:
    key = _analysis_overlay_cache_key(symbol, levels, modes, analysis_bars)
    cached = _analysis_overlay_cache.get(key)
    if cached is None:
        return None
    _analysis_overlay_cache.move_to_end(key)
    return cached.model_copy(
        update={
            "chart_timeframe": chart_timeframe,
            "requested_bar_count": requested_bar_count,
            "bars_by_level": _bar_counts(bars_by_level, levels),
        },
        deep=True,
    )


def _load_fast_analysis_overlay_cache(
    *,
    symbol: str,
    service_url: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
) -> ChanOverlayResponse | None:
    key = _fast_analysis_overlay_cache_key(symbol, service_url, levels, modes)
    cached = _analysis_overlay_fast_cache.get(key)
    if cached is None:
        return None
    created_at, overlay = cached
    if monotonic() - created_at > _ANALYSIS_FAST_CACHE_TTL_SECONDS:
        _analysis_overlay_fast_cache.pop(key, None)
        return None
    _analysis_overlay_fast_cache.move_to_end(key)
    return overlay.model_copy(
        update={
            "chart_timeframe": chart_timeframe,
            "requested_bar_count": requested_bar_count,
            "bars_by_level": _bar_counts(bars_by_level, levels),
        },
        deep=True,
    )


def _publish_fast_analysis_overlay_cache(
    *,
    symbol: str,
    service_url: str,
    levels: list[str],
    modes: list[str],
    overlay: ChanOverlayResponse,
) -> None:
    key = _fast_analysis_overlay_cache_key(symbol, service_url, levels, modes)
    _analysis_overlay_fast_cache[key] = (monotonic(), overlay.model_copy(deep=True))
    _analysis_overlay_fast_cache.move_to_end(key)
    while len(_analysis_overlay_fast_cache) > _ANALYSIS_CACHE_MAX_ITEMS:
        _analysis_overlay_fast_cache.popitem(last=False)


def _fast_analysis_overlay_cache_key(
    symbol: str,
    service_url: str,
    levels: list[str],
    modes: list[str],
) -> tuple:
    return (
        symbol.upper(),
        service_url.rstrip("/"),
        id(analyze_with_chan_service),
        tuple(levels),
        tuple(modes),
    )


def _publish_analysis_overlay_cache(
    *,
    symbol: str,
    levels: list[str],
    modes: list[str],
    analysis_bars: list[dict],
    overlay: ChanOverlayResponse,
) -> None:
    key = _analysis_overlay_cache_key(symbol, levels, modes, analysis_bars)
    _analysis_overlay_cache[key] = overlay.model_copy(deep=True)
    _analysis_overlay_cache.move_to_end(key)
    while len(_analysis_overlay_cache) > _ANALYSIS_CACHE_MAX_ITEMS:
        _analysis_overlay_cache.popitem(last=False)


def _analysis_overlay_cache_key(
    symbol: str,
    levels: list[str],
    modes: list[str],
    analysis_bars: list[dict],
) -> tuple:
    if not analysis_bars:
        return (symbol.upper(), tuple(levels), tuple(modes), "empty")
    first = analysis_bars[0]
    last = analysis_bars[-1]
    return (
        symbol.upper(),
        tuple(levels),
        tuple(modes),
        len(analysis_bars),
        first.get("time"),
        last.get("time"),
        last.get("revision"),
        last.get("open"),
        last.get("high"),
        last.get("low"),
        last.get("close"),
        last.get("volume"),
    )


async def _load_chan_analysis_bars(
    *,
    request: Request,
    settings: Settings,
    symbol: str,
    visible_bars: dict[str, list[dict]],
    end: datetime | None,
) -> list[dict]:
    if settings.use_seed_data:
        return visible_bars.get("5f", [])
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        return visible_bars.get("5f", [])
    # Use the full 5f history up to the requested end time so Chan recursion
    # is computed on the complete path, while the chart still renders only the
    # requested window.
    return await get_bars_db(
        pool,
        symbol,
        "5f",
        None,
        end,
        1_000_000,
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


def _parse_modes(value: str) -> list[str]:
    requested = [item.strip().lower() for item in value.split(",") if item.strip()]
    modes = requested or list(DEFAULT_MODES)
    invalid = [item for item in modes if item not in DEFAULT_MODES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported chan modes: {', '.join(invalid)}",
        )
    return modes
