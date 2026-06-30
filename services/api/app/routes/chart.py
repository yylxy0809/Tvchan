from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import (
    BarResponse,
    BarsResponse,
    ChanCenterResponse,
    ChanOverlayResponse,
    ChanStrokeResponse,
    ChartBundleChanLevelResponse,
    ChartBundleChanResponse,
    ChartBundleResponse,
    ChartBundleSourceWatermarksResponse,
    ChartBundleV3Response,
    ChartBundleWarningResponse,
    ChartWindowRangeResponse,
    ChartWindowResponse,
)
from app.repositories.bars import generate_seed_bars, resolve_symbol
from app.repositories.postgres import get_bars_db, resolve_symbol_db
from app.routes.chan import build_chan_overlay
from trading_protocol import normalize_timeframe

router = APIRouter(prefix="/chart", tags=["chart"], dependencies=[Depends(require_token)])
v2_router = APIRouter(prefix="/chart", tags=["chart"], dependencies=[Depends(require_token)])
v3_router = APIRouter(prefix="/chart", tags=["chart"], dependencies=[Depends(require_token)])

DEFAULT_ANALYSIS_LEVELS = ("5f", "30f", "1d")
DEFAULT_CHAN_MODES = ("confirmed", "predictive")
FALLBACK_AGGREGATION_TIMEFRAMES = {"1w", "1m"}


@router.get("/window", response_model=ChartWindowResponse)
async def get_chart_window(
    request: Request,
    symbol: str = Query(..., min_length=6, max_length=16),
    timeframe: str = Query(default="5f"),
    levels: str = Query(default="5f,30f,1d"),
    modes: str = Query(default="confirmed,predictive"),
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=300, ge=1),
    settings: Settings = Depends(get_settings),
) -> ChartWindowResponse:
    return await build_chart_window(
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


@v2_router.get("/bundle", response_model=ChartBundleResponse)
async def get_chart_bundle(
    request: Request,
    symbol: str = Query(..., min_length=6, max_length=16),
    timeframe: str = Query(default="5f"),
    levels: str = Query(default="5f,30f,1d"),
    modes: str = Query(default="confirmed,predictive"),
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=300, ge=1),
    settings: Settings = Depends(get_settings),
) -> ChartBundleResponse:
    return await build_chart_bundle(
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


@v3_router.get("/bundle", response_model=ChartBundleV3Response)
async def get_chart_bundle_v3(
    request: Request,
    symbol: str = Query(..., min_length=6, max_length=16),
    timeframe: str = Query(default="5f"),
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=300, ge=1),
    settings: Settings = Depends(get_settings),
) -> ChartBundleV3Response:
    return await build_chart_bundle_v3(
        request=request,
        symbol=symbol,
        timeframe=timeframe,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        settings=settings,
    )


async def build_chart_window(
    request: Request,
    symbol: str,
    timeframe: str,
    levels: str,
    modes: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    settings: Settings,
) -> ChartWindowResponse:
    payload = await _build_chart_payload(
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
    return ChartWindowResponse(schema_version="chart-window.v1", **payload)


async def build_chart_bundle(
    request: Request,
    symbol: str,
    timeframe: str,
    levels: str,
    modes: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    settings: Settings,
) -> ChartBundleResponse:
    payload = await _build_chart_payload(
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
    return ChartBundleResponse(schema_version="chart-bundle.v2", **payload)


async def build_chart_bundle_v3(
    request: Request,
    symbol: str,
    timeframe: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    settings: Settings,
) -> ChartBundleV3Response:
    payload = await _build_chart_payload(
        request=request,
        symbol=symbol,
        timeframe=timeframe,
        levels=",".join(DEFAULT_ANALYSIS_LEVELS),
        modes=",".join(DEFAULT_CHAN_MODES),
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        settings=settings,
    )
    view_bars = payload["bars"]
    chan = payload["chan"]
    chart_timeframe = str(payload["chart_timeframe"])
    if not isinstance(chan, ChanOverlayResponse):
        raise TypeError("chart payload chan must be a ChanOverlayResponse")
    if chart_timeframe == "5f":
        canonical_5f_bars = view_bars
    else:
        canonical_5f = await build_bars_response(
            request=request,
            symbol=str(payload["symbol"]),
            timeframe="5f",
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            settings=settings,
        )
        canonical_5f_bars = canonical_5f.bars

    return ChartBundleV3Response(
        schema_version="chart-bundle.v3",
        snapshot_id=str(payload["snapshot_id"]),
        snapshot_version=chan.snapshot_version,
        symbol=str(payload["symbol"]),
        chart_timeframe=chart_timeframe,
        base_timeframe=chan.base_timeframe,
        bar_time_semantics=chan.base_ts_semantics,
        range=payload["range"],
        analysis_levels=list(DEFAULT_ANALYSIS_LEVELS),
        bars=view_bars,
        chan=_group_chan_by_level(chan, view_bars),
        source_watermarks=_source_watermarks(
            view_bars=view_bars,
            canonical_5f_bars=canonical_5f_bars,
            chan=chan,
        ),
        warnings=_bundle_warnings(
            chart_timeframe=chart_timeframe,
            view_bars=view_bars,
        ),
    )


async def _build_chart_payload(
    request: Request,
    symbol: str,
    timeframe: str,
    levels: str,
    modes: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    settings: Settings,
) -> dict[str, object]:
    bars = await build_bars_response(
        request=request,
        symbol=symbol,
        timeframe=timeframe,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        settings=settings,
    )
    chan = await build_chan_overlay(
        request=request,
        symbol=bars.symbol,
        timeframe=bars.timeframe,
        levels=levels,
        modes=modes,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        settings=settings,
    )
    return {
        "snapshot_id": _snapshot_id(bars, chan.engine, chan.snapshot_version),
        "symbol": bars.symbol,
        "chart_timeframe": bars.timeframe,
        "range": ChartWindowRangeResponse(
            from_time=_datetime_to_unix(from_ts),
            to_time=_datetime_to_unix(to_ts),
            limit=limit,
        ),
        "bars": bars.bars,
        "chan": chan,
    }


def _group_chan_by_level(
    chan: ChanOverlayResponse,
    view_bars: list[BarResponse] | None = None,
) -> ChartBundleChanResponse:
    range_start, range_end = _chan_view_range(view_bars or [])
    grouped = {}
    for level in DEFAULT_ANALYSIS_LEVELS:
        strokes = [item for item in chan.strokes if item.level == level]
        segments = [item for item in chan.segments if item.level == level]
        centers = [item for item in chan.centers if item.level == level]
        signals = [item for item in chan.signals if item.level == level]
        channels = [item for item in chan.channels if item.level == level]
        if range_start is not None and range_end is not None:
            strokes = [item for item in strokes if _line_intersects(item, range_start, range_end)]
            segments = [item for item in segments if _line_intersects(item, range_start, range_end)]
            centers = [item for item in centers if _center_intersects(item, range_start, range_end)]
            signals = [item for item in signals if _point_in_range(item.base_ts or item.time, range_start, range_end)]
            channels = [item for item in channels if _point_in_range(item.base_ts or item.time, range_start, range_end)]
        grouped[level] = ChartBundleChanLevelResponse(
            bar_count=chan.bars_by_level.get(level, 0),
            strokes=strokes,
            segments=segments,
            centers=centers,
            signals=signals,
            channels=channels,
        )
    return ChartBundleChanResponse(engine=chan.engine, levels=grouped)


def _chan_view_range(bars: list[BarResponse]) -> tuple[int | None, int | None]:
    if not bars:
        return None, None
    first = bars[0].time
    last = bars[-1].time
    padding = _range_padding_seconds(bars)
    return first - padding, last + padding


def _range_padding_seconds(bars: list[BarResponse]) -> int:
    if len(bars) < 2:
        return 24 * 60 * 60
    gaps = [
        bars[index].time - bars[index - 1].time
        for index in range(1, min(len(bars), 32))
        if bars[index].time > bars[index - 1].time
    ]
    if not gaps:
        return 24 * 60 * 60
    gaps.sort()
    interval = gaps[len(gaps) // 2]
    return max(interval * 3, 24 * 60 * 60)


def _line_intersects(
    item: ChanStrokeResponse,
    range_start: int,
    range_end: int,
) -> bool:
    start = item.begin_base_ts or item.start.base_ts or item.start.time
    end = item.end_base_ts or item.end.base_ts or item.end.time
    left = min(start, end)
    right = max(start, end)
    return left <= range_end and right >= range_start


def _center_intersects(
    item: ChanCenterResponse,
    range_start: int,
    range_end: int,
) -> bool:
    start = item.begin_base_ts or item.start_time
    end = item.end_base_ts or item.end_time
    left = min(start, end)
    right = max(start, end)
    return left <= range_end and right >= range_start


def _point_in_range(value: int | None, range_start: int, range_end: int) -> bool:
    return value is not None and range_start <= value <= range_end


def _source_watermarks(
    *,
    view_bars: list[BarResponse],
    canonical_5f_bars: list[BarResponse],
    chan: ChanOverlayResponse,
) -> ChartBundleSourceWatermarksResponse:
    canonical_last_complete = _last_complete_bar_end(canonical_5f_bars)
    return ChartBundleSourceWatermarksResponse(
        canonical_5f_last_complete_end=canonical_last_complete,
        canonical_5f_last_seen_end=canonical_5f_bars[-1].time if canonical_5f_bars else None,
        view_last_complete_end=_last_complete_bar_end(view_bars),
        analysis_generated_at=int(datetime.now(tz=UTC).timestamp()),
        analysis_source=_analysis_source(chan.engine),
        aggregation_source="canonical-5f",
        imported_5f_through=canonical_last_complete,
    )


def _last_complete_bar_end(bars: list[BarResponse]) -> int | None:
    for bar in reversed(bars):
        if bar.complete:
            return bar.time
    return None


def _analysis_source(engine: str) -> str:
    if engine.startswith("database:"):
        return "precomputed"
    if engine.startswith("chan-service:"):
        return "service"
    return "fallback"


def _bundle_warnings(
    *,
    chart_timeframe: str,
    view_bars: list[BarResponse],
) -> list[ChartBundleWarningResponse]:
    warnings: list[ChartBundleWarningResponse] = []
    if view_bars and not view_bars[-1].complete:
        warnings.append(
            ChartBundleWarningResponse(
                code="VIEW_BAR_INCOMPLETE",
                severity="info",
                message=f"latest {chart_timeframe} bar is incomplete",
            )
        )
    if chart_timeframe in FALLBACK_AGGREGATION_TIMEFRAMES:
        warnings.append(
            ChartBundleWarningResponse(
                code="AGGREGATION_FALLBACK",
                severity="warning",
                message=(
                    f"{chart_timeframe} view bars are not yet aggregated from canonical 5f "
                    "bars; stored timeframe rows are used until calendar-aware aggregation is available"
                ),
            )
        )
    return warnings


async def build_bars_response(
    request: Request,
    symbol: str,
    timeframe: str,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    settings: Settings,
) -> BarsResponse:
    try:
        normalized_timeframe = normalize_timeframe(timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not settings.use_seed_data:
        pool = getattr(request.app.state, "db_pool", None)
        if pool is None:
            raise HTTPException(status_code=503, detail="Database pool is not ready")
        symbol_row = await resolve_symbol_db(pool, symbol)
        if symbol_row is None:
            raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")
        bars = await get_bars_db(
            pool,
            symbol_row["symbol"],
            normalized_timeframe,
            from_ts,
            to_ts,
            limit,
        )
        return BarsResponse(
            symbol=symbol_row["symbol"],
            timeframe=normalized_timeframe,
            bars=[BarResponse(**bar) for bar in bars],
        )

    symbol_info = resolve_symbol(symbol)
    if symbol_info is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")

    bars = generate_seed_bars(
        symbol=symbol_info.symbol,
        timeframe=normalized_timeframe,
        start=from_ts,
        end=to_ts,
        limit=limit,
    )
    return BarsResponse(
        symbol=symbol_info.symbol,
        timeframe=normalized_timeframe,
        bars=[BarResponse(**bar.as_api_dict()) for bar in bars],
    )


def _snapshot_id(
    bars: BarsResponse,
    chan_engine: str,
    chan_snapshot_version: str = "",
) -> str:
    first = bars.bars[0] if bars.bars else None
    last = bars.bars[-1] if bars.bars else None
    payload = {
        "symbol": bars.symbol,
        "timeframe": bars.timeframe,
        "count": len(bars.bars),
        "first": first.time if first else None,
        "last": last.time if last else None,
        "last_revision": last.revision if last else None,
        "chan_engine": chan_engine,
        "chan_snapshot_version": chan_snapshot_version,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def _datetime_to_unix(value: datetime | None) -> int | None:
    if value is None:
        return None
    return int(value.timestamp())
