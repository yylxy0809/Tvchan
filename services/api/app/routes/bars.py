from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import BarResponse, BarsResponse
from app.repositories.bars import generate_seed_bars, resolve_symbol
from app.repositories.postgres import get_bars_db, resolve_symbol_db
from trading_protocol import normalize_timeframe

router = APIRouter(prefix="/bars", tags=["bars"], dependencies=[Depends(require_token)])


@router.get("", response_model=BarsResponse)
async def get_bars(
    request: Request,
    symbol: str = Query(..., min_length=6, max_length=16),
    timeframe: str = Query(default="5f"),
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=300, ge=1, le=5000),
    settings: Settings = Depends(get_settings),
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
