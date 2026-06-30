from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import SymbolResponse, SymbolsResponse
from app.repositories.bars import search_symbols
from app.repositories.postgres import search_symbols_db

router = APIRouter(prefix="/symbols", tags=["symbols"], dependencies=[Depends(require_token)])


@router.get("", response_model=SymbolsResponse)
async def list_symbols(
    request: Request,
    keyword: str = Query(default="", max_length=64),
    limit: int = Query(default=20, ge=1, le=100),
    settings: Settings = Depends(get_settings),
) -> SymbolsResponse:
    if not settings.use_seed_data:
        pool = getattr(request.app.state, "db_pool", None)
        if pool is None:
            raise HTTPException(status_code=503, detail="Database pool is not ready")
        rows = await search_symbols_db(pool, keyword, limit)
        return SymbolsResponse(items=[SymbolResponse(**row) for row in rows])

    return SymbolsResponse(
        items=[
            SymbolResponse(
                symbol=item.symbol,
                code=item.code,
                exchange=item.exchange,
                name=item.name,
                asset_type=item.asset_type,
            )
            for item in search_symbols(keyword, limit)
        ]
    )
