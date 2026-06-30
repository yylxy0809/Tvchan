from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import ChanScreenerResponse
from app.repositories.chan_screener import conditions_from_llm_payload, query_chan_screener
from app.services.llm_client import parse_chan_query_with_llm

router = APIRouter(prefix="/screener", tags=["screener"], dependencies=[Depends(require_token)])


@router.get("/chan", response_model=ChanScreenerResponse)
async def chan_screener(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500),
    mode: str = Query(default="current", pattern="^(current|confirmed|predictive)$"),
    limit: int = Query(default=100, ge=1, le=1000),
    settings: Settings = Depends(get_settings),
) -> ChanScreenerResponse:
    if settings.use_seed_data:
        raise HTTPException(status_code=503, detail="Chan screener requires database mode")
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not ready")
    parser = "rules"
    parser_error: str | None = None
    parsed_conditions = None
    parsed_unsupported = None
    if settings.llm_enabled and settings.llm_api_key:
        try:
            llm_result = await parse_chan_query_with_llm(
                query=q,
                api_key=settings.llm_api_key,
                base_url=settings.llm_base_url,
                model=settings.llm_model,
                timeout_seconds=settings.llm_timeout_seconds,
            )
            parsed_conditions, parsed_unsupported = conditions_from_llm_payload(llm_result.payload)
            if parsed_conditions:
                parser = "llm"
            else:
                parsed_conditions = None
                parsed_unsupported = None
        except Exception as exc:
            parser_error = str(exc)[:300]
    return ChanScreenerResponse(
        **await query_chan_screener(
            pool,
            query=q,
            mode=mode,
            limit=limit,
            parsed_conditions=parsed_conditions,
            parsed_unsupported=parsed_unsupported,
            parser=parser,
            parser_error=parser_error,
        )
    )
