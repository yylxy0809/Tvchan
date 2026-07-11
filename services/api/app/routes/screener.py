from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.config import Settings, get_settings
from app.core.security import require_token
from app.models import ChanScreenerResponse, WencaiScreenerResponse
from app.repositories.chan_screener import (
    conditions_from_llm_payload,
    parse_chan_screener_query,
    query_chan_screener,
)
from app.repositories import runtime_config as runtime_config_repository
from app.services.llm_config import WENCAI_CONFIG_KEY, resolve_active_llm_provider
from app.services.llm_client import parse_chan_query_with_llm
from app.services.wencai_client import WencaiConfig, WencaiConfigError, WencaiUpstreamError, query_wencai

router = APIRouter(prefix="/screener", tags=["screener"], dependencies=[Depends(require_token)])


@router.get("/wencai", response_model=WencaiScreenerResponse)
async def wencai_screener(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    settings: Settings = Depends(get_settings),
) -> WencaiScreenerResponse:
    if page_size not in {20, 50, 100}:
        raise HTTPException(status_code=422, detail="page_size must be one of 20, 50, 100")
    pool = getattr(request.app.state, "db_pool", None)
    config = await _load_wencai_config(pool, settings=settings)
    try:
        result = await query_wencai(
            query=q.strip(),
            page=page,
            page_size=page_size,
            config=config,
        )
    except WencaiConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except WencaiUpstreamError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return WencaiScreenerResponse(
        query=result.query,
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        fetched_at=result.fetched_at,
        conditions=_split_conditions(q),
        suggestions=["主板", "成交量>1000万股", "MACD零上金叉", "振幅>3%"],
        items=[item.__dict__ for item in result.items],
    )


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
    parsed_conditions, parsed_unsupported = parse_chan_screener_query(q)
    if not parsed_conditions:
        parsed_conditions = None
        parsed_unsupported = None
        llm_provider = await resolve_active_llm_provider(pool, settings)
    else:
        llm_provider = None
    if llm_provider is not None:
        try:
            llm_result = await parse_chan_query_with_llm(
                query=q,
                api_key=llm_provider.api_key,
                base_url=llm_provider.base_url,
                model=llm_provider.model,
                timeout_seconds=llm_provider.timeout_seconds,
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


async def _load_wencai_config(pool, *, settings: Settings) -> WencaiConfig:
    if pool is not None:
        row = await runtime_config_repository.get_config(pool, WENCAI_CONFIG_KEY)
        if row is not None and isinstance(row.get("value"), dict):
            value = row["value"]
            return WencaiConfig(
                base_url=str(value.get("base_url") or settings.iwencai_base_url),
                api_key=str(value.get("api_key") or settings.iwencai_api_key),
                cookie=str(value.get("cookie") or ""),
                user_agent=str(value.get("user_agent") or "") or None,
                pro=bool(value.get("pro", False)),
                timeout_seconds=float(value.get("timeout_seconds") or 20),
            )
    return WencaiConfig(
        base_url=settings.iwencai_base_url,
        api_key=settings.iwencai_api_key,
        cookie=settings.wencai_cookie,
        user_agent=settings.wencai_user_agent or None,
        pro=settings.wencai_pro,
        timeout_seconds=settings.wencai_timeout_seconds,
    )


def _split_conditions(query: str) -> list[str]:
    return [
        item.strip()
        for item in re.split(r"[，,。；;\n]+", query)
        if item.strip()
    ][:8]
