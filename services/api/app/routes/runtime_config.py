from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import require_admin_token
from app.core.config import Settings, get_settings
from app.models import (
    ConnectivityTestResponse,
    LlmProviderResponse,
    LlmProviderTestResponse,
    LlmProvidersResponse,
    RuntimeConfigResponse,
    RuntimeConfigUpdateRequest,
    WencaiConfigResponse,
    WencaiConfigUpdateRequest,
)
from app.repositories import runtime_config as runtime_config_repository
from app.services.llm_config import (
    WENCAI_CONFIG_KEY,
    load_llm_providers,
    llm_config_to_response,
    mask_secret,
    provider_from_payload,
    save_llm_providers,
    test_llm_provider,
)
from app.services.wencai_client import WencaiConfig, test_wencai_config

FRONTEND_FEATURE_CONFIG_KEY = "frontend.features"

router = APIRouter(prefix="/config", tags=["config"])
admin_router = APIRouter(prefix="/admin/runtime-config", tags=["admin"])
wencai_admin_router = APIRouter(prefix="/admin/wencai", tags=["admin"])
llm_admin_router = APIRouter(prefix="/admin/llm", tags=["admin"])


@router.get("/features", response_model=RuntimeConfigResponse)
async def get_frontend_feature_config(request: Request) -> RuntimeConfigResponse:
    pool = _require_pool(request)
    row = await runtime_config_repository.get_config(pool, FRONTEND_FEATURE_CONFIG_KEY)
    if row is None:
        return RuntimeConfigResponse(
            key=FRONTEND_FEATURE_CONFIG_KEY,
            value={},
            version=0,
            updated_at=None,
        )
    return RuntimeConfigResponse(**row)


@admin_router.put("/{key}", response_model=RuntimeConfigResponse)
async def update_runtime_config(
    key: str,
    payload: RuntimeConfigUpdateRequest,
    request: Request,
    _admin=Depends(require_admin_token),
) -> RuntimeConfigResponse:
    pool = _require_pool(request)
    row = await runtime_config_repository.upsert_config(
        pool,
        key=key,
        value=payload.value,
    )
    return RuntimeConfigResponse(**row)


def _require_pool(request: Request):
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database runtime config store is not available",
        )
    return pool


@wencai_admin_router.get("/config", response_model=WencaiConfigResponse)
async def get_wencai_config(
    request: Request,
    _admin=Depends(require_admin_token),
    settings: Settings = Depends(get_settings),
) -> WencaiConfigResponse:
    pool = _require_pool(request)
    config = await _load_wencai_config(pool, settings=settings)
    return _wencai_response(config)


@wencai_admin_router.put("/config", response_model=WencaiConfigResponse)
async def update_wencai_config(
    payload: WencaiConfigUpdateRequest,
    request: Request,
    _admin=Depends(require_admin_token),
    settings: Settings = Depends(get_settings),
) -> WencaiConfigResponse:
    pool = _require_pool(request)
    existing = await _load_wencai_config(pool, settings=settings)
    api_key = _resolve_secret(payload.api_key or "", existing.api_key)
    cookie = _resolve_secret(payload.cookie or "", existing.cookie)
    config = WencaiConfig(
        base_url=(payload.base_url or "").strip() or existing.base_url,
        api_key=api_key,
        cookie=cookie,
        user_agent=(payload.user_agent or "").strip() or None,
        pro=payload.pro,
        timeout_seconds=payload.timeout_seconds,
    )
    await runtime_config_repository.upsert_config(
        pool,
        key=WENCAI_CONFIG_KEY,
        value=_wencai_storage(config),
    )
    return _wencai_response(config)


@wencai_admin_router.post("/test", response_model=ConnectivityTestResponse)
async def test_wencai_connectivity(
    payload: WencaiConfigUpdateRequest,
    request: Request,
    _admin=Depends(require_admin_token),
    settings: Settings = Depends(get_settings),
) -> ConnectivityTestResponse:
    pool = _require_pool(request)
    existing = await _load_wencai_config(pool, settings=settings)
    config = WencaiConfig(
        base_url=(payload.base_url or "").strip() or existing.base_url,
        api_key=_resolve_secret(payload.api_key or "", existing.api_key),
        cookie=_resolve_secret(payload.cookie or "", existing.cookie),
        user_agent=(payload.user_agent or "").strip() or None,
        pro=payload.pro,
        timeout_seconds=payload.timeout_seconds,
    )
    result = await test_wencai_config(config)
    return ConnectivityTestResponse(**result.__dict__)


@llm_admin_router.get("/providers", response_model=LlmProvidersResponse)
async def get_llm_providers(
    request: Request,
    _admin=Depends(require_admin_token),
    settings: Settings = Depends(get_settings),
) -> LlmProvidersResponse:
    pool = _require_pool(request)
    config = await load_llm_providers(pool, settings=settings)
    return LlmProvidersResponse(**llm_config_to_response(config))


@llm_admin_router.put("/providers", response_model=LlmProvidersResponse)
async def update_llm_providers(
    payload: LlmProvidersResponse,
    request: Request,
    _admin=Depends(require_admin_token),
) -> LlmProvidersResponse:
    pool = _require_pool(request)
    saved = await save_llm_providers(pool, payload.model_dump())
    return LlmProvidersResponse(**llm_config_to_response(saved))


@llm_admin_router.post("/test", response_model=LlmProviderTestResponse)
async def test_llm_connectivity(
    payload: LlmProviderResponse,
    request: Request,
    _admin=Depends(require_admin_token),
    settings: Settings = Depends(get_settings),
) -> LlmProviderTestResponse:
    pool = _require_pool(request)
    existing_config = await load_llm_providers(pool, settings=settings)
    existing = next((item for item in existing_config.providers if item.id == payload.id), None)
    provider = provider_from_payload(payload.model_dump(), existing)
    result = await test_llm_provider(provider)
    return LlmProviderTestResponse(**result.__dict__)


async def _load_wencai_config(pool, *, settings: Settings) -> WencaiConfig:
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


def _wencai_storage(config: WencaiConfig) -> dict:
    return {
        "base_url": config.base_url,
        "api_key": config.api_key,
        "cookie": config.cookie,
        "user_agent": config.user_agent,
        "pro": config.pro,
        "timeout_seconds": config.timeout_seconds,
    }


def _wencai_response(config: WencaiConfig) -> WencaiConfigResponse:
    return WencaiConfigResponse(
        base_url=config.base_url,
        api_key=mask_secret(config.api_key),
        cookie=mask_secret(config.cookie),
        user_agent=config.user_agent,
        pro=config.pro,
        timeout_seconds=config.timeout_seconds,
    )


def _resolve_secret(candidate: str, previous: str) -> str:
    if "..." in candidate or candidate == "***":
        return previous
    return candidate.strip()
