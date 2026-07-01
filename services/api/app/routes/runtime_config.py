from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import require_admin_token
from app.models import RuntimeConfigResponse, RuntimeConfigUpdateRequest
from app.repositories import runtime_config as runtime_config_repository

FRONTEND_FEATURE_CONFIG_KEY = "frontend.features"

router = APIRouter(prefix="/config", tags=["config"])
admin_router = APIRouter(prefix="/admin/runtime-config", tags=["admin"])


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
