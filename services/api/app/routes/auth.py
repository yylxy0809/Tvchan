from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.config import Settings, get_settings
from app.core.security import authenticate_token_value, static_token_principal
from app.models import LoginRequest, LoginResponse

router = APIRouter(prefix="/auth", tags=["auth"])

_AUTHENTICATION_SERVICE_UNAVAILABLE = "Authentication service unavailable"


@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    http_request: Request,
    settings: Settings = Depends(get_settings),
) -> LoginResponse:
    principal = static_token_principal(request.token, settings)
    if principal is None:
        pool = getattr(http_request.app.state, "db_pool", None)
        if pool is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=_AUTHENTICATION_SERVICE_UNAVAILABLE,
            )
        try:
            principal = await authenticate_token_value(request.token, pool, settings)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=_AUTHENTICATION_SERVICE_UNAVAILABLE,
            ) from exc
    if principal is None:
        return LoginResponse(valid=False)
    return LoginResponse(
        valid=True,
        role=principal.role,
        display_name=principal.display_name,
        label=principal.label,
        token_id=principal.token_id,
    )
