from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.core.config import Settings, get_settings
from app.core.security import authenticate_token_value
from app.models import LoginRequest, LoginResponse

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    http_request: Request,
    settings: Settings = Depends(get_settings),
) -> LoginResponse:
    principal = await authenticate_token_value(
        request.token,
        getattr(http_request.app.state, "db_pool", None),
        settings,
    )
    if principal is None:
        return LoginResponse(valid=False)
    return LoginResponse(
        valid=True,
        role=principal.role,
        display_name=principal.display_name,
        label=principal.label,
        token_id=principal.token_id,
    )
