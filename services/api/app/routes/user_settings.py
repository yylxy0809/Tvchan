from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.config import Settings, get_settings
from app.core.security import hash_token, require_token
from app.models import (
    UserSettingResponse,
    UserSettingsResponse,
    UserSettingUpdateRequest,
)
from app.repositories import user_settings as user_settings_repository

AUTH_DISABLED_OWNER = "__auth_disabled_user_settings__"

router = APIRouter(prefix="/user/settings", tags=["user-settings"])


@router.get("", response_model=UserSettingsResponse)
async def list_user_settings(
    request: Request,
    _principal=Depends(require_token),
    settings: Settings = Depends(get_settings),
) -> UserSettingsResponse:
    pool = _require_pool(request)
    rows = await user_settings_repository.list_settings(
        pool,
        _owner_token_hash(request, settings),
    )
    return UserSettingsResponse(items=[UserSettingResponse(**row) for row in rows])


@router.put("/{bucket}", response_model=UserSettingResponse)
async def update_user_setting(
    bucket: str,
    payload: UserSettingUpdateRequest,
    request: Request,
    _principal=Depends(require_token),
    settings: Settings = Depends(get_settings),
) -> UserSettingResponse:
    if bucket not in user_settings_repository.USER_SETTING_BUCKET_SET:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Unknown user setting bucket",
        )
    pool = _require_pool(request)
    row = await user_settings_repository.upsert_setting(
        pool,
        owner_token_hash=_owner_token_hash(request, settings),
        bucket=bucket,
        value=payload.value,
    )
    return UserSettingResponse(**row)


def _owner_token_hash(request: Request, settings: Settings) -> str:
    if not settings.api_token and not settings.admin_api_token:
        return hash_token(AUTH_DISABLED_OWNER)

    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )
    return hash_token(token.strip())


def _require_pool(request: Request):
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database user settings store is not available",
        )
    return pool
