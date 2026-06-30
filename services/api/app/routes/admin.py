from __future__ import annotations

from secrets import token_urlsafe

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.core.security import hash_token, require_admin_token
from app.models import (
    AdminTokenCreateRequest,
    AdminTokenCreateResponse,
    AdminTokenListResponse,
    AdminTokenResponse,
)
from app.repositories import tokens as token_repository

router = APIRouter(prefix="/admin/tokens", tags=["admin"])


@router.get("", response_model=AdminTokenListResponse)
async def list_user_tokens(
    request: Request,
    _admin=Depends(require_admin_token),
) -> AdminTokenListResponse:
    pool = _require_pool(request)
    return AdminTokenListResponse(items=await token_repository.list_tokens(pool))


@router.post("", response_model=AdminTokenCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_user_token(
    payload: AdminTokenCreateRequest,
    request: Request,
    _admin=Depends(require_admin_token),
) -> AdminTokenCreateResponse:
    pool = _require_pool(request)
    plain_token = token_urlsafe(32)
    row = await token_repository.create_token(
        pool,
        token_hash=hash_token(plain_token),
        label=payload.label,
        display_name=payload.display_name,
    )
    return AdminTokenCreateResponse(**row, token=plain_token)


@router.post("/{token_id}/disable", response_model=AdminTokenResponse)
async def disable_user_token(
    token_id: int,
    request: Request,
    _admin=Depends(require_admin_token),
) -> AdminTokenResponse:
    pool = _require_pool(request)
    row = await token_repository.disable_token(pool, token_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Token not found")
    return AdminTokenResponse(**row)


@router.delete("/{token_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user_token(
    token_id: int,
    request: Request,
    _admin=Depends(require_admin_token),
) -> Response:
    pool = _require_pool(request)
    deleted = await token_repository.delete_token(pool, token_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Token not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _require_pool(request: Request):
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database token store is not available",
        )
    return pool
