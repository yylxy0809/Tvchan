from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import Settings, get_settings

bearer = HTTPBearer(auto_error=False)
AUTHENTICATION_SERVICE_UNAVAILABLE = "Authentication service unavailable"


class AuthenticationServiceUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class TokenPrincipal:
    role: str
    display_name: str | None = None
    label: str | None = None
    token_id: int | None = None


def hash_token(token: str) -> str:
    return sha256(token.encode("utf-8")).hexdigest()


def effective_admin_token(settings: Settings) -> str:
    return settings.admin_api_token


def static_token_principal(token: str, settings: Settings) -> TokenPrincipal | None:
    admin_token = effective_admin_token(settings)
    if admin_token and token == admin_token:
        return TokenPrincipal(role="admin", display_name="Administrator", label="admin")
    if settings.api_token and token == settings.api_token:
        return TokenPrincipal(role="user", display_name="API token", label="api-token")
    return None


async def authenticate_token_value(
    token: str | None,
    pool,
    settings: Settings,
) -> TokenPrincipal | None:
    if not token:
        return None

    principal = static_token_principal(token, settings)
    if principal is not None:
        return principal

    if pool is None:
        raise AuthenticationServiceUnavailable

    from app.repositories.tokens import find_active_token_by_hash, touch_token_last_used

    try:
        row = await find_active_token_by_hash(pool, hash_token(token))
        if row is None:
            return None
        if not await touch_token_last_used(pool, row["id"]):
            return None
    except Exception as exc:
        raise AuthenticationServiceUnavailable from exc
    return TokenPrincipal(
        role=row["role"],
        display_name=row["display_name"],
        label=row["label"],
        token_id=row["id"],
    )


async def require_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    settings: Settings = Depends(get_settings),
) -> TokenPrincipal:
    if not settings.api_token and not settings.admin_api_token:
        return TokenPrincipal(role="user", display_name="Auth disabled", label="auth-disabled")
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )
    try:
        principal = await authenticate_token_value(
            credentials.credentials,
            getattr(request.app.state, "db_pool", None),
            settings,
        )
    except AuthenticationServiceUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=AUTHENTICATION_SERVICE_UNAVAILABLE,
        ) from exc
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid bearer token",
        )
    return principal


async def require_admin_token(
    principal: TokenPrincipal = Depends(require_token),
) -> TokenPrincipal:
    if principal.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin token required",
        )
    return principal
