from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, ValidationError
from starlette.concurrency import run_in_threadpool

from app.core.security import TokenPrincipal, bearer, require_token
from app.history import DEFAULT_CHUNK_SIZE_BYTES, export_store
from app.history.exports import (
    MAX_BARS_PER_EXPORT,
    MAX_CHUNK_SIZE_BYTES,
    ExportBuildBusy,
    ExportCapacityExceeded,
    ExportOwnerCapacityExceeded,
    ExportTooLarge,
)


MAX_HISTORY_EXPORT_REQUEST_BYTES = 16 * 1024 * 1024
HISTORY_EXPORT_READ_TIMEOUT_SECONDS = 10.0
_OWNER_KEY_SECRET = secrets.token_bytes(32)

router = APIRouter(
    prefix="/history/export",
    tags=["history"],
    dependencies=[Depends(require_token)],
)


class HistoryExportRequest(BaseModel):
    bars: list[dict[str, Any]] = Field(default_factory=list, max_length=MAX_BARS_PER_EXPORT)
    metadata: dict[str, Any] = Field(default_factory=dict)
    chunk_size_bytes: int | None = Field(
        default=None,
        ge=1,
        le=MAX_CHUNK_SIZE_BYTES,
        description=(
            "Optional gzip chunk size in bytes. Defaults to "
            f"{DEFAULT_CHUNK_SIZE_BYTES}."
        ),
    )


@router.post("")
async def create_history_export(
    request: Request,
    principal: TokenPrincipal = Depends(require_token),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict[str, Any]:
    owner_key = _history_export_owner_key(principal, credentials)
    try:
        with export_store.reserve_build(owner_key):
            export_request = await _read_history_export_request(request)
            record = await run_in_threadpool(
                export_store.create_export,
                owner_key=owner_key,
                bars=export_request.bars,
                metadata=export_request.metadata,
                chunk_size_bytes=export_request.chunk_size_bytes,
            )
    except ExportTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except (ExportBuildBusy, ExportOwnerCapacityExceeded) as exc:
        raise HTTPException(
            status_code=429,
            detail="History export capacity is temporarily unavailable",
            headers={"Retry-After": "1"},
        ) from exc
    except ExportCapacityExceeded as exc:
        raise HTTPException(
            status_code=507,
            detail="History export storage capacity is unavailable",
            headers={"Retry-After": "1"},
        ) from exc
    return record.manifest()


@router.get("/{request_id}/chunks/{index}")
def get_history_export_chunk(
    request_id: str,
    index: int,
    principal: TokenPrincipal = Depends(require_token),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> Response:
    owner_key = _history_export_owner_key(principal, credentials)
    chunk = export_store.get_chunk(owner_key, request_id, index)
    if chunk is None:
        raise HTTPException(status_code=404, detail="History export chunk not found")
    return Response(
        content=chunk.data,
        media_type="application/gzip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="history-{request_id}-{index}.json.gz"'
            ),
            "X-History-Export-Request-Id": request_id,
            "X-History-Export-Chunk-Index": str(index),
            "X-History-Export-Chunk-Sha256": chunk.sha256,
        },
    )


async def _read_history_export_request(request: Request) -> HistoryExportRequest:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > MAX_HISTORY_EXPORT_REQUEST_BYTES:
                raise HTTPException(status_code=413, detail="History export request is too large")
        except ValueError:
            pass
    try:
        async with asyncio.timeout(HISTORY_EXPORT_READ_TIMEOUT_SECONDS):
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_HISTORY_EXPORT_REQUEST_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail="History export request is too large",
                    )
                body.extend(chunk)
            return await run_in_threadpool(
                HistoryExportRequest.model_validate_json,
                bytes(body),
            )
    except TimeoutError as exc:
        raise HTTPException(
            status_code=408,
            detail="History export request timed out",
        ) from exc
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail="Invalid history export request",
        ) from exc


def _history_export_owner_key(
    principal: TokenPrincipal,
    credentials: HTTPAuthorizationCredentials | None,
) -> str:
    if principal.label == "auth-disabled":
        return "auth-disabled"
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return hmac.new(
        _OWNER_KEY_SECRET,
        credentials.credentials.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
