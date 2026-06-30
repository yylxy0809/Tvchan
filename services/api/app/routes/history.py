from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.core.security import require_token
from app.history import DEFAULT_CHUNK_SIZE_BYTES, export_store


router = APIRouter(
    prefix="/history/export",
    tags=["history"],
    dependencies=[Depends(require_token)],
)


class HistoryExportRequest(BaseModel):
    bars: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    chunk_size_bytes: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Optional gzip chunk size in bytes. Defaults to "
            f"{DEFAULT_CHUNK_SIZE_BYTES}."
        ),
    )


@router.post("")
def create_history_export(request: HistoryExportRequest) -> dict[str, Any]:
    record = export_store.create_export(
        bars=request.bars,
        metadata=request.metadata,
        chunk_size_bytes=request.chunk_size_bytes,
    )
    return record.manifest()


@router.get("/{request_id}/chunks/{index}")
def get_history_export_chunk(request_id: str, index: int) -> Response:
    chunk = export_store.get_chunk(request_id, index)
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
