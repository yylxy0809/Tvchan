from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

DEFAULT_CHUNK_SIZE_BYTES = 256 * 1024


@dataclass(frozen=True)
class ExportChunk:
    index: int
    data: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()

    @property
    def size_bytes(self) -> int:
        return len(self.data)


@dataclass(frozen=True)
class ExportRecord:
    request_id: str
    created_at: str
    metadata: dict[str, Any]
    bar_count: int
    uncompressed_size_bytes: int
    chunks: list[ExportChunk]

    @property
    def compressed_size_bytes(self) -> int:
        return sum(chunk.size_bytes for chunk in self.chunks)

    def manifest(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "created_at": self.created_at,
            "format": "json",
            "compression": "gzip",
            "bar_count": self.bar_count,
            "metadata": self.metadata,
            "uncompressed_size_bytes": self.uncompressed_size_bytes,
            "compressed_size_bytes": self.compressed_size_bytes,
            "chunk_count": len(self.chunks),
            "chunks": [
                {
                    "index": chunk.index,
                    "href": (
                        f"/api/v1/history/export/{self.request_id}"
                        f"/chunks/{chunk.index}"
                    ),
                    "size_bytes": chunk.size_bytes,
                    "sha256": chunk.sha256,
                    "compression": "gzip",
                }
                for chunk in self.chunks
            ],
        }


class InMemoryHistoryExportStore:
    def __init__(self) -> None:
        self._records: dict[str, ExportRecord] = {}

    def create_export(
        self,
        *,
        bars: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
        chunk_size_bytes: int | None = None,
    ) -> ExportRecord:
        request_id = uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        export_metadata = dict(metadata or {})
        payload = {
            "request_id": request_id,
            "created_at": created_at,
            "metadata": export_metadata,
            "bars": bars,
        }
        raw = json.dumps(
            payload,
            default=_json_default,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        compressed = gzip.compress(raw)
        chunk_size = _normalize_chunk_size(chunk_size_bytes)
        chunks = [
            ExportChunk(index=index, data=compressed[start : start + chunk_size])
            for index, start in enumerate(range(0, len(compressed), chunk_size))
        ]
        record = ExportRecord(
            request_id=request_id,
            created_at=created_at,
            metadata=export_metadata,
            bar_count=len(bars),
            uncompressed_size_bytes=len(raw),
            chunks=chunks,
        )
        self._records[request_id] = record
        return record

    def get_chunk(self, request_id: str, index: int) -> ExportChunk | None:
        record = self._records.get(request_id)
        if record is None or index < 0 or index >= len(record.chunks):
            return None
        return record.chunks[index]


def _normalize_chunk_size(chunk_size_bytes: int | None) -> int:
    if chunk_size_bytes is None:
        return DEFAULT_CHUNK_SIZE_BYTES
    return max(1, int(chunk_size_bytes))


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


export_store = InMemoryHistoryExportStore()
