from __future__ import annotations

import gzip
import hashlib
import json
import time
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from threading import BoundedSemaphore, RLock
from typing import Any, Callable, Iterator
from uuid import uuid4

DEFAULT_CHUNK_SIZE_BYTES = 256 * 1024
MAX_CHUNK_SIZE_BYTES = 1024 * 1024
MAX_BARS_PER_EXPORT = 100_000
MAX_UNCOMPRESSED_EXPORT_BYTES = 16 * 1024 * 1024
MAX_COMPRESSED_EXPORT_BYTES = 16 * 1024 * 1024
MAX_EXPORT_CHUNKS = 1024
HISTORY_EXPORT_TTL_SECONDS = 15 * 60
MAX_HISTORY_EXPORT_RECORDS = 32
MAX_HISTORY_EXPORT_RECORDS_PER_OWNER = 4
MAX_HISTORY_EXPORT_STORED_BYTES = 64 * 1024 * 1024
MAX_HISTORY_EXPORT_STORED_BYTES_PER_OWNER = 32 * 1024 * 1024
MAX_CONCURRENT_HISTORY_EXPORT_BUILDS = 2


class ExportTooLarge(ValueError):
    pass


class ExportBuildBusy(RuntimeError):
    pass


class ExportCapacityExceeded(RuntimeError):
    pass


class ExportOwnerCapacityExceeded(ExportCapacityExceeded):
    pass


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
    expires_at: str
    metadata: dict[str, Any]
    bar_count: int
    uncompressed_size_bytes: int
    chunks: tuple[ExportChunk, ...]

    @property
    def compressed_size_bytes(self) -> int:
        return sum(chunk.size_bytes for chunk in self.chunks)

    def manifest(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "format": "json",
            "compression": "gzip",
            "bar_count": self.bar_count,
            "metadata": deepcopy(self.metadata),
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


@dataclass(frozen=True)
class _StoredExport:
    owner_key: str
    expires_at_monotonic: float
    chunks: tuple[ExportChunk, ...]

    @property
    def compressed_size_bytes(self) -> int:
        return sum(chunk.size_bytes for chunk in self.chunks)


class InMemoryHistoryExportStore:
    def __init__(
        self,
        *,
        ttl_seconds: int = HISTORY_EXPORT_TTL_SECONDS,
        max_records: int = MAX_HISTORY_EXPORT_RECORDS,
        max_records_per_owner: int = MAX_HISTORY_EXPORT_RECORDS_PER_OWNER,
        max_stored_bytes: int = MAX_HISTORY_EXPORT_STORED_BYTES,
        max_stored_bytes_per_owner: int = MAX_HISTORY_EXPORT_STORED_BYTES_PER_OWNER,
        max_bars: int = MAX_BARS_PER_EXPORT,
        max_uncompressed_bytes: int = MAX_UNCOMPRESSED_EXPORT_BYTES,
        max_compressed_bytes: int = MAX_COMPRESSED_EXPORT_BYTES,
        max_chunk_size_bytes: int = MAX_CHUNK_SIZE_BYTES,
        max_chunks: int = MAX_EXPORT_CHUNKS,
        max_concurrent_builds: int = MAX_CONCURRENT_HISTORY_EXPORT_BUILDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_records = max_records
        self.max_records_per_owner = max_records_per_owner
        self.max_stored_bytes = max_stored_bytes
        self.max_stored_bytes_per_owner = max_stored_bytes_per_owner
        self.max_bars = max_bars
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.max_compressed_bytes = max_compressed_bytes
        self.max_chunk_size_bytes = max_chunk_size_bytes
        self.max_chunks = max_chunks
        self._clock = clock
        self._lock = RLock()
        self._build_slots = BoundedSemaphore(max_concurrent_builds)
        self._active_owners: set[str] = set()
        self._records: dict[str, _StoredExport] = {}

    @contextmanager
    def reserve_build(self, owner_key: str) -> Iterator[None]:
        if not self._build_slots.acquire(blocking=False):
            raise ExportBuildBusy("History export build capacity is busy")
        try:
            with self._lock:
                if owner_key in self._active_owners:
                    raise ExportBuildBusy("A history export is already building")
                self._prune_expired_locked()
                self._check_build_capacity_locked(owner_key)
                self._active_owners.add(owner_key)
            try:
                yield
            finally:
                with self._lock:
                    self._active_owners.discard(owner_key)
        finally:
            self._build_slots.release()

    def create_export(
        self,
        *,
        owner_key: str,
        bars: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
        chunk_size_bytes: int | None = None,
    ) -> ExportRecord:
        with self._lock:
            if owner_key not in self._active_owners:
                raise RuntimeError("History export build slot is required")
        if len(bars) > self.max_bars:
            raise ExportTooLarge("History export contains too many bars")
        chunk_size = _normalize_chunk_size(
            chunk_size_bytes,
            max_chunk_size_bytes=self.max_chunk_size_bytes,
        )
        request_id = uuid4().hex
        created_monotonic = self._clock()
        created = datetime.now(timezone.utc)
        created_at = created.isoformat()
        expires_at = (created + timedelta(seconds=self.ttl_seconds)).isoformat()
        export_metadata = deepcopy(metadata or {})
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
        if len(raw) > self.max_uncompressed_bytes:
            raise ExportTooLarge("History export uncompressed payload is too large")
        compressed = gzip.compress(raw)
        if len(compressed) > self.max_compressed_bytes:
            raise ExportTooLarge("History export compressed payload is too large")
        chunk_count = (len(compressed) + chunk_size - 1) // chunk_size
        if chunk_count > self.max_chunks:
            raise ExportTooLarge("History export would create too many chunks")
        chunks = tuple(
            ExportChunk(index=index, data=compressed[start : start + chunk_size])
            for index, start in enumerate(range(0, len(compressed), chunk_size))
        )
        record = ExportRecord(
            request_id=request_id,
            created_at=created_at,
            expires_at=expires_at,
            metadata=export_metadata,
            bar_count=len(bars),
            uncompressed_size_bytes=len(raw),
            chunks=chunks,
        )
        with self._lock:
            self._prune_expired_locked()
            self._check_capacity_locked(owner_key, record.compressed_size_bytes)
            self._records[request_id] = _StoredExport(
                owner_key=owner_key,
                expires_at_monotonic=created_monotonic + self.ttl_seconds,
                chunks=record.chunks,
            )
        return record

    def get_chunk(self, owner_key: str, request_id: str, index: int) -> ExportChunk | None:
        with self._lock:
            self._prune_expired_locked()
            stored = self._records.get(request_id)
            if (
                stored is None
                or stored.owner_key != owner_key
                or index < 0
                or index >= len(stored.chunks)
            ):
                return None
            return stored.chunks[index]

    @property
    def record_count(self) -> int:
        with self._lock:
            self._prune_expired_locked()
            return len(self._records)

    def clear(self) -> None:
        with self._lock:
            self._records.clear()

    def _prune_expired_locked(self) -> None:
        now = self._clock()
        expired = [
            request_id
            for request_id, stored in self._records.items()
            if now >= stored.expires_at_monotonic
        ]
        for request_id in expired:
            self._records.pop(request_id, None)

    def _check_capacity_locked(self, owner_key: str, size_bytes: int) -> None:
        owner_records = [
            stored for stored in self._records.values() if stored.owner_key == owner_key
        ]
        if len(owner_records) >= self.max_records_per_owner:
            raise ExportOwnerCapacityExceeded("History export owner record capacity exceeded")
        owner_bytes = sum(item.compressed_size_bytes for item in owner_records)
        if owner_bytes + size_bytes > self.max_stored_bytes_per_owner:
            raise ExportOwnerCapacityExceeded("History export owner byte capacity exceeded")
        if len(self._records) >= self.max_records:
            raise ExportCapacityExceeded("History export record capacity exceeded")
        stored_bytes = sum(
            item.compressed_size_bytes for item in self._records.values()
        )
        if stored_bytes + size_bytes > self.max_stored_bytes:
            raise ExportCapacityExceeded("History export byte capacity exceeded")

    def _check_build_capacity_locked(self, owner_key: str) -> None:
        owner_records = [
            stored for stored in self._records.values() if stored.owner_key == owner_key
        ]
        if len(owner_records) >= self.max_records_per_owner:
            raise ExportOwnerCapacityExceeded("History export owner record capacity exceeded")
        if sum(item.compressed_size_bytes for item in owner_records) >= (
            self.max_stored_bytes_per_owner
        ):
            raise ExportOwnerCapacityExceeded("History export owner byte capacity exceeded")
        if len(self._records) >= self.max_records:
            raise ExportCapacityExceeded("History export record capacity exceeded")
        if sum(item.compressed_size_bytes for item in self._records.values()) >= (
            self.max_stored_bytes
        ):
            raise ExportCapacityExceeded("History export byte capacity exceeded")


def _normalize_chunk_size(
    chunk_size_bytes: int | None,
    *,
    max_chunk_size_bytes: int = MAX_CHUNK_SIZE_BYTES,
) -> int:
    if chunk_size_bytes is None:
        return DEFAULT_CHUNK_SIZE_BYTES
    chunk_size = int(chunk_size_bytes)
    if chunk_size < 1 or chunk_size > max_chunk_size_bytes:
        raise ExportTooLarge("History export chunk size is outside the allowed range")
    return chunk_size


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


export_store = InMemoryHistoryExportStore()
