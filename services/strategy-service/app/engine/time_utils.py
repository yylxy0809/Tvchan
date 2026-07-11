from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def utc_time(value: Any) -> datetime:
    """Parse only timezone-aware instants and normalize them to UTC."""
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported timestamp type: {type(value).__name__}")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Naive datetime is not allowed")
    return result.astimezone(UTC)


def cutoff_key(value: Any) -> datetime:
    return utc_time(value)


def iso_utc(value: Any) -> str:
    return utc_time(value).isoformat()
