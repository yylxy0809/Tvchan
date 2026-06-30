from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ProviderHealth:
    name: str
    ok: bool
    message: str = ""


@dataclass(frozen=True)
class BackfillRequest:
    symbols: list[str]
    timeframes: list[str]
    start: datetime | None = None
    end: datetime | None = None
    limit: int = 300

