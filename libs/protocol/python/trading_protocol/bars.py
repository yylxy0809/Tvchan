from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Bar:
    symbol: str
    timeframe: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float | None = None
    complete: bool = True
    revision: int = 0
    source: str = "seed"

    def as_api_dict(self) -> dict:
        return {
            "time": int(self.ts.timestamp()),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "complete": self.complete,
            "revision": self.revision,
        }
