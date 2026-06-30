from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from collector.models import ProviderHealth
from trading_protocol import Bar, SymbolInfo


class MarketDataProvider(ABC):
    name: str

    @abstractmethod
    async def list_symbols(self) -> list[SymbolInfo]:
        raise NotImplementedError

    @abstractmethod
    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        raise NotImplementedError

    @abstractmethod
    async def healthcheck(self) -> ProviderHealth:
        raise NotImplementedError

