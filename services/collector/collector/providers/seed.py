from __future__ import annotations

from datetime import datetime

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from trading_protocol import Bar, SymbolInfo


class SeedProvider(MarketDataProvider):
    name = "seed"

    async def list_symbols(self) -> list[SymbolInfo]:
        from app.repositories.bars import SEED_SYMBOLS

        return SEED_SYMBOLS

    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        from app.repositories.bars import generate_seed_bars

        return generate_seed_bars(symbol, timeframe, start=start, end=end, limit=limit)

    async def healthcheck(self) -> ProviderHealth:
        return ProviderHealth(name=self.name, ok=True, message="seed provider ready")

