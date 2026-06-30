from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    code: str
    exchange: str
    name: str
    asset_type: str = "stock"
    market: str = "A_SHARE"
    is_active: bool = True

