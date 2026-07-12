from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


SYMBOL_PATTERN = re.compile(r"^\d{6}\.(?:SH|SZ|BJ)$")


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("symbol must use the 000001.SZ form")
    return symbol


class BootstrapRequest(BaseModel):
    chart_symbol: str
    chart_epoch: int = Field(ge=0)
    watchlist_id: str = "default"
    watchlist_revision: int = Field(default=0, ge=0)
    watchlist_symbols: list[str] = Field(default_factory=list, max_length=500)

    @field_validator("chart_symbol")
    @classmethod
    def validate_chart_symbol(cls, value: str) -> str:
        return normalize_symbol(value)

    @field_validator("watchlist_symbols")
    @classmethod
    def validate_watchlist_symbols(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(normalize_symbol(value) for value in values))


class SetSidebarContext(BaseModel):
    type: Literal["set_sidebar_context"]
    subscription_id: str = Field(min_length=1, max_length=128)
    chart_symbol: str
    chart_epoch: int = Field(ge=0)
    watchlist_id: str = "default"
    watchlist_revision: int = Field(default=0, ge=0)
    watchlist_symbols: list[str] = Field(default_factory=list, max_length=500)
    channels: list[
        Literal["watchlist_quotes", "active_profile", "strength", "news"]
    ] = Field(default_factory=list)
    after_sequence: int = Field(default=0, ge=0)
    snapshot_version: int = Field(default=0, ge=0)

    @field_validator("chart_symbol")
    @classmethod
    def validate_chart_symbol(cls, value: str) -> str:
        return normalize_symbol(value)

    @field_validator("watchlist_symbols")
    @classmethod
    def validate_watchlist_symbols(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(normalize_symbol(value) for value in values))


JsonObject = dict[str, Any]
