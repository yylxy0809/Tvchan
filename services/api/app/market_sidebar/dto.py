from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
        Literal["watchlist_quotes", "active_profile", "strength", "news", "chan_strategy"]
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


class IwencaiDomain(BaseModel):
    """Canonical flat metadata shared by every external sidebar domain."""

    model_config = ConfigDict(extra="allow")
    source: Literal["iwencai", "notte"]
    freshness: Literal["fresh", "stale", "unavailable"]
    as_of: str
    trading_date: str


class ChanStrokeState(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    level: Literal["5f", "30f", "1d"]
    label: str
    direction: Literal["up", "down", "unknown"]
    state_label: str = Field(alias="stateLabel")
    mode: Literal["confirmed", "predictive"] | None
    mode_label: str = Field(alias="modeLabel")
    confirmed: bool | None
    anchor_time: int | None = Field(alias="anchorTime")
    anchor_price: float | None = Field(alias="anchorPrice")


class LocalChanState(BaseModel):
    source: Literal["local_db"]
    stroke_states: list[ChanStrokeState]


class LocalStrategySignal(BaseModel):
    key: str
    label: str
    value: str
    tone: Literal["up", "down", "neutral"]
    source: Literal["local_db"]


class ActiveProfile(IwencaiDomain):
    symbol: str
    quote: IwencaiDomain
    identity: IwencaiDomain
    valuation: IwencaiDomain
    capital_flow: IwencaiDomain
    themes: list[Any]
    chan_state: LocalChanState
    strategy_signals: list[LocalStrategySignal]


class SidebarBootstrapResponse(BaseModel):
    context: JsonObject
    watchlist_quotes: dict[str, IwencaiDomain]
    active_symbol_profile: ActiveProfile
    strongest_preview: IwencaiDomain
    news_preview: IwencaiDomain
    snapshot_version: int
    sequence: int
