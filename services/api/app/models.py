from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str
    db: str
    redis: str
    collector: str
    chan: str
    server_time: str
    seed_data: bool
    data_source: str = "seed"
    data_note: str | None = None


class SymbolResponse(BaseModel):
    symbol: str
    code: str
    exchange: str
    name: str
    asset_type: str = "stock"


class SymbolsResponse(BaseModel):
    items: list[SymbolResponse]


class BarResponse(BaseModel):
    time: int = Field(description="Unix timestamp in seconds")
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float | None = None
    complete: bool
    revision: int


class BarsResponse(BaseModel):
    symbol: str
    timeframe: str
    bars: list[BarResponse]


class ChanPointResponse(BaseModel):
    time: int
    price: float
    base_ts: int | None = None
    base_seq: int | None = None


class ChanStrokeResponse(BaseModel):
    id: str
    level: str
    mode: str
    start: ChanPointResponse
    end: ChanPointResponse
    begin_base_ts: int | None = None
    end_base_ts: int | None = None
    begin_base_seq: int | None = None
    end_base_seq: int | None = None
    direction: str
    confirmed: bool


class ChanCenterResponse(BaseModel):
    id: str
    level: str
    mode: str
    start_time: int
    end_time: int
    begin_base_ts: int | None = None
    end_base_ts: int | None = None
    begin_base_seq: int | None = None
    end_base_seq: int | None = None
    low: float
    high: float
    confirmed: bool


class ChanSignalResponse(BaseModel):
    id: str
    level: str
    mode: str
    time: int
    base_ts: int | None = None
    base_seq: int | None = None
    price: float
    signal_type: str
    side: str | None = None
    bsp_type: str | None = None
    features: dict[str, float | int | str | bool | None] = Field(default_factory=dict)
    confirmed: bool


class ChanChannelResponse(BaseModel):
    id: str
    level: str
    mode: str
    time: int
    base_ts: int | None = None
    base_seq: int | None = None
    upper: float
    lower: float
    period: int | None = None
    confirmed: bool = True


class ChanOverlayResponse(BaseModel):
    symbol: str
    chart_timeframe: str
    levels: list[str]
    modes: list[str]
    snapshot_version: str = ""
    base_timeframe: str = "5f"
    base_ts_semantics: str = "bar_end"
    engine: str
    requested_bar_count: int
    bars_by_level: dict[str, int]
    strokes: list[ChanStrokeResponse]
    segments: list[ChanStrokeResponse]
    centers: list[ChanCenterResponse]
    signals: list[ChanSignalResponse]
    channels: list[ChanChannelResponse] = Field(default_factory=list)


class ChartBundleChanLevelResponse(BaseModel):
    bar_count: int
    strokes: list[ChanStrokeResponse]
    segments: list[ChanStrokeResponse]
    centers: list[ChanCenterResponse]
    signals: list[ChanSignalResponse]
    channels: list[ChanChannelResponse] = Field(default_factory=list)


class ChartBundleChanResponse(BaseModel):
    engine: str
    levels: dict[str, ChartBundleChanLevelResponse]


class ChartBundleSourceWatermarksResponse(BaseModel):
    canonical_5f_last_complete_end: int | None = None
    canonical_5f_last_seen_end: int | None = None
    view_last_complete_end: int | None = None
    analysis_generated_at: int
    analysis_source: str
    aggregation_source: str = "canonical-5f"
    imported_5f_through: int | None = None


class ChartBundleWarningResponse(BaseModel):
    code: str
    severity: str
    message: str


class ChartWindowRangeResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_time: int | None = Field(default=None, alias="from")
    to_time: int | None = Field(default=None, alias="to")
    limit: int


class ChartWindowResponse(BaseModel):
    schema_version: str = "chart-window.v1"
    snapshot_id: str
    symbol: str
    chart_timeframe: str
    range: ChartWindowRangeResponse
    bars: list[BarResponse]
    chan: ChanOverlayResponse


class ChartBundleResponse(BaseModel):
    schema_version: str = "chart-bundle.v2"
    snapshot_id: str
    symbol: str
    chart_timeframe: str
    range: ChartWindowRangeResponse
    bars: list[BarResponse]
    chan: ChanOverlayResponse


class ChartBundleV3Response(BaseModel):
    schema_version: str = "chart-bundle.v3"
    snapshot_id: str
    snapshot_version: str
    symbol: str
    chart_timeframe: str
    base_timeframe: str = "5f"
    bar_time_semantics: str = "bar_end"
    range: ChartWindowRangeResponse
    analysis_levels: list[str] = Field(default_factory=lambda: ["5f", "30f", "1d"])
    bars: list[BarResponse]
    chan: ChartBundleChanResponse
    source_watermarks: ChartBundleSourceWatermarksResponse
    warnings: list[ChartBundleWarningResponse] = Field(default_factory=list)


class LoginRequest(BaseModel):
    token: str = Field(min_length=1)


class LoginResponse(BaseModel):
    valid: bool
    role: str | None = None
    display_name: str | None = None
    label: str | None = None
    token_id: int | None = None


class AdminTokenResponse(BaseModel):
    id: int
    label: str
    display_name: str | None = None
    role: str
    is_active: bool
    created_at: datetime
    updated_at: datetime
    disabled_at: datetime | None = None
    last_used_at: datetime | None = None


class AdminTokenCreateRequest(BaseModel):
    label: str = Field(min_length=1, max_length=128)
    display_name: str | None = Field(default=None, max_length=128)


class AdminTokenCreateResponse(AdminTokenResponse):
    token: str


class AdminTokenListResponse(BaseModel):
    items: list[AdminTokenResponse]


class RuntimeConfigResponse(BaseModel):
    key: str
    value: Any
    version: int
    updated_at: datetime | None = None


class RuntimeConfigUpdateRequest(BaseModel):
    value: Any


class UserSettingResponse(BaseModel):
    bucket: str
    value: Any
    version: int
    updated_at: datetime | None = None


class UserSettingsResponse(BaseModel):
    items: list[UserSettingResponse]


class UserSettingUpdateRequest(BaseModel):
    value: Any


class ChanScreenerConditionResponse(BaseModel):
    level: str
    kind: str
    direction: str | None = None
    value: str | None = None
    raw: str


class ChanScreenerLevelStateResponse(BaseModel):
    level: str
    mode: str | None = None
    structure_state: str | None = None
    structure_direction: str | None = None
    latest_stroke_direction: str | None = None
    latest_segment_direction: str | None = None
    center_count: int = 0
    last_signal_type: str | None = None
    last_signal_side: str | None = None
    last_signal_bsp_type: str | None = None
    is_complete: bool | None = None
    asof: int | None = None
    source_bar_until: int | None = None


class ChanScreenerMarketResponse(BaseModel):
    price: float | None = None
    change_percent: float | None = None
    industry: str | None = None
    fund_net_inflow: float | None = None
    latest_bar_time: int | None = None


class ChanScreenerItemResponse(BaseModel):
    symbol: str
    code: str
    exchange: str
    name: str
    states: dict[str, ChanScreenerLevelStateResponse] = Field(default_factory=dict)
    trend_status: dict[str, str | None] = Field(default_factory=dict)
    stroke_states: dict[str, str | None] = Field(default_factory=dict)
    segment_states: dict[str, str | None] = Field(default_factory=dict)
    market: ChanScreenerMarketResponse = Field(default_factory=ChanScreenerMarketResponse)


class ChanScreenerResponse(BaseModel):
    query: str
    mode: str
    parser: str = "rules"
    parser_error: str | None = None
    conditions: list[ChanScreenerConditionResponse]
    unsupported: list[str] = Field(default_factory=list)
    items: list[ChanScreenerItemResponse]
