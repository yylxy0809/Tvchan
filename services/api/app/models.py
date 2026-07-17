from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str
    db: str
    redis: str
    collector: str
    module_c: dict[str, Any] = Field(default_factory=dict)
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
    seq: int | None = None
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
    seq: int | None = None
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
    seq: int | None = None
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


class ModuleCExecutionTaskResponse(BaseModel):
    chan_level: int
    status: str
    count: int
    attempts: int
    bars: int
    strokes: int
    segments: int
    centers: int
    signals: int
    latest_update: datetime | None = None


class ModuleCExecutionProgressResponse(BaseModel):
    shard_count: int
    active_symbols: int
    disposition_rows: int
    latest_task_update: datetime | None = None
    retryable_failed: int | None = None
    exhausted_failed: int | None = None
    expired_leases: int
    tasks: list[ModuleCExecutionTaskResponse] = Field(default_factory=list)


class ModuleCFrozenConfigResponse(BaseModel):
    contract: str | None = None
    levels: list[str] = Field(default_factory=list)
    modes: list[str] = Field(default_factory=list)
    concurrency_per_worker: int | None = None
    shard_count: int | None = None
    max_attempts: int | None = None
    eligibility_build_id: str | None = None


class ModuleCFreshnessExpectedResponse(BaseModel):
    timeframe: str
    expected: datetime | None = None


class ModuleCFreshnessActualResponse(ModuleCFreshnessExpectedResponse):
    actual_min: datetime | None = None
    actual_max: datetime | None = None
    empty_scopes: int
    stale_scopes: int
    future_scopes: int


class ModuleCFreshnessResponse(BaseModel):
    as_of: datetime | None = None
    status: Literal["current", "stale", "unavailable"]
    reasons: list[str] = Field(default_factory=list)
    expected_closed_watermarks: list[ModuleCFreshnessExpectedResponse]
    actual_checkpoint_watermarks: list[ModuleCFreshnessActualResponse]


class ModuleCExecutionProvenanceResponse(BaseModel):
    policy: str | None = None
    eligibility_build_id: str | None = None
    manifest_version: str | None = None
    eligibility_manifest_sha256: str | None = None
    build_manifest_sha256: str | None = None
    canonical_audit_run_id: str | None = None
    audit_evidence_sha256: str | None = None
    audit_checkpoint_sha256: str | None = None
    audit_status: str | None = None
    audit_apply_mode: bool | None = None
    audit_gate_pass: bool | None = None
    freshness_contract_version: str | None = None
    freshness_contract_sha256: str | None = None
    catalog_generation_id: str | None = None
    catalog_control_revision: int | None = None
    catalog_manifest_sha256: str | None = None
    audit_active_universe_sha256: str | None = None
    catalog_generation_status: str | None = None
    catalog_is_active: bool
    live_catalog_control_revision: int | None = None
    catalog_revision_matches: bool
    eligibility_manifest_matches: bool
    config_hash_matches: bool
    execution_identity_matches: bool
    frozen_config_matches: bool
    live_universe_matches: bool
    catalog_manifest_matches: bool
    evidence_complete: bool
    drift_reasons: list[str] = Field(default_factory=list)


class ModuleCExecutionBatchResponse(BaseModel):
    batch_id: int
    batch_key: str
    batch_kind: str
    parent_status: str
    child_status: str
    publication_namespace: str
    profile_id: str
    run_group_id: str
    code_commit: str
    image_digest: str
    vendor_manifest_sha256: str
    config_hash: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime
    execution: ModuleCExecutionProgressResponse
    frozen_config: ModuleCFrozenConfigResponse
    freshness: ModuleCFreshnessResponse
    provenance: ModuleCExecutionProvenanceResponse


class ModuleCExecutionStatusResponse(BaseModel):
    observed_at: datetime
    readonly: Literal[True]
    running_parent_batches: int
    running_child_batches: int
    running_tasks: int
    batch: ModuleCExecutionBatchResponse | None = None


class RuntimeConfigResponse(BaseModel):
    key: str
    value: Any
    version: int
    updated_at: datetime | None = None


class RuntimeConfigUpdateRequest(BaseModel):
    value: Any


class WencaiApiKeyConfig(BaseModel):
    label: str = Field(default="default", min_length=1, max_length=128)
    key: str = ""
    enabled: bool = True
    priority: int = 0


class WencaiConfigResponse(BaseModel):
    base_url: str = "https://openapi.iwencai.com"
    api_key: str = ""
    cookie: str = ""
    user_agent: str | None = None
    pro: bool = False
    timeout_seconds: float = 20
    config_version: int = 0
    api_keys: list[WencaiApiKeyConfig] = Field(default_factory=list)


class WencaiConfigUpdateRequest(BaseModel):
    base_url: str | None = None
    api_key: str | None = None
    cookie: str | None = None
    user_agent: str | None = None
    pro: bool = False
    timeout_seconds: float = 20
    api_keys: list[WencaiApiKeyConfig] | None = None


class ConnectivityTestResponse(BaseModel):
    ok: bool
    latency_ms: int
    message: str
    sample_count: int = 0
    capability: str = "screener"
    source: str = "iwencai"
    error_class: str | None = None


class LlmProviderResponse(BaseModel):
    id: str
    name: str
    base_url: str
    api_key: str = ""
    models: list[str] = Field(default_factory=list)
    active_model: str = ""
    enabled: bool = True
    timeout_seconds: float = 20


class LlmProvidersResponse(BaseModel):
    active_provider_id: str | None = None
    providers: list[LlmProviderResponse] = Field(default_factory=list)


class LlmProviderTestResponse(BaseModel):
    ok: bool
    latency_ms: int
    provider: str
    model: str
    message: str


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


class WencaiScreenerItemResponse(BaseModel):
    symbol: str
    code: str
    exchange: str
    name: str
    price: float | None = None
    change_percent: float | None = None
    buy_signal: str = ""
    technical_shape: str = ""
    reason: str = ""
    high_break_reason: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


class WencaiScreenerResponse(BaseModel):
    query: str
    total: int
    page: int
    page_size: int
    source: str = "wencai"
    fetched_at: datetime
    conditions: list[str] = Field(default_factory=list)
    suggestions: list[str] = Field(default_factory=list)
    items: list[WencaiScreenerItemResponse]
