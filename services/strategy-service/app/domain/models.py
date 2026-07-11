from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.domain.enums import ScanStatus


@dataclass(slots=True)
class SymbolInfo:
    symbol_id: int
    symbol: str
    code: str
    exchange: str
    name: str


@dataclass(slots=True)
class GateOutcome:
    name: str
    passed: bool
    reason: str | None = None
    features: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PublishedHead:
    run_id: int
    snapshot_version: str
    bar_from: datetime | None
    bar_until: datetime
    published_at: datetime | None


@dataclass(slots=True)
class ChanSignal:
    signal_id: int | None
    level: str
    mode: str
    point_time: datetime
    base_time: datetime
    base_seq: int | None
    price: float
    signal_type: str
    side: str | None
    bsp_type: str | None
    confirmed: bool
    features: dict[str, Any] = field(default_factory=dict)
    run_id: int | None = None
    snapshot_version: str | None = None


@dataclass(slots=True)
class ChanStroke:
    seq: int
    level: str
    mode: str
    direction: str
    start_time: datetime
    end_time: datetime
    start_price: float
    end_price: float
    begin_base_time: datetime
    end_base_time: datetime
    confirmed: bool


@dataclass(slots=True)
class ChanCenter:
    seq: int
    level: str
    mode: str
    start_time: datetime
    end_time: datetime
    low: float
    high: float
    confirmed: bool
    begin_base_time: datetime | None = None
    end_base_time: datetime | None = None


@dataclass(slots=True)
class WeeklyContext:
    weekly_b1: ChanSignal | None
    weekly_b2: ChanSignal
    weekly_bsp_type: str
    context_mode: str
    context_score: float
    anchor_time: datetime
    anchor_source: str
    stop_reference_price: float
    stop_reference_source: str
    prior_weekly_b1_found: bool
    same_bar_with_b1: bool
    same_price_with_b1: bool
    dif: float
    dea: float
    latest_close: float
    is_active: bool
    failure_reason: str | None = None


@dataclass(slots=True)
class DailySetup:
    daily_b1: ChanSignal
    daily_b2: ChanSignal | None
    daily_b2s: ChanSignal | None
    previous_down_stroke: ChanStroke
    first_up_stroke: ChanStroke
    center_low: float | None
    center_high: float | None
    center_type: str | None
    structure_score: float
    location_score: float
    momentum_score: float
    strength_score: float
    features: dict[str, Any]


@dataclass(slots=True)
class EntryEvaluation:
    confidence_score: float
    has_30f_b1: bool
    thirty_b1: ChanSignal | None
    five_b2_confirm: ChanSignal | None
    daily_bottom_time: datetime | None
    entry_level: str | None
    reasons: dict[str, Any]


@dataclass(slots=True)
class ExitDecision:
    should_exit: bool
    reason: str | None = None
    signal_time: datetime | None = None
    execution_timeframe: str | None = None


@dataclass(slots=True)
class ScanResult:
    status: ScanStatus
    symbol: SymbolInfo
    as_of_time: datetime
    weekly_context: WeeklyContext
    daily_setup: DailySetup
    entry: EntryEvaluation
    failed_gate: str | None = None


@dataclass(slots=True)
class ScanDiagnosis:
    symbol: SymbolInfo
    as_of_time: datetime
    strategy_code: str
    market_cap: float | None
    heads: dict[str, PublishedHead | None]
    weekly_signals: list[ChanSignal]
    daily_signals: list[ChanSignal]
    gates: list[GateOutcome]
    weekly_context: WeeklyContext | None = None
    daily_setup: DailySetup | None = None
    entry: EntryEvaluation | None = None
    result: ScanResult | None = None

    @property
    def failed_gate(self) -> str | None:
        for gate in self.gates:
            if not gate.passed:
                return gate.name
        return None

    @property
    def failed_reason(self) -> str | None:
        for gate in self.gates:
            if not gate.passed:
                return gate.reason
        return None


@dataclass(slots=True)
class Trade:
    symbol: SymbolInfo
    entry_time: datetime
    entry_price: float
    entry_reason: str
    entry_confidence: float
    entry_level: str
    daily_b1_price: float
    stop_price: float
    features: dict[str, Any]
    exit_time: datetime | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0
    holding_bars: int = 0
    holding_days: int = 0

    @property
    def is_open(self) -> bool:
        return self.exit_time is None

    @property
    def return_pct(self) -> float | None:
        if self.exit_price is None:
            return None
        return (self.exit_price - self.entry_price) / self.entry_price
