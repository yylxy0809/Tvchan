from __future__ import annotations

from pydantic import BaseModel, Field


class ChanBar(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


class ChanAnalyzeRequest(BaseModel):
    symbol: str
    timeframe: str
    chan_levels: list[str] = Field(default_factory=lambda: ["5f", "30f", "1d"])
    modes: list[str] = Field(default_factory=lambda: ["confirmed", "predictive"])
    bars: list[ChanBar]


class ChanPoint(BaseModel):
    time: int
    price: float
    base_ts: int | None = None
    base_seq: int | None = None


class ChanStroke(BaseModel):
    id: str
    level: str
    mode: str
    start: ChanPoint
    end: ChanPoint
    begin_base_ts: int | None = None
    end_base_ts: int | None = None
    begin_base_seq: int | None = None
    end_base_seq: int | None = None
    direction: str
    confirmed: bool


class ChanCenter(BaseModel):
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


class ChanSignal(BaseModel):
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


class ChanChannel(BaseModel):
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


class ChanAnalyzeResponse(BaseModel):
    symbol: str
    timeframe: str
    snapshot_version: str = ""
    base_timeframe: str = "5f"
    base_ts_semantics: str = "bar_end"
    engine: str = "module-b:chan.py"
    strokes: list[ChanStroke]
    segments: list[ChanStroke]
    centers: list[ChanCenter]
    signals: list[ChanSignal]
    channels: list[ChanChannel] = Field(default_factory=list)
