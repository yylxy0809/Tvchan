from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Generic, TypeVar


IWENCAI_SOURCE = "iwencai"
NOTTE_SOURCE = "notte"
EXTERNAL_SOURCES = frozenset({IWENCAI_SOURCE, NOTTE_SOURCE})


class Freshness(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


class ProviderError(StrEnum):
    AUTHENTICATION = "authentication"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    SCHEMA = "schema_error"
    UNAVAILABLE = "unavailable"


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class MarketDataMetadata:
    source: str = IWENCAI_SOURCE
    trading_date: date | None = None
    snapshot_version: str | None = None
    provider_ts: datetime | None = None
    received_at: datetime = field(default_factory=_now)
    freshness: Freshness = Freshness.UNAVAILABLE

    def __post_init__(self) -> None:
        if self.source not in EXTERNAL_SOURCES:
            raise ValueError("sidebar external data source must be iwencai or notte")
        for value in (self.provider_ts, self.received_at):
            if value is not None and value.tzinfo is None:
                raise ValueError("market data timestamps must be timezone-aware")
        if self.freshness is Freshness.FRESH and self.trading_date is None:
            raise ValueError("fresh snapshots require trading_date")


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class MarketDataResult(Generic[T]):
    value: T | None
    metadata: MarketDataMetadata
    error: ProviderError | None = None

    @classmethod
    def available(cls, value: T, *, trading_date: date, provider_ts: datetime | None = None, snapshot_version: str | None = None, source: str = IWENCAI_SOURCE) -> MarketDataResult[T]:
        return cls(value, MarketDataMetadata(source=source, trading_date=trading_date, provider_ts=provider_ts, snapshot_version=snapshot_version, freshness=Freshness.FRESH))

    @classmethod
    def unavailable(cls, *, error: ProviderError = ProviderError.UNAVAILABLE, trading_date: date | None = None, source: str = IWENCAI_SOURCE) -> MarketDataResult[T]:
        return cls(None, MarketDataMetadata(source=source, trading_date=trading_date), error)

    def as_stale(self, error: ProviderError = ProviderError.UNAVAILABLE) -> MarketDataResult[T]:
        return replace(self, metadata=replace(self.metadata, freshness=Freshness.STALE), error=error)


@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    price: float | None = None
    change: float | None = None
    change_percent: float | None = None
    volume: float | None = None
    amount: float | None = None
    turnover_rate: float | None = None


@dataclass(frozen=True, slots=True)
class Profile:
    symbol: str
    name: str | None = None
    exchange: str | None = None
    industry: str | None = None
    description: str | None = None
    market_cap: float | None = None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    turnover_rate: float | None = None


@dataclass(frozen=True, slots=True)
class Valuation:
    symbol: str
    market_cap: float | None = None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    ps_ratio: float | None = None


@dataclass(frozen=True, slots=True)
class Themes:
    symbol: str
    industry: str | None = None
    concepts: tuple[str, ...] = ()
    business_summary: str | None = None


@dataclass(frozen=True, slots=True)
class CapitalFlow:
    symbol: str
    net_inflow: float | None = None
    main_net_inflow: float | None = None
    large_net_inflow: float | None = None
    medium_net_inflow: float | None = None
    small_net_inflow: float | None = None


@dataclass(frozen=True, slots=True)
class MarketLeaderDetail:
    name: str
    change_percent: float | None = None


@dataclass(frozen=True, slots=True)
class MarketThemeDetail:
    name: str
    change_percent: float | None = None
    main_net_inflow_wan: float | None = None


@dataclass(frozen=True, slots=True)
class MarketStrength:
    score: float | None = None
    leaders: tuple[str, ...] = ()
    themes: tuple[str, ...] = ()
    up_count: int | None = None
    down_count: int | None = None
    limit_up_count: int | None = None
    limit_down_count: int | None = None
    index_level: float | None = None
    index_change_percent: float | None = None
    leader_details: tuple[MarketLeaderDetail, ...] = ()
    theme_details: tuple[MarketThemeDetail, ...] = ()


@dataclass(frozen=True, slots=True)
class NewsSource:
    name: str
    url: str


@dataclass(frozen=True, slots=True)
class NewsItem:
    event_id: str
    symbol: str
    category: str
    title: str
    fact_summary: str
    published_at: datetime
    first_seen_at: datetime
    source: str = IWENCAI_SOURCE
    sources: tuple[NewsSource, ...] = ()
    impact_tags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SidebarContext:
    chart_symbol: str
    chart_epoch: int
    watchlist_symbols: tuple[str, ...] = ()
    watchlist_revision: int = 0

    def __post_init__(self) -> None:
        if not self.chart_symbol or self.chart_epoch < 0 or self.watchlist_revision < 0:
            raise ValueError("invalid sidebar context")
        object.__setattr__(self, "watchlist_symbols", tuple(dict.fromkeys(self.watchlist_symbols)))


@dataclass(frozen=True, slots=True)
class MarketDataSnapshot:
    context: SidebarContext
    active_quote: MarketDataResult[Quote]
    active_profile: MarketDataResult[Profile]
    valuation: MarketDataResult[Valuation]
    themes: MarketDataResult[Themes]
    watchlist_quotes: dict[str, MarketDataResult[Quote]]
    capital_flow: MarketDataResult[CapitalFlow]
    market_strength: MarketDataResult[MarketStrength]
    news: MarketDataResult[tuple[NewsItem, ...]]
