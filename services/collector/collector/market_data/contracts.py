from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Generic, TypeVar


class Freshness(StrEnum):
    LIVE = "live"
    DELAYED = "delayed"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class MarketDataMetadata:
    source: str
    provider_version: str | None = None
    provider_ts: datetime | None = None
    received_at: datetime = field(default_factory=_now)
    freshness: Freshness = Freshness.UNAVAILABLE

    def __post_init__(self) -> None:
        for value in (self.provider_ts, self.received_at):
            if value is not None and value.tzinfo is None:
                raise ValueError("market data timestamps must be timezone-aware")


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class MarketDataResult(Generic[T]):
    value: T | None
    metadata: MarketDataMetadata
    error: str | None = None

    @classmethod
    def available(
        cls,
        value: T,
        *,
        source: str,
        freshness: Freshness = Freshness.LIVE,
        provider_version: str | None = None,
        provider_ts: datetime | None = None,
        received_at: datetime | None = None,
    ) -> MarketDataResult[T]:
        return cls(
            value=value,
            metadata=MarketDataMetadata(
                source=source,
                provider_version=provider_version,
                provider_ts=provider_ts,
                received_at=received_at or _now(),
                freshness=freshness,
            ),
        )

    @classmethod
    def unavailable(cls, *, source: str, error: str | None = None) -> MarketDataResult[T]:
        return cls(
            value=None,
            metadata=MarketDataMetadata(source=source, freshness=Freshness.UNAVAILABLE),
            error=error,
        )

    def as_stale(self, error: str | None = None) -> MarketDataResult[T]:
        return replace(
            self,
            metadata=replace(self.metadata, freshness=Freshness.STALE),
            error=error,
        )


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
class CapitalFlow:
    symbol: str
    net_inflow: float | None = None
    main_net_inflow: float | None = None
    large_net_inflow: float | None = None
    medium_net_inflow: float | None = None
    small_net_inflow: float | None = None


@dataclass(frozen=True, slots=True)
class StrengthLeader:
    name: str
    change_percent: float | None = None


@dataclass(frozen=True, slots=True)
class StrengthTheme:
    name: str
    change_percent: float | None = None
    main_net_inflow_wan: float | None = None


@dataclass(frozen=True, slots=True)
class MarketStrength:
    score: float | None = None
    leaders: tuple[str, ...] = ()
    themes: tuple[str, ...] = ()
    leader_details: tuple[StrengthLeader, ...] = ()
    theme_details: tuple[StrengthTheme, ...] = ()


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
    source: str
    sources: tuple[NewsSource, ...] = ()
    impact_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.published_at.tzinfo is None or self.first_seen_at.tzinfo is None:
            raise ValueError("news timestamp must be timezone-aware")


@dataclass(frozen=True, slots=True)
class SidebarContext:
    chart_symbol: str
    chart_epoch: int
    watchlist_symbols: tuple[str, ...] = ()
    watchlist_revision: int = 0

    def __post_init__(self) -> None:
        if not self.chart_symbol:
            raise ValueError("chart_symbol is required")
        if self.chart_epoch < 0 or self.watchlist_revision < 0:
            raise ValueError("context revisions cannot be negative")
        object.__setattr__(self, "watchlist_symbols", tuple(dict.fromkeys(self.watchlist_symbols)))


@dataclass(frozen=True, slots=True)
class MarketDataSnapshot:
    context: SidebarContext
    active_quote: MarketDataResult[Quote]
    active_profile: MarketDataResult[Profile]
    watchlist_quotes: dict[str, MarketDataResult[Quote]]
    capital_flow: MarketDataResult[CapitalFlow]
    market_strength: MarketDataResult[MarketStrength]
    news: MarketDataResult[tuple[NewsItem, ...]]
