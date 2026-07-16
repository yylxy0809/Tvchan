from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .contracts import (
    Freshness,
    MarketDataResult,
    NewsItem,
    NewsSource as ContractNewsSource,
)


class NewsStatus(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class NewsSource:
    name: str
    url: str


@dataclass(frozen=True, slots=True)
class RawNewsItem:
    provider_id: str | None
    title: str
    url: str
    published_at: datetime
    summary: str
    source_name: str
    category: str
    entities: tuple[str, ...]
    query_kind: str


@dataclass(frozen=True, slots=True)
class NewsEvent:
    event_id: str
    symbol: str
    category: str
    title: str
    fact_summary: str
    published_at: datetime
    sources: tuple[NewsSource, ...]
    impact_tags: tuple[str, ...]
    score: float


@dataclass(frozen=True, slots=True)
class NewsFeed:
    chart_symbol: str
    chart_epoch: int
    status: NewsStatus
    events: tuple[NewsEvent, ...] = ()
    updated_at: datetime | None = None
    error: str | None = None
    source: str = "iwencai_news_search"


def feed_to_market_data_result(
    feed: NewsFeed, *, since: datetime | None = None
) -> MarketDataResult[tuple[NewsItem, ...]]:
    if feed.status is NewsStatus.UNAVAILABLE:
        return MarketDataResult.unavailable(source=feed.source, error=feed.error)

    first_seen_at = feed.updated_at
    if first_seen_at is None:
        raise ValueError("available news feed requires updated_at")
    events = tuple(event for event in feed.events if since is None or event.published_at >= since)
    items = tuple(
        NewsItem(
            event_id=event.event_id,
            symbol=event.symbol,
            category=event.category,
            title=event.title,
            fact_summary=event.fact_summary,
            published_at=event.published_at,
            first_seen_at=first_seen_at,
            source=feed.source,
            sources=tuple(ContractNewsSource(source.name, source.url) for source in event.sources),
            impact_tags=event.impact_tags,
        )
        for event in events
    )
    freshness = Freshness.STALE if feed.status is NewsStatus.STALE else Freshness.LIVE
    provider_ts = max((item.published_at for item in items), default=None)
    result = MarketDataResult.available(
        items,
        source=feed.source,
        freshness=freshness,
        provider_ts=provider_ts,
        received_at=first_seen_at,
    )
    if feed.error is None:
        return result
    return MarketDataResult(value=result.value, metadata=result.metadata, error=feed.error)


_TRACKING_KEYS = {"spm", "from", "source"}
_TITLE_NOISE = re.compile(r"[\W_]+", re.UNICODE)
_CATEGORY_WEIGHT = {"risk": 1.5, "announcement": 1.4, "policy": 1.3, "company": 1.2, "industry": 1.0}
_SOURCE_WEIGHT = {"交易所": 1.5, "公司公告": 1.5, "巨潮资讯": 1.45, "新华社": 1.4}


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in _TRACKING_KEYS
    ]
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, urlencode(query), ""))


def _normalized_title(title: str) -> str:
    return _TITLE_NOISE.sub("", title).lower()


def _deduplicate(items: list[RawNewsItem]) -> list[RawNewsItem]:
    result: list[RawNewsItem] = []
    ids: set[str] = set()
    urls: set[str] = set()
    for item in items:
        url = canonicalize_url(item.url)
        if (item.provider_id and item.provider_id in ids) or url in urls:
            continue
        if item.provider_id:
            ids.add(item.provider_id)
        urls.add(url)
        result.append(item)
    return result


def _same_event(left: RawNewsItem, right: RawNewsItem) -> bool:
    if abs((left.published_at - right.published_at).total_seconds()) > 6 * 3600:
        return False
    if not set(left.entities).intersection(right.entities):
        return False
    a, b = _normalized_title(left.title), _normalized_title(right.title)
    return a == b or (min(len(a), len(b)) >= 12 and (a in b or b in a))


def normalize_news(
    items: list[RawNewsItem], *, symbol: str, now: datetime, limit: int = 20
) -> tuple[NewsEvent, ...]:
    groups: list[list[RawNewsItem]] = []
    for item in _deduplicate(items):
        if item.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")
        group = next((group for group in groups if _same_event(group[0], item)), None)
        if group is None:
            groups.append([item])
        else:
            group.append(item)

    events: list[NewsEvent] = []
    for group in groups:
        primary = max(group, key=lambda value: _SOURCE_WEIGHT.get(value.source_name, 1.0))
        source_map = {
            canonicalize_url(value.url): NewsSource(value.source_name, canonicalize_url(value.url))
            for value in group
        }
        age_hours = max(0.0, (now - primary.published_at).total_seconds() / 3600)
        score = (
            _CATEGORY_WEIGHT.get(primary.category, 1.0)
            * _SOURCE_WEIGHT.get(primary.source_name, 1.0)
            / (1.0 + age_hours / 24.0)
        )
        identity = primary.provider_id or "|".join(
            (_normalized_title(primary.title), primary.published_at.strftime("%Y%m%d%H"), symbol)
        )
        events.append(
            NewsEvent(
                event_id="sha256:" + hashlib.sha256(identity.encode("utf-8")).hexdigest(),
                symbol=symbol,
                category=primary.category,
                title=primary.title,
                fact_summary=primary.summary or primary.title,
                published_at=max(value.published_at for value in group),
                sources=tuple(source_map.values()),
                impact_tags=tuple(dict.fromkeys((*primary.entities, primary.category))),
                score=score,
            )
        )
    return tuple(sorted(events, key=lambda event: (-event.score, -event.published_at.timestamp(), event.event_id))[:limit])
