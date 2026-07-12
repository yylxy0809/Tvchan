from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from collector.market_data.news import RawNewsItem, normalize_news


NOW = datetime(2026, 7, 12, 2, 0, tzinfo=UTC)


def raw(id_: str | None, title: str, url: str, hour: int, source: str = "财经网") -> RawNewsItem:
    return RawNewsItem(
        provider_id=id_,
        title=title,
        url=url,
        published_at=datetime(2026, 7, 12, hour, tzinfo=UTC),
        summary=title,
        source_name=source,
        category="announcement",
        entities=("平安银行",),
        query_kind="company",
    )


def test_normalization_deduplicates_provider_id_and_canonical_url() -> None:
    events = normalize_news(
        [
            raw("same", "公告一", "https://example.cn/a?utm_source=x", 1),
            raw("same", "公告一", "https://mirror.cn/a", 1),
            raw(None, "公告二", "HTTPS://EXAMPLE.CN/b/?spm=1", 0),
            raw(None, "公告二", "https://example.cn/b", 0),
        ],
        symbol="000001.SZ",
        now=NOW,
    )

    assert len(events) == 2


def test_normalization_clusters_similar_reprints_and_keeps_sources() -> None:
    events = normalize_news(
        [
            raw("1", "平安银行发布2026年度业绩公告", "https://exchange.cn/1", 1, "交易所"),
            raw("2", "平安银行发布2026年度业绩公告。", "https://media.cn/2", 1, "财经网"),
        ],
        symbol="000001.SZ",
        now=NOW,
    )

    assert len(events) == 1
    assert {source.name for source in events[0].sources} == {"交易所", "财经网"}


def test_normalization_ranks_risk_and_authoritative_recent_events_deterministically() -> None:
    risk = raw("risk", "平安银行收到监管处罚", "https://exchange.cn/r", 1, "交易所")
    risk = replace(risk, category="risk", query_kind="risk")
    ordinary = raw("ordinary", "银行行业动态", "https://blog.cn/o", 1, "博客")

    events = normalize_news([ordinary, risk], symbol="000001.SZ", now=NOW, limit=5)

    assert [event.title for event in events] == [risk.title, ordinary.title]
    assert all(event.event_id.startswith("sha256:") for event in events)
