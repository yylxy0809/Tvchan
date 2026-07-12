from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest

from collector.market_data.iwencai import (
    HttpxNewsTransport,
    IwencaiConfig,
    IwencaiNewsAdapter,
    NewsRequest,
)
from collector.market_data.contracts import Freshness, NewsItem
from collector.market_data.news import NewsStatus


NOW = datetime(2026, 7, 12, 2, 0, tzinfo=UTC)


class RecordingTransport:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.requests: list[NewsRequest] = []

    async def search(self, request: NewsRequest) -> object:
        self.requests.append(request)
        response = self.responses[min(len(self.requests) - 1, len(self.responses) - 1)]
        if isinstance(response, Exception):
            raise response
        return response


def item(id_: str, title: str, url: str, published_at: str) -> dict[str, object]:
    return {
        "id": id_,
        "title": title,
        "url": url,
        "published_at": published_at,
        "summary": title,
        "source_name": "交易所",
        "category": "announcement",
        "entities": ["平安银行"],
    }


def test_adapter_issues_exactly_three_fixed_searches_with_chart_context() -> None:
    transport = RecordingTransport([{"items": []}, {"items": []}, {"items": []}])
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="test-secret"), transport)

    feed = asyncio.run(
        adapter.get_feed(
            chart_symbol="000001.SZ",
            chart_epoch=18,
            security_name="平安银行",
            industry="银行",
            now=NOW,
        )
    )

    assert feed.status is NewsStatus.FRESH
    assert [(r.chart_symbol, r.chart_epoch) for r in transport.requests] == [
        ("000001.SZ", 18),
        ("000001.SZ", 18),
        ("000001.SZ", 18),
    ]
    assert [r.query for r in transport.requests] == [
        "平安银行 000001.SZ 最新公告 业务进展 业绩 重大事项",
        "银行 最新政策 行业动态 产业链变化",
        "平安银行 监管 问询 处罚 诉讼 减持 风险",
    ]


def test_adapter_rejects_invalid_master_data_before_transport() -> None:
    transport = RecordingTransport([])
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="secret"), transport)

    with pytest.raises(ValueError, match="chart_symbol"):
        asyncio.run(
            adapter.get_feed("000001.SZ;drop", 1, "平安银行", "银行", now=NOW)
        )
    assert transport.requests == []


def test_adapter_rejects_non_http_result_url_as_schema_error() -> None:
    transport = RecordingTransport(
        [{"items": [item("1", "恶意链接", "javascript:alert(1)", "2026-07-12T09:00:00+08:00")]}]
    )
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="placeholder"), transport)

    feed = asyncio.run(adapter.get_feed("000001.SZ", 1, "平安银行", "银行", now=NOW))

    assert feed.status is NewsStatus.UNAVAILABLE
    assert feed.error == "schema_error"


@pytest.mark.parametrize(
    ("response", "error"),
    [
        ({"wrong": []}, "schema_error"),
        ({"items": [{"id": "x"}]}, "schema_error"),
        (httpx.ReadTimeout("secret-value"), "timeout"),
        (httpx.HTTPStatusError("secret-value", request=httpx.Request("GET", "https://x"), response=httpx.Response(429)), "rate_limited"),
        (httpx.HTTPStatusError("secret-value", request=httpx.Request("GET", "https://x"), response=httpx.Response(401)), "authentication"),
    ],
)
def test_adapter_classifies_failures_without_leaking_key_or_raw_error(
    response: object, error: str
) -> None:
    transport = RecordingTransport([response])
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="secret-value"), transport)

    feed = asyncio.run(adapter.get_feed("000001.SZ", 1, "平安银行", "银行", now=NOW))

    assert feed.status is NewsStatus.UNAVAILABLE
    assert feed.error == error
    assert "secret-value" not in repr(feed)


def test_failed_refresh_returns_last_success_as_stale_for_same_symbol() -> None:
    transport = RecordingTransport(
        [
            {"items": [item("1", "平安银行发布公告", "https://example.cn/a", "2026-07-12T09:00:00+08:00")]},
            {"items": []},
            {"items": []},
            httpx.ReadTimeout("timeout"),
        ]
    )
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="secret"), transport)
    first = asyncio.run(adapter.get_feed("000001.SZ", 1, "平安银行", "银行", now=NOW))
    second = asyncio.run(adapter.get_feed("000001.SZ", 2, "平安银行", "银行", now=NOW))

    assert first.status is NewsStatus.FRESH
    assert second.status is NewsStatus.STALE
    assert second.chart_epoch == 2
    assert second.events == first.events
    assert second.error == "timeout"


def test_standard_get_news_resolves_master_data_and_maps_contract_dto() -> None:
    transport = RecordingTransport(
        [
            {"items": [item("1", "平安银行发布公告", "https://example.cn/a", "2026-07-12T09:00:00+08:00")]},
            {"items": []},
            {"items": []},
        ]
    )
    resolved: list[str] = []

    def resolve(symbol: str) -> tuple[str, str]:
        resolved.append(symbol)
        return "平安银行", "银行"

    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="placeholder"), transport, resolver=resolve)
    result = asyncio.run(adapter.get_news("000001.SZ"))

    assert resolved == ["000001.SZ"]
    assert result.metadata.freshness is Freshness.LIVE
    assert result.metadata.source == "iwencai_news_search"
    assert result.value is not None and isinstance(result.value[0], NewsItem)
    assert result.value[0].sources[0].url == "https://example.cn/a"
    assert {request.chart_epoch for request in transport.requests} == {0}


def test_standard_get_news_uses_symbol_and_explicit_industry_fallback_without_master_factory() -> None:
    transport = RecordingTransport([{"items": []}, {"items": []}, {"items": []}])
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="placeholder"), transport)

    result = asyncio.run(adapter.get_news("000001.SZ"))

    assert result.value == ()
    assert transport.requests[0].query.startswith("000001.SZ 000001.SZ ")
    assert transport.requests[1].query.startswith("A股行业（行业未知） ")


def test_standard_get_news_applies_since_after_normalization() -> None:
    transport = RecordingTransport(
        [
            {"items": [item("old", "旧公告", "https://example.cn/old", "2026-07-11T09:00:00+08:00")]},
            {"items": []},
            {"items": []},
        ]
    )
    adapter = IwencaiNewsAdapter(
        IwencaiConfig(api_key="placeholder"), transport, resolver=lambda _: ("平安银行", "银行")
    )

    result = asyncio.run(adapter.get_news("000001.SZ", since=NOW))

    assert result.value == ()


def test_standard_get_news_maps_cached_failure_to_stale_result() -> None:
    transport = RecordingTransport(
        [
            {"items": [item("1", "公告", "https://example.cn/a", "2026-07-12T09:00:00+08:00")]},
            {"items": []},
            {"items": []},
            httpx.ReadTimeout("do-not-expose"),
        ]
    )
    adapter = IwencaiNewsAdapter(
        IwencaiConfig(api_key="placeholder"), transport, resolver=lambda _: ("平安银行", "银行")
    )
    asyncio.run(adapter.get_news("000001.SZ"))
    result = asyncio.run(adapter.get_news("000001.SZ"))

    assert result.value is not None
    assert result.metadata.freshness is Freshness.STALE
    assert result.error == "timeout"
    assert "do-not-expose" not in repr(result)


def test_standard_get_news_maps_resolver_failure_to_unavailable() -> None:
    def fail(_: str) -> tuple[str, str]:
        raise RuntimeError("api-key-must-not-leak")

    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="api-key-must-not-leak"), RecordingTransport([]), resolver=fail)

    result = asyncio.run(adapter.get_news("000001.SZ"))

    assert result.value is None
    assert result.metadata.freshness is Freshness.UNAVAILABLE
    assert result.error == "master_data_unavailable"
    assert "api-key-must-not-leak" not in repr(result)


def test_standard_get_news_maps_invalid_symbol_to_unavailable_without_resolving() -> None:
    resolved: list[str] = []
    adapter = IwencaiNewsAdapter(
        IwencaiConfig(api_key="placeholder"),
        RecordingTransport([]),
        resolver=lambda symbol: (resolved.append(symbol) or ("name", "industry")),
    )

    result = asyncio.run(adapter.get_news("000001.SZ;drop"))

    assert result.metadata.freshness is Freshness.UNAVAILABLE
    assert result.error == "invalid_symbol"
    assert resolved == []


@pytest.mark.parametrize("endpoint", ["http://openapi.iwencai.com/search", "https://user:secret@openapi.iwencai.com/search", "https://other.example/search"])
def test_http_transport_rejects_untrusted_or_non_https_endpoint_without_leaking_url_credentials(endpoint: str) -> None:
    with pytest.raises(ValueError) as exc:
        HttpxNewsTransport(
            endpoint=endpoint,
            api_key="api-key",
            request_builder=lambda request, key: {"method": "GET"},
            response_parser=lambda response: response,
            allowed_hosts=("openapi.iwencai.com",),
        )

    assert "secret" not in str(exc.value)


def test_http_transport_disables_cross_host_redirects() -> None:
    async def redirect(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://attacker.example/news"}, request=request)

    transport = HttpxNewsTransport(
        endpoint="https://openapi.iwencai.com/search",
        api_key="api-key",
        request_builder=lambda request, key: {"method": "GET"},
        response_parser=lambda response: response,
        transport=httpx.MockTransport(redirect),
        allowed_hosts=("openapi.iwencai.com",),
    )

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(transport.search(NewsRequest("query", "company", "000001.SZ", 1)))


def test_http_transport_uses_injected_allowlist_not_process_environment(monkeypatch) -> None:
    monkeypatch.setenv("IWENCAI_ALLOWED_HOSTS", "attacker.example")

    transport = HttpxNewsTransport(
        endpoint="https://trusted.example/search",
        api_key="api-key",
        request_builder=lambda request, key: {"method": "GET"},
        response_parser=lambda response: response,
        allowed_hosts=("trusted.example",),
    )

    assert transport._endpoint == "https://trusted.example/search"


def test_iwencai_error_log_is_structured_and_redacted(caplog) -> None:
    adapter = IwencaiNewsAdapter(IwencaiConfig(api_key="api-key-secret"), RecordingTransport([httpx.ReadTimeout("raw-response-secret")]))

    feed = asyncio.run(adapter.get_feed("000001.SZ", 1, "name", "industry", now=NOW))

    assert feed.error == "timeout"
    assert any(record.event == "iwencai_news_fetch_failed" and record.error == "timeout" for record in caplog.records)
    assert "api-key-secret" not in caplog.text
    assert "raw-response-secret" not in caplog.text
