from __future__ import annotations

import asyncio
import re
import logging
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Callable, Mapping, Protocol
from urllib.parse import urlsplit

import httpx

from .contracts import MarketDataResult, NewsItem
from .iwencai_contract import SchemaError
from .news import NewsFeed, NewsStatus, RawNewsItem, feed_to_market_data_result, normalize_news


SOURCE = "iwencai_news_search"
DEFAULT_ALLOWED_HOSTS = ("openapi.iwencai.com",)
_LOG = logging.getLogger(__name__)
_SYMBOL = re.compile(r"^(?:\d{6})\.(?:SH|SZ|BJ)$")
_SAFE_MASTER_TEXT = re.compile(r"^[^\r\n\x00-\x1f]{1,80}$")
_DEFAULT_INDUSTRY = "A股行业（行业未知）"


@dataclass(frozen=True, slots=True)
class IwencaiConfig:
    api_key: str = field(repr=False)
    timeout_seconds: float = 5.0
    result_items_key: str = "items"

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("IWENCAI_API_KEY is required")
        if not 0 < self.timeout_seconds <= 5:
            raise ValueError("timeout_seconds must be within (0, 5]")


@dataclass(frozen=True, slots=True)
class NewsRequest:
    query: str
    query_kind: str
    chart_symbol: str
    chart_epoch: int


class NewsTransport(Protocol):
    async def search(self, request: NewsRequest) -> object: ...


class MasterDataResolver(Protocol):
    def __call__(self, symbol: str) -> tuple[str, str]: ...


RequestBuilder = Callable[[NewsRequest, str], Mapping[str, object]]
ResponseParser = Callable[[object], object]


class HttpxNewsTransport:
    """HTTP boundary whose endpoint and payload contract must be supplied by the caller."""

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        request_builder: RequestBuilder,
        response_parser: ResponseParser,
        timeout_seconds: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
        allowed_hosts: tuple[str, ...] | None = None,
    ) -> None:
        if not endpoint or not request_builder or not response_parser:
            raise ValueError("endpoint, request_builder and response_parser are required")
        parsed_endpoint = urlsplit(endpoint)
        configured_hosts = allowed_hosts if allowed_hosts is not None else DEFAULT_ALLOWED_HOSTS
        if parsed_endpoint.scheme.lower() != "https" or not parsed_endpoint.hostname or parsed_endpoint.username or parsed_endpoint.password:
            raise ValueError("Iwencai endpoint must use HTTPS")
        if not configured_hosts or parsed_endpoint.hostname.lower() not in configured_hosts:
            raise ValueError("Iwencai endpoint host is not allowed")
        self._endpoint = endpoint
        self._api_key = api_key
        self._request_builder = request_builder
        self._response_parser = response_parser
        self._timeout = timeout_seconds
        self._transport = transport

    async def search(self, request: NewsRequest) -> object:
        kwargs = dict(self._request_builder(request, self._api_key))
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport, follow_redirects=False) as client:
            response = await client.request(url=self._endpoint, **kwargs)
            response.raise_for_status()
            return self._response_parser(response.json())


class IwencaiNewsAdapter:
    def __init__(
        self,
        config: IwencaiConfig,
        transport: NewsTransport,
        *,
        resolver: MasterDataResolver | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._resolver = resolver or default_master_data_resolver
        self._last_success: dict[str, NewsFeed] = {}

    async def get_news(
        self, symbol: str, since: datetime | None = None
    ) -> MarketDataResult[tuple[NewsItem, ...]]:
        if not _SYMBOL.fullmatch(symbol):
            return MarketDataResult.unavailable(source=SOURCE, error="invalid_symbol")
        try:
            security_name, industry = self._resolver(symbol)
        except Exception:
            return MarketDataResult.unavailable(source=SOURCE, error="master_data_unavailable")
        feed = await self.get_feed(symbol, 0, security_name, industry)
        try:
            return feed_to_market_data_result(feed, since=since)
        except ValueError:
            return MarketDataResult.unavailable(source=SOURCE, error="normalization_error")

    async def get_feed(
        self,
        chart_symbol: str,
        chart_epoch: int,
        security_name: str,
        industry: str,
        *,
        now: datetime | None = None,
    ) -> NewsFeed:
        self._validate_master_data(chart_symbol, chart_epoch, security_name, industry)
        received_at = now or datetime.now(UTC)
        requests = (
            NewsRequest(f"{security_name} {chart_symbol} 最新公告 业务进展 业绩 重大事项", "company", chart_symbol, chart_epoch),
            NewsRequest(f"{industry} 最新政策 行业动态 产业链变化", "industry", chart_symbol, chart_epoch),
            NewsRequest(f"{security_name} 监管 问询 处罚 诉讼 减持 风险", "risk", chart_symbol, chart_epoch),
        )
        raw_items: list[RawNewsItem] = []
        try:
            responses = await asyncio.gather(
                *(self._transport.search(request) for request in requests)
            )
            for request, response in zip(requests, responses, strict=True):
                raw_items.extend(self._parse_response(response, request.query_kind))
            feed = NewsFeed(
                chart_symbol=chart_symbol,
                chart_epoch=chart_epoch,
                status=NewsStatus.FRESH,
                events=normalize_news(raw_items, symbol=chart_symbol, now=received_at),
                updated_at=received_at,
            )
            self._last_success[chart_symbol] = feed
            return feed
        except Exception as exc:
            error = self._classify_error(exc)
            _LOG.warning(
                "iwencai_news_fetch_failed",
                extra={"event": "iwencai_news_fetch_failed", "error": error, "error_type": type(exc).__name__},
            )
            cached = self._last_success.get(chart_symbol)
            if cached is not None:
                return replace(cached, chart_epoch=chart_epoch, status=NewsStatus.STALE, error=error)
            return NewsFeed(chart_symbol, chart_epoch, NewsStatus.UNAVAILABLE, error=error)

    @staticmethod
    def _validate_master_data(symbol: str, epoch: int, name: str, industry: str) -> None:
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("invalid chart_symbol")
        if epoch < 0:
            raise ValueError("chart_epoch cannot be negative")
        if not _SAFE_MASTER_TEXT.fullmatch(name) or not _SAFE_MASTER_TEXT.fullmatch(industry):
            raise ValueError("invalid security master data")

    def _parse_response(self, response: object, query_kind: str) -> list[RawNewsItem]:
        if not isinstance(response, Mapping):
            raise SchemaError
        items = response.get(self._config.result_items_key)
        if not isinstance(items, list):
            raise SchemaError
        parsed: list[RawNewsItem] = []
        required = ("title", "url", "published_at", "source_name")
        for item in items:
            if not isinstance(item, Mapping) or any(not isinstance(item.get(key), str) or not item[key] for key in required):
                raise SchemaError
            try:
                published_at = datetime.fromisoformat(str(item["published_at"]).replace("Z", "+00:00"))
            except ValueError as exc:
                raise SchemaError from exc
            if published_at.tzinfo is None:
                raise SchemaError
            url = urlsplit(str(item["url"]))
            if url.scheme.lower() not in ("http", "https") or not url.netloc:
                raise SchemaError
            entities = item.get("entities", [])
            if not isinstance(entities, list) or not all(isinstance(value, str) for value in entities):
                raise SchemaError
            parsed.append(
                RawNewsItem(
                    provider_id=str(item["id"]) if item.get("id") is not None else None,
                    title=str(item["title"]),
                    url=str(item["url"]),
                    published_at=published_at,
                    summary=str(item.get("summary") or item["title"]),
                    source_name=str(item["source_name"]),
                    category=str(item.get("category") or query_kind),
                    entities=tuple(entities),
                    query_kind=query_kind,
                )
            )
        return parsed

    @staticmethod
    def _classify_error(exc: Exception) -> str:
        if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
            return "timeout"
        if isinstance(exc, httpx.HTTPStatusError):
            if exc.response.status_code == 429:
                return "rate_limited"
            if exc.response.status_code in (401, 403):
                return "authentication"
            return "provider_error"
        if isinstance(exc, SchemaError):
            return "schema_error"
        if isinstance(exc, (httpx.HTTPError, ValueError)):
            return "provider_error"
        return "unavailable"


def default_master_data_resolver(symbol: str) -> tuple[str, str]:
    """Search by chart symbol when no verified security master is configured."""
    return symbol, _DEFAULT_INDUSTRY
