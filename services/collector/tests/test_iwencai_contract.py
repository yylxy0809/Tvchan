from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from collector.market_data.iwencai import NewsRequest, SchemaError
from collector.market_data.iwencai_contract import (
    build_search_endpoint,
    build_search_request,
    parse_search_response,
)


FIXTURES = Path(__file__).parent / "fixtures" / "iwencai"


def test_request_builder_uses_official_news_search_contract() -> None:
    request = build_search_request(NewsRequest("000001.SZ 最新新闻", "company", "000001.SZ", 1), "secret")

    assert request["method"] == "POST"
    assert request["json"] == {
        "query": "000001.SZ 最新新闻",
        "channels": ["news"],
        "app_id": "AIME_SKILL",
        "size": 50,
    }
    headers = request["headers"]
    assert headers["Authorization"] == "Bearer secret"
    assert headers["Content-Type"] == "application/json"
    assert headers["X-Claw-Call-Type"] == "fresh"
    assert headers["X-Claw-Skill-Id"] == "news-search"
    assert re.fullmatch(r"[0-9a-f]{64}", headers["X-Claw-Trace-Id"])


def test_response_parser_normalizes_the_official_response_shape() -> None:
    payload = json.loads((FIXTURES / "news_search_success.json").read_text(encoding="utf-8"))

    parsed = parse_search_response(payload)

    assert parsed == {
        "items": [
            {
                "id": "news-20260712-001",
                "title": "平安银行发布半年度业绩快报",
                "summary": "平安银行披露半年度业绩快报。",
                "url": "https://finance.example.cn/news/20260712/001",
                "published_at": "2026-07-12T09:30:00+08:00",
                "source_name": "证券时报",
            }
        ]
    }


def test_response_parser_accepts_production_epoch_publish_time() -> None:
    payload = json.loads((FIXTURES / "news_search_success.json").read_text(encoding="utf-8"))
    payload["data"][0]["publish_time"] = 1_782_224_510

    parsed = parse_search_response(payload)

    assert parsed["items"][0]["published_at"] == "2026-06-23T14:21:50+00:00"


def test_response_parser_accepts_provider_http_article_links() -> None:
    payload = json.loads((FIXTURES / "news_search_success.json").read_text(encoding="utf-8"))
    payload["data"][0]["url"] = "http://finance.example.cn/news/20260712/001"

    parsed = parse_search_response(payload)

    assert parsed["items"][0]["url"].startswith("http://")


@pytest.mark.parametrize(
    "payload",
    [
        {"status_code": 1, "data": []},
        {"status_code": 0, "data": [{}]},
        {"status_code": 0, "data": [{"title": "x", "summary": "x", "url": "https://example.cn", "publish_time": "not-a-date", "extra": {}}]},
    ],
)
def test_response_parser_rejects_invalid_official_payloads(payload: object) -> None:
    with pytest.raises(SchemaError):
        parse_search_response(payload)


@pytest.mark.parametrize(
    "base_url",
    ["http://openapi.iwencai.com", "https://user:secret@openapi.iwencai.com", "https://openapi.iwencai.com:444", "https://openapi.iwencai.com?next=https://attacker.example"],
)
def test_search_endpoint_requires_safe_https_base_url(base_url: str) -> None:
    with pytest.raises(ValueError):
        build_search_endpoint(base_url, ("openapi.iwencai.com",))


def test_search_endpoint_appends_fixed_path_to_base_url() -> None:
    assert build_search_endpoint("https://openapi.iwencai.com/", ("openapi.iwencai.com",)) == "https://openapi.iwencai.com/v1/comprehensive/search"
