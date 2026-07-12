from __future__ import annotations

import re
import secrets
import math
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Mapping
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from .iwencai import NewsRequest


SEARCH_PATH = "/v1/comprehensive/search"
DEFAULT_RESULT_SIZE = 50
_TRACE_ID = re.compile(r"^[0-9a-f]{64}$")
_CHINA_TIMEZONE = timezone(timedelta(hours=8))


class SchemaError(ValueError):
    pass


def build_search_endpoint(base_url: str, allowed_hosts: tuple[str, ...]) -> str:
    parsed = urlsplit(base_url)
    try:
        host = parsed.hostname.lower() if parsed.hostname else None
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Iwencai base URL must be an allowed HTTPS origin") from exc
    if (
        parsed.scheme.lower() != "https"
        or host is None
        or parsed.username
        or parsed.password
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
        or host not in allowed_hosts
    ):
        raise ValueError("Iwencai base URL must be an allowed HTTPS origin")
    return urlunsplit(("https", parsed.netloc, SEARCH_PATH, "", ""))


def build_search_request(request: NewsRequest, api_key: str) -> Mapping[str, object]:
    trace_id = secrets.token_hex(32)
    if not _TRACE_ID.fullmatch(trace_id):  # pragma: no cover - guards a security invariant.
        raise RuntimeError("failed to generate Iwencai trace id")
    return {
        "method": "POST",
        "headers": {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-Claw-Call-Type": "fresh",
            "X-Claw-Skill-Id": "news-search",
            "X-Claw-Skill-Version": "2.0.0",
            "X-Claw-Plugin-Id": "none",
            "X-Claw-Plugin-Version": "none",
            "X-Claw-Trace-Id": trace_id,
        },
        "json": {
            "query": request.query,
            "channels": ["news"],
            "app_id": "AIME_SKILL",
            "size": DEFAULT_RESULT_SIZE,
        },
    }


def parse_search_response(payload: object) -> object:
    if not isinstance(payload, Mapping) or payload.get("status_code") != 0:
        raise SchemaError("invalid Iwencai response")
    data = payload.get("data")
    if not isinstance(data, list):
        raise SchemaError("invalid Iwencai response data")
    return {"items": [_normalize_item(item) for item in data]}


def _normalize_item(item: object) -> dict[str, str]:
    if not isinstance(item, Mapping):
        raise SchemaError("invalid Iwencai news item")
    required_text = ("id", "title", "summary", "url")
    if any(not isinstance(item.get(field), str) or not item[field].strip() for field in required_text):
        raise SchemaError("invalid Iwencai news item")
    if not isinstance(item.get("publish_time"), (str, int, float)):
        raise SchemaError("invalid Iwencai news item")
    extra = item.get("extra")
    if not isinstance(extra, Mapping):
        raise SchemaError("invalid Iwencai news item")
    source = extra.get("real_publish_source") or extra.get("publish_source")
    if not isinstance(source, str) or not source.strip():
        raise SchemaError("invalid Iwencai news item")
    url = urlsplit(item["url"])
    if url.scheme.lower() not in ("http", "https") or not url.netloc or url.username or url.password:
        raise SchemaError("invalid Iwencai news item")
    return {
        "id": item["id"].strip(),
        "title": item["title"].strip(),
        "summary": item["summary"].strip(),
        "url": item["url"].strip(),
        "published_at": _parse_publish_time(item["publish_time"]),
        "source_name": source.strip(),
    }


def _parse_publish_time(value: str | int | float) -> str:
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise SchemaError("invalid Iwencai publish_time")
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError) as exc:
            raise SchemaError("invalid Iwencai publish_time") from exc
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_CHINA_TIMEZONE)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SchemaError("invalid Iwencai publish_time") from exc
        if parsed.tzinfo is None:
            raise SchemaError("Iwencai publish_time must include a timezone")
    return parsed.isoformat()
