from __future__ import annotations

import asyncio
import json
import re
import secrets
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
import urllib.error
import urllib.request


class WencaiConfigError(ValueError):
    pass


class WencaiUpstreamError(RuntimeError):
    pass


@dataclass(frozen=True)
class WencaiConfig:
    base_url: str = "https://openapi.iwencai.com"
    api_key: str = ""
    cookie: str = ""
    user_agent: str | None = None
    pro: bool = False
    timeout_seconds: float = 20


@dataclass(frozen=True)
class WencaiItem:
    symbol: str
    code: str
    exchange: str
    name: str
    price: float | None = None
    change_percent: float | None = None
    buy_signal: str = ""
    technical_shape: str = ""
    reason: str = ""
    high_break_reason: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WencaiQueryResult:
    query: str
    total: int
    page: int
    page_size: int
    fetched_at: datetime
    items: list[WencaiItem]


@dataclass(frozen=True)
class WencaiConnectivityResult:
    ok: bool
    latency_ms: int
    message: str
    sample_count: int


async def query_wencai(
    *,
    query: str,
    page: int,
    page_size: int,
    config: WencaiConfig,
) -> WencaiQueryResult:
    if config.api_key.strip():
        payload = await asyncio.to_thread(
            _fetch_openapi_page,
            query=query,
            page=page,
            page_size=page_size,
            config=config,
        )
        records = _records_from_result(payload.get("datas"))
        total = _int_from_unknown(payload.get("code_count"), default=len(records))
        items = [_normalize_item(record) for record in records]
    else:
        if not config.cookie.strip():
            raise WencaiConfigError("IWENCAI_API_KEY or WenCai cookie is required")
        records = await asyncio.to_thread(
            _fetch_all_records,
            query=query,
            config=config,
        )
        items = [_normalize_item(record) for record in records]
        total = len(items)
        start = max(0, (page - 1) * page_size)
        end = start + page_size
        items = items[start:end]
    return WencaiQueryResult(
        query=query,
        total=total,
        page=page,
        page_size=page_size,
        fetched_at=datetime.now(UTC),
        items=items,
    )


async def test_wencai_config(config: WencaiConfig) -> WencaiConnectivityResult:
    start = time.perf_counter()
    try:
        result = await query_wencai(
            query="今日涨停",
            page=1,
            page_size=1,
            config=config,
        )
        return WencaiConnectivityResult(
            ok=True,
            latency_ms=_elapsed_ms(start),
            message="问财连接正常",
            sample_count=len(result.items),
        )
    except Exception as exc:
        return WencaiConnectivityResult(
            ok=False,
            latency_ms=_elapsed_ms(start),
            message=str(exc),
            sample_count=0,
        )


def _fetch_all_records(*, query: str, config: WencaiConfig) -> list[dict[str, Any]]:
    try:
        result = _call_pywencai_get(
            query=query,
            cookie=config.cookie.strip(),
            user_agent=config.user_agent or None,
            pro=config.pro,
            loop=True,
            perpage=100,
            retry=1,
            no_detail=True,
            log=False,
        )
    except Exception as exc:
        raise WencaiUpstreamError(f"问财请求失败：{exc}") from exc
    return _records_from_result(result)


def _fetch_openapi_page(
    *,
    query: str,
    page: int,
    page_size: int,
    config: WencaiConfig,
) -> dict[str, Any]:
    result = _call_iwencai_openapi(
        query=query,
        page=str(page),
        limit=str(page_size),
        api_key=config.api_key.strip(),
        base_url=config.base_url.strip() or "https://openapi.iwencai.com",
        timeout=config.timeout_seconds,
    )
    if not isinstance(result, dict):
        raise WencaiUpstreamError("问财 OpenAPI 返回格式异常")
    if "datas" not in result:
        message = result.get("message") or result.get("msg") or result.get("error") or result
        raise WencaiUpstreamError(f"问财 OpenAPI 请求失败：{message}")
    return result


def _call_iwencai_openapi(
    *,
    query: str,
    page: str,
    limit: str,
    api_key: str,
    base_url: str,
    timeout: float,
) -> dict[str, Any]:
    trace_id = secrets.token_hex(32)
    payload = {
        "query": query,
        "page": page,
        "limit": limit,
        "is_cache": "1",
        "expand_index": "true",
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-Claw-Call-Type": "normal",
        "X-Claw-Skill-Id": "hithink-astock-selector",
        "X-Claw-Skill-Version": "1.0.0",
        "X-Claw-Plugin-Id": "none",
        "X-Claw-Plugin-Version": "none",
        "X-Claw-Trace-Id": trace_id,
    }
    request = urllib.request.Request(
        _iwencai_openapi_endpoint(base_url),
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        raise WencaiUpstreamError(f"问财 OpenAPI HTTP {exc.code}: {body or exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise WencaiUpstreamError(f"问财 OpenAPI 网络错误：{exc.reason}") from exc
    try:
        value = json.loads(body)
    except json.JSONDecodeError as exc:
        raise WencaiUpstreamError(f"问财 OpenAPI 返回非 JSON：{body[:200]}") from exc
    if not isinstance(value, dict):
        raise WencaiUpstreamError("问财 OpenAPI 返回格式异常")
    return value


def _iwencai_openapi_endpoint(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if normalized.endswith("/v1/query2data"):
        return normalized
    return f"{normalized}/v1/query2data"


def _call_pywencai_get(**kwargs):
    try:
        import pywencai
    except ImportError as exc:
        raise WencaiUpstreamError("pywencai is not installed") from exc
    return pywencai.get(**kwargs)


def _records_from_result(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    if hasattr(value, "to_dict"):
        try:
            records = value.to_dict(orient="records")
            return [dict(item) for item in records if isinstance(item, dict)]
        except TypeError:
            pass
    if isinstance(value, dict):
        return [value]
    return []


def _int_from_unknown(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_item(record: dict[str, Any]) -> WencaiItem:
    code = _extract_code(record)
    exchange = _extract_exchange(record, code)
    name = _first_text(record, ("股票简称", "股票名称", "简称", "名称", "name")) or code
    return WencaiItem(
        symbol=f"{code}.{exchange}",
        code=code,
        exchange=exchange,
        name=name,
        price=_first_number(record, ("最新价", "现价", "收盘价", "price")),
        change_percent=_first_number(record, ("涨跌幅", "涨幅", "change_percent")),
        buy_signal=_first_text(record, ("买入信号", "买入", "信号", "buy_signal")) or "",
        technical_shape=_first_text(record, ("技术形态", "形态", "technical_shape")) or "",
        reason=_first_text(record, ("条件说明", "理由", "reason")) or "",
        high_break_reason=_first_text(record, ("突破前高", "新高", "high_break_reason")) or "",
        raw=record,
    )


def _extract_code(record: dict[str, Any]) -> str:
    raw = _first_text(record, ("股票代码", "代码", "code", "symbol")) or ""
    match = re.search(r"(\d{6})", raw)
    return match.group(1) if match else raw.strip()


def _extract_exchange(record: dict[str, Any], code: str) -> str:
    raw = _first_text(record, ("交易所", "exchange", "市场")) or ""
    upper = raw.upper()
    if "SH" in upper or "沪" in raw:
        return "SH"
    if "SZ" in upper or "深" in raw:
        return "SZ"
    return "SH" if code.startswith("6") else "SZ"


def _first_text(record: dict[str, Any], needles: tuple[str, ...]) -> str | None:
    for key, value in record.items():
        key_text = str(key)
        if any(needle in key_text for needle in needles):
            if value is None:
                return None
            return str(value).strip()
    return None


def _first_number(record: dict[str, Any], needles: tuple[str, ...]) -> float | None:
    text = _first_text(record, needles)
    if text is None or text == "":
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def _elapsed_ms(start: float) -> int:
    return int(round((time.perf_counter() - start) * 1000))
