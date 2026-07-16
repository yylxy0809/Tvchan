from __future__ import annotations

import os
import json
from collections.abc import Mapping

from .iwencai import HttpxIwencaiTransport, IwencaiApiKey, IwencaiConfig, IwencaiSidebarProvider
from .iwencai_contract import build_endpoint
from .notte import NotteConfig, NotteSidebarProvider
from .provider import FallbackMarketDataProvider, UnifiedMarketDataProvider


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def create_market_data_provider(
    *,
    env: Mapping[str, str] = os.environ,
    http_transport_cls: type = HttpxIwencaiTransport,
    notte_function: object | None = None,
) -> UnifiedMarketDataProvider:
    base_url = _required(env, "IWENCAI_BASE_URL")
    api_keys = _api_keys(env)
    hosts = tuple(value.strip().lower() for value in env.get("IWENCAI_ALLOWED_HOSTS", "openapi.iwencai.com").split(",") if value.strip())
    if not hosts:
        raise ValueError("IWENCAI_ALLOWED_HOSTS is required")
    try:
        timeout = float(env.get("IWENCAI_TIMEOUT_SECONDS", "5"))
    except ValueError as exc:
        raise ValueError("IWENCAI_TIMEOUT_SECONDS must be a number") from exc
    if not 0 < timeout <= 5:
        raise ValueError("IWENCAI_TIMEOUT_SECONDS must be within (0, 5]")
    iwencai = IwencaiSidebarProvider(IwencaiConfig(timeout_seconds=timeout, api_keys=api_keys), http_transport_cls(query_endpoint=build_endpoint(base_url, hosts), news_endpoint=build_endpoint(base_url, hosts, news=True), api_key=api_keys[0].key, api_keys=api_keys, timeout_seconds=timeout, allowed_hosts=hosts))
    order = tuple(item.strip().lower() for item in env.get("MARKET_DATA_PROVIDER_ORDER", "notte,iwencai").split(",") if item.strip())
    if "notte" not in order or not env.get("NOTTE_API_KEY", "").strip():
        return iwencai
    notte = NotteSidebarProvider(NotteConfig.from_env(env), function=notte_function)
    return FallbackMarketDataProvider(notte, iwencai) if order.index("notte") < order.index("iwencai") else FallbackMarketDataProvider(iwencai, notte)


def _api_keys(env: Mapping[str, str]) -> tuple[IwencaiApiKey, ...]:
    raw = env.get("IWENCAI_API_KEYS", "").strip()
    if not raw:
        return (IwencaiApiKey(key=_required(env, "IWENCAI_API_KEY")),)
    try: values = json.loads(raw)
    except json.JSONDecodeError as exc: raise ValueError("IWENCAI_API_KEYS must be JSON") from exc
    if not isinstance(values, list): raise ValueError("IWENCAI_API_KEYS must be an array")
    return tuple(IwencaiApiKey(label=str(item.get("label") or "default"), key=str(item.get("key") or ""), enabled=bool(item.get("enabled", True)), priority=int(item.get("priority", 0))) for item in values if isinstance(item, Mapping))
