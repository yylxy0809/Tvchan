from __future__ import annotations

import importlib
import os
import re
from collections.abc import Callable, Mapping
from typing import Any

from .iwencai import HttpxNewsTransport, IwencaiConfig, IwencaiNewsAdapter
from .iwencai_contract import build_search_endpoint, build_search_request, parse_search_response
from .local_quotes import LocalPostgresQuoteSource
from .provider import CompositeMarketDataProvider
from .westock import NodeJsonlTransport, PooledBridgeTransport, WeStockAdapter


_WESTOCK_VERSION = "westock-data@1.0.4"
_SHA256 = re.compile(r"^[a-fA-F0-9]{64}$")


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _positive_float(env: Mapping[str, str], name: str, default: str) -> float:
    try:
        value = float(env.get(name, default))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def _positive_int(env: Mapping[str, str], name: str, default: str) -> int:
    try:
        value = int(env.get(name, default))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _host_allowlist(env: Mapping[str, str]) -> tuple[str, ...]:
    hosts = tuple(host.strip().lower() for host in env.get("IWENCAI_ALLOWED_HOSTS", "openapi.iwencai.com").split(",") if host.strip())
    if not hosts:
        raise ValueError("IWENCAI_ALLOWED_HOSTS must contain at least one host")
    return hosts


def _load_factory(path: str, setting: str) -> Callable[[], Any]:
    module_name, separator, attribute = path.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError(f"{setting} must use module:callable syntax")
    try:
        factory = getattr(importlib.import_module(module_name), attribute)
    except Exception as exc:
        raise ValueError(f"{setting} could not be loaded") from exc
    if not callable(factory):
        raise ValueError(f"{setting} must reference a callable")
    return factory


def _create(path: str, setting: str) -> Any:
    factory = _load_factory(path, setting)
    try:
        return factory()
    except Exception as exc:
        raise ValueError(f"{setting} factory failed") from exc


def create_market_data_provider(
    *,
    env: Mapping[str, str] = os.environ,
    node_transport_cls: type = NodeJsonlTransport,
    http_transport_cls: type = HttpxNewsTransport,
) -> CompositeMarketDataProvider:
    """Build the configured provider; injectable classes keep construction tests offline."""
    database_url = _required(env, "DATABASE_URL")
    normalizer_path = env.get(
        "WESTOCK_NORMALIZER_FACTORY", "collector.market_data.westock_normalizer:create_westock_normalizer"
    ).strip()
    if not normalizer_path:
        raise ValueError("WESTOCK_NORMALIZER_FACTORY is required")
    module_path = _required(env, "WESTOCK_BRIDGE_MODULE")
    module_version = _required(env, "WESTOCK_BRIDGE_MODULE_VERSION")
    module_hash = _required(env, "WESTOCK_BRIDGE_SHA256")
    base_url = _required(env, "IWENCAI_BASE_URL")
    api_key = _required(env, "IWENCAI_API_KEY")

    westock_timeout = _positive_float(env, "WESTOCK_TIMEOUT_SECONDS", "10")
    max_output_bytes = _positive_int(env, "WESTOCK_MAX_OUTPUT_BYTES", "1048576")
    batch_size = _positive_int(env, "WESTOCK_BATCH_SIZE", "100")
    process_pool_size = _positive_int(env, "WESTOCK_PROCESS_POOL_SIZE", "5")
    iwencai_timeout = _positive_float(env, "IWENCAI_TIMEOUT_SECONDS", "5")
    iwencai_allowed_hosts = _host_allowlist(env)
    if iwencai_timeout > 5:
        raise ValueError("IWENCAI_TIMEOUT_SECONDS must be at most 5")
    if module_version != _WESTOCK_VERSION:
        raise ValueError("WESTOCK_BRIDGE_MODULE_VERSION is unsupported")
    if not _SHA256.fullmatch(module_hash):
        raise ValueError("WESTOCK_BRIDGE_SHA256 must be a 64-character hexadecimal digest")

    normalizer = _create(normalizer_path, "WESTOCK_NORMALIZER_FACTORY")
    resolver_path = env.get("IWENCAI_MASTER_DATA_RESOLVER_FACTORY", "").strip()
    resolver = _create(resolver_path, "IWENCAI_MASTER_DATA_RESOLVER_FACTORY") if resolver_path else None
    transport_options = {
        "timeout_seconds": westock_timeout,
        "max_output_bytes": max_output_bytes,
        "env": {
            "WESTOCK_BRIDGE_MODULE": module_path,
            "WESTOCK_BRIDGE_MODULE_VERSION": module_version,
            "WESTOCK_BRIDGE_SHA256": module_hash,
        },
    }
    node_transports = tuple(node_transport_cls(**transport_options) for _ in range(process_pool_size))
    node_transport = node_transports[0] if len(node_transports) == 1 else PooledBridgeTransport(node_transports)
    market = WeStockAdapter(
        node_transport,
        batch_size=batch_size,
        normalizer=normalizer,
        max_transport_concurrency=min(
            _positive_int(env, "WESTOCK_MAX_TRANSPORT_CONCURRENCY", "16"),
            process_pool_size,
        ),
    )

    endpoint = build_search_endpoint(base_url, iwencai_allowed_hosts)
    news_transport = http_transport_cls(
        endpoint=endpoint,
        api_key=api_key,
        request_builder=build_search_request,
        response_parser=parse_search_response,
        timeout_seconds=iwencai_timeout,
        allowed_hosts=iwencai_allowed_hosts,
    )
    news = IwencaiNewsAdapter(
        IwencaiConfig(api_key=api_key, timeout_seconds=iwencai_timeout),
        news_transport,
        resolver=resolver,
    )
    return CompositeMarketDataProvider(LocalPostgresQuoteSource(database_url), market, news)
