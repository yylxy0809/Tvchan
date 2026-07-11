from __future__ import annotations

from dataclasses import dataclass

from collector.providers.base import MarketDataProvider
from collector.providers.http_kline import BaiduKlineProvider, TencentKlineProvider
from collector.providers.mootdx_provider import MootdxProvider
from collector.providers.pool import MarketDataPool
from collector.providers.pytdx_provider import PytdxProvider
from collector.providers.seed import SeedProvider


@dataclass(frozen=True)
class ProviderFactoryConfig:
    names: list[str]
    tdx_host: str | None = None
    tdx_port: int = 7709
    tdx_timeout: int = 10
    tdx_retries: int = 3
    http_timeout: float = 5.0
    pool_policy: str = "primary_failover"
    pool_timeout_seconds: float = 8.0
    pool_hedged_delay_seconds: float = 0.35


def create_market_provider(config: ProviderFactoryConfig) -> MarketDataProvider:
    providers = [create_single_provider(name, config) for name in config.names]
    if len(providers) == 1 and normalize_provider_name(config.names[0]) != "pool":
        return providers[0]
    return MarketDataPool(
        providers,
        policy=config.pool_policy,
        timeout_seconds=config.pool_timeout_seconds,
        hedged_delay_seconds=config.pool_hedged_delay_seconds,
    )


def create_single_provider(name: str, config: ProviderFactoryConfig) -> MarketDataProvider:
    normalized = normalize_provider_name(name)
    if normalized == "pool":
        raise ValueError("pool is a wrapper provider and cannot be nested")
    if normalized == "seed":
        return SeedProvider()
    if normalized == "pytdx":
        return PytdxProvider(
            host=config.tdx_host,
            port=config.tdx_port,
            timeout=config.tdx_timeout,
            retries=config.tdx_retries,
        )
    if normalized == "mootdx":
        return MootdxProvider()
    if normalized == "tencent":
        return TencentKlineProvider(timeout=config.http_timeout)
    if normalized == "baidu":
        return BaiduKlineProvider(timeout=config.http_timeout)
    raise ValueError(f"Unknown market data provider: {name}")


def normalize_provider_name(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def parse_provider_names(value: str | None) -> list[str]:
    names = [item.strip() for item in (value or "").split(",") if item.strip()]
    return names or ["pytdx"]
