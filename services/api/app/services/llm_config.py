from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from app.core.config import Settings
from app.repositories import runtime_config
from app.services.llm_client import parse_chan_query_with_llm


LLM_CONFIG_KEY = "llm.providers"
WENCAI_CONFIG_KEY = "wencai.config"


@dataclass(frozen=True)
class LlmProvider:
    id: str
    name: str
    base_url: str
    api_key: str
    models: list[str]
    active_model: str
    enabled: bool = True
    timeout_seconds: float = 20

    @property
    def model(self) -> str:
        return self.active_model or (self.models[0] if self.models else "")


@dataclass(frozen=True)
class LlmProvidersConfig:
    active_provider_id: str | None
    providers: list[LlmProvider]


@dataclass(frozen=True)
class LlmConnectivityResult:
    ok: bool
    latency_ms: int
    provider: str
    model: str
    message: str


async def resolve_active_llm_provider(pool, settings: Settings) -> LlmProvider | None:
    runtime = await load_llm_providers(pool, settings=settings, include_env_default=False)
    provider = _select_active_provider(runtime)
    if provider is not None:
        return provider
    env_provider = env_llm_provider(settings)
    if env_provider.enabled and env_provider.api_key and env_provider.model:
        return env_provider
    return None


async def load_llm_providers(
    pool,
    *,
    settings: Settings,
    include_env_default: bool = True,
) -> LlmProvidersConfig:
    row = await runtime_config.get_config(pool, LLM_CONFIG_KEY)
    if row is not None:
        config = _providers_config_from_value(row.get("value"))
        if config.providers:
            return config
    if include_env_default:
        provider = env_llm_provider(settings)
        return LlmProvidersConfig(active_provider_id=provider.id, providers=[provider])
    return LlmProvidersConfig(active_provider_id=None, providers=[])


async def save_llm_providers(pool, payload: dict[str, Any]) -> LlmProvidersConfig:
    existing = await load_llm_providers(
        pool,
        settings=Settings(),
        include_env_default=False,
    )
    existing_by_id = {provider.id: provider for provider in existing.providers}
    providers = []
    for item in payload.get("providers") or []:
        provider = _provider_from_value(item)
        if provider is None:
            continue
        previous = existing_by_id.get(provider.id)
        api_key = _resolve_secret(provider.api_key, previous.api_key if previous else "")
        providers.append(
            LlmProvider(
                id=provider.id,
                name=provider.name,
                base_url=provider.base_url,
                api_key=api_key,
                models=provider.models,
                active_model=provider.active_model,
                enabled=provider.enabled,
                timeout_seconds=provider.timeout_seconds,
            )
        )
    active_provider_id = _clean_text(payload.get("active_provider_id"))
    config = LlmProvidersConfig(active_provider_id=active_provider_id, providers=providers)
    await runtime_config.upsert_config(
        pool,
        key=LLM_CONFIG_KEY,
        value=llm_config_to_storage(config),
    )
    return config


async def test_llm_provider(provider: LlmProvider) -> LlmConnectivityResult:
    start = time.perf_counter()
    if not provider.api_key.strip():
        return LlmConnectivityResult(False, _elapsed_ms(start), provider.id, provider.model, "API Key 未配置")
    if not provider.base_url.strip():
        return LlmConnectivityResult(False, _elapsed_ms(start), provider.id, provider.model, "Base URL 未配置")
    if not provider.model:
        return LlmConnectivityResult(False, _elapsed_ms(start), provider.id, provider.model, "模型未配置")
    try:
        await parse_chan_query_with_llm(
            query="日线趋势上涨",
            api_key=provider.api_key,
            base_url=provider.base_url,
            model=provider.model,
            timeout_seconds=provider.timeout_seconds,
        )
        return LlmConnectivityResult(True, _elapsed_ms(start), provider.id, provider.model, "LLM 连接正常")
    except Exception as exc:
        return LlmConnectivityResult(False, _elapsed_ms(start), provider.id, provider.model, str(exc)[:300])


def env_llm_provider(settings: Settings) -> LlmProvider:
    return LlmProvider(
        id="siliconflow-env",
        name="硅基流动（环境变量）",
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        models=[settings.llm_model] if settings.llm_model else [],
        active_model=settings.llm_model,
        enabled=settings.llm_enabled,
        timeout_seconds=settings.llm_timeout_seconds,
    )


def llm_config_to_storage(config: LlmProvidersConfig) -> dict[str, Any]:
    return {
        "active_provider_id": config.active_provider_id,
        "providers": [
            {
                "id": provider.id,
                "name": provider.name,
                "base_url": provider.base_url,
                "api_key": provider.api_key,
                "models": provider.models,
                "active_model": provider.active_model,
                "enabled": provider.enabled,
                "timeout_seconds": provider.timeout_seconds,
            }
            for provider in config.providers
        ],
    }


def llm_config_to_response(config: LlmProvidersConfig) -> dict[str, Any]:
    data = llm_config_to_storage(config)
    for provider in data["providers"]:
        provider["api_key"] = mask_secret(str(provider.get("api_key") or ""))
    return data


def provider_from_payload(value: dict[str, Any], existing: LlmProvider | None = None) -> LlmProvider:
    provider = _provider_from_value(value)
    if provider is None:
        raise ValueError("LLM 接入点配置无效")
    return LlmProvider(
        id=provider.id,
        name=provider.name,
        base_url=provider.base_url,
        api_key=_resolve_secret(provider.api_key, existing.api_key if existing else ""),
        models=provider.models,
        active_model=provider.active_model,
        enabled=provider.enabled,
        timeout_seconds=provider.timeout_seconds,
    )


def mask_secret(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if len(text) <= 8:
        return "***"
    return f"{text[:4]}...{text[-4:]}"


def is_masked_secret(value: str | None) -> bool:
    return bool(value and ("..." in value or value == "***"))


def _providers_config_from_value(value: Any) -> LlmProvidersConfig:
    if not isinstance(value, dict):
        return LlmProvidersConfig(active_provider_id=None, providers=[])
    providers = [
        provider
        for provider in (_provider_from_value(item) for item in value.get("providers") or [])
        if provider is not None
    ]
    return LlmProvidersConfig(
        active_provider_id=_clean_text(value.get("active_provider_id")),
        providers=providers,
    )


def _provider_from_value(value: Any) -> LlmProvider | None:
    if not isinstance(value, dict):
        return None
    provider_id = _clean_text(value.get("id"))
    name = _clean_text(value.get("name")) or provider_id
    base_url = _clean_text(value.get("base_url") or value.get("baseUrl"))
    api_key = _clean_text(value.get("api_key") or value.get("apiKey")) or ""
    raw_models = value.get("models")
    models = [
        item.strip()
        for item in (raw_models if isinstance(raw_models, list) else [])
        if isinstance(item, str) and item.strip()
    ]
    active_model = _clean_text(value.get("active_model") or value.get("activeModel")) or (models[0] if models else "")
    enabled = value.get("enabled")
    timeout = _float_or_default(value.get("timeout_seconds") or value.get("timeoutSeconds"), 20)
    if not provider_id or not base_url:
        return None
    return LlmProvider(
        id=provider_id,
        name=name or provider_id,
        base_url=base_url,
        api_key=api_key,
        models=models,
        active_model=active_model,
        enabled=True if enabled is None else bool(enabled),
        timeout_seconds=timeout,
    )


def _select_active_provider(config: LlmProvidersConfig) -> LlmProvider | None:
    enabled = [provider for provider in config.providers if provider.enabled and provider.api_key and provider.model]
    if not enabled:
        return None
    if config.active_provider_id:
        for provider in enabled:
            if provider.id == config.active_provider_id:
                return provider
    return enabled[0]


def _resolve_secret(candidate: str, previous: str) -> str:
    if is_masked_secret(candidate):
        return previous
    return candidate.strip()


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _float_or_default(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _elapsed_ms(start: float) -> int:
    return int(round((time.perf_counter() - start) * 1000))
