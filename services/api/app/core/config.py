from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    app_env: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    api_token: str = field(default_factory=lambda: os.getenv("API_TOKEN", "dev-local-token"))
    admin_api_token: str = field(default_factory=lambda: os.getenv("ADMIN_API_TOKEN", ""))
    cors_origins: tuple[str, ...] = tuple(
        origin.strip()
        for origin in os.getenv(
            "CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
        ).split(",")
        if origin.strip()
    )
    cors_origin_regex: str | None = os.getenv("CORS_ORIGIN_REGEX") or None
    public_base_url: str = os.getenv("PUBLIC_BASE_URL", "")
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    chan_lifecycle_observer: str = field(
        default_factory=lambda: os.getenv("CHAN_LIFECYCLE_OBSERVER", "chan-lifecycle-v1")
    )
    chan_lifecycle_observer_stale_seconds: int = field(
        default_factory=lambda: int(os.getenv("CHAN_LIFECYCLE_OBSERVER_STALE_SECONDS", "120"))
    )
    use_seed_data: bool = os.getenv("USE_SEED_DATA", "true").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    use_precomputed_chan: bool = os.getenv("USE_PRECOMPUTED_CHAN", "true").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_base_url: str = os.getenv("LLM_BASE_URL", "https://api.siliconflow.cn/v1")
    llm_model: str = os.getenv("LLM_MODEL", "deepseek-ai/DeepSeek-V3.2")
    llm_timeout_seconds: float = float(os.getenv("LLM_TIMEOUT_SECONDS", "20"))
    llm_enabled: bool = os.getenv("LLM_ENABLED", "true").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    wencai_cookie: str = os.getenv("WENCAI_COOKIE", "")
    iwencai_base_url: str = os.getenv("IWENCAI_BASE_URL", "https://openapi.iwencai.com")
    iwencai_allowed_hosts: tuple[str, ...] = tuple(
        host.strip().lower()
        for host in os.getenv("IWENCAI_ALLOWED_HOSTS", "openapi.iwencai.com").split(",")
        if host.strip()
    )
    iwencai_api_key: str = os.getenv("IWENCAI_API_KEY", "")
    wencai_user_agent: str = os.getenv("WENCAI_USER_AGENT", "")
    wencai_pro: bool = os.getenv("WENCAI_PRO", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    wencai_timeout_seconds: float = float(os.getenv("WENCAI_TIMEOUT_SECONDS", "20"))

    def __post_init__(self) -> None:
        if self.chan_lifecycle_observer_stale_seconds <= 0:
            raise RuntimeError("CHAN_LIFECYCLE_OBSERVER_STALE_SECONDS must be greater than zero")
        if self.app_env.strip().lower() == "production":
            _validate_production_token("API_TOKEN", self.api_token)
            if self.admin_api_token:
                _validate_production_token("ADMIN_API_TOKEN", self.admin_api_token)


@lru_cache
def get_settings() -> Settings:
    return Settings()


_PUBLIC_API_TOKENS = {
    "dev-local-token",
    "change-me",
    "change-me-before-long-running",
    "change-me-long-random-token",
    "replace-me",
    "your-api-token",
}


def _validate_production_token(name: str, token: str) -> None:
    normalized = token.strip().lower()
    if (
        not normalized
        or normalized in _PUBLIC_API_TOKENS
        or any(value in normalized for value in ("dev-local-token", "change-me", "replace-me"))
    ):
        raise RuntimeError(f"{name} must be a non-placeholder secret when APP_ENV=production")
    if len(token) < 32 or len(set(token)) < 12:
        raise RuntimeError(f"{name} must be a high-entropy secret when APP_ENV=production")
