from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    app_env: str = os.getenv("APP_ENV", "development")
    api_token: str = os.getenv("API_TOKEN", "dev-local-token")
    admin_api_token: str = os.getenv("ADMIN_API_TOKEN", "")
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
    iwencai_api_key: str = os.getenv("IWENCAI_API_KEY", "")
    wencai_user_agent: str = os.getenv("WENCAI_USER_AGENT", "")
    wencai_pro: bool = os.getenv("WENCAI_PRO", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    wencai_timeout_seconds: float = float(os.getenv("WENCAI_TIMEOUT_SECONDS", "20"))


@lru_cache
def get_settings() -> Settings:
    return Settings()
