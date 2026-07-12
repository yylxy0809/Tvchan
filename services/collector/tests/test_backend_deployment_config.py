from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_market_data_profile_uses_runtime_contract_environment_and_fails_once() -> None:
    compose = (ROOT / "deploy" / "docker-compose.backend.yml").read_text(encoding="utf-8")

    assert 'profiles: ["market-data"]' in compose
    assert "MARKET_DATA_PROVIDER_FACTORY: ${MARKET_DATA_PROVIDER_FACTORY:-collector.market_data.factory:create_market_data_provider}" in compose
    assert "MARKET_DATA_DEMAND_REPOSITORY_FACTORY" in compose
    assert "\n      DEMAND_REPOSITORY_FACTORY:" not in compose
    assert "WESTOCK_NORMALIZER_FACTORY" in compose
    assert "IWENCAI_MASTER_DATA_RESOLVER_FACTORY" in compose
    assert "IWENCAI_TRANSPORT_FACTORY" in compose
    assert "IWENCAI_REQUEST_BUILDER_FACTORY" in compose
    assert "IWENCAI_RESPONSE_PARSER_FACTORY" in compose
    assert "IWENCAI_ALLOWED_HOSTS: ${IWENCAI_ALLOWED_HOSTS:-openapi.iwencai.com}" in compose
    assert "IWENCAI_TIMEOUT_SECONDS" in compose
    assert 'restart: "no"' in compose
    assert "pinned WESTOCK_BRIDGE_SHA256" in compose


def test_production_api_token_is_not_defaulted_or_allowed_as_a_placeholder() -> None:
    compose = (ROOT / "deploy" / "docker-compose.backend.yml").read_text(encoding="utf-8")
    example = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "API_TOKEN: ${API_TOKEN:-}" in compose
    assert "API_TOKEN must be a non-placeholder secret when APP_ENV=production" in compose
    assert "dev-local-token" not in example
    assert "change-me" not in example


def test_market_data_env_examples_match_allowed_hosts_contract() -> None:
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    backend_example = (ROOT / "deploy" / "backend.env.example").read_text(encoding="utf-8")

    assert "IWENCAI_ALLOWED_HOSTS=openapi.iwencai.com" in example
    assert "IWENCAI_ALLOWED_HOSTS=openapi.iwencai.com" in backend_example
    assert "ADMIN_API_TOKEN=\n" in backend_example
    assert "set a high-entropy" in backend_example


def test_default_compose_has_no_windows_tablespace_path() -> None:
    compose = (ROOT / "deploy" / "docker-compose.backend.yml").read_text(encoding="utf-8")
    windows_override = (ROOT / "deploy" / "docker-compose.backend.windows.yml").read_text(encoding="utf-8")

    assert "G:/" not in compose
    assert "POSTGRES_CHAN_C_TABLESPACE_HOST_PATH:?" in windows_override
