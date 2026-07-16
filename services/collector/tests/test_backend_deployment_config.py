from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_iwencai_sidebar_worker_uses_redis_events_without_polling_or_fallback_providers() -> None:
    compose = (ROOT / "deploy" / "docker-compose.backend.yml").read_text(encoding="utf-8")

    assert "  iwencai-sidebar-event-worker:" in compose
    assert 'profiles: ["sidebar-iwencai"]' in compose
    assert 'command: ["python", "-m", "collector.worker", "iwencai-sidebar-events"]' in compose
    assert "DATABASE_URL: postgresql://${POSTGRES_USER:-trader}:${POSTGRES_PASSWORD:-trader}@timescaledb:5432/${POSTGRES_DB:-tradingview_local}" in compose
    assert "REDIS_URL: redis://redis:6379/0" in compose
    assert "MARKET_DATA_INTERVAL_SECONDS" not in compose
    assert "collector.market_data_provider" not in compose
    assert "westock" not in compose.lower()
    assert "IWENCAI_MASTER_DATA_RESOLVER_FACTORY" not in compose
    assert "IWENCAI_TRANSPORT_FACTORY" not in compose
    assert "IWENCAI_REQUEST_BUILDER_FACTORY" not in compose
    assert "IWENCAI_RESPONSE_PARSER_FACTORY" not in compose
    assert "IWENCAI_ALLOWED_HOSTS: ${IWENCAI_ALLOWED_HOSTS:-openapi.iwencai.com}" in compose
    assert "IWENCAI_TIMEOUT_SECONDS" in compose
    assert "NOTTE_API_KEY: ${NOTTE_API_KEY:-}" in compose
    assert "NOTTE_FUNCTION_ID:" in compose
    assert "MARKET_DATA_PROVIDER_ORDER:" in compose
    worker = compose[compose.index("  iwencai-sidebar-event-worker:"):compose.index("  history-backfill-worker:")]
    assert "healthcheck:" not in worker
    assert "restart: unless-stopped" in worker


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
    assert "NOTTE_API_KEY=" in example
    assert "NOTTE_API_KEY=" in backend_example
    assert "COMPOSE_PROFILES=realtime-pipeline,sidebar-iwencai" in backend_example
    assert "WESTOCK" not in example
    assert "MARKET_DATA_INTERVAL_SECONDS" not in example
    assert "IWENCAI_MASTER_DATA_RESOLVER_FACTORY" not in example
    assert "IWENCAI_TRANSPORT_FACTORY" not in example
    assert "IWENCAI_REQUEST_BUILDER_FACTORY" not in example
    assert "IWENCAI_RESPONSE_PARSER_FACTORY" not in example
    assert "WENCAI_" not in backend_example.replace("IWENCAI_", "")
    assert "ADMIN_API_TOKEN=\n" in backend_example
    assert "set a high-entropy" in backend_example


def test_default_compose_has_no_windows_tablespace_path() -> None:
    compose = (ROOT / "deploy" / "docker-compose.backend.yml").read_text(encoding="utf-8")
    windows_override = (ROOT / "deploy" / "docker-compose.backend.windows.yml").read_text(encoding="utf-8")

    assert "G:/" not in compose
    assert "POSTGRES_CHAN_C_TABLESPACE_HOST_PATH:?" in windows_override


def test_iwencai_sidebar_events_worker_is_registered() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "collector.worker", "--list"],
        cwd=ROOT / "services" / "collector",
        check=True,
        capture_output=True,
        text=True,
    )
    assert "iwencai-sidebar-events\t" in result.stdout
