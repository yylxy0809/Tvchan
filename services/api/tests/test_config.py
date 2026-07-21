from __future__ import annotations

from pathlib import Path

import pytest

from app.core.config import Settings, get_settings
from app.main import create_app


def test_development_allows_local_default_token() -> None:
    assert Settings(app_env="development", api_token="dev-local-token").api_token == "dev-local-token"


@pytest.mark.parametrize("token", ["", "dev-local-token", "change-me", "replace-me", "a" * 32])
def test_production_rejects_missing_placeholder_and_low_entropy_api_tokens(token: str) -> None:
    with pytest.raises(RuntimeError, match="API_TOKEN must be"):
        Settings(app_env="production", api_token=token)


def test_production_accepts_high_entropy_api_token() -> None:
    token = "uy7RK4p9wQ2xM6zB1cD8fG3hJ5kL0nP!"
    assert Settings(app_env="production", api_token=token).api_token == token


@pytest.mark.parametrize(
    "token",
    ["dev-local-token", "change-me-admin-token", "replace-me", "b" * 32],
)
def test_production_rejects_placeholder_and_low_entropy_admin_tokens(token: str) -> None:
    with pytest.raises(RuntimeError, match="ADMIN_API_TOKEN must be"):
        Settings(
            app_env="production",
            api_token="uy7RK4p9wQ2xM6zB1cD8fG3hJ5kL0nP!",
            admin_api_token=token,
        )


def test_production_allows_empty_or_high_entropy_admin_token() -> None:
    api_token = "uy7RK4p9wQ2xM6zB1cD8fG3hJ5kL0nP!"
    admin_token = "Q8!rT2wY6pL9sD4fG7hJ1kZ5xC3vB0nM"
    assert Settings(app_env="production", api_token=api_token).admin_api_token == ""
    assert Settings(
        app_env="production",
        api_token=api_token,
        admin_api_token=admin_token,
    ).admin_api_token == admin_token


@pytest.mark.parametrize("app_env", ["development", "test", "production"])
def test_all_environments_reject_identical_api_and_admin_tokens(app_env: str) -> None:
    token = "uy7RK4p9wQ2xM6zB1cD8fG3hJ5kL0nP!"

    with pytest.raises(RuntimeError, match="ADMIN_API_TOKEN must differ from API_TOKEN"):
        Settings(app_env=app_env, api_token=token, admin_api_token=token)


def test_create_app_fails_fast_for_production_placeholder_token(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("API_TOKEN", "dev-local-token")
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="non-placeholder secret"):
            create_app()
    finally:
        get_settings.cache_clear()


def test_create_app_fails_fast_for_production_placeholder_admin_token(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("API_TOKEN", "uy7RK4p9wQ2xM6zB1cD8fG3hJ5kL0nP!")
    monkeypatch.setenv("ADMIN_API_TOKEN", "change-me-admin-token")
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="ADMIN_API_TOKEN must be"):
            create_app()
    finally:
        get_settings.cache_clear()


def test_create_app_fails_fast_for_identical_api_and_admin_tokens(monkeypatch) -> None:
    token = "uy7RK4p9wQ2xM6zB1cD8fG3hJ5kL0nP!"
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("API_TOKEN", token)
    monkeypatch.setenv("ADMIN_API_TOKEN", token)
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="ADMIN_API_TOKEN must differ from API_TOKEN"):
            create_app()
    finally:
        get_settings.cache_clear()


def test_lifecycle_observer_name_can_be_overridden(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_LIFECYCLE_OBSERVER", "canonical-observer")

    assert Settings().chan_lifecycle_observer == "canonical-observer"


def test_lifecycle_observer_stale_threshold_can_be_overridden(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_LIFECYCLE_OBSERVER_STALE_SECONDS", "180")

    assert Settings().chan_lifecycle_observer_stale_seconds == 180


def test_lifecycle_observer_stale_threshold_must_be_positive(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_LIFECYCLE_OBSERVER_STALE_SECONDS", "0")

    with pytest.raises(RuntimeError, match="must be greater than zero"):
        Settings()


def test_database_pool_sizes_are_bounded_and_configurable(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "6")

    settings = Settings()

    assert settings.database_pool_min_size == 2
    assert settings.database_pool_max_size == 6


@pytest.mark.parametrize(
    ("minimum", "maximum", "message"),
    [("0", "8", "MIN_SIZE"), ("4", "3", "MAX_SIZE")],
)
def test_database_pool_sizes_fail_fast(monkeypatch, minimum: str, maximum: str, message: str) -> None:
    monkeypatch.setenv("DATABASE_POOL_MIN_SIZE", minimum)
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", maximum)

    with pytest.raises(RuntimeError, match=message):
        Settings()


def test_database_pool_maximum_has_a_hard_upper_bound(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "33")

    with pytest.raises(RuntimeError, match="at most 32"):
        Settings()


def test_websocket_access_log_omits_query_arguments() -> None:
    nginx = (
        Path(__file__).resolve().parents[3] / "deploy/nginx.tv.conf"
    ).read_text(encoding="utf-8")

    assert '"$request_method $uri $server_protocol"' in nginx
    assert "access_log /var/log/nginx/ws_access.log ws_no_args;" in nginx
    ws_location = nginx.split("location /ws/ {", 1)[1].split("}", 1)[0]
    assert "$request_uri" not in ws_location
    assert "$args" not in ws_location
