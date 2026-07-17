from __future__ import annotations

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


def test_lifecycle_observer_name_can_be_overridden(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_LIFECYCLE_OBSERVER", "canonical-observer")

    assert Settings().chan_lifecycle_observer == "canonical-observer"
