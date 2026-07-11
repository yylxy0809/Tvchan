from __future__ import annotations

from pathlib import Path

from collector.providers.factory import parse_provider_names


def test_mandatory_collector_and_api_requirements_do_not_require_mootdx() -> None:
    root = Path(__file__).resolve().parents[3]
    collector_requirements = (root / "services" / "collector" / "requirements.txt").read_text(encoding="utf-8")
    api_requirements = (root / "services" / "api" / "requirements.txt").read_text(encoding="utf-8")

    assert "mootdx" not in collector_requirements.lower()
    assert "mootdx" not in api_requirements.lower()
    assert "httpx==0.28.1" in collector_requirements
    assert "httpx==0.28.1" in api_requirements


def test_default_provider_configuration_does_not_require_mootdx() -> None:
    assert parse_provider_names(None) == ["pytdx"]
    assert parse_provider_names("") == ["pytdx"]
