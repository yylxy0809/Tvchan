from __future__ import annotations

from pathlib import Path


def test_collector_image_includes_the_collector_owned_module_c_adapter() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.collector"
    content = dockerfile.read_text(encoding="utf-8")
    assert "COPY services/collector /app/services/collector" in content
    assert "services/chan-service" not in content


def test_collector_image_installs_the_pinned_westock_cli_with_registry_integrity() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.collector"
    content = dockerfile.read_text(encoding="utf-8")
    assert "westock-data-clawhub@1.0.4" in content
    assert "sha512-Cr4IS69wJ6aFdaDv7Sh/Zwf1FEj+8BHxegIltjWg4bswjV2SfbG9VmM0YN4SwfaLJlP1INzM0Ed3LXP+3WpjSA==" in content
    assert "npx" not in content
