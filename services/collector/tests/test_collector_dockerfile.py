from __future__ import annotations

from pathlib import Path


def test_collector_image_includes_the_collector_owned_module_c_adapter() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.collector"
    content = dockerfile.read_text(encoding="utf-8")
    assert "COPY services/collector /app/services/collector" in content
    assert "services/chan-service" not in content


def test_collector_image_has_no_node_or_westock_bridge() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.collector"
    content = dockerfile.read_text(encoding="utf-8")
    assert "nodejs" not in content
    assert "npm" not in content
    assert "westock" not in content.lower()


def test_api_image_has_no_node_or_westock_bridge() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.api"
    content = dockerfile.read_text(encoding="utf-8")
    assert "nodejs" not in content
    assert "npm" not in content
    assert "westock" not in content.lower()
