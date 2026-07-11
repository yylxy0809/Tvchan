from __future__ import annotations

from pathlib import Path


def test_collector_image_includes_the_collector_owned_module_c_adapter() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.collector"
    content = dockerfile.read_text(encoding="utf-8")
    assert "COPY services/collector /app/services/collector" in content
    assert "services/chan-service" not in content
