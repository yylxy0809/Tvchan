from pathlib import Path

import pytest

from app.engine.phase_1_20r import build_preflight_manifest


def test_preflight_requires_all_source_artifacts(tmp_path: Path):
    present = tmp_path / "present.json"
    present.write_text('{"value": 1}\n', encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="missing.json"):
        build_preflight_manifest([present, tmp_path / "missing.json"])


def test_preflight_records_file_facts(tmp_path: Path):
    source = tmp_path / "source.jsonl"
    source.write_text('{"alpha": 1}\n{"beta": 2}\n', encoding="utf-8")
    result = build_preflight_manifest([source])
    record = result["artifacts"][0]
    assert record["absolute_path"] == str(source.resolve())
    assert record["line_count"] == 2
    assert record["schema_keys"] == ["alpha", "beta"]
    assert len(record["sha256"]) == 64
