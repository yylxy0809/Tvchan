from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from collector import parquet_bootstrap_audit as audit_module


def test_audit_collects_inventory_and_schema_samples(tmp_path, monkeypatch) -> None:
    first_zip = tmp_path / "2024.zip"
    second_zip = tmp_path / "2025.zip"
    _write_zip(
        first_zip,
        {
            "20240102.parquet": b"first",
            "20240103.parquet": b"second",
            "README.txt": b"ignore",
        },
    )
    _write_zip(
        second_zip,
        {
            "20250106.parquet": b"third",
        },
    )

    sample_columns = {
        (str(first_zip), "20240102.parquet"): list(audit_module.REQUIRED_COLUMNS),
        (str(second_zip), "20250106.parquet"): list(audit_module.REQUIRED_COLUMNS),
    }

    monkeypatch.setattr(
        audit_module,
        "_read_member_columns",
        lambda zip_path, member_name: sample_columns[(str(zip_path), member_name)],
    )

    result = audit_module.audit_parquet_bootstrap_root(tmp_path, sample_size=1)

    assert result.root == str(tmp_path.resolve())
    assert result.zip_count == 2
    assert result.member_count == 3
    assert result.years == [2024, 2025]
    assert result.required_columns_ok is True
    assert result.trade_time_semantics == "bar_end"
    assert result.trade_time_offset_minutes == 0
    assert result.timezone_adjustment == "none"
    assert result.sample_errors == []
    assert [member.member_name for member in result.sample_members] == [
        "20240102.parquet",
        "20250106.parquet",
    ]


def test_audit_records_missing_columns_and_sample_errors(tmp_path, monkeypatch) -> None:
    zip_path = tmp_path / "2026.zip"
    _write_zip(
        zip_path,
        {
            "20260105.parquet": b"a",
            "20260106.parquet": b"b",
        },
    )

    def fake_reader(zip_file, member_name):
        if member_name == "20260105.parquet":
            return [
                "code",
                "trade_time",
                "open",
                "high",
                "low",
                "close",
                "vol",
            ]
        raise ValueError("bad parquet payload")

    monkeypatch.setattr(audit_module, "_read_member_columns", fake_reader)

    result = audit_module.audit_parquet_bootstrap_root(tmp_path, sample_size=2)

    assert result.zip_count == 1
    assert result.member_count == 2
    assert result.required_columns_ok is False
    assert result.sample_members[0].member_name == "20260105.parquet"
    assert result.sample_members[0].missing_required_columns == ["amount"]
    assert result.sample_errors[0].member_name == "20260106.parquet"
    assert "bad parquet payload" in result.sample_errors[0].error


def test_read_member_columns_requires_pyarrow_and_pandas(tmp_path, monkeypatch) -> None:
    zip_path = tmp_path / "2026.zip"
    _write_zip(zip_path, {"20260105.parquet": b"not-a-real-parquet"})

    def fake_import_module(name: str):
        if name == "pyarrow.parquet":
            raise ModuleNotFoundError(name)
        if name == "pandas":
            raise ModuleNotFoundError(name)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(audit_module.importlib, "import_module", fake_import_module)

    with pytest.raises(RuntimeError, match="pyarrow, pandas"):
        audit_module._read_member_columns(zip_path, "20260105.parquet")


def test_main_outputs_json(tmp_path, monkeypatch, capsys) -> None:
    zip_path = tmp_path / "2026.zip"
    _write_zip(zip_path, {"20260105.parquet": b"sample"})

    monkeypatch.setattr(
        audit_module,
        "_read_member_columns",
        lambda zip_file, member_name: list(audit_module.REQUIRED_COLUMNS),
    )

    exit_code = audit_module.main(["--root", str(tmp_path), "--sample-size", "1"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["root"] == str(tmp_path.resolve())
    assert payload["zip_count"] == 1
    assert payload["member_count"] == 1
    assert payload["required_columns_ok"] is True
    assert payload["trade_time_semantics"] == "bar_end"


def _write_zip(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for member_name, payload in members.items():
            archive.writestr(member_name, payload)
