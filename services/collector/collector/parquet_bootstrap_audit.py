from __future__ import annotations

import argparse
import importlib
import io
import json
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence


REQUIRED_COLUMNS = (
    "code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)


@dataclass(frozen=True)
class SampleMember:
    zip_path: str
    member_name: str
    columns: list[str]
    missing_required_columns: list[str]
    required_columns_present: bool


@dataclass(frozen=True)
class SampleError:
    zip_path: str
    member_name: str | None
    error: str


@dataclass(frozen=True)
class BootstrapAuditResult:
    root: str
    zip_count: int
    member_count: int
    years: list[int]
    required_columns_ok: bool
    sample_errors: list[SampleError]
    sample_members: list[SampleMember]
    trade_time_semantics: str = "bar_end"
    trade_time_offset_minutes: int = 0
    timezone_adjustment: str = "none"

    def to_dict(self) -> dict:
        return asdict(self)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lightweight parquet bootstrap audit for yearly zip sources"
    )
    parser.add_argument("--root", required=True, help="Root directory containing yearly *.zip files")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1,
        help="Parquet members to sample per zip when validating schema",
    )
    return parser.parse_args(argv)


def audit_parquet_bootstrap_root(
    root: str | Path,
    *,
    sample_size: int = 1,
) -> BootstrapAuditResult:
    if sample_size < 0:
        raise ValueError("sample_size must be >= 0")

    root_path = Path(root).expanduser().resolve()
    if not root_path.exists():
        raise FileNotFoundError(f"Parquet bootstrap root does not exist: {root_path}")
    if not root_path.is_dir():
        raise NotADirectoryError(f"Parquet bootstrap root is not a directory: {root_path}")

    zip_paths = sorted(root_path.glob("*.zip"))
    years = sorted(
        {
            int(path.stem)
            for path in zip_paths
            if len(path.stem) == 4 and path.stem.isdigit()
        }
    )

    member_count = 0
    sample_errors: list[SampleError] = []
    sample_members: list[SampleMember] = []

    for zip_path in zip_paths:
        try:
            with zipfile.ZipFile(zip_path) as archive:
                parquet_entries = [
                    entry
                    for entry in archive.infolist()
                    if not entry.is_dir() and entry.filename.lower().endswith(".parquet")
                ]
        except Exception as exc:
            sample_errors.append(
                SampleError(
                    zip_path=str(zip_path),
                    member_name=None,
                    error=f"Failed to inspect zip: {exc}",
                )
            )
            continue

        member_count += len(parquet_entries)
        if sample_size == 0:
            continue

        for entry in parquet_entries[:sample_size]:
            try:
                columns = _read_member_columns(zip_path, entry.filename)
                missing_columns = [
                    column for column in REQUIRED_COLUMNS if column not in set(columns)
                ]
                sample_members.append(
                    SampleMember(
                        zip_path=str(zip_path),
                        member_name=entry.filename,
                        columns=columns,
                        missing_required_columns=missing_columns,
                        required_columns_present=not missing_columns,
                    )
                )
            except Exception as exc:
                sample_errors.append(
                    SampleError(
                        zip_path=str(zip_path),
                        member_name=entry.filename,
                        error=str(exc),
                    )
                )

    required_columns_ok = bool(sample_members) and not sample_errors and all(
        member.required_columns_present for member in sample_members
    )

    return BootstrapAuditResult(
        root=str(root_path),
        zip_count=len(zip_paths),
        member_count=member_count,
        years=years,
        required_columns_ok=required_columns_ok,
        sample_errors=sample_errors,
        sample_members=sample_members,
    )


def _read_member_columns(zip_path: str | Path, member_name: str) -> list[str]:
    parquet_module = _require_parquet_module()
    with zipfile.ZipFile(zip_path) as archive:
        payload = archive.read(member_name)
    schema = parquet_module.read_schema(io.BytesIO(payload))
    return [str(name) for name in schema.names]


def _require_parquet_module():
    missing: list[str] = []
    try:
        parquet_module = importlib.import_module("pyarrow.parquet")
    except ModuleNotFoundError:
        parquet_module = None
        missing.append("pyarrow")

    try:
        importlib.import_module("pandas")
    except ModuleNotFoundError:
        missing.append("pandas")

    if missing:
        missing_text = ", ".join(missing)
        raise RuntimeError(
            "parquet bootstrap audit requires installed parquet dependencies: "
            f"{missing_text}"
        )

    return parquet_module


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = audit_parquet_bootstrap_root(args.root, sample_size=args.sample_size)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
