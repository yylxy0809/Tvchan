from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from app.engine.time_utils import utc_time
from app.repositories.historical_lifecycle_repo import HistoricalLifecycleScopeError, validate_stats


class HistoricalLifecycleDatasetError(RuntimeError):
    pass


def _iso(value: Any) -> Any:
    return utc_time(value).isoformat() if isinstance(value, datetime) else value


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def historical_lifecycle_record(row: dict[str, Any]) -> dict[str, Any]:
    event_type = str(row["event_type"])
    return {
        "dataset_class": "official",
        "record_kind": "event",
        "event_id": int(row["id"]),
        "fingerprint": str(row["fingerprint"]),
        "symbol_id": int(row["symbol_id"]),
        "chan_level": int(row["chan_level"]),
        "structure_type": str(row["structure_type"]),
        "side_or_direction": row.get("side_or_direction"),
        "bsp_type": row.get("bsp_type"),
        "price_x1000": row.get("price_x1000"),
        "event_type": event_type,
        "effective_time": _iso(row["effective_time"]),
        "observed_time": _iso(row["observed_time"]),
        "point_time": _iso(row["point_time"]),
        "first_seen_time": _iso(row["effective_time"]) if event_type == "first_seen" else None,
        "confirm_time": _iso(row["effective_time"]) if event_type == "confirmed" else None,
        "current_mode": row.get("current_mode"),
        "run_id": row.get("run_id"),
        "publication_profile": "historical_replay",
        "provenance": _mapping(row.get("provenance")),
    }


def encode_historical_lifecycle_record(row: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            historical_lifecycle_record(row),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sync_file(handle: Any) -> None:
    handle.flush()
    os.fsync(handle.fileno())


async def export_historical_lifecycle_dataset(
    *,
    snapshot: Any,
    effective_cutoff: datetime,
    output_dir: Path,
    prefetch: int = 1000,
) -> dict[str, Any]:
    cutoff = utc_time(effective_cutoff)
    stats = {key: int(value or 0) for key, value in snapshot.stats.items()}
    try:
        validate_stats(stats)
    except HistoricalLifecycleScopeError as exc:
        raise HistoricalLifecycleDatasetError(str(exc)) from exc

    parent_dir = output_dir.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    if output_dir.exists():
        if (output_dir / "manifest.json").exists():
            raise HistoricalLifecycleDatasetError(
                "Output directory already contains a published dataset"
            )
        owned_orphan_names = {"official.jsonl", "official.jsonl.tmp", "manifest.json.tmp"}
        if any(path.name not in owned_orphan_names for path in output_dir.iterdir()):
            raise HistoricalLifecycleDatasetError(
                "Output directory contains unrecognized incomplete artifacts"
            )
        shutil.rmtree(output_dir)
    staging_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.staging-", dir=parent_dir)
    )
    output_path = staging_dir / "official.jsonl"
    output_tmp = staging_dir / "official.jsonl.tmp"
    manifest_path = staging_dir / "manifest.json"
    manifest_tmp = staging_dir / "manifest.json.tmp"

    digest = hashlib.sha256()
    levels: Counter[str] = Counter()
    event_types: Counter[str] = Counter()
    row_count = 0
    min_effective = max_effective = None
    min_observed = max_observed = None
    try:
        with output_tmp.open("wb") as handle:
            async for raw in snapshot.events(prefetch=prefetch):
                row = dict(raw)
                effective = utc_time(row["effective_time"])
                observed = utc_time(row["observed_time"])
                if str(row.get("publication_profile")) != "historical_replay":
                    raise HistoricalLifecycleDatasetError("Non-historical event reached official export")
                if effective > cutoff:
                    raise HistoricalLifecycleDatasetError("Future-effective event reached official export")
                if observed < effective:
                    raise HistoricalLifecycleDatasetError("Lifecycle event clock order is invalid")
                if utc_time(row["point_time"]) > effective:
                    raise HistoricalLifecycleDatasetError(
                        "Lifecycle structure point is later than its causal cutoff"
                    )
                encoded = encode_historical_lifecycle_record(row)
                handle.write(encoded)
                digest.update(encoded)
                row_count += 1
                levels[str(int(row["chan_level"]))] += 1
                event_types[str(row["event_type"])] += 1
                min_effective = effective if min_effective is None else min(min_effective, effective)
                max_effective = effective if max_effective is None else max(max_effective, effective)
                min_observed = observed if min_observed is None else min(min_observed, observed)
                max_observed = observed if max_observed is None else max(max_observed, observed)
            _sync_file(handle)
        if row_count != stats.get("row_count", 0):
            raise HistoricalLifecycleDatasetError("Streamed row count does not match snapshot stats")

        manifest = {
            "schema_version": "historical-lifecycle-dataset-v1",
            "dataset_kind": "historical_replay_effective_time",
            "dataset_validation": "PASS",
            "cutoff_basis": "effective_time",
            "effective_cutoff": cutoff.isoformat(),
            **snapshot.scope.manifest(),
            "publication_profile": "historical_replay",
            "row_count": row_count,
            "counts_by_level": dict(sorted(levels.items(), key=lambda item: int(item[0]))),
            "counts_by_event_type": dict(sorted(event_types.items())),
            "min_effective_time": _iso(min_effective),
            "max_effective_time": _iso(max_effective),
            "min_observed_time": _iso(min_observed),
            "max_observed_time": _iso(max_observed),
            "observed_after_cutoff_count": stats.get("observed_after_cutoff_count", 0),
            "effective_after_cutoff_count": 0,
            "invalid_clock_count": stats.get("invalid_clock_count", 0),
            "non_scope_count": stats.get("non_scope_count", 0),
            "official_jsonl_sha256": digest.hexdigest(),
            "source_contract": "sealed_historical_replay_event_ledger_v1",
            "strategy_decision": "NOT_EVALUATED",
        }
        with manifest_tmp.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            _sync_file(handle)
        os.replace(output_tmp, output_path)
        os.replace(manifest_tmp, manifest_path)
        os.replace(staging_dir, output_dir)
        return manifest
    except Exception:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        raise
