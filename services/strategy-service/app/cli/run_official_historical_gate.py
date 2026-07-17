from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from app.db import create_pool
from app.engine.official_historical_gate import build_official_historical_gate
from app.engine.historical_lifecycle_dataset import encode_historical_lifecycle_record
from app.engine.time_utils import utc_time
from app.repositories.historical_lifecycle_repo import HistoricalLifecycleRepository


def _write_text(path: Path, content: str) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _write_json(path: Path, payload: object) -> None:
    _write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_dataset_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Historical lifecycle dataset manifest must be an object")
    return payload


def _validate_dataset_manifest(
    *,
    manifest: dict[str, Any],
    manifest_path: Path,
    scope: dict[str, Any],
    as_of_time: datetime,
    stats: dict[str, int],
    levels: list[dict[str, Any]],
    snapshot_jsonl_sha256: str,
) -> str:
    expected_scope = {
        key: scope[key]
        for key in (
            "replay_batch_id",
            "source_batch_id",
            "contract_hash",
            "scope_hash",
            "eligible_universe_snapshot_id",
            "canonical_gate_snapshot_id",
        )
    }
    if manifest.get("dataset_validation") != "PASS":
        raise ValueError("Historical lifecycle dataset is not validated")
    if manifest.get("schema_version") != "historical-lifecycle-dataset-v1":
        raise ValueError("Historical lifecycle dataset schema is invalid")
    if manifest.get("dataset_kind") != "historical_replay_effective_time":
        raise ValueError("Historical lifecycle dataset kind is invalid")
    if manifest.get("cutoff_basis") != "effective_time":
        raise ValueError("Historical lifecycle dataset cutoff basis is invalid")
    if manifest.get("effective_cutoff") != utc_time(as_of_time).isoformat():
        raise ValueError("Historical lifecycle dataset cutoff does not match the gate")
    if any(manifest.get(key) != value for key, value in expected_scope.items()):
        raise ValueError("Historical lifecycle dataset scope does not match the gate")
    if (
        manifest.get("source_contract") != "sealed_historical_replay_event_ledger_v1"
        or manifest.get("publication_profile") != "historical_replay"
    ):
        raise ValueError("Historical lifecycle dataset source contract is invalid")
    for field in (
        "effective_after_cutoff_count",
        "invalid_clock_count",
        "non_scope_count",
    ):
        if int(manifest.get(field, -1)) != 0:
            raise ValueError(f"Historical lifecycle dataset {field} is invalid")
    expected_levels = {
        str(int(row["chan_level"])): int(row["event_count"])
        for row in levels
    }
    if manifest.get("counts_by_level") != expected_levels:
        raise ValueError("Historical lifecycle dataset level counts do not match the gate")
    if int(manifest.get("row_count", -1)) != int(stats.get("row_count", -2)):
        raise ValueError("Historical lifecycle dataset row count does not match the gate")
    expected_hash = str(manifest.get("official_jsonl_sha256", ""))
    if len(expected_hash) != 64 or any(char not in "0123456789abcdef" for char in expected_hash):
        raise ValueError("Historical lifecycle dataset content hash is invalid")
    dataset_path = manifest_path.parent / "official.jsonl"
    if not dataset_path.is_file() or _sha256_file(dataset_path) != expected_hash:
        raise ValueError("Historical lifecycle dataset content does not match its manifest")
    if snapshot_jsonl_sha256 != expected_hash:
        raise ValueError("Historical lifecycle dataset content does not match the DB snapshot")
    return expected_hash


async def _hash_snapshot_events(snapshot: Any, *, prefetch: int = 1000) -> str:
    digest = hashlib.sha256()
    async for raw in snapshot.events(prefetch=prefetch):
        digest.update(encode_historical_lifecycle_record(dict(raw)))
    return digest.hexdigest()


def _markdown(title: str, payload: dict) -> str:
    lines = [
        f"# {title}",
        "",
        f"- Decision: `{payload['decision']}`",
        f"- Input hash: `{payload['input_hash']}`",
        "",
        "## Gate waterfall",
        "",
    ]
    lines.extend(f"- `{row['gate']}`: `{row['count']}`" for row in payload["gate_stages"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{item}`" for item in payload["blockers"])
    return "\n".join(lines) + "\n"


def _build_official_dataset_manifest(
    report: dict,
    *,
    scope: dict[str, Any],
    as_of_time: str,
    dataset_manifest: dict[str, Any] | None = None,
) -> dict:
    return {
        **(dataset_manifest or {}),
        "as_of_time": as_of_time,
        "cutoff_basis": "effective_time",
        "publication_profile": "historical_replay",
        "gate_counts_by_level": report["official_events_by_level"],
        "replay_batch_id": scope["replay_batch_id"],
        "source_batch_id": scope["source_batch_id"],
        "contract_hash": scope["contract_hash"],
        "scope_hash": scope["scope_hash"],
        "decision": report["decision"],
        "input_hash": report["input_hash"],
    }


def _publish_gate_artifacts(
    *,
    output_dir: Path,
    report: dict[str, Any],
    scope: dict[str, Any],
    cutoff: datetime,
    dataset_manifest: dict[str, Any],
    failures: list[str],
) -> None:
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists():
        if any(output_dir.iterdir()):
            raise ValueError("Gate output directory must be empty")
        output_dir.rmdir()
    staging_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.staging-", dir=output_dir.parent)
    )
    try:
        _write_json(staging_dir / "gate_waterfall.json", report)
        _write_text(
            staging_dir / "gate_waterfall.md",
            _markdown("Official Historical Gate Waterfall", report),
        )
        fail_rows = [
            {
                "symbol": symbol,
                "failed_gate": "predictive_weekly_b2_visible",
                "reason": "official_predictive_weekly_b2_unavailable",
            }
            for symbol in failures
        ]
        _write_text(
            staging_dir / "fail_samples.jsonl",
            "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in fail_rows),
        )
        _write_text(staging_dir / "candidate_samples.jsonl", "")
        manifest = _build_official_dataset_manifest(
            report,
            scope=scope,
            as_of_time=cutoff.isoformat(),
            dataset_manifest=dataset_manifest,
        )
        _write_json(staging_dir / "official_dataset_manifest.json", manifest)
        _write_json(
            staging_dir / "diagnostic_dataset_manifest.json",
            {"as_of_time": cutoff.isoformat(), "count": 0, "excluded_from_official": True},
        )
        metrics = {
            "strategy_code": report["strategy_code"],
            "status": "not_run",
            "trade_count": 0,
            "metrics": None,
            "reason": "official_event_replay_not_implemented",
            "input_hash": report["input_hash"],
            "decision": report["decision"],
        }
        _write_json(staging_dir / "event_replay_metrics.json", metrics)
        _write_text(
            staging_dir / "event_replay_metrics.md",
            "# Official Event Replay\n\n- Status: `not_run`\n"
            "- Reason: `official_event_replay_not_implemented`\n- Trade count: `0`\n"
            f"- Decision: `{report['decision']}`\n",
        )
        _write_json(
            staging_dir / "next_phase_decision.json",
            {
                "decision": report["decision"],
                "blockers": report["blockers"],
                "input_hash": report["input_hash"],
            },
        )
        trace_dir = staging_dir / "trace"
        trace_dir.mkdir()
        for index, item in enumerate(fail_rows[:3], start=1):
            trace = {
                **item,
                "trace_status": "incomplete",
                "unavailable_from_gate": "predictive_weekly_b2_visible",
                "official": True,
            }
            _write_json(trace_dir / f"rejection-{index}.json", trace)
            _write_text(
                trace_dir / f"rejection-{index}.md",
                f"# Rejection Trace {index}\n\n- Symbol: `{item['symbol']}`\n"
                "- Failed gate: `predictive_weekly_b2_visible`\n"
                "- Complete candidate trace: `unavailable`\n",
            )
        artifact_hashes = {
            path.relative_to(staging_dir).as_posix(): _sha256_file(path)
            for path in sorted(staging_dir.rglob("*"))
            if path.is_file()
        }
        _write_json(
            staging_dir / "gate-complete.json",
            {
                "schema_version": "official-historical-gate-complete-v1",
                "input_hash": report["input_hash"],
                "official_jsonl_sha256": dataset_manifest["official_jsonl_sha256"],
                "artifacts": artifact_hashes,
            },
        )
        os.replace(staging_dir, output_dir)
    except Exception:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        raise


async def _run(
    *,
    replay_batch_id: int,
    expected_contract_hash: str,
    as_of_time: datetime,
    output_dir: Path,
    dataset_manifest_path: Path,
) -> dict:
    cutoff = utc_time(as_of_time)
    dataset_manifest = _load_dataset_manifest(dataset_manifest_path)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("Gate output directory must be empty")
    pool = await create_pool(min_size=1, max_size=1)
    try:
        repository = HistoricalLifecycleRepository(pool)
        async with repository.open_snapshot(
            replay_batch_id=replay_batch_id,
            expected_contract_hash=expected_contract_hash,
            effective_cutoff=cutoff,
        ) as snapshot:
            snapshot_jsonl_sha256 = await _hash_snapshot_events(snapshot)
            counts, levels, failures = await snapshot.gate_inputs()
            scope = snapshot.scope.manifest()
            official_jsonl_sha256 = _validate_dataset_manifest(
                manifest=dataset_manifest,
                manifest_path=dataset_manifest_path,
                scope=scope,
                as_of_time=cutoff,
                stats=snapshot.stats,
                levels=levels,
                snapshot_jsonl_sha256=snapshot_jsonl_sha256,
            )
            report = build_official_historical_gate(
                {
                    "as_of_time": cutoff.isoformat(),
                    "scope_hash": scope["scope_hash"],
                    "scope": scope,
                    "official_jsonl_sha256": official_jsonl_sha256,
                    "counts": counts,
                    "official_events_by_level": levels,
                }
            )
    finally:
        await pool.close()

    _publish_gate_artifacts(
        output_dir=output_dir,
        report=report,
        scope=scope,
        cutoff=cutoff,
        dataset_manifest=dataset_manifest,
        failures=failures,
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-batch-id", type=int, required=True)
    parser.add_argument("--expected-contract-hash", required=True)
    parser.add_argument("--as-of", type=utc_time, required=True)
    parser.add_argument("--dataset-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(
        _run(
            replay_batch_id=args.replay_batch_id,
            expected_contract_hash=args.expected_contract_hash,
            as_of_time=args.as_of,
            output_dir=args.output_dir,
            dataset_manifest_path=args.dataset_manifest,
        )
    )
    print(
        json.dumps(
            {
                "decision": report["decision"],
                "blockers": report["blockers"],
                "input_hash": report["input_hash"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
