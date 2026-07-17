import json
from datetime import UTC, datetime

import pytest

from app.cli.run_official_historical_gate import (
    _build_official_dataset_manifest,
    _publish_gate_artifacts,
    _validate_dataset_manifest,
)
import app.cli.run_official_historical_gate as gate_cli
from app.engine.official_historical_gate import build_official_historical_gate


def bound_scope(seed: str = "scope") -> dict:
    import hashlib

    payload = {
        "replay_batch_id": 9,
        "source_batch_id": 6,
        "contract_version": "historical-replay-v1",
        "contract_hash": "a" * 64,
        "run_group_id": "historical-replay-full",
        "publication_namespace": "historical-replay",
        "profile_id": "module-c-historical-replay-v1",
        "config_hash": seed,
        "eligible_universe_snapshot_id": "eligible",
        "canonical_gate_snapshot_id": "gate",
        "contract_cutoff": "2026-07-03T07:00:00+00:00",
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return {**payload, "scope_hash": hashlib.sha256(encoded.encode()).hexdigest()}


def test_gate_fails_closed_when_predictive_weekly_b2_is_unavailable():
    report = build_official_historical_gate({
        "as_of_time": "2026-07-03T07:00:00+00:00",
        "counts": {
            "source_high_level_eligible": 5531,
            "official_high_level_visible": 5531,
            "intraday_eligible": 61,
            "predictive_weekly_b1": 4,
            "predictive_weekly_b2": 0,
            "strict_daily_episodes": 0,
            "official_30f_confirmations": 0,
            "official_5f_confirmations": 0,
            "official_candidates": 0,
        },
        "official_events_by_level": [],
    })

    assert report["decision"] == "NO_GO"
    assert report["gate_counts_monotonic"] is True
    assert "official_predictive_weekly_b2_unavailable" in report["blockers"]


def test_official_manifest_uses_the_fail_closed_gate_decision_and_scope():
    manifest = _build_official_dataset_manifest({
        "decision": "NO_GO",
        "official_events_by_level": [],
        "input_hash": "c" * 64,
    }, scope={
        "replay_batch_id": 9,
        "source_batch_id": 6,
        "contract_hash": "a" * 64,
        "scope_hash": "b" * 64,
    }, as_of_time="2026-07-03T07:00:00+00:00")

    assert manifest["decision"] == "NO_GO"
    assert manifest["replay_batch_id"] == 9
    assert manifest["contract_hash"] == "a" * 64
    assert manifest["scope_hash"] == "b" * 64


def test_gate_input_hash_is_bound_to_replay_scope() -> None:
    snapshot = {
        "as_of_time": "2026-07-03T07:00:00+00:00",
        "counts": {
            "source_high_level_eligible": 10,
            "official_high_level_visible": 10,
            "intraday_eligible": 4,
            "predictive_weekly_b1": 1,
            "predictive_weekly_b2": 0,
            "strict_daily_episodes": 0,
            "official_30f_confirmations": 0,
            "official_5f_confirmations": 0,
            "official_candidates": 0,
        },
        "official_events_by_level": [{"chan_level": 1440, "event_count": 20}],
    }
    scope_a = bound_scope("a")
    scope_b = bound_scope("b")
    first = build_official_historical_gate({
        **snapshot,
        "scope_hash": scope_a["scope_hash"],
        "scope": scope_a,
        "official_jsonl_sha256": "c" * 64,
    })
    second = build_official_historical_gate({
        **snapshot,
        "scope_hash": scope_b["scope_hash"],
        "scope": scope_b,
        "official_jsonl_sha256": "c" * 64,
    })
    assert first["input_hash"] != second["input_hash"]

    third = build_official_historical_gate({
        **snapshot,
        "scope_hash": scope_a["scope_hash"],
        "scope": scope_a,
        "official_jsonl_sha256": "d" * 64,
    })
    assert first["input_hash"] != third["input_hash"]


def test_gate_validates_the_exact_exported_dataset(tmp_path) -> None:
    dataset = tmp_path / "official.jsonl"
    dataset.write_text('{"event_id":1}\n', encoding="utf-8", newline="\n")
    import hashlib

    digest = hashlib.sha256(dataset.read_bytes()).hexdigest()
    scope = {
        "replay_batch_id": 9,
        "source_batch_id": 6,
        "contract_hash": "a" * 64,
        "scope_hash": "b" * 64,
        "eligible_universe_snapshot_id": "eligible",
        "canonical_gate_snapshot_id": "gate",
    }
    cutoff = datetime(2026, 7, 3, 7, tzinfo=UTC)
    manifest = {
        "schema_version": "historical-lifecycle-dataset-v1",
        "dataset_validation": "PASS",
        "dataset_kind": "historical_replay_effective_time",
        "cutoff_basis": "effective_time",
        "source_contract": "sealed_historical_replay_event_ledger_v1",
        "publication_profile": "historical_replay",
        "effective_after_cutoff_count": 0,
        "invalid_clock_count": 0,
        "non_scope_count": 0,
        "effective_cutoff": cutoff.isoformat(),
        **scope,
        "row_count": 1,
        "counts_by_level": {"1440": 1},
        "official_jsonl_sha256": digest,
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    assert _validate_dataset_manifest(
        manifest=manifest,
        manifest_path=manifest_path,
        scope=scope,
        as_of_time=cutoff,
        stats={"row_count": 1},
        levels=[{"chan_level": 1440, "event_count": 1}],
        snapshot_jsonl_sha256=digest,
    ) == digest

    with pytest.raises(ValueError, match="DB snapshot"):
        _validate_dataset_manifest(
            manifest=manifest,
            manifest_path=manifest_path,
            scope=scope,
            as_of_time=cutoff,
            stats={"row_count": 1},
            levels=[{"chan_level": 1440, "event_count": 1}],
            snapshot_jsonl_sha256="f" * 64,
        )

    dataset.write_text('{"event_id":2}\n', encoding="utf-8", newline="\n")
    with pytest.raises(ValueError, match="content"):
        _validate_dataset_manifest(
            manifest=manifest,
            manifest_path=manifest_path,
            scope=scope,
            as_of_time=cutoff,
            stats={"row_count": 1},
            levels=[{"chan_level": 1440, "event_count": 1}],
            snapshot_jsonl_sha256=digest,
        )


def test_gate_fails_closed_without_bound_scope_and_dataset_hash() -> None:
    report = build_official_historical_gate(
        {
            "as_of_time": "2026-07-03T07:00:00+00:00",
            "counts": {
                "source_high_level_eligible": 10,
                "official_high_level_visible": 10,
                "intraday_eligible": 10,
                "predictive_weekly_b1": 10,
                "predictive_weekly_b2": 10,
                "strict_daily_episodes": 10,
                "official_30f_confirmations": 10,
                "official_5f_confirmations": 10,
                "official_candidates": 10,
            },
            "official_events_by_level": [],
        }
    )
    assert report["decision"] == "NO_GO"
    assert "unbound_historical_lifecycle_input" in report["blockers"]


def test_gate_cannot_authorize_before_official_event_replay_is_implemented() -> None:
    scope = bound_scope()
    report = build_official_historical_gate(
        {
            "as_of_time": "2026-07-03T07:00:00+00:00",
            "scope_hash": scope["scope_hash"],
            "scope": scope,
            "official_jsonl_sha256": "e" * 64,
            "counts": {
                "source_high_level_eligible": 10,
                "official_high_level_visible": 10,
                "intraday_eligible": 10,
                "predictive_weekly_b1": 10,
                "predictive_weekly_b2": 10,
                "strict_daily_episodes": 10,
                "official_30f_confirmations": 10,
                "official_5f_confirmations": 10,
                "official_candidates": 10,
            },
            "official_events_by_level": [],
        }
    )
    assert report["decision"] == "NO_GO"
    assert "official_event_replay_not_implemented" in report["blockers"]


def test_gate_artifact_publication_is_atomic_and_retryable(monkeypatch, tmp_path) -> None:
    output_dir = tmp_path / "gate"
    report = {
        "decision": "NO_GO",
        "input_hash": "a" * 64,
        "strategy_code": "weekly_daily_b2_resonance_v1",
        "official_events_by_level": [],
        "gate_stages": [{"gate": "strict", "count": 0}],
        "blockers": ["official_predictive_weekly_b2_unavailable"],
    }
    scope = {
        "replay_batch_id": 9,
        "source_batch_id": 6,
        "contract_hash": "b" * 64,
        "scope_hash": "c" * 64,
    }
    dataset_manifest = {"official_jsonl_sha256": "d" * 64}
    original_write_text = gate_cli._write_text
    calls = 0

    def fail_midway(path, content):
        nonlocal calls
        calls += 1
        if calls == 4:
            raise OSError("injected gate artifact failure")
        original_write_text(path, content)

    monkeypatch.setattr(gate_cli, "_write_text", fail_midway)
    with pytest.raises(OSError, match="gate artifact"):
        _publish_gate_artifacts(
            output_dir=output_dir,
            report=report,
            scope=scope,
            cutoff=datetime(2026, 7, 3, 7, tzinfo=UTC),
            dataset_manifest=dataset_manifest,
            failures=["001220.SZ"],
        )
    assert not output_dir.exists()

    monkeypatch.setattr(gate_cli, "_write_text", original_write_text)
    _publish_gate_artifacts(
        output_dir=output_dir,
        report=report,
        scope=scope,
        cutoff=datetime(2026, 7, 3, 7, tzinfo=UTC),
        dataset_manifest=dataset_manifest,
        failures=["001220.SZ"],
    )
    marker = json.loads((output_dir / "gate-complete.json").read_text(encoding="utf-8"))
    metrics = json.loads(
        (output_dir / "event_replay_metrics.json").read_text(encoding="utf-8")
    )
    assert marker["input_hash"] == report["input_hash"]
    assert "gate_waterfall.json" in marker["artifacts"]
    assert metrics["decision"] == report["decision"]
