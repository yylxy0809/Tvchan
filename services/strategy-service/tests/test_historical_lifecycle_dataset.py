from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest
import app.engine.historical_lifecycle_dataset as dataset_module

from app.engine.historical_lifecycle_dataset import (
    HistoricalLifecycleDatasetError,
    export_historical_lifecycle_dataset,
)
from app.repositories.historical_lifecycle_repo import (
    BATCH_SCOPE_SQL,
    EVENTS_SQL,
    GATE_COUNTS_SQL,
    RELATIONSHIP_STATS_SQL,
    STATS_SQL,
    HistoricalLifecycleRepository,
    HistoricalLifecycleScopeError,
    build_scope,
    validate_stats,
)


CUTOFF = datetime(2026, 7, 3, 7, tzinfo=UTC)
CONTRACT = {
    "contract_version": "historical-replay-v1",
    "source_batch_id": 6,
    "config_hash": "cfg",
    "run_group": "historical_replay",
    "cutoff_time": CUTOFF.isoformat(),
    "cutoff_policy": "native_closed_bars_strategy_forward_windows_v1",
    "eligible_universe_snapshot_id": "eligible",
    "canonical_gate_snapshot_id": "gate",
}
CONTRACT_HASH = hashlib.sha256(
    json.dumps(CONTRACT, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()


def scope_row(**overrides):
    row = {
        "replay_batch_id": 9,
        "source_batch_id": 6,
        "parent_status": "sealed",
        "parent_sealed_at": CUTOFF,
        "batch_kind": "historical_replay",
        "publication_namespace": "historical-replay",
        "profile_id": "module-c-historical-replay-v1",
        "run_group_id": "historical_replay",
        "config_hash": "cfg",
        "effective_config": {
            "replay_contract_version": "historical-replay-v1",
            "source_batch_id": 6,
        },
        "audit_references": [{"type": "source_batch", "batch_id": 6}],
        "child_status": "sealed",
        "contract_version": "historical-replay-v1",
        "contract_hash": CONTRACT_HASH,
        "contract": CONTRACT,
        "eligible_universe_snapshot_id": "eligible",
        "canonical_gate_snapshot_id": "gate",
        "cutoff_policy": "native_closed_bars_strategy_forward_windows_v1",
        "source_batch_status": "sealed",
        "source_batch_sealed_at": CUTOFF,
    }
    row.update(overrides)
    return row


def event(event_id: int, *, effective_time=CUTOFF, observed_time=None, level=1440):
    return {
        "id": event_id,
        "fingerprint": f"fp-{event_id}",
        "event_type": "first_seen",
        "effective_time": effective_time,
        "observed_time": observed_time or effective_time + timedelta(days=12),
        "point_time": effective_time - timedelta(days=1),
        "previous_mode": None,
        "current_mode": "predictive",
        "run_id": event_id,
        "provenance": {"publication_profile": "historical_replay"},
        "symbol_id": event_id,
        "chan_level": level,
        "head_mode": "predictive",
        "publication_profile": "historical_replay",
        "snapshot_version": "v1",
        "published_at": observed_time or effective_time + timedelta(days=12),
        "structure_type": "signal",
        "side_or_direction": "buy",
        "bsp_type": "1",
        "price_x1000": 123000,
    }


def test_scope_requires_exact_sealed_replay_contract() -> None:
    scope = build_scope(scope_row(), expected_contract_hash=CONTRACT_HASH)
    assert scope.replay_batch_id == 9
    assert scope.source_batch_id == 6
    assert scope.contract_hash == CONTRACT_HASH

    with pytest.raises(HistoricalLifecycleScopeError, match="sealed"):
        build_scope(scope_row(child_status="running"), expected_contract_hash=CONTRACT_HASH)
    with pytest.raises(HistoricalLifecycleScopeError, match="digest"):
        build_scope(scope_row(contract={"tampered": True}), expected_contract_hash=CONTRACT_HASH)
    with pytest.raises(HistoricalLifecycleScopeError, match="expected contract"):
        build_scope(scope_row(), expected_contract_hash="0" * 64)
    with pytest.raises(HistoricalLifecycleScopeError, match="parent lineage"):
        build_scope(
            scope_row(publication_namespace="wrong"),
            expected_contract_hash=CONTRACT_HASH,
        )


def test_repository_sql_is_exact_batch_fenced_and_never_reads_current() -> None:
    sql = (BATCH_SCOPE_SQL + RELATIONSHIP_STATS_SQL + STATS_SQL + EVENTS_SQL).lower()
    for table in (
        "chan_structure_lifecycle_events",
        "chan_c_head_history",
        "chan_c_runs",
        "chan_c_historical_replay_heads",
        "chan_c_historical_replay_tasks",
        "chan_c_historical_replay_batches",
        "chan_c_batches",
    ):
        assert table in sql
    assert "batch_kind = 'historical_replay'" in sql
    assert "parent.status = 'sealed'" in sql
    assert "child.status = 'sealed'" in sql
    assert "event.effective_time <= $2::timestamptz" in EVENTS_SQL.lower()
    assert "event.observed_time <= $2" not in EVENTS_SQL.lower()
    assert "chan_structure_lifecycle_current" not in sql
    relationship_sql = RELATIONSHIP_STATS_SQL.lower()
    assert "expected_heads as materialized" in relationship_sql
    assert "history_old_run_id is distinct from expected_old_run_id" in relationship_sql
    assert "invalid_outbox_payload" in relationship_sql
    assert "event.point_time <> identity.point_time" in relationship_sql
    assert "run.run_identity = task.replay_identity" in relationship_sql
    gate_sql = GATE_COUNTS_SQL.lower()
    assert "run.batch_id = $1" in gate_sql
    assert "full_batch.batch_id = $3" in gate_sql
    assert "full_batch.eligibility_build_id::text = $4" in gate_sql
    assert "build.manifest_hash = $5" in gate_sql
    assert "event.price_x1000" in gate_sql
    assert "identity.price_x1000" in gate_sql
    assert "batch_id=6" not in gate_sql


def test_snapshot_stats_fail_closed_on_future_or_cross_scope_rows() -> None:
    with pytest.raises(HistoricalLifecycleScopeError, match="future_effective_count"):
        validate_stats(
            {
                "total_tasks": 1,
                "candidate_count": 1,
                "scoped_count": 1,
                "future_effective_count": 1,
            }
        )
    with pytest.raises(HistoricalLifecycleScopeError, match="candidate/scoped"):
        validate_stats({"total_tasks": 1, "candidate_count": 2, "scoped_count": 1})
    with pytest.raises(HistoricalLifecycleScopeError, match="missing_heads"):
        validate_stats({"total_tasks": 1, "missing_heads": 1})
    with pytest.raises(HistoricalLifecycleScopeError, match="source batch is empty"):
        validate_stats({"total_tasks": 0})


def test_repository_uses_one_readonly_repeatable_read_snapshot() -> None:
    class Context:
        def __init__(self, value):
            self.value = value

        async def __aenter__(self):
            return self.value

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class Connection:
        def __init__(self):
            self.transaction_args = None
            self.queries = []

        def transaction(self, **kwargs):
            self.transaction_args = kwargs
            return Context(None)

        async def fetchrow(self, sql, *args):
            self.queries.append((sql, args))
            if sql == BATCH_SCOPE_SQL:
                return scope_row()
            if sql == RELATIONSHIP_STATS_SQL:
                return {
                    "total_tasks": 1,
                    "completed_tasks": 1,
                    "excluded_tasks": 0,
                }
            if sql == STATS_SQL:
                return {
                    "candidate_count": 1,
                    "scoped_count": 1,
                    "row_count": 1,
                    "future_effective_count": 0,
                    "invalid_clock_count": 0,
                    "non_scope_count": 0,
                    "observed_after_cutoff_count": 1,
                }
            raise AssertionError("unexpected query")

        def cursor(self, sql, *args, **kwargs):
            self.queries.append((sql, args, kwargs))
            return "server-cursor"

    class Pool:
        def __init__(self, connection):
            self.connection = connection

        def acquire(self):
            return Context(self.connection)

    async def exercise():
        connection = Connection()
        repository = HistoricalLifecycleRepository(Pool(connection))
        async with repository.open_snapshot(
            replay_batch_id=9,
            expected_contract_hash=CONTRACT_HASH,
            effective_cutoff=CUTOFF,
        ) as snapshot:
            assert snapshot.events(prefetch=321) == "server-cursor"
            assert snapshot.stats["total_tasks"] == 1
        return connection

    connection = asyncio.run(exercise())
    assert connection.transaction_args == {
        "isolation": "repeatable_read",
        "readonly": True,
    }
    assert [query[0] for query in connection.queries[:3]] == [
        BATCH_SCOPE_SQL,
        RELATIONSHIP_STATS_SQL,
        STATS_SQL,
    ]
    assert connection.queries[-1][2]["prefetch"] == 321


class FakeSnapshot:
    def __init__(self, rows, *, stats=None):
        self.scope = build_scope(scope_row(), expected_contract_hash=CONTRACT_HASH)
        self.stats = stats or {
            "total_tasks": 1,
            "candidate_count": len(rows),
            "scoped_count": len(rows),
            "row_count": len(rows),
            "future_effective_count": 0,
            "invalid_clock_count": 0,
            "non_scope_count": 0,
            "observed_after_cutoff_count": len(rows),
        }
        self._rows = rows

    def events(self, *, prefetch=1000):
        async def generate():
            for row in self._rows:
                yield row

        return generate()


def test_export_streams_late_observed_causal_rows_and_hashes_content(tmp_path) -> None:
    snapshot = FakeSnapshot([event(1), event(2, level=10080)])
    manifest = asyncio.run(
        export_historical_lifecycle_dataset(
            snapshot=snapshot,
            effective_cutoff=CUTOFF,
            output_dir=tmp_path,
        )
    )

    output = tmp_path / "official.jsonl"
    assert output.exists()
    assert not (tmp_path / "official.jsonl.tmp").exists()
    assert manifest["dataset_validation"] == "PASS"
    assert manifest["cutoff_basis"] == "effective_time"
    assert manifest["row_count"] == 2
    assert manifest["counts_by_level"] == {"1440": 1, "10080": 1}
    assert manifest["observed_after_cutoff_count"] == 2
    assert manifest["official_jsonl_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["effective_time"] <= CUTOFF.isoformat()
    assert rows[0]["observed_time"] > CUTOFF.isoformat()
    assert rows[0]["bsp_type"] == "1"
    assert rows[0]["price_x1000"] == 123000


def test_export_fails_closed_without_publishing_manifest(tmp_path) -> None:
    snapshot = FakeSnapshot(
        [event(1)],
        stats={
            "total_tasks": 1,
            "candidate_count": 2,
            "scoped_count": 1,
            "row_count": 1,
            "future_effective_count": 0,
            "invalid_clock_count": 0,
            "non_scope_count": 1,
            "observed_after_cutoff_count": 1,
        },
    )
    with pytest.raises(HistoricalLifecycleDatasetError, match="non_scope_count"):
        asyncio.run(
            export_historical_lifecycle_dataset(
                snapshot=snapshot,
                effective_cutoff=CUTOFF,
                output_dir=tmp_path,
            )
        )
    assert not (tmp_path / "manifest.json").exists()
    assert not (tmp_path / "official.jsonl").exists()


def test_export_rejects_structure_points_after_the_causal_cutoff(tmp_path) -> None:
    row = event(1)
    row["point_time"] = CUTOFF + timedelta(seconds=1)
    with pytest.raises(HistoricalLifecycleDatasetError, match="structure point"):
        asyncio.run(
            export_historical_lifecycle_dataset(
                snapshot=FakeSnapshot([row]),
                effective_cutoff=CUTOFF,
                output_dir=tmp_path,
            )
        )


def test_failed_directory_promotion_is_recoverable(monkeypatch, tmp_path) -> None:
    original_replace = dataset_module.os.replace
    calls = 0

    def fail_manifest_replace(source, target):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected directory promotion failure")
        return original_replace(source, target)

    monkeypatch.setattr(dataset_module.os, "replace", fail_manifest_replace)
    output_dir = tmp_path / "dataset"
    with pytest.raises(OSError, match="directory promotion"):
        asyncio.run(
            export_historical_lifecycle_dataset(
                snapshot=FakeSnapshot([event(1)]),
                effective_cutoff=CUTOFF,
                output_dir=output_dir,
            )
        )
    assert not output_dir.exists()

    monkeypatch.setattr(dataset_module.os, "replace", original_replace)
    manifest = asyncio.run(
        export_historical_lifecycle_dataset(
            snapshot=FakeSnapshot([event(2)]),
            effective_cutoff=CUTOFF,
            output_dir=output_dir,
        )
    )
    assert manifest["row_count"] == 1
    assert (output_dir / "manifest.json").exists()
