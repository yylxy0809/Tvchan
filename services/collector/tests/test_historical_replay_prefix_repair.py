from __future__ import annotations

import json
import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from collector import historical_replay_prefix_repair as repair


ROOT = Path(__file__).resolve().parents[3]
REPAIR_ID = UUID("8e98cf15-d846-4de8-988a-026f630ac3a7")
HASH = "a" * 64


def manifest() -> dict[str, object]:
    return {
        "contract_version": repair.CONTRACT_VERSION,
        "repair_id": str(REPAIR_ID),
        "batch_id": 9,
        "replay_contract_hash": HASH,
        "entries": [
            {
                "history_id": 11,
                "outbox_id": 21,
                "new_run_id": 101,
                "new_cutoff": "2026-04-29T07:00:00Z",
                "current_old_run_id": None,
                "current_old_cutoff": None,
                "predecessor_history_id": 10,
                "target_old_run_id": 100,
                "target_old_cutoff": "2026-04-28T07:00:00Z",
            },
            {
                "history_id": 12,
                "outbox_id": 22,
                "new_run_id": 101,
                "new_cutoff": "2026-04-29T07:00:00Z",
                "current_old_run_id": None,
                "current_old_cutoff": None,
                "predecessor_history_id": 9,
                "target_old_run_id": 100,
                "target_old_cutoff": "2026-04-28T07:00:00Z",
            },
        ],
    }


def test_manifest_digest_is_canonical_and_external_hash_is_mandatory(tmp_path: Path) -> None:
    first = manifest()
    second = json.loads(json.dumps(first, sort_keys=False))
    second["entries"] = list(reversed(second["entries"]))
    path = tmp_path / "repair.json"
    path.write_text(json.dumps(first), encoding="utf-8")

    digest = repair.canonical_sha256(first)
    loaded = repair.load_manifest(
        path, expected_manifest_sha256=digest,
        expected_contract_hash=HASH,
    )

    assert loaded.repair_id == REPAIR_ID
    assert [entry.history_id for entry in loaded.entries] == [11, 12]
    assert repair.canonical_sha256(second) != digest
    with pytest.raises(repair.InvalidRepairManifest, match="manifest sha256"):
        repair.load_manifest(
            path, expected_manifest_sha256="b" * 64,
            expected_contract_hash=HASH,
        )


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (lambda item: item.update(contract_version="other"), "contract_version"),
        (lambda item: item.update(batch_id=0), "batch_id"),
        (lambda item: item.update(entries=[]), "exactly 2"),
        (
            lambda item: item["entries"][1].update(history_id=11),
            "duplicate history_id",
        ),
        (
            lambda item: item["entries"][0].update(target_old_run_id=None),
            "target_old_run_id",
        ),
    ],
)
def test_manifest_rejects_non_exact_contract(mutate, error: str) -> None:
    payload = manifest()
    mutate(payload)
    with pytest.raises(repair.InvalidRepairManifest, match=error):
        repair.parse_manifest(payload, expected_contract_hash=HASH)


def test_event_set_hash_normalizes_timestamps_and_payload_order() -> None:
    timestamp = datetime(2026, 4, 29, 7, tzinfo=UTC)
    event = {
        "fingerprint": "f",
        "event_type": "first_seen",
        "point_time": timestamp,
        "previous_mode": None,
        "current_mode": "predictive",
        "run_id": 101,
        "effective_time": timestamp,
        "observed_time": timestamp,
        "provenance": {"z": 2, "a": 1},
    }
    reordered = {**event, "provenance": {"a": 1, "z": 2}}

    assert repair.event_set_sha256([event]) == repair.event_set_sha256([reordered])


@pytest.mark.parametrize("field", ["id", "created_at"])
def test_event_identity_hash_freezes_restorable_row_identity(field: str) -> None:
    timestamp = datetime(2026, 4, 29, 7, tzinfo=UTC)
    before = {
        "id": 1, "fingerprint": "f", "head_history_id": 11,
        "event_type": "first_seen", "effective_time": timestamp,
        "point_time": timestamp, "previous_mode": None,
        "current_mode": "predictive", "run_id": 101,
        "provenance": {"a": 1}, "created_at": timestamp,
        "observed_time": timestamp,
    }
    replaced = dict(before)
    replaced[field] = 2 if field == "id" else datetime(2026, 4, 30, 7, tzinfo=UTC)

    assert repair.event_set_sha256([before]) == repair.event_set_sha256([replaced])
    assert repair.event_identity_set_sha256([before]) != repair.event_identity_set_sha256(
        [replaced]
    )


def test_migration_042_is_durable_narrow_and_idempotent() -> None:
    sql = (
        ROOT / "db" / "sql" / "042_historical_replay_prefix_repair.sql"
    ).read_text(encoding="utf-8").lower()

    assert "create table if not exists chan_c_historical_replay_prefix_repairs" in sql
    assert "create table if not exists chan_c_historical_replay_prefix_repair_snapshots" in sql
    assert "history_before jsonb not null" in sql
    assert "outbox_before jsonb not null" in sql
    assert "events_before jsonb not null" in sql
    assert "before_event_set_sha256 varchar(64) not null" in sql
    assert "before_event_identity_sha256 varchar(64) not null" in sql
    assert "before update or delete" in sql
    assert "guard_historical_replay_prefix_repair_header" in sql
    assert "old.status = 'applied' and new.status = 'verified'" in sql
    assert "old.status in ('applied', 'verified') and new.status = 'rolled_back'" in sql
    assert "verified repair evidence is immutable during rollback" in sql
    assert "klines" not in sql


def test_apply_sql_contract_has_fixed_locks_cas_and_no_forbidden_writes() -> None:
    normalized = " ".join(repair.APPLY_EVIDENCE_SQL.lower().split())

    assert "pg_try_advisory_lock(hashtext('chan-lifecycle-v1'))" in repair.TRY_LOCK_SQL
    assert normalized.index("from chan_c_batches") < normalized.index(
        "from chan_c_historical_replay_batches"
    )
    assert normalized.index("from chan_c_historical_replay_batches") < normalized.index(
        "from chan_c_head_history"
    )
    assert "order by id for update" in normalized
    assert "update chan_c_head_history" in normalized
    assert "update chan_c_head_outbox" in normalized
    assert "delete from chan_structure_lifecycle_events" in normalized
    assert "lease_version = lease_version + 1" in normalized
    for forbidden in (
        "update klines", "delete from klines", "update chan_c_runs",
        "update chan_c_historical_replay_heads",
        "update chan_c_historical_replay_tasks", "update chan_c_batches",
        "update chan_c_historical_replay_batches",
    ):
        assert forbidden not in normalized


def test_parse_args_requires_hashes_and_actor_for_mutations() -> None:
    common = [
        "--database-url", "postgresql://example",
        "--manifest", "manifest.json",
        "--manifest-sha256", "b" * 64,
        "--expected-contract-hash", HASH,
    ]
    assert repair.parse_args(["plan", *common]).action == "plan"
    with pytest.raises(SystemExit):
        repair.parse_args(["apply", *common])
    apply = repair.parse_args([
        "apply", *common, "--actor", "operator",
        "--expected-before-event-identity-sha256", "c" * 64,
        "--expected-target-event-sha256", "d" * 64,
    ])
    assert apply.actor == "operator"


def test_v1_manifest_requires_two_null_prefix_entries() -> None:
    payload = manifest()
    payload["entries"] = payload["entries"][:1]
    with pytest.raises(repair.InvalidRepairManifest, match="exactly 2"):
        repair.parse_manifest(payload, expected_contract_hash=HASH)

    payload = manifest()
    payload["entries"][0]["current_old_run_id"] = 99
    with pytest.raises(repair.InvalidRepairManifest, match="must be null"):
        repair.parse_manifest(payload, expected_contract_hash=HASH)


class MutationConnection:
    def __init__(
        self, *, child_status: str = "running", child_hash: str = HASH, existing=None,
    ) -> None:
        self.child_status = child_status
        self.child_hash = child_hash
        self.existing = existing
        self.calls: list[str] = []
        self.execute_sqls: list[str] = []
        self.deleted = 0

    @asynccontextmanager
    async def transaction(self, **kwargs):
        self.calls.append(f"transaction:{kwargs.get('isolation')}")
        try:
            yield
        except Exception:
            self.calls.append("rollback")
            raise
        else:
            self.calls.append("commit")

    async def fetchval(self, sql: str, *args):
        self.calls.append("lock" if "pg_try" in sql else "unlock")
        return True

    async def fetchrow(self, sql: str, *args):
        normalized = sql.lower()
        if "from chan_c_batches" in normalized:
            self.calls.append("parent")
            return {"status": "sealed", "batch_kind": "historical_replay"}
        if "from chan_c_historical_replay_batches" in normalized:
            self.calls.append("child")
            return {"status": self.child_status, "contract_hash": self.child_hash}
        if "from chan_c_historical_replay_prefix_repairs" in normalized:
            self.calls.append("audit")
            return self.existing
        raise AssertionError(sql)

    async def fetch(self, sql: str, *args):
        raise AssertionError(sql)

    async def execute(self, sql: str, *args):
        normalized = sql.lower()
        self.execute_sqls.append(normalized)
        if "delete from chan_structure_lifecycle_events" in normalized:
            return f"DELETE {self.deleted}"
        if "update " in normalized:
            return "UPDATE 1"
        return "INSERT 0 1"


def _parsed_manifest() -> repair.RepairManifest:
    return repair.parse_manifest(manifest(), expected_contract_hash=HASH)


def test_first_apply_locks_parent_child_audit_then_freezes_plan_hashes(monkeypatch) -> None:
    parsed = _parsed_manifest()
    histories = {
        entry.history_id: {"id": entry.history_id, "published_at": entry.new_cutoff}
        for entry in parsed.entries
    }
    outboxes = {
        entry.outbox_id: {"id": entry.outbox_id, "payload": {"old_run_id": None}}
        for entry in parsed.entries
    }
    before = [
        {"head_history_id": entry.history_id, "fingerprint": str(entry.history_id)}
        for entry in parsed.entries
    ]
    target = [
        {"head_history_id": entry.history_id, "fingerprint": f"target-{entry.history_id}"}
        for entry in parsed.entries
    ]

    async def validated(conn, manifest, *, locked):
        assert locked is True
        return histories, outboxes, before

    async def expected(conn, manifest, histories, outboxes):
        return target, {
            entry.history_id: repair.event_set_sha256(
                [row for row in target if row["head_history_id"] == entry.history_id]
            )
            for entry in parsed.entries
        }

    monkeypatch.setattr(repair, "_validate_batch_and_anomalies", validated)
    monkeypatch.setattr(repair, "_expected_events", expected)
    connection = MutationConnection()
    connection.deleted = len(before)
    result = asyncio.run(repair.apply_repair(
        connection, parsed, actor="operator",
        expected_before_event_identity_sha256=repair.event_identity_set_sha256(before),
        expected_target_event_sha256=repair.event_set_sha256(target),
    ))

    assert result["status"] == "applied"
    assert connection.calls[:5] == [
        "lock", "transaction:serializable", "parent", "child", "audit",
    ]


def test_first_apply_rejects_semantically_equal_replaced_event_identity(monkeypatch) -> None:
    parsed = _parsed_manifest()
    timestamp = datetime(2026, 4, 29, 7, tzinfo=UTC)
    reviewed = [{
        "id": 1, "head_history_id": 11, "fingerprint": "same",
        "created_at": timestamp,
    }]
    replaced = [{**reviewed[0], "id": 2}]
    histories = {
        entry.history_id: {"id": entry.history_id, "published_at": entry.new_cutoff}
        for entry in parsed.entries
    }
    outboxes = {
        entry.outbox_id: {"id": entry.outbox_id, "payload": {"old_run_id": None}}
        for entry in parsed.entries
    }

    async def validated(conn, manifest, *, locked):
        return histories, outboxes, replaced

    async def expected(conn, manifest, histories, outboxes):
        return [], {entry.history_id: repair.event_set_sha256([]) for entry in parsed.entries}

    monkeypatch.setattr(repair, "_validate_batch_and_anomalies", validated)
    monkeypatch.setattr(repair, "_expected_events", expected)
    with pytest.raises(repair.RepairStateConflict, match="reviewed plan"):
        asyncio.run(repair.apply_repair(
            MutationConnection(), parsed, actor="operator",
            expected_before_event_identity_sha256=(
                repair.event_identity_set_sha256(reviewed)
            ),
            expected_target_event_sha256=repair.event_set_sha256([]),
        ))


def test_first_apply_rejects_sealed_child_before_audit() -> None:
    parsed = _parsed_manifest()
    connection = MutationConnection(child_status="sealed")
    with pytest.raises(repair.RepairStateConflict, match="running child"):
        asyncio.run(repair.apply_repair(
            connection, parsed, actor="operator",
            expected_before_event_identity_sha256="c" * 64,
            expected_target_event_sha256="d" * 64,
        ))
    assert connection.calls == [
        "lock", "transaction:serializable", "parent", "child", "rollback", "unlock",
    ]


def test_existing_applied_repair_rejects_mixed_outbox_state(monkeypatch) -> None:
    parsed = _parsed_manifest()
    audit = {
        "status": "applied", "manifest_sha256": parsed.sha256,
        "replay_contract_hash": HASH,
        "before_event_set_sha256": "c" * 64,
        "before_event_identity_sha256": "c" * 64,
        "target_event_set_sha256": "d" * 64,
    }

    class Connection:
        async def fetch(self, sql, *args):
            return [{"history_id": 11}, {"history_id": 12}]

    async def current(conn, manifest, snapshots):
        return [], [{"status": "pending"}, {"status": "completed"}], []

    monkeypatch.setattr(repair, "_actual_repaired_state", current)
    with pytest.raises(repair.RepairStateConflict, match="mixed"):
        asyncio.run(repair._validate_existing_apply(
            Connection(), parsed, audit,
            expected_before_identity_hash="c" * 64,
            expected_target_hash="d" * 64,
        ))


def test_pre_repair_finalizer_requires_only_two_invalid_history_rows() -> None:
    valid = {
        "blockers": ["invalid_history"],
        "counts": {
            "total_tasks": 10, "completed_tasks": 10,
            "invalid_history": 2, "invalid_outbox_payload": 0,
        },
        "lifecycle_reconciliation": {"decision": "PASS"},
    }
    repair.validate_pre_repair_finalizer(valid)
    invalid = json.loads(json.dumps(valid))
    invalid["counts"]["invalid_outbox_payload"] = 1
    with pytest.raises(repair.RepairStateConflict, match="exact invalid_history"):
        repair.validate_pre_repair_finalizer(invalid)


def test_verify_rejects_a_still_blocked_finalizer() -> None:
    repair.validate_post_repair_finalizer({"ready": True, "blockers": []})
    with pytest.raises(repair.RepairStateConflict, match="remains blocked"):
        repair.validate_post_repair_finalizer({
            "ready": False, "blockers": ["invalid_lifecycle_events"],
        })


def test_partial_completed_and_pending_rows_are_rollback_safe() -> None:
    empty_hash = repair.event_set_sha256([])
    snapshots = [
        {"history_id": 11, "target_event_set_sha256": empty_hash},
        {"history_id": 12, "target_event_set_sha256": empty_hash},
    ]
    repair.validate_rollback_target_state([
        {"head_history_id": 11, "status": "completed", "processed_at": "done"},
        {"head_history_id": 12, "status": "pending", "processed_at": None},
    ], [], snapshots)

    with pytest.raises(repair.RepairStateConflict, match="unfinished outbox"):
        repair.validate_rollback_target_state([
            {"head_history_id": 11, "status": "pending", "processed_at": None},
            {"head_history_id": 12, "status": "pending", "processed_at": None},
        ], [{"head_history_id": 11, "fingerprint": "unexpected"}], snapshots)


def test_rollback_rejects_sealed_child_before_audit() -> None:
    connection = MutationConnection(child_status="sealed")
    with pytest.raises(repair.RepairStateConflict, match="sealed child evidence"):
        asyncio.run(repair.rollback_repair(
            connection, _parsed_manifest(), actor="operator",
        ))
    assert connection.calls == [
        "lock", "transaction:serializable", "parent", "child", "rollback", "unlock",
    ]


def test_rollback_rejects_child_contract_drift_before_audit() -> None:
    connection = MutationConnection(child_hash="f" * 64)
    with pytest.raises(repair.RepairStateConflict, match="running child"):
        asyncio.run(repair.rollback_repair(
            connection, _parsed_manifest(), actor="operator",
        ))
    assert connection.calls == [
        "lock", "transaction:serializable", "parent", "child", "rollback", "unlock",
    ]


def _audit_snapshots(parsed: repair.RepairManifest):
    now = datetime(2026, 5, 1, tzinfo=UTC)
    empty_hash = repair.event_set_sha256([])
    return [{
        "history_id": entry.history_id,
        "outbox_id": entry.outbox_id,
        "history_before": {
            "old_run_id": None, "old_base_to_bar_end": None,
        },
        "outbox_before": {
            "status": "completed", "lease_version": 1, "lease_token": "old",
            "lease_until": None, "attempts": 1, "payload": {"old_run_id": None},
            "processed_at": repair._jsonable(now),
            "created_at": repair._jsonable(now),
            "updated_at": repair._jsonable(now),
            "next_attempt_at": None, "last_error": None,
            "failed_at": None, "dead_lettered_at": None,
        },
        "events_before": [],
        "target_event_set_sha256": empty_hash,
    } for entry in parsed.entries]


def test_verify_rejects_child_contract_drift_before_audit() -> None:
    connection = MutationConnection(child_hash="f" * 64)
    with pytest.raises(repair.RepairStateConflict, match="manifest-fenced"):
        asyncio.run(repair.verify_repair(
            connection, _parsed_manifest(), actor="operator",
        ))
    assert connection.calls == [
        "lock", "transaction:serializable", "parent", "child", "rollback", "unlock",
    ]


@pytest.mark.parametrize("child_status", ["running", "sealed"])
def test_verify_main_path_updates_audit_only_after_hash_and_finalizer(
    monkeypatch, child_status: str,
) -> None:
    parsed = _parsed_manifest()
    target = [{"head_history_id": 11, "fingerprint": "target"}]
    target_hash = repair.event_set_sha256(target)
    audit = {
        "status": "applied", "manifest_sha256": parsed.sha256,
        "target_event_set_sha256": target_hash,
    }
    snapshots = _audit_snapshots(parsed)

    async def load(conn, repair_id):
        return audit, snapshots

    async def current(conn, manifest, snapshots):
        return (
            [{"id": 11}, {"id": 12}],
            [
                {"id": 21, "status": "completed", "processed_at": "done"},
                {"id": 22, "status": "completed", "processed_at": "done"},
            ],
            target,
        )

    async def expected(conn, manifest, histories, outboxes):
        return target, {}

    async def finalizer(*args, **kwargs):
        return {"ready": True, "blockers": []}

    monkeypatch.setattr(repair, "_load_repair_snapshots", load)
    monkeypatch.setattr(repair, "_actual_repaired_state", current)
    monkeypatch.setattr(repair, "_expected_events", expected)
    monkeypatch.setattr(repair, "finalize_replay_batch", finalizer)
    connection = MutationConnection(child_status=child_status)
    result = asyncio.run(repair.verify_repair(
        connection, parsed, actor="operator",
    ))
    assert result["status"] == "verified"
    assert any(
        "update chan_c_historical_replay_prefix_repairs" in sql
        for sql in connection.execute_sqls
    )
    assert connection.calls[-2:] == ["commit", "unlock"]


def test_verify_finalizer_failure_never_updates_audit(monkeypatch) -> None:
    parsed = _parsed_manifest()
    target_hash = repair.event_set_sha256([])

    async def load(conn, repair_id):
        return {
            "status": "applied", "manifest_sha256": parsed.sha256,
            "target_event_set_sha256": target_hash,
        }, _audit_snapshots(parsed)

    async def current(conn, manifest, snapshots):
        return ([{"id": 11}, {"id": 12}], [
            {"id": 21, "status": "completed", "processed_at": "done"},
            {"id": 22, "status": "completed", "processed_at": "done"},
        ], [])

    async def expected(conn, manifest, histories, outboxes):
        return [], {}

    async def finalizer(*args, **kwargs):
        return {"ready": False, "blockers": ["invalid_lifecycle_events"]}

    monkeypatch.setattr(repair, "_load_repair_snapshots", load)
    monkeypatch.setattr(repair, "_actual_repaired_state", current)
    monkeypatch.setattr(repair, "_expected_events", expected)
    monkeypatch.setattr(repair, "finalize_replay_batch", finalizer)
    connection = MutationConnection()
    with pytest.raises(repair.RepairStateConflict, match="remains blocked"):
        asyncio.run(repair.verify_repair(connection, parsed, actor="operator"))
    assert not any(
        "update chan_c_historical_replay_prefix_repairs" in sql
        for sql in connection.execute_sqls
    )
    assert "rollback" in connection.calls


def test_applied_partial_rollback_executes_restore_and_header_cas(monkeypatch) -> None:
    parsed = _parsed_manifest()
    snapshots = _audit_snapshots(parsed)

    async def load(conn, repair_id):
        return {
            "status": "applied", "manifest_sha256": parsed.sha256,
        }, snapshots

    async def current(conn, manifest, snapshots):
        return ([], [
            {"id": 21, "head_history_id": 11, "status": "completed", "processed_at": "done"},
            {"id": 22, "head_history_id": 12, "status": "pending", "processed_at": None},
        ], [])

    monkeypatch.setattr(repair, "_load_repair_snapshots", load)
    monkeypatch.setattr(repair, "_actual_repaired_state", current)
    connection = MutationConnection()
    result = asyncio.run(repair.rollback_repair(
        connection, parsed, actor="operator",
    ))
    assert result["status"] == "rolled_back"
    assert sum("update chan_c_head_history" in sql for sql in connection.execute_sqls) == 2
    assert sum("update chan_c_head_outbox" in sql for sql in connection.execute_sqls) == 2
    assert any(
        "update chan_c_historical_replay_prefix_repairs" in sql
        for sql in connection.execute_sqls
    )


def test_rollback_cas_failure_rolls_back_transaction(monkeypatch) -> None:
    parsed = _parsed_manifest()
    snapshots = _audit_snapshots(parsed)

    async def load(conn, repair_id):
        return {"status": "applied", "manifest_sha256": parsed.sha256}, snapshots

    async def current(conn, manifest, snapshots):
        return ([], [
            {"id": 21, "head_history_id": 11, "status": "pending", "processed_at": None},
            {"id": 22, "head_history_id": 12, "status": "pending", "processed_at": None},
        ], [])

    class FailingConnection(MutationConnection):
        def __init__(self):
            super().__init__()
            self.history_updates = 0

        async def execute(self, sql, *args):
            if "update chan_c_head_history" in sql.lower():
                self.history_updates += 1
                if self.history_updates == 2:
                    self.execute_sqls.append(sql.lower())
                    return "UPDATE 0"
            return await super().execute(sql, *args)

    monkeypatch.setattr(repair, "_load_repair_snapshots", load)
    monkeypatch.setattr(repair, "_actual_repaired_state", current)
    connection = FailingConnection()
    with pytest.raises(repair.RepairStateConflict, match="history CAS failed"):
        asyncio.run(repair.rollback_repair(connection, parsed, actor="operator"))
    assert "rollback" in connection.calls
    assert not any(
        "update chan_c_historical_replay_prefix_repairs" in sql
        for sql in connection.execute_sqls
    )


@pytest.mark.parametrize("status", ["verified", "rolled_back"])
def test_existing_terminal_apply_is_exactly_idempotent(monkeypatch, status: str) -> None:
    parsed = _parsed_manifest()
    before_hash = repair.event_set_sha256([])
    target = [{"head_history_id": 11, "fingerprint": "target"}]
    target_hash = repair.event_set_sha256(target)
    audit = {
        "status": status,
        "manifest_sha256": parsed.sha256,
        "replay_contract_hash": HASH,
        "before_event_set_sha256": before_hash,
        "before_event_identity_sha256": before_hash,
        "target_event_set_sha256": target_hash,
    }
    snapshots = _audit_snapshots(parsed)

    class Connection:
        async def fetch(self, sql, *args):
            return snapshots

    async def current(conn, manifest, loaded_snapshots):
        assert loaded_snapshots == snapshots
        return ([], [
            {"status": "completed"}, {"status": "completed"},
        ], target)

    rolled_back_validated = False

    async def rolled_back(conn, manifest, loaded_snapshots):
        nonlocal rolled_back_validated
        assert loaded_snapshots == snapshots
        rolled_back_validated = True
        return {"history_count": 2, "event_count": 0}

    monkeypatch.setattr(repair, "_actual_repaired_state", current)
    monkeypatch.setattr(repair, "_validate_rolled_back_state", rolled_back)
    result = asyncio.run(repair._validate_existing_apply(
        Connection(), parsed, audit,
        expected_before_identity_hash=before_hash,
        expected_target_hash=target_hash,
    ))

    assert result == {
        "action": "apply", "repair_id": str(REPAIR_ID),
        "status": status, "idempotent": True,
    }
    assert rolled_back_validated is (status == "rolled_back")
