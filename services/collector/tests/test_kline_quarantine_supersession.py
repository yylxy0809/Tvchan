from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from collector.kline_quarantine_supersession import (
    build_supersession_records,
    validate_audit_evidence,
)


AUDIT_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
IMPORT_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
OBSERVED_AT = datetime(2026, 7, 19, 8, tzinfo=timezone.utc)


def _audit_row(**overrides):
    row = {
        "status": "completed",
        "apply_mode": False,
        "parameters": {
            "contract_version": "module-c-strict-audit-v2",
            "observed_at": OBSERVED_AT.isoformat(),
        },
        "summary": {
            "evidence_complete": True,
            "evidence_sha256": "a" * 64,
        },
    }
    row.update(overrides)
    return row


def _group(**overrides):
    row = {
        "import_run_id": IMPORT_ID,
        "import_status": "completed",
        "import_completed_at": OBSERVED_AT - timedelta(days=1),
        "reason": "missing_source_file",
        "symbol_id": 7,
        "symbol": "688001.SH",
        "timeframe": "5f",
        "quarantine_rows": 1,
        "max_quarantine_id": 99,
    }
    row.update(overrides)
    return row


def _checkpoint(**overrides):
    row = {
        "symbol_id": 7,
        "timeframe": 5,
        "status": "completed",
        "rows_scanned": 100,
        "metadata": {"disposition": "eligible"},
    }
    row.update(overrides)
    return row


def test_completed_scope_audit_can_supersede_older_exact_quarantine_group() -> None:
    evidence_sha, observed_at = validate_audit_evidence(_audit_row())

    records = build_supersession_records(
        audit_run_id=AUDIT_ID,
        audit_evidence_sha256=evidence_sha,
        audit_observed_at=observed_at,
        groups=[_group()],
        checkpoints=[_checkpoint()],
        justification="new canonical audit covers this exact scope",
    )

    assert len(records) == 1
    record = records[0]
    assert record.source_import_run_id == IMPORT_ID
    assert record.max_quarantine_id == 99
    assert record.quarantine_rows == 1
    assert record.canonical_audit_run_id == AUDIT_ID


@pytest.mark.parametrize(
    ("checkpoint", "message"),
    [
        (_checkpoint(metadata={"disposition": "unresolved"}), "not canonical-eligible"),
        (_checkpoint(rows_scanned=0), "has no canonical rows"),
        (_checkpoint(status="failed"), "not canonical-eligible"),
    ],
)
def test_unresolved_or_empty_scope_remains_quarantined(checkpoint, message) -> None:
    evidence_sha, observed_at = validate_audit_evidence(_audit_row())
    with pytest.raises(ValueError, match=message):
        build_supersession_records(
            audit_run_id=AUDIT_ID,
            audit_evidence_sha256=evidence_sha,
            audit_observed_at=observed_at,
            groups=[_group()],
            checkpoints=[checkpoint],
            justification="explicit review",
        )


def test_audit_must_be_completed_read_only_v2_with_complete_evidence() -> None:
    for override in (
        {"status": "running"},
        {"apply_mode": True},
        {"parameters": {"contract_version": "legacy", "observed_at": OBSERVED_AT.isoformat()}},
        {"summary": {"evidence_complete": False, "evidence_sha256": "a" * 64}},
    ):
        with pytest.raises(ValueError):
            validate_audit_evidence(_audit_row(**override))


def test_import_completed_after_audit_observation_fails_closed() -> None:
    evidence_sha, observed_at = validate_audit_evidence(_audit_row())
    with pytest.raises(ValueError, match="not older than canonical audit"):
        build_supersession_records(
            audit_run_id=AUDIT_ID,
            audit_evidence_sha256=evidence_sha,
            audit_observed_at=observed_at,
            groups=[_group(import_completed_at=OBSERVED_AT + timedelta(seconds=1))],
            checkpoints=[_checkpoint()],
            justification="explicit review",
        )


def test_migration_is_append_only_and_snapshot_bounded() -> None:
    sql = (
        Path(__file__).parents[3]
        / "db"
        / "sql"
        / "048_kline_import_quarantine_supersession.sql"
    ).read_text(encoding="utf-8").lower()
    assert "quarantine_rows bigint not null" in sql
    assert "max_quarantine_id bigint not null" in sql
    assert "canonical_audit_run_id uuid not null" in sql
    assert "before update or delete" in sql
    assert "append-only" in sql
    assert "kline_import_quarantine_append_only" in sql
