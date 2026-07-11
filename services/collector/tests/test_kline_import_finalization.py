from __future__ import annotations

from uuid import uuid4

from collector.kline_import_finalization import _parse_shards, terminal_status, unfinished_shards


def _row(**overrides):
    row = {
        "import_run_id": uuid4(), "status": "running", "parameters": {"shard_index": 8, "shard_count": 16},
        "expected_tasks": 3, "checkpoint_tasks": 3, "completed_tasks": 3,
        "failed_tasks": 0, "unfinished_tasks": 0,
    }
    row.update(overrides)
    return row


def test_only_exact_completed_checkpoint_set_can_complete() -> None:
    assert terminal_status(_row()) == ("completed", None)
    assert terminal_status(_row(checkpoint_tasks=2, completed_tasks=2)) == (None, None)
    assert terminal_status(_row(expected_tasks=0, checkpoint_tasks=0, completed_tasks=0)) == (None, None)


def test_reconciled_failed_checkpoint_set_becomes_failed_but_partial_stays_resumable() -> None:
    status, failure = terminal_status(_row(completed_tasks=2, failed_tasks=1))
    assert status == "failed"
    assert "1 of 3" in failure
    assert terminal_status(_row(checkpoint_tasks=2, completed_tasks=1, failed_tasks=1)) == (None, None)
    assert terminal_status(_row(checkpoint_tasks=3, completed_tasks=2, unfinished_tasks=1)) == (None, None)


def test_unfinished_shards_selects_only_running_nonterminal_runs() -> None:
    partial = _row(checkpoint_tasks=2, completed_tasks=2)
    complete = _row(status="running")
    failed = _row(status="failed", checkpoint_tasks=1, completed_tasks=1)
    selected = unfinished_shards([partial, complete, failed])
    assert selected == [{
        "import_run_id": str(partial["import_run_id"]), "shard_index": 8, "shard_count": 16,
        "expected_tasks": 3, "checkpoint_tasks": 2, "completed_tasks": 2,
        "failed_tasks": 0, "unfinished_tasks": 0,
    }]


def test_shard_range_parser_is_explicit_and_rejects_negative() -> None:
    assert _parse_shards("8-10,15") == {8, 9, 10, 15}
    try:
        _parse_shards("-1")
    except ValueError as exc:
        assert "non-negative" in str(exc)
    else:
        raise AssertionError("negative shard was accepted")
