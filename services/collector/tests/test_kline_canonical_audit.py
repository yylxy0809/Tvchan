from __future__ import annotations

from datetime import datetime, timezone
import asyncio
import json
from pathlib import Path

import pytest

from collector.kline_canonical_audit import (
    AuditRow,
    AuditRunner,
    PlannedActions,
    AtomicJsonlWriter,
    DAILY_PERIOD_ROWS_SQL,
    apply_action_batches,
    apply_actions,
    aligned_windows,
    build_shard_sql,
    choose_winner,
    canonical_period_end_from_daily,
    clear_checkpoint_state,
    consolidate_checkpoint_markers,
    load_checkpoint_markers,
    logical_period_key,
    parse_args,
    period_bounds,
    plan_lunch_reopen_duplicate,
    run_audit,
    validate_bar,
    write_checkpoint_marker,
)
import collector.kline_canonical_audit as audit_module


UTC = timezone.utc


def row(**overrides: object) -> AuditRow:
    values: dict[str, object] = {
        "symbol_id": 1,
        "timeframe": "1d",
        "ts": datetime(2026, 1, 2, 7, tzinfo=UTC),
        "open_x1000": 1000,
        "high_x1000": 1200,
        "low_x1000": 900,
        "close_x1000": 1100,
        "volume": 10,
        "amount_x100": 100,
        "is_complete": True,
        "revision": 1,
        "source": 9,
        "updated_at": datetime(2026, 1, 2, 8, tzinfo=UTC),
    }
    values.update(overrides)
    return AuditRow(**values)  # type: ignore[arg-type]


def test_defaults_and_apply_guard() -> None:
    args = parse_args([])
    assert args.timeframes == "5f,15f,30f,1h,1d,1w,1m"
    assert args.concurrency == 2
    assert args.statement_timeout_seconds == 20
    assert args.lock_timeout_seconds == 1
    assert args.single_window is False
    assert parse_args(["--single-window"]).single_window is True
    with pytest.raises(SystemExit):
        parse_args(["--apply"])


def test_logical_grouping_and_coverage_aware_winner() -> None:
    at_boundary = datetime(2026, 1, 2, 7, tzinfo=UTC)
    older_pytdx = row(source=2, revision=9)
    native = row(source=9, revision=1)
    newer_pytdx = row(source=2, ts=datetime(2026, 1, 3, 7, tzinfo=UTC), revision=1)

    assert logical_period_key("1w", at_boundary) == ("1w", datetime(2025, 12, 29, tzinfo=logical_period_key("1w", at_boundary)[1].tzinfo))
    assert choose_winner([older_pytdx, native], at_boundary) is native
    assert choose_winner([newer_pytdx, native], datetime(2026, 1, 2, 7, tzinfo=UTC)) is newer_pytdx


def test_audit_row_from_asyncpg_shape_ignores_stored_timeframe_code() -> None:
    stored = dict(row(timeframe="1w").__dict__)
    stored["timeframe"] = 10080
    parsed = AuditRow.from_record(stored, "1w")
    assert parsed.timeframe == "1w"
    assert parsed.symbol_id == 1


def test_winner_uses_canonical_daily_timestamp_for_coverage_boundary() -> None:
    # Midnight local Jan 3 normalizes to the Jan 3 15:00 daily label.  Ranking
    # must therefore prefer pytdx after a coverage boundary earlier that day.
    midnight_pytdx = row(source=2, ts=datetime(2026, 1, 2, 16, tzinfo=UTC), revision=1)
    native = row(source=9, ts=datetime(2026, 1, 3, 7, tzinfo=UTC), revision=9)
    assert choose_winner([midnight_pytdx, native], datetime(2026, 1, 3, 0, tzinfo=UTC)) is midnight_pytdx


@pytest.mark.parametrize(
    ("timeframe", "timestamp", "valid"),
    [
        ("5f", datetime(2026, 1, 2, 1, 35, tzinfo=UTC), True),
        ("15f", datetime(2026, 1, 2, 3, 5, tzinfo=UTC), False),
        ("30f", datetime(2026, 1, 2, 3, 30, tzinfo=UTC), True),
        ("1h", datetime(2026, 1, 2, 3, 30, tzinfo=UTC), True),
        ("1d", datetime(2026, 1, 2, 7, tzinfo=UTC), True),
        ("1w", datetime(2026, 1, 2, 7, tzinfo=UTC), True),
        ("1m", datetime(2026, 1, 2, 7, tzinfo=UTC), True),
        ("1d", datetime(2026, 1, 2, 6, tzinfo=UTC), False),
    ],
)
def test_timestamp_validation_for_all_periods(timeframe: str, timestamp: datetime, valid: bool) -> None:
    reasons = validate_bar(row(timeframe=timeframe, ts=timestamp))
    assert ("invalid_timestamp" not in reasons) is valid


def test_ohlcv_validation() -> None:
    assert "invalid_ohlcv" in validate_bar(row(high_x1000=800))
    assert "invalid_ohlcv" in validate_bar(row(volume=-1))
    assert "invalid_ohlcv" in validate_bar(row(amount_x100=-1))


def test_period_end_uses_only_complete_valid_canonical_daily_winners() -> None:
    valid = row(timeframe="1d", source=9)
    incomplete = row(timeframe="1d", source=2, is_complete=False, revision=9)
    midnight = row(timeframe="1d", ts=datetime(2026, 1, 1, 16, tzinfo=UTC), source=2, revision=10)
    dirty = row(timeframe="1d", source=2, high_x1000=800, revision=11)

    assert canonical_period_end_from_daily([valid, incomplete, midnight, dirty], None) == valid.ts
    assert canonical_period_end_from_daily([incomplete, midnight, dirty], None) is None


def test_bounded_shard_sql_and_windows() -> None:
    intraday = build_shard_sql("5f")
    daily = build_shard_sql("1d")
    assert "min(k.ts) as min_ts, max(k.ts) as max_ts" in intraday.lower()
    assert "interval '3 months'" in intraday.lower()
    assert "interval '1 year'" in daily.lower()
    assert "group by" not in intraday.lower()
    assert "symbol_id = $1" in intraday.lower()


def test_daily_period_query_is_sargable_and_uses_exact_period_bounds() -> None:
    sql = DAILY_PERIOD_ROWS_SQL.lower()
    assert "ts >= $2" in sql and "ts < $3" in sql
    assert "date_trunc" not in sql
    week_start, week_end = period_bounds(datetime(2026, 1, 7, 7, tzinfo=UTC), "1w")
    month_start, month_end = period_bounds(datetime(2026, 1, 7, 7, tzinfo=UTC), "1m")
    assert (week_start, week_end) == (datetime(2026, 1, 4, 16, tzinfo=UTC), datetime(2026, 1, 11, 16, tzinfo=UTC))
    assert (month_start, month_end) == (datetime(2025, 12, 31, 16, tzinfo=UTC), datetime(2026, 1, 31, 16, tzinfo=UTC))


def test_weekly_monthly_windows_do_not_split_periods_across_december_january() -> None:
    start = datetime(2020, 12, 30, 7, tzinfo=UTC)
    end = datetime(2031, 1, 3, 7, tzinfo=UTC)
    weekly = list(aligned_windows(start, end, "1w"))
    monthly = list(aligned_windows(start, end, "1m"))
    assert all(left.astimezone(audit_module.SHANGHAI_TZ).weekday() == 0 for left, _ in weekly[1:])
    assert all(right.astimezone(audit_module.SHANGHAI_TZ).weekday() == 6 for _, right in weekly[:-1])
    assert all(left.astimezone(audit_module.SHANGHAI_TZ).day == 1 for left, _ in monthly[1:])
    assert all((right + audit_module.timedelta(microseconds=1)).astimezone(audit_module.SHANGHAI_TZ).day == 1 for _, right in monthly[:-1])
    assert all(weekly[index][1] + audit_module.timedelta(microseconds=1) == weekly[index + 1][0] for index in range(len(weekly) - 1))
    assert all(monthly[index][1] + audit_module.timedelta(microseconds=1) == monthly[index + 1][0] for index in range(len(monthly) - 1))


def test_narrow_weekly_explicit_window_is_clipped_and_never_expands_to_years() -> None:
    start = datetime(2026, 1, 14, 16, tzinfo=UTC)  # Shanghai Jan 15 00:00
    end = datetime(2026, 1, 16, 15, 59, 59, 999999, tzinfo=UTC)
    assert list(aligned_windows(start, end, "1w")) == [(start, end)]


def test_cli_rejects_invalid_bounds_uuid_and_transaction_cap() -> None:
    for argv in (
        ["--apply", "--audit-run-id", "nope"],
        ["--start", "2026-01-01T00:00:00"],
        ["--start", "2026-01-02", "--end", "2026-01-01"],
        ["--transaction-group-cap", "501"],
    ):
        with pytest.raises(SystemExit):
            parse_args(argv)


def test_main_returns_nonzero_when_summary_has_shard_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    async def failed_run(args):
        return {"failures": 1}

    monkeypatch.setattr(audit_module, "parse_args", lambda: type("Args", (), {"database_url": "postgresql://read-only"})())
    monkeypatch.setattr(audit_module, "run_audit", failed_run)
    with pytest.raises(SystemExit) as error:
        audit_module.main()
    assert error.value.code == 1


def test_dry_run_performs_zero_writes_and_reports_disagreement() -> None:
    runner = AuditRunner(apply=False, audit_run_id=None, transaction_cap=500)
    actions = asyncio.run(runner.plan_group([row(), row(source=4, close_x1000=1111)], None))
    assert runner.write_count == 0
    assert actions.disagreement is True
    assert len(actions.quarantine) == 1
    assert len(actions.delete) == 1


def test_apply_metadata_serializes_parsed_timestamp_bounds(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured_parameters: list[str] = []
    record = dict(row().__dict__)
    record.pop("timeframe")

    class Connection:
        async def execute(self, sql: str, *args: object):
            if "insert into kline_audit_runs" in sql.lower():
                captured_parameters.append(str(args[2]))

        async def fetch(self, sql: str, *args: object, timeout: float | None = None):
            if "from symbols" in sql.lower():
                return [{"id": 1}]
            if "timeframe = 1440" in sql.lower():
                return []
            return [record]

        async def fetchrow(self, sql: str, *args: object, timeout: float | None = None):
            return {"min_ts": row().ts, "max_ts": row().ts}

        async def fetchval(self, sql: str, *args: object, timeout: float | None = None):
            return None

        def transaction(self):
            class Context:
                async def __aenter__(self): return None
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()

    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self): return Connection()
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()

        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False

    monkeypatch.setattr(audit_module.asyncpg, "create_pool", lambda *args, **kwargs: Pool())
    args = parse_args([
        "--database-url", "postgresql://apply-test", "--apply",
        "--audit-run-id", "00000000-0000-0000-0000-000000000002",
        "--timeframes", "1d", "--start", "2026-01-01T00:00:00+00:00",
        "--end", "2026-01-02T00:00:00+00:00", "--output-dir", str(tmp_path),
    ])

    asyncio.run(run_audit(args))

    assert len(captured_parameters) == 1
    assert json.loads(captured_parameters[0])["start"] == "2026-01-01T00:00:00+00:00"


def test_dry_run_resume_uses_only_read_pool_methods(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, float | None]] = []
    weekly = row(timeframe="1w", ts=datetime(2025, 12, 28, 16, tzinfo=UTC))
    record = dict(weekly.__dict__)
    record.pop("timeframe")

    class Connection:
        async def fetch(self, sql: str, *args: object, timeout: float | None = None):
            calls.append(("fetch", timeout))
            if "from symbols" in sql.lower():
                return [{"id": 1}]
            if "timeframe = 1440" in sql.lower():
                return []
            return [record]

        async def fetchrow(self, sql: str, *args: object, timeout: float | None = None):
            calls.append(("fetchrow", timeout))
            return {"min_ts": weekly.ts, "max_ts": weekly.ts}

        async def fetchval(self, sql: str, *args: object, timeout: float | None = None):
            calls.append(("fetchval", timeout))
            return None

        async def execute(self, sql: str, *args: object):
            raise AssertionError(f"dry-run write attempted: {sql}")

    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self): return Connection()
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()

        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False

    monkeypatch.setattr(audit_module.asyncpg, "create_pool", lambda *args, **kwargs: Pool())
    args = parse_args(["--database-url", "postgresql://read-only", "--resume", "--audit-run-id", "00000000-0000-0000-0000-000000000001", "--timeframes", "1w", "--output-dir", str(tmp_path)])
    asyncio.run(run_audit(args))
    assert {method for method, _ in calls} == {"fetch", "fetchrow", "fetchval"}
    assert all(timeout == 20 for _, timeout in calls)


def test_dry_run_effective_window_never_leaves_explicit_bounds(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    fetch_windows: list[tuple[object, ...]] = []
    minimum = datetime(2025, 1, 1, 7, tzinfo=UTC)
    maximum = datetime(2027, 1, 1, 7, tzinfo=UTC)
    weekly = row(timeframe="1w", ts=datetime(2026, 1, 15, 7, tzinfo=UTC))
    record = dict(weekly.__dict__)
    record.pop("timeframe")

    class Connection:
        async def fetch(self, sql: str, *args: object, timeout: float | None = None):
            if "from symbols" in sql.lower():
                return [{"id": 1}]
            fetch_windows.append(args)
            return [record]
        async def fetchrow(self, sql: str, *args: object, timeout: float | None = None):
            return {"min_ts": minimum, "max_ts": maximum}
        async def fetchval(self, sql: str, *args: object, timeout: float | None = None): return None
        async def execute(self, sql: str, *args: object): raise AssertionError("dry-run write")

    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self): return Connection()
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False

    monkeypatch.setattr(audit_module.asyncpg, "create_pool", lambda *args, **kwargs: Pool())
    args = parse_args(["--database-url", "postgresql://read-only", "--timeframes", "1w", "--start", "2026-01-15", "--end", "2026-01-16", "--output-dir", str(tmp_path)])
    asyncio.run(run_audit(args))
    assert fetch_windows == [(1, 10080, args.start, args.end)]


def test_apply_quarantines_before_delete_and_rolls_back() -> None:
    events: list[str] = []

    class Connection:
        async def execute(self, sql: str, *args: object) -> str:
            if "set_config" in sql:
                return "OK"
            events.append("quarantine" if "kline_audit_quarantine" in sql else "delete")
            if "delete from klines" in sql.lower():
                raise RuntimeError("delete failed")
            return "OK"

        def transaction(self):
            class Context:
                async def __aenter__(self): return object()  # asyncpg Transaction has no execute().
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()

    with pytest.raises(RuntimeError):
        asyncio.run(apply_actions(Connection(), "run-1", [row()], [row(source=4)]))
    assert events == ["quarantine", "delete"]


def test_apply_batches_501_groups_and_uses_connection_inside_real_transaction_shape() -> None:
    events: list[str] = []

    class Connection:
        async def fetchrow(self, sql: str, *args: object):
            assert "from kline_scope_catalog_control" in sql
            return {"control_key": "active"}

        async def execute(self, sql: str, *args: object) -> str:
            events.append("q" if "kline_audit_quarantine" in sql else "d" if "delete from klines" in sql.lower() else "set")
            return "UPDATE 1" if "update kline_scope_catalog" in sql.lower() else "OK"

        def transaction(self):
            class Context:
                async def __aenter__(self):
                    events.append("begin")
                    return object()
                async def __aexit__(self, exc_type, exc, tb):
                    events.append("rollback" if exc_type else "commit")
                    return False
            return Context()

    groups = [type("Actions", (), {"quarantine": [row(source=4)], "delete": [row(source=4)], "normalize": None, "disagreement": False})() for _ in range(501)]
    asyncio.run(apply_action_batches(Connection(), "00000000-0000-0000-0000-000000000001", groups, group_cap=500, lock_timeout_seconds=1))
    assert events.count("begin") == 2
    assert events.count("commit") == 2
    assert events.index("q") < events.index("d")


def test_apply_batch_invalidates_deduplicated_mutated_scopes_after_all_writes(monkeypatch) -> None:
    events: list[str] = []
    invalidated: list[tuple[int, int]] = []

    class Connection:
        async def execute(self, sql: str, *args: object) -> str:
            if "set_config" not in sql:
                events.append("mutation")
            return "DELETE 1"

        def transaction(self):
            class Context:
                async def __aenter__(self):
                    events.append("begin")
                    return object()

                async def __aexit__(self, exc_type, exc, tb):
                    events.append("rollback" if exc_type else "commit")
                    return False

            return Context()

    async def invalidate_scopes(_connection, *, scopes):
        events.append("catalog-invalidate")
        invalidated.extend(scopes)
        return len(scopes)

    monkeypatch.setattr(audit_module, "invalidate_scopes", invalidate_scopes)
    groups = [
        PlannedActions(delete=[row(), row(source=4)]),
        PlannedActions(delete=[row(symbol_id=2, timeframe="1w")]),
    ]

    asyncio.run(apply_action_batches(
        Connection(),
        "00000000-0000-0000-0000-000000000001",
        groups,
        group_cap=500,
        lock_timeout_seconds=1,
    ))

    assert invalidated == [(1, 1440), (2, 10080)]
    assert events == ["begin", "mutation", "mutation", "mutation", "catalog-invalidate", "commit"]


def test_apply_batch_rolls_back_when_a_later_delete_fails() -> None:
    events: list[str] = []

    class Connection:
        async def execute(self, sql: str, *args: object) -> str:
            events.append("delete" if "delete from klines" in sql.lower() else "write")
            if events.count("delete") == 2:
                raise RuntimeError("boom")
            return "OK"

        def transaction(self):
            class Context:
                async def __aenter__(self): events.append("begin"); return object()
                async def __aexit__(self, exc_type, exc, tb): events.append("rollback" if exc_type else "commit"); return False
            return Context()

    actions = PlannedActions(quarantine=[row(source=4)], delete=[row(source=4)])
    with pytest.raises(RuntimeError):
        asyncio.run(apply_action_batches(Connection(), "00000000-0000-0000-0000-000000000001", [actions, actions], group_cap=500, lock_timeout_seconds=1))
    assert events[-1] == "rollback"


def test_normalization_moves_timestamp_by_delete_then_insert() -> None:
    events: list[str] = []
    source = row(ts=datetime(2026, 1, 2, 16, tzinfo=UTC))
    target = datetime(2026, 1, 3, 7, tzinfo=UTC)

    class Connection:
        async def fetchrow(self, sql: str, *args: object):
            assert "from kline_scope_catalog_control" in sql
            return {"control_key": "active"}

        async def execute(self, sql: str, *args: object) -> str:
            if "set_config" in sql:
                return "OK"
            if sql.lstrip().lower().startswith("delete from klines"):
                events.append("delete-source")
                assert args == (1, 1440, source.ts, 9, 1, source.updated_at)
                return "DELETE 1"
            if sql.lstrip().lower().startswith("insert into klines"):
                events.append("insert-target")
                assert args[:3] == (1, 1440, target)
                assert args[3:12] == (1000, 1200, 900, 1100, 10, 100, True, 1, 9)
                assert args[12] == source.updated_at
                return "INSERT 1"
            if sql.lstrip().lower().startswith("update kline_scope_catalog"):
                events.append("catalog-invalidate")
                assert args == ([1], [1440])
                return "UPDATE 1"
            raise AssertionError(sql)

        def transaction(self):
            class Context:
                async def __aenter__(self): events.append("begin"); return object()
                async def __aexit__(self, exc_type, exc, tb): events.append("rollback" if exc_type else "commit"); return False
            return Context()

    asyncio.run(apply_actions(Connection(), "run-1", [], [], normalize=(source, target)))
    assert events == ["begin", "delete-source", "insert-target", "catalog-invalidate", "commit"]


def test_normalization_target_collision_rolls_back_original_delete() -> None:
    events: list[str] = []
    source = row(ts=datetime(2026, 1, 2, 16, tzinfo=UTC))

    class Connection:
        async def execute(self, sql: str, *args: object) -> str:
            if "set_config" in sql:
                return "OK"
            if sql.lstrip().lower().startswith("delete from klines"):
                events.append("delete-source")
                return "DELETE 1"
            if sql.lstrip().lower().startswith("insert into klines"):
                events.append("insert-target")
                return "INSERT 0"
            raise AssertionError(sql)

        def transaction(self):
            class Context:
                async def __aenter__(self): events.append("begin"); return object()
                async def __aexit__(self, exc_type, exc, tb): events.append("rollback" if exc_type else "commit"); return False
            return Context()

    with pytest.raises(RuntimeError, match="target collided"):
        asyncio.run(apply_actions(Connection(), "run-1", [], [], normalize=(source, datetime(2026, 1, 3, 7, tzinfo=UTC))))
    assert events == ["begin", "delete-source", "insert-target", "rollback"]


def test_jsonl_writers_use_unique_temp_paths_and_cleanup(tmp_path) -> None:
    first = AtomicJsonlWriter(tmp_path / "conflicts.jsonl")
    second = AtomicJsonlWriter(tmp_path / "conflicts.jsonl")
    assert first.temp_path != second.temp_path
    first.write({"a": 1})
    first.cleanup()
    assert not first.temp_path.exists()
    second.write({"a": 2})
    second.promote()
    assert (tmp_path / "conflicts.jsonl").read_text(encoding="utf-8") == '{"a": 2}\n'


def test_atomic_write_uses_unique_temp_and_cleans_up(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "summary.json"
    audit_module._atomic_write(target, "one")
    assert target.read_text(encoding="utf-8") == "one"
    assert not list(tmp_path.glob(".summary.json.*.tmp"))

    def fail_replace(self, other):
        raise OSError("replace failed")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError):
        audit_module._atomic_write(target, "two")
    assert not list(tmp_path.glob(".summary.json.*.tmp"))


def test_directory_fsync_runs_on_posix_after_atomic_replace(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(audit_module.os, "open", lambda path, flags: calls.append(("open", (path, flags))) or 42)
    monkeypatch.setattr(audit_module.os, "fsync", lambda descriptor: calls.append(("fsync", descriptor)))
    monkeypatch.setattr(audit_module.os, "close", lambda descriptor: calls.append(("close", descriptor)))
    audit_module._fsync_directory(tmp_path, platform="posix")
    assert [name for name, _ in calls] == ["open", "fsync", "close"]


def test_directory_fsync_is_noop_on_windows(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(audit_module.os, "open", lambda *args: pytest.fail("Windows must not open directory"))
    audit_module._fsync_directory(tmp_path, platform="nt")


def test_dry_resume_streams_durable_checkpoint_markers(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = "00000000-0000-0000-0000-000000000099"
    output = tmp_path / run_id
    for index in range(2000):
        write_checkpoint_marker(output, {"symbol_id": index, "timeframe": "1w", "shard_start": "a", "shard_end": "b", "status": "completed"})

    class Connection:
        async def fetch(self, sql: str, *args: object, timeout: float | None = None): return []
        async def fetchrow(self, sql: str, *args: object, timeout: float | None = None): return None
        async def fetchval(self, sql: str, *args: object, timeout: float | None = None): return None
        async def execute(self, sql: str, *args: object): raise AssertionError("dry-run write")
    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self): return Connection()
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False

    monkeypatch.setattr(audit_module.asyncpg, "create_pool", lambda *args, **kwargs: Pool())
    args = parse_args(["--database-url", "postgresql://read-only", "--resume", "--audit-run-id", run_id, "--output-dir", str(tmp_path)])
    asyncio.run(run_audit(args))


def test_checkpoint_markers_survive_interruption_and_ignore_corrupt_marker(tmp_path) -> None:
    output = tmp_path / "run"
    first = {"symbol_id": 1, "timeframe": "1w", "shard_start": "2026-01-01", "shard_end": "2026-01-02", "status": "completed"}
    second = {"symbol_id": 2, "timeframe": "1w", "shard_start": "2026-01-01", "shard_end": "2026-01-02", "status": "completed"}
    write_checkpoint_marker(output, first)
    write_checkpoint_marker(output, second)
    state = output / ".checkpoint-state"
    (state / "partial.json").write_text("{not json", encoding="utf-8")

    completed, invalid = load_checkpoint_markers(output)
    assert completed == {(1, "1w", "2026-01-01", "2026-01-02"), (2, "1w", "2026-01-01", "2026-01-02")}
    assert invalid == 1
    records, ignored = consolidate_checkpoint_markers(output)
    assert (records, ignored) == (2, 1)
    assert len((output / "checkpoints.jsonl").read_text(encoding="utf-8").splitlines()) == 2


def test_fresh_run_clears_only_current_checkpoint_state_and_temps(tmp_path) -> None:
    output = tmp_path / "run"
    write_checkpoint_marker(output, {"symbol_id": 1, "timeframe": "1w", "shard_start": "a", "shard_end": "b", "status": "completed"})
    state = output / ".checkpoint-state"
    stale = state / ".stale.tmp"
    stale.write_text("partial", encoding="utf-8")
    unrelated = output / "keep.txt"
    unrelated.write_text("keep", encoding="utf-8")
    clear_checkpoint_state(output)
    assert not list(state.iterdir())
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_sole_valid_bar_is_never_deleted_and_invalid_intraday_is_unresolved() -> None:
    runner = AuditRunner(apply=True, audit_run_id="run-1", transaction_cap=500)
    sole = asyncio.run(runner.plan_group([row()], None))
    invalid = asyncio.run(runner.plan_group([row(timeframe="5f", ts=datetime(2026, 1, 2, 2, 2, tzinfo=UTC))], None))
    assert sole.delete == []
    assert invalid.unresolved is True


def test_invalid_loser_is_removed_only_with_valid_deterministic_winner() -> None:
    runner = AuditRunner(apply=True, audit_run_id="run-1", transaction_cap=500)
    valid = row(timeframe="5f", ts=datetime(2026, 1, 2, 1, 35, tzinfo=UTC))
    invalid_loser = row(timeframe="5f", ts=valid.ts, source=4, volume=-1)
    actions = asyncio.run(runner.plan_group([valid, invalid_loser], None))
    assert actions.winner is valid
    assert actions.quarantine == [invalid_loser]
    assert actions.delete == [invalid_loser]


def lunch_row(**overrides: object) -> AuditRow:
    values: dict[str, object] = {"timeframe": "15f", "source": 2, "ts": datetime(2026, 1, 2, 5, tzinfo=UTC)}
    values.update(overrides)
    return row(**values)


def comparator_row(**overrides: object) -> AuditRow:
    values: dict[str, object] = {"timeframe": "15f", "source": 2, "ts": datetime(2026, 1, 2, 3, 30, tzinfo=UTC)}
    values.update(overrides)
    return row(**values)


def test_matching_source2_lunch_reopen_duplicate_is_planned_for_quarantine_delete() -> None:
    lunch = lunch_row()
    comparator = comparator_row()
    actions = plan_lunch_reopen_duplicate(lunch, comparator)
    assert actions.winner is comparator
    assert actions.quarantine == [lunch]
    assert actions.delete == [lunch]
    assert actions.quarantine_reason == "lunch_reopen_duplicate"


@pytest.mark.parametrize("comparator", [None, comparator_row(close_x1000=1111)])
def test_lunch_reopen_without_matching_1130_is_unresolved(comparator: AuditRow | None) -> None:
    actions = plan_lunch_reopen_duplicate(lunch_row(), comparator)
    assert actions.unresolved is True
    assert actions.disagreement is True
    assert actions.delete == []


@pytest.mark.parametrize(
    ("comparator", "reason"),
    [
        (comparator_row(source=9), "missing_same_source_1130_comparator"),
        (comparator_row(amount_x100=None), "amount_unproven"),
    ],
)
def test_lunch_reopen_requires_same_source_and_proven_amount(comparator: AuditRow, reason: str) -> None:
    actions = plan_lunch_reopen_duplicate(lunch_row(), comparator, cross_source_match=comparator.source != 2)
    assert actions.unresolved is True
    assert reason in actions.reasons
    assert actions.quarantine == []
    assert actions.delete == []
    if comparator.source != 2:
        assert "cross_source_match" in actions.reasons


def test_invalid_1300_is_repaired_only_when_same_day_has_valid_afternoon_bar() -> None:
    invalid = lunch_row(timeframe="5f")
    repaired = plan_lunch_reopen_duplicate(invalid, None, has_valid_afternoon_bar=True)
    unresolved = plan_lunch_reopen_duplicate(invalid, None, has_valid_afternoon_bar=False)
    assert repaired.quarantine == [invalid]
    assert repaired.delete == [invalid]
    assert repaired.quarantine_reason == "invalid_lunch_reopen_timestamp"
    assert unresolved.unresolved is True


def test_lunch_reopen_plans_detects_valid_afternoon_companion() -> None:
    invalid = lunch_row(timeframe="5f")
    valid_afternoon = lunch_row(timeframe="5f", source=6, ts=datetime(2026, 1, 2, 5, 5, tzinfo=UTC))
    plans = audit_module._lunch_reopen_plans([invalid, valid_afternoon], None)
    assert plans[invalid].delete == [invalid]
    assert plans[invalid].quarantine_reason == "invalid_lunch_reopen_timestamp"


def test_lunch_reopen_dry_run_has_no_writes() -> None:
    runner = AuditRunner(apply=False, audit_run_id=None)
    actions = plan_lunch_reopen_duplicate(lunch_row(), comparator_row())
    assert actions.delete
    assert runner.write_count == 0


def test_lunch_reopen_apply_quarantines_before_delete() -> None:
    events: list[tuple[str, str | None]] = []
    lunch = lunch_row()
    actions = plan_lunch_reopen_duplicate(lunch, comparator_row())

    class Connection:
        async def fetchrow(self, sql: str, *args: object):
            assert "from kline_scope_catalog_control" in sql
            return {"control_key": "active"}

        async def execute(self, sql: str, *args: object) -> str:
            if "kline_audit_quarantine" in sql:
                events.append(("quarantine", args[1]))
            elif "delete from klines" in sql.lower():
                events.append(("delete", None))
            if "update kline_scope_catalog" in sql.lower():
                return "UPDATE 1"
            return "OK"
        def transaction(self):
            class Context:
                async def __aenter__(self): return object()
                async def __aexit__(self, exc_type, exc, tb): return False
            return Context()

    asyncio.run(apply_action_batches(Connection(), "00000000-0000-0000-0000-000000000001", [actions], group_cap=500, lock_timeout_seconds=1))
    assert events == [("quarantine", "lunch_reopen_duplicate"), ("delete", None)]


def test_monthly_midnight_fallback_without_trustworthy_daily_end_is_unresolved() -> None:
    runner = AuditRunner(apply=True, audit_run_id="run-1")
    fallback = row(timeframe="1m", ts=datetime(2026, 1, 31, 16, tzinfo=UTC))
    actions = asyncio.run(runner.plan_group([fallback], None, expected_timestamp=None))
    assert actions.unresolved is True
    assert actions.normalize is None


@pytest.mark.parametrize(
    ("timeframe", "fallback_ts", "expected_ts", "now", "should_normalize"),
    [
        ("1w", datetime(2026, 1, 4, 16, tzinfo=UTC), datetime(2026, 1, 9, 7, tzinfo=UTC), datetime(2026, 1, 15, 1, tzinfo=UTC), True),
        ("1w", datetime(2026, 1, 11, 16, tzinfo=UTC), datetime(2026, 1, 14, 7, tzinfo=UTC), datetime(2026, 1, 15, 1, tzinfo=UTC), False),
        ("1m", datetime(2025, 12, 30, 16, tzinfo=UTC), datetime(2025, 12, 31, 7, tzinfo=UTC), datetime(2026, 1, 15, 1, tzinfo=UTC), True),
        ("1m", datetime(2026, 1, 31, 16, tzinfo=UTC), datetime(2026, 2, 10, 7, tzinfo=UTC), datetime(2026, 2, 10, 1, tzinfo=UTC), False),
    ],
)
def test_weekly_monthly_normalization_requires_closed_shanghai_period(
    timeframe: str, fallback_ts: datetime, expected_ts: datetime, now: datetime, should_normalize: bool
) -> None:
    runner = AuditRunner(apply=True, audit_run_id="run-1")
    actions = asyncio.run(runner.plan_group(
        [row(timeframe=timeframe, ts=fallback_ts)],
        None,
        expected_timestamp=expected_ts,
        now=now,
    ))
    assert (actions.normalize is not None) is should_normalize
    assert actions.unresolved is (not should_normalize)


def test_resume_idempotency_and_transaction_cap() -> None:
    runner = AuditRunner(apply=True, audit_run_id="run-1", transaction_cap=2)
    assert runner.next_groups([[row()], [row()], [row()]]) == [[row()], [row()]]
    runner.completed_shards.add((1, "1d", "2026-01-01", "2026-12-31"))
    assert runner.should_skip_shard(1, "1d", "2026-01-01", "2026-12-31")
