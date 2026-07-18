from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.aggregate_increment_import import (
    AggregateTask,
    bind_quarantines_to_run,
    discover_tasks,
    load_authoritative_symbol_meta,
    manifest_run_id,
    parse_task,
    parse_args,
    preflight_manifest,
    run_once,
    symbol_master_evidence,
    verify_symbol_master,
    verify_frozen_file,
)
from collector.module_c_eligibility import FRESHNESS_CONTRACT_VERSION


def _write_intraday(path: Path, symbols: list[str], *, rows_per_group: int = 2) -> None:
    count = len(symbols)
    pq.write_table(
        pa.table({
            "trade_date": [datetime(2026, 7, 6)] * count,
            "trade_time": [datetime(2026, 7, 6, 9, 30)] * count,
            "ts_code": symbols,
            "open": [10.0] * count,
            "high": [10.5] * count,
            "low": [9.5] * count,
            "close": [10.0] * count,
            "vol": [1000.0] * count,
            "amount": [10_000.0] * count,
        }),
        path,
        row_group_size=rows_per_group,
    )


def _write_daily(path: Path, symbols: list[str]) -> None:
    count = len(symbols)
    pq.write_table(
        pa.table({
            "trade_date": [datetime(2026, 7, 6)] * count,
            "ts_code": symbols,
            "open": [10.0] * count,
            "high": [10.5] * count,
            "low": [9.5] * count,
            "close": [10.0] * count,
            "vol": [10.0] * count,
            "amount": [10.0] * count,
        }),
        path,
    )


def _write_master(path: Path) -> None:
    pq.write_table(pa.table({
        "ts_code": ["000001.SZ", "920001.BJ"],
        "name": ["平安银行", "北交新股"],
        "list_status": ["L", "L"],
    }), path)


def _write_freshness(path: Path) -> None:
    path.write_text(__import__("json").dumps({
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": "2026-07-10T08:00:00Z",
        "trading_calendar": {"id": "test-calendar", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            "5f": "2026-07-10T07:00:00Z", "30f": "2026-07-10T07:00:00Z",
            "1d": "2026-07-10T07:00:00Z", "1w": "2026-07-10T07:00:00Z",
            "1m": "2026-06-30T07:00:00Z",
        },
    }), encoding="utf-8")


def test_discovery_accepts_only_three_explicit_aggregate_files_and_chunks_rows(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ"] * 5)
    _write_intraday(tmp_path / "stock_30min_data.parquet", ["000001.SZ"] * 3)
    _write_daily(tmp_path / "stock_daily(1).parquet", ["000001.SZ"])
    _write_intraday(tmp_path / "stock_1min_data.parquet", ["000001.SZ"])

    tasks = discover_tasks(tmp_path, batch_size=2)

    assert {task.path.name for task in tasks} == {
        "stock_5min_data.parquet", "stock_30min_data.parquet", "stock_daily(1).parquet",
    }
    assert "1m" not in {task.timeframe for task in tasks}
    assert all("sha256=" in task.source_checksum for task in tasks)
    assert all("@sha256=" in task.source_ref for task in tasks)
    assert all("row_group=" in task.source_ref and "offset=" in task.source_ref for task in tasks)
    assert sum(task.row_count for task in tasks if task.timeframe == "5f") == 5


def test_parse_is_master_authoritative_and_excludes_bj_30f(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(
        tmp_path / "stock_30min_data.parquet",
        ["000001.SZ", "920001.BJ", "999999.SZ"],
        rows_per_group=10,
    )
    meta = load_authoritative_symbol_meta(tmp_path)
    task = discover_tasks(tmp_path, batch_size=10, filenames=("stock_30min_data.parquet",))[0]

    parsed = parse_task(task, symbol_meta=meta)

    assert [bar[0:2] for bar in parsed.bars] == [("000001", "SZ")]
    assert {item.reason for item in parsed.quarantines} == {
        "excluded_bj_30f_policy", "symbol_not_in_authoritative_master",
    }
    assert list(parsed.symbols.values())[0][2] == "平安银行"


def test_zero_turnover_flat_halt_is_quarantined_and_never_advances_stage(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    path = tmp_path / "stock_5min_data.parquet"
    pq.write_table(pa.table({
        "trade_date": [datetime(2026, 7, 6)],
        "trade_time": [datetime(2026, 7, 6, 9, 30)],
        "ts_code": ["000001.SZ"],
        "open": [10.0], "high": [10.0], "low": [10.0], "close": [10.0],
        "vol": [0.0], "amount": [0.0],
    }), path)
    task = discover_tasks(tmp_path, batch_size=10, filenames=(path.name,))[0]

    parsed = parse_task(task, symbol_meta=load_authoritative_symbol_meta(tmp_path))

    assert parsed.bars == []
    assert [item.reason for item in parsed.quarantines] == ["zero_turnover_halt_day_not_freshness"]


def test_bj_is_excluded_by_default_for_all_timeframes_and_30f_cannot_opt_in(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["920001.BJ"])
    five = discover_tasks(tmp_path, batch_size=10, filenames=("stock_5min_data.parquet",))[0]
    default = parse_task(five, symbol_meta=load_authoritative_symbol_meta(tmp_path))
    opted_in = parse_task(
        five,
        symbol_meta=load_authoritative_symbol_meta(tmp_path),
        exchanges={"BJ"},
        allow_bj=True,
    )
    assert [item.reason for item in default.quarantines] == ["excluded_bj_default_source_mismatch"]
    assert len(opted_in.bars) == 1

    _write_intraday(tmp_path / "stock_30min_data.parquet", ["920001.BJ"])
    thirty = discover_tasks(tmp_path, batch_size=10, filenames=("stock_30min_data.parquet",))[0]
    still_excluded = parse_task(
        thirty,
        symbol_meta=load_authoritative_symbol_meta(tmp_path),
        exchanges={"BJ"},
        allow_bj=True,
    )
    assert still_excluded.bars == []
    assert [item.reason for item in still_excluded.quarantines] == ["excluded_bj_30f_policy"]


def test_explicit_symbol_filter_is_canary_safe(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ", "920001.BJ"])
    task = discover_tasks(tmp_path, batch_size=10, filenames=("stock_5min_data.parquet",))[0]
    parsed = parse_task(
        task,
        symbol_meta=load_authoritative_symbol_meta(tmp_path),
        symbols_filter={"000001.SZ"},
    )
    assert [bar[:2] for bar in parsed.bars] == [("000001", "SZ")]
    assert parsed.quarantines == []


def test_master_symbol_still_requires_pinned_active_universe_membership(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ", "920001.BJ"])
    task = discover_tasks(tmp_path, batch_size=10, filenames=("stock_5min_data.parquet",))[0]
    parsed = parse_task(
        task,
        symbol_meta=load_authoritative_symbol_meta(tmp_path),
        exchanges={"SH", "SZ", "BJ"},
        allow_bj=True,
        pinned_symbols={"000001.SZ"},
    )
    assert [bar[:2] for bar in parsed.bars] == [("000001", "SZ")]
    assert [item.reason for item in parsed.quarantines] == ["symbol_not_in_pinned_active_universe"]


def test_file_mutation_changes_durable_identity(tmp_path: Path) -> None:
    path = tmp_path / "stock_5min_data.parquet"
    _write_intraday(path, ["000001.SZ"])
    first = discover_tasks(tmp_path, batch_size=10, filenames=(path.name,))[0]
    _write_intraday(path, ["000001.SZ", "000001.SZ"])
    second = discover_tasks(tmp_path, batch_size=10, filenames=(path.name,))[0]

    assert first.source_checksum != second.source_checksum
    with pytest.raises(RuntimeError, match="changed after manifest"):
        verify_frozen_file(first)


def test_invalid_batch_size_is_rejected_before_reading(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        discover_tasks(tmp_path, batch_size=0)


def test_task_batch_is_bounded_to_requested_offset(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ"] * 5, rows_per_group=5)
    tasks = discover_tasks(tmp_path, batch_size=2, filenames=("stock_5min_data.parquet",))
    assert [(task.row_offset, task.row_count) for task in tasks] == [(0, 2), (2, 2), (4, 1)]
    assert [len(parse_task(task, symbol_meta=load_authoritative_symbol_meta(tmp_path)).bars) for task in tasks] == [2, 2, 1]


def test_append_only_sql_contract_is_present() -> None:
    from collector.aggregate_increment_import import (
        APPEND_ONLY_MISMATCH_SQL,
        INSERT_NEW_ROWS_SQL,
        LOCK_ACTIVE_SYMBOLS_SQL,
    )

    normalized = " ".join(APPEND_ONLY_MISMATCH_SQL.lower().split())
    assert "stage.bar_end <= scope.original_max" in normalized
    assert "is distinct from" in normalized
    assert "existing.revision is distinct from stage.revision" in normalized
    assert "existing.source is distinct from stage.source" in normalized
    assert "raise" not in normalized
    assert "stage.bar_end > scope.original_max" in " ".join(INSERT_NEW_ROWS_SQL.lower().split())
    assert "on conflict" in INSERT_NEW_ROWS_SQL.lower()
    locked = " ".join(LOCK_ACTIVE_SYMBOLS_SQL.lower().split())
    assert "symbol.is_active is true" in locked
    assert "for share of symbol" in locked


def test_run_identity_binds_canary_selection_and_exchange_policy(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ"])
    tasks = discover_tasks(tmp_path, batch_size=10, filenames=("stock_5min_data.parquet",))
    full = manifest_run_id(tmp_path, tasks)
    canary = manifest_run_id(tmp_path, tasks, symbols={"000001.SZ"})
    bj = manifest_run_id(tmp_path, tasks, exchanges={"BJ"}, allow_bj=True)
    assert len({full, canary, bj}) == 3


def test_dry_run_is_offline_and_reports_frozen_file_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ"])
    _write_intraday(tmp_path / "stock_30min_data.parquet", ["000001.SZ"])
    _write_daily(tmp_path / "stock_daily(1).parquet", ["000001.SZ"])
    monkeypatch.delenv("DATABASE_URL", raising=False)
    args = parse_args(["--root", str(tmp_path), "--dry-run"])

    import asyncio
    result = asyncio.run(run_once(args))

    assert args.database_url is None
    assert result["dry_run"] is True
    assert result["validation_level"] == "manifest_only"
    assert [(item["name"], item["rows"], item["tasks"]) for item in result["source_files"]] == [
        ("stock_5min_data.parquet", 1, 1),
        ("stock_30min_data.parquet", 1, 1),
        ("stock_daily(1).parquet", 1, 1),
    ]
    assert all(len(item["sha256"]) == 64 for item in result["source_files"])


def test_write_mode_requires_database_url_after_offline_validation(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ"])
    _write_intraday(tmp_path / "stock_30min_data.parquet", ["000001.SZ"])
    _write_daily(tmp_path / "stock_daily(1).parquet", ["000001.SZ"])
    _write_freshness(tmp_path / "freshness.json")
    args = parse_args([
        "--root", str(tmp_path),
        "--freshness-contract", str(tmp_path / "freshness.json"),
        "--expected-quarantine-counts", "{}",
    ])
    args.database_url = None
    import asyncio
    with pytest.raises(ValueError, match="database-url"):
        asyncio.run(run_once(args))


def test_cross_row_group_conflict_fails_during_zero_db_write_preflight(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    path = tmp_path / "stock_5min_data.parquet"
    pq.write_table(pa.table({
        "trade_date": [datetime(2026, 7, 6)] * 2,
        "trade_time": [datetime(2026, 7, 6, 9, 30)] * 2,
        "ts_code": ["000001.SZ"] * 2,
        "open": [10.0, 10.0], "high": [10.5, 10.5], "low": [9.5, 9.5],
        "close": [10.0, 10.2], "vol": [1000.0, 1000.0], "amount": [10_000.0, 10_000.0],
    }), path, row_group_size=1)
    tasks = discover_tasks(tmp_path, batch_size=1, filenames=(path.name,))
    meta = load_authoritative_symbol_meta(tmp_path)
    with pytest.raises(RuntimeError, match="cross-batch"):
        preflight_manifest(
            tasks,
            symbol_meta=meta,
            symbols_filter=set(),
            exchanges={"SH", "SZ"},
            allow_bj=False,
            halted_days=set(),
            pinned_symbols=set(meta),
            expected_closed=None,
            expected_quarantine_counts={},
            expected_excluded_symbols=set(),
        )


def test_authoritative_upper_bound_quarantines_future_without_staging(tmp_path: Path) -> None:
    root = tmp_path
    _write_master(root / "stock_basic_data.parquet")
    _write_intraday(root / "stock_5min_data.parquet", ["000001.SZ"])
    task = discover_tasks(root, batch_size=1, filenames=("stock_5min_data.parquet",))[0]
    parsed = parse_task(
        task,
        symbol_meta=load_authoritative_symbol_meta(root),
        expected_closed={"5f": datetime.fromisoformat("2026-07-05T07:00:00+00:00")},
    )
    assert parsed.bars == []
    assert [item.reason for item in parsed.quarantines] == ["after_authoritative_expected_closed_bound"]

    with pytest.raises(RuntimeError, match="disposition drift"):
        preflight_manifest(
            [task], symbol_meta=load_authoritative_symbol_meta(root), symbols_filter=set(),
            exchanges={"SH", "SZ"}, allow_bj=False, halted_days=set(),
            pinned_symbols={"000001.SZ"},
            expected_closed={"5f": datetime.fromisoformat("2026-07-05T07:00:00+00:00")},
            expected_quarantine_counts={}, expected_excluded_symbols=set(),
        )


def test_invalid_rows_cannot_be_declared_as_policy_or_complete(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    path = tmp_path / "stock_5min_data.parquet"
    _write_intraday(path, ["000001.SZ"])
    table = pq.read_table(path).set_column(3, "open", pa.array([-1.0]))
    pq.write_table(table, path)
    task = discover_tasks(tmp_path, filenames=(path.name,))[0]
    with pytest.raises(RuntimeError, match="disposition drift"):
        preflight_manifest(
            [task], symbol_meta=load_authoritative_symbol_meta(tmp_path), symbols_filter=set(),
            exchanges={"SH", "SZ"}, allow_bj=False, halted_days=set(),
            pinned_symbols={"000001.SZ"}, expected_closed=None,
            expected_quarantine_counts={}, expected_excluded_symbols=set(),
        )


def test_symbol_master_evidence_detects_replacement(tmp_path: Path) -> None:
    path = tmp_path / "stock_basic_data.parquet"
    _write_master(path)
    evidence = symbol_master_evidence(tmp_path)
    pq.write_table(pa.table({
        "ts_code": ["000001.SZ"], "name": ["changed"], "list_status": ["L"],
    }), path)
    with pytest.raises(RuntimeError, match="symbol master changed"):
        verify_symbol_master(tmp_path, evidence)


def test_quarantine_identity_is_bound_to_frozen_run(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["920001.BJ"])
    task = discover_tasks(tmp_path, filenames=("stock_5min_data.parquet",))[0]
    parsed = parse_task(task, symbol_meta=load_authoritative_symbol_meta(tmp_path))
    first = manifest_run_id(tmp_path, [task])
    second = manifest_run_id(tmp_path, [task], symbols={"920001.BJ"})
    assert bind_quarantines_to_run(parsed, first).quarantines[0].source_ref.endswith(f"@run_id={first}")
    assert bind_quarantines_to_run(parsed, second).quarantines[0].source_ref.endswith(f"@run_id={second}")


def test_write_mode_rejects_naked_expected_bounds(tmp_path: Path) -> None:
    _write_master(tmp_path / "stock_basic_data.parquet")
    _write_intraday(tmp_path / "stock_5min_data.parquet", ["000001.SZ"])
    _write_intraday(tmp_path / "stock_30min_data.parquet", ["000001.SZ"])
    _write_daily(tmp_path / "stock_daily(1).parquet", ["000001.SZ"])
    args = parse_args([
        "--root", str(tmp_path), "--expected-closed-5f", "2026-07-10T07:00:00Z",
        "--expected-quarantine-counts", "{}", "--database-url", "postgresql://unused",
    ])
    import asyncio
    with pytest.raises(ValueError, match="freshness-contract"):
        asyncio.run(run_once(args))
