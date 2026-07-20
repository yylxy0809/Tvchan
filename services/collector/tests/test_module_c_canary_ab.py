from __future__ import annotations

from datetime import UTC, datetime

import pytest

from collector.module_c_canary_ab import (
    PUBLISHED_RUNS_SQL,
    _center_record,
    _json_object,
    _line_record,
    _signal_record,
    compare_payloads,
    parse_args,
)


def test_ab_selector_uses_completed_task_runs_not_only_new_head_history() -> None:
    normalized = " ".join(PUBLISHED_RUNS_SQL.lower().split())
    assert "join chan_c_full_recompute_tasks task on task.run_id = r.id" in normalized
    assert "task.status = 'completed'" in normalized
    assert "join chan_c_head_history" not in normalized


def test_line_record_preserves_point_order_direction_and_native_time() -> None:
    assert _line_record(
        {
            "mode": "confirmed",
            "start": {"time": 100, "price": 10.123},
            "end": {"time": 200, "price": 11.456},
            "direction": "up",
            "confirmed": True,
            "begin_base_ts": 101,
            "end_base_ts": 199,
            "begin_base_seq": 3,
            "end_base_seq": 7,
        },
        seq=2,
    ) == {
        "seq": 2,
        "mode": "confirmed",
        "start_time": 100,
        "end_time": 200,
        "start_price_x1000": 10123,
        "end_price_x1000": 11456,
        "direction": "up",
        "confirmed": True,
        "begin_base_ts": 101,
        "end_base_ts": 199,
        "begin_base_seq": 3,
        "end_base_seq": 7,
    }


def test_center_and_signal_records_cover_ab_contract_fields() -> None:
    when = datetime(2026, 7, 1, tzinfo=UTC)
    assert _center_record(
        {
            "mode": "predictive",
            "start_time": when,
            "end_time": 200,
            "low": 9.1,
            "high": 10.2,
            "confirmed": False,
        },
        seq=1,
    )["low_x1000"] == 9100
    signal = _signal_record(
        {
            "mode": "confirmed",
            "time": 300,
            "price": 12.345,
            "signal_type": "buy_1",
            "side": "buy",
            "bsp_type": "1",
            "confirmed": True,
        }
    )
    assert signal["price_x1000"] == 12345
    assert signal["side"] == "buy"
    assert signal["bsp_type"] == "1"
    assert _json_object('{"side":"buy"}')["side"] == "buy"


def test_compare_payloads_reports_pointwise_difference_and_caps_samples() -> None:
    direct = {"strokes": [{"seq": 0}, {"seq": 1}], "segments": [], "centers": [], "signals": []}
    persisted = {"strokes": [{"seq": 0}, {"seq": 9}, {"seq": 10}], "segments": [], "centers": [], "signals": []}
    result = compare_payloads(direct, persisted, max_samples=1)
    assert result["difference_count"] == 2
    assert result["objects"]["strokes"]["mismatch_count"] == 2
    assert len(result["objects"]["strokes"]["samples"]) == 1


def test_parse_args_requires_one_published_run_selector(tmp_path) -> None:
    with pytest.raises(SystemExit):
        parse_args(["--database-url", "postgresql://local/test", "--output-dir", str(tmp_path)])
    args = parse_args(
        [
            "--database-url",
            "postgresql://local/test",
            "--batch-id",
            "3",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert args.batch_id == 3
    assert args.run_group_id is None
