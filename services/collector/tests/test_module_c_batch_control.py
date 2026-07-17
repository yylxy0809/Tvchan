from __future__ import annotations

import json

import pytest

from collector.module_c_batch_control import (
    load_selection,
    validate_canary_report,
    validate_canary_run_set,
    validate_terminal_tasks,
)
from trading_protocol import MODULE_C_CONFIG_HASH


def _selection() -> dict[str, object]:
    names = ["600000.SH", "300001.SZ", "688001.SH", "920047.BJ"] + [
        f"{index:06d}.SZ" for index in range(4, 20)
    ]
    return {
        "contract_version": "module-c-canary-selection-v1",
        "symbols": [
            {
                "symbol": name,
                "traits": (
                    ["main_board", "suspended_or_sparse", "gap", "price_limit", "long_history"]
                    if index == 0 else ["chinext"] if index == 1 else ["star"] if index == 2
                    else ["bj"] if index == 3 else []
                ),
                "evidence": ["test"],
            }
            for index, name in enumerate(names)
        ],
    }


def test_selection_requires_exactly_twenty_unique_symbols_and_trait_coverage(tmp_path) -> None:
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(_selection()), encoding="utf-8")
    symbols, digest, payload = load_selection(path)
    assert len(symbols) == 20
    assert len(digest) == 64
    assert payload["contract_version"] == "module-c-canary-selection-v1"

    invalid = _selection()
    invalid["symbols"] = invalid["symbols"][:-1]
    path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly 20"):
        load_selection(path)


def test_selection_rejects_missing_diversity_trait(tmp_path) -> None:
    payload = _selection()
    payload["symbols"][3]["traits"].remove("bj")
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required traits"):
        load_selection(path)


def test_terminal_tasks_fail_closed() -> None:
    validate_terminal_tasks(
        child_status="completed",
        disposition_rows=100,
        statuses={"completed": 75, "excluded": 25},
    )
    with pytest.raises(RuntimeError, match="not completed"):
        validate_terminal_tasks(
            child_status="running", disposition_rows=100, statuses={"completed": 100}
        )
    with pytest.raises(RuntimeError, match="blocking"):
        validate_terminal_tasks(
            child_status="completed",
            disposition_rows=100,
            statuses={"completed": 99, "failed": 1},
        )


def test_canary_report_must_match_batch_and_cover_strict_result(tmp_path) -> None:
    path = tmp_path / "report.json"
    report = {
        "selector": {"batch_id": 42},
        "passed": True,
        "symbols": 20,
        "published_runs": 80,
        "failed_runs": 0,
        "difference_count": 0,
    }
    path.write_text(json.dumps(report), encoding="utf-8")
    digest, loaded = validate_canary_report(path, batch_id=42)
    assert len(digest) == 64
    assert loaded == report
    with pytest.raises(RuntimeError, match="batch_id"):
        validate_canary_report(path, batch_id=43)


def test_canary_report_run_set_must_exactly_match_completed_tasks() -> None:
    expected = [{"run_id": 11, "symbol": "600000.SH", "chan_level": 5}]
    report = {
        "published_runs": 1,
        "runs": [{
            "run_id": 11,
            "symbol": "600000.SH",
            "level": "5f",
            "modes": ["confirmed", "predictive"],
            "config_hash": MODULE_C_CONFIG_HASH,
            "passed": True,
        }],
    }
    validate_canary_run_set(report, expected)
    report["runs"][0]["run_id"] = 12
    with pytest.raises(RuntimeError, match="run set"):
        validate_canary_run_set(report, expected)
