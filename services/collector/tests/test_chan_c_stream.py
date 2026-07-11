from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from collector.chan_c_stream import group_jobs_by_level, group_jobs_by_mode

from collector.storage.chan_c_stream_postgres import (
    closed_period_cutoffs_utc,
    queue_name_for_chan_c,
    schedule_interval_seconds,
)


def test_chan_c_stream_queue_names_and_intervals_cover_five_levels() -> None:
    assert queue_name_for_chan_c("5f") == "chan_c_5f"
    assert schedule_interval_seconds("5f") == 300
    assert schedule_interval_seconds("30f") == 1800
    assert schedule_interval_seconds("1d") == 7200
    assert schedule_interval_seconds("1w") == 10080 * 60
    assert schedule_interval_seconds("1m") == 43200 * 60


def test_chan_c_stream_groups_jobs_by_native_level_and_mode() -> None:
    jobs = [
        {"chan_level": 5, "mode": "confirmed"},
        {"chan_level": 30, "mode": "confirmed"},
        {"chan_level": 30, "mode": "predictive"},
        {"chan_level": 1440, "mode": "confirmed"},
        {"chan_level": 10080, "mode": "confirmed"},
        {"chan_level": 43200, "mode": "predictive"},
    ]

    by_level = group_jobs_by_level(jobs)
    assert sorted(by_level) == ["1d", "1m", "1w", "30f", "5f"]
    assert len(by_level["30f"]) == 2

    by_mode = group_jobs_by_mode(by_level["30f"])
    assert sorted(by_mode) == ["confirmed", "predictive"]


def test_chan_c_stream_uses_frozen_module_c_semantic_hash_for_tail_runs() -> None:
    source = Path(__file__).resolve().parents[1] / "collector" / "chan_c_stream.py"
    assert "tail_config_hash=MODULE_C_CONFIG_HASH" in source.read_text(encoding="utf-8")


def test_closed_period_cutoffs_use_current_local_week_and_month_start() -> None:
    week_cutoff, month_cutoff = closed_period_cutoffs_utc(
        datetime(2026, 7, 7, 3, 45, tzinfo=timezone.utc)
    )

    assert week_cutoff == datetime(2026, 7, 5, 16, 0, tzinfo=timezone.utc)
    assert month_cutoff == datetime(2026, 6, 30, 16, 0, tzinfo=timezone.utc)
