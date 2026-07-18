from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timezone
from pathlib import Path

import collector.chan_c_stream as stream
from collector.chan_c_stream import (
    bars_through_claimed_period,
    group_jobs_by_claimed_target,
    group_jobs_by_level,
    group_jobs_by_mode,
)
from trading_protocol import Bar

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


def test_chan_c_stream_groups_jobs_by_frozen_claimed_target() -> None:
    first = datetime(2026, 7, 3, 6, 55, tzinfo=UTC)
    second = datetime(2026, 7, 3, 7, 0, tzinfo=UTC)

    grouped = group_jobs_by_claimed_target(
        [
            {"id": 1, "claimed_target_bar_end": second},
            {"id": 2, "claimed_target_bar_end": first},
            {"id": 3, "claimed_target_bar_end": second},
        ]
    )

    assert [job["id"] for job in grouped[first]] == [2]
    assert [job["id"] for job in grouped[second]] == [1, 3]


def test_chan_c_stream_clips_each_publication_to_its_claimed_target(monkeypatch) -> None:
    first = datetime(2026, 7, 3, 6, 55, tzinfo=UTC)
    second = datetime(2026, 7, 3, 7, 0, tzinfo=UTC)
    bars = [
        Bar("000001.SZ", "5f", first, 10, 11, 9, 10.5, 100),
        Bar("000001.SZ", "5f", second, 10, 11, 9, 10.5, 100),
    ]

    class KlineWriter:
        async def get_bars_chunk(self, *_args, **_kwargs):
            return bars

    class ChanWriter:
        def __init__(self):
            self.calls = []

        async def replace_incremental_analysis(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "run_id": len(self.calls),
                "snapshot_version": f"v{len(self.calls)}",
            }

    observed_bar_ends = []

    async def fake_compute(*, bars_by_level, **_kwargs):
        observed_bar_ends.append(bars_by_level["5f"][-1].ts)
        return {
            "snapshot_version": "computed",
            "strokes": [],
            "segments": [],
            "centers": [],
            "signals": [],
        }

    monkeypatch.setattr(stream, "compute_module_c_overlay", fake_compute)
    monkeypatch.setattr(stream, "validate_module_c_response", lambda **_kwargs: None)
    monkeypatch.setattr(
        stream,
        "filter_chan_response_level",
        lambda response, _level: response,
    )
    writer = ChanWriter()
    jobs = [
        {
            "id": 10,
            "chan_level": 5,
            "mode": "confirmed",
            "anchor_bar_end": first,
            "last_bar_end": first,
            "claimed_target_bar_end": first,
            "claim_token": "claim-a",
            "lease_version": 7,
        },
        {
            "id": 11,
            "chan_level": 5,
            "mode": "predictive",
            "anchor_bar_end": first,
            "last_bar_end": second,
            "claimed_target_bar_end": second,
            "claim_token": "claim-b",
            "lease_version": 8,
        },
    ]

    runs = asyncio.run(
        stream.process_symbol_tail(
            kline_writer=KlineWriter(),
            chan_writer=writer,
            symbol="000001.SZ",
            jobs=jobs,
            chan_py_path=None,
            tail_bar_limit=10,
            context_bars=0,
            redis_url=None,
        )
    )

    assert runs == 2
    assert observed_bar_ends == [first, second]
    assert [call["bar_until"] for call in writer.calls] == [first, second]
    assert [call["publication_task_id"] for call in writer.calls] == [10, 11]
    assert [call["publication_claim_token"] for call in writer.calls] == [
        "claim-a",
        "claim-b",
    ]
    assert [call["publication_lease_version"] for call in writer.calls] == [7, 8]


def test_weekly_claim_uses_canonical_last_bar_in_the_same_period() -> None:
    claimed_monday = datetime(2026, 7, 6, 7, 0, tzinfo=UTC)
    canonical_friday = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)
    next_friday = datetime(2026, 7, 17, 7, 0, tzinfo=UTC)
    bars = [
        Bar("000001.SZ", "1w", canonical_friday, 10, 11, 9, 10.5, 100),
        Bar("000001.SZ", "1w", next_friday, 10, 11, 9, 10.5, 100),
    ]

    selected, publication_bar_until = bars_through_claimed_period(
        level="1w",
        bars=bars,
        claimed_target=claimed_monday,
        symbol="000001.SZ",
    )

    assert [bar.ts for bar in selected] == [canonical_friday]
    assert publication_bar_until == canonical_friday


def test_weekly_process_accepts_canonical_bar_before_raw_period_label(
    monkeypatch,
) -> None:
    anchor = datetime(2026, 7, 3, 7, 0, tzinfo=UTC)
    claimed_friday = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)
    canonical_thursday = datetime(2026, 7, 9, 7, 0, tzinfo=UTC)
    bar = Bar("000001.SZ", "1w", canonical_thursday, 10, 11, 9, 10.5, 100)

    class KlineWriter:
        async def get_bars_chunk(self, *_args, **_kwargs):
            return [bar]

    class ChanWriter:
        def __init__(self):
            self.call = None

        async def replace_incremental_analysis(self, **kwargs):
            self.call = kwargs
            return {"run_id": 1, "snapshot_version": "v1"}

    async def fake_compute(**_kwargs):
        return {
            "snapshot_version": "computed",
            "strokes": [],
            "segments": [],
            "centers": [],
            "signals": [],
        }

    monkeypatch.setattr(stream, "compute_module_c_overlay", fake_compute)
    monkeypatch.setattr(stream, "validate_module_c_response", lambda **_kwargs: None)
    monkeypatch.setattr(
        stream,
        "filter_chan_response_level",
        lambda response, _level: response,
    )
    writer = ChanWriter()

    runs = asyncio.run(
        stream.process_symbol_tail(
            kline_writer=KlineWriter(),
            chan_writer=writer,
            symbol="000001.SZ",
            jobs=[
                {
                    "id": 12,
                    "chan_level": 10080,
                    "mode": "confirmed",
                    "anchor_bar_end": anchor,
                    "last_bar_end": claimed_friday,
                    "claimed_target_bar_end": claimed_friday,
                    "claim_token": "claim-week",
                    "lease_version": 9,
                }
            ],
            chan_py_path=None,
            tail_bar_limit=10,
            context_bars=0,
            redis_url=None,
        )
    )

    assert runs == 1
    assert writer.call["bar_until"] == canonical_thursday
    assert writer.call["publication_target_bar_end"] == claimed_friday


def test_later_tail_failure_does_not_reclassify_an_already_completed_task(
    monkeypatch,
) -> None:
    first = datetime(2026, 7, 3, 6, 55, tzinfo=UTC)
    second = datetime(2026, 7, 3, 7, 0, tzinfo=UTC)
    bars = [
        Bar("000001.SZ", "5f", first, 10, 11, 9, 10.5, 100),
        Bar("000001.SZ", "5f", second, 10, 11, 9, 10.5, 100),
    ]

    class KlineWriter:
        async def get_bars_chunk(self, *_args, **_kwargs):
            return bars

    class TaskStore:
        def __init__(self):
            self.states = {10: "running", 11: "running"}

        async def complete_tail_task(self, *, task_id, error=None, **_kwargs):
            if self.states[task_id] != "running":
                return False
            self.states[task_id] = "failed" if error else "success"
            return True

    class ChanWriter:
        def __init__(self, task_store):
            self.calls = 0
            self.task_store = task_store

        async def replace_incremental_analysis(self, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("second publication failed")
            self.task_store.states[kwargs["publication_task_id"]] = "success"
            return {"run_id": 1, "snapshot_version": "v1"}

    async def fake_compute(**_kwargs):
        return {
            "snapshot_version": "computed",
            "strokes": [],
            "segments": [],
            "centers": [],
            "signals": [],
        }

    monkeypatch.setattr(stream, "compute_module_c_overlay", fake_compute)
    monkeypatch.setattr(stream, "validate_module_c_response", lambda **_kwargs: None)
    monkeypatch.setattr(
        stream,
        "filter_chan_response_level",
        lambda response, _level: response,
    )
    task_store = TaskStore()
    jobs = [
        {
            "id": 10,
            "symbol": "000001.SZ",
            "chan_level": 5,
            "mode": "confirmed",
            "anchor_bar_end": first,
            "last_bar_end": first,
            "claimed_target_bar_end": first,
            "claim_token": "claim-a",
            "lease_version": 7,
        },
        {
            "id": 11,
            "symbol": "000001.SZ",
            "chan_level": 5,
            "mode": "predictive",
            "anchor_bar_end": first,
            "last_bar_end": second,
            "claimed_target_bar_end": second,
            "claim_token": "claim-b",
            "lease_version": 8,
        },
    ]

    runs = asyncio.run(
        stream.process_tail_tasks(
            kline_writer=KlineWriter(),
            chan_writer=ChanWriter(task_store),
            task_store=task_store,
            tasks=jobs,
            chan_py_path=None,
            tail_bar_limit=10,
            context_bars=0,
            concurrency=1,
            redis_url=None,
        )
    )

    assert runs == 0
    assert task_store.states == {10: "success", 11: "failed"}


def test_chan_c_stream_uses_frozen_module_c_semantic_hash_for_tail_runs() -> None:
    source = Path(__file__).resolve().parents[1] / "collector" / "chan_c_stream.py"
    assert "tail_config_hash=MODULE_C_CONFIG_HASH" in source.read_text(encoding="utf-8")


def test_closed_period_cutoffs_use_current_local_week_and_month_start() -> None:
    week_cutoff, month_cutoff = closed_period_cutoffs_utc(
        datetime(2026, 7, 7, 3, 45, tzinfo=timezone.utc)
    )

    assert week_cutoff == datetime(2026, 7, 5, 16, 0, tzinfo=timezone.utc)
    assert month_cutoff == datetime(2026, 6, 30, 16, 0, tzinfo=timezone.utc)
