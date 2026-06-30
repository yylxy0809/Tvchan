from __future__ import annotations

from datetime import UTC, datetime, timedelta

from collector.storage.chan_state import Center, StrokeLike, derive_level_state


BASE = datetime(2026, 6, 1, 10, 0, tzinfo=UTC)


def _dt(minutes: int) -> datetime:
    return BASE + timedelta(minutes=minutes)


def test_level_state_without_center_uses_latest_stroke_direction() -> None:
    state = derive_level_state(
        strokes=[
            StrokeLike(
                seq=1,
                direction=1,
                confirmed=True,
                begin_base_ts=_dt(0),
                end_base_ts=_dt(5),
            )
        ],
        segments=[],
        centers=[],
        signals=[],
        source_bar_until=_dt(5),
    )

    assert state["structure_state"] == "no_center"
    assert state["structure_direction"] == 1
    assert state["latest_stroke_direction"] == 1


def test_one_center_is_consolidation() -> None:
    state = derive_level_state(
        strokes=[],
        segments=[
            StrokeLike(
                seq=1,
                direction=-1,
                confirmed=True,
                begin_base_ts=_dt(0),
                end_base_ts=_dt(30),
            )
        ],
        centers=[
            Center(
                seq=1,
                low_x1000=10_000,
                high_x1000=11_000,
                confirmed=True,
                begin_base_ts=_dt(5),
                end_base_ts=_dt(20),
            )
        ],
        signals=[],
        source_bar_until=_dt(30),
    )

    assert state["structure_state"] == "consolidation"
    assert state["structure_direction"] == -1
    assert state["center_count"] == 1


def test_two_same_direction_centers_are_up_trend() -> None:
    state = derive_level_state(
        strokes=[],
        segments=[
            StrokeLike(
                seq=1,
                direction=1,
                confirmed=True,
                begin_base_ts=_dt(0),
                end_base_ts=_dt(60),
            )
        ],
        centers=[
            Center(1, 10_000, 11_000, True, _dt(5), _dt(20)),
            Center(2, 11_500, 12_000, True, _dt(30), _dt(50)),
        ],
        signals=[],
        source_bar_until=_dt(60),
    )

    assert state["structure_state"] == "trend"
    assert state["structure_direction"] == 1


def test_overlapping_centers_remain_consolidation() -> None:
    state = derive_level_state(
        strokes=[],
        segments=[
            StrokeLike(
                seq=1,
                direction=1,
                confirmed=True,
                begin_base_ts=_dt(0),
                end_base_ts=_dt(60),
            )
        ],
        centers=[
            Center(1, 10_000, 11_000, True, _dt(5), _dt(20)),
            Center(2, 10_500, 11_500, True, _dt(30), _dt(50)),
        ],
        signals=[],
        source_bar_until=_dt(60),
    )

    assert state["structure_state"] == "consolidation"
    assert state["structure_direction"] == 1
