from datetime import UTC, datetime, timedelta

import pytest

from app.engine.lifecycle_datasets import (
    OfficialLifecycleUnavailable,
    build_lifecycle_datasets,
    require_official_dataset,
)
from app.repositories.lifecycle_repo import CURRENT_SQL, EVENTS_SQL


AS_OF = datetime(2026, 7, 13, 1, tzinfo=UTC)


def event(
    event_id: int,
    profile: str,
    event_type: str = "first_seen",
    *,
    effective_time=AS_OF,
    observed_time=None,
):
    if observed_time is None:
        observed_time = effective_time
    return {
        "id": event_id,
        "fingerprint": f"fp-{event_id}",
        "event_type": event_type,
        "effective_time": effective_time,
        "observed_time": observed_time,
        "point_time": AS_OF - timedelta(days=1),
        "current_mode": "predictive",
        "run_id": event_id,
        "provenance": {"publication_profile": profile},
        "symbol_id": 1,
        "chan_level": 30,
        "publication_profile": profile,
        "structure_type": "signal",
    }


def test_profiles_are_physically_separated_and_baseline_never_fakes_first_seen() -> None:
    payload = build_lifecycle_datasets(
        events=[
            event(1, "historical_replay"),
            event(2, "online"),
            event(3, "baseline", "baseline_observed"),
        ],
        current=[],
        as_of_time=AS_OF,
    )
    assert payload["decision"] == "GO"
    assert [row["event_id"] for row in payload["datasets"]["official"]] == [1]
    assert [row["event_id"] for row in payload["datasets"]["observable"]] == [2]
    assert [row["event_id"] for row in payload["datasets"]["diagnostic"]] == [3]
    assert payload["datasets"]["diagnostic"][0]["first_seen_time"] is None
    assert payload["datasets"]["official"][0]["observed_time"] == AS_OF.isoformat()


def test_official_history_fails_closed_without_historical_replay() -> None:
    payload = build_lifecycle_datasets(
        events=[event(2, "online"), event(3, "baseline", "baseline_observed")],
        current=[],
        as_of_time=AS_OF,
    )
    assert payload["decision"] == "NO_GO"
    assert payload["datasets"]["official"] == []
    with pytest.raises(OfficialLifecycleUnavailable):
        require_official_dataset(payload)


def test_future_events_are_rejected_instead_of_leaking_into_as_of_dataset() -> None:
    payload = build_lifecycle_datasets(
        events=[event(1, "historical_replay", effective_time=AS_OF + timedelta(seconds=1))],
        current=[],
        as_of_time=AS_OF,
    )
    assert payload["future_rows_rejected"] == 1
    assert payload["datasets"]["official"] == []
    assert payload["decision"] == "NO_GO"


def test_future_observations_are_rejected_even_when_effective_time_is_causal() -> None:
    payload = build_lifecycle_datasets(
        events=[
            event(
                1,
                "historical_replay",
                effective_time=AS_OF - timedelta(days=1),
                observed_time=AS_OF + timedelta(seconds=1),
            )
        ],
        current=[],
        as_of_time=AS_OF,
    )
    assert payload["future_rows_rejected"] == 1
    assert payload["datasets"]["official"] == []
    assert payload["decision"] == "NO_GO"


def test_naive_as_of_is_rejected() -> None:
    with pytest.raises(ValueError):
        build_lifecycle_datasets(events=[], current=[], as_of_time=datetime(2026, 7, 13))


def test_repository_contract_is_lifecycle_only() -> None:
    sql = (EVENTS_SQL + CURRENT_SQL).lower()
    assert "chan_structure_lifecycle_events" in sql
    assert "chan_structure_lifecycle_current" in sql
    assert "chan_c_runs" not in sql
    assert "effective_time <= $1" in sql
    assert "e.observed_time" in sql
    assert "observed_time <= $1" in sql
