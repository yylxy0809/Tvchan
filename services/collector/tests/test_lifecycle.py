from datetime import UTC, datetime, timezone, timedelta

import pytest

from collector.lifecycle import LifecycleState, structure_fingerprint, transition_event


def test_fingerprint_normalizes_equivalent_utc_offsets() -> None:
    base = datetime(2026, 7, 1, 1, 0, tzinfo=UTC)
    common = dict(
        symbol_id=1,
        chan_level=30,
        structure_type="signal",
        side_or_direction="buy",
        bsp_type="1",
        end_time=None,
        price_x1000=12345,
        start_price_x1000=None,
        end_price_x1000=None,
        low_x1000=None,
        high_x1000=None,
        config_hash="v1",
    )
    assert structure_fingerprint(point_time=base, **common) == structure_fingerprint(
        point_time=base.astimezone(timezone(timedelta(hours=8))), **common
    )


def test_fingerprint_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        structure_fingerprint(
            symbol_id=1, chan_level=30, structure_type="signal",
            side_or_direction="buy", bsp_type="1",
            point_time=datetime(2026, 7, 1, 1, 0), end_time=None,
            price_x1000=1, low_x1000=None, high_x1000=None, config_hash="v1",
            start_price_x1000=None, end_price_x1000=None,
        )


def test_predictive_line_endpoint_extension_keeps_identity() -> None:
    common = dict(
        symbol_id=1, chan_level=5, structure_type="stroke",
        side_or_direction="up", bsp_type=None,
        point_time=datetime(2026, 7, 1, 1, tzinfo=UTC),
        price_x1000=None, start_price_x1000=10000,
        low_x1000=None, high_x1000=None, config_hash="v1",
    )
    first = structure_fingerprint(
        end_time=datetime(2026, 7, 1, 2, tzinfo=UTC), end_price_x1000=11000, **common
    )
    extended = structure_fingerprint(
        end_time=datetime(2026, 7, 1, 3, tzinfo=UTC), end_price_x1000=12000, **common
    )
    assert first == extended


def test_extending_center_bounds_keeps_identity() -> None:
    common = dict(
        symbol_id=1, chan_level=30, structure_type="center",
        side_or_direction=None, bsp_type=None,
        point_time=datetime(2026, 7, 1, 1, tzinfo=UTC),
        end_time=None, price_x1000=None, start_price_x1000=None,
        end_price_x1000=None, config_hash="v1",
    )
    assert structure_fingerprint(low_x1000=10000, high_x1000=11000, **common) == structure_fingerprint(
        low_x1000=9900, high_x1000=11200, **common
    )


def test_transition_contract_preserves_baseline_and_lifecycle_states() -> None:
    assert transition_event(profile="baseline", previous=None, current_mode="confirmed") == "baseline_observed"
    assert transition_event(profile="online", previous=None, current_mode="predictive") == "first_seen"
    assert transition_event(profile="online", previous=LifecycleState("active", "predictive"), current_mode="confirmed") == "confirmed"
    assert transition_event(profile="online", previous=LifecycleState("active", "confirmed"), current_mode=None) == "disappeared"
    assert transition_event(profile="online", previous=LifecycleState("disappeared", "confirmed"), current_mode="confirmed") == "reappeared"
