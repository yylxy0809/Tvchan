import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from collector.lifecycle_observer import (
    ACK_OUTBOX_SQL,
    CLAIM_OUTBOX_SQL,
    REBUILD_CURRENT_PROJECTION_SQL,
    RENEW_OUTBOX_SQL,
    LifecycleObserver,
    LostLifecycleLease,
    Observation,
    plan_events,
)


def observation(mode: str = "predictive", *, hour: int = 1) -> Observation:
    return Observation(
        symbol_id=1,
        chan_level=5,
        structure_type="signal",
        side_or_direction="buy",
        bsp_type="1",
        point_time=datetime(2026, 7, 1, hour, tzinfo=UTC),
        price_x1000=12345,
        config_hash="cfg",
        mode=mode,
    )


def test_baseline_only_emits_baseline_observed() -> None:
    events = plan_events(profile="baseline", previous=[], current=[observation()])
    assert [event.event_type for event in events] == ["baseline_observed"]
    assert plan_events(profile="baseline", previous=[observation()], current=[]) == []


def test_online_transition_sequence() -> None:
    predictive = observation()
    confirmed = observation("confirmed")
    assert predictive.fingerprint == confirmed.fingerprint
    assert [e.event_type for e in plan_events(profile="online", previous=[], current=[predictive])] == ["first_seen"]
    assert [e.event_type for e in plan_events(profile="online", previous=[predictive], current=[confirmed])] == ["confirmed"]
    assert [e.event_type for e in plan_events(profile="online", previous=[confirmed], current=[])] == ["disappeared"]


def test_reappeared_is_classified_when_projection_says_disappeared() -> None:
    from collector.lifecycle import LifecycleState

    item = observation()
    events = plan_events(
        profile="online",
        previous=[],
        current=[item],
        states={item.fingerprint: LifecycleState("disappeared", "predictive")},
    )
    assert [event.event_type for event in events] == ["reappeared"]


def test_identity_normalizes_offsets_and_rejects_naive_time() -> None:
    utc = observation()
    offset = Observation(**{**utc.__dict__, "point_time": utc.point_time + timedelta(hours=8)})
    offset = Observation(**{**offset.__dict__, "point_time": offset.point_time.replace(tzinfo=UTC)})
    # Same instant expressed in +08:00.
    from datetime import timezone

    offset = Observation(**{**utc.__dict__, "point_time": datetime(2026, 7, 1, 9, tzinfo=timezone(timedelta(hours=8)))})
    assert utc.fingerprint == offset.fingerprint
    with pytest.raises(ValueError):
        Observation(**{**utc.__dict__, "point_time": datetime(2026, 7, 1, 1)}).fingerprint


def test_sql_contract_has_skip_locked_fencing_and_event_rebuild() -> None:
    assert "for update skip locked" in CLAIM_OUTBOX_SQL.lower()
    assert "where status <> 'completed'" in CLAIM_OUTBOX_SQL
    assert "join due on due.id = outbox.id" in CLAIM_OUTBOX_SQL
    assert "lease_version = outbox.lease_version + 1" in CLAIM_OUTBOX_SQL
    assert "lease_token = $2" in ACK_OUTBOX_SQL
    assert "lease_version = $3" in ACK_OUTBOX_SQL
    assert "lease_token = $2" in RENEW_OUTBOX_SQL
    assert "truncate chan_structure_lifecycle_current" in REBUILD_CURRENT_PROJECTION_SQL.lower()
    assert "chan_structure_lifecycle_events" in REBUILD_CURRENT_PROJECTION_SQL
    assert "event_type = 'first_seen'" in REBUILD_CURRENT_PROJECTION_SQL
    assert "in ('baseline_observed', 'first_seen')" not in REBUILD_CURRENT_PROJECTION_SQL


def test_ack_is_guarded_and_watermark_only_moves_after_success() -> None:
    class Conn:
        def __init__(self, accepted: bool):
            self.accepted = accepted
            self.executed = []

        async def fetchrow(self, sql, *args):
            self.ack = (sql, args)
            return {"id": args[0]} if self.accepted else None

        async def execute(self, sql, *args):
            self.executed.append((sql, args))

    claimed = {"id": 7, "lease_token": "token", "lease_version": 3}
    async def exercise() -> None:
        rejected = Conn(False)
        assert not await LifecycleObserver().acknowledge(rejected, claimed)
        assert rejected.executed == []

        accepted = Conn(True)
        assert await LifecycleObserver().acknowledge(accepted, claimed)
        assert accepted.ack[1] == (7, "token", 3)
        assert accepted.executed[0][1] == ("chan-lifecycle-v1",)

    asyncio.run(exercise())


def test_lost_lease_aborts_atomic_persist() -> None:
    class Tx:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            self.exc_type = exc_type

    class Observer(LifecycleObserver):
        async def append_events(self, *_args, **_kwargs):
            return None
        async def acknowledge(self, *_args, **_kwargs):
            return False

    class Conn:
        def __init__(self):
            self.tx = Tx()
        def transaction(self):
            return self.tx
        async def execute(self, *_args):
            return None

    async def exercise() -> None:
        conn = Conn()
        with pytest.raises(LostLifecycleLease):
            await Observer().persist_and_acknowledge(
                conn, claimed={"id": 9}, events=[], effective_time=datetime.now(UTC),
                run_id=1, current_mode="confirmed",
            )
        assert conn.tx.exc_type is LostLifecycleLease

    asyncio.run(exercise())
