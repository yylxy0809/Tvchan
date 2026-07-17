import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from collector.lifecycle_observer import (
    ACK_OUTBOX_SQL,
    CLAIM_OUTBOX_SQL,
    FAIL_OUTBOX_SQL,
    REBUILD_CURRENT_PROJECTION_SQL,
    RENEW_OUTBOX_SQL,
    UPSERT_WATERMARK_SQL,
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
    assert "where status in ('pending', 'processing', 'failed')" in CLAIM_OUTBOX_SQL
    assert "join due on due.id = outbox.id" in CLAIM_OUTBOX_SQL
    assert "lease_version = outbox.lease_version + 1" in CLAIM_OUTBOX_SQL
    assert "lease_token = $2" in ACK_OUTBOX_SQL
    assert "lease_version = $3" in ACK_OUTBOX_SQL
    assert "lease_token = $2" in RENEW_OUTBOX_SQL
    assert "status in ('pending', 'failed')" in CLAIM_OUTBOX_SQL
    assert "attempts >= $4" in FAIL_OUTBOX_SQL
    assert "lease_token = $2" in FAIL_OUTBOX_SQL
    assert "lease_version = $3" in FAIL_OUTBOX_SQL
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


def test_idle_pulse_refreshes_the_existing_durable_watermark_without_skipping_work() -> None:
    class Conn:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[object, ...]]] = []

        async def execute(self, sql: str, *args: object) -> None:
            self.executed.append((sql, args))

    async def exercise() -> None:
        conn = Conn()
        await LifecycleObserver(observer_name="canonical-observer").pulse(conn)
        assert conn.executed == [(UPSERT_WATERMARK_SQL, ("canonical-observer",))]

    asyncio.run(exercise())
    normalized = " ".join(UPSERT_WATERMARK_SQL.lower().split())
    assert "min(id) - 1 from chan_c_head_outbox where status <> 'completed'" in normalized
    assert "select max(id) from chan_c_head_outbox" in normalized


def test_failed_claim_is_fenced_and_can_be_dead_lettered() -> None:
    class Conn:
        async def fetchrow(self, sql, *args):
            self.sql = sql
            self.args = args
            return {"id": args[0], "status": "dead_letter"}

    async def exercise() -> None:
        conn = Conn()
        claimed = {"id": 11, "lease_token": "lease", "lease_version": 4}
        assert await LifecycleObserver().fail(
            conn, claimed, error="poison", max_attempts=4, retry_delay_seconds=7
        )
        assert conn.args == (11, "lease", 4, 4, 7, "poison")

    asyncio.run(exercise())


def test_process_next_records_retry_instead_of_losing_poison_message() -> None:
    class Observer(LifecycleObserver):
        async def claim(self, *_args, **_kwargs):
            return [{"id": 5, "lease_token": "token", "lease_version": 2}]

        async def process_claimed(self, *_args, **_kwargs):
            raise RuntimeError("poison")

        async def fail(self, *_args, **kwargs):
            self.failure = kwargs
            return True

    class Conn:
        async def fetchval(self, sql):
            return "try_advisory_lock" in sql or "advisory_unlock" in sql

    async def exercise() -> None:
        observer = Observer()
        assert await observer.process_next(
            Conn(), max_attempts=3, retry_delay_seconds=9
        ) == 1
        assert observer.failure == {
            "error": "poison", "max_attempts": 3, "retry_delay_seconds": 9
        }

    asyncio.run(exercise())


def test_migration_036_adds_retry_and_dead_letter_contract() -> None:
    migration = (
        Path(__file__).resolve().parents[3] / "db" / "sql" / "036_lifecycle_outbox_retry.sql"
    ).read_text(encoding="utf-8").lower()
    for column in ("next_attempt_at", "last_error", "failed_at", "dead_lettered_at"):
        assert f"add column if not exists {column}" in migration
    assert "dead_letter" in migration


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


class CapturingProcessObserver(LifecycleObserver):
    def __init__(self, observations_by_run: dict[int, list[Observation]]) -> None:
        super().__init__()
        self.observations_by_run = observations_by_run
        self.persisted: dict[str, object] | None = None

    async def renew(self, *_args, **_kwargs) -> bool:
        return True

    async def load_observations(self, _conn, *, run_id: int | None, **_kwargs):
        return list(self.observations_by_run.get(int(run_id or 0), []))

    async def persist_and_acknowledge(self, _conn, **kwargs) -> None:
        self.persisted = {**kwargs, "events": list(kwargs["events"])}


class ProcessClaimedConnection:
    def __init__(
        self, *, profile: str, published_at: datetime, cutoff: datetime | None,
        visible_heads: list[dict[str, object]] | None = None,
        reject_live_heads: bool = False,
        run_config_hash: str = "cfg",
    ) -> None:
        self.profile = profile
        self.published_at = published_at
        self.cutoff = cutoff
        self.visible_heads = visible_heads or []
        self.reject_live_heads = reject_live_heads
        self.live_head_queries = 0
        self.run_config_hash = run_config_hash

    def claimed(self, *, outbox_id: int, head_history_id: int, **overrides):
        payload = {
            "id": head_history_id,
            "symbol_id": 1,
            "chan_level": 5,
            "mode": "predictive",
            "base_timeframe": 5,
            "config_hash": "cfg",
            "publication_profile": self.profile,
            "run_group_id": "run-group",
            "old_run_id": 1,
            "new_run_id": 2,
            "snapshot_version": "snapshot-2",
        }
        payload.update(overrides)
        return {"id": outbox_id, "head_history_id": head_history_id, "payload": payload}

    async def fetchrow(self, sql: str, *args):
        assert "chan_c_head_history" in sql
        return {
            "id": args[0],
            "symbol_id": 1,
            "chan_level": 5,
            "mode": "predictive",
            "base_timeframe": 5,
            "config_hash": "cfg",
            "old_run_id": 1,
            "new_run_id": 2,
            "publication_profile": self.profile,
            "run_group_id": "run-group",
            "snapshot_version": "snapshot-2",
            "published_at": self.published_at,
        }

    async def fetchval(self, sql: str, *_args):
        if "select config_hash" in sql:
            return self.run_config_hash
        if "select cutoff_time" in sql:
            return self.cutoff
        raise AssertionError(sql)

    async def fetch(self, sql: str, *_args):
        if "chan_structure_lifecycle_current" in sql:
            return []
        if "scheme2_chan_c_published_heads" in sql:
            self.live_head_queries += 1
            if self.reject_live_heads:
                raise AssertionError("historical replay must not query current published heads")
            return self.visible_heads
        raise AssertionError(sql)


def test_historical_disappearance_uses_only_causal_replay_heads() -> None:
    published_at = datetime(2026, 7, 17, 1, tzinfo=UTC)
    cutoff = datetime(2026, 7, 3, 7, tzinfo=UTC)
    item = observation()
    observer = CapturingProcessObserver({1: [item], 2: []})
    connection = ProcessClaimedConnection(
        profile="historical_replay",
        published_at=published_at,
        cutoff=cutoff,
        reject_live_heads=True,
    )

    count = asyncio.run(observer.process_claimed(
        connection,
        connection.claimed(outbox_id=7, head_history_id=11),
    ))

    assert count == 1
    assert connection.live_head_queries == 0
    assert observer.persisted is not None
    assert [event.event_type for event in observer.persisted["events"]] == ["disappeared"]
    assert observer.persisted["effective_time"] == cutoff
    assert observer.persisted["observed_time"] == published_at


def test_online_disappearance_is_suppressed_when_another_current_mode_still_exposes_it() -> None:
    published_at = datetime(2026, 7, 17, 1, tzinfo=UTC)
    predictive = observation()
    confirmed = Observation(**{**predictive.__dict__, "mode": "confirmed"})
    observer = CapturingProcessObserver({1: [predictive], 2: [], 3: [confirmed]})
    connection = ProcessClaimedConnection(
        profile="online",
        published_at=published_at,
        cutoff=None,
        visible_heads=[{"run_id": 3, "mode": "confirmed", "config_hash": "cfg"}],
    )

    count = asyncio.run(observer.process_claimed(
        connection,
        connection.claimed(outbox_id=8, head_history_id=12),
    ))

    assert count == 0
    assert connection.live_head_queries == 1
    assert observer.persisted is not None
    assert observer.persisted["events"] == []
    assert observer.persisted["effective_time"] == published_at
    assert observer.persisted["observed_time"] == published_at


@pytest.mark.parametrize(
    ("history_profile", "payload_profile"),
    [("online", "historical_replay"), ("historical_replay", "online")],
)
def test_profile_mismatch_fails_before_lifecycle_persistence(
    history_profile: str, payload_profile: str,
) -> None:
    published_at = datetime(2026, 7, 17, 1, tzinfo=UTC)
    observer = CapturingProcessObserver({1: [observation()], 2: []})
    connection = ProcessClaimedConnection(
        profile=history_profile,
        published_at=published_at,
        cutoff=published_at if history_profile == "historical_replay" else None,
    )

    with pytest.raises(RuntimeError, match="publication_profile"):
        asyncio.run(observer.process_claimed(
            connection,
            connection.claimed(
                outbox_id=9, head_history_id=13,
                publication_profile=payload_profile,
            ),
        ))

    assert observer.persisted is None


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("config_hash", "other-cfg"), ("old_run_id", 101), ("new_run_id", 202),
        ("symbol_id", 99), ("chan_level", 30), ("mode", "confirmed"),
        ("base_timeframe", 30), ("run_group_id", "other-group"),
        ("snapshot_version", "other-snapshot"),
    ],
)
def test_payload_identity_mismatch_fails_before_lifecycle_persistence(
    field: str, wrong_value,
) -> None:
    published_at = datetime(2026, 7, 17, 1, tzinfo=UTC)
    observer = CapturingProcessObserver({})
    connection = ProcessClaimedConnection(
        profile="online", published_at=published_at, cutoff=None,
    )

    with pytest.raises(RuntimeError, match=field):
        asyncio.run(observer.process_claimed(
            connection,
            connection.claimed(
                outbox_id=10, head_history_id=14, **{field: wrong_value},
            ),
        ))

    assert observer.persisted is None


def test_history_config_must_match_new_run_before_lifecycle_persistence() -> None:
    published_at = datetime(2026, 7, 17, 1, tzinfo=UTC)
    observer = CapturingProcessObserver({})
    connection = ProcessClaimedConnection(
        profile="online", published_at=published_at, cutoff=None,
        run_config_hash="different-run-config",
    )

    with pytest.raises(RuntimeError, match="head-history/run config mismatch"):
        asyncio.run(observer.process_claimed(
            connection, connection.claimed(outbox_id=11, head_history_id=15),
        ))

    assert observer.persisted is None


@pytest.mark.parametrize("payload", [None, [], "secret-not-json"])
def test_malformed_payload_fails_before_lifecycle_persistence(payload) -> None:
    published_at = datetime(2026, 7, 17, 1, tzinfo=UTC)
    observer = CapturingProcessObserver({})
    connection = ProcessClaimedConnection(
        profile="online", published_at=published_at, cutoff=None,
    )
    claimed = connection.claimed(outbox_id=10, head_history_id=14)
    claimed["payload"] = payload

    with pytest.raises(RuntimeError) as error:
        asyncio.run(observer.process_claimed(connection, claimed))

    assert "secret-not-json" not in str(error.value)
    assert observer.persisted is None


def test_migration_039_separates_observed_and_effective_time() -> None:
    migration = (
        Path(__file__).resolve().parents[3] / "db" / "sql" / "039_lifecycle_event_observed_time.sql"
    ).read_text(encoding="utf-8").lower()
    assert "add column if not exists observed_time" in migration
    assert "effective_time <= observed_time" in migration
