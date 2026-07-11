from app.engine.post_daily_refresh_visibility_v2 import audit_post_daily_refresh_visibility_v2


def test_refresh_requires_first_seen_between_setup_and_as_of():
    episodes = [{"episode_id": "e", "daily_setup_first_seen_time": "2025-01-01T00:00:00+00:00", "as_of_time": "2025-01-03T00:00:00+00:00"}]
    ledger = [{"fingerprint": "f", "first_seen_time": "2025-01-04T00:00:00+00:00", "signal_point_time": "2025-01-02T00:00:00+00:00"}]
    row = audit_post_daily_refresh_visibility_v2(episodes, ledger)["rows"][0]
    assert row["historically_visible"] is False
    assert row["reason"] == "first_seen_after_as_of"


def test_duplicate_refresh_only_counts_once_per_episode():
    episodes = [{"episode_id": "e", "daily_setup_first_seen_time": "2025-01-01T00:00:00+00:00", "as_of_time": "2025-01-03T00:00:00+00:00"}]
    ledger = [{"fingerprint": "f", "first_seen_time": "2025-01-02T00:00:00+00:00", "signal_point_time": "2025-01-02T00:00:00+00:00"}]
    assert len(audit_post_daily_refresh_visibility_v2(episodes, ledger)["rows"]) == 1
