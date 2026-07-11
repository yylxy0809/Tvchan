from app.engine.strategy_episode_builder import build_daily_setup_episodes, build_weekly_context_episodes


def test_observations_fold_to_independent_episode_identities():
    weekly = build_weekly_context_episodes([
        {"symbol": "000001.SZ", "weekly_signal_fingerprint": "w", "weekly_context_first_seen_time": "2025-01-01T00:00:00+00:00"},
        {"symbol": "000001.SZ", "weekly_signal_fingerprint": "w", "weekly_context_first_seen_time": "2025-01-01T00:00:00+00:00"},
    ])
    assert len(weekly) == 1
    rows = [
        {"weekly_context_episode_id": weekly[0]["episode_id"], "daily_setup_signal_fingerprint": "d", "as_of_time": "2025-01-02T00:00:00+00:00"},
        {"weekly_context_episode_id": weekly[0]["episode_id"], "daily_setup_signal_fingerprint": "d", "as_of_time": "2025-01-03T00:00:00+00:00"},
        {"weekly_context_episode_id": weekly[0]["episode_id"], "daily_setup_signal_fingerprint": "d2", "as_of_time": "2025-01-03T00:00:00+00:00"},
    ]
    assert len(build_daily_setup_episodes(rows)) == 2


def test_weekly_context_is_part_of_daily_identity():
    rows = [
        {"weekly_context_episode_id": "w1", "daily_setup_signal_fingerprint": "d"},
        {"weekly_context_episode_id": "w2", "daily_setup_signal_fingerprint": "d"},
    ]
    assert len(build_daily_setup_episodes(rows)) == 2
