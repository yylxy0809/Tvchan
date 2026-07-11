from app.engine.intraday_cutoff_grid import build_expected_intraday_cutoff_grid
import pytest


def test_grid_uses_complete_unique_klines_and_inclusive_end():
    episodes = [{"episode_id": "e", "symbol": "000001.SZ", "daily_setup_first_seen_time": "2025-01-01T09:30:00+00:00", "trigger_window_end": "2025-01-01T10:00:00+00:00"}]
    klines = [
        {"symbol": "000001.SZ", "timeframe": 30, "ts": "2025-01-01T09:30:00+00:00", "is_complete": True},
        {"symbol": "000001.SZ", "timeframe": 30, "ts": "2025-01-01T10:00:00+00:00", "is_complete": True},
        {"symbol": "000001.SZ", "timeframe": 30, "ts": "2025-01-01T10:00:00+00:00", "is_complete": True},
        {"symbol": "000001.SZ", "timeframe": 5, "ts": "2025-01-01T09:35:00+00:00", "is_complete": False},
    ]
    grid = build_expected_intraday_cutoff_grid(episodes, klines)
    assert [row["cutoff_bar_end"] for row in grid] == ["2025-01-01T09:30:00+00:00", "2025-01-01T10:00:00+00:00"]
    assert {row["level"] for row in grid} == {"30f"}


def test_weekend_or_suspension_without_complete_klines_creates_no_expected_cutoff():
    episode = [{"episode_id": "e", "symbol": "x", "daily_setup_first_seen_time": "2025-01-04T00:00:00+00:00", "trigger_window_end": "2025-01-05T23:00:00+00:00"}]
    assert build_expected_intraday_cutoff_grid(episode, []) == []


def test_grid_rejects_naive_times_and_normalizes_equal_offset_instants():
    naive = [{"episode_id": "e", "symbol": "x", "daily_setup_first_seen_time": "2025-01-01T09:30:00", "trigger_window_end": "2025-01-01T10:00:00+00:00"}]
    with pytest.raises(ValueError, match="Naive"):
        build_expected_intraday_cutoff_grid(naive, [])
    episode = [{"episode_id": "e", "symbol": "x", "daily_setup_first_seen_time": "2025-01-01T08:00:00+00:00", "trigger_window_end": "2025-01-01T09:00:00+00:00"}]
    grid = build_expected_intraday_cutoff_grid(episode, [{"symbol": "x", "timeframe": 30, "ts": "2025-01-01T17:00:00+08:00", "is_complete": True}])
    assert grid[0]["cutoff_bar_end"] == "2025-01-01T09:00:00+00:00"
