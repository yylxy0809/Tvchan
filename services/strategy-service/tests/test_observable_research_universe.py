from app.engine.observable_research_universe import build_observable_research_universe, build_reconstructed_episodes
import pytest


def test_universe_requires_all_levels_and_does_not_count_observations_as_episodes():
    runs = [{"symbol": "x", "level": level, "mode": "predictive", "cutoff_bar_end": "2025-01-01T00:00:00+00:00"} for level in ("1w", "1d", "30f", "5f")]
    result = build_observable_research_universe(runs)
    assert result["observable_symbols"] == ["x"]


def test_universe_records_kline_and_market_cap_diagnostic_status():
    runs = [{"symbol": "x", "level": level, "mode": "predictive", "cutoff_bar_end": "2025-01-01T00:00:00+00:00"} for level in ("1w", "1d", "30f", "5f")]
    klines = [{"symbol": "x", "timeframe": tf, "ts": "2025-01-01T00:00:00+00:00", "is_complete": True} for tf in (10080, 1440, 30, 5)]
    result = build_observable_research_universe(runs, klines=klines, market_cap_by_symbol={"x": None})
    assert result["kline_location_verified"] is True
    assert result["market_cap_status"]["x"] == "missing_diagnostic_only"


def test_episode_rebuild_does_not_link_a_future_weekly_context_to_daily_setup():
    universe = {"observable_symbols": ["x"]}
    lifecycle = [
        {"identity": "wb1", "symbol": "x", "level": "1w", "mode": "predictive", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-01-03T00:00:00+00:00", "point_time": "2025-01-03T00:00:00+00:00", "price_x1000": 10, "first_seen_run_id": 1},
        {"identity": "wb2", "symbol": "x", "level": "1w", "mode": "predictive", "side": "buy", "bsp_type": "2", "first_seen_time": "2025-01-04T00:00:00+00:00", "point_time": "2025-01-04T00:00:00+00:00", "price_x1000": 20, "first_seen_run_id": 2},
        {"identity": "db1", "symbol": "x", "level": "1d", "mode": "predictive", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-01-01T00:00:00+00:00", "point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 10, "first_seen_run_id": 3},
        {"identity": "db2", "symbol": "x", "level": "1d", "mode": "predictive", "side": "buy", "bsp_type": "2", "first_seen_time": "2025-01-02T00:00:00+00:00", "point_time": "2025-01-02T00:00:00+00:00", "price_x1000": 20, "first_seen_run_id": 4},
    ]
    weekly, daily = build_reconstructed_episodes(universe=universe, lifecycle=lifecycle, structure_runs=[], klines=[])
    assert len(weekly) == 1
    assert daily == []


def test_observable_universe_rejects_naive_cutoffs():
    runs = [{"symbol": "x", "level": level, "mode": "predictive", "cutoff_bar_end": "2025-01-01T00:00:00"} for level in ("1w", "1d", "30f", "5f")]
    with pytest.raises(ValueError, match="Naive"):
        build_observable_research_universe(runs)


def test_observable_universe_rejects_any_naive_kline_and_normalizes_offsets():
    runs = [{"symbol": "x", "level": level, "mode": "predictive", "cutoff_bar_end": "2025-01-01T00:00:00+00:00"} for level in ("1w", "1d", "30f", "5f")]
    klines = [{"symbol": "x", "timeframe": tf, "ts": "2025-01-01T08:00:00+08:00", "is_complete": True} for tf in (10080, 1440, 30, 5)]
    assert build_observable_research_universe(runs, klines=klines)["observable_symbols"] == ["x"]
    klines[2]["ts"] = "2025-01-01T00:00:00"
    with pytest.raises(ValueError, match="Naive"):
        build_observable_research_universe(runs, klines=klines)


def test_universe_exposes_explicit_official_observable_and_diagnostic_counts():
    runs = [{"symbol": "x", "level": level, "mode": "predictive", "cutoff_bar_end": "2025-01-01T00:00:00+00:00"} for level in ("1w", "1d", "30f", "5f")]
    result = build_observable_research_universe(runs, market_cap_by_symbol={"x": None})
    assert result["official_eligible_symbol_count"] == 0
    assert result["observable_symbol_count"] == 1
    assert result["diagnostic_symbol_count"] == 1
