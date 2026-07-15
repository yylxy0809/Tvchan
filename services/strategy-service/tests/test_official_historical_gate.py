from app.engine.official_historical_gate import build_official_historical_gate


def test_gate_fails_closed_when_predictive_weekly_b2_is_unavailable():
    report = build_official_historical_gate({
        "as_of_time": "2026-07-03T07:00:00+00:00",
        "counts": {
            "source_high_level_eligible": 5531,
            "official_high_level_visible": 5531,
            "intraday_eligible": 61,
            "predictive_weekly_b1": 4,
            "predictive_weekly_b2": 0,
            "strict_daily_episodes": 0,
            "official_30f_confirmations": 0,
            "official_5f_confirmations": 0,
            "official_candidates": 0,
        },
        "official_events_by_level": [],
    })

    assert report["decision"] == "NO_GO"
    assert report["gate_counts_monotonic"] is True
    assert "official_predictive_weekly_b2_unavailable" in report["blockers"]
