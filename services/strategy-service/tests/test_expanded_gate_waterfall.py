from app.engine.expanded_gate_waterfall import build_gate_waterfall


def test_waterfall_does_not_default_unknown_dif_to_pass():
    row = build_gate_waterfall([{"symbol": "x", "episode_id": "e", "weekly_b1": True, "weekly_b2": True, "weekly_price_relation_valid": True, "weekly_dif": None}])["rows"][0]
    assert row["weekly_dif_gt_zero"] is None
    assert row["gate_status"]["weekly_dif_gt_zero"] == "blocked_unreconstructable"
    assert row["blocker"] == "weekly_dif_not_reconstructable"
