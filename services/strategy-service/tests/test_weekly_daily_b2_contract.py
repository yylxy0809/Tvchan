from app.contracts.weekly_daily_b2_contract import can_trigger, official_contract, score


def test_official_confidence_weights_and_30f_gate():
    assert score(thirty_f=True, bottom=False, five_f=False) == 40
    assert score(thirty_f=False, bottom=True, five_f=True) == 60
    assert score(thirty_f=True, bottom=True, five_f=False) == 70
    assert score(thirty_f=True, bottom=False, five_f=True) == 70
    assert can_trigger(score=100, fresh_thirty_f=False) is False


def test_official_does_not_accept_1p():
    contract = official_contract()
    assert contract["entry"]["require_30f_b1"] is True
    assert "1p" not in contract["entry"]["accepted_30f_types"]
