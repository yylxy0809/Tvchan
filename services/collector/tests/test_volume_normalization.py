from decimal import Decimal

from collector.volume_normalization import CompleteIntradayAggregate, decide_volume_multiplier


def test_amount_implied_price_selects_share_multiplier() -> None:
    decision = decide_volume_multiplier(raw_volume=100, amount=1_000, low=9.9, high=10.1)

    assert decision.action == "accept"
    assert decision.multiplier == 1
    assert decision.volume_shares == Decimal("100")
    assert decision.basis == "amount_implied_price"


def test_amount_implied_price_selects_hundred_share_multiplier() -> None:
    decision = decide_volume_multiplier(raw_volume=100, amount=100_000, low=9.9, high=10.1)

    assert decision.action == "accept"
    assert decision.multiplier == 100
    assert decision.volume_shares == Decimal("10000")


def test_price_rounding_tolerance_is_small_and_documented() -> None:
    # 10.105 is just above high=10.10 but inside the default 0.1% allowance.
    decision = decide_volume_multiplier(raw_volume=100, amount=1_010.5, low=9.9, high=10.1)

    assert decision.action == "accept"
    assert decision.multiplier == 1


def test_no_price_candidate_can_use_complete_intraday_aggregate() -> None:
    evidence = CompleteIntradayAggregate(
        volume_shares=10_000, complete=True, source_name="parquet_30f", source_ref="stock_30min/000001.SZ.parquet",
    )
    decision = decide_volume_multiplier(raw_volume=100, amount=None, low=9.9, high=10.1, aggregate=evidence)

    assert decision.action == "accept"
    assert decision.multiplier == 100
    assert decision.basis == "complete_intraday_aggregate"
    assert decision.aggregate_checked is True
    assert decision.provenance(raw_volume=100) == {
        "raw_volume": "100",
        "selected_multiplier": 100,
        "normalized_volume_shares": "10000",
        "volume_decision_basis": "complete_intraday_aggregate",
        "volume_decision_reason": None,
        "volume_decision_detail": None,
        "implied_prices": {},
        "aggregate_checked": True,
        "aggregate_source_name": "parquet_30f",
        "aggregate_source_ref": "stock_30min/000001.SZ.parquet",
    }


def test_ambiguous_price_envelope_uses_complete_aggregate_as_tiebreaker() -> None:
    # Both 1 and 100 produce prices within this intentionally broad envelope.
    evidence = CompleteIntradayAggregate(volume_shares=100, complete=True, source_name="parquet_5f")
    decision = decide_volume_multiplier(raw_volume=100, amount=10_000, low=0, high=101, aggregate=evidence)

    assert decision.action == "accept"
    assert decision.multiplier == 1
    assert decision.basis == "complete_intraday_aggregate"


def test_incomplete_or_nonunique_evidence_quarantines() -> None:
    incomplete = CompleteIntradayAggregate(volume_shares=10_000, complete=False, source_name="parquet_30f")
    decision = decide_volume_multiplier(raw_volume=100, amount=None, low=9.9, high=10.1, aggregate=incomplete)

    assert decision.action == "quarantine"
    assert decision.reason == "ambiguous_volume_unit"
    assert decision.detail == "missing_amount_and_no_unique_complete_intraday_aggregate"
    assert decision.multiplier is None
    assert decision.volume_shares is None


def test_invalid_zero_volume_is_quarantined_even_when_amount_is_zero() -> None:
    decision = decide_volume_multiplier(raw_volume=0, amount=0, low=9.9, high=10.1)

    assert decision.action == "quarantine"
    assert decision.reason == "ambiguous_volume_unit"
    assert decision.detail == "invalid_volume_or_price_envelope"
