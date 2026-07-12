"""Deterministic, source-agnostic volume-unit decisions for native imports.

``klines.volume`` is always shares.  Native Parquet inputs may instead carry
either shares or hundred-shares.  This module deliberately does not write to
PostgreSQL or read Parquet: importers supply one row and, optionally, a
complete independently-derived intraday daily aggregate.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal


MULTIPLIERS: tuple[int, int] = (1, 100)
"""The only source units accepted by the historical import contract."""

# Prices in the source are normally quoted to cents or finer.  The 0.1%
# relative allowance plus one milli-yuan absolute floor only absorbs source
# rounding; it is far too small to make 1 and 100 interchangeable normally.
DEFAULT_PRICE_RELATIVE_TOLERANCE = Decimal("0.001")
DEFAULT_PRICE_ABSOLUTE_TOLERANCE = Decimal("0.001")
DEFAULT_VOLUME_RELATIVE_TOLERANCE = Decimal("0.001")


@dataclass(frozen=True)
class CompleteIntradayAggregate:
    """Independent, complete intraday evidence for one symbol trading day.

    ``volume_shares`` must already be in canonical shares.  The calling
    importer is responsible for only supplying this object when all expected
    native intraday bars have been accepted for that trading day.
    """

    volume_shares: Decimal | int | float | str
    complete: bool
    source_name: str
    source_ref: str | None = None


@dataclass(frozen=True)
class VolumeDecision:
    """Result suitable for canonical insertion or import quarantine."""

    action: Literal["accept", "quarantine"]
    multiplier: int | None
    volume_shares: Decimal | None
    basis: Literal["amount_implied_price", "complete_intraday_aggregate", "ambiguous"]
    reason: str | None
    detail: str | None
    implied_prices: tuple[tuple[int, Decimal], ...]
    aggregate_checked: bool
    aggregate_source_name: str | None = None
    aggregate_source_ref: str | None = None

    def provenance(self, *, raw_volume: Decimal | int | float | str) -> dict[str, Any]:
        """Return JSON-compatible fields for importer audit/provenance storage."""

        return {
            "raw_volume": str(raw_volume),
            "selected_multiplier": self.multiplier,
            "normalized_volume_shares": None if self.volume_shares is None else str(self.volume_shares),
            "volume_decision_basis": self.basis,
            "volume_decision_reason": self.reason,
            "volume_decision_detail": self.detail,
            "implied_prices": {str(multiplier): str(price) for multiplier, price in self.implied_prices},
            "aggregate_checked": self.aggregate_checked,
            "aggregate_source_name": self.aggregate_source_name,
            "aggregate_source_ref": self.aggregate_source_ref,
        }


def decide_volume_multiplier(
    *,
    raw_volume: Decimal | int | float | str,
    amount: Decimal | int | float | str | None,
    low: Decimal | int | float | str,
    high: Decimal | int | float | str,
    aggregate: CompleteIntradayAggregate | None = None,
    price_relative_tolerance: Decimal = DEFAULT_PRICE_RELATIVE_TOLERANCE,
    price_absolute_tolerance: Decimal = DEFAULT_PRICE_ABSOLUTE_TOLERANCE,
    volume_relative_tolerance: Decimal = DEFAULT_VOLUME_RELATIVE_TOLERANCE,
) -> VolumeDecision:
    """Choose shares (``1``) or hundred-shares (``100``), else quarantine.

    When ``amount`` is usable, a multiplier qualifies only if
    ``amount / (raw_volume * multiplier)`` falls inside the row's inclusive
    ``[low, high]`` envelope after the documented small rounding allowance.
    Only exactly one qualifying multiplier is accepted.  If that evidence is
    absent or ambiguous, a *complete* independent intraday aggregate may make
    the same unique choice by comparing canonical daily volume.  All remaining
    outcomes are explicit ``ambiguous_volume_unit`` quarantine decisions.
    """

    try:
        volume, low_price, high_price = (_decimal(raw_volume), _decimal(low), _decimal(high))
        usable_amount = None if amount is None else _decimal(amount)
    except (InvalidOperation, ValueError):
        return _ambiguous("invalid_numeric_volume_evidence", (), aggregate)
    if volume <= 0 or low_price < 0 or high_price < low_price:
        return _ambiguous("invalid_volume_or_price_envelope", (), aggregate)

    implied_prices: tuple[tuple[int, Decimal], ...] = ()
    price_candidates: list[int] = []
    if usable_amount is not None and usable_amount > 0:
        implied_prices = tuple((multiplier, usable_amount / (volume * multiplier)) for multiplier in MULTIPLIERS)
        price_candidates = [
            multiplier
            for multiplier, implied in implied_prices
            if _within_price_envelope(
                implied, low_price, high_price,
                relative_tolerance=price_relative_tolerance,
                absolute_tolerance=price_absolute_tolerance,
            )
        ]
        if len(price_candidates) == 1:
            return _accepted(volume, price_candidates[0], "amount_implied_price", implied_prices, aggregate_checked=False)

    aggregate_candidates = _aggregate_candidates(volume, aggregate, volume_relative_tolerance)
    if len(aggregate_candidates) == 1:
        return _accepted(
            volume, aggregate_candidates[0], "complete_intraday_aggregate", implied_prices,
            aggregate_checked=True, aggregate=aggregate,
        )

    if usable_amount is None:
        reason = "missing_amount_and_no_unique_complete_intraday_aggregate"
    elif usable_amount <= 0:
        reason = "nonpositive_amount_and_no_unique_complete_intraday_aggregate"
    elif len(price_candidates) == 0:
        reason = "no_multiplier_matches_amount_price_envelope"
    else:
        reason = "multiple_multipliers_match_amount_price_envelope"
    return _ambiguous(reason, implied_prices, aggregate, aggregate_checked=aggregate is not None)


def _accepted(
    raw_volume: Decimal,
    multiplier: int,
    basis: Literal["amount_implied_price", "complete_intraday_aggregate"],
    implied_prices: tuple[tuple[int, Decimal], ...],
    *,
    aggregate_checked: bool,
    aggregate: CompleteIntradayAggregate | None = None,
) -> VolumeDecision:
    return VolumeDecision(
        action="accept", multiplier=multiplier, volume_shares=raw_volume * multiplier, basis=basis, reason=None, detail=None,
        implied_prices=implied_prices, aggregate_checked=aggregate_checked,
        aggregate_source_name=None if aggregate is None else aggregate.source_name,
        aggregate_source_ref=None if aggregate is None else aggregate.source_ref,
    )


def _aggregate_candidates(
    raw_volume: Decimal,
    aggregate: CompleteIntradayAggregate | None,
    tolerance: Decimal,
) -> list[int]:
    if aggregate is None or not aggregate.complete:
        return []
    try:
        aggregate_volume = _decimal(aggregate.volume_shares)
    except (InvalidOperation, ValueError):
        return []
    if aggregate_volume < 0:
        return []
    return [
        multiplier for multiplier in MULTIPLIERS
        if _within_relative(aggregate_volume, raw_volume * multiplier, tolerance)
    ]


def _ambiguous(
    reason: str,
    implied_prices: tuple[tuple[int, Decimal], ...],
    aggregate: CompleteIntradayAggregate | None,
    *,
    aggregate_checked: bool = False,
) -> VolumeDecision:
    return VolumeDecision(
        action="quarantine", multiplier=None, volume_shares=None, basis="ambiguous", reason="ambiguous_volume_unit", detail=reason,
        implied_prices=implied_prices, aggregate_checked=aggregate_checked,
        aggregate_source_name=None if aggregate is None else aggregate.source_name,
        aggregate_source_ref=None if aggregate is None else aggregate.source_ref,
    )


def _within_price_envelope(
    value: Decimal, low: Decimal, high: Decimal, *, relative_tolerance: Decimal, absolute_tolerance: Decimal,
) -> bool:
    tolerance = max(absolute_tolerance, abs(low) * relative_tolerance, abs(high) * relative_tolerance)
    return low - tolerance <= value <= high + tolerance


def _within_relative(actual: Decimal, expected: Decimal, tolerance: Decimal) -> bool:
    return abs(actual - expected) <= max(Decimal("1"), abs(expected)) * tolerance


def _decimal(value: Decimal | int | float | str) -> Decimal:
    result = value if isinstance(value, Decimal) else Decimal(str(value))
    if not result.is_finite():
        raise ValueError("numeric evidence must be finite")
    return result
