from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal


PublicationProfile = Literal["baseline", "online", "historical_replay"]


def utc_instant(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Lifecycle timestamps must be timezone-aware")
    return value.astimezone(UTC)


def structure_fingerprint(
    *,
    symbol_id: int,
    chan_level: int,
    structure_type: str,
    side_or_direction: str | None,
    bsp_type: str | None,
    point_time: datetime,
    end_time: datetime | None,
    price_x1000: int | None,
    start_price_x1000: int | None,
    end_price_x1000: int | None,
    low_x1000: int | None,
    high_x1000: int | None,
    config_hash: str,
    identity_version: int = 1,
) -> str:
    """Return a run-independent identity for one persisted Chan structure."""
    stable_prices = {
        "price_x1000": price_x1000 if structure_type == "signal" else None,
        "start_price_x1000": start_price_x1000 if structure_type in {"stroke", "segment"} else None,
        "low_x1000": None,
        "high_x1000": None,
    }
    payload = {
        "identity_version": identity_version,
        "symbol_id": symbol_id,
        "chan_level": chan_level,
        "structure_type": structure_type,
        "side_or_direction": side_or_direction or "",
        "bsp_type": bsp_type or "",
        "point_time": utc_instant(point_time).isoformat(),
        # Endpoints evolve while predictive structures extend. They remain in
        # the payload but are deliberately excluded from stable identity.
        **stable_prices,
        "config_hash": config_hash,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LifecycleState:
    status: str
    mode: str | None


def transition_event(
    *,
    profile: PublicationProfile,
    previous: LifecycleState | None,
    current_mode: str | None,
) -> str | None:
    """Classify one structure observation without inventing historic visibility."""
    if profile == "baseline":
        return "baseline_observed" if current_mode else None
    if current_mode is None:
        return "disappeared" if previous and previous.status != "disappeared" else None
    if previous is None:
        return "first_seen"
    if previous.status == "disappeared":
        return "reappeared"
    if previous.mode == "predictive" and current_mode == "confirmed":
        return "confirmed"
    return None
