from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.market_sidebar.repository import SidebarSnapshotRepository


IWENCAI_SOURCE = "iwencai"
NOTTE_SOURCE = "notte"
EXTERNAL_SOURCES = frozenset({IWENCAI_SOURCE, NOTTE_SOURCE})
LOCAL_SOURCE = "local_db"
SHANGHAI = ZoneInfo("Asia/Shanghai")
EXTERNAL_DOMAINS = ("quote", "profile", "valuation", "capital_flow", "themes", "strength", "news")


@dataclass(frozen=True)
class SidebarContext:
    connection_id: str
    subscription_id: str
    chart_symbol: str
    chart_epoch: int
    watchlist_symbols: tuple[str, ...]
    channels: frozenset[str]
    watchlist_id: str = "default"
    watchlist_revision: int = 0


class SidebarAggregator:
    """Cache-only sidebar assembly. Cache population is requested separately."""

    def __init__(self, repository: SidebarSnapshotRepository, now: Callable[[], datetime] | None = None) -> None:
        self.repository = repository
        self._now = now or (lambda: datetime.now(SHANGHAI))
        self._resync_fences: set[tuple[str, str, int, int, int]] = set()
        self._stream_cursors: dict[tuple[str, str, int], tuple[int, int]] = {}
        self._stream_hashes: dict[tuple[str, str, int], dict[str, str]] = {}

    async def bootstrap(self, **kwargs: Any) -> dict[str, Any]:
        snapshot = await self._read_snapshot(**kwargs)
        snapshot["sequence"] = snapshot["snapshot_version"] = 0
        return snapshot

    async def request_refresh(self, context: SidebarContext, reason: str) -> None:
        # The collector decides whether a current-trading-day cache miss needs work.
        try:
            await self.repository.publish_refresh({
                "type": "sidebar_refresh_requested",
                "reason": reason,
                "chart_symbol": context.chart_symbol,
                "watchlist_symbols": list(context.watchlist_symbols),
                "watchlist_id": context.watchlist_id,
                "watchlist_revision": context.watchlist_revision,
                "chart_epoch": context.chart_epoch,
            })
        except Exception:
            # Redis/collector outages leave bootstrap cache-only and readable.
            return

    async def _read_snapshot(self, *, chart_symbol: str, chart_epoch: int, watchlist_id: str, watchlist_revision: int, watchlist_symbols: list[str] | tuple[str, ...]) -> dict[str, Any]:
        symbols = tuple(dict.fromkeys(watchlist_symbols))
        quote_symbols = tuple(dict.fromkeys((*symbols, chart_symbol)))
        trading_date, current_trading_day = _trading_day(self._now())
        keys = [
            *(self._key(trading_date, "quote", symbol) for symbol in quote_symbols),
            *(self._key(trading_date, domain, chart_symbol) for domain in EXTERNAL_DOMAINS if domain not in {"quote", "strength"}),
            self._key(trading_date, "strength", "market"),
        ]
        values = await self.repository.get_json_many(keys)
        quote_values = values[:len(quote_symbols)]
        domains = dict(zip((domain for domain in EXTERNAL_DOMAINS if domain not in {"quote", "strength"}), values[len(quote_symbols):-1], strict=True))
        strength = values[-1]
        quotes = {
            symbol: _external(value, trading_date, current_trading_day, defaults={"symbol": symbol})
            for symbol, value in zip(quote_symbols, quote_values, strict=True)
        }
        external = {
            domain: _external(value, trading_date, current_trading_day)
            for domain, value in domains.items()
        }
        local = await self.repository.get_local_projection(chart_symbol)
        profile_metadata = _metadata(external["profile"])
        themes = _theme_items(external["themes"])
        news = {
            **external["news"],
            "symbol": chart_symbol,
            "chart_epoch": chart_epoch,
            "items": _items(external["news"]),
        }
        return {
            "context": {"chart_symbol": chart_symbol, "chart_epoch": chart_epoch, "watchlist_id": watchlist_id, "watchlist_revision": watchlist_revision},
            "watchlist_quotes": {symbol: quotes[symbol] for symbol in symbols},
            "active_symbol_profile": {
                **profile_metadata,
                "symbol": chart_symbol,
                "quote": quotes[chart_symbol],
                "identity": external["profile"],
                "valuation": external["valuation"],
                "capital_flow": external["capital_flow"],
                "themes": themes,
                "chan_state": _chan_state(local.get("chan_state")),
                "strategy_signals": _strategy_signals(local.get("strategy_signals")),
            },
            "strongest_preview": _external(strength, trading_date, current_trading_day),
            "news_preview": news,
        }

    @staticmethod
    def _key(trading_date: str, domain: str, subject: str) -> str:
        return f"sidebar:iwencai:{trading_date}:{domain}:{subject}"

    async def delta_events(self, context: SidebarContext, after_sequence: int, snapshot_version: int) -> list[dict[str, Any]]:
        stream_key = (context.connection_id, context.subscription_id, context.chart_epoch)
        current_sequence, current_version = self._stream_cursors.get(stream_key, (0, 0))
        if ((after_sequence and after_sequence != current_sequence) or (snapshot_version and snapshot_version != current_version)):
            fence = (*stream_key, after_sequence, snapshot_version)
            if fence in self._resync_fences:
                return []
            self._resync_fences.add(fence)
            snapshot = await self._read_snapshot(chart_symbol=context.chart_symbol, chart_epoch=context.chart_epoch, watchlist_id=context.watchlist_id, watchlist_revision=context.watchlist_revision, watchlist_symbols=context.watchlist_symbols)
            sequence, version = self._advance_stream(stream_key)
            snapshot.update(sequence=sequence, snapshot_version=version)
            return [{"type": "sidebar_resync_required", "subscription_id": context.subscription_id, "sequence": sequence, "snapshot_version": version, **_event_context(context), "cursor": {"sequence": sequence, "snapshot_version": version}, "reason": "cursor_mismatch", "snapshot": snapshot}]

        snapshot = await self._read_snapshot(chart_symbol=context.chart_symbol, chart_epoch=context.chart_epoch, watchlist_id=context.watchlist_id, watchlist_revision=context.watchlist_revision, watchlist_symbols=context.watchlist_symbols)
        candidates = (("watchlist_quotes", "watchlist_quote_delta", {"quotes": snapshot["watchlist_quotes"]}), ("active_profile", "active_profile_delta", {"profile": snapshot["active_symbol_profile"]}), ("strength", "strength_delta", {"strength": snapshot["strongest_preview"]}), ("news", "news_delta", {"news": snapshot["news_preview"]}), ("chan_strategy", "chan_strategy_delta", {"profile": snapshot["active_symbol_profile"]}))
        events: list[dict[str, Any]] = []
        hashes = self._stream_hashes.setdefault(stream_key, {})
        for channel, event_type, body in candidates:
            if channel not in context.channels:
                continue
            payload = {**_event_context(context), **body}
            content_hash = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            if hashes.get(channel) == content_hash:
                continue
            hashes[channel] = content_hash
            sequence, version = self._advance_stream(stream_key)
            events.append({"type": event_type, "subscription_id": context.subscription_id, "sequence": sequence, "snapshot_version": version, **payload})
        return events

    async def unsubscribe(self, connection_id: str, subscription_id: str) -> None:
        self._cleanup_streams(lambda key: key[0] == connection_id and key[1] == subscription_id)

    async def disconnect(self, connection_id: str) -> None:
        self._cleanup_streams(lambda key: key[0] == connection_id)

    def _cleanup_streams(self, matches: Callable[[tuple[str, str, int]], bool]) -> None:
        for key in {key for key in (*self._stream_cursors, *self._stream_hashes) if matches(key)}:
            self._stream_cursors.pop(key, None)
            self._stream_hashes.pop(key, None)
        self._resync_fences = {fence for fence in self._resync_fences if not matches(fence[:3])}

    def _advance_stream(self, key: tuple[str, str, int]) -> tuple[int, int]:
        sequence, version = self._stream_cursors.get(key, (0, 0))
        self._stream_cursors[key] = (sequence + 1, version + 1)
        return self._stream_cursors[key]


def sidebar_stream_id(context: SidebarContext) -> str:
    digest = hashlib.blake2s(f"{context.connection_id}\0{context.subscription_id}\0{context.chart_epoch}".encode(), digest_size=12).hexdigest()
    return f"sidebar:{digest}"


def _event_context(context: SidebarContext) -> dict[str, Any]:
    return {"stream_id": sidebar_stream_id(context), "chart_symbol": context.chart_symbol, "chart_epoch": context.chart_epoch, "watchlist_id": context.watchlist_id, "watchlist_revision": context.watchlist_revision}


def _trading_day(now: datetime) -> tuple[str, bool]:
    local = now.astimezone(SHANGHAI).date()
    current = local.weekday() < 5
    while local.weekday() >= 5:
        local -= timedelta(days=1)
    return local.isoformat(), current


def _external(value: dict | None, trading_date: str, current_trading_day: bool, *, defaults: dict[str, Any] | None = None) -> dict[str, Any]:
    envelope = value if isinstance(value, dict) else {}
    source = envelope.get("source")
    valid_source = source in EXTERNAL_SOURCES
    raw_payload = envelope.get("value") if "value" in envelope else envelope
    if isinstance(raw_payload, dict):
        result = dict(raw_payload)
    elif isinstance(raw_payload, list):
        result = {"items": raw_payload}
    else:
        result = {}
    result = {**(defaults or {}), **result}
    result["source"] = source if valid_source else IWENCAI_SOURCE
    result["trading_date"] = str(envelope.get("trading_date") or trading_date)
    result["as_of"] = str(
        envelope.get("provider_ts")
        or envelope.get("received_at")
        or envelope.get("as_of")
        or f"{result['trading_date']}T00:00:00+08:00"
    )
    freshness = envelope.get("freshness") if valid_source else "unavailable"
    result["freshness"] = freshness if freshness in {"fresh", "stale", "unavailable"} else "unavailable"
    if not current_trading_day and result["freshness"] == "fresh":
        result["freshness"] = "stale"
    return result


def _metadata(value: dict[str, Any]) -> dict[str, Any]:
    return {key: value[key] for key in ("source", "freshness", "as_of", "trading_date")}


def _items(value: dict[str, Any]) -> list[Any]:
    items = value.get("items")
    return items if isinstance(items, list) else []


def _theme_items(value: dict[str, Any]) -> list[Any]:
    concepts = value.get("concepts")
    if isinstance(concepts, (list, tuple)):
        return list(concepts)
    return _items(value)


def _chan_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"source": LOCAL_SOURCE, "stroke_states": []}
    states = value.get("stroke_states")
    return {"source": LOCAL_SOURCE, "stroke_states": states if isinstance(states, list) else []}


def _strategy_signals(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [{**item, "source": LOCAL_SOURCE} for item in value if isinstance(item, dict)]
