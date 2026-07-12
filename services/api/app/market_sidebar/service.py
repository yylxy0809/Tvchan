from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Callable

from app.market_sidebar.repository import SidebarSnapshotRepository


UNAVAILABLE_SOURCE = "normalized_snapshot"


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
    def __init__(self, repository: SidebarSnapshotRepository) -> None:
        self.repository = repository
        self._resync_fences: set[tuple[str, str, int, int, int]] = set()
        self._stream_cursors: dict[tuple[str, str, int], tuple[int, int]] = {}
        self._stream_hashes: dict[tuple[str, str, int], dict[str, str]] = {}

    async def bootstrap(
        self,
        *,
        chart_symbol: str,
        chart_epoch: int,
        watchlist_id: str = "default",
        watchlist_revision: int = 0,
        watchlist_symbols: list[str] | tuple[str, ...],
    ) -> dict[str, Any]:
        snapshot = await self._read_snapshot(
            chart_symbol=chart_symbol,
            chart_epoch=chart_epoch,
            watchlist_id=watchlist_id,
            watchlist_revision=watchlist_revision,
            watchlist_symbols=watchlist_symbols,
        )
        # Bootstrap is an HTTP snapshot, not a replayable WebSocket cursor.
        snapshot["snapshot_version"] = 0
        snapshot["sequence"] = 0
        return snapshot

    async def _read_snapshot(
        self,
        *,
        chart_symbol: str,
        chart_epoch: int,
        watchlist_id: str,
        watchlist_revision: int,
        watchlist_symbols: list[str] | tuple[str, ...],
    ) -> dict[str, Any]:
        symbols = tuple(dict.fromkeys(watchlist_symbols))
        quote_symbols = tuple(dict.fromkeys((*symbols, chart_symbol)))
        keys = [
            *(f"market:quote:{symbol}" for symbol in quote_symbols),
            f"market:profile:{chart_symbol}",
            f"market:finance:{chart_symbol}",
            f"market:fund:{chart_symbol}",
            "market:strength:latest",
            f"market:news:{chart_symbol}",
        ]
        values = await self.repository.get_json_many(keys)
        quotes = dict(zip(quote_symbols, values[:len(quote_symbols)], strict=True))
        profile, finance, fund, strength, news = values[len(quote_symbols):]
        return {
            "context": {
                "chart_symbol": chart_symbol,
                "chart_epoch": chart_epoch,
                "watchlist_id": watchlist_id,
                "watchlist_revision": watchlist_revision,
            },
            "watchlist_quotes": {
                symbol: quotes[symbol] or _unavailable() for symbol in symbols
            },
            "active_symbol_profile": {
                "symbol": chart_symbol,
                "quote": quotes[chart_symbol] or _unavailable(),
                "identity": profile or _unavailable(),
                "valuation": finance or _unavailable(),
                "capital_flow": fund or _unavailable(),
                "themes": (profile or {}).get("themes", []),
                "chan_state": _unavailable("canonical_module_c"),
                "strategy_signals": [],
            },
            "strongest_preview": strength or _unavailable(),
            "news_preview": _news_preview(news, chart_symbol, chart_epoch, quotes),
        }

    async def delta_events(
        self,
        context: SidebarContext,
        after_sequence: int,
        snapshot_version: int,
    ) -> list[dict[str, Any]]:
        stream_key = (context.connection_id, context.subscription_id, context.chart_epoch)
        current_sequence, current_version = self._stream_cursors.get(stream_key, (0, 0))
        cursor_mismatch = (
            after_sequence != 0 and after_sequence != current_sequence
        ) or (
            snapshot_version != 0 and snapshot_version != current_version
        )
        if cursor_mismatch:
            fence = (
                context.connection_id,
                context.subscription_id,
                context.chart_epoch,
                after_sequence,
                snapshot_version,
            )
            if fence in self._resync_fences:
                return []
            self._resync_fences.add(fence)
            snapshot = await self._read_snapshot(
                chart_symbol=context.chart_symbol,
                chart_epoch=context.chart_epoch,
                watchlist_id=context.watchlist_id,
                watchlist_revision=context.watchlist_revision,
                watchlist_symbols=context.watchlist_symbols,
            )
            sequence, version = self._advance_stream(stream_key)
            snapshot["sequence"] = sequence
            snapshot["snapshot_version"] = version
            return [{
                "type": "sidebar_resync_required",
                "subscription_id": context.subscription_id,
                "sequence": sequence,
                "snapshot_version": version,
                **_event_context(context),
                "cursor": {
                    "sequence": sequence,
                    "snapshot_version": version,
                },
                "reason": "cursor_mismatch",
                "snapshot": snapshot,
            }]

        snapshot = await self._read_snapshot(
            chart_symbol=context.chart_symbol,
            chart_epoch=context.chart_epoch,
            watchlist_id=context.watchlist_id,
            watchlist_revision=context.watchlist_revision,
            watchlist_symbols=context.watchlist_symbols,
        )
        candidates = [
            ("watchlist_quotes", "watchlist_quote_delta", {
                "quotes": snapshot["watchlist_quotes"],
            }),
            ("active_profile", "active_profile_delta", {
                "profile": snapshot["active_symbol_profile"],
            }),
            ("strength", "strength_delta", {"strength": snapshot["strongest_preview"]}),
            ("news", "news_delta", {
                "source": "iwencai_news_search",
                "news": snapshot["news_preview"],
            }),
        ]
        events = []
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
            events.append({
                "type": event_type,
                "subscription_id": context.subscription_id,
                "sequence": sequence,
                "snapshot_version": version,
                **payload,
            })
        return events

    async def unsubscribe(self, connection_id: str, subscription_id: str) -> None:
        self._cleanup_streams(
            lambda key: key[0] == connection_id and key[1] == subscription_id
        )

    async def disconnect(self, connection_id: str) -> None:
        self._cleanup_streams(lambda key: key[0] == connection_id)

    def _cleanup_streams(
        self, matches: Callable[[tuple[str, str, int]], bool]
    ) -> None:
        keys = [key for key in self._stream_cursors if matches(key)]
        keys.extend(key for key in self._stream_hashes if matches(key) and key not in keys)
        for key in keys:
            self._stream_cursors.pop(key, None)
            self._stream_hashes.pop(key, None)
        self._resync_fences = {
            fence for fence in self._resync_fences
            if not matches(fence[:3])
        }

    def _advance_stream(self, stream_key: tuple[str, str, int]) -> tuple[int, int]:
        sequence, version = self._stream_cursors.get(stream_key, (0, 0))
        cursor = (sequence + 1, version + 1)
        self._stream_cursors[stream_key] = cursor
        return cursor


def sidebar_stream_id(context: SidebarContext) -> str:
    identity = f"{context.connection_id}\0{context.subscription_id}\0{context.chart_epoch}"
    digest = hashlib.blake2s(identity.encode("utf-8"), digest_size=12).hexdigest()
    return f"sidebar:{digest}"


def _event_context(context: SidebarContext) -> dict[str, Any]:
    return {
        "stream_id": sidebar_stream_id(context),
        "chart_symbol": context.chart_symbol,
        "chart_epoch": context.chart_epoch,
        "watchlist_id": context.watchlist_id,
        "watchlist_revision": context.watchlist_revision,
    }


def _unavailable(source: str = UNAVAILABLE_SOURCE) -> dict[str, str]:
    return {"source": source, "freshness": "unavailable"}


def _news_preview(
    value: dict | None,
    symbol: str,
    chart_epoch: int,
    quotes: dict[str, dict | None],
) -> dict[str, Any]:
    value = value or {}
    freshness = str(value.get("freshness") or "unavailable")
    items = []
    for raw_item in value.get("items", []):
        item = dict(raw_item)
        related = item.get("related_symbols") or [symbol]
        related_symbols = []
        for raw_symbol in related:
            related_symbol = (
                raw_symbol.get("symbol") if isinstance(raw_symbol, dict) else raw_symbol
            )
            if not isinstance(related_symbol, str):
                continue
            related_symbol = related_symbol.upper()
            quote = quotes.get(related_symbol) or {}
            change_percent = quote.get("change_percent")
            related_symbols.append({
                "symbol": related_symbol,
                "change_percent": change_percent
                if isinstance(change_percent, (int, float)) else None,
            })
        item["related_symbols"] = related_symbols
        items.append(item)
    return {
        "symbol": symbol,
        "chart_epoch": chart_epoch,
        "status": freshness,
        "items": items,
        "as_of": value.get("as_of") or value.get("received_at"),
        "source": "iwencai_news_search",
    }
