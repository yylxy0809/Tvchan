from __future__ import annotations

import asyncio
import time

from fastapi.testclient import TestClient

from app.main import create_app
from app.market_sidebar.repository import (
    RedisSidebarSnapshotRepository,
    SidebarSnapshotRepository,
)
from app.market_sidebar.service import SidebarAggregator, SidebarContext
from app.routes import realtime


AUTH = {"Authorization": "Bearer dev-local-token"}


class FakeRepository(SidebarSnapshotRepository):
    def __init__(self, values: dict[str, dict] | None = None) -> None:
        self.values = values or {}
        self.reads: list[str] = []
        self.read_batches: list[list[str]] = []
        self.demands: dict[str, tuple[dict, int]] = {}
        self.demand_writes: list[str] = []
        self.demand_deletes: list[str] = []
        self.closed = False

    async def get_json(self, key: str) -> dict | None:
        self.reads.append(key)
        return self.values.get(key)

    async def get_json_many(self, keys: list[str] | tuple[str, ...]) -> list[dict | None]:
        self.read_batches.append(list(keys))
        self.reads.extend(keys)
        return [self.values.get(key) for key in keys]

    async def set_demand(self, key: str, value: dict, ttl_seconds: int) -> None:
        self.demands[key] = (value, ttl_seconds)
        self.demand_writes.append(key)

    async def delete_demand(self, key: str) -> None:
        self.demands.pop(key, None)
        self.demand_deletes.append(key)

    async def close(self) -> None:
        self.closed = True


def test_bootstrap_reads_normalized_snapshots_without_inferring_active_symbol() -> None:
    repository = FakeRepository(
        {
            "market:quote:000001.SZ": {
                "price": 10,
                "change_percent": 1.25,
                "freshness": "live",
            },
            "market:quote:600000.SH": {"price": 8, "freshness": "delayed"},
            "market:profile:000001.SZ": {"name": "Ping An", "freshness": "live"},
            "market:strength:latest": {"items": [{"symbol": "000001.SZ"}]},
            "market:news:000001.SZ": {
                "items": [{"event_id": "news-1", "title": "Notice"}],
                "freshness": "fresh",
            },
        }
    )
    app = create_app()
    app.state.market_sidebar_repository = repository

    with TestClient(app) as client:
        response = client.post(
            "/api/v3/market/sidebar/bootstrap",
            headers=AUTH,
            json={
                "chart_symbol": "000001.SZ",
                "chart_epoch": 18,
                "watchlist_id": "default",
                "watchlist_revision": 7,
                "watchlist_symbols": ["600000.SH"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["context"] == {
        "chart_symbol": "000001.SZ",
        "chart_epoch": 18,
        "watchlist_id": "default",
        "watchlist_revision": 7,
    }
    assert payload["active_symbol_profile"]["symbol"] == "000001.SZ"
    assert payload["active_symbol_profile"]["quote"]["price"] == 10
    assert set(payload["watchlist_quotes"]) == {"600000.SH"}
    assert payload["news_preview"]["chart_epoch"] == 18
    assert payload["news_preview"]["items"][0]["related_symbols"] == [
        {"symbol": "000001.SZ", "change_percent": 1.25}
    ]
    assert payload["sequence"] == 0
    assert payload["snapshot_version"] == 0
    assert len(repository.read_batches) == 1
    assert "market:profile:600000.SH" not in repository.reads


def test_bootstrap_returns_structured_unavailable_domains_when_redis_is_empty() -> None:
    app = create_app()
    app.state.market_sidebar_repository = FakeRepository()

    with TestClient(app) as client:
        response = client.post(
            "/api/v3/market/sidebar/bootstrap",
            headers=AUTH,
            json={
                "chart_symbol": "430047.BJ",
                "chart_epoch": 2,
                "watchlist_symbols": [],
            },
        )

    payload = response.json()
    assert response.status_code == 200
    assert payload["watchlist_quotes"] == {}
    assert payload["active_symbol_profile"]["symbol"] == "430047.BJ"
    assert payload["active_symbol_profile"]["quote"]["freshness"] == "unavailable"
    assert payload["active_symbol_profile"]["identity"]["freshness"] == "unavailable"
    assert payload["strongest_preview"]["freshness"] == "unavailable"
    assert payload["news_preview"]["status"] == "unavailable"
    assert payload["news_preview"]["items"] == []


def test_sidebar_events_are_fenced_deduplicated_and_bounded_resync_recovers() -> None:
    repository = FakeRepository(
        {
            "market:quote:000001.SZ": {"price": 10, "freshness": "live"},
            "market:profile:000001.SZ": {"name": "A", "freshness": "live"},
            "market:strength:latest": {"items": [], "freshness": "live"},
            "market:news:000001.SZ": {"items": [], "freshness": "fresh"},
        }
    )
    aggregator = SidebarAggregator(repository)
    context = SidebarContext(
        connection_id="connection-1",
        subscription_id="right-sidebar",
        chart_symbol="000001.SZ",
        chart_epoch=18,
        watchlist_symbols=("000001.SZ",),
        channels=frozenset({"watchlist_quotes", "active_profile", "strength", "news"}),
    )

    async def scenario() -> None:
        events = await aggregator.delta_events(context, after_sequence=0, snapshot_version=0)
        assert [event["type"] for event in events] == [
            "watchlist_quote_delta",
            "active_profile_delta",
            "strength_delta",
            "news_delta",
        ]
        assert all(event["chart_epoch"] == 18 for event in events[1::2])
        assert events[-1]["source"] == "iwencai_news_search"
        assert [event["sequence"] for event in events] == sorted(
            event["sequence"] for event in events
        )
        assert len({event["stream_id"] for event in events}) == 1
        for event in events:
            assert {
                "chart_symbol": event["chart_symbol"],
                "chart_epoch": event["chart_epoch"],
                "watchlist_id": event["watchlist_id"],
                "watchlist_revision": event["watchlist_revision"],
            } == {
                "chart_symbol": "000001.SZ",
                "chart_epoch": 18,
                "watchlist_id": "default",
                "watchlist_revision": 0,
            }
        assert set(events[0]["quotes"]) == {"000001.SZ"}
        assert set(events[-1]["news"]) == {
            "symbol", "chart_epoch", "status", "items", "as_of", "source"
        }

        last = events[-1]
        assert await aggregator.delta_events(
            context, last["sequence"], last["snapshot_version"]
        ) == []

        repository.values["market:quote:000001.SZ"] = {
            "price": 11,
            "freshness": "live",
        }
        changed = await aggregator.delta_events(
            context, last["sequence"], last["snapshot_version"]
        )
        assert [event["type"] for event in changed] == [
            "watchlist_quote_delta",
            "active_profile_delta",
        ]
        assert changed[0]["quotes"]["000001.SZ"]["price"] == 11

        resync = await aggregator.delta_events(
            context,
            after_sequence=999,
            snapshot_version=999,
        )
        assert [event["type"] for event in resync] == ["sidebar_resync_required"]
        assert resync[0]["snapshot"]["active_symbol_profile"]["symbol"] == "000001.SZ"
        assert (await aggregator.delta_events(context, 999, 999)) == []

    asyncio.run(scenario())


def test_bootstrap_cursor_does_not_initialize_realtime_stream() -> None:
    aggregator = SidebarAggregator(FakeRepository())
    context = SidebarContext(
        connection_id="connection-1",
        subscription_id="right-sidebar",
        chart_symbol="000001.SZ",
        chart_epoch=18,
        watchlist_symbols=(),
        channels=frozenset({"active_profile"}),
    )

    async def scenario() -> None:
        bootstrap = await aggregator.bootstrap(
            chart_symbol="000001.SZ",
            chart_epoch=18,
            watchlist_id="default",
            watchlist_revision=3,
            watchlist_symbols=[],
        )
        events = await aggregator.delta_events(
            context,
            bootstrap["sequence"],
            bootstrap["snapshot_version"],
        )
        assert bootstrap["sequence"] == bootstrap["snapshot_version"] == 0
        assert [event["type"] for event in events] == ["active_profile_delta"]
        assert events[0]["sequence"] == 1

    asyncio.run(scenario())


def test_sidebar_streams_are_isolated_by_connection_and_epoch() -> None:
    aggregator = SidebarAggregator(FakeRepository())

    def context(connection_id: str, epoch: int) -> SidebarContext:
        return SidebarContext(
            connection_id=connection_id,
            subscription_id="right-sidebar",
            chart_symbol="000001.SZ",
            chart_epoch=epoch,
            watchlist_symbols=(),
            channels=frozenset({"active_profile"}),
        )

    async def scenario() -> None:
        first = await aggregator.delta_events(context("browser-a", 1), 0, 0)
        second = await aggregator.delta_events(context("browser-b", 1), 0, 0)
        next_epoch = await aggregator.delta_events(context("browser-a", 2), 0, 0)

        assert first[0]["sequence"] == 1
        assert second[0]["sequence"] == 1
        assert next_epoch[0]["sequence"] == 1
        assert len({first[0]["stream_id"], second[0]["stream_id"], next_epoch[0]["stream_id"]}) == 3

    asyncio.run(scenario())


def test_cross_connection_cursor_replay_resyncs_new_stream_from_one() -> None:
    aggregator = SidebarAggregator(FakeRepository())

    def context(connection_id: str) -> SidebarContext:
        return SidebarContext(
            connection_id=connection_id,
            subscription_id="right-sidebar",
            chart_symbol="000001.SZ",
            chart_epoch=7,
            watchlist_symbols=(),
            channels=frozenset({"active_profile"}),
            watchlist_id="focus",
            watchlist_revision=4,
        )

    async def scenario() -> None:
        original = (await aggregator.delta_events(context("browser-a"), 0, 0))[0]
        replay = await aggregator.delta_events(
            context("browser-b"),
            original["sequence"],
            original["snapshot_version"],
        )

        assert replay[0]["type"] == "sidebar_resync_required"
        assert replay[0]["stream_id"] != original["stream_id"]
        assert replay[0]["sequence"] == 1
        assert replay[0]["cursor"] == {"sequence": 1, "snapshot_version": 1}
        assert replay[0]["watchlist_id"] == "focus"
        assert replay[0]["watchlist_revision"] == 4

    asyncio.run(scenario())


def test_sidebar_resync_carries_current_snapshot_and_cleanup_releases_state() -> None:
    aggregator = SidebarAggregator(FakeRepository())
    context = SidebarContext(
        connection_id="browser-a",
        subscription_id="right-sidebar",
        chart_symbol="000001.SZ",
        chart_epoch=1,
        watchlist_symbols=(),
        channels=frozenset({"active_profile"}),
    )

    async def scenario() -> None:
        await aggregator.delta_events(context, 0, 0)
        resync = await aggregator.delta_events(context, 99, 99)
        assert resync[0]["type"] == "sidebar_resync_required"
        assert resync[0]["snapshot"]["sequence"] == resync[0]["sequence"]
        assert resync[0]["snapshot"]["snapshot_version"] == resync[0]["snapshot_version"]

        await aggregator.unsubscribe("browser-a", "right-sidebar")
        assert not aggregator._stream_cursors
        assert not aggregator._stream_hashes
        assert not aggregator._resync_fences

        await aggregator.delta_events(context, 0, 0)
        await aggregator.disconnect("browser-a")
        assert not aggregator._stream_cursors

    asyncio.run(scenario())


def test_redis_repository_uses_mget_and_closes_client() -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.mget_calls: list[list[str]] = []
            self.closed = False

        async def mget(self, keys: list[str]) -> list[str | None]:
            self.mget_calls.append(keys)
            return ['{"price":10}', None]

        async def aclose(self) -> None:
            self.closed = True

    async def scenario() -> None:
        client = FakeRedis()
        repository = RedisSidebarSnapshotRepository("redis://unused", client=client)
        assert await repository.get_json_many(["market:quote:000001.SZ", "missing"]) == [
            {"price": 10},
            None,
        ]
        assert client.mget_calls == [["market:quote:000001.SZ", "missing"]]
        await repository.close()
        assert client.closed

    asyncio.run(scenario())


def test_lifespan_closes_sidebar_repository() -> None:
    repository = FakeRepository()
    app = create_app()
    app.state.market_sidebar_repository = repository

    with TestClient(app):
        pass

    assert repository.closed


def test_realtime_accepts_sidebar_context_and_emits_fenced_deltas() -> None:
    app = create_app()
    app.state.market_sidebar_repository = FakeRepository()

    with TestClient(app) as client:
        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
            ws.send_json(
                {
                    "type": "set_sidebar_context",
                    "subscription_id": "right-sidebar",
                    "chart_symbol": "000001.SZ",
                    "chart_epoch": 18,
                    "watchlist_symbols": ["600000.SH"],
                    "channels": [
                        "watchlist_quotes",
                        "active_profile",
                        "strength",
                        "news",
                    ],
                }
            )
            accepted = ws.receive_json()
            assert accepted == {
                "type": "sidebar_context_set",
                "subscription_id": "right-sidebar",
                "chart_symbol": "000001.SZ",
                "chart_epoch": 18,
                "watchlist_id": "default",
                "watchlist_revision": 0,
                "stream_id": accepted["stream_id"],
                "sequence": 0,
                "snapshot_version": 0,
                "cursor": {"sequence": 0, "snapshot_version": 0},
            }
            events = [ws.receive_json() for _ in range(4)]
            assert [event["type"] for event in events] == [
                "watchlist_quote_delta",
                "active_profile_delta",
                "strength_delta",
                "news_delta",
            ]
            assert events[1]["chart_symbol"] == "000001.SZ"
            assert events[1]["chart_epoch"] == 18
            assert events[3]["source"] == "iwencai_news_search"
            assert all(event["stream_id"] == accepted["stream_id"] for event in events)
            assert all(event["watchlist_id"] == "default" for event in events)
            assert all(event["watchlist_revision"] == 0 for event in events)


def test_realtime_keeps_sidebar_context_and_pushes_only_changed_snapshots(monkeypatch) -> None:
    monkeypatch.setattr(realtime, "SIDEBAR_POLL_INTERVAL_SECONDS", 0.01)
    repository = FakeRepository(
        {"market:quote:000001.SZ": {"price": 10, "freshness": "live"}}
    )
    app = create_app()
    app.state.market_sidebar_repository = repository

    with TestClient(app) as client:
        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
            ws.send_json(
                {
                    "type": "set_sidebar_context",
                    "subscription_id": "right-sidebar",
                    "chart_symbol": "000001.SZ",
                    "chart_epoch": 18,
                    "watchlist_symbols": ["000001.SZ"],
                    "channels": ["watchlist_quotes"],
                }
            )
            assert ws.receive_json()["type"] == "sidebar_context_set"
            first = ws.receive_json()
            assert first["type"] == "watchlist_quote_delta"
            assert first["quotes"]["000001.SZ"]["price"] == 10

            repository.values["market:quote:000001.SZ"] = {
                "price": 11,
                "freshness": "live",
            }
            changed = ws.receive_json()
            assert changed["type"] == "watchlist_quote_delta"
            assert changed["quotes"]["000001.SZ"]["price"] == 11
            assert changed["sequence"] == first["sequence"] + 1

            ws.send_json({"type": "unsubscribe", "id": "right-sidebar"})
            assert ws.receive_json() == {
                "type": "unsubscribed",
                "id": "right-sidebar",
            }


def test_realtime_context_update_preserves_stream_and_monotonic_sequence(monkeypatch) -> None:
    monkeypatch.setattr(realtime, "SIDEBAR_POLL_INTERVAL_SECONDS", 0.01)
    repository = FakeRepository(
        {"market:quote:000001.SZ": {"price": 10, "freshness": "live"}}
    )
    app = create_app()
    app.state.market_sidebar_repository = repository

    with TestClient(app) as client:
        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
            base = {
                "type": "set_sidebar_context",
                "subscription_id": "right-sidebar",
                "chart_symbol": "000001.SZ",
                "chart_epoch": 18,
                "watchlist_id": "default",
                "watchlist_symbols": ["000001.SZ"],
                "channels": ["watchlist_quotes"],
            }
            ws.send_json(base)
            accepted = ws.receive_json()
            first = ws.receive_json()

            ws.send_json({
                **base,
                "watchlist_revision": 1,
                "after_sequence": first["sequence"],
                "snapshot_version": first["snapshot_version"],
            })
            updated = ws.receive_json()
            second = ws.receive_json()

            assert updated["stream_id"] == accepted["stream_id"]
            assert second["stream_id"] == accepted["stream_id"]
            assert second["sequence"] == first["sequence"] + 1
            assert second["watchlist_revision"] == 1


def test_realtime_cross_connection_replay_gets_new_stream_resync_from_one() -> None:
    app = create_app()
    app.state.market_sidebar_repository = FakeRepository()

    with TestClient(app) as client:
        request = {
            "type": "set_sidebar_context",
            "subscription_id": "right-sidebar",
            "chart_symbol": "000001.SZ",
            "chart_epoch": 9,
            "watchlist_id": "focus",
            "watchlist_revision": 2,
            "watchlist_symbols": [],
            "channels": ["active_profile"],
        }
        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as first:
            first.send_json(request)
            first_accepted = first.receive_json()
            original = first.receive_json()

        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as second:
            second.send_json({
                **request,
                "after_sequence": original["sequence"],
                "snapshot_version": original["snapshot_version"],
            })
            second_accepted = second.receive_json()
            replay = second.receive_json()

            assert second_accepted["stream_id"] != first_accepted["stream_id"]
            assert replay["type"] == "sidebar_resync_required"
            assert replay["stream_id"] == second_accepted["stream_id"]
            assert replay["sequence"] == 1
            assert replay["cursor"] == {"sequence": 1, "snapshot_version": 1}
            assert replay["chart_symbol"] == "000001.SZ"
            assert replay["chart_epoch"] == 9
            assert replay["watchlist_id"] == "focus"
            assert replay["watchlist_revision"] == 2


def test_two_realtime_connections_register_independent_demands_and_cleanup() -> None:
    repository = FakeRepository()
    app = create_app()
    app.state.market_sidebar_repository = repository

    with TestClient(app) as client:
        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as first:
            with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as second:
                for ws, symbol in ((first, "000001.SZ"), (second, "600000.SH")):
                    ws.send_json(
                        {
                            "type": "set_sidebar_context",
                            "subscription_id": "right-sidebar",
                            "chart_symbol": symbol,
                            "chart_epoch": 1,
                            "watchlist_symbols": [symbol],
                            "channels": [],
                        }
                    )
                    assert ws.receive_json()["type"] == "sidebar_context_set"

                assert len(repository.demands) == 2
                payloads = [entry[0] for entry in repository.demands.values()]
                assert {payload["chart_symbol"] for payload in payloads} == {
                    "000001.SZ",
                    "600000.SH",
                }
                assert all(ttl == 30 for _, ttl in repository.demands.values())

            deadline = time.time() + 1
            while len(repository.demands) != 1 and time.time() < deadline:
                time.sleep(0.01)
            assert len(repository.demands) == 1

        deadline = time.time() + 1
        while repository.demands and time.time() < deadline:
            time.sleep(0.01)
        assert repository.demands == {}
        assert len(repository.demand_deletes) == 2


def test_sidebar_demand_ttl_is_refreshed_and_unsubscribe_deletes(monkeypatch) -> None:
    monkeypatch.setattr(realtime, "SIDEBAR_POLL_INTERVAL_SECONDS", 0.005)
    monkeypatch.setattr(realtime, "SIDEBAR_DEMAND_REFRESH_SECONDS", 0.02)
    repository = FakeRepository()
    app = create_app()
    app.state.market_sidebar_repository = repository

    with TestClient(app) as client:
        with client.websocket_connect("/ws/v1/realtime?token=dev-local-token") as ws:
            ws.send_json(
                {
                    "type": "set_sidebar_context",
                    "subscription_id": "right-sidebar",
                    "chart_symbol": "000001.SZ",
                    "chart_epoch": 1,
                    "watchlist_symbols": ["600000.SH"],
                    "channels": [],
                }
            )
            assert ws.receive_json()["type"] == "sidebar_context_set"
            time.sleep(0.06)
            assert len(repository.demand_writes) >= 2
            key = repository.demand_writes[0]
            assert key.startswith("market:sidebar:demand:")
            assert key.endswith(":right-sidebar")
            payload, ttl = repository.demands[key]
            assert payload["chart_symbol"] == "000001.SZ"
            assert payload["watchlist_symbols"] == ["600000.SH"]
            assert payload["updated_at"]
            assert ttl == 30

            ws.send_json({"type": "unsubscribe", "id": "right-sidebar"})
            assert ws.receive_json()["type"] == "unsubscribed"
            assert key not in repository.demands
