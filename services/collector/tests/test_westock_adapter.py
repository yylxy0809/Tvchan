from __future__ import annotations

import asyncio
import hashlib
import threading
import time

import pytest

from collector.market_data.contracts import CapitalFlow, Freshness, Profile, Quote
from collector.market_data.westock import (
    BridgeProtocolError,
    NodeJsonlTransport,
    PooledBridgeTransport,
    WeStockAdapter,
    _minimal_child_env,
    to_westock_symbol,
)


class RecordingTransport:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def request(self, payload: dict[str, object]) -> dict[str, object]:
        self.requests.append(payload)
        return {"id": payload["id"], "ok": True, "data": payload["symbols"]}


class StrictNormalizer:
    def normalize_quotes(self, symbols, raw):
        assert raw == ["sh600000"]
        return {symbols[0]: Quote(symbol=symbols[0], price=10.5)}

    def normalize_profile(self, symbol, profile_raw):
        assert profile_raw == ["sh600000"]
        return Profile(symbol=symbol, name="浦发银行")

    def normalize_capital_flow(self, symbol, raw):
        assert raw == ["sh600000"]
        return CapitalFlow(symbol=symbol, main_net_inflow=100.0)


def test_pooled_bridge_transport_distributes_requests_and_closes_all() -> None:
    class Transport:
        def __init__(self, name): self.name, self.closed = name, False
        def request(self, payload): return {"transport": self.name, "payload": payload}
        def close(self): self.closed = True

    first, second = Transport("first"), Transport("second")
    pool = PooledBridgeTransport((first, second))

    assert pool.request({"id": 1})["transport"] == "first"
    assert pool.request({"id": 2})["transport"] == "second"
    pool.close()
    assert first.closed and second.closed


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("600000.SH", "sh600000"),
        ("000001.SZ", "sz000001"),
        ("688001.SH", "sh688001"),
        ("430047.BJ", "bj430047"),
        ("600000.sh", "sh600000"),
    ],
)
def test_to_westock_symbol_uses_explicit_exchange_suffix(symbol: str, expected: str) -> None:
    assert to_westock_symbol(symbol) == expected


@pytest.mark.parametrize("symbol", ["600000", "sh600000", "600000.HK", "1;rm -rf /"])
def test_to_westock_symbol_rejects_noncanonical_or_unsafe_symbols(symbol: str) -> None:
    with pytest.raises(ValueError):
        to_westock_symbol(symbol)


@pytest.mark.parametrize("operation", ["profile", "finance", "asfund"])
def test_adapter_deduplicates_and_batches_each_operation(operation: str) -> None:
    transport = RecordingTransport()
    adapter = WeStockAdapter(transport=transport, batch_size=2)

    result = getattr(adapter, operation)(["600000.SH", "000001.SZ", "600000.SH", "430047.BJ"])

    assert result == ["sh600000", "sz000001", "bj430047"]
    assert [request["symbols"] for request in transport.requests] == [
        ["sh600000", "sz000001"],
        ["bj430047"],
    ]
    assert {request["operation"] for request in transport.requests} == {operation}
    assert len({request["id"] for request in transport.requests}) == 2


def test_quote_is_explicitly_unsupported_without_calling_the_transport() -> None:
    transport = RecordingTransport()
    adapter = WeStockAdapter(transport=transport)

    with pytest.raises(BridgeProtocolError, match="quote is unsupported"):
        adapter.quote(["600000.SH"])

    assert transport.requests == []


def test_board_dispatches_once_without_symbols() -> None:
    transport = RecordingTransport()
    adapter = WeStockAdapter(transport=transport)

    assert adapter.board() == []
    assert len(transport.requests) == 1
    assert transport.requests[0]["operation"] == "board"
    assert transport.requests[0]["symbols"] == []


def test_adapter_rejects_mismatched_bridge_response_id() -> None:
    class BadTransport:
        def request(self, payload: dict[str, object]) -> dict[str, object]:
            return {"id": "another-request", "ok": True, "data": []}

    adapter = WeStockAdapter(transport=BadTransport())

    with pytest.raises(BridgeProtocolError, match="response id"):
        adapter.profile(["600000.SH"])


def test_adapter_surfaces_bounded_provider_error_without_raw_payload() -> None:
    class FailedTransport:
        def request(self, payload: dict[str, object]) -> dict[str, object]:
            return {"id": payload["id"], "ok": False, "error": "x" * 10_000}

    adapter = WeStockAdapter(transport=FailedTransport())

    with pytest.raises(BridgeProtocolError) as exc_info:
        adapter.profile(["600000.SH"])

    assert len(str(exc_info.value)) < 600


def test_node_jsonl_transport_uses_verified_injected_module(tmp_path) -> None:
    module = tmp_path / "provider.mjs"
    module.write_text(
        "export async function handle(request) { return request.symbols.map(symbol => ({symbol})); }\n",
        encoding="utf-8",
    )
    digest = hashlib.sha256(module.read_bytes()).hexdigest()

    with NodeJsonlTransport(
        timeout_seconds=2,
        env={
            "WESTOCK_BRIDGE_MODULE": str(module),
            "WESTOCK_BRIDGE_MODULE_VERSION": "westock-data@1.0.4",
            "WESTOCK_BRIDGE_SHA256": digest,
        },
    ) as transport:
        adapter = WeStockAdapter(transport=transport)
        assert adapter.profile(["600000.SH"]) == [{"symbol": "sh600000"}]


def test_node_jsonl_transport_rejects_unverified_module(tmp_path) -> None:
    module = tmp_path / "provider.mjs"
    module.write_text("export async function handle() { return []; }\n", encoding="utf-8")

    with NodeJsonlTransport(
        timeout_seconds=2,
        env={
            "WESTOCK_BRIDGE_MODULE": str(module),
            "WESTOCK_BRIDGE_MODULE_VERSION": "westock-data@1.0.4",
            "WESTOCK_BRIDGE_SHA256": "0" * 64,
        },
    ) as transport:
        with pytest.raises(BridgeProtocolError, match="exited|not running"):
            WeStockAdapter(transport=transport).profile(["600000.SH"])


def test_node_transport_environment_is_allowlisted(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    monkeypatch.setenv("IWENCAI_API_KEY", "api-secret")
    monkeypatch.setenv("PATH", "node-path")

    child_env = _minimal_child_env(
        {
            "WESTOCK_BRIDGE_MODULE": "/verified/provider.mjs",
            "WESTOCK_BRIDGE_MODULE_VERSION": "westock-data@1.0.4",
            "WESTOCK_BRIDGE_SHA256": "a" * 64,
            "DATABASE_URL": "postgresql://other-secret",
            "IWENCAI_API_KEY": "other-api-secret",
        },
        2048,
    )

    assert child_env["PATH"] == "node-path"
    assert child_env["WESTOCK_MAX_OUTPUT_BYTES"] == "2048"
    assert "DATABASE_URL" not in child_env
    assert "IWENCAI_API_KEY" not in child_env


def test_profile_internal_requests_respect_transport_concurrency_limit() -> None:
    class BlockingTransport:
        active = 0
        maximum = 0
        lock = threading.Lock()

        def request(self, payload):
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
            try:
                time.sleep(0.01)
                return {"id": payload["id"], "ok": True, "data": payload["symbols"]}
            finally:
                with self.lock:
                    self.active -= 1

    transport = BlockingTransport()
    adapter = WeStockAdapter(transport=transport, normalizer=StrictNormalizer(), max_transport_concurrency=2)

    async def exercise():
        return await asyncio.gather(*(adapter.get_profile("600000.SH") for _ in range(4)))

    results = asyncio.run(exercise())

    assert all(result.value is not None for result in results)
    assert transport.maximum <= 2


def test_standard_source_interface_returns_only_contract_dtos() -> None:
    adapter = WeStockAdapter(transport=RecordingTransport(), normalizer=StrictNormalizer())

    async def exercise():
        return (
            await adapter.get_quotes(["600000.SH"]),
            await adapter.get_profile("600000.SH"),
            await adapter.get_capital_flow("600000.SH"),
        )

    quotes, profile, capital_flow = asyncio.run(exercise())

    assert quotes["600000.SH"].value is None
    assert profile.value == Profile(symbol="600000.SH", name="浦发银行")
    assert capital_flow.value == CapitalFlow(symbol="600000.SH", main_net_inflow=100.0)
    assert all(result.metadata.source == "westock_data" for result in [*quotes.values(), profile, capital_flow])


def test_standard_source_interface_converts_errors_to_unavailable() -> None:
    adapter = WeStockAdapter(transport=RecordingTransport(), normalizer=StrictNormalizer())

    async def exercise():
        return (await adapter.get_quotes(["600000.SH"]))["600000.SH"], await adapter.get_market_strength()

    result, strength = asyncio.run(exercise())

    assert result.value is None
    assert result.metadata.freshness is Freshness.UNAVAILABLE
    assert result.error == "quote is unsupported by westock-data-clawhub@1.0.4"
    assert strength.value is None
    assert strength.metadata.freshness is Freshness.UNAVAILABLE
