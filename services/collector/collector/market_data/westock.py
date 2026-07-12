from __future__ import annotations

import asyncio
import json
import os
import subprocess
import threading
import uuid
from pathlib import Path
from queue import Empty, Queue
from collections.abc import Iterable
from typing import Protocol

from .contracts import CapitalFlow, Freshness, MarketDataResult, MarketStrength, Profile, Quote


_SYMBOL_PATTERN = __import__("re").compile(r"^(\d{6})\.(SH|SZ|BJ)$", __import__("re").IGNORECASE)
_OPERATIONS = frozenset({"profile", "finance", "asfund", "board"})
_SOURCE = "westock_data"
_PROVIDER_VERSION = "westock-data@1.0.4"
_PROCESS_ENV_KEYS = ("PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP")
_BRIDGE_ENV_KEYS = ("WESTOCK_BRIDGE_MODULE", "WESTOCK_BRIDGE_MODULE_VERSION", "WESTOCK_BRIDGE_SHA256")


class BridgeProtocolError(RuntimeError):
    pass


class BridgeTimeoutError(TimeoutError):
    pass


class BridgeTransport(Protocol):
    def request(self, payload: dict[str, object]) -> dict[str, object]: ...


class PooledBridgeTransport:
    """Bound concurrency with independent persistent bridge processes."""

    def __init__(self, transports: Iterable[BridgeTransport]) -> None:
        self._transports = tuple(transports)
        if not self._transports:
            raise ValueError("at least one WeStock bridge transport is required")
        self._available: Queue[BridgeTransport] = Queue(maxsize=len(self._transports))
        for transport in self._transports:
            self._available.put(transport)

    def request(self, payload: dict[str, object]) -> dict[str, object]:
        transport = self._available.get()
        try:
            return transport.request(payload)
        finally:
            self._available.put(transport)

    def close(self) -> None:
        for transport in self._transports:
            close = getattr(transport, "close", None)
            if close is not None:
                close()


class WeStockNormalizer(Protocol):
    """Schema boundary implemented only after a provider response version is verified."""

    def normalize_quotes(self, symbols: tuple[str, ...], raw: list[object]) -> dict[str, Quote]: ...

    def normalize_profile(self, symbol: str, profile_raw: list[object]) -> Profile: ...

    def normalize_capital_flow(self, symbol: str, raw: list[object]) -> CapitalFlow: ...

    def normalize_market_strength(self, raw: list[object]) -> MarketStrength: ...


def to_westock_symbol(symbol: str) -> str:
    match = _SYMBOL_PATTERN.fullmatch(symbol)
    if match is None:
        raise ValueError(f"invalid canonical A-share symbol: {symbol!r}")
    code, exchange = match.groups()
    return f"{exchange.lower()}{code}"


class NodeJsonlTransport:
    """Serialized transport to one persistent, locally installed Node bridge."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        max_output_bytes: int = 1_048_576,
        node_executable: str = "node",
        bridge_path: str | Path | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        if timeout_seconds <= 0 or max_output_bytes <= 0:
            raise ValueError("bridge limits must be positive")
        path = Path(bridge_path) if bridge_path else Path(__file__).with_name("westock_bridge.mjs")
        child_env = _minimal_child_env(env, max_output_bytes)
        self._timeout = timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._lock = threading.Lock()
        self._responses: Queue[bytes | BaseException] = Queue(maxsize=1)
        self._process = subprocess.Popen(
            [node_executable, str(path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=child_env,
            shell=False,
        )
        threading.Thread(target=self._read_stdout, daemon=True).start()

    def _read_stdout(self) -> None:
        assert self._process.stdout is not None
        while True:
            try:
                line = self._process.stdout.readline(self._max_output_bytes + 1)
                if not line:
                    self._responses.put(BridgeProtocolError("WeStock bridge exited"))
                    return
                if len(line) > self._max_output_bytes or not line.endswith(b"\n"):
                    self._responses.put(BridgeProtocolError("WeStock bridge output exceeded limit"))
                    self.close()
                    return
                self._responses.put(line)
            except BaseException as exc:  # propagate reader failures to the caller
                self._responses.put(exc)
                return

    def request(self, payload: dict[str, object]) -> dict[str, object]:
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"
        with self._lock:
            if self._process.poll() is not None:
                raise BridgeProtocolError("WeStock bridge is not running")
            assert self._process.stdin is not None
            self._process.stdin.write(encoded)
            self._process.stdin.flush()
            try:
                response = self._responses.get(timeout=self._timeout)
            except Empty as exc:
                self.close()
                raise BridgeTimeoutError("WeStock bridge request timed out") from exc
            if isinstance(response, BaseException):
                raise response
            try:
                parsed = json.loads(response)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BridgeProtocolError("WeStock bridge returned invalid JSON") from exc
            if not isinstance(parsed, dict):
                raise BridgeProtocolError("WeStock bridge response must be an object")
            return parsed

    def close(self) -> None:
        if self._process.poll() is None:
            self._process.kill()
        if self._process.stdin is not None:
            self._process.stdin.close()

    def __enter__(self) -> "NodeJsonlTransport":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class WeStockAdapter:
    def __init__(
        self,
        transport: BridgeTransport | None = None,
        *,
        batch_size: int = 100,
        normalizer: WeStockNormalizer | None = None,
        max_transport_concurrency: int = 16,
    ) -> None:
        if batch_size <= 0 or max_transport_concurrency <= 0:
            raise ValueError("WeStock limits must be positive")
        self._transport = transport or NodeJsonlTransport()
        self._batch_size = batch_size
        self._normalizer = normalizer
        self._transport_limiter = asyncio.Semaphore(max_transport_concurrency)

    def quote(self, symbols: list[str]) -> list[object]:
        raise BridgeProtocolError("quote is unsupported by westock-data-clawhub@1.0.4")

    def profile(self, symbols: list[str]) -> list[object]:
        return self._dispatch("profile", symbols)

    def finance(self, symbols: list[str]) -> list[object]:
        return self._dispatch("finance", symbols)

    def asfund(self, symbols: list[str]) -> list[object]:
        return self._dispatch("asfund", symbols)

    def board(self, _symbols: list[str] | None = None) -> list[object]:
        return self._dispatch("board", [])

    async def get_quotes(self, symbols: Iterable[str]) -> dict[str, MarketDataResult[Quote]]:
        canonical = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        if not canonical:
            return {}
        return self._unavailable_quotes(canonical, "quote is unsupported by westock-data-clawhub@1.0.4")

    def close(self) -> None:
        close = getattr(self._transport, "close", None)
        if close is not None:
            close()

    async def get_profile(self, symbol: str) -> MarketDataResult[Profile]:
        canonical = symbol.upper()
        if self._normalizer is None:
            return self._unavailable("WeStock response schema is not verified")
        try:
            profile_raw = await self._request(self.profile, [canonical])
            # The CLI finance command returns financial statements whose schema varies
            # by enterprise type. It is not a source for the profile valuation fields.
            value = self._normalizer.normalize_profile(canonical, profile_raw)
            if not isinstance(value, Profile) or value.symbol != canonical:
                raise TypeError("normalizer returned an invalid Profile")
            return self._available(value)
        except Exception:
            return self._unavailable("WeStock profile normalization failed")

    async def get_capital_flow(self, symbol: str) -> MarketDataResult[CapitalFlow]:
        canonical = symbol.upper()
        if self._normalizer is None:
            return self._unavailable("WeStock response schema is not verified")
        try:
            raw = await self._request(self.asfund, [canonical])
            value = self._normalizer.normalize_capital_flow(canonical, raw)
            if not isinstance(value, CapitalFlow) or value.symbol != canonical:
                raise TypeError("normalizer returned an invalid CapitalFlow")
            return self._available(value)
        except Exception:
            return self._unavailable("WeStock capital-flow normalization failed")

    async def get_market_strength(self) -> MarketDataResult[MarketStrength]:
        if self._normalizer is None:
            return self._unavailable("WeStock response schema is not verified")
        try:
            value = await self._request(self.board, [])
            normalized = self._normalizer.normalize_market_strength(value)
            if not isinstance(normalized, MarketStrength):
                raise TypeError("normalizer returned an invalid MarketStrength")
            return self._available(normalized)
        except Exception:
            return self._unavailable("WeStock market-strength normalization failed")

    @staticmethod
    def _available(value):
        return MarketDataResult.available(
            value,
            source=_SOURCE,
            provider_version=_PROVIDER_VERSION,
            freshness=Freshness.LIVE,
        )

    @staticmethod
    def _unavailable(error: str):
        return MarketDataResult.unavailable(source=_SOURCE, error=error)

    @classmethod
    def _unavailable_quotes(
        cls, symbols: tuple[str, ...], error: str
    ) -> dict[str, MarketDataResult[Quote]]:
        return {symbol: cls._unavailable(error) for symbol in symbols}

    def _dispatch(self, operation: str, symbols: list[str]) -> list[object]:
        if operation not in _OPERATIONS:
            raise ValueError(f"unsupported WeStock operation: {operation}")
        mapped = list(dict.fromkeys(to_westock_symbol(symbol) for symbol in symbols))
        if operation == "board":
            return self._request_batch(operation, [])
        results: list[object] = []
        for offset in range(0, len(mapped), self._batch_size):
            results.extend(self._request_batch(operation, mapped[offset : offset + self._batch_size]))
        return results

    def _request_batch(self, operation: str, symbols: list[str]) -> list[object]:
        request_id = uuid.uuid4().hex
        response = self._transport.request(
            {"id": request_id, "operation": operation, "symbols": symbols}
        )
        if response.get("id") != request_id:
            raise BridgeProtocolError("WeStock bridge response id mismatch")
        if response.get("ok") is not True:
            error = str(response.get("error", "provider request failed"))[:512]
            raise BridgeProtocolError(f"WeStock provider error: {error}")
        data = response.get("data")
        if not isinstance(data, list):
            raise BridgeProtocolError("WeStock bridge data must be a list")
        return data

    async def _request(self, operation, symbols: list[str]) -> list[object]:
        async with self._transport_limiter:
            return await asyncio.to_thread(operation, symbols)


def _minimal_child_env(env: dict[str, str] | None, max_output_bytes: int) -> dict[str, str]:
    child_env = {key: os.environ[key] for key in _PROCESS_ENV_KEYS if key in os.environ}
    if env:
        child_env.update({key: env[key] for key in _BRIDGE_ENV_KEYS if key in env})
    child_env["WESTOCK_MAX_OUTPUT_BYTES"] = str(max_output_bytes)
    return child_env
