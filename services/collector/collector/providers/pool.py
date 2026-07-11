from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from collector.models import ProviderHealth
from collector.providers.base import MarketDataProvider
from trading_protocol import Bar, SymbolInfo, normalize_timeframe
from trading_protocol.timeframes import TIMEFRAMES


class BarQualityError(RuntimeError):
    def __init__(self, message: str, *, flags: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.flags = flags or {"error": message}


@dataclass(frozen=True)
class ProviderAttempt:
    source: str
    status: str
    latency_ms: int
    bars: list[Bar] = field(default_factory=list)
    quality_flags: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    winner: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class ProviderReport:
    symbol: str
    timeframe: str
    policy: str
    winning_source: str | None
    attempts: list[ProviderAttempt]


class MarketDataPool(MarketDataProvider):
    """Multi-source provider that writes only one validated canonical result."""

    name = "market_data_pool"

    def __init__(
        self,
        providers: list[MarketDataProvider],
        *,
        policy: str = "primary_failover",
        timeout_seconds: float = 8.0,
        hedged_delay_seconds: float = 0.35,
    ) -> None:
        if not providers:
            raise ValueError("MarketDataPool requires at least one provider")
        self.providers = providers
        self.policy = policy
        self.timeout_seconds = timeout_seconds
        self.hedged_delay_seconds = hedged_delay_seconds
        self.last_report: ProviderReport | None = None
        self._reports: dict[tuple[str, str], ProviderReport] = {}

    async def list_symbols(self) -> list[SymbolInfo]:
        failures: list[str] = []
        for provider in self.providers:
            try:
                symbols = await asyncio.wait_for(provider.list_symbols(), timeout=self.timeout_seconds)
            except Exception as exc:
                failures.append(f"{provider.name}: {str(exc)[:200]}")
                continue
            if symbols:
                return symbols
        raise RuntimeError("No market data source could list symbols: " + "; ".join(failures))

    async def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 300,
    ) -> list[Bar]:
        normalized = normalize_timeframe(timeframe)
        if self.policy == "hedged":
            bars, report = await self._get_bars_hedged(symbol, normalized, start, end, limit)
        else:
            bars, report = await self._get_bars_primary_failover(symbol, normalized, start, end, limit)
        self.last_report = report
        self._reports[(symbol.upper(), normalized)] = report
        return bars

    async def healthcheck(self) -> ProviderHealth:
        checks = await asyncio.gather(
            *(provider.healthcheck() for provider in self.providers),
            return_exceptions=True,
        )
        ok_sources = []
        messages = []
        for provider, result in zip(self.providers, checks):
            if isinstance(result, Exception):
                messages.append(f"{provider.name}: {str(result)[:120]}")
                continue
            messages.append(f"{result.name}: {result.message or result.ok}")
            if result.ok:
                ok_sources.append(result.name)
        return ProviderHealth(
            name=self.name,
            ok=bool(ok_sources),
            message="; ".join(messages),
        )

    async def _get_bars_primary_failover(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> tuple[list[Bar], ProviderReport]:
        attempts: list[ProviderAttempt] = []
        for provider in self.providers:
            attempt = await self._fetch_once(provider, symbol, timeframe, start, end, limit)
            if attempt.status == "success":
                attempts.append(ProviderAttempt(**{**attempt.__dict__, "winner": True, "reason": "primary_success"}))
                report = ProviderReport(symbol, timeframe, self.policy, provider.name, attempts)
                return attempt.bars, report
            attempts.append(attempt)
        report = ProviderReport(symbol, timeframe, self.policy, None, attempts)
        self.last_report = report
        self._reports[(symbol.upper(), timeframe)] = report
        errors = "; ".join(f"{item.source}:{item.error or item.status}" for item in attempts)
        raise RuntimeError(f"No valid market data source for {symbol} {timeframe}: {errors}")

    async def _get_bars_hedged(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> tuple[list[Bar], ProviderReport]:
        tasks = []
        for index, provider in enumerate(self.providers):
            async def run(p=provider, delay=self.hedged_delay_seconds * index):
                if delay > 0:
                    await asyncio.sleep(delay)
                return await self._fetch_once(p, symbol, timeframe, start, end, limit)

            tasks.append(asyncio.create_task(run()))

        attempts = await asyncio.gather(*tasks)
        successful = [item for item in attempts if item.status == "success"]
        if not successful:
            report = ProviderReport(symbol, timeframe, self.policy, None, list(attempts))
            self.last_report = report
            self._reports[(symbol.upper(), timeframe)] = report
            errors = "; ".join(f"{item.source}:{item.error or item.status}" for item in attempts)
            raise RuntimeError(f"No valid market data source for {symbol} {timeframe}: {errors}")

        winner = min(successful, key=lambda item: self._provider_index(item.source))
        final_attempts: list[ProviderAttempt] = []
        for attempt in attempts:
            if attempt.source == winner.source:
                final_attempts.append(ProviderAttempt(**{**attempt.__dict__, "winner": True, "reason": "hedged_winner"}))
            elif attempt.status == "success":
                flags = dict(attempt.quality_flags)
                if not bars_equivalent(winner.bars, attempt.bars):
                    flags["conflict_with_winner"] = winner.source
                final_attempts.append(
                    ProviderAttempt(
                        **{
                            **attempt.__dict__,
                            "quality_flags": flags,
                            "winner": False,
                            "reason": "hedged_loser",
                        }
                    )
                )
            else:
                final_attempts.append(attempt)
        return winner.bars, ProviderReport(symbol, timeframe, self.policy, winner.source, final_attempts)

    async def _fetch_once(
        self,
        provider: MarketDataProvider,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> ProviderAttempt:
        started = time.perf_counter()
        bars: list[Bar] = []
        try:
            bars = await asyncio.wait_for(
                provider.get_bars(symbol, timeframe, start=start, end=end, limit=limit),
                timeout=self.timeout_seconds,
            )
            validated = validate_bars(symbol=symbol, timeframe=timeframe, bars=bars)
            return ProviderAttempt(
                source=provider.name,
                status="success",
                latency_ms=elapsed_ms(started),
                bars=validated,
                quality_flags={"ok": True, "bar_count": len(validated)},
            )
        except BarQualityError as exc:
            return ProviderAttempt(
                source=provider.name,
                status="quality_failed",
                latency_ms=elapsed_ms(started),
                bars=bars,
                quality_flags=exc.flags,
                error=str(exc),
                reason="quality_failed",
            )
        except Exception as exc:
            return ProviderAttempt(
                source=provider.name,
                status="failed",
                latency_ms=elapsed_ms(started),
                error=str(exc)[:1000],
                reason="provider_failed",
            )

    def _provider_index(self, source: str) -> int:
        for index, provider in enumerate(self.providers):
            if provider.name == source:
                return index
        return len(self.providers)

    def report_for(self, symbol: str, timeframe: str) -> ProviderReport | None:
        return self._reports.get((symbol.upper(), normalize_timeframe(timeframe)))


def validate_bars(*, symbol: str, timeframe: str, bars: list[Bar]) -> list[Bar]:
    normalized = normalize_timeframe(timeframe)
    if not bars:
        raise BarQualityError("empty bars", flags={"empty": True})
    last_ts = None
    seen: set[datetime] = set()
    for index, bar in enumerate(bars):
        if bar.symbol.upper() != symbol.upper():
            raise BarQualityError(
                "symbol mismatch",
                flags={"symbol_mismatch": {"expected": symbol, "actual": bar.symbol, "index": index}},
            )
        if normalize_timeframe(bar.timeframe) != normalized:
            raise BarQualityError(
                "timeframe mismatch",
                flags={"timeframe_mismatch": {"expected": normalized, "actual": bar.timeframe, "index": index}},
            )
        if bar.ts in seen:
            raise BarQualityError("duplicate bar timestamp", flags={"duplicate_ts": bar.ts.isoformat()})
        seen.add(bar.ts)
        if last_ts is not None and bar.ts <= last_ts:
            raise BarQualityError("bars are not strictly ordered", flags={"not_ordered_at": index})
        last_ts = bar.ts
        if not _valid_ohlc(bar):
            raise BarQualityError("invalid OHLC", flags={"invalid_ohlc_at": index})
        if int(bar.volume) < 0:
            raise BarQualityError("negative volume", flags={"negative_volume_at": index})
        if not _valid_bar_end(bar.ts, normalized):
            raise BarQualityError(
                "timestamp is not aligned to timeframe grid",
                flags={"unaligned_ts": bar.ts.isoformat(), "timeframe": normalized},
            )
    return bars


def bars_equivalent(left: list[Bar], right: list[Bar]) -> bool:
    if len(left) != len(right):
        return False
    right_by_ts = {bar.ts: bar for bar in right}
    for item in left:
        other = right_by_ts.get(item.ts)
        if other is None:
            return False
        if (
            round(item.open, 3),
            round(item.high, 3),
            round(item.low, 3),
            round(item.close, 3),
            int(item.volume),
        ) != (
            round(other.open, 3),
            round(other.high, 3),
            round(other.low, 3),
            round(other.close, 3),
            int(other.volume),
        ):
            return False
    return True


def elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _valid_ohlc(bar: Bar) -> bool:
    values = (bar.open, bar.high, bar.low, bar.close)
    if not all(math.isfinite(float(value)) for value in values):
        return False
    return bar.high >= max(bar.open, bar.close, bar.low) and bar.low <= min(bar.open, bar.close, bar.high)


def _valid_bar_end(ts: datetime, timeframe: str) -> bool:
    minutes = TIMEFRAMES[timeframe].minutes
    if minutes >= 1440:
        return True
    minute_of_day = ts.hour * 60 + ts.minute
    return minute_of_day % minutes == 0 and ts.second == 0
