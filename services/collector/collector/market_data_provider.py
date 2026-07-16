from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import asdict, is_dataclass
from datetime import date
from typing import Any, Awaitable, Callable, Mapping

from .market_data import MarketDataCoordinator, MarketDataResult, SidebarContext
from .market_data.factory import create_market_data_provider
from .market_data.trading_day_cache import RedisTradingDayCache, TradingCalendar, WeekdayTradingCalendar


REFRESH_CHANNEL = "market:sidebar:refresh_requests"
REFRESH_STREAM = REFRESH_CHANNEL
UPDATE_CHANNEL = "market:sidebar:updates"
WENCAI_CONFIG_KEY = "wencai.config"
_EVENT_TYPES = frozenset({"sidebar_refresh_requested"})
_MAX_SYMBOLS = 500
_PENDING_MIN_IDLE_MS = 60_000
_PENDING_CLAIM_INTERVAL_SECONDS = 60
_PENDING_BATCH_SIZE = 10

logger = logging.getLogger(__name__)


class PostgresWencaiConfigLoader:
    def __init__(self, pool: Any, env: Mapping[str,str] = os.environ) -> None: self._pool,self._env=pool,env
    async def load(self) -> tuple[int, dict[str, str]]:
        row = await self._pool.fetchrow("select value, version from runtime_config where key = $1", WENCAI_CONFIG_KEY)
        if row is None:
            api_key,base_url=self._env.get("IWENCAI_API_KEY",""),self._env.get("IWENCAI_BASE_URL","")
            if not api_key or not base_url:raise ValueError("wencai.config or environment configuration is required")
            keys=[{"label":"default","key":api_key,"enabled":True,"priority":0}]
            return 0,{"IWENCAI_API_KEY":api_key,"IWENCAI_API_KEYS":json.dumps(keys),"IWENCAI_BASE_URL":base_url,"IWENCAI_TIMEOUT_SECONDS":self._env.get("IWENCAI_TIMEOUT_SECONDS","5")}
        value = row["value"] if isinstance(row, Mapping) else row[0]
        version = row["version"] if isinstance(row, Mapping) else row[1]
        if isinstance(value, str): value = json.loads(value)
        if not isinstance(value, Mapping): raise ValueError("wencai.config is invalid")
        api_key, base_url = str(value.get("api_key") or "").strip(), str(value.get("base_url") or "").strip()
        api_keys=value.get("api_keys") or ([{"label":"default","key":api_key,"enabled":True,"priority":0}] if api_key else [])
        if not isinstance(api_keys,list) or not any(isinstance(item,Mapping) and item.get("key") and item.get("enabled",True) for item in api_keys) or not base_url: raise ValueError("wencai.config is incomplete")
        timeout = min(5.0, float(value.get("timeout_seconds") or 5))
        return int(version), {"IWENCAI_API_KEY": api_key or str(api_keys[0]["key"]), "IWENCAI_API_KEYS": json.dumps(api_keys), "IWENCAI_BASE_URL": base_url, "IWENCAI_TIMEOUT_SECONDS": str(timeout)}


def parse_sidebar_event(raw: Any) -> SidebarContext | None:
    if isinstance(raw, bytes): raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str):
        try: raw = json.loads(raw)
        except json.JSONDecodeError: return None
    if not isinstance(raw, Mapping) or raw.get("type") not in _EVENT_TYPES: return None
    chart_symbol = raw.get("chart_symbol")
    symbols = raw.get("watchlist_symbols", [])
    if not isinstance(chart_symbol, str) or not isinstance(symbols, list) or len(symbols) > _MAX_SYMBOLS: return None
    if not chart_symbol or len(chart_symbol) > 32 or any(not isinstance(item, str) or not item or len(item) > 32 for item in symbols): return None
    try:
        epoch, revision = int(raw.get("chart_epoch", 0)), int(raw.get("watchlist_revision", 0))
        return SidebarContext(chart_symbol.upper(), epoch, tuple(item.upper() for item in symbols), revision)
    except (TypeError, ValueError): return None


class MarketDataProviderRuntime:
    """Blocking Redis Pub/Sub consumer. Only received refresh events may call iWencai."""
    def __init__(self, redis: Any, config_loader: Any, *, provider_factory: Callable[..., Any] = create_market_data_provider, env: Mapping[str, str] = os.environ, calendar: TradingCalendar | None = None) -> None:
        self._redis, self._config_loader, self._provider_factory, self._env = redis, config_loader, provider_factory, dict(env)
        self._calendar = calendar or WeekdayTradingCalendar()
        self._version: int | None = None
        self._coordinator: MarketDataCoordinator | None = None

    async def handle_message(self, raw: Any) -> bool:
        context = parse_sidebar_event(raw)
        if context is None: return False
        coordinator = await self._coordinator_for_current_config()
        snapshot = await coordinator.load_context(context)
        # API listeners only need a valid source-tagged notification; cache is the payload authority.
        if coordinator.changed:
            await self._redis.publish(UPDATE_CHANNEL, json.dumps({"source": "iwencai", "chart_symbol": context.chart_symbol, "chart_epoch": context.chart_epoch, "watchlist_revision": context.watchlist_revision}, separators=(",", ":")))
        return snapshot is not None

    async def listen(self, stop: asyncio.Event) -> None:
        from redis.exceptions import TimeoutError as RedisTimeoutError

        group,consumer="iwencai-sidebar","collector"
        try: await self._redis.xgroup_create(REFRESH_STREAM,group,id="0",mkstream=True)
        except Exception: pass
        await self._replay_pending(group, consumer)
        last_claim = 0.0
        while not stop.is_set():
            try:
                if time.monotonic() - last_claim >= _PENDING_CLAIM_INTERVAL_SECONDS:
                    await self._claim_pending(group, consumer)
                    last_claim = time.monotonic()
                entries=await self._redis.xreadgroup(group,consumer,{REFRESH_STREAM:">"},count=1,block=4000)
            except RedisTimeoutError:
                continue
            for _,messages in entries:
                await self._process_stream_messages(group, messages)

    async def _replay_pending(self, group: str, consumer: str) -> None:
        """Retry this consumer's unacknowledged work before accepting new entries."""
        while True:
            try:
                entries = await self._redis.xreadgroup(
                    group, consumer, {REFRESH_STREAM: "0"}, count=_PENDING_BATCH_SIZE
                )
            except Exception:
                logger.exception("Redis Stream pending replay failed", extra={"stream": REFRESH_STREAM, "group": group})
                return
            if not entries:
                return
            acknowledged = 0
            for _, messages in entries:
                acknowledged += await self._process_stream_messages(group, messages)
            if not acknowledged:
                return

    async def _claim_pending(self, group: str, consumer: str) -> None:
        """Claim abandoned entries from another worker after a bounded idle period."""
        start_id = "0-0"
        while True:
            try:
                next_id, messages, _ = await self._redis.xautoclaim(
                    REFRESH_STREAM,
                    group,
                    consumer,
                    _PENDING_MIN_IDLE_MS,
                    start_id,
                    count=_PENDING_BATCH_SIZE,
                )
            except Exception:
                logger.exception("Redis Stream pending claim failed", extra={"stream": REFRESH_STREAM, "group": group})
                return
            await self._process_stream_messages(group, messages)
            if next_id == "0-0" or next_id == start_id:
                return
            start_id = next_id

    async def _process_stream_messages(self, group: str, messages: list[tuple[Any, Mapping[Any, Any]]]) -> int:
        acknowledged = 0
        for message_id, fields in messages:
            raw = _stream_payload(fields)
            try:
                processed = await self.handle_message(raw)
                if not processed:
                    logger.error("Redis Stream message was not processed; leaving pending", extra={"stream": REFRESH_STREAM, "group": group, "message_id": message_id})
                    continue
                await self._redis.xack(REFRESH_STREAM, group, message_id)
                acknowledged += 1
            except Exception:
                logger.exception("Redis Stream message processing failed; leaving pending", extra={"stream": REFRESH_STREAM, "group": group, "message_id": message_id})
        return acknowledged

    async def _coordinator_for_current_config(self) -> MarketDataCoordinator:
        version, config = await self._config_loader.load()
        if self._coordinator is None or self._version != version:
            provider = self._provider_factory(env={**self._env, **config})
            self._coordinator = MarketDataCoordinator(provider, cache=RedisTradingDayCache(self._redis), calendar=self._calendar)
            self._version = version
        return self._coordinator


def _stream_payload(fields: Mapping[Any, Any]) -> Any:
    """Accept redis-py clients configured with bytes or decoded response fields."""
    return fields.get("payload") or fields.get("data") or fields.get(b"payload") or fields.get(b"data")


async def _main() -> None:
    import asyncpg
    import redis.asyncio as redis
    redis_client = redis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"), decode_responses=True)
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(name, stop.set)
        except NotImplementedError: signal.signal(name, lambda *_: loop.call_soon_threadsafe(stop.set))
    try: await MarketDataProviderRuntime(redis_client, PostgresWencaiConfigLoader(pool)).listen(stop)
    finally:
        await redis_client.aclose(); await pool.close()


async def main(argv: list[str] | None = None) -> None:
    if argv: raise ValueError("iwencai-sidebar-events accepts no arguments")
    await _main()


if __name__ == "__main__": asyncio.run(main())
