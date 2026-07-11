from __future__ import annotations

import json
from typing import Any

from trading_protocol import Bar

BAR_UPDATE_CHANNEL = "market:bar_updates"
CHAN_HEAD_UPDATE_CHANNEL = "chan:head_updates"
CHAN_HEAD_EVENT_SCHEMA_VERSION = "chan-head.v1"
CHAN_HEAD_SEQUENCE_KEY_PREFIX = "chan:head_sequence"


async def publish_bar_update(
    *,
    redis_url: str,
    symbol: str,
    timeframe: str,
    bar: Bar,
) -> bool:
    try:
        import redis.asyncio as redis
    except ImportError:
        return False

    client = redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=0.5,
    )
    try:
        payload: dict[str, Any] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "bar": bar.as_api_dict(),
        }
        await client.publish(
            BAR_UPDATE_CHANNEL,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )
        return True
    except Exception:
        return False
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


async def publish_chan_head_update(
    *,
    redis_url: str,
    symbol: str,
    level: str,
    modes: list[str],
    bar_until: Any,
    run_id: int,
    snapshot_version: str,
) -> bool:
    symbol = symbol.upper().strip()
    snapshot_version = snapshot_version.strip()
    if not symbol or not level or not modes:
        raise ValueError("Chan head event requires symbol, level, and modes")
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id < 1:
        raise ValueError("Chan head event requires a committed run_id")
    if not snapshot_version:
        raise ValueError("Chan head event requires a committed snapshot_version")
    if len(set(modes)) != len(modes) or any(not mode for mode in modes):
        raise ValueError("Chan head event modes must be unique and non-empty")

    try:
        import redis.asyncio as redis
    except ImportError:
        return False

    client = redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=0.5,
    )
    try:
        bar_until_value = (
            bar_until.isoformat() if hasattr(bar_until, "isoformat") else str(bar_until)
        )
        for mode in modes:
            stream = f"{symbol}:{level}:{mode}"
            sequence = int(
                await client.incr(f"{CHAN_HEAD_SEQUENCE_KEY_PREFIX}:{stream}")
            )
            payload: dict[str, Any] = {
                "type": "chan_head_update",
                "schema_version": CHAN_HEAD_EVENT_SCHEMA_VERSION,
                "id": f"{stream}:{run_id}:{snapshot_version}",
                "symbol": symbol,
                "level": level,
                "mode": mode,
                "sequence": sequence,
                "snapshot_version": snapshot_version,
                "run_id": run_id,
                "bar_until": bar_until_value,
            }
            await client.publish(
                CHAN_HEAD_UPDATE_CHANNEL,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            )
        return True
    except Exception:
        return False
    finally:
        try:
            await client.aclose()
        except Exception:
            pass
