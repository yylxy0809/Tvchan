from __future__ import annotations

import json
from typing import Any

from trading_protocol import Bar

BAR_UPDATE_CHANNEL = "market:bar_updates"


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

