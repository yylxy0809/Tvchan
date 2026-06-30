from __future__ import annotations

import httpx
from pydantic import TypeAdapter

from app.models import (
    BarResponse,
    ChanChannelResponse,
    ChanCenterResponse,
    ChanOverlayResponse,
    ChanSignalResponse,
    ChanStrokeResponse,
)


class ChanServiceError(RuntimeError):
    pass


async def analyze_with_chan_service(
    *,
    base_url: str,
    symbol: str,
    chart_timeframe: str,
    levels: list[str],
    modes: list[str],
    requested_bar_count: int,
    bars_by_level: dict[str, list[dict]],
    analysis_bars: list[dict] | None = None,
) -> ChanOverlayResponse:
    base_bars = analysis_bars or bars_by_level.get("5f")
    if not base_bars:
        raise ChanServiceError("Chan service requires 5f base bars for recursive analysis")
    async with httpx.AsyncClient(
        base_url=base_url.rstrip("/"),
        timeout=30.0,
        trust_env=False,
    ) as client:
        payload = {
            "symbol": symbol,
            "timeframe": "5f",
            "chan_levels": levels,
            "modes": modes,
            "bars": [_bar_payload(bar) for bar in base_bars],
        }
        try:
            response = await client.post("/analyze", json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ChanServiceError(f"Chan service analyze failed: {exc}") from exc
        response_payload = response.json()

    strokes_adapter = TypeAdapter(list[ChanStrokeResponse])
    centers_adapter = TypeAdapter(list[ChanCenterResponse])
    signals_adapter = TypeAdapter(list[ChanSignalResponse])
    channels_adapter = TypeAdapter(list[ChanChannelResponse])

    return ChanOverlayResponse(
        symbol=symbol,
        chart_timeframe=chart_timeframe,
        levels=levels,
        modes=modes,
        snapshot_version=str(response_payload.get("snapshot_version") or ""),
        base_timeframe=str(response_payload.get("base_timeframe") or "5f"),
        base_ts_semantics=str(response_payload.get("base_ts_semantics") or "bar_end"),
        engine=_overlay_engine_name([response_payload]),
        requested_bar_count=requested_bar_count,
        bars_by_level={level: len(bars_by_level.get(level, [])) for level in levels},
        strokes=strokes_adapter.validate_python(response_payload.get("strokes", [])),
        segments=strokes_adapter.validate_python(response_payload.get("segments", [])),
        centers=centers_adapter.validate_python(response_payload.get("centers", [])),
        signals=signals_adapter.validate_python(response_payload.get("signals", [])),
        channels=channels_adapter.validate_python(response_payload.get("channels", [])),
    )


def _bar_payload(bar: dict) -> dict:
    parsed = BarResponse.model_validate(bar)
    return {
        "time": parsed.time,
        "open": parsed.open,
        "high": parsed.high,
        "low": parsed.low,
        "close": parsed.close,
        "volume": parsed.volume,
    }


def _overlay_engine_name(responses: list[dict]) -> str:
    engine_names = {
        item.get("engine")
        for item in responses
        if isinstance(item, dict) and item.get("engine")
    }
    if not engine_names:
        return "chan-service:unknown"
    if len(engine_names) == 1:
        return f"chan-service:{next(iter(engine_names))}"
    return f"chan-service:mixed[{','.join(sorted(engine_names))}]"
