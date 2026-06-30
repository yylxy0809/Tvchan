from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChanPoint:
    time: int
    price: float


@dataclass(frozen=True)
class ChanStroke:
    id: str
    level: str
    mode: str
    start: ChanPoint
    end: ChanPoint
    direction: str
    confirmed: bool


@dataclass(frozen=True)
class ChanCenter:
    id: str
    level: str
    mode: str
    start_time: int
    end_time: int
    low: float
    high: float
    confirmed: bool


@dataclass(frozen=True)
class ChanSignal:
    id: str
    level: str
    mode: str
    time: int
    price: float
    signal_type: str
    confirmed: bool


@dataclass(frozen=True)
class ChanPlaceholderResult:
    strokes: list[ChanStroke]
    segments: list[ChanStroke]
    centers: list[ChanCenter]
    signals: list[ChanSignal]


def analyze_chan_placeholder(
    *,
    symbol: str,
    level: str,
    modes: list[str],
    bars: list[dict],
) -> ChanPlaceholderResult:
    pivots = _find_pivots(bars)
    if len(pivots) < 2:
        return ChanPlaceholderResult([], [], [], [])

    strokes: list[ChanStroke] = []
    segments: list[ChanStroke] = []
    centers: list[ChanCenter] = []
    signals: list[ChanSignal] = []

    for mode in modes:
        confirmed = mode == "confirmed"
        prefix = f"{symbol}:{level}:{mode}"
        mode_pivots = pivots[:-1] if not confirmed and len(pivots) > 2 else pivots
        mode_strokes = _build_strokes(prefix, level, mode, confirmed, mode_pivots)
        mode_segments = _build_segments(prefix, level, mode, confirmed, mode_strokes)
        mode_centers = _build_centers(prefix, level, mode, confirmed, mode_segments)
        mode_signals = _build_signals(prefix, level, mode, confirmed, mode_strokes)
        strokes.extend(mode_strokes)
        segments.extend(mode_segments)
        centers.extend(mode_centers)
        signals.extend(mode_signals)

    return ChanPlaceholderResult(strokes, segments, centers, signals)


def _find_pivots(bars: list[dict]) -> list[tuple[str, dict]]:
    if len(bars) < 3:
        return []

    pivots: list[tuple[str, dict]] = []
    for index in range(1, len(bars) - 1):
        prev_bar = bars[index - 1]
        bar = bars[index]
        next_bar = bars[index + 1]
        is_top = bar["high"] >= prev_bar["high"] and bar["high"] >= next_bar["high"]
        is_bottom = bar["low"] <= prev_bar["low"] and bar["low"] <= next_bar["low"]
        if is_top and is_bottom:
            continue
        if is_top:
            _append_pivot(pivots, "top", bar)
        elif is_bottom:
            _append_pivot(pivots, "bottom", bar)

    if not pivots:
        return []
    return _compress_pivots(pivots)


def _append_pivot(pivots: list[tuple[str, dict]], kind: str, bar: dict) -> None:
    if pivots and pivots[-1][0] == kind:
        last_bar = pivots[-1][1]
        if kind == "top" and bar["high"] >= last_bar["high"]:
            pivots[-1] = (kind, bar)
        elif kind == "bottom" and bar["low"] <= last_bar["low"]:
            pivots[-1] = (kind, bar)
        return
    pivots.append((kind, bar))


def _compress_pivots(pivots: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    compressed: list[tuple[str, dict]] = []
    for kind, bar in pivots:
        if compressed and compressed[-1][0] == kind:
            _append_pivot(compressed, kind, bar)
        else:
            compressed.append((kind, bar))
    return compressed


def _build_strokes(
    prefix: str,
    level: str,
    mode: str,
    confirmed: bool,
    pivots: list[tuple[str, dict]],
) -> list[ChanStroke]:
    strokes: list[ChanStroke] = []
    for index in range(1, len(pivots)):
        left_kind, left_bar = pivots[index - 1]
        right_kind, right_bar = pivots[index]
        if left_kind == right_kind:
            continue
        start_price = left_bar["high"] if left_kind == "top" else left_bar["low"]
        end_price = right_bar["high"] if right_kind == "top" else right_bar["low"]
        direction = "down" if left_kind == "top" else "up"
        strokes.append(
            ChanStroke(
                id=f"{prefix}:stroke:{index - 1}",
                level=level,
                mode=mode,
                start=ChanPoint(time=left_bar["time"], price=start_price),
                end=ChanPoint(time=right_bar["time"], price=end_price),
                direction=direction,
                confirmed=confirmed,
            )
        )
    return strokes


def _build_segments(
    prefix: str,
    level: str,
    mode: str,
    confirmed: bool,
    strokes: list[ChanStroke],
) -> list[ChanStroke]:
    if len(strokes) < 3:
        return [
            ChanStroke(
                id=stroke.id.replace(":stroke:", ":segment:"),
                level=stroke.level,
                mode=stroke.mode,
                start=stroke.start,
                end=stroke.end,
                direction=stroke.direction,
                confirmed=stroke.confirmed,
            )
            for stroke in strokes
        ]

    segments: list[ChanStroke] = []
    for index in range(0, len(strokes) - 2, 2):
        first = strokes[index]
        third = strokes[index + 2]
        segments.append(
            ChanStroke(
                id=f"{prefix}:segment:{len(segments)}",
                level=level,
                mode=mode,
                start=first.start,
                end=third.end,
                direction=first.direction,
                confirmed=confirmed,
            )
        )
    return segments


def _build_centers(
    prefix: str,
    level: str,
    mode: str,
    confirmed: bool,
    segments: list[ChanStroke],
) -> list[ChanCenter]:
    centers: list[ChanCenter] = []
    if len(segments) < 3:
        return centers
    for index in range(len(segments) - 2):
        window = segments[index : index + 3]
        low = max(min(item.start.price, item.end.price) for item in window)
        high = min(max(item.start.price, item.end.price) for item in window)
        if low > high:
            continue
        centers.append(
            ChanCenter(
                id=f"{prefix}:center:{len(centers)}",
                level=level,
                mode=mode,
                start_time=window[0].start.time,
                end_time=window[-1].end.time,
                low=low,
                high=high,
                confirmed=confirmed,
            )
        )
    return centers


def _build_signals(
    prefix: str,
    level: str,
    mode: str,
    confirmed: bool,
    strokes: list[ChanStroke],
) -> list[ChanSignal]:
    if not strokes:
        return []
    signals: list[ChanSignal] = []
    for index, stroke in enumerate(strokes):
        signal_type = "B1" if stroke.direction == "up" else "S1"
        signals.append(
            ChanSignal(
                id=f"{prefix}:signal:{index}",
                level=level,
                mode=mode,
                time=stroke.end.time,
                price=stroke.end.price,
                signal_type=signal_type,
                confirmed=confirmed,
            )
        )
    return signals
