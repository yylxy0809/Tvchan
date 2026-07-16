"""Collector-owned adapter for the native-timeframe Module C chan.py engine."""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from trading_protocol import MODULE_C_SEMANTICS

MODULE_C_ENGINE = "module-c:chan.py-native-levels"
CHAN_PY_RECURSION_LIMIT = int(os.getenv("CHAN_PY_RECURSION_LIMIT", "200000"))
DEFAULT_CHAN_CONFIG = {
    "bi_algo": "normal", "bi_strict": True, "bi_fx_check": "strict", "gap_as_kl": True,
    "bi_end_is_peak": True, "bi_allow_sub_peak": False, "seg_algo": "chan",
    "zs_combine": False, "zs_combine_mode": "zs", "bsp1_only_multibi_zs": False,
    "max_bs2_rate": 0.9999, "bs1_peak": True, "bsp2_follow_1": True,
    "bsp3_follow_1": True, "strict_bsp3": False, "bs_type": "1,2,3a,3b",
    "bsp2s_follow_2": False, "max_bsp2s_lv": None, "auto_skip_illegal_sub_lv": True,
}
BSP_TYPE_CN = {"1": "1类", "1p": "1p类", "2": "2类", "2s": "2s类", "3a": "3a类", "3b": "3b类"}


def build_overlay(request: dict[str, Any]) -> dict[str, Any]:
    if sys.getrecursionlimit() < CHAN_PY_RECURSION_LIMIT:
        sys.setrecursionlimit(CHAN_PY_RECURSION_LIMIT)

    vendor_root = _vendor_root(request.get("chan_py_path"))
    if not vendor_root.exists():
        raise RuntimeError(f"chan.py module not found: {vendor_root}")
    if str(vendor_root) not in sys.path:
        sys.path.insert(0, str(vendor_root))

    from ChanConfig import CChanConfig
    from Common.CEnum import DATA_FIELD, KL_TYPE, TREND_TYPE
    from Common.CTime import CTime
    from KLine.KLine_List import CKLine_List
    from KLine.KLine_Unit import CKLine_Unit

    symbol = str(request["symbol"]).upper()
    source_timeframe = str(request.get("timeframe") or "5f")
    analysis_levels = list(request.get("chan_levels") or [source_timeframe])
    include_confirmed, include_predictive = _mode_flags(request.get("modes", ["confirmed", "predictive"]))
    bars_by_level = _bars_by_level(request)

    strokes: list[dict[str, Any]] = []
    segments: list[dict[str, Any]] = []
    centers: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    channels: list[dict[str, Any]] = []

    for level in analysis_levels:
        bars = bars_by_level.get(level) or []
        if not bars:
            raise RuntimeError(f"Module C requires stored {level} bars for {symbol}")
        view = _build_level_view(
            symbol=symbol,
            level=level,
            bars=bars,
            include_confirmed=include_confirmed,
            include_predictive=include_predictive,
            config=CChanConfig(MODULE_C_SEMANTICS.build_chan_config(DEFAULT_CHAN_CONFIG)),
            kline_list_cls=CKLine_List,
            kline_unit_cls=CKLine_Unit,
            data_field=DATA_FIELD,
            ctime_cls=CTime,
            kl_type_enum=KL_TYPE,
            trend_type_enum=TREND_TYPE,
        )
        strokes.extend(view["strokes"])
        segments.extend(view["segments"])
        centers.extend(view["centers"])
        signals.extend(view["signals"])
        channels.extend(view["channels"])

    return {
        "symbol": symbol,
        "timeframe": source_timeframe,
        "snapshot_version": _snapshot_version(symbol, analysis_levels, bars_by_level),
        "base_timeframe": "native",
        "base_ts_semantics": "bar_end",
        "engine": MODULE_C_ENGINE,
        "strokes": strokes,
        "segments": segments,
        "centers": centers,
        "signals": signals,
        "channels": channels,
    }


def _build_level_view(
    *,
    symbol: str,
    level: str,
    bars: list[dict[str, Any]],
    include_confirmed: bool,
    include_predictive: bool,
    config: Any,
    kline_list_cls: Any,
    kline_unit_cls: Any,
    data_field: Any,
    ctime_cls: Any,
    kl_type_enum: Any,
    trend_type_enum: Any,
) -> dict[str, list[dict[str, Any]]]:
    kl_type = _to_kl_type(kl_type_enum, level)
    kl_data = kline_list_cls(kl_type, conf=config)
    for klu in _rows_to_klu_list(bars, kl_type, kline_unit_cls, data_field, ctime_cls):
        kl_data.add_single_klu(klu)
    if len(kl_data.lst) > 0:
        kl_data.cal_seg_and_zs()

    return {
        "strokes": _line_payloads(
            symbol,
            level,
            "stroke",
            _iter_items(getattr(kl_data, "bi_list", [])),
            include_confirmed,
            include_predictive,
        ),
        "segments": _line_payloads(
            symbol,
            level,
            "segment",
            _iter_items(getattr(kl_data, "seg_list", [])),
            include_confirmed,
            include_predictive,
        ),
        "centers": _center_payloads(
            symbol,
            level,
            _iter_items(getattr(kl_data, "zs_list", [])),
            include_confirmed,
            include_predictive,
        ),
        "signals": _signal_payloads(
            symbol,
            level,
            getattr(kl_data, "bs_point_lst", None),
            include_confirmed,
            include_predictive,
        ),
        "channels": _channel_payloads(symbol, level, _iter_klu_items(kl_data), trend_type_enum),
    }


def _bars_by_level(request: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    raw = request.get("bars_by_level")
    if isinstance(raw, dict) and raw:
        return {str(level): list(bars or []) for level, bars in raw.items()}
    timeframe = str(request.get("timeframe") or "5f")
    return {timeframe: list(request.get("bars") or [])}


def _snapshot_version(
    symbol: str,
    levels: list[str],
    bars_by_level: dict[str, list[dict[str, Any]]],
) -> str:
    digest = hashlib.sha256()
    parts = []
    for level in levels:
        bars = bars_by_level.get(level) or []
        first = int(bars[0]["time"]) if bars else 0
        last = int(bars[-1]["time"]) if bars else 0
        parts.append(f"{level}:{first}:{last}:{len(bars)}")
        digest.update(f"{level}|".encode("utf-8"))
        for bar in bars:
            digest.update(
                (
                    f"{int(bar['time'])}|{float(bar['open']):.8f}|{float(bar['high']):.8f}|"
                    f"{float(bar['low']):.8f}|{float(bar['close']):.8f}|"
                    f"{int(bar.get('volume', 0))}|"
                ).encode("utf-8")
            )
    return f"{symbol}:module-c:{'|'.join(parts)}:{digest.hexdigest()[:16]}"


def _vendor_root(path_hint: str | None = None) -> Path:
    if path_hint:
        path = Path(path_hint).expanduser().resolve()
        return path.parent if path.suffix == ".py" and path.is_file() else path
    return Path(__file__).resolve().parents[3] / "work" / "vendor" / "chan.py-main"


def _to_kl_type(enum: Any, level: str) -> Any:
    names = {"5f": "K_5M", "15f": "K_15M", "30f": "K_30M", "1h": "K_60M", "1d": "K_DAY", "1w": "K_WEEK", "1m": "K_MON"}
    if level not in names:
        raise ValueError(f"Unsupported chan.py level: {level}")
    return getattr(enum, names[level])


def _rows_to_klu_list(bars: list[dict[str, Any]], kl_type: Any, cls: Any, fields: Any, ctime: Any) -> list[Any]:
    result = []
    for bar in bars:
        values = [_safe_float(bar.get(k)) for k in ("open", "high", "low", "close")]
        if min(values) <= 0:
            continue
        dt = datetime.fromtimestamp(int(bar["time"]), UTC).astimezone(ZoneInfo("Asia/Shanghai"))
        base_ts = int(bar["time"])
        item_time = ctime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, auto=False)
        item_time._module_c_base_ts = base_ts
        payload = {fields.FIELD_TIME: item_time, fields.FIELD_OPEN: values[0], fields.FIELD_HIGH: values[1], fields.FIELD_LOW: values[2], fields.FIELD_CLOSE: values[3], fields.FIELD_VOLUME: _safe_float(bar.get("volume", 0))}
        for attr, key in (("FIELD_TURNOVER", "amount"), ("FIELD_TURNRATE", "turnrate")):
            field = getattr(fields, attr, None)
            if field is not None:
                payload[field] = _safe_float(bar.get(key, 0))
        item = cls(payload, autofix=True)
        item._module_c_base_ts = base_ts
        if hasattr(item, "set_idx"):
            item.set_idx(len(result))
        item.kl_type = kl_type
        result.append(item)
    return result


def _line_payloads(symbol: str, level: str, part: str, items: Any, confirmed: bool, predictive: bool) -> list[dict[str, Any]]:
    result = []
    for index, item in enumerate(_iter_items(items)):
        mode = "confirmed" if bool(getattr(item, "is_sure", True)) else "predictive"
        if not _include(mode, confirmed, predictive): continue
        direction = "up" if _is_up(item) else "down"; begin, end = item.get_begin_klu(), item.get_end_klu()
        result.append({"id": f"{symbol}:{level}:{mode}:{part}:{getattr(item, 'idx', index)}", "level": level, "mode": mode, "start": _point(begin, begin.low if direction == "up" else begin.high), "end": _point(end, end.high if direction == "up" else end.low), "begin_base_ts": _time(begin), "end_base_ts": _time(end), "begin_base_seq": _seq(begin), "end_base_seq": _seq(end), "direction": direction, "confirmed": mode == "confirmed"})
    return result


def _center_payloads(symbol: str, level: str, items: Any, confirmed: bool, predictive: bool) -> list[dict[str, Any]]:
    result = []
    for index, item in enumerate(_iter_items(items)):
        begin, end = getattr(item, "begin", None), getattr(item, "end", None); mode = "confirmed" if bool(getattr(item, "is_sure", True)) else "predictive"
        if begin is not None and end is not None and _include(mode, confirmed, predictive): result.append({"id": f"{symbol}:{level}:{mode}:center:{index}", "level": level, "mode": mode, "start_time": _time(begin), "end_time": _time(end), "begin_base_ts": _time(begin), "end_base_ts": _time(end), "begin_base_seq": _seq(begin), "end_base_seq": _seq(end), "low": float(item.low), "high": float(item.high), "confirmed": mode == "confirmed"})
    return result


def _signal_payloads(symbol: str, level: str, signals: Any, confirmed: bool, predictive: bool) -> list[dict[str, Any]]:
    iterate = getattr(signals, "bsp_iter", None); result = []
    for index, bsp in enumerate(iterate() if callable(iterate) else []):
        sure = bool(getattr(getattr(bsp, "bi", None), "is_sure", True)); mode = "confirmed" if sure else "predictive"
        if not _include(mode, confirmed, predictive): continue
        side = "buy" if bool(getattr(bsp, "is_buy", False)) else "sell"; klu = bsp.klu; raw_types = list(getattr(bsp, "type", None) or [None])
        for offset, item in enumerate(raw_types):
            raw = str(getattr(item, "value", item)) if item is not None else None; ts = _time(klu)
            result.append({"id": f"{symbol}:{level}:{mode}:signal:{index * 10 + offset}", "level": level, "mode": mode, "time": ts, "base_ts": ts, "base_seq": _seq(klu), "price": float(klu.low if side == "buy" else klu.high), "signal_type": f"{BSP_TYPE_CN.get(raw, raw or '')}{'买' if side == 'buy' else '卖'}", "side": side, "bsp_type": raw, "features": _features(bsp), "confirmed": sure})
    return result


def _channel_payloads(symbol: str, level: str, items: Any, trend: Any) -> list[dict[str, Any]]:
    result = []
    for index, item in enumerate(_iter_klu_items(items)):
        values = getattr(item, "trend", None)
        if not isinstance(values, dict): continue
        upper, lower = values.get(trend.MAX, {}), values.get(trend.MIN, {}); periods = sorted(set(upper) & set(lower))
        if periods:
            period, ts = int(periods[-1]), _time(item); result.append({"id": f"{symbol}:{level}:confirmed:channel:{period}:{index}", "level": level, "mode": "confirmed", "time": ts, "base_ts": ts, "base_seq": _seq(item), "upper": _safe_float(upper[period]), "lower": _safe_float(lower[period]), "period": period, "confirmed": True})
    return result


def _mode_flags(modes: Any) -> tuple[bool, bool]:
    values = {str(value).lower() for value in modes}; return "confirmed" in values, "predictive" in values
def _include(mode: str, confirmed: bool, predictive: bool) -> bool: return (mode == "confirmed" and confirmed) or (mode == "predictive" and predictive)
def _iter_items(items: Any) -> list[Any]: return list(items) if items is not None else []
def _iter_klu_items(items: Any) -> list[Any]:
    iterator = getattr(items, "klu_iter", None); return list(iterator()) if callable(iterator) else _iter_items(getattr(items, "lst", []))
def _time(item: Any) -> int:
    base_ts = getattr(item, "_module_c_base_ts", None)
    if base_ts is not None:
        return int(base_ts)
    value = item if hasattr(item, "ts") else getattr(item, "time", None)
    base_ts = getattr(value, "_module_c_base_ts", None)
    if base_ts is not None:
        return int(base_ts)
    if value is not None and all(hasattr(value, name) for name in ("year", "month", "day", "hour", "minute")):
        return int(datetime(
            int(value.year), int(value.month), int(value.day), int(value.hour), int(value.minute),
            int(getattr(value, "second", 0)), tzinfo=ZoneInfo("Asia/Shanghai"),
        ).timestamp())
    if value is None or not hasattr(value, "ts"): raise AttributeError(f"Unsupported time payload: {type(item)!r}")
    return int(value.ts)
def _seq(item: Any) -> int | None:
    value = getattr(item, "idx", None); return int(value) if value is not None else None
def _point(item: Any, price: float) -> dict[str, Any]:
    ts = _time(item); return {"time": ts, "price": float(price), "base_ts": ts, "base_seq": _seq(item)}
def _safe_float(value: Any, default: float = 0.0) -> float:
    try: return float(default if value is None else value)
    except (TypeError, ValueError): return float(default)
def _is_up(item: Any) -> bool:
    method = getattr(item, "is_up", None); return bool(method()) if callable(method) else str(getattr(item, "dir", "")).endswith("UP")
def _features(bsp: Any) -> dict[str, float | int | str | bool | None]:
    for attr in ("feature_dict", "features"):
        value = getattr(getattr(bsp, "features", None), attr, None)
        if isinstance(value, dict): return {str(k): v for k, v in value.items() if isinstance(v, (str, int, float, bool)) or v is None}
    return {}
