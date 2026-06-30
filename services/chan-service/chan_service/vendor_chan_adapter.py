from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


MODULE_B_ENGINE = "module-b:chan.py"
MODULE_B_RECURSION_LIMIT = int(os.getenv("CHAN_PY_RECURSION_LIMIT", "200000"))

# Passed directly into Vespa314/chan.py. Keep this adapter thin: no local
# reimplementation of inclusion, fractal, stroke, segment, zs, or BSP logic.
DEFAULT_CHAN_CONFIG = {
    "bi_algo": "normal",
    "bi_strict": True,
    "bi_fx_check": "strict",
    "bi_end_is_peak": True,
    "bi_allow_sub_peak": True,
    "seg_algo": "chan",
    "left_seg_method": "peak",
    "zs_combine": True,
    "zs_combine_mode": "zs",
    "zs_algo": "normal",
    "one_bi_zs": False,
    "divergence_rate": 99999.0,
    "min_zs_cnt": 0,
    "bsp1_only_multibi_zs": True,
    "max_bs2_rate": 0.9999,
    "macd_algo": "peak",
    "bs1_peak": True,
    "bs_type": "1,1p,2,2s,3a,3b",
    "bsp2_follow_1": True,
    "bsp3_follow_1": True,
    "bsp3_peak": False,
    "trigger_step": False,
    "skip_step": 0,
    "trend_metrics": [20],
    "print_warning": False,
    "print_err_time": False,
    "kl_data_check": False,
    "auto_skip_illegal_sub_lv": True,
}

BSP_TYPE_CN = {
    "1": "1\u7c7b",
    "1p": "1p\u7c7b",
    "2": "2\u7c7b",
    "2s": "2s\u7c7b",
    "3a": "3a\u7c7b",
    "3b": "3b\u7c7b",
}


def build_overlay(request: dict[str, Any]) -> dict[str, Any]:
    if sys.getrecursionlimit() < MODULE_B_RECURSION_LIMIT:
        sys.setrecursionlimit(MODULE_B_RECURSION_LIMIT)

    vendor_root = _vendor_root(request.get("chan_py_path"))
    if not vendor_root.exists():
        raise RuntimeError(f"chan.py module not found: {vendor_root}")
    if str(vendor_root) not in sys.path:
        sys.path.insert(0, str(vendor_root))

    from ChanConfig import CChanConfig
    from Common.CEnum import DATA_FIELD, KL_TYPE, SEG_TYPE, TREND_TYPE
    from Common.CTime import CTime
    from BuySellPoint.BSPointList import CBSPointList
    from KLine.KLine_List import CKLine_List, cal_seg, get_seglist_instance, update_zs_in_seg
    from KLine.KLine_Unit import CKLine_Unit
    from ZS.ZSList import CZSList

    symbol = str(request["symbol"]).upper()
    source_timeframe = str(request.get("timeframe") or "5f")
    source_kl_type = _to_kl_type(KL_TYPE, source_timeframe)
    analysis_levels = list(request.get("chan_levels") or ["5f", "30f", "1d"])
    include_confirmed, include_predictive = _mode_flags(request.get("modes", ["confirmed", "predictive"]))

    config = CChanConfig(DEFAULT_CHAN_CONFIG.copy())
    kl_data = CKLine_List(source_kl_type, conf=config)
    for klu in _rows_to_klu_list(request["bars"], source_kl_type, CKLine_Unit, DATA_FIELD, CTime):
        kl_data.add_single_klu(klu)
    if len(kl_data.lst) > 0:
        kl_data.cal_seg_and_zs()
    daily_layer = _build_daily_layer(
        kl_data,
        config,
        SEG_TYPE,
        get_seglist_instance,
        cal_seg,
        update_zs_in_seg,
        CZSList,
        CBSPointList,
    )

    level_views = {
        "5f": {
            "strokes": _line_payloads(
                symbol,
                "5f",
                "stroke",
                _iter_items(getattr(kl_data, "bi_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "segments": _line_payloads(
                symbol,
                "5f",
                "segment",
                _iter_items(getattr(kl_data, "seg_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "centers": _center_payloads(
                symbol,
                "5f",
                _iter_items(getattr(kl_data, "zs_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "signals": _signal_payloads(
                symbol,
                "5f",
                getattr(kl_data, "bs_point_lst", None),
                include_confirmed,
                include_predictive,
            ),
            "channels": _channel_payloads(symbol, "5f", _iter_klu_items(kl_data), TREND_TYPE),
        },
        "30f": {
            "strokes": _line_payloads(
                symbol,
                "30f",
                "stroke",
                _iter_items(getattr(kl_data, "seg_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "segments": _line_payloads(
                symbol,
                "30f",
                "segment",
                _iter_items(getattr(kl_data, "segseg_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "centers": _center_payloads(
                symbol,
                "30f",
                _iter_items(getattr(kl_data, "segzs_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "signals": _signal_payloads(
                symbol,
                "30f",
                getattr(kl_data, "seg_bs_point_lst", None),
                include_confirmed,
                include_predictive,
            ),
            "channels": [],
        },
        "1d": {
            "strokes": _line_payloads(
                symbol,
                "1d",
                "stroke",
                _iter_items(getattr(kl_data, "segseg_list", [])),
                include_confirmed,
                include_predictive,
            ),
            "segments": _line_payloads(
                symbol,
                "1d",
                "segment",
                _iter_items(daily_layer["segments"]),
                include_confirmed,
                include_predictive,
            ),
            "centers": _center_payloads(
                symbol,
                "1d",
                _iter_items(daily_layer["centers"]),
                include_confirmed,
                include_predictive,
            ),
            "signals": _signal_payloads(
                symbol,
                "1d",
                daily_layer["signals"],
                include_confirmed,
                include_predictive,
            ),
            "channels": [],
        },
    }

    strokes: list[dict[str, Any]] = []
    segments: list[dict[str, Any]] = []
    centers: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    channels: list[dict[str, Any]] = []
    for level in analysis_levels:
        view = level_views.get(level)
        if view is None:
            continue
        strokes.extend(view["strokes"])
        segments.extend(view["segments"])
        centers.extend(view["centers"])
        signals.extend(view["signals"])
        channels.extend(view["channels"])

    return {
        "symbol": symbol,
        "timeframe": source_timeframe,
        "snapshot_version": _snapshot_version(symbol, request["bars"]),
        "base_timeframe": "5f",
        "base_ts_semantics": "bar_end",
        "engine": MODULE_B_ENGINE,
        "strokes": strokes,
        "segments": segments,
        "centers": centers,
        "signals": signals,
        "channels": channels,
    }


def _build_daily_layer(
    kl_data: Any,
    config: Any,
    seg_type_enum: Any,
    get_seglist_instance_fn: Any,
    cal_seg_fn: Any,
    update_zs_in_seg_fn: Any,
    zs_list_cls: Any,
    bs_point_list_cls: Any,
) -> dict[str, Any]:
    _attach_recursive_segment_boundaries(getattr(kl_data, "seg_list", []))
    source_strokes = getattr(kl_data, "segseg_list", [])
    _attach_recursive_segment_boundaries(source_strokes)

    segments = get_seglist_instance_fn(seg_config=config.seg_conf, lv=seg_type_enum.SEG)
    centers = zs_list_cls(zs_config=config.zs_conf)
    signals = bs_point_list_cls(bs_point_config=config.seg_bs_point_conf)
    if len(source_strokes) == 0:
        return {"segments": segments, "centers": centers, "signals": signals}

    cal_seg_fn(source_strokes, segments, -1)
    _attach_recursive_segment_boundaries(segments)
    centers.cal_bi_zs(source_strokes, segments)
    update_zs_in_seg_fn(source_strokes, segments, centers)
    signals.cal(source_strokes, segments)
    return {"segments": segments, "centers": centers, "signals": signals}


def _attach_recursive_segment_boundaries(items: Iterable[Any]) -> None:
    for item in _iter_items(items):
        start = getattr(item, "start_bi", None)
        end = getattr(item, "end_bi", None)
        if start is None or end is None:
            continue
        if not hasattr(item, "begin_klc"):
            begin_klc = getattr(start, "begin_klc", None)
            if begin_klc is None and hasattr(start, "get_begin_klu"):
                begin_klc = start.get_begin_klu()
            item.begin_klc = begin_klc
        if not hasattr(item, "end_klc"):
            end_klc = getattr(end, "end_klc", None)
            if end_klc is None and hasattr(end, "get_end_klu"):
                end_klc = end.get_end_klu()
            item.end_klc = end_klc


def _rows_to_klu_list(
    bars: list[dict[str, Any]],
    kl_type: Any,
    kline_unit_cls: Any,
    data_field: Any,
    ctime_cls: Any,
) -> list[Any]:
    turnover_field = getattr(data_field, "FIELD_TURNOVER", None)
    turnrate_field = getattr(data_field, "FIELD_TURNRATE", None)
    result = []
    for bar in bars:
        open_price = _safe_float(bar.get("open"))
        high_price = _safe_float(bar.get("high"))
        low_price = _safe_float(bar.get("low"))
        close_price = _safe_float(bar.get("close"))
        if min(open_price, high_price, low_price, close_price) <= 0:
            continue
        payload = {
            data_field.FIELD_TIME: _to_ctime(int(bar["time"]), ctime_cls),
            data_field.FIELD_OPEN: open_price,
            data_field.FIELD_HIGH: high_price,
            data_field.FIELD_LOW: low_price,
            data_field.FIELD_CLOSE: close_price,
            data_field.FIELD_VOLUME: _safe_float(bar.get("volume", 0)),
        }
        if turnover_field is not None:
            payload[turnover_field] = _safe_float(bar.get("amount", 0))
        if turnrate_field is not None:
            payload[turnrate_field] = _safe_float(bar.get("turnrate", 0))
        klu = kline_unit_cls(payload, autofix=True)
        if hasattr(klu, "set_idx"):
            klu.set_idx(len(result))
        klu.kl_type = kl_type
        result.append(klu)
    return result


def _line_payloads(
    symbol: str,
    level: str,
    part: str,
    items: Iterable[Any],
    include_confirmed: bool,
    include_predictive: bool,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        mode = _mode_from_sure_flag(bool(getattr(item, "is_sure", True)))
        if not _include_mode(mode, include_confirmed, include_predictive):
            continue
        direction = "up" if _is_up(item) else "down"
        begin_klu = item.get_begin_klu()
        end_klu = item.get_end_klu()
        start_price = float(begin_klu.low if direction == "up" else begin_klu.high)
        end_price = float(end_klu.high if direction == "up" else end_klu.low)
        item_idx = getattr(item, "idx", index)
        payloads.append(
            {
                "id": f"{symbol}:{level}:{mode}:{part}:{item_idx}",
                "level": level,
                "mode": mode,
                "start": _point_payload(begin_klu, start_price),
                "end": _point_payload(end_klu, end_price),
                "begin_base_ts": _time_payload(begin_klu),
                "end_base_ts": _time_payload(end_klu),
                "begin_base_seq": _seq_payload(begin_klu),
                "end_base_seq": _seq_payload(end_klu),
                "direction": direction,
                "confirmed": mode == "confirmed",
            }
        )
    return payloads


def _center_payloads(
    symbol: str,
    level: str,
    items: Iterable[Any],
    include_confirmed: bool,
    include_predictive: bool,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for index, zs in enumerate(items):
        begin = getattr(zs, "begin", None)
        end = getattr(zs, "end", None)
        if begin is None or end is None:
            continue
        mode = _mode_from_sure_flag(bool(getattr(zs, "is_sure", True)))
        if not _include_mode(mode, include_confirmed, include_predictive):
            continue
        payloads.append(
            {
                "id": f"{symbol}:{level}:{mode}:center:{index}",
                "level": level,
                "mode": mode,
                "start_time": _time_payload(begin),
                "end_time": _time_payload(end),
                "begin_base_ts": _time_payload(begin),
                "end_base_ts": _time_payload(end),
                "begin_base_seq": _seq_payload(begin),
                "end_base_seq": _seq_payload(end),
                "low": float(zs.low),
                "high": float(zs.high),
                "confirmed": mode == "confirmed",
            }
        )
    return payloads


def _signal_payloads(
    symbol: str,
    level: str,
    signal_list: Any,
    include_confirmed: bool,
    include_predictive: bool,
) -> list[dict[str, Any]]:
    if signal_list is None:
        return []
    iter_fn = getattr(signal_list, "bsp_iter", None)
    source = list(iter_fn()) if callable(iter_fn) else []
    payloads: list[dict[str, Any]] = []
    for index, bsp in enumerate(source):
        is_sure = bool(getattr(getattr(bsp, "bi", None), "is_sure", True))
        mode = _mode_from_sure_flag(is_sure)
        if not _include_mode(mode, include_confirmed, include_predictive):
            continue
        is_buy = bool(getattr(bsp, "is_buy", False))
        types = list(getattr(bsp, "type", None) or [])
        if not types:
            payloads.append(
                _signal_payload(
                    symbol,
                    level,
                    mode,
                    index,
                    bsp,
                    "\u4e70" if is_buy else "\u5356",
                    None,
                    is_buy,
                    is_sure,
                )
            )
            continue
        for type_index, bsp_type in enumerate(types):
            raw_value = str(getattr(bsp_type, "value", str(bsp_type)))
            side_label = "\u4e70" if is_buy else "\u5356"
            label = f"{BSP_TYPE_CN.get(raw_value, raw_value)}{side_label}"
            payloads.append(
                _signal_payload(
                    symbol,
                    level,
                    mode,
                    index * 10 + type_index,
                    bsp,
                    label,
                    raw_value,
                    is_buy,
                    is_sure,
                )
            )
    return payloads


def _signal_payload(
    symbol: str,
    level: str,
    mode: str,
    index: int,
    bsp: Any,
    label: str,
    raw_type: str | None,
    is_buy: bool,
    is_sure: bool,
) -> dict[str, Any]:
    klu = bsp.klu
    base_ts = _time_payload(klu)
    return {
        "id": f"{symbol}:{level}:{mode}:signal:{index}",
        "level": level,
        "mode": mode,
        "time": base_ts,
        "base_ts": base_ts,
        "base_seq": _seq_payload(klu),
        "price": float(klu.low if is_buy else klu.high),
        "signal_type": label,
        "side": "buy" if is_buy else "sell",
        "bsp_type": raw_type or label,
        "features": _features_payload(bsp),
        "confirmed": is_sure,
    }


def _channel_payloads(
    symbol: str,
    level: str,
    klu_items: Iterable[Any],
    trend_type_enum: Any,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    max_key = getattr(trend_type_enum, "MAX")
    min_key = getattr(trend_type_enum, "MIN")
    for index, klu in enumerate(klu_items):
        trend = getattr(klu, "trend", None)
        if not isinstance(trend, dict):
            continue
        upper_by_period = trend.get(max_key, {})
        lower_by_period = trend.get(min_key, {})
        if not upper_by_period or not lower_by_period:
            continue
        common_periods = sorted(set(upper_by_period.keys()) & set(lower_by_period.keys()))
        if not common_periods:
            continue
        period = int(common_periods[-1])
        base_ts = _time_payload(klu)
        payloads.append(
            {
                "id": f"{symbol}:{level}:confirmed:channel:{period}:{index}",
                "level": level,
                "mode": "confirmed",
                "time": base_ts,
                "base_ts": base_ts,
                "base_seq": _seq_payload(klu),
                "upper": _safe_float(upper_by_period.get(period)),
                "lower": _safe_float(lower_by_period.get(period)),
                "period": period,
                "confirmed": True,
            }
        )
    return payloads


def _features_payload(bsp: Any) -> dict[str, float | int | str | bool | None]:
    features = getattr(bsp, "features", None)
    for attr in ("feature_dict", "features"):
        raw = getattr(features, attr, None)
        if isinstance(raw, dict):
            return {
                str(key): value
                for key, value in raw.items()
                if isinstance(value, (str, int, float, bool)) or value is None
            }
    return {}


def _vendor_root(path_hint: str | None = None) -> Path:
    if path_hint:
        hinted = Path(path_hint).expanduser().resolve()
        if hinted.suffix.lower() == ".py" and hinted.is_file():
            return hinted.parent
        return hinted
    root = Path(__file__).resolve().parents[3]
    return root / "work" / "vendor" / "chan.py-main"


def _to_ctime(timestamp: int, ctime_cls: Any) -> Any:
    dt = datetime.fromtimestamp(timestamp)
    return ctime_cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, auto=False)


def _to_kl_type(kl_type_enum: Any, level: str) -> Any:
    mapping = {
        "5f": "K_5M",
        "15f": "K_15M",
        "30f": "K_30M",
        "1h": "K_60M",
        "1d": "K_DAY",
        "1w": "K_WEEK",
        "1m": "K_MON",
    }
    if level not in mapping:
        raise ValueError(f"Unsupported chan.py level: {level}")
    return getattr(kl_type_enum, mapping[level])


def _time_payload(value: Any) -> int:
    if hasattr(value, "ts"):
        return int(value.ts)
    nested_time = getattr(value, "time", None)
    if nested_time is not None and hasattr(nested_time, "ts"):
        return int(nested_time.ts)
    raise AttributeError(f"Unsupported time payload: {type(value)!r}")


def _seq_payload(value: Any) -> int | None:
    idx = getattr(value, "idx", None)
    if idx is not None:
        return int(idx)
    nested_time = getattr(value, "time", None)
    nested_idx = getattr(nested_time, "idx", None)
    return int(nested_idx) if nested_idx is not None else None


def _point_payload(value: Any, price: float) -> dict[str, Any]:
    base_ts = _time_payload(value)
    return {
        "time": base_ts,
        "price": float(price),
        "base_ts": base_ts,
        "base_seq": _seq_payload(value),
    }


def _snapshot_version(symbol: str, bars: list[dict[str, Any]]) -> str:
    if not bars:
        return f"{symbol}:5f:empty"
    first = int(bars[0]["time"])
    last = int(bars[-1]["time"])
    digest = hashlib.sha256()
    for bar in bars:
        digest.update(
            (
                f"{int(bar['time'])}|{float(bar['open']):.8f}|{float(bar['high']):.8f}|"
                f"{float(bar['low']):.8f}|{float(bar['close']):.8f}|"
                f"{int(bar.get('volume', 0))}|"
            ).encode("utf-8")
        )
    return f"{symbol}:5f:{first}:{last}:{len(bars)}:{digest.hexdigest()[:16]}"


def _mode_flags(modes: Iterable[str]) -> tuple[bool, bool]:
    normalized = {str(mode).lower() for mode in modes}
    return "confirmed" in normalized, "predictive" in normalized


def _mode_from_sure_flag(is_sure: bool) -> str:
    return "confirmed" if is_sure else "predictive"


def _include_mode(mode: str, include_confirmed: bool, include_predictive: bool) -> bool:
    return (mode == "confirmed" and include_confirmed) or (mode == "predictive" and include_predictive)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def _iter_items(items: Any) -> list[Any]:
    return list(items) if items is not None else []


def _iter_klu_items(kl_data: Any) -> list[Any]:
    iter_fn = getattr(kl_data, "klu_iter", None)
    if callable(iter_fn):
        return list(iter_fn())
    return _iter_items(getattr(kl_data, "lst", []))


def _is_up(item: Any) -> bool:
    is_up = getattr(item, "is_up", None)
    if callable(is_up):
        return bool(is_up())
    direction = getattr(item, "dir", None)
    return str(direction).endswith("UP")
