from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


REQUIRED_COLUMNS = (
    "ts_code", "trade_date", "trade_time", "open", "high", "low", "close", "vol", "amount"
)
TIMEFRAME_DIRS = {
    "1min": "stock_1min", "5min": "stock_5min", "15min": "stock_15min",
    "30min": "stock_30min", "60min": "stock_60min",
}


@dataclass
class FileProfile:
    path: str
    expected_symbol: str
    rows: int = 0
    row_groups: int = 0
    columns: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    min_trade_time: str | None = None
    max_trade_time: str | None = None
    symbol_mismatch_rows: int = 0
    date_mismatch_rows: int = 0
    duplicate_key_rows: int = 0
    invalid_ohlc_rows: int = 0
    negative_volume_rows: int = 0
    negative_amount_rows: int = 0
    invalid_session_rows: int = 0
    opening_snapshot_rows: int = 0
    symbol_days: int = 0
    incomplete_session_days: int = 0
    missing_expected_bars: int = 0
    projected_accepted_rows: int = 0
    projected_quarantined_rows: int = 0
    projected_rejected_rows: int = 0
    day_count_histogram: dict[str, int] = field(default_factory=dict)
    anomaly_examples: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass
class RootProfile:
    root: str
    timeframe: str
    discovered_files: int
    profiled_files: int
    shard_index: int
    shard_count: int
    total_rows: int
    required_columns_ok: bool
    min_trade_time: str | None
    max_trade_time: str | None
    anomaly_totals: dict[str, int]
    files: list[FileProfile]
    active_symbols: int = 0
    inactive_symbols: int = 0
    active_symbols_with_file: int = 0
    active_symbols_missing_file: int = 0
    inactive_symbols_with_file: int = 0
    active_missing_symbols: list[str] = field(default_factory=list)
    daily_profile: dict[str, Any] = field(default_factory=dict)
    volume_unit_evidence: dict[str, Any] = field(default_factory=dict)
    execution: dict[str, Any] = field(default_factory=dict)
    dry_run: bool = True
    database_writes: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _true_count(mask: pa.Array) -> int:
    return int(pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py() or 0)


def _examples(batch: pa.RecordBatch, mask: pa.Array, kind: str, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    indices = pc.indices_nonzero(pc.fill_null(mask, False)).slice(0, limit).to_pylist()
    result = []
    names = set(batch.schema.names)
    fields = ("ts_code", "trade_date", "trade_time")
    for index in indices:
        row = {name: _iso(batch.column(name)[index].as_py()) for name in fields if name in names}
        row["kind"] = kind
        result.append(row)
    return result


def expected_session_times(timeframe: str) -> set[str]:
    minutes = int(timeframe.removesuffix("min"))
    if minutes not in {1, 5, 15, 30, 60}:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    # Source contract includes a 09:30 opening snapshot. Afternoon bars start at
    # 13:00 + interval (13:05 for 5min); timestamps are bar_end and are never shifted.
    result = {"09:30"}
    for start, end in ((9 * 60 + 30 + minutes, 11 * 60 + 30), (13 * 60 + minutes, 15 * 60)):
        for value in range(start, end + 1, minutes):
            result.add(f"{value // 60:02d}:{value % 60:02d}")
    return result


def profile_file(path: str | Path, *, timeframe: str = "5min", batch_size: int = 65_536,
                 max_examples: int = 20) -> FileProfile:
    path = Path(path)
    result = FileProfile(path=str(path), expected_symbol=path.stem)
    try:
        parquet = pq.ParquetFile(path)
        result.row_groups = parquet.num_row_groups
        result.columns = list(parquet.schema_arrow.names)
        result.missing_columns = [c for c in REQUIRED_COLUMNS if c not in result.columns]
        if result.missing_columns:
            result.rows = parquet.metadata.num_rows
            result.projected_rejected_rows = result.rows
            return result

        day_counts: Counter[str] = Counter()
        day_slots: dict[str, set[str]] = {}
        valid_slots = expected_session_times(timeframe)
        previous_key: tuple[Any, Any] | None = None
        for batch in parquet.iter_batches(batch_size=batch_size, columns=list(REQUIRED_COLUMNS)):
            result.rows += batch.num_rows
            symbol = batch.column("ts_code")
            trade_date = batch.column("trade_date")
            trade_time = batch.column("trade_time")
            opens, highs = batch.column("open"), batch.column("high")
            lows, closes = batch.column("low"), batch.column("close")
            volume, amount = batch.column("vol"), batch.column("amount")

            symbol_bad = pc.not_equal(symbol, pa.scalar(path.stem))
            # Both fields are timestamps in this source; their calendar date must agree.
            date_bad = pc.not_equal(pc.floor_temporal(trade_date, unit="day"), pc.floor_temporal(trade_time, unit="day"))
            max_oc = pc.max_element_wise(opens, closes)
            min_oc = pc.min_element_wise(opens, closes)
            ohlc_bad = pc.or_(pc.less(highs, max_oc), pc.greater(lows, min_oc))
            ohlc_bad = pc.or_(ohlc_bad, pc.less(highs, lows))
            volume_bad = pc.less(volume, 0)
            amount_bad = pc.less(amount, 0)
            session_values = [value.strftime("%H:%M") for value in trade_time.to_pylist()]
            session_bad = pa.array([value not in valid_slots for value in session_values])
            opening_snapshot = pa.array([value == "09:30" for value in session_values])
            quarantine_mask = pc.or_(pc.or_(symbol_bad, date_bad), pc.or_(ohlc_bad, volume_bad))
            quarantine_mask = pc.or_(quarantine_mask, amount_bad)
            quarantine_mask = pc.or_(quarantine_mask, session_bad)

            result.symbol_mismatch_rows += _true_count(symbol_bad)
            result.date_mismatch_rows += _true_count(date_bad)
            result.invalid_ohlc_rows += _true_count(ohlc_bad)
            result.negative_volume_rows += _true_count(volume_bad)
            result.negative_amount_rows += _true_count(amount_bad)
            result.invalid_session_rows += _true_count(session_bad)
            result.opening_snapshot_rows += _true_count(opening_snapshot)
            result.projected_quarantined_rows += _true_count(quarantine_mask)
            remaining = max_examples - len(result.anomaly_examples)
            for mask, kind in ((symbol_bad, "symbol_mismatch"), (date_bad, "date_mismatch"),
                               (ohlc_bad, "invalid_ohlc"), (volume_bad, "negative_volume"),
                               (amount_bad, "negative_amount"),
                               (session_bad, "invalid_session")):
                examples = _examples(batch, mask, kind, remaining)
                result.anomaly_examples.extend(examples)
                remaining = max_examples - len(result.anomaly_examples)

            times = trade_time.to_pylist()
            dates = trade_date.to_pylist()
            symbols = symbol.to_pylist()
            for code, date_value, time_value in zip(symbols, dates, times):
                day = _iso(date_value)[:10]
                day_counts[day] += 1
                day_slots.setdefault(day, set()).add(time_value.strftime("%H:%M"))
                key = (code, time_value)
                if key == previous_key:
                    result.duplicate_key_rows += 1
                    if len(result.anomaly_examples) < max_examples:
                        result.anomaly_examples.append({"kind": "duplicate_key", "ts_code": code,
                                                        "trade_date": _iso(date_value), "trade_time": _iso(time_value)})
                previous_key = key
            if times:
                batch_min, batch_max = min(times), max(times)
                if result.min_trade_time is None or batch_min < datetime.fromisoformat(result.min_trade_time):
                    result.min_trade_time = _iso(batch_min)
                if result.max_trade_time is None or batch_max > datetime.fromisoformat(result.max_trade_time):
                    result.max_trade_time = _iso(batch_max)

        result.day_count_histogram = {
            str(count): days for count, days in sorted(Counter(day_counts.values()).items())
        }
        result.symbol_days = len(day_slots)
        missing_by_day = [len(valid_slots - slots) for slots in day_slots.values()]
        result.incomplete_session_days = sum(value > 0 for value in missing_by_day)
        result.missing_expected_bars = sum(missing_by_day)
        result.projected_quarantined_rows = min(
            result.rows, result.projected_quarantined_rows + result.duplicate_key_rows
        )
        result.projected_accepted_rows = result.rows - result.projected_quarantined_rows
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
    return result


def profile_root(root: str | Path, *, timeframe: str = "5min", max_files: int | None = None,
                 batch_size: int = 65_536, max_examples_per_file: int = 20,
                 profile_daily: bool = True, daily_sample_batches: int | None = 1,
                 shard_index: int = 0, shard_count: int = 1) -> RootProfile:
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be >= 0 and < shard_count")
    root_path = Path(root).expanduser().resolve()
    source_dir = root_path / TIMEFRAME_DIRS[timeframe]
    if not source_dir.is_dir():
        raise NotADirectoryError(f"Parquet timeframe directory does not exist: {source_dir}")
    discovered = sorted(source_dir.glob("*.parquet"))
    shard_paths = discovered[shard_index::shard_count]
    selected = shard_paths if max_files is None else shard_paths[:max_files]
    files = [profile_file(p, timeframe=timeframe, batch_size=batch_size, max_examples=max_examples_per_file) for p in selected]
    mins = [f.min_trade_time for f in files if f.min_trade_time]
    maxs = [f.max_trade_time for f in files if f.max_trade_time]
    keys = ("symbol_mismatch_rows", "date_mismatch_rows", "duplicate_key_rows",
            "invalid_ohlc_rows", "negative_volume_rows", "negative_amount_rows", "invalid_session_rows",
            "projected_accepted_rows", "projected_quarantined_rows", "projected_rejected_rows")
    totals = {key: sum(getattr(f, key) for f in files) for key in keys}
    totals["file_errors"] = sum(f.error is not None for f in files)
    basic = profile_symbol_coverage(root_path, {p.stem for p in discovered})
    daily = profile_daily_file(root_path / "stock_daily.parquet", batch_size=batch_size,
                               max_batches=daily_sample_batches) if profile_daily else {}
    volume = profile_volume_units(root_path, selected[0] if selected else None,
                                  batch_size=batch_size, max_daily_batches=daily_sample_batches)
    return RootProfile(
        root=str(root_path), timeframe=timeframe, discovered_files=len(discovered), profiled_files=len(files),
        shard_index=shard_index, shard_count=shard_count,
        total_rows=sum(f.rows for f in files), required_columns_ok=bool(files) and all(not f.missing_columns for f in files),
        min_trade_time=min(mins) if mins else None, max_trade_time=max(maxs) if maxs else None,
        anomaly_totals=totals, files=files, **basic, daily_profile=daily, volume_unit_evidence=volume,
    )


def profile_symbol_coverage(root: Path, available: set[str]) -> dict[str, Any]:
    path = root / "stock_basic_data.parquet"
    active: set[str] = set()
    inactive: set[str] = set()
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=65_536, columns=["ts_code", "list_status"]):
        for code, status in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()):
            (active if status == "L" else inactive).add(code)
    return {
        "active_symbols": len(active), "inactive_symbols": len(inactive),
        "active_symbols_with_file": len(active & available),
        "active_symbols_missing_file": len(active - available),
        "inactive_symbols_with_file": len(inactive & available),
        "active_missing_symbols": sorted(active - available),
    }


def profile_daily_file(path: Path, *, batch_size: int, max_batches: int | None) -> dict[str, Any]:
    """Profile the shared daily file in batches; this is deliberately read-only.

    Daily bars use a date-only source timestamp.  We report large per-symbol
    date discontinuities as evidence, rather than treating exchange holidays as
    data errors without an authoritative trading calendar.
    """
    parquet = pq.ParquetFile(path)
    required = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    sampled = 0
    minimum = maximum = None
    counters: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    previous_by_symbol: dict[str, Any] = {}
    first_by_symbol: dict[str, str] = {}
    last_by_symbol: dict[str, str] = {}
    date_gap_candidates = 0
    last_key: tuple[str, Any] | None = None
    for index, batch in enumerate(parquet.iter_batches(batch_size=batch_size, columns=required)):
        if max_batches is not None and index >= max_batches:
            break
        sampled += batch.num_rows
        codes = batch.column("ts_code").to_pylist()
        values = batch.column("trade_date").to_pylist()
        opens, highs, lows, closes = (batch.column(name) for name in ("open", "high", "low", "close"))
        volumes, amounts = batch.column("vol"), batch.column("amount")
        max_oc = pc.max_element_wise(opens, closes)
        min_oc = pc.min_element_wise(opens, closes)
        ohlc_bad = pc.or_(pc.less(highs, max_oc), pc.greater(lows, min_oc))
        ohlc_bad = pc.or_(ohlc_bad, pc.less(highs, lows))
        volume_bad = pc.less(volumes, 0)
        amount_bad = pc.less(amounts, 0)
        counters["invalid_ohlc_rows"] += _true_count(ohlc_bad)
        counters["negative_volume_rows"] += _true_count(volume_bad)
        counters["negative_amount_rows"] += _true_count(amount_bad)
        for mask, kind in ((ohlc_bad, "invalid_ohlc"), (volume_bad, "negative_volume"), (amount_bad, "negative_amount")):
            for item in _examples(batch, mask, kind, max(0, 20 - len(examples))):
                examples.append(item)
        if values:
            minimum = min(values) if minimum is None else min(minimum, min(values))
            maximum = max(values) if maximum is None else max(maximum, max(values))
        for code, value in zip(codes, values):
            key = (code, value)
            if key == last_key:
                counters["duplicate_key_rows"] += 1
            last_key = key
            day = _iso(value)[:10]
            first_by_symbol.setdefault(code, day)
            last_by_symbol[code] = day
            previous = previous_by_symbol.get(code)
            if previous is not None and (value - previous).days > 14:
                date_gap_candidates += 1
            previous_by_symbol[code] = value
    return {"path": str(path), "metadata_rows": parquet.metadata.num_rows,
            "row_groups": parquet.num_row_groups, "columns": parquet.schema_arrow.names,
            "missing_columns": [c for c in required if c not in parquet.schema_arrow.names],
            "sampled_rows": sampled, "sample_min_trade_date": _iso(minimum),
            "sample_max_trade_date": _iso(maximum), "full_scan": max_batches is None,
            "anomaly_totals": dict(counters), "anomaly_examples": examples,
            "symbols_observed": len(first_by_symbol), "first_trade_date": min(first_by_symbol.values(), default=None),
            "last_trade_date": max(last_by_symbol.values(), default=None),
            "per_symbol_first_last_examples": [
                {"ts_code": code, "first_trade_date": first_by_symbol[code], "last_trade_date": last_by_symbol[code]}
                for code in sorted(first_by_symbol)[:20]
            ], "large_date_gap_candidates": date_gap_candidates}


def profile_volume_units(root: Path, intraday_path: Path | None, *, batch_size: int,
                         max_daily_batches: int) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "intraday_unit_decision": "pending_evidence", "daily_unit_decision": "pending_evidence",
        "normalization": None, "matched_symbol_days": 0, "ratios_intraday_to_daily_x100": [],
    }
    if intraday_path is None:
        return evidence
    intraday: dict[tuple[str, str], float] = Counter()
    parquet = pq.ParquetFile(intraday_path)
    for batch in parquet.iter_batches(batch_size=batch_size, columns=["ts_code", "trade_date", "vol"]):
        for code, date, volume in zip(*(batch.column(i).to_pylist() for i in range(3))):
            intraday[(code, _iso(date)[:10])] += float(volume or 0)
    ratios = []
    daily = pq.ParquetFile(root / "stock_daily.parquet")
    for index, batch in enumerate(daily.iter_batches(batch_size=batch_size, columns=["ts_code", "trade_date", "vol"])):
        if index >= max_daily_batches:
            break
        for code, date, volume in zip(*(batch.column(i).to_pylist() for i in range(3))):
            key = (code, _iso(date)[:10])
            if key in intraday and volume and float(volume) > 0:
                ratios.append(intraday[key] / (float(volume) * 100.0))
    evidence["matched_symbol_days"] = len(ratios)
    evidence["ratios_intraday_to_daily_x100"] = [round(value, 6) for value in ratios[:20]]
    if ratios and sum(abs(value - 1.0) <= 0.05 for value in ratios) / len(ratios) >= 0.9:
        evidence.update({"intraday_unit_decision": "shares", "daily_unit_decision": "hundred_shares",
                         "normalization": "multiply daily vol by 100; keep intraday vol unchanged",
                         "decision_status": "supported_by_cross_timeframe_ratio"})
    else:
        evidence["decision_status"] = "insufficient_or_inconsistent_sample"
    return evidence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only, bounded-memory profiler for local per-symbol Parquet history")
    parser.add_argument("--root", required=True)
    parser.add_argument("--timeframe", choices=sorted(TIMEFRAME_DIRS), default="5min")
    parser.add_argument("--max-files", type=int)
    parser.add_argument("--batch-size", type=int, default=65_536)
    parser.add_argument("--max-examples-per-file", type=int, default=20)
    parser.add_argument("--daily-sample-batches", type=int, default=1)
    parser.add_argument("--full-daily-scan", action="store_true",
                        help="Scan all stock_daily rows and report daily OHLC/volume/amount evidence")
    parser.add_argument("--skip-daily-profile", action="store_true")
    parser.add_argument("--shard-index", type=int, default=0,
                        help="Zero-based static shard index over the sorted file list")
    parser.add_argument("--shard-count", type=int, default=1,
                        help="Total static shards; default 1 profiles every file")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.perf_counter()
    result = profile_root(args.root, timeframe=args.timeframe, max_files=args.max_files,
                          batch_size=args.batch_size, max_examples_per_file=args.max_examples_per_file,
                          profile_daily=not args.skip_daily_profile,
                          daily_sample_batches=None if args.full_daily_scan else args.daily_sample_batches,
                          shard_index=args.shard_index, shard_count=args.shard_count)
    result.execution = {"pid": os.getpid(), "batch_size": args.batch_size,
                        "elapsed_seconds": round(time.perf_counter() - started, 3),
                        "full_daily_scan": args.full_daily_scan}
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.required_columns_ok and not result.anomaly_totals["file_errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
