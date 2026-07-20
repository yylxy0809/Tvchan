from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from collector.chan_module_c_recompute import compute_module_c_overlay
from collector.market_fill import filter_chan_response_level
from collector.storage.postgres import PostgresKlineWriter, price_to_x1000
from trading_protocol import MODULE_C_CONFIG_HASH


LEVEL_NAMES = {5: "5f", 30: "30f", 1440: "1d", 10080: "1w", 43200: "1m"}
LEVEL_ORDER = {name: index for index, name in enumerate(LEVEL_NAMES.values())}
MODE_NAMES = {1: "confirmed", 2: "predictive"}

PUBLISHED_RUNS_SQL = """
select r.id as run_id,
       r.symbol_id,
       s.code || '.' || s.exchange as symbol,
       r.chan_level,
       r.bar_from,
       r.bar_until,
       r.bar_count,
       r.config_hash,
       r.run_group_id,
       r.batch_id,
       array['confirmed'::varchar, 'predictive'::varchar] as modes
from chan_c_runs r
join chan_c_full_recompute_tasks task
  on task.run_id = r.id
join symbols s on s.id = r.symbol_id
where r.status = 'success'
  and task.status = 'completed'
  and (($1::bigint is not null and task.batch_id = $1)
       or ($2::varchar is not null and r.run_group_id = $2))
group by r.id, r.symbol_id, s.code, s.exchange, r.chan_level, r.bar_from,
         r.bar_until, r.bar_count, r.config_hash, r.run_group_id, r.batch_id
order by s.code, s.exchange, r.chan_level, r.id
"""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare published Module C canary runs with direct chan.py calculation"
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--batch-id", type=int)
    selector.add_argument("--run-group-id")
    parser.add_argument("--chan-py-path", default=os.getenv("CHAN_PY_PATH"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-difference-samples", type=int, default=20)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    if args.batch_id is not None and args.batch_id <= 0:
        parser.error("--batch-id must be positive")
    if args.max_difference_samples < 0:
        parser.error("--max-difference-samples must be non-negative")
    return args


def _epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(value)


def _line_record(item: Mapping[str, Any], *, seq: int | None = None) -> dict[str, Any]:
    start = item["start"]
    end = item["end"]
    item_seq = item.get("seq")
    return {
        "seq": int(seq if item_seq is None and seq is not None else item_seq or 0),
        "mode": str(item.get("mode") or "confirmed"),
        "start_time": _epoch(start["time"]),
        "end_time": _epoch(end["time"]),
        "start_price_x1000": price_to_x1000(start["price"]),
        "end_price_x1000": price_to_x1000(end["price"]),
        "direction": str(item.get("direction") or ""),
        "confirmed": bool(item.get("confirmed")),
        "begin_base_ts": _epoch(item.get("begin_base_ts") or start["time"]),
        "end_base_ts": _epoch(item.get("end_base_ts") or end["time"]),
        "begin_base_seq": item.get("begin_base_seq"),
        "end_base_seq": item.get("end_base_seq"),
    }


def _center_record(item: Mapping[str, Any], *, seq: int | None = None) -> dict[str, Any]:
    item_seq = item.get("seq")
    return {
        "seq": int(seq if item_seq is None and seq is not None else item_seq or 0),
        "mode": str(item.get("mode") or "confirmed"),
        "start_time": _epoch(item["start_time"]),
        "end_time": _epoch(item["end_time"]),
        "low_x1000": price_to_x1000(item["low"]),
        "high_x1000": price_to_x1000(item["high"]),
        "confirmed": bool(item.get("confirmed")),
        "begin_base_ts": _epoch(item.get("begin_base_ts") or item["start_time"]),
        "end_base_ts": _epoch(item.get("end_base_ts") or item["end_time"]),
        "begin_base_seq": item.get("begin_base_seq"),
        "end_base_seq": item.get("end_base_seq"),
    }


def _signal_record(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "mode": str(item.get("mode") or "confirmed"),
        "time": _epoch(item["time"]),
        "price_x1000": price_to_x1000(item["price"]),
        "signal_type": str(item.get("signal_type") or ""),
        "side": item.get("side"),
        "bsp_type": item.get("bsp_type"),
        "confirmed": bool(item.get("confirmed")),
        "base_ts": _epoch(item.get("base_ts") or item["time"]),
        "base_seq": item.get("base_seq"),
    }


def normalize_direct(response: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return {
        "strokes": [_line_record(item, seq=index) for index, item in enumerate(response.get("strokes", []))],
        "segments": [_line_record(item, seq=index) for index, item in enumerate(response.get("segments", []))],
        "centers": [_center_record(item, seq=index) for index, item in enumerate(response.get("centers", []))],
        "signals": sorted((_signal_record(item) for item in response.get("signals", [])), key=_json_sort_key),
    }


def _json_sort_key(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_object(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str) and value:
        decoded = json.loads(value)
        return decoded if isinstance(decoded, Mapping) else {}
    return {}


def compare_payloads(
    direct: Mapping[str, list[dict[str, Any]]],
    persisted: Mapping[str, list[dict[str, Any]]],
    *,
    max_samples: int,
) -> dict[str, Any]:
    objects: dict[str, Any] = {}
    sample_budget = max_samples
    for object_type in ("strokes", "segments", "centers", "signals"):
        expected = list(direct.get(object_type, []))
        actual = list(persisted.get(object_type, []))
        mismatches: list[dict[str, Any]] = []
        for index in range(max(len(expected), len(actual))):
            left = expected[index] if index < len(expected) else None
            right = actual[index] if index < len(actual) else None
            if left != right and sample_budget > 0:
                mismatches.append({"index": index, "direct": left, "persisted": right})
                sample_budget -= 1
        mismatch_count = sum(
            1
            for index in range(max(len(expected), len(actual)))
            if (expected[index] if index < len(expected) else None)
            != (actual[index] if index < len(actual) else None)
        )
        objects[object_type] = {
            "direct_count": len(expected),
            "persisted_count": len(actual),
            "mismatch_count": mismatch_count,
            "samples": mismatches,
        }
    difference_count = sum(item["mismatch_count"] for item in objects.values())
    return {"difference_count": difference_count, "objects": objects}


async def _fetch_persisted(connection: asyncpg.Connection, run_id: int) -> dict[str, list[dict[str, Any]]]:
    lines: dict[str, list[dict[str, Any]]] = {}
    for object_type, table in (("strokes", "chan_c_strokes"), ("segments", "chan_c_segments")):
        rows = await connection.fetch(
            f"""
            select seq, mode, start_ts, end_ts, start_price_x1000, end_price_x1000,
                   direction, is_confirmed, coalesce(begin_base_ts,start_ts) begin_base_ts,
                   coalesce(end_base_ts,end_ts) end_base_ts, begin_base_seq, end_base_seq
            from {table} where run_id=$1 order by seq
            """,
            run_id,
        )
        lines[object_type] = [
            {
                "seq": int(row["seq"]),
                "mode": MODE_NAMES.get(int(row["mode"]), str(row["mode"])),
                "start_time": _epoch(row["start_ts"]),
                "end_time": _epoch(row["end_ts"]),
                "start_price_x1000": int(row["start_price_x1000"]),
                "end_price_x1000": int(row["end_price_x1000"]),
                "direction": {1: "up", -1: "down"}.get(int(row["direction"]), str(row["direction"])),
                "confirmed": bool(row["is_confirmed"]),
                "begin_base_ts": _epoch(row["begin_base_ts"]),
                "end_base_ts": _epoch(row["end_base_ts"]),
                "begin_base_seq": row["begin_base_seq"],
                "end_base_seq": row["end_base_seq"],
            }
            for row in rows
        ]
    center_rows = await connection.fetch(
        """
        select seq, mode, start_ts, end_ts, low_x1000, high_x1000, is_confirmed,
               coalesce(begin_base_ts,start_ts) begin_base_ts,
               coalesce(end_base_ts,end_ts) end_base_ts, begin_base_seq, end_base_seq
        from chan_c_centers where run_id=$1 order by seq
        """,
        run_id,
    )
    lines["centers"] = [
        {
            "seq": int(row["seq"]),
            "mode": MODE_NAMES.get(int(row["mode"]), str(row["mode"])),
            "start_time": _epoch(row["start_ts"]),
            "end_time": _epoch(row["end_ts"]),
            "low_x1000": int(row["low_x1000"]),
            "high_x1000": int(row["high_x1000"]),
            "confirmed": bool(row["is_confirmed"]),
            "begin_base_ts": _epoch(row["begin_base_ts"]),
            "end_base_ts": _epoch(row["end_base_ts"]),
            "begin_base_seq": row["begin_base_seq"],
            "end_base_seq": row["end_base_seq"],
        }
        for row in center_rows
    ]
    signal_rows = await connection.fetch(
        """
        select mode, ts, price_x1000, signal_type, is_confirmed,
               coalesce(base_ts,ts) base_ts, base_seq, extra
        from chan_c_signals where run_id=$1 order by coalesce(base_ts,ts), id
        """,
        run_id,
    )
    lines["signals"] = sorted(
        (
            {
                "mode": MODE_NAMES.get(int(row["mode"]), str(row["mode"])),
                "time": _epoch(row["ts"]),
                "price_x1000": int(row["price_x1000"]),
                "signal_type": str(row["signal_type"]),
                "side": _json_object(row["extra"]).get("side"),
                "bsp_type": _json_object(row["extra"]).get("bsp_type"),
                "confirmed": bool(row["is_confirmed"]),
                "base_ts": _epoch(row["base_ts"]),
                "base_seq": row["base_seq"],
            }
            for row in signal_rows
        ),
        key=_json_sort_key,
    )
    return lines


async def build_report(args: argparse.Namespace) -> dict[str, Any]:
    connection = await asyncpg.connect(args.database_url)
    try:
        runs = await connection.fetch(PUBLISHED_RUNS_SQL, args.batch_id, args.run_group_id)
    finally:
        await connection.close()
    if not runs:
        raise RuntimeError("No successfully published Module C runs matched the selector")

    results: list[dict[str, Any]] = []
    runs_by_symbol: dict[str, list[Mapping[str, Any]]] = {}
    for run in runs:
        runs_by_symbol.setdefault(str(run["symbol"]), []).append(run)
    async with PostgresKlineWriter(args.database_url, pool_min_size=1, pool_max_size=1) as kline_writer:
        connection = await asyncpg.connect(args.database_url)
        try:
            for symbol, symbol_runs in runs_by_symbol.items():
                for run in symbol_runs:
                    try:
                        level = LEVEL_NAMES.get(int(run["chan_level"]))
                        if level is None:
                            raise RuntimeError(f"Unsupported published Chan level: {run['chan_level']}")
                        modes = sorted(str(mode) for mode in run["modes"])
                        bars = [
                            bar for bar in await kline_writer.get_bars(symbol, level)
                            if (run["bar_from"] is None or bar.ts >= run["bar_from"])
                            and bar.ts <= run["bar_until"]
                        ]
                        if not bars:
                            raise RuntimeError(f"No canonical bars matched the published run range: {level}")
                        direct_response = await compute_module_c_overlay(
                            symbol=symbol, levels=[level], modes=modes, bars_by_level={level: bars},
                            chan_py_path=args.chan_py_path,
                        )
                        direct = normalize_direct(filter_chan_response_level(direct_response, level))
                        persisted = await _fetch_persisted(connection, int(run["run_id"]))
                        comparison = compare_payloads(direct, persisted, max_samples=args.max_difference_samples)
                        config_match = str(run["config_hash"]) == MODULE_C_CONFIG_HASH
                        bar_count_match = int(run["bar_count"] or 0) == len(bars)
                        results.append(
                            {
                                **_base_result(run, symbol, level),
                                "config_match": config_match, "bar_count": len(bars),
                                "bar_count_match": bar_count_match, **comparison, "error": None,
                                "passed": config_match and bar_count_match and comparison["difference_count"] == 0,
                            }
                        )
                    except Exception as error:
                        level = LEVEL_NAMES.get(int(run["chan_level"]), str(run["chan_level"]))
                        results.append({
                            **_base_result(run, symbol, level),
                            "config_match": str(run["config_hash"]) == MODULE_C_CONFIG_HASH,
                            "bar_count": 0, "bar_count_match": False, "difference_count": 1,
                            "objects": {}, "error": str(error)[:1000], "passed": False,
                        })
        finally:
            await connection.close()

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "selector": {"batch_id": args.batch_id, "run_group_id": args.run_group_id},
        "expected_config_hash": MODULE_C_CONFIG_HASH,
        "published_runs": len(results),
        "symbols": len({row["symbol"] for row in results}),
        "levels": sorted({row["level"] for row in results}, key=LEVEL_ORDER.__getitem__),
        "difference_count": sum(row["difference_count"] for row in results),
        "failed_runs": sum(not row["passed"] for row in results),
        "passed": all(row["passed"] for row in results),
        "runs": results,
    }
    _write_report(args.output_dir, report)
    return report


def _base_result(run: Mapping[str, Any], symbol: str, level: str) -> dict[str, Any]:
    return {
        "run_id": int(run["run_id"]),
        "batch_id": run["batch_id"],
        "run_group_id": run["run_group_id"],
        "symbol": symbol,
        "level": level,
        "modes": [str(value) for value in run["modes"]],
        "config_hash": str(run["config_hash"]),
    }


def _write_report(output_dir: Path, report: Mapping[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"
    lines = [
        "# Module C canary A/B report",
        "",
        f"- passed: `{str(bool(report['passed'])).lower()}`",
        f"- published_runs: `{report['published_runs']}`",
        f"- symbols: `{report['symbols']}`",
        f"- levels: `{', '.join(report['levels'])}`",
        f"- failed_runs: `{report['failed_runs']}`",
        f"- difference_count: `{report['difference_count']}`",
        "",
        "| run_id | symbol | level | passed | differences | error |",
        "|---:|---|---|---|---:|---|",
    ]
    for row in report["runs"]:
        error = str(row.get("error") or "").replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| {row['run_id']} | {row['symbol']} | {row['level']} | "
            f"{str(bool(row['passed'])).lower()} | {row['difference_count']} | {error} |"
        )
    lines.append("")
    markdown = "\n".join(lines)
    _atomic_write(output_dir / "canary_ab_report.json", json_text)
    _atomic_write(output_dir / "canary_ab_report.md", markdown)
    _atomic_write(output_dir / "canary_ab_summary.json", json_text)
    _atomic_write(output_dir / "canary_ab_summary.md", markdown)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def _main(argv: Sequence[str] | None = None) -> None:
    report = await build_report(parse_args(argv))
    print(json.dumps({key: report[key] for key in ("passed", "published_runs", "symbols", "failed_runs", "difference_count")}, sort_keys=True))
    if not report["passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(_main())
