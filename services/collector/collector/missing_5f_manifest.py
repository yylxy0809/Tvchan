from __future__ import annotations

import argparse
import asyncio
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from collector.providers.factory import ProviderFactoryConfig, create_single_provider
from collector.providers.pool import validate_bars


PROVIDER_PLAN = {
    "SH": ["pytdx", "mootdx", "tencent", "baidu"],
    "SZ": ["pytdx", "mootdx", "tencent", "baidu"],
    # Pytdx's market encoding has no Beijing market and must never receive BJ symbols.
    "BJ": ["tencent", "baidu"],
}


def build_manifest(master_path: Path, five_min_root: Path) -> list[dict[str, Any]]:
    table = pq.read_table(master_path)
    required = {"ts_code", "list_status"}
    missing_columns = required.difference(table.column_names)
    if missing_columns:
        raise ValueError(f"master parquet missing columns: {sorted(missing_columns)}")

    rows: list[dict[str, Any]] = []
    for item in table.to_pylist():
        if str(item.get("list_status") or "").strip().upper() != "L":
            continue
        symbol = str(item.get("ts_code") or "").strip().upper()
        exchange = symbol.rsplit(".", 1)[-1] if "." in symbol else "UNKNOWN"
        path = five_min_root / f"{symbol}.parquet"
        reason, blocker = classify_file(path)
        if reason is None:
            continue
        plan = PROVIDER_PLAN.get(exchange, [])
        if exchange not in PROVIDER_PLAN:
            blocker = "unsupported_exchange"
        rows.append(
            {
                "symbol": symbol,
                "code": str(item.get("symbol") or symbol.split(".", 1)[0]),
                "exchange": exchange,
                "name": item.get("name"),
                "market": item.get("market"),
                "list_date": item.get("list_date"),
                "five_f_path": str(path),
                "missing_reason": reason,
                "provider_plan": ",".join(plan),
                "blocker": blocker,
                "probe_status": "not_requested",
                "probe_provider": None,
                "probe_bars": None,
                "probe_error": None,
            }
        )
    return sorted(rows, key=lambda row: row["symbol"])


def classify_file(path: Path) -> tuple[str | None, str | None]:
    if not path.exists():
        return "file_absent", "provider_backfill_required"
    if path.stat().st_size == 0:
        return "file_empty", "invalid_local_file"
    try:
        metadata = pq.read_metadata(path)
    except Exception:
        return "parquet_unreadable", "invalid_local_file"
    if metadata.num_rows == 0:
        return "parquet_no_rows", "invalid_local_file"
    return None, None


async def probe_rows(rows: list[dict[str, Any]], *, limit: int, timeout: float) -> None:
    if not 1 <= len(rows) <= 10:
        raise ValueError("network probe requires explicitly selecting 1-10 missing symbols")
    config = ProviderFactoryConfig(names=[], http_timeout=timeout, pool_timeout_seconds=timeout)
    for row in rows:
        errors: list[str] = []
        for provider_name in PROVIDER_PLAN.get(row["exchange"], []):
            if row["exchange"] == "BJ" and provider_name in {"pytdx", "mootdx"}:
                raise AssertionError("BJ symbol must not be sent to a TDX provider")
            try:
                provider = create_single_provider(provider_name, config)
                bars = await asyncio.wait_for(
                    provider.get_bars(row["symbol"], "5f", limit=limit), timeout=timeout
                )
                validate_bars(symbol=row["symbol"], timeframe="5f", bars=bars)
            except Exception as exc:
                errors.append(f"{provider_name}:{type(exc).__name__}:{str(exc)[:180]}")
                continue
            row.update(
                probe_status="available",
                probe_provider=provider_name,
                probe_bars=len(bars),
                probe_error="; ".join(errors) or None,
            )
            break
        else:
            row.update(probe_status="unavailable", probe_error="; ".join(errors) or "no approved provider")


def write_outputs(rows: list[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "missing_5f_manifest.csv"
    json_path = output_dir / "missing_5f_summary.json"
    fieldnames = list(rows[0]) if rows else ["symbol", "exchange", "missing_reason", "provider_plan", "blocker"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "missing_count": len(rows),
        "by_exchange": {
            exchange: Counter(row["exchange"] for row in rows).get(exchange, 0)
            for exchange in ("SH", "SZ", "BJ")
        },
        "by_reason": dict(sorted(Counter(row["missing_reason"] for row in rows).items())),
        "by_blocker": dict(sorted(Counter(row["blocker"] for row in rows).items())),
        "manifest": str(csv_path.resolve()),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an offline manifest of active A-shares missing 5f parquet")
    parser.add_argument("--master", type=Path, default=Path(r"F:\data\stock_basic_data.parquet"))
    parser.add_argument("--five-min-root", type=Path, default=Path(r"F:\data\stock_5min"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/missing-5f-manifest"))
    parser.add_argument("--probe", action="store_true", help="Explicitly enable bounded network probes")
    parser.add_argument("--probe-symbols", help="Comma-separated missing symbols; required with --probe, max 10")
    parser.add_argument("--probe-limit", type=int, default=10)
    parser.add_argument("--probe-timeout", type=float, default=8.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = build_manifest(args.master, args.five_min_root)
    if args.probe:
        requested = [value.strip().upper() for value in (args.probe_symbols or "").split(",") if value.strip()]
        if not 1 <= len(requested) <= 10:
            raise SystemExit("--probe requires --probe-symbols with 1-10 symbols")
        by_symbol = {row["symbol"]: row for row in rows}
        unknown = [symbol for symbol in requested if symbol not in by_symbol]
        if unknown:
            raise SystemExit(f"probe symbols are not in missing manifest: {unknown}")
        asyncio.run(probe_rows([by_symbol[symbol] for symbol in requested], limit=args.probe_limit, timeout=args.probe_timeout))
    summary = write_outputs(rows, args.output_dir)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
