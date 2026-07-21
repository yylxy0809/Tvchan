"""Freeze a small, explicit strict-v2 eligibility subset for recovery recompute."""
from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from collector.module_c_batch_control import (
    CODE_TO_TIMEFRAME,
    LEVELS,
    Disposition,
    _json_object,
    _strict_v2_provenance,
    revalidate_strict_v2_build,
    validate_strict_build,
)
from collector.module_c_eligibility import _stable_hash, _write_outputs, build_summary
from trading_protocol import MODULE_C_CONFIG_HASH


def parse_symbols(raw: str) -> tuple[str, ...]:
    symbols = tuple(dict.fromkeys(part.strip().upper() for part in raw.split(",") if part.strip()))
    if not symbols or len(symbols) > 20:
        raise ValueError("supplemental scope requires 1..20 explicit symbols")
    return symbols


def build_dispositions(rows: Sequence[Mapping[str, Any]], symbols: Sequence[str]) -> list[Disposition]:
    expected = set(symbols)
    names = {str(row["symbol"]).upper() for row in rows}
    if names != expected or len(rows) != len(symbols) * len(LEVELS):
        raise RuntimeError("Source build lacks the exact supplemental five-level scope")
    counts = Counter(str(row["symbol"]).upper() for row in rows)
    if any(counts[symbol] != len(LEVELS) for symbol in symbols):
        raise RuntimeError("Every supplemental symbol must have exactly five dispositions")
    dispositions = [
        Disposition(
            symbol_id=int(row["symbol_id"]),
            symbol=str(row["symbol"]),
            timeframe=CODE_TO_TIMEFRAME[int(row["timeframe"])],
            timeframe_code=int(row["timeframe"]),
            eligible=bool(row["eligible"]),
            reasons=tuple(row["reasons"] or ()),
            covered_until=row["covered_until"],
            unresolved_rows=int(row["unresolved_rows"] or 0),
        )
        for row in rows
    ]
    if any(not row.eligible or row.unresolved_rows for row in dispositions):
        raise RuntimeError("Supplemental recompute requires five eligible resolved scopes per symbol")
    return dispositions


async def freeze_supplemental(conn: asyncpg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    symbols = parse_symbols(args.symbols)
    async with conn.transaction(isolation="serializable"):
        source = await conn.fetchrow(
            """
            select build_id::text,config_hash,active_universe_hash,manifest_hash,
                   active_symbols,disposition_rows,parameters,summary,
                   canonical_audit_run_id::text,audit_evidence_sha256,
                   audit_checkpoint_sha256,freshness_contract_version,
                   freshness_contract_sha256,catalog_generation_id::text,
                   catalog_control_revision,catalog_manifest_sha256,
                   audit_active_universe_sha256
              from module_c_eligibility_builds
             where build_id=$1::uuid for share
            """,
            args.source_build_id,
        )
        if source is None:
            raise RuntimeError(f"Unknown source eligibility build: {args.source_build_id}")
        if str(source["config_hash"]) != MODULE_C_CONFIG_HASH:
            raise RuntimeError("Source eligibility config_hash is not the production contract")
        await validate_strict_build(conn, source, build_id=args.source_build_id, require_v2=True)
        await revalidate_strict_v2_build(conn, source, build_id=args.source_build_id)
        if str(source["active_universe_hash"]) != str(source["audit_active_universe_sha256"]):
            raise RuntimeError("Source eligibility active universe is not audit-bound")
        source_rows = int(await conn.fetchval(
            "select count(*) from module_c_eligibility where build_id=$1::uuid",
            args.source_build_id,
        ))
        if source_rows != int(source["disposition_rows"]):
            raise RuntimeError("Source eligibility build is incomplete")
        rows = await conn.fetch(
            """
            select symbol_id,symbol,timeframe,eligible,reasons,covered_until,unresolved_rows
              from module_c_eligibility
             where build_id=$1::uuid and symbol=any($2::text[])
             order by symbol_id,array_position($3::int[],timeframe)
            """,
            args.source_build_id,
            list(symbols),
            list(LEVELS),
        )
        dispositions = build_dispositions(rows, symbols)
        provenance, copied_parameters = _strict_v2_provenance(source)
        build_id = uuid.UUID(args.build_id) if args.build_id else uuid.uuid4()
        manifest_hash = _stable_hash(row.json_record() for row in dispositions)
        active_hash = _stable_hash(
            name for _, name in sorted({(row.symbol_id, row.symbol) for row in dispositions})
        )
        summary = build_summary(dispositions)
        parameters = {
            **copied_parameters,
            "scope": "supplemental",
            "source_build_id": args.source_build_id,
            "supplemental_contract_version": "module-c-supplemental-selection-v1",
            "supplemental_symbols": list(symbols),
            "justification": args.justification,
        }
        active_symbols = len(symbols)
        disposition_rows = len(dispositions)
        inserted = await conn.execute(
            """
            insert into module_c_eligibility_builds (
                build_id,manifest_version,config_hash,active_universe_hash,manifest_hash,
                active_symbols,disposition_rows,parameters,summary,canonical_audit_run_id,
                audit_evidence_sha256,audit_checkpoint_sha256,freshness_contract_version,
                freshness_contract_sha256,catalog_generation_id,catalog_control_revision,
                catalog_manifest_sha256,audit_active_universe_sha256
            ) values ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::uuid,$11,$12,$13,$14,
                      $15::uuid,$16,$17,$18)
            on conflict (build_id) do nothing
            """,
            build_id,args.manifest_version,MODULE_C_CONFIG_HASH,active_hash,manifest_hash,
            active_symbols,disposition_rows,json.dumps(parameters,sort_keys=True),
            json.dumps(summary,sort_keys=True),provenance["canonical_audit_run_id"],
            provenance["audit_evidence_sha256"],provenance["audit_checkpoint_sha256"],
            provenance["freshness_contract_version"],provenance["freshness_contract_sha256"],
            provenance["catalog_generation_id"],provenance["catalog_control_revision"],
            provenance["catalog_manifest_sha256"],provenance["audit_active_universe_sha256"],
        )
        if inserted.endswith(" 1"):
            await conn.copy_records_to_table(
                "module_c_eligibility",
                records=[(
                    build_id,row.symbol_id,row.symbol,row.timeframe_code,row.eligible,
                    list(row.reasons),row.covered_until,row.unresolved_rows,
                ) for row in dispositions],
                columns=("build_id","symbol_id","symbol","timeframe","eligible","reasons","covered_until","unresolved_rows"),
            )
        existing = await conn.fetchrow(
            "select manifest_version,config_hash,active_universe_hash,manifest_hash,active_symbols,"
            "disposition_rows,parameters from module_c_eligibility_builds where build_id=$1",
            build_id,
        )
        expected = {
            "manifest_version": args.manifest_version,
            "config_hash": MODULE_C_CONFIG_HASH,
            "active_universe_hash": active_hash,
            "manifest_hash": manifest_hash,
            "active_symbols": active_symbols,
            "disposition_rows": disposition_rows,
            "parameters": parameters,
        }
        actual = {**dict(existing), "parameters": _json_object(existing["parameters"])}
        if actual != expected:
            raise RuntimeError("Existing supplemental eligibility build does not exactly match")
        if int(await conn.fetchval(
            "select count(*) from module_c_eligibility where build_id=$1", build_id
        )) != disposition_rows:
            raise RuntimeError("Supplemental eligibility rows are incomplete")
        metadata = {
            "build_id": str(build_id),"manifest_version": args.manifest_version,
            "config_hash": MODULE_C_CONFIG_HASH,"active_universe_hash": active_hash,
            "manifest_hash": manifest_hash,"active_symbols": active_symbols,
            **provenance,
            "excluded_summary": {"excluded_scopes": 0, "reasons": {}},
            **summary,
        }
        _write_outputs(args.output_dir, dispositions, metadata)
        return metadata


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-build-id", required=True)
    parser.add_argument("--build-id")
    parser.add_argument("--manifest-version", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--justification", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


async def _main(args: argparse.Namespace) -> dict[str, Any]:
    conn = await asyncpg.connect(args.database_url)
    try:
        return await freeze_supplemental(conn, args)
    finally:
        await conn.close()


def main() -> None:
    print(json.dumps(asyncio.run(_main(_parser().parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
