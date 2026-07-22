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
from collector.kline_sql_gate import (
    ACTIVE_CATALOG_GENERATION_SQL,
    ACTIVE_CATALOG_MANIFEST_SQL,
    _json_value,
    _manifest_sha256,
)
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


def validate_catalog_scope_rows(
    catalog_rows: Sequence[Mapping[str, Any]],
    checkpoint_rows: Sequence[Mapping[str, Any]],
    symbol_ids: set[int],
) -> str:
    expected = {(symbol_id, level) for symbol_id in symbol_ids for level in LEVELS}
    checkpoints = {
        (int(row["symbol_id"]), int(row["timeframe"])): row for row in checkpoint_rows
    }
    manifest = []
    observed = set()
    for row in catalog_rows:
        key = (int(row["symbol_id"]), int(row["timeframe"]))
        observed.add(key)
        checkpoint = checkpoints.get(key)
        metadata = _json_object(checkpoint["metadata"]) if checkpoint else {}
        if (
            checkpoint is None
            or str(checkpoint["status"]) != "completed"
            or metadata.get("disposition") != "eligible"
            or str(row["state"]) != "present"
            or not bool(row["bounds_complete"])
            or row["min_ts"] != checkpoint["shard_start"]
            or row["max_ts"] != checkpoint["shard_end"]
        ):
            raise RuntimeError("Supplemental catalog scope does not match audit checkpoint bounds")
        manifest.append({
            "symbol_id": key[0],
            "timeframe": key[1],
            "state": "present",
            "bounds_complete": True,
            "min_ts": _json_value(row["min_ts"]),
            "max_ts": _json_value(row["max_ts"]),
            "updated_at": _json_value(row["updated_at"]),
        })
    if observed != expected or set(checkpoints) != expected:
        raise RuntimeError("Supplemental catalog scope is incomplete")
    return _manifest_sha256(manifest)


async def freeze_catalog_scope(
    conn: asyncpg.Connection,
    *,
    audit_run_id: str,
    catalog_generation_id: str,
    catalog_control_revision: int,
    symbol_ids: set[int],
) -> str:
    generation = await conn.fetchrow(ACTIVE_CATALOG_GENERATION_SQL)
    if (
        generation is None
        or str(generation["generation_id"]) != catalog_generation_id
        or int(generation["revision"]) != catalog_control_revision
    ):
        raise RuntimeError("Supplemental catalog generation or revision drifted")
    catalog_rows = await conn.fetch(
        ACTIVE_CATALOG_MANIFEST_SQL,
        uuid.UUID(catalog_generation_id),
        sorted(symbol_ids),
        list(LEVELS),
    )
    checkpoint_rows = await conn.fetch(
        "select symbol_id,timeframe,status,shard_start,shard_end,metadata "
        "from kline_audit_checkpoints where audit_run_id=$1::uuid "
        "and symbol_id=any($2::bigint[]) and timeframe=any($3::int[]) "
        "order by symbol_id,timeframe",
        audit_run_id,
        sorted(symbol_ids),
        list(LEVELS),
    )
    return validate_catalog_scope_rows(catalog_rows, checkpoint_rows, symbol_ids)


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
        scope_manifest_sha256 = await freeze_catalog_scope(
            conn,
            audit_run_id=provenance["canonical_audit_run_id"],
            catalog_generation_id=provenance["catalog_generation_id"],
            catalog_control_revision=provenance["catalog_control_revision"],
            symbol_ids={row.symbol_id for row in dispositions},
        )
        scoped_source = dict(source)
        scoped_source["parameters"] = {
            **_json_object(source["parameters"]),
            "scope": "supplemental",
            "supplemental_contract_version": "module-c-supplemental-selection-v2",
            "supplemental_symbols": list(symbols),
            "supplemental_catalog_manifest_sha256": scope_manifest_sha256,
        }
        await revalidate_strict_v2_build(conn, scoped_source, build_id=args.source_build_id)
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
            "supplemental_contract_version": "module-c-supplemental-selection-v2",
            "supplemental_symbols": list(symbols),
            "supplemental_catalog_manifest_sha256": scope_manifest_sha256,
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
