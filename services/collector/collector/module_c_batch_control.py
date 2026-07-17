from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from collector.chan_module_c_recompute import ensure_recompute_batch_on_connection
from collector.lifecycle_reconciliation import build_reconciliation
from collector.module_c_execution_report import HEAD_COVERAGE_SQL
from collector.module_c_eligibility import (
    CODE_TO_TIMEFRAME,
    Disposition,
    FRESHNESS_CONTRACT_VERSION,
    _SHA256_RE,
    _load_strict_inputs,
    _stable_hash,
    _write_outputs,
    build_summary,
    parse_freshness_contract,
)
from trading_protocol import MODULE_C_CONFIG_HASH


LEVELS = (5, 30, 1440, 10080, 43200)
LEVEL_NAMES = tuple(CODE_TO_TIMEFRAME[level] for level in LEVELS)
REQUIRED_CANARY_TRAITS = frozenset(
    {"main_board", "chinext", "star", "bj", "suspended_or_sparse", "gap", "price_limit", "long_history"}
)
STRICT_V2_PROVENANCE_FIELDS = (
    "canonical_audit_run_id",
    "audit_evidence_sha256",
    "audit_checkpoint_sha256",
    "freshness_contract_version",
    "freshness_contract_sha256",
    "catalog_generation_id",
    "catalog_control_revision",
    "catalog_manifest_sha256",
    "audit_active_universe_sha256",
)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _strict_v2_provenance(
    source: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    source_parameters = _json_object(source["parameters"])
    try:
        provenance = {
            "canonical_audit_run_id": str(uuid.UUID(str(source["canonical_audit_run_id"]))),
            "audit_evidence_sha256": str(source["audit_evidence_sha256"]),
            "audit_checkpoint_sha256": str(source["audit_checkpoint_sha256"]),
            "freshness_contract_version": str(source["freshness_contract_version"]),
            "freshness_contract_sha256": str(source["freshness_contract_sha256"]),
            "catalog_generation_id": str(uuid.UUID(str(source["catalog_generation_id"]))),
            "catalog_control_revision": int(source["catalog_control_revision"]),
            "catalog_manifest_sha256": str(source["catalog_manifest_sha256"]),
            "audit_active_universe_sha256": str(source["audit_active_universe_sha256"]),
        }
        parameter_provenance = {
            "canonical_audit_run_id": str(
                uuid.UUID(str(source_parameters["canonical_audit_run_id"]))
            ),
            "audit_evidence_sha256": str(source_parameters["audit_evidence_sha256"]),
            "audit_checkpoint_sha256": str(source_parameters["audit_checkpoint_sha256"]),
            "freshness_contract_version": str(
                source_parameters["freshness_contract_version"]
            ),
            "freshness_contract_sha256": str(source_parameters["freshness_contract_sha256"]),
            "catalog_generation_id": str(
                uuid.UUID(str(source_parameters["catalog_generation_id"]))
            ),
            "catalog_control_revision": int(source_parameters["catalog_control_revision"]),
            "catalog_manifest_sha256": str(source_parameters["catalog_manifest_sha256"]),
            "audit_active_universe_sha256": str(
                source_parameters["audit_active_universe_sha256"]
            ),
        }
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise RuntimeError(
            "Strict-v2 eligibility provenance is incomplete or inconsistent"
        ) from error
    freshness_contract = source_parameters.get("freshness_contract")
    try:
        validated_freshness = parse_freshness_contract(freshness_contract)
    except (TypeError, ValueError) as error:
        raise RuntimeError(
            "Strict-v2 eligibility provenance is incomplete or inconsistent"
        ) from error
    if (
        source_parameters.get("policy") != "strict-v2"
        or provenance != parameter_provenance
        or any(
            not _SHA256_RE.fullmatch(provenance[field])
            for field in (
                "audit_evidence_sha256",
                "audit_checkpoint_sha256",
                "freshness_contract_sha256",
                "catalog_manifest_sha256",
                "audit_active_universe_sha256",
            )
        )
        or provenance["freshness_contract_version"] != FRESHNESS_CONTRACT_VERSION
        or validated_freshness.sha256 != provenance["freshness_contract_sha256"]
    ):
        raise RuntimeError("Strict-v2 eligibility provenance is incomplete or inconsistent")
    parameters = {
        "policy": "strict-v2",
        "canonical_audit_run_id": str(provenance["canonical_audit_run_id"]),
        "audit_evidence_sha256": str(provenance["audit_evidence_sha256"]),
        "audit_checkpoint_sha256": str(provenance["audit_checkpoint_sha256"]),
        "freshness_contract": validated_freshness.normalized,
        "freshness_contract_version": str(provenance["freshness_contract_version"]),
        "freshness_contract_sha256": str(provenance["freshness_contract_sha256"]),
        "catalog_generation_id": str(provenance["catalog_generation_id"]),
        "catalog_control_revision": int(provenance["catalog_control_revision"]),
        "catalog_manifest_sha256": str(provenance["catalog_manifest_sha256"]),
        "audit_active_universe_sha256": str(
            provenance["audit_active_universe_sha256"]
        ),
    }
    return provenance, parameters


def load_selection(path: Path) -> tuple[tuple[str, ...], str, dict[str, Any]]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    if payload.get("contract_version") != "module-c-canary-selection-v1":
        raise ValueError("Unsupported canary selection contract_version")
    entries = payload.get("symbols")
    if not isinstance(entries, list) or len(entries) != 20:
        raise ValueError("Canary selection must contain exactly 20 symbols")
    names = tuple(str(entry.get("symbol") or "").strip().upper() for entry in entries)
    if any(not name or "." not in name for name in names) or len(set(names)) != 20:
        raise ValueError("Canary selection symbols must be 20 unique canonical names")
    traits = {
        str(trait)
        for entry in entries
        for trait in (entry.get("traits") if isinstance(entry.get("traits"), list) else [])
    }
    missing_traits = sorted(REQUIRED_CANARY_TRAITS - traits)
    if missing_traits:
        raise ValueError(f"Canary selection is missing required traits: {', '.join(missing_traits)}")
    by_trait = {
        trait: [entry for entry in entries if trait in (entry.get("traits") or [])]
        for trait in REQUIRED_CANARY_TRAITS
    }
    checks = {
        "bj": lambda name: name.endswith(".BJ"),
        "star": lambda name: name.endswith(".SH") and name.split(".", 1)[0].startswith(("688", "689")),
        "chinext": lambda name: name.endswith(".SZ") and name.split(".", 1)[0].startswith(("300", "301")),
        "main_board": lambda name: (
            name.endswith(".SH") and name.split(".", 1)[0].startswith(("600", "601", "603", "605"))
        ) or (
            name.endswith(".SZ") and name.split(".", 1)[0].startswith(("000", "001", "002", "003"))
        ),
    }
    for trait, predicate in checks.items():
        if not any(predicate(str(entry.get("symbol") or "").upper()) for entry in by_trait[trait]):
            raise ValueError(f"Canary selection trait {trait} is not supported by its symbol identity")
    for trait in ("suspended_or_sparse", "gap", "price_limit", "long_history"):
        if not any(entry.get("evidence") for entry in by_trait[trait]):
            raise ValueError(f"Canary selection trait {trait} requires auditable evidence")
    return names, hashlib.sha256(raw).hexdigest(), payload


async def validate_strict_build(
    conn: asyncpg.Connection,
    build: Mapping[str, Any],
    *,
    build_id: str,
    require_v2: bool = False,
) -> None:
    parameters = _json_object(build["parameters"])
    policy = parameters.get("policy")
    if policy == "strict-v2":
        provenance, _copied_parameters = _strict_v2_provenance(build)
        audit_run_id = provenance["canonical_audit_run_id"]
    else:
        audit_run_id = parameters.get("canonical_audit_run_id")
    if (
        policy not in {"strict-v1", "strict-v2"}
        or (require_v2 and policy != "strict-v2")
        or not audit_run_id
    ):
        raise RuntimeError("Eligibility build is not bound to a completed strict canonical audit")
    audit_status = await conn.fetchval(
        "select status from kline_audit_runs where audit_run_id=$1::uuid", audit_run_id
    )
    if audit_status != "completed":
        raise RuntimeError("Eligibility canonical audit is not completed")
    contract = await conn.fetchrow(
        """
        select count(*)::integer row_count,
               count(distinct symbol_id)::integer symbol_count,
               count(*) filter (where eligible and unresolved_rows <> 0)::integer unresolved_eligible,
               count(distinct timeframe)::integer timeframe_count
          from module_c_eligibility where build_id=$1::uuid
        """,
        build_id,
    )
    if (
        int(contract["row_count"]) != int(build["disposition_rows"])
        or int(contract["symbol_count"]) != int(build["active_symbols"])
        or int(contract["timeframe_count"]) != 5
        or int(contract["unresolved_eligible"]) != 0
    ):
        raise RuntimeError("Eligibility build fails the strict five-level contract")


async def revalidate_strict_v2_build(
    conn: asyncpg.Connection,
    build: Mapping[str, Any],
    *,
    build_id: str,
) -> None:
    """Revalidate one frozen build against the current strict-v2 input snapshot."""
    provenance, parameters = _strict_v2_provenance(build)
    freshness = parse_freshness_contract(parameters["freshness_contract"])
    strict = await _load_strict_inputs(
        conn,
        provenance["canonical_audit_run_id"],
        freshness,
    )
    observed = {
        "canonical_audit_run_id": provenance["canonical_audit_run_id"],
        "audit_evidence_sha256": strict.audit_evidence_sha256,
        "audit_checkpoint_sha256": strict.audit_checkpoint_sha256,
        "freshness_contract_version": freshness.contract_version,
        "freshness_contract_sha256": freshness.sha256,
        "catalog_generation_id": str(strict.catalog_generation_id),
        "catalog_control_revision": strict.catalog_control_revision,
        "catalog_manifest_sha256": strict.catalog_manifest_sha256,
        "audit_active_universe_sha256": strict.audit_active_universe_sha256,
    }
    if observed != provenance:
        raise RuntimeError("Strict-v2 eligibility provenance drifted after freeze")
    await validate_strict_build(conn, build, build_id=build_id, require_v2=True)


async def validate_pristine_task_manifest(
    conn: asyncpg.Connection,
    *,
    batch_id: int,
    build_id: str,
    disposition_rows: int,
) -> None:
    contract = await conn.fetchrow(
        """
        with expected as materialized (
            select symbol_id, symbol, timeframe, eligible, reasons, covered_until,
                   case when eligible then 'pending' else 'excluded' end expected_status,
                   mod((hashtextextended(symbol, 0) & 2147483647)::integer, 1024)::smallint
                       expected_shard_bucket,
                   coalesce((
                       select jsonb_object_agg(head.mode, head.run_id)
                         from scheme2_chan_c_published_heads head
                        where head.symbol_id = eligibility.symbol_id
                          and head.chan_level = eligibility.timeframe
                          and head.base_timeframe = eligibility.timeframe
                          and head.status = 'published'
                   ), '{}'::jsonb) expected_heads
              from module_c_eligibility eligibility
             where eligibility.build_id=$2::uuid
        ), actual as materialized (
            select symbol_id, symbol, chan_level, eligible, exclusion_reasons,
                   target_bar_until, shard_bucket, expected_heads, status,
                   attempts, lease_version,
                   worker_id, claim_token, lease_until, lease_heartbeat_at,
                   run_id, bar_count, stroke_count, segment_count, center_count,
                   signal_count, last_error, started_at, finished_at
              from chan_c_full_recompute_tasks
             where batch_id=$1
        ), mismatches as (
            select 1
              from expected
              full join actual
                on actual.symbol_id=expected.symbol_id
               and actual.chan_level=expected.timeframe
             where expected.symbol_id is null
                or actual.symbol_id is null
                or actual.symbol is distinct from expected.symbol
                or actual.eligible is distinct from expected.eligible
                or actual.exclusion_reasons is distinct from expected.reasons
                or actual.target_bar_until is distinct from expected.covered_until
                or actual.shard_bucket is distinct from expected.expected_shard_bucket
                or actual.expected_heads is distinct from expected.expected_heads
                or actual.status is distinct from expected.expected_status
                or actual.attempts <> 0 or actual.lease_version <> 0
                or actual.worker_id is not null or actual.claim_token is not null
                or actual.lease_until is not null or actual.lease_heartbeat_at is not null
                or actual.run_id is not null or actual.bar_count is not null
                or actual.stroke_count is not null or actual.segment_count is not null
                or actual.center_count is not null or actual.signal_count is not null
                or actual.last_error is not null or actual.started_at is not null
                or actual.finished_at is not null
        )
        select (select count(*) from expected)::integer expected_rows,
               (select count(*) from actual)::integer task_rows,
               (select count(*) from mismatches)::integer mismatch_rows
        """,
        batch_id,
        build_id,
    )
    if (
        contract is None
        or int(contract["expected_rows"]) != disposition_rows
        or int(contract["task_rows"]) != disposition_rows
        or int(contract["mismatch_rows"]) != 0
    ):
        raise RuntimeError("Cannot activate a drifted or non-pristine task manifest")


def validate_activation_identity(batch: Mapping[str, Any]) -> None:
    effective_config = _json_object(batch["effective_config"])
    max_attempts = effective_config.get("max_attempts")
    expected_config = {
        "contract": "module-c-native-five-level-v1",
        "levels": list(LEVEL_NAMES),
        "modes": ["confirmed", "predictive"],
        "concurrency_per_worker": 1,
        "shard_count": int(batch["child_shard_count"]),
        "eligibility_build_id": str(batch["build_id"]),
        "max_attempts": max_attempts,
    }
    if (
        batch["batch_kind"] not in {"canary", "baseline"}
        or not isinstance(max_attempts, int)
        or isinstance(max_attempts, bool)
        or max_attempts < 1
        or effective_config != expected_config
        or str(batch["parent_config_hash"]) != str(batch["config_hash"])
        or str(batch["parent_manifest_hash"]) != str(batch["manifest_hash"])
        or str(batch["child_config_hash"]) != str(batch["config_hash"])
        or str(batch["child_run_group_id"]) != str(batch["parent_run_group_id"])
        or str(batch["child_publication_namespace"])
        != str(batch["parent_publication_namespace"])
        or str(batch["child_profile_id"]) != str(batch["parent_profile_id"])
        or int(batch["child_active_symbols"]) != int(batch["active_symbols"])
        or int(batch["child_disposition_rows"]) != int(batch["disposition_rows"])
    ):
        raise RuntimeError("Cannot activate a drifted Module C batch identity")


def validate_terminal_tasks(
    *, child_status: str, disposition_rows: int, statuses: Mapping[str, int]
) -> None:
    if child_status != "completed":
        raise RuntimeError(f"Full-recompute child is not completed: {child_status}")
    observed = sum(int(value) for value in statuses.values())
    if observed != disposition_rows:
        raise RuntimeError(f"Task manifest is incomplete: tasks={observed} expected={disposition_rows}")
    blocking = sum(int(statuses.get(status, 0)) for status in ("pending", "running", "failed"))
    if blocking:
        raise RuntimeError(f"Task manifest has {blocking} blocking tasks")


def validate_canary_report(path: Path | None, *, batch_id: int) -> tuple[str, dict[str, Any]]:
    if path is None:
        raise RuntimeError("--canary-ab-report is required to seal a canary")
    raw = path.read_bytes()
    report = json.loads(raw.decode("utf-8"))
    if (report.get("selector") or {}).get("batch_id") != batch_id:
        raise RuntimeError("Canary A/B report batch_id does not match")
    if (
        report.get("passed") is not True
        or int(report.get("symbols") or 0) != 20
        or int(report.get("failed_runs") or 0) != 0
        or int(report.get("difference_count") or 0) != 0
    ):
        raise RuntimeError("Canary A/B report did not pass the strict contract")
    return hashlib.sha256(raw).hexdigest(), report


def validate_canary_run_set(
    report: Mapping[str, Any], expected_rows: Sequence[Mapping[str, Any]]
) -> None:
    expected = {
        (int(row["run_id"]), str(row["symbol"]), CODE_TO_TIMEFRAME[int(row["chan_level"])])
        for row in expected_rows
    }
    reported_rows = report.get("runs")
    if not isinstance(reported_rows, list):
        raise RuntimeError("Canary A/B report is missing its run evidence")
    reported: set[tuple[int, str, str]] = set()
    for row in reported_rows:
        if (
            row.get("passed") is not True
            or row.get("config_hash") != MODULE_C_CONFIG_HASH
            or sorted(row.get("modes") or []) != ["confirmed", "predictive"]
        ):
            raise RuntimeError("Canary A/B report contains a non-passing run identity")
        reported.add((int(row["run_id"]), str(row["symbol"]), str(row["level"])))
    if reported != expected or int(report.get("published_runs") or 0) != len(expected):
        raise RuntimeError("Canary A/B report run set does not match completed tasks")


async def freeze_canary(conn: asyncpg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    symbols, selection_sha, selection = load_selection(args.selection_manifest)
    async with conn.transaction(isolation="serializable"):
        source = await conn.fetchrow(
            """
            select build_id::text, config_hash, active_symbols, disposition_rows,
                   parameters, summary, canonical_audit_run_id::text,
                   audit_evidence_sha256, audit_checkpoint_sha256,
                   freshness_contract_version, freshness_contract_sha256,
                   catalog_generation_id::text, catalog_control_revision,
                   catalog_manifest_sha256, audit_active_universe_sha256
              from module_c_eligibility_builds
             where build_id=$1::uuid
             for share
            """,
            args.source_build_id,
        )
        if source is None:
            raise RuntimeError(f"Unknown source eligibility build: {args.source_build_id}")
        if str(source["config_hash"]) != MODULE_C_CONFIG_HASH:
            raise RuntimeError("Source eligibility config_hash is not the production Module C contract")
        await validate_strict_build(
            conn, source, build_id=args.source_build_id, require_v2=True
        )
        source_provenance, copied_parameters = _strict_v2_provenance(source)
        source_rows = int(await conn.fetchval(
            "select count(*) from module_c_eligibility where build_id=$1::uuid",
            args.source_build_id,
        ))
        if source_rows != int(source["disposition_rows"]):
            raise RuntimeError("Source eligibility build is incomplete")
        rows = await conn.fetch(
            """
            select symbol_id, symbol, timeframe, eligible, reasons, covered_until, unresolved_rows
              from module_c_eligibility
             where build_id=$1::uuid and symbol=any($2::text[])
             order by symbol_id, array_position($3::int[], timeframe)
            """,
            args.source_build_id,
            list(symbols),
            list(LEVELS),
        )
        selected_names = {str(row["symbol"]).upper() for row in rows}
        if selected_names != set(symbols) or len(rows) != 100:
            missing = sorted(set(symbols) - selected_names)
            raise RuntimeError(f"Source build lacks the frozen five-level selection: {missing!r}")
        counts = Counter(str(row["symbol"]).upper() for row in rows)
        if any(counts[name] != 5 for name in symbols):
            raise RuntimeError("Every canary symbol must have exactly five source dispositions")
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
        if any(not any(row.eligible for row in dispositions if row.symbol.upper() == name) for name in symbols):
            raise RuntimeError("Every canary symbol must retain at least one eligible scope")
        manifest_hash = _stable_hash(row.json_record() for row in dispositions)
        active_hash = _stable_hash(
            name for _, name in sorted({(row.symbol_id, row.symbol) for row in dispositions})
        )
        build_id = uuid.UUID(args.build_id) if args.build_id else uuid.uuid4()
        summary = build_summary(dispositions)
        parameters = {
            **copied_parameters,
            "scope": "canary",
            "source_build_id": args.source_build_id,
            "selection_contract_version": selection["contract_version"],
            "selection_manifest_sha256": selection_sha,
            "selection_traits": sorted(
                {str(trait) for entry in selection["symbols"] for trait in entry.get("traits", [])}
            ),
        }
        inserted = await conn.execute(
            """
            insert into module_c_eligibility_builds (
                build_id, manifest_version, config_hash, active_universe_hash,
                manifest_hash, active_symbols, disposition_rows, parameters, summary,
                canonical_audit_run_id,audit_evidence_sha256,audit_checkpoint_sha256,
                freshness_contract_version,freshness_contract_sha256,catalog_generation_id,
                catalog_control_revision,catalog_manifest_sha256,audit_active_universe_sha256
            ) values ($1,$2,$3,$4,$5,20,100,$6::jsonb,$7::jsonb,$8::uuid,$9,$10,$11,$12,
                      $13::uuid,$14,$15,$16)
            on conflict (build_id) do nothing
            """,
            build_id,
            args.manifest_version,
            MODULE_C_CONFIG_HASH,
            active_hash,
            manifest_hash,
            json.dumps(parameters, sort_keys=True),
            json.dumps(summary, sort_keys=True),
            source_provenance["canonical_audit_run_id"],
            source_provenance["audit_evidence_sha256"],
            source_provenance["audit_checkpoint_sha256"],
            source_provenance["freshness_contract_version"],
            source_provenance["freshness_contract_sha256"],
            source_provenance["catalog_generation_id"],
            source_provenance["catalog_control_revision"],
            source_provenance["catalog_manifest_sha256"],
            source_provenance["audit_active_universe_sha256"],
        )
        if inserted.endswith(" 1"):
            await conn.copy_records_to_table(
                "module_c_eligibility",
                records=[
                    (
                        build_id,
                        row.symbol_id,
                        row.symbol,
                        row.timeframe_code,
                        row.eligible,
                        list(row.reasons),
                        row.covered_until,
                        row.unresolved_rows,
                    )
                    for row in dispositions
                ],
                columns=(
                    "build_id", "symbol_id", "symbol", "timeframe", "eligible",
                    "reasons", "covered_until", "unresolved_rows",
                ),
            )
        existing = await conn.fetchrow(
            """
            select manifest_version, config_hash, active_universe_hash, manifest_hash,
                   active_symbols, disposition_rows, parameters,
                   canonical_audit_run_id::text,audit_evidence_sha256,
                   audit_checkpoint_sha256,freshness_contract_version,
                   freshness_contract_sha256,catalog_generation_id::text,
                   catalog_control_revision,catalog_manifest_sha256,
                   audit_active_universe_sha256
              from module_c_eligibility_builds where build_id=$1
            """,
            build_id,
        )
        expected = {
            "manifest_version": args.manifest_version,
            "config_hash": MODULE_C_CONFIG_HASH,
            "active_universe_hash": active_hash,
            "manifest_hash": manifest_hash,
            "active_symbols": 20,
            "disposition_rows": 100,
            "parameters": parameters,
            **source_provenance,
        }
        actual = {**dict(existing), "parameters": _json_object(existing["parameters"])}
        if actual != expected:
            raise RuntimeError("Existing canary eligibility build does not exactly match")
        persisted = int(await conn.fetchval(
            "select count(*) from module_c_eligibility where build_id=$1", build_id
        ))
        if persisted != 100:
            raise RuntimeError(f"Canary eligibility rows are incomplete: {persisted}/100")
        metadata = {
            "build_id": str(build_id),
            "manifest_version": args.manifest_version,
            "config_hash": MODULE_C_CONFIG_HASH,
            "active_universe_hash": active_hash,
            "manifest_hash": manifest_hash,
            "active_symbols": 20,
            **source_provenance,
            "excluded_summary": {
                "excluded_scopes": sum(not row.eligible for row in dispositions),
                "reasons": dict(sorted(Counter(
                    reason for row in dispositions for reason in row.reasons
                ).items())),
            },
            **summary,
        }
        _write_outputs(args.output_dir, dispositions, metadata)
    return metadata


async def _batch_watermark(conn: asyncpg.Connection, build_id: str) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        select timeframe, min(covered_until) minimum, max(covered_until) maximum,
               count(*) filter (where eligible)::integer eligible,
               count(*) filter (where not eligible)::integer excluded
          from module_c_eligibility where build_id=$1::uuid
         group by timeframe order by timeframe
        """,
        build_id,
    )
    return {
        CODE_TO_TIMEFRAME[int(row["timeframe"])]: {
            "minimum": row["minimum"].isoformat() if row["minimum"] else None,
            "maximum": row["maximum"].isoformat() if row["maximum"] else None,
            "eligible": int(row["eligible"]),
            "excluded": int(row["excluded"]),
        }
        for row in rows
    }


async def prepare_batch(conn: asyncpg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    async with conn.transaction(isolation="serializable"):
        await conn.execute("select pg_advisory_xact_lock(hashtext($1))", args.batch_key)
        build = await conn.fetchrow(
            """
            select build_id::text, config_hash, manifest_hash, active_symbols,
                   disposition_rows, parameters, canonical_audit_run_id::text,
                   audit_evidence_sha256, audit_checkpoint_sha256,
                   freshness_contract_version, freshness_contract_sha256,
                   catalog_generation_id::text, catalog_control_revision,
                   catalog_manifest_sha256, audit_active_universe_sha256
              from module_c_eligibility_builds where build_id=$1::uuid for share
            """,
            args.eligibility_build_id,
        )
        if build is None or str(build["config_hash"]) != MODULE_C_CONFIG_HASH:
            raise RuntimeError("Eligibility build is missing or has the wrong config_hash")
        await revalidate_strict_v2_build(
            conn, build, build_id=args.eligibility_build_id
        )
        rows = int(await conn.fetchval(
            "select count(*) from module_c_eligibility where build_id=$1::uuid",
            args.eligibility_build_id,
        ))
        if rows != int(build["disposition_rows"]) or rows != int(build["active_symbols"]) * 5:
            raise RuntimeError("Eligibility build is incomplete")
        parameters = _json_object(build["parameters"])
        approved_canary = None
        if args.batch_kind == "canary":
            if int(build["active_symbols"]) != 20 or parameters.get("scope") != "canary":
                raise RuntimeError("Canary batches require a frozen 20-symbol canary eligibility build")
        else:
            if parameters.get("scope") == "canary":
                raise RuntimeError("A baseline batch cannot use a canary eligibility build")
            if not args.approved_canary_batch_id:
                raise RuntimeError("A baseline batch requires --approved-canary-batch-id")
            approved_canary = await conn.fetchrow(
                """
                select parent.id, parent.status, parent.code_commit, parent.image_digest,
                       parent.vendor_manifest_sha256, parent.config_hash,
                       parent.publication_namespace, parent.profile_id,
                       parent.audit_references, parent.effective_config,
                       child.status child_status, child.shard_count, build.parameters
                  from chan_c_batches parent
                  join chan_c_full_recompute_batches child on child.batch_id=parent.id
                  join module_c_eligibility_builds build on build.build_id=child.eligibility_build_id
                 where parent.id=$1 and parent.batch_kind='canary'
                 for share of parent, child, build
                """,
                args.approved_canary_batch_id,
            )
            source_id = _json_object(approved_canary["parameters"]).get("source_build_id") if approved_canary else None
            if (
                approved_canary is None
                or approved_canary["status"] != "sealed"
                or approved_canary["child_status"] != "completed"
                or source_id != args.eligibility_build_id
            ):
                raise RuntimeError("Approved canary is not sealed against this baseline eligibility build")
            for field in ("code_commit", "image_digest", "vendor_manifest_sha256", "config_hash"):
                expected_value = MODULE_C_CONFIG_HASH if field == "config_hash" else getattr(args, field)
                if str(approved_canary[field]) != expected_value:
                    raise RuntimeError(f"Approved canary {field} does not match")
            if (
                approved_canary["publication_namespace"] != args.publication_namespace
                or approved_canary["profile_id"] != args.profile_id
                or int(approved_canary["shard_count"]) != args.shard_count
            ):
                raise RuntimeError("Approved canary execution identity does not match")
            canary_config = _json_object(approved_canary["effective_config"])
            expected_contract = {
                "contract": "module-c-native-five-level-v1",
                "levels": list(LEVEL_NAMES),
                "modes": ["confirmed", "predictive"],
                "concurrency_per_worker": 1,
                "shard_count": args.shard_count,
                "max_attempts": args.max_attempts,
            }
            if any(canary_config.get(key) != value for key, value in expected_contract.items()):
                raise RuntimeError("Approved canary effective config does not match")
            canary_references = approved_canary["audit_references"]
            canary_references = list(
                canary_references
                if isinstance(canary_references, list)
                else json.loads(canary_references)
            )
            if not any(reference.get("type") == "canary_ab" for reference in canary_references):
                raise RuntimeError("Approved canary lacks a strict canary A/B attestation")
        watermark = await _batch_watermark(conn, args.eligibility_build_id)
        audit_references = [
            {"type": "eligibility_build", "build_id": args.eligibility_build_id},
            *(
                [{"type": "canonical_audit", "audit_run_id": parameters["canonical_audit_run_id"]}]
                if parameters.get("canonical_audit_run_id") else []
            ),
            *(
                [{"type": "approved_canary", "batch_id": args.approved_canary_batch_id}]
                if args.approved_canary_batch_id else []
            ),
        ]
        effective_config = {
            "contract": "module-c-native-five-level-v1",
            "levels": list(LEVEL_NAMES),
            "modes": ["confirmed", "predictive"],
            "concurrency_per_worker": 1,
            "shard_count": args.shard_count,
            "eligibility_build_id": args.eligibility_build_id,
            "max_attempts": args.max_attempts,
        }
        inserted_id = await conn.fetchval(
            """
            insert into chan_c_batches (
                batch_key, publication_namespace, profile_id, run_group_id, batch_kind,
                status, code_commit, image_digest, vendor_manifest_sha256,
                effective_config, config_hash, eligible_manifest_uri,
                eligible_manifest_sha256, input_watermark, audit_references, notes
            ) values ($1,$2,$3,$4,$5,'planned',$6,$7,$8,$9::jsonb,$10,$11,$12,$13::jsonb,$14::jsonb,$15)
            on conflict do nothing returning id
            """,
            args.batch_key, args.publication_namespace, args.profile_id, args.run_group_id,
            args.batch_kind, args.code_commit, args.image_digest, args.vendor_manifest_sha256,
            json.dumps(effective_config, sort_keys=True), MODULE_C_CONFIG_HASH,
            f"db://module_c_eligibility/{args.eligibility_build_id}", str(build["manifest_hash"]),
            json.dumps(watermark, sort_keys=True), json.dumps(audit_references, sort_keys=True), args.notes,
        )
        parent = await conn.fetchrow(
            """
            select id, status, code_commit, image_digest, vendor_manifest_sha256,
                   config_hash, run_group_id, batch_kind, eligible_manifest_sha256
              from chan_c_batches where batch_key=$1 for update
            """,
            args.batch_key,
        )
        if parent is None:
            raise RuntimeError("Batch key conflicts with another immutable batch identity")
        expected = {
            "code_commit": args.code_commit,
            "image_digest": args.image_digest,
            "vendor_manifest_sha256": args.vendor_manifest_sha256,
            "config_hash": MODULE_C_CONFIG_HASH,
            "run_group_id": args.run_group_id,
            "batch_kind": args.batch_kind,
            "eligible_manifest_sha256": str(build["manifest_hash"]),
        }
        if {key: parent[key] for key in expected} != expected or parent["status"] != "planned":
            raise RuntimeError("Existing parent batch manifest does not exactly match")
        batch_id = int(inserted_id or parent["id"])
        await ensure_recompute_batch_on_connection(
            conn=conn,
            batch_id=batch_id,
            eligibility_build_id=args.eligibility_build_id,
            run_group_id=args.run_group_id,
            config_hash=MODULE_C_CONFIG_HASH,
            publication_namespace=args.publication_namespace,
            profile_id=args.profile_id,
            shard_count=args.shard_count,
            levels=list(LEVEL_NAMES),
            max_attempts=args.max_attempts,
            allow_create=True,
        )
    return {"batch_id": batch_id, "status": "planned", "created": inserted_id is not None}


async def activate_batch(conn: asyncpg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    async with conn.transaction(isolation="serializable"):
        row = await conn.fetchrow(
            """
            select parent.status parent_status, parent.batch_kind,
                   parent.config_hash parent_config_hash,
                   parent.eligible_manifest_sha256 parent_manifest_hash,
                   parent.effective_config,
                   parent.run_group_id parent_run_group_id,
                   parent.publication_namespace parent_publication_namespace,
                   parent.profile_id parent_profile_id,
                   child.status child_status,
                   child.disposition_rows child_disposition_rows,
                   child.active_symbols child_active_symbols,
                   child.shard_count child_shard_count,
                   child.config_hash child_config_hash,
                   child.run_group_id child_run_group_id,
                   child.publication_namespace child_publication_namespace,
                   child.profile_id child_profile_id,
                   child.eligibility_build_id::text build_id,
                   build.config_hash, build.manifest_hash, build.active_symbols,
                   build.disposition_rows, build.parameters,
                   build.canonical_audit_run_id::text,
                   build.audit_evidence_sha256, build.audit_checkpoint_sha256,
                   build.freshness_contract_version, build.freshness_contract_sha256,
                   build.catalog_generation_id::text, build.catalog_control_revision,
                   build.catalog_manifest_sha256, build.audit_active_universe_sha256,
                   (select count(*) from chan_c_full_recompute_tasks task where task.batch_id=parent.id) task_count
              from chan_c_batches parent
              join chan_c_full_recompute_batches child on child.batch_id=parent.id
              join module_c_eligibility_builds build on build.build_id=child.eligibility_build_id
             where parent.id=$1 for update of parent, child
            """,
            args.batch_id,
        )
        if row is None or row["parent_status"] != "planned" or row["child_status"] != "pending":
            raise RuntimeError("Only a complete planned/pending Module C batch can be activated")
        validate_activation_identity(row)
        if int(row["task_count"]) != int(row["child_disposition_rows"]):
            raise RuntimeError("Cannot activate an incomplete task manifest")
        await revalidate_strict_v2_build(
            conn, row, build_id=str(row["build_id"])
        )
        await validate_pristine_task_manifest(
            conn,
            batch_id=args.batch_id,
            build_id=str(row["build_id"]),
            disposition_rows=int(row["child_disposition_rows"]),
        )
        child_result = await conn.execute(
            """
            update chan_c_full_recompute_batches
               set status='running', started_at=coalesce(started_at, clock_timestamp()),
                   finished_at=null, updated_at=clock_timestamp()
             where batch_id=$1 and status='pending'
            """,
            args.batch_id,
        )
        if child_result != "UPDATE 1":
            raise RuntimeError("Failed to atomically activate the Module C parent/child batch")
        parent_result = await conn.execute(
            """
            update chan_c_batches
               set status='running'
             where id=$1 and status='planned'
            """,
            args.batch_id,
        )
        if parent_result != "UPDATE 1":
            raise RuntimeError("Failed to atomically activate the Module C parent/child batch")
    return {"batch_id": args.batch_id, "status": "running"}


async def status_batch(conn: asyncpg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        batch = await conn.fetchrow(
            """
            select parent.id, parent.batch_key, parent.batch_kind, parent.status parent_status,
                   child.status child_status, child.shard_count, child.disposition_rows,
                   max(task.updated_at) latest_update
              from chan_c_batches parent
              join chan_c_full_recompute_batches child on child.batch_id=parent.id
              left join chan_c_full_recompute_tasks task on task.batch_id=parent.id
             where parent.id=$1
             group by parent.id, child.batch_id
            """,
            args.batch_id,
        )
        if batch is None:
            raise RuntimeError(f"Unknown Module C batch: {args.batch_id}")
        rows = await conn.fetch(
            """
            select chan_level, status, count(*)::integer count
              from chan_c_full_recompute_tasks where batch_id=$1
             group by chan_level,status order by chan_level,status
            """,
            args.batch_id,
        )
    return {**dict(batch), "tasks": [dict(row) for row in rows]}


async def seal_batch(conn: asyncpg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    canary_sha = None
    canary_report = None
    async with conn.transaction(isolation="serializable"):
        batch = await conn.fetchrow(
            """
            select parent.id, parent.batch_kind, parent.status parent_status,
                   parent.audit_references, parent.publication_namespace,
                   parent.profile_id, parent.run_group_id, parent.config_hash,
                   child.status child_status,
                   child.disposition_rows
              from chan_c_batches parent
              join chan_c_full_recompute_batches child on child.batch_id=parent.id
             where parent.id=$1 for update of parent, child
            """,
            args.batch_id,
        )
        if batch is None or batch["parent_status"] != "running":
            raise RuntimeError("Only a running Module C batch can be sealed")
        if batch["batch_kind"] == "canary":
            canary_sha, canary_report = validate_canary_report(args.canary_ab_report, batch_id=args.batch_id)
        rows = await conn.fetch(
            "select status,count(*)::integer count from chan_c_full_recompute_tasks where batch_id=$1 group by status",
            args.batch_id,
        )
        statuses = {str(row["status"]): int(row["count"]) for row in rows}
        validate_terminal_tasks(
            child_status=str(batch["child_status"]),
            disposition_rows=int(batch["disposition_rows"]),
            statuses=statuses,
        )
        expected_runs = await conn.fetch(
            """
            select task.run_id, task.symbol, task.chan_level
              from chan_c_full_recompute_tasks task
              join chan_c_runs run on run.id=task.run_id
             where task.batch_id=$1 and task.eligible and task.status='completed'
               and run.batch_id=$1 and run.status='success'
               and run.publication_namespace=$2 and run.profile_id=$3
               and run.run_group_id=$4 and run.config_hash=$5
               and run.base_timeframe=task.chan_level
             order by task.symbol_id,task.chan_level
            """,
            args.batch_id,
            batch["publication_namespace"],
            batch["profile_id"],
            batch["run_group_id"],
            batch["config_hash"],
        )
        if len(expected_runs) != statuses.get("completed", 0):
            raise RuntimeError("Completed task run identities do not match the parent batch")
        coverage_rows = await conn.fetch(HEAD_COVERAGE_SQL, args.batch_id)
        coverage = {
            "missing": sum(int(row["missing"]) for row in coverage_rows),
            "missing_history": sum(int(row["missing_history"]) for row in coverage_rows),
            "missing_outbox": sum(int(row["missing_outbox"]) for row in coverage_rows),
            "outbox_incomplete": sum(int(row["outbox_incomplete"]) for row in coverage_rows),
        }
        if any(coverage.values()):
            raise RuntimeError(f"Head/history/outbox gate failed: {coverage!r}")
        profile_mismatches = int(await conn.fetchval(
            """
            with expected as (
                select task.symbol_id,task.chan_level,mode.mode
                  from chan_c_full_recompute_tasks task
                  cross join (values ('confirmed'::varchar),('predictive'::varchar)) mode(mode)
                 where task.batch_id=$1 and task.eligible
            )
            select count(*)
              from expected
              join scheme2_chan_c_published_heads head
                on head.symbol_id=expected.symbol_id and head.chan_level=expected.chan_level
               and head.base_timeframe=expected.chan_level and head.mode=expected.mode
               and head.status='published'
              join chan_c_runs run on run.id=head.run_id
              join chan_c_head_history history
                on history.new_run_id=head.run_id and history.symbol_id=expected.symbol_id
               and history.chan_level=expected.chan_level and history.mode=expected.mode
             where head.publication_namespace is distinct from $2
                or head.profile_id is distinct from $3
                or run.config_hash is distinct from $4
                or run.base_timeframe is distinct from expected.chan_level
                or history.config_hash is distinct from $4
                or history.base_timeframe is distinct from expected.chan_level
            """,
            args.batch_id,
            batch["publication_namespace"],
            batch["profile_id"],
            batch["config_hash"],
        ))
        if profile_mismatches:
            raise RuntimeError(f"Published head/history profile identity mismatches: {profile_mismatches}")
        reconciliation = await build_reconciliation(conn)
        if reconciliation["decision"] != "PASS":
            raise RuntimeError(f"Lifecycle reconciliation failed: {reconciliation['blockers']!r}")
        references = batch["audit_references"]
        references = list(references if isinstance(references, list) else json.loads(references))
        if canary_report is not None:
            eligible_tasks = statuses.get("completed", 0)
            validate_canary_run_set(canary_report, expected_runs)
            references.append({
                "type": "canary_ab", "sha256": canary_sha,
                "uri": str(args.canary_ab_report), "published_runs": eligible_tasks,
            })
        await conn.execute(
            """
            update chan_c_batches
               set status='sealed', sealed_at=clock_timestamp(), sealed_by=$2,
                   audit_references=$3::jsonb
             where id=$1 and status='running'
            """,
            args.batch_id,
            args.sealed_by,
            json.dumps(references, sort_keys=True),
        )
    return {"batch_id": args.batch_id, "status": "sealed"}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Control frozen Module C recompute batches")
    parser.add_argument("action", choices=("freeze-canary", "prepare", "activate", "status", "seal"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--batch-id", type=int)
    parser.add_argument("--source-build-id")
    parser.add_argument("--build-id")
    parser.add_argument("--manifest-version")
    parser.add_argument("--selection-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--batch-key")
    parser.add_argument("--batch-kind", choices=("canary", "baseline"), default="canary")
    parser.add_argument("--eligibility-build-id")
    parser.add_argument("--run-group-id")
    parser.add_argument("--code-commit")
    parser.add_argument("--image-digest")
    parser.add_argument("--vendor-manifest-sha256")
    parser.add_argument("--publication-namespace", default="production")
    parser.add_argument("--profile-id", default="module-c-native-5lvl")
    parser.add_argument("--shard-count", type=int, default=4)
    parser.add_argument("--max-attempts", type=int)
    parser.add_argument("--approved-canary-batch-id", type=int)
    parser.add_argument("--notes")
    parser.add_argument("--sealed-by", default=os.getenv("USERNAME") or "module-c-batch-control")
    parser.add_argument("--canary-ab-report", type=Path)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    required_by_action = {
        "freeze-canary": ("source_build_id", "manifest_version", "selection_manifest", "output_dir"),
        "prepare": (
            "batch_key", "eligibility_build_id", "run_group_id", "code_commit",
            "image_digest", "vendor_manifest_sha256",
            "max_attempts",
        ),
        "activate": ("batch_id",),
        "status": ("batch_id",),
        "seal": ("batch_id",),
    }
    missing = [name for name in required_by_action[args.action] if not getattr(args, name)]
    if missing:
        parser.error(args.action + " requires " + ", ".join(f"--{name.replace('_','-')}" for name in missing))
    if args.shard_count < 1:
        parser.error("--shard-count must be positive")
    if args.max_attempts is not None and args.max_attempts < 1:
        parser.error("--max-attempts must be positive")
    if args.vendor_manifest_sha256 and (
        len(args.vendor_manifest_sha256) != 64
        or any(char not in "0123456789abcdef" for char in args.vendor_manifest_sha256)
    ):
        parser.error("--vendor-manifest-sha256 must be 64 lowercase hexadecimal characters")
    return args


async def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    conn = await asyncpg.connect(args.database_url)
    try:
        handlers = {
            "freeze-canary": freeze_canary,
            "prepare": prepare_batch,
            "activate": activate_batch,
            "status": status_batch,
            "seal": seal_batch,
        }
        result = await handlers[args.action](conn, args)
    finally:
        await conn.close()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
