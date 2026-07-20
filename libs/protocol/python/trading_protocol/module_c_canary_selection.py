from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter
from fractions import Fraction
from typing import Any, Mapping


CONTRACT_VERSION = "module-c-canary-selection-v2"
NON_BJ_CONTRACT_VERSION = "module-c-canary-selection-v3"
BARS_PER_COMPLETE_5F_SESSION = 49
ACTIVITY_BASIS = "pinned-audit-5f-rows-per-49-bar-1d-session-v1"
BOARD_ORDER = ("main_board", "chinext", "star", "bj")
BOARD_QUOTAS = {board: 5 for board in BOARD_ORDER}
BOUNDARY_COUNTS = {"lower": 2, "middle": 1, "upper": 2}
BOUNDARY_ORDER = ("lower", "lower", "middle", "upper", "upper")
NON_BJ_BOARD_ORDER = ("main_board", "chinext")
NON_BJ_BOARD_QUOTAS = {"main_board": 10, "chinext": 10}
NON_BJ_BOUNDARY_COUNTS = {"lower": 3, "middle": 4, "upper": 3}
NON_BJ_BOUNDARY_ORDER = (
    "lower", "lower", "lower", "middle", "middle", "middle", "middle", "upper", "upper", "upper",
)
FRESHNESS_CONTRACT_VERSION = "module-c-authoritative-freshness-v1"
STRICT_PROVENANCE_FIELDS = (
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
SOURCE_FIELDS = ("eligibility_build_id", "eligibility_manifest_sha256", *STRICT_PROVENANCE_FIELDS)
SHA_FIELDS = frozenset(
    {
        "eligibility_manifest_sha256",
        "audit_evidence_sha256",
        "audit_checkpoint_sha256",
        "freshness_contract_sha256",
        "catalog_manifest_sha256",
        "audit_active_universe_sha256",
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LEVELS = {"5f", "30f", "1d", "1w", "1m"}


def canonical_selection_sha256(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload + b"\n").hexdigest()


def classify_board(symbol: str) -> str | None:
    code, separator, exchange = symbol.strip().upper().partition(".")
    if not separator:
        return None
    if exchange == "BJ":
        return "bj"
    if exchange == "SH" and code.startswith(("688", "689")):
        return "star"
    if exchange == "SZ" and code.startswith(("300", "301")):
        return "chinext"
    if (
        exchange == "SH" and code.startswith(("600", "601", "603", "605"))
    ) or (
        exchange == "SZ" and code.startswith(("000", "001", "002", "003"))
    ):
        return "main_board"
    return None


def _integer(value: Any, label: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if value < (1 if positive else 0):
        raise ValueError(f"{label} is out of range")
    return value


def normalize_selection_source(source: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(source, Mapping) or set(source) != set(SOURCE_FIELDS):
        raise ValueError("selection source must use the exact strict-v2 schema")
    normalized = {field: str(source[field]) for field in SOURCE_FIELDS}
    normalized["catalog_control_revision"] = _integer(
        source["catalog_control_revision"], "catalog_control_revision"
    )
    for field in SHA_FIELDS:
        if not _SHA256_RE.fullmatch(normalized[field]):
            raise ValueError(f"selection source {field} must be lowercase SHA-256")
    for field in ("eligibility_build_id", "canonical_audit_run_id", "catalog_generation_id"):
        try:
            normalized[field] = str(uuid.UUID(normalized[field]))
        except (ValueError, AttributeError) as error:
            raise ValueError(f"selection source {field} must be a UUID") from error
    if normalized["freshness_contract_version"] != FRESHNESS_CONTRACT_VERSION:
        raise ValueError("selection source freshness contract version is unsupported")
    return normalized


def selection_policy() -> dict[str, Any]:
    return selection_policy_for(CONTRACT_VERSION)


def selection_policy_for(contract_version: str) -> dict[str, Any]:
    if contract_version == CONTRACT_VERSION:
        board_quotas = BOARD_QUOTAS
        boundary_counts = BOUNDARY_COUNTS
    elif contract_version == NON_BJ_CONTRACT_VERSION:
        board_quotas = NON_BJ_BOARD_QUOTAS
        boundary_counts = NON_BJ_BOUNDARY_COUNTS
    else:
        raise ValueError("Unsupported canary selection contract_version")
    return {
        "symbol_count": sum(board_quotas.values()),
        "board_quotas": dict(board_quotas),
        "activity_boundary_counts_per_board": dict(boundary_counts),
        "activity_basis": ACTIVITY_BASIS,
        "bars_per_complete_5f_session": BARS_PER_COMPLETE_5F_SESSION,
        "legacy_free_text_scenario_traits": "not_authoritative_without_frozen_evidence",
        "tie_break": ["activity_ratio", "symbol_id", "symbol"],
    }


def selection_spec(contract_version: str) -> tuple[tuple[str, ...], dict[str, int], dict[str, int], tuple[str, ...]]:
    if contract_version == CONTRACT_VERSION:
        return BOARD_ORDER, BOARD_QUOTAS, BOUNDARY_COUNTS, BOUNDARY_ORDER
    if contract_version == NON_BJ_CONTRACT_VERSION:
        return (
            NON_BJ_BOARD_ORDER,
            NON_BJ_BOARD_QUOTAS,
            NON_BJ_BOUNDARY_COUNTS,
            NON_BJ_BOUNDARY_ORDER,
        )
    raise ValueError("Unsupported canary selection contract_version")


def _validated_symbols(
    payload: Mapping[str, Any], source: Mapping[str, Any], *, contract_version: str
) -> list[dict[str, Any]]:
    board_order, board_quotas, boundary_counts_spec, boundary_order = selection_spec(contract_version)
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or len(symbols) != sum(board_quotas.values()):
        raise ValueError("Canary selection has the wrong symbol count")
    identities: set[int] = set()
    names: set[str] = set()
    board_counts: Counter[str] = Counter()
    boundary_counts: dict[str, Counter[str]] = {board: Counter() for board in board_order}
    board_rows: dict[str, list[tuple[Fraction, int, str, str]]] = {
        board: [] for board in board_order
    }
    normalized: list[dict[str, Any]] = []
    for raw in symbols:
        if not isinstance(raw, Mapping) or set(raw) != {
            "symbol_id", "symbol", "board", "activity_boundary", "traits",
            "eligible_timeframes", "evidence",
        }:
            raise ValueError("selection-v2 symbol entry must use the exact schema")
        symbol_id = _integer(raw.get("symbol_id"), "symbol_id", positive=True)
        symbol = str(raw.get("symbol") or "").strip().upper()
        board = str(raw.get("board") or "")
        boundary = str(raw.get("activity_boundary") or "")
        evidence = raw.get("evidence")
        if symbol_id in identities or symbol in names:
            raise ValueError("Canary selection symbols must be 20 unique identities")
        if classify_board(symbol) != board or board not in board_quotas:
            raise ValueError("selection board evidence is inconsistent")
        if boundary not in boundary_counts_spec or not isinstance(evidence, Mapping):
            raise ValueError("selection activity boundary evidence is incomplete")
        if raw.get("traits") != [board, f"{boundary}_activity_boundary"]:
            raise ValueError("selection-v2 traits are inconsistent")
        eligible = raw.get("eligible_timeframes")
        if (
            not isinstance(eligible, list)
            or len(eligible) != len(set(eligible))
            or any(level not in _LEVELS for level in eligible)
            or "5f" not in eligible
            or "1d" not in eligible
        ):
            raise ValueError("selection-v2 eligible timeframe evidence is incomplete")
        if set(evidence) != {
            "basis", "canonical_audit_run_id", "five_minute_rows", "daily_rows",
            "activity_ratio_numerator", "activity_ratio_denominator",
        }:
            raise ValueError("selection-v2 activity evidence must use the exact schema")
        numerator = _integer(evidence.get("activity_ratio_numerator"), "activity numerator")
        denominator = _integer(evidence.get("activity_ratio_denominator"), "activity denominator", positive=True)
        five_rows = _integer(evidence.get("five_minute_rows"), "five_minute_rows")
        daily_rows = _integer(evidence.get("daily_rows"), "daily_rows", positive=True)
        if (
            evidence.get("basis") != ACTIVITY_BASIS
            or str(evidence.get("canonical_audit_run_id")) != source["canonical_audit_run_id"]
            or Fraction(numerator, denominator)
            != Fraction(five_rows, daily_rows * BARS_PER_COMPLETE_5F_SESSION)
        ):
            raise ValueError("selection-v2 activity ratio evidence is inconsistent")
        ratio = Fraction(numerator, denominator)
        identities.add(symbol_id)
        names.add(symbol)
        board_counts[board] += 1
        boundary_counts[board][boundary] += 1
        board_rows[board].append((ratio, symbol_id, symbol, boundary))
        normalized.append(dict(raw))
    if dict(board_counts) != board_quotas or any(
        dict(boundary_counts[board]) != boundary_counts_spec for board in board_order
    ):
        raise ValueError("selection-v2 board or activity boundary quotas are incomplete")
    for board in board_order:
        rows = board_rows[board]
        if [row[3] for row in rows] != list(boundary_order) or rows != sorted(
            rows, key=lambda row: row[:3]
        ):
            raise ValueError("selection-v2 deterministic order is inconsistent")
    return normalized


def validate_selection_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != {
        "contract_version", "source", "policy", "symbols", "selection_sha256"
    }:
        raise ValueError("selection-v2 manifest must use the exact schema")
    contract_version = str(payload.get("contract_version") or "")
    selection_spec(contract_version)
    source = normalize_selection_source(payload.get("source"))
    if payload.get("policy") != selection_policy_for(contract_version):
        raise ValueError("selection policy does not match the deterministic contract")
    _validated_symbols(payload, source, contract_version=contract_version)
    unsigned = {key: payload[key] for key in payload if key != "selection_sha256"}
    if payload.get("selection_sha256") != canonical_selection_sha256(unsigned):
        raise ValueError("selection-v2 canonical SHA-256 is invalid")
    return dict(payload)


def selection_active_universe_sha256(payload: Mapping[str, Any]) -> str:
    validated = validate_selection_manifest(payload)
    identities = sorted(
        (int(entry["symbol_id"]), str(entry["symbol"]).strip().upper())
        for entry in validated["symbols"]
    )
    digest = hashlib.sha256()
    for _symbol_id, symbol in identities:
        digest.update(json.dumps(symbol, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _empty_evidence(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "contract_version": None,
        "manifest_sha256": None,
        "source_build_id": None,
        "activity_basis": None,
        "board_counts": {},
        "boundary_counts": {},
        "contract_matches": None,
        "hash_matches": None,
        "source_matches": None,
        "quotas_match": None,
        "active_universe_matches": None,
        "drift_reasons": [],
    }


def evaluate_selection_evidence(
    parameters: Any,
    strict_provenance: Any,
    active_universe_hash: Any,
    *,
    applicable: bool = True,
) -> dict[str, Any]:
    if not applicable:
        return _empty_evidence("not_applicable")
    params = dict(parameters) if isinstance(parameters, Mapping) else {}
    raw = params.get("canary_selection")
    if not isinstance(raw, Mapping):
        result = _empty_evidence("unavailable")
        result.update(
            contract_version=params.get("selection_contract_version")
            if isinstance(params.get("selection_contract_version"), str) else None,
            manifest_sha256=params.get("selection_manifest_sha256")
            if isinstance(params.get("selection_manifest_sha256"), str) else None,
            source_build_id=params.get("source_build_id")
            if isinstance(params.get("source_build_id"), str) else None,
            drift_reasons=["canary_selection_unavailable"],
        )
        return result
    manifest = dict(raw)
    source = manifest.get("source") if isinstance(manifest.get("source"), Mapping) else {}
    policy = manifest.get("policy") if isinstance(manifest.get("policy"), Mapping) else {}
    result = _empty_evidence("failed")
    result.update(
        contract_version=manifest.get("contract_version") if isinstance(manifest.get("contract_version"), str) else None,
        manifest_sha256=manifest.get("selection_sha256") if isinstance(manifest.get("selection_sha256"), str) else None,
        source_build_id=source.get("eligibility_build_id") if isinstance(source.get("eligibility_build_id"), str) else None,
        activity_basis=policy.get("activity_basis") if isinstance(policy.get("activity_basis"), str) else None,
    )
    symbols = manifest.get("symbols") if isinstance(manifest.get("symbols"), list) else []
    contract_version = manifest.get("contract_version") if isinstance(manifest.get("contract_version"), str) else ""
    try:
        board_order, board_quotas, boundary_counts_spec, _boundary_order = selection_spec(contract_version)
        expected_policy = selection_policy_for(contract_version)
    except ValueError:
        board_order, board_quotas, boundary_counts_spec = (), {}, {}
        expected_policy = None
    board_counts: Counter[str] = Counter()
    boundary_counts: dict[str, Counter[str]] = {board: Counter() for board in board_order}
    traits: set[str] = set()
    for entry in symbols:
        if not isinstance(entry, Mapping):
            continue
        board = entry.get("board")
        boundary = entry.get("activity_boundary")
        if isinstance(board, str) and board in board_quotas:
            board_counts[board] += 1
            if isinstance(boundary, str) and boundary in boundary_counts_spec:
                boundary_counts[board][boundary] += 1
        if isinstance(entry.get("traits"), list):
            traits.update(str(value) for value in entry["traits"])
    result["board_counts"] = {board: board_counts[board] for board in board_order}
    result["boundary_counts"] = {
        board: {boundary: boundary_counts[board][boundary] for boundary in boundary_counts_spec}
        for board in board_order
    }
    provenance = dict(strict_provenance) if isinstance(strict_provenance, Mapping) else {}
    contract_matches = bool(
        contract_version in {CONTRACT_VERSION, NON_BJ_CONTRACT_VERSION}
        and params.get("scope") == "canary"
        and params.get("selection_contract_version") == contract_version
        and policy == expected_policy
        and params.get("selection_traits") == sorted(traits)
    )
    unsigned = {key: manifest[key] for key in manifest if key != "selection_sha256"}
    embedded_sha = manifest.get("selection_sha256")
    try:
        canonical_sha = canonical_selection_sha256(unsigned)
    except (TypeError, ValueError):
        canonical_sha = None
    hash_matches = bool(
        isinstance(embedded_sha, str)
        and embedded_sha == canonical_sha
        and params.get("selection_manifest_sha256") == embedded_sha
    )
    try:
        normalized_source = normalize_selection_source(source)
    except (TypeError, ValueError, KeyError):
        normalized_source = {}
    source_matches = bool(
        normalized_source
        and normalized_source.get("eligibility_build_id") == params.get("source_build_id")
        and all(normalized_source.get(field) == provenance.get(field) for field in STRICT_PROVENANCE_FIELDS)
    )
    try:
        validate_selection_manifest(manifest)
        quotas_match = True
        expected_active = selection_active_universe_sha256(manifest)
    except (TypeError, ValueError, KeyError, AttributeError):
        quotas_match = False
        expected_active = None
    active_matches = bool(
        expected_active is not None
        and isinstance(active_universe_hash, str)
        and active_universe_hash == expected_active
    )
    reasons: list[str] = []
    for matches, reason in (
        (contract_matches, "canary_selection_contract_drift"),
        (hash_matches, "canary_selection_hash_drift"),
        (source_matches, "canary_selection_source_drift"),
        (quotas_match, "canary_selection_quota_drift"),
        (active_matches, "canary_selection_active_universe_drift"),
    ):
        if not matches:
            reasons.append(reason)
    result.update(
        status="pass" if not reasons else "failed",
        contract_matches=contract_matches,
        hash_matches=hash_matches,
        source_matches=source_matches,
        quotas_match=quotas_match,
        active_universe_matches=active_matches,
        drift_reasons=reasons,
    )
    return result
