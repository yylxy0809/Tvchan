import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import collector.module_c_execution_report as report_module
from collector.module_c_eligibility import _canonical_sha256
from collector.module_c_canary_selection import build_selection_manifest
from trading_protocol.module_c_canary_selection import (
    evaluate_selection_evidence,
    selection_active_universe_sha256,
)

from collector.module_c_execution_report import (
    BATCH_SQL,
    CANONICAL_SQL,
    HEAD_COVERAGE_SQL,
    _json_object,
    build_report,
    build_selection_evidence,
    parse_args,
    write_artifacts,
)


AUDIT_ID = "11111111-1111-1111-1111-111111111111"
CATALOG_ID = "22222222-2222-2222-2222-222222222222"
BUILD_ID = "33333333-3333-3333-3333-333333333333"
EVIDENCE_SHA = "a" * 64
CHECKPOINT_SHA = "b" * 64
CATALOG_SHA = "c" * 64
UNIVERSE_SHA = "d" * 64
MANIFEST_SHA = "e" * 64
CONFIG_HASH = "module-c:test-config"
FRESHNESS_CONTRACT = {
    "contract_version": "module-c-authoritative-freshness-v1",
    "as_of": "2026-07-18T00:00:00+00:00",
    "trading_calendar": {"id": "test-calendar", "sha256": "e" * 64},
    "expected_closed_watermarks": {
        "5f": "2026-07-17T07:00:00+00:00",
        "30f": "2026-07-17T07:00:00+00:00",
        "1d": "2026-07-17T07:00:00+00:00",
        "1w": "2026-07-17T07:00:00+00:00",
        "1m": "2026-06-30T07:00:00+00:00",
    },
}
FRESHNESS_SHA = _canonical_sha256(FRESHNESS_CONTRACT)


def _strict_parameters(*, policy: str = "strict-v2") -> dict[str, object]:
    return {
        "policy": policy,
        "canonical_audit_run_id": AUDIT_ID,
        "audit_evidence_sha256": EVIDENCE_SHA,
        "audit_checkpoint_sha256": CHECKPOINT_SHA,
        "freshness_contract": FRESHNESS_CONTRACT,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": FRESHNESS_SHA,
        "catalog_generation_id": CATALOG_ID,
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": CATALOG_SHA,
        "audit_active_universe_sha256": UNIVERSE_SHA,
    }


def _selection_manifest() -> dict[str, object]:
    source = {
        "eligibility_build_id": BUILD_ID,
        "eligibility_manifest_sha256": MANIFEST_SHA,
        "canonical_audit_run_id": AUDIT_ID,
        "audit_evidence_sha256": EVIDENCE_SHA,
        "audit_checkpoint_sha256": CHECKPOINT_SHA,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": FRESHNESS_SHA,
        "catalog_generation_id": CATALOG_ID,
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": CATALOG_SHA,
        "audit_active_universe_sha256": UNIVERSE_SHA,
    }
    names = [
        f"{prefix}{index:03d}.{exchange}"
        for prefix, exchange in (("600", "SH"), ("300", "SZ"), ("688", "SH"), ("920", "BJ"))
        for index in range(6)
    ]
    symbols = list(enumerate(names, start=1))
    dispositions = [
        {
            "symbol_id": symbol_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "eligible": not (symbol.endswith(".BJ") and timeframe == 30),
            "reasons": ["bj_30f_excluded"] if symbol.endswith(".BJ") and timeframe == 30 else [],
            "covered_until": None,
            "unresolved_rows": 0,
        }
        for symbol_id, symbol in symbols
        for timeframe in (5, 30, 1440, 10080, 43200)
    ]
    checkpoints = [
        {
            "symbol_id": symbol_id,
            "timeframe": timeframe,
            "status": "completed",
            "rows_scanned": 1000 + symbol_id if timeframe == 5 else 100,
        }
        for symbol_id, _symbol in symbols
        for timeframe in (5, 30, 1440, 10080, 43200)
    ]
    return build_selection_manifest(
        source=source, dispositions=dispositions, checkpoints=checkpoints
    )


def _canary_parameters() -> dict[str, object]:
    selection = _selection_manifest()
    return {
        **_strict_parameters(),
        "scope": "canary",
        "source_build_id": BUILD_ID,
        "selection_contract_version": selection["contract_version"],
        "selection_manifest_sha256": selection["selection_sha256"],
        "canary_selection": selection,
        "selection_traits": sorted({
            trait for entry in selection["symbols"] for trait in entry["traits"]
        }),
    }


def _selection_active_universe_hash(parameters: dict[str, object]) -> str:
    try:
        return selection_active_universe_sha256(parameters["canary_selection"])
    except ValueError:
        return "f" * 64


@pytest.fixture(autouse=True)
def _strict_loader(monkeypatch):
    async def load(_conn, audit_run_id, freshness, *, for_share):
        assert audit_run_id == AUDIT_ID
        assert freshness.sha256 == FRESHNESS_SHA
        assert for_share is False
        return SimpleNamespace(
            audit_evidence_sha256=EVIDENCE_SHA,
            audit_checkpoint_sha256=CHECKPOINT_SHA,
            catalog_generation_id=CATALOG_ID,
            catalog_control_revision=7,
            catalog_manifest_sha256=CATALOG_SHA,
            audit_active_universe_sha256=UNIVERSE_SHA,
        )

    monkeypatch.setattr(report_module, "_load_strict_inputs", load)


def test_json_object_decodes_asyncpg_jsonb_text() -> None:
    assert _json_object('{"gate_pass": true}') == {"gate_pass": True}


class Conn:
    def __init__(
        self,
        *,
        batch_status="completed",
        task_status="completed",
        missing_heads=0,
        outbox_status="completed",
        lifecycle_mismatches=0,
        official_heads=1,
        baseline_historical=0,
        future_leaks=0,
        strict_policy="strict-v2",
        provenance_overrides=None,
        canonical_gate_pass=True,
        batch_kind="baseline",
        eligibility_parameters=None,
    ):
        self.batch_status = batch_status
        self.task_status = task_status
        self.missing_heads = missing_heads
        self.outbox_status = outbox_status
        self.lifecycle_mismatches = lifecycle_mismatches
        self.official_heads = official_heads
        self.baseline_historical = baseline_historical
        self.future_leaks = future_leaks
        self.strict_policy = strict_policy
        self.provenance_overrides = provenance_overrides or {}
        self.canonical_gate_pass = canonical_gate_pass
        self.batch_kind = batch_kind
        self.eligibility_parameters = eligibility_parameters
        self.canonical_args = None

    async def fetchrow(self, sql, *args):
        if "report:batch" in sql:
            eligibility_parameters = self.eligibility_parameters or _strict_parameters(
                policy=self.strict_policy
            )
            active_universe_hash = (
                _selection_active_universe_hash(eligibility_parameters)
                if self.batch_kind == "canary"
                and "canary_selection" in eligibility_parameters
                else UNIVERSE_SHA
            )
            batch = {
                "batch_id": args[0],
                "eligibility_build_id": BUILD_ID,
                "child_eligibility_build_id": BUILD_ID,
                "build_id": BUILD_ID,
                "run_group_id": "group",
                "child_run_group_id": "group",
                "parent_run_group_id": "group",
                "config_hash": CONFIG_HASH,
                "child_config_hash": CONFIG_HASH,
                "parent_config_hash": CONFIG_HASH,
                "build_config_hash": CONFIG_HASH,
                "publication_namespace": "production",
                "child_publication_namespace": "production",
                "parent_publication_namespace": "production",
                "profile_id": "native-five-level",
                "child_profile_id": "native-five-level",
                "parent_profile_id": "native-five-level",
                "shard_count": 4,
                "child_shard_count": 4,
                "status": self.batch_status,
                "active_symbols": 1,
                "disposition_rows": 1,
                "created_at": datetime.now(UTC),
                "started_at": datetime.now(UTC),
                "finished_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
                "batch_key": "batch",
                "batch_kind": self.batch_kind,
                "parent_effective_config": {
                    "contract": "module-c-native-five-level-v1",
                    "levels": ["5f", "30f", "1d", "1w", "1m"],
                    "modes": ["confirmed", "predictive"],
                    "concurrency_per_worker": 1,
                    "shard_count": 4,
                    "eligibility_build_id": BUILD_ID,
                    "max_attempts": 3,
                },
                "code_commit": "a" * 40,
                "image_digest": "sha256:test",
                "vendor_manifest_sha256": "b" * 64,
                "eligible_manifest_uri": "manifest.jsonl",
                "eligible_manifest_sha256": MANIFEST_SHA,
                "input_watermark": {},
                "audit_references": [],
                "manifest_version": "strict-v2",
                "active_universe_hash": active_universe_hash,
                "manifest_hash": MANIFEST_SHA,
                "eligibility_parameters": eligibility_parameters,
                "eligibility_summary": {},
                "canonical_audit_run_id": AUDIT_ID,
                "audit_evidence_sha256": EVIDENCE_SHA,
                "audit_checkpoint_sha256": CHECKPOINT_SHA,
                "freshness_contract_version": "module-c-authoritative-freshness-v1",
                "freshness_contract_sha256": FRESHNESS_SHA,
                "catalog_generation_id": CATALOG_ID,
                "catalog_control_revision": 7,
                "catalog_manifest_sha256": CATALOG_SHA,
                "audit_active_universe_sha256": UNIVERSE_SHA,
            }
            batch.update(self.provenance_overrides)
            return batch
        if "report:canonical" in sql:
            self.canonical_args = args
            return {
                "audit_run_id": AUDIT_ID,
                "status": "completed",
                "apply_mode": False,
                "summary": {"gate_pass": self.canonical_gate_pass},
                "parameters": {},
            }
        if "report:official" in sql:
            return {
                "official_expected_heads": 1,
                "historical_replay_heads": self.official_heads,
                "official_missing_heads": 1 - self.official_heads,
                "baseline_claimed_historical_events": self.baseline_historical,
                "future_leak_events": self.future_leaks,
            }
        if "report:db-resource" in sql:
            return {
                "database_name": "test",
                "database_size_bytes": 100,
                "current_wal_lsn": "0/1",
                "active_queries": 1,
            }
        if "event_state" in sql:
            return {
                "count": self.lifecycle_mismatches,
                "samples": ["fingerprint"] if self.lifecycle_mismatches else [],
            }
        if "with missing as" in sql:
            return {"count": 0, "samples": []}
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        if "report:task-progress" in sql:
            return [{
                "chan_level": 5,
                "status": self.task_status,
                "count": 1,
                "attempts": 1,
                "bars": 10,
                "strokes": 2,
                "segments": 1,
                "centers": 1,
                "signals": 1,
                "latest_update": datetime.now(UTC),
            }]
        if "report:head-coverage" in sql:
            return [{
                "chan_level": 5,
                "mode": "confirmed",
                "expected": 2,
                "published": 2 - self.missing_heads,
                "missing": self.missing_heads,
                "direct_batch": 2 - self.missing_heads,
                "equivalent_noop": 0,
                "missing_history": self.missing_heads,
                "missing_outbox": self.missing_heads,
                "outbox_incomplete": self.missing_heads,
            }]
        if "report:outbox" in sql:
            return [{"status": self.outbox_status, "count": 2}]
        if "report:failures" in sql:
            return ([{"symbol": "SH.600000", "chan_level": 5, "status": "failed"}]
                    if self.task_status == "failed" else [])
        if "group by status" in sql:
            return [{
                "status": self.outbox_status,
                "count": 2,
                "oldest_age_seconds": 1,
            }]
        if "chan_lifecycle_observer_watermarks" in sql:
            return [{"observer_name": "observer", "last_outbox_id": 2, "updated_at": None}]
        raise AssertionError(sql)


def test_report_go_requires_all_contract_evidence() -> None:
    connection = Conn()
    report = asyncio.run(build_report(connection, 7, resource_metrics={"cpu_percent": 25.0}))
    assert report["next_phase_decision"]["decision"] == "GO"
    assert connection.canonical_args == (AUDIT_ID,)
    assert report["strict_v2_provenance"]["decision"] == "PASS"
    assert report["run_manifest"]["strict_v2_provenance"]["frozen"] == report[
        "strict_v2_provenance"
    ]["observed"]
    assert report["recompute_summary"]["tasks"]["statuses"] == {"completed": 1}
    assert report["published_head_coverage"]["summary"]["missing"] == 0
    assert report["resource_metrics"][0]["cpu_percent"] == 25.0
    assert report["selection"]["status"] == "not_applicable"


def test_canary_report_validates_first_class_selection_v2_evidence() -> None:
    report = asyncio.run(build_report(
        Conn(batch_kind="canary", eligibility_parameters=_canary_parameters()), 7
    ))

    selection = report["selection"]
    assert selection["status"] == "pass"
    assert selection["contract_matches"] is True
    assert selection["hash_matches"] is True
    assert selection["source_matches"] is True
    assert selection["quotas_match"] is True
    assert selection["active_universe_matches"] is True
    assert selection["board_counts"] == {
        "main_board": 5, "chinext": 5, "star": 5, "bj": 5
    }
    assert all(counts == {"lower": 2, "middle": 1, "upper": 2}
               for counts in selection["boundary_counts"].values())
    assert report["next_phase_decision"]["decision"] == "GO"


def test_canary_report_fails_closed_for_missing_or_tampered_selection() -> None:
    missing = asyncio.run(build_report(
        Conn(batch_kind="canary", eligibility_parameters=_strict_parameters()), 7
    ))
    assert missing["selection"]["status"] == "unavailable"
    assert "canary_selection_unavailable" in missing["selection"]["drift_reasons"]

    parameters = _canary_parameters()
    parameters["canary_selection"]["symbols"][0]["symbol"] = "600999.SH"
    tampered = asyncio.run(build_report(
        Conn(batch_kind="canary", eligibility_parameters=parameters), 7
    ))
    assert tampered["selection"]["status"] == "failed"
    assert tampered["selection"]["hash_matches"] is False
    assert {blocker["code"] for blocker in tampered["next_phase_decision"]["blockers"]} >= {
        "canary_selection_failed"
    }


def test_canary_report_fails_closed_when_subset_universe_or_traits_drift() -> None:
    wrong_universe = asyncio.run(build_report(
        Conn(
            batch_kind="canary",
            eligibility_parameters=_canary_parameters(),
            provenance_overrides={"active_universe_hash": "f" * 64},
        ),
        7,
    ))
    assert wrong_universe["strict_v2_provenance"]["decision"] == "FAIL"
    assert wrong_universe["selection"]["active_universe_matches"] is False
    assert "canary_selection_active_universe_drift" in wrong_universe["selection"][
        "drift_reasons"
    ]

    parameters = _canary_parameters()
    parameters["selection_traits"] = ["bj"]
    wrong_traits = asyncio.run(build_report(
        Conn(batch_kind="canary", eligibility_parameters=parameters), 7
    ))
    assert wrong_traits["selection"]["status"] == "failed"
    assert "canary_selection_contract_drift" in wrong_traits["selection"][
        "drift_reasons"
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda parameters: parameters.pop("canary_selection"),
        lambda parameters: parameters["canary_selection"]["symbols"][0].update(
            {"symbol": "600999.SH"}
        ),
        lambda parameters: parameters["canary_selection"]["symbols"][0].update(
            {"activity_boundary": []}
        ),
        lambda parameters: parameters.update({"selection_contract_version": "v1"}),
        lambda parameters: parameters.update({"source_build_id": AUDIT_ID}),
    ],
)
def test_collector_selection_evidence_is_exact_shared_evaluator(
    mutation,
) -> None:
    parameters = _canary_parameters()
    active_universe_hash = _selection_active_universe_hash(parameters)
    mutation(parameters)
    frozen = _strict_parameters()
    provenance = {"decision": "PASS", "frozen": frozen}
    batch = {
        "batch_kind": "canary",
        "eligibility_parameters": parameters,
        "active_universe_hash": active_universe_hash,
    }

    assert build_selection_evidence(batch, provenance) == evaluate_selection_evidence(
        parameters,
        frozen,
        active_universe_hash,
        applicable=True,
    )


def test_collector_baseline_selection_evidence_is_exact_shared_non_applicable() -> None:
    batch = {
        "batch_kind": "baseline",
        "eligibility_parameters": _strict_parameters(),
        "active_universe_hash": UNIVERSE_SHA,
    }
    frozen = _strict_parameters()
    provenance = {"decision": "PASS", "frozen": frozen}

    assert build_selection_evidence(batch, provenance) == evaluate_selection_evidence(
        batch["eligibility_parameters"],
        frozen,
        UNIVERSE_SHA,
        applicable=False,
    )


def test_report_no_go_lists_each_required_blocking_class() -> None:
    report = asyncio.run(build_report(
        Conn(
            batch_status="running",
            task_status="failed",
            missing_heads=1,
            outbox_status="dead_letter",
            lifecycle_mismatches=1,
            official_heads=0,
            baseline_historical=1,
            future_leaks=1,
        ),
        8,
    ))
    codes = {item["code"] for item in report["next_phase_decision"]["blockers"]}
    assert report["next_phase_decision"]["decision"] == "NO_GO"
    assert {
        "recompute_incomplete",
        "recompute_failed",
        "published_head_coverage_incomplete",
        "outbox_not_drained",
        "lifecycle_reconciliation_failed",
        "official_historical_coverage_missing",
        "baseline_claims_historical_first_seen",
        "official_future_leak",
    } <= codes


def test_report_fails_visible_when_strict_v2_provenance_is_unavailable() -> None:
    report = asyncio.run(build_report(Conn(strict_policy="legacy"), 8))

    assert report["strict_v2_provenance"]["decision"] == "FAIL"
    assert report["strict_v2_provenance"]["failure_code"] == "unavailable"
    assert report["next_phase_decision"]["decision"] == "NO_GO"
    assert "strict_v2_provenance_unavailable" in {
        blocker["code"] for blocker in report["next_phase_decision"]["blockers"]
    }


def test_report_fails_visible_when_live_strict_v2_inputs_drift(monkeypatch) -> None:
    async def drifted(*_args, **_kwargs):
        raise RuntimeError("active K-line scope catalog no longer matches audit evidence")

    monkeypatch.setattr(report_module, "_load_strict_inputs", drifted)
    report = asyncio.run(build_report(Conn(), 8))

    assert report["strict_v2_provenance"]["decision"] == "FAIL"
    assert report["strict_v2_provenance"]["failure_code"] == "drift"
    assert report["next_phase_decision"]["decision"] == "NO_GO"
    assert "strict_v2_provenance_drift" in {
        blocker["code"] for blocker in report["next_phase_decision"]["blockers"]
    }


def test_report_fails_visible_when_frozen_column_and_parameters_drift() -> None:
    report = asyncio.run(
        build_report(
            Conn(provenance_overrides={"catalog_control_revision": 8}),
            8,
        )
    )

    assert report["strict_v2_provenance"]["decision"] == "FAIL"
    assert report["strict_v2_provenance"]["failure_code"] == "drift"
    assert report["next_phase_decision"]["decision"] == "NO_GO"


@pytest.mark.parametrize("canonical_gate_pass", [False, None])
def test_report_fails_closed_when_pinned_canonical_gate_did_not_pass(
    canonical_gate_pass,
) -> None:
    report = asyncio.run(
        build_report(Conn(canonical_gate_pass=canonical_gate_pass), 8)
    )

    assert report["canonical_gate"]["gate_available"] is True
    assert report["canonical_gate"]["gate_pass"] is False
    assert report["next_phase_decision"]["decision"] == "NO_GO"
    assert "canonical_gate_failed" in {
        blocker["code"] for blocker in report["next_phase_decision"]["blockers"]
    }


@pytest.mark.parametrize(
    "overrides,failure_code",
    [
        ({"eligible_manifest_sha256": "f" * 64}, "drift"),
        ({"eligible_manifest_sha256": None}, "unavailable"),
        ({"parent_config_hash": "module-c:other"}, "drift"),
        ({"build_config_hash": None}, "unavailable"),
    ],
)
def test_report_fails_closed_when_batch_identity_provenance_is_invalid(
    overrides,
    failure_code,
) -> None:
    report = asyncio.run(
        build_report(Conn(provenance_overrides=overrides), 8)
    )

    assert report["strict_v2_provenance"]["decision"] == "FAIL"
    assert report["strict_v2_provenance"]["failure_code"] == failure_code
    assert report["next_phase_decision"]["decision"] == "NO_GO"


@pytest.mark.parametrize(
    "overrides,failure_code",
    [
        ({"child_profile_id": None}, "unavailable"),
        ({"batch_kind": "diagnostic"}, "drift"),
        ({"child_run_group_id": "other-group"}, "drift"),
        ({"child_publication_namespace": "other-namespace"}, "drift"),
        ({"child_config_hash": "module-c:other"}, "drift"),
        ({"child_shard_count": 2}, "drift"),
        ({"build_id": "44444444-4444-4444-4444-444444444444"}, "drift"),
        (
            {
                "parent_effective_config": {
                    "contract": "module-c-native-five-level-v1",
                    "levels": ["5f", "30f", "1d", "1w", "1m"],
                    "modes": ["confirmed", "predictive"],
                    "concurrency_per_worker": 1,
                    "shard_count": 4,
                    "eligibility_build_id": BUILD_ID,
                    "max_attempts": 3,
                    "unexpected": True,
                }
            },
            "drift",
        ),
        (
            {
                "parent_effective_config": {
                    "contract": "module-c-native-five-level-v1",
                    "levels": ["30f", "5f", "1d", "1w", "1m"],
                    "modes": ["confirmed", "predictive"],
                    "concurrency_per_worker": 1,
                    "shard_count": 4,
                    "eligibility_build_id": BUILD_ID,
                    "max_attempts": 3,
                }
            },
            "drift",
        ),
        (
            {
                "parent_effective_config": {
                    "contract": "module-c-native-five-level-v1",
                    "levels": ["5f", "30f", "1d", "1w", "1m"],
                    "modes": ["confirmed", "predictive"],
                    "concurrency_per_worker": 1,
                    "shard_count": 4,
                    "eligibility_build_id": BUILD_ID,
                    "max_attempts": True,
                }
            },
            "unavailable",
        ),
    ],
)
def test_report_fails_closed_when_execution_identity_is_invalid(
    overrides,
    failure_code,
) -> None:
    report = asyncio.run(
        build_report(Conn(provenance_overrides=overrides), 8)
    )

    assert report["strict_v2_provenance"]["decision"] == "FAIL"
    assert report["strict_v2_provenance"]["failure_code"] == failure_code
    assert report["next_phase_decision"]["decision"] == "NO_GO"


def test_canonical_gate_is_pinned_and_report_queries_never_scan_klines() -> None:
    canonical = " ".join(CANONICAL_SQL.lower().split())
    assert "where audit_run_id = $1::uuid" in canonical
    assert "order by completed_at" not in canonical
    assert "limit 1" not in canonical
    batch = " ".join(BATCH_SQL.lower().split())
    for field in (
        "canonical_audit_run_id",
        "audit_evidence_sha256",
        "audit_checkpoint_sha256",
        "freshness_contract_version",
        "freshness_contract_sha256",
        "catalog_generation_id",
        "catalog_control_revision",
        "catalog_manifest_sha256",
        "audit_active_universe_sha256",
    ):
        assert f"eligibility.{field}" in batch
    assert "evidence.config_hash as parent_config_hash" in batch
    assert "eligibility.config_hash as build_config_hash" in batch
    for field in (
        "effective_config as parent_effective_config",
        "run_group_id as parent_run_group_id",
        "publication_namespace as parent_publication_namespace",
        "profile_id as parent_profile_id",
        "run_group_id as child_run_group_id",
        "publication_namespace as child_publication_namespace",
        "profile_id as child_profile_id",
        "shard_count as child_shard_count",
        "eligibility_build_id::text as child_eligibility_build_id",
        "eligibility.build_id::text as build_id",
    ):
        assert field in batch
    for sql in report_module.__dict__.values():
        if isinstance(sql, str) and "report:" in sql:
            assert " from klines" not in " ".join(sql.lower().split())
            assert not sql.lstrip().lower().startswith(("insert ", "update ", "delete "))


def test_head_coverage_accepts_direct_batch_or_strict_input_equivalent_noop() -> None:
    normalized = " ".join(HEAD_COVERAGE_SQL.split()).lower()
    assert "head_run_status = 'success'" in normalized
    assert "(head_batch_id = $1 and head_run_batch_id = $1) or (task_status = 'completed' and input_identity_equivalent)" in normalized
    assert "task_run.symbol_id = head_run.symbol_id" in normalized
    assert "task_run.chan_level = head_run.chan_level" in normalized
    assert "task_run.mode = head_run.mode" in normalized
    assert "task_run.input_signature = head_run.input_signature" in normalized
    assert "task_run.config_hash = head_run.config_hash" in normalized
    assert "task_run.bar_from is not distinct from head_run.bar_from" in normalized
    assert "task_run.bar_until = head_run.bar_until" in normalized
    assert "task_run.bar_until = expected.target_bar_until" in normalized
    assert "task_run.bar_count is not distinct from head_run.bar_count" in normalized
    assert "task_run.bar_count is not distinct from expected.task_bar_count" in normalized
    assert "task_run.base_timeframe = head_run.base_timeframe" in normalized
    assert "expected.expected_head_run_id = head.run_id" in normalized
    assert "task_status = 'completed' and recorded_head_equivalent" in normalized
    assert "history.new_run_id = head.run_id" in normalized
    assert "outbox_status is distinct from 'completed'" in normalized


def test_write_artifacts_replaces_all_required_report_files(tmp_path) -> None:
    report = asyncio.run(build_report(Conn(), 9))
    write_artifacts(tmp_path, report)
    expected = {
        "run_manifest.json",
        "kline_canonical_gate.json",
        "kline_canonical_gate.md",
        "recompute_progress.jsonl",
        "recompute_summary.json",
        "recompute_summary.md",
        "published_head_coverage.json",
        "resource_metrics.jsonl",
        "failure_samples.jsonl",
        "next_phase_decision.json",
        "strict_v2_provenance.json",
        "selection.json",
    }
    assert {path.name for path in tmp_path.iterdir()} == expected
    assert json.loads((tmp_path / "next_phase_decision.json").read_text(encoding="utf-8"))["decision"] == "GO"
    assert json.loads((tmp_path / "selection.json").read_text(encoding="utf-8")) == report["selection"]
    assert not list(tmp_path.glob("*.tmp"))


def test_cli_requires_batch_and_output_and_accepts_os_metrics(tmp_path) -> None:
    args = parse_args([
        "--database-url", "postgresql://test",
        "--batch-id", "12",
        "--output-dir", str(tmp_path),
        "--cpu-percent", "44.5",
        "--memory-rss-bytes", "1024",
        "--disk-free-bytes", "2048",
    ])
    assert args.batch_id == 12
    assert args.output_dir == tmp_path
    assert args.cpu_percent == 44.5
