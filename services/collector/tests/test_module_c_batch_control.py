from __future__ import annotations

import asyncio
import json
import uuid
from argparse import Namespace
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import collector.module_c_batch_control as batch_control
from collector.module_c_batch_control import (
    _strict_v2_provenance,
    activate_batch,
    freeze_canary,
    load_selection,
    revalidate_strict_v2_build,
    validate_canary_report,
    validate_canary_run_set,
    validate_activation_identity,
    validate_production_canary_selection,
    validate_strict_build,
    validate_pristine_task_manifest,
    validate_terminal_tasks,
)
from collector.module_c_eligibility import _canonical_sha256
from trading_protocol import MODULE_C_CONFIG_HASH


def _selection() -> dict[str, object]:
    names = ["600000.SH", "300001.SZ", "688001.SH", "920047.BJ"] + [
        f"{index:06d}.SZ" for index in range(4, 20)
    ]
    return {
        "contract_version": "module-c-canary-selection-v1",
        "symbols": [
            {
                "symbol": name,
                "traits": (
                    ["main_board", "suspended_or_sparse", "gap", "price_limit", "long_history"]
                    if index == 0 else ["chinext"] if index == 1 else ["star"] if index == 2
                    else ["bj"] if index == 3 else []
                ),
                "evidence": ["test"],
            }
            for index, name in enumerate(names)
        ],
    }


def test_selection_requires_exactly_twenty_unique_symbols_and_trait_coverage(tmp_path) -> None:
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(_selection()), encoding="utf-8")
    symbols, digest, payload = load_selection(path)
    assert len(symbols) == 20
    assert len(digest) == 64
    assert payload["contract_version"] == "module-c-canary-selection-v1"

    invalid = _selection()
    invalid["symbols"] = invalid["symbols"][:-1]
    path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly 20"):
        load_selection(path)


def test_prepare_cli_requires_explicit_positive_max_attempts() -> None:
    base = [
        "prepare",
        "--database-url", "postgresql://disposable",
        "--batch-key", "canary-20260718",
        "--eligibility-build-id", "33333333-3333-3333-3333-333333333333",
        "--run-group-id", "group-1",
        "--code-commit", "commit",
        "--image-digest", "image",
        "--vendor-manifest-sha256", "a" * 64,
    ]
    with pytest.raises(SystemExit):
        batch_control.parse_args(base)
    with pytest.raises(SystemExit):
        batch_control.parse_args([*base, "--max-attempts", "0"])
    assert batch_control.parse_args([*base, "--max-attempts", "3"]).max_attempts == 3


def test_selection_rejects_missing_diversity_trait(tmp_path) -> None:
    payload = _selection()
    payload["symbols"][3]["traits"].remove("bj")
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required traits"):
        load_selection(path)


def test_terminal_tasks_fail_closed() -> None:
    validate_terminal_tasks(
        child_status="completed",
        disposition_rows=100,
        statuses={"completed": 75, "excluded": 25},
    )
    with pytest.raises(RuntimeError, match="not completed"):
        validate_terminal_tasks(
            child_status="running", disposition_rows=100, statuses={"completed": 100}
        )
    with pytest.raises(RuntimeError, match="blocking"):
        validate_terminal_tasks(
            child_status="completed",
            disposition_rows=100,
            statuses={"completed": 99, "failed": 1},
        )


def test_canary_report_must_match_batch_and_cover_strict_result(tmp_path) -> None:
    path = tmp_path / "report.json"
    report = {
        "selector": {"batch_id": 42},
        "passed": True,
        "symbols": 20,
        "published_runs": 80,
        "failed_runs": 0,
        "difference_count": 0,
    }
    path.write_text(json.dumps(report), encoding="utf-8")
    digest, loaded = validate_canary_report(path, batch_id=42)
    assert len(digest) == 64
    assert loaded == report
    with pytest.raises(RuntimeError, match="batch_id"):
        validate_canary_report(path, batch_id=43)


def test_canary_report_run_set_must_exactly_match_completed_tasks() -> None:
    expected = [{"run_id": 11, "symbol": "600000.SH", "chan_level": 5}]
    report = {
        "published_runs": 1,
        "runs": [{
            "run_id": 11,
            "symbol": "600000.SH",
            "level": "5f",
            "modes": ["confirmed", "predictive"],
            "config_hash": MODULE_C_CONFIG_HASH,
            "passed": True,
        }],
    }
    validate_canary_run_set(report, expected)
    report["runs"][0]["run_id"] = 12
    with pytest.raises(RuntimeError, match="run set"):
        validate_canary_run_set(report, expected)


def _strict_v2_source() -> dict[str, object]:
    freshness_contract = {
        "contract_version": "module-c-authoritative-freshness-v1",
        "as_of": "2026-07-03T07:00:00+00:00",
        "trading_calendar": {"id": "calendar", "sha256": "f" * 64},
        "expected_closed_watermarks": {
            timeframe: "2026-07-03T07:00:00+00:00"
            for timeframe in ("5f", "30f", "1d", "1w", "1m")
        },
    }
    provenance = {
        "canonical_audit_run_id": "11111111-1111-1111-1111-111111111111",
        "audit_evidence_sha256": "a" * 64,
        "audit_checkpoint_sha256": "b" * 64,
        "freshness_contract_version": "module-c-authoritative-freshness-v1",
        "freshness_contract_sha256": _canonical_sha256(freshness_contract),
        "catalog_generation_id": "22222222-2222-2222-2222-222222222222",
        "catalog_control_revision": 7,
        "catalog_manifest_sha256": "d" * 64,
        "audit_active_universe_sha256": "e" * 64,
    }
    return {
        **provenance,
        "parameters": {
            "policy": "strict-v2",
            **provenance,
            "freshness_contract": freshness_contract,
        },
    }


def _strict_inputs(source: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(
        audit_evidence_sha256=source["audit_evidence_sha256"],
        audit_checkpoint_sha256=source["audit_checkpoint_sha256"],
        audit_active_universe_sha256=source["audit_active_universe_sha256"],
        catalog_generation_id=uuid.UUID(str(source["catalog_generation_id"])),
        catalog_control_revision=source["catalog_control_revision"],
        catalog_manifest_sha256=source["catalog_manifest_sha256"],
    )


@pytest.mark.parametrize(
    "field,drifted",
    [
        ("audit_evidence_sha256", "0" * 64),
        ("audit_checkpoint_sha256", "0" * 64),
        ("audit_active_universe_sha256", "0" * 64),
        ("catalog_generation_id", uuid.UUID(int=9)),
        ("catalog_control_revision", 8),
        ("catalog_manifest_sha256", "0" * 64),
    ],
)
def test_revalidate_strict_v2_build_rejects_live_input_drift(
    monkeypatch, field, drifted
) -> None:
    source = _strict_v2_source()
    strict = _strict_inputs(source)
    setattr(strict, field, drifted)

    async def fake_load(*_args, **_kwargs):
        return strict

    async def fake_contract(*_args, **_kwargs):
        return None

    monkeypatch.setattr(batch_control, "_load_strict_inputs", fake_load)
    monkeypatch.setattr(batch_control, "validate_strict_build", fake_contract)

    with pytest.raises(RuntimeError, match="provenance drifted"):
        asyncio.run(
            revalidate_strict_v2_build(
                object(), source, build_id="33333333-3333-3333-3333-333333333333"
            )
        )


def test_revalidate_strict_v2_build_reuses_complete_strict_input_validation(
    monkeypatch,
) -> None:
    source = _strict_v2_source()
    calls: list[tuple[object, ...]] = []

    async def fake_load(conn, audit_run_id, freshness):
        calls.append((conn, audit_run_id, freshness.sha256))
        return _strict_inputs(source)

    async def fake_contract(conn, build, *, build_id, require_v2):
        calls.append((conn, build, build_id, require_v2))

    connection = object()
    monkeypatch.setattr(batch_control, "_load_strict_inputs", fake_load)
    monkeypatch.setattr(batch_control, "validate_strict_build", fake_contract)

    asyncio.run(
        revalidate_strict_v2_build(
            connection, source, build_id="33333333-3333-3333-3333-333333333333"
        )
    )

    assert calls[0][0] is connection
    assert calls[0][1] == source["canonical_audit_run_id"]
    assert calls[1] == (
        connection,
        source,
        "33333333-3333-3333-3333-333333333333",
        True,
    )


class _TaskManifestConnection:
    def __init__(self, *, expected=100, tasks=100, mismatches=0) -> None:
        self.result = {
            "expected_rows": expected,
            "task_rows": tasks,
            "mismatch_rows": mismatches,
        }
        self.query = ""
        self.args = ()

    async def fetchrow(self, sql, *args):
        self.query = " ".join(sql.lower().split())
        self.args = args
        return self.result


@pytest.mark.parametrize(
    "expected,tasks,mismatches",
    [(99, 100, 0), (100, 99, 0), (100, 100, 1)],
)
def test_pristine_task_manifest_rejects_missing_extra_or_drifted_rows(
    expected, tasks, mismatches
) -> None:
    connection = _TaskManifestConnection(
        expected=expected, tasks=tasks, mismatches=mismatches
    )

    with pytest.raises(RuntimeError, match="drifted or non-pristine"):
        asyncio.run(
            validate_pristine_task_manifest(
                connection,
                batch_id=42,
                build_id="33333333-3333-3333-3333-333333333333",
                disposition_rows=100,
            )
        )

    assert connection.args == (42, "33333333-3333-3333-3333-333333333333")
    assert "full join actual" in connection.query
    assert "target_bar_until is distinct from expected.covered_until" in connection.query
    assert "actual.attempts <> 0" in connection.query


def test_pristine_task_manifest_rejects_only_expected_heads_drift() -> None:
    connection = _TaskManifestConnection(expected=100, tasks=100, mismatches=1)

    with pytest.raises(RuntimeError, match="drifted or non-pristine"):
        asyncio.run(
            validate_pristine_task_manifest(
                connection,
                batch_id=42,
                build_id="33333333-3333-3333-3333-333333333333",
                disposition_rows=100,
            )
        )

    assert "jsonb_object_agg(head.mode, head.run_id)" in connection.query
    assert "head.status = 'published'" in connection.query
    assert (
        "actual.expected_heads is distinct from expected.expected_heads"
        in connection.query
    )


def test_canary_strict_v2_provenance_is_copied_exactly() -> None:
    source = _strict_v2_source()
    provenance = {
        field: source[field]
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
        )
    }

    copied, parameters = _strict_v2_provenance(source)

    assert copied == provenance
    assert parameters["policy"] == "strict-v2"
    assert parameters["freshness_contract"] == source["parameters"]["freshness_contract"]

    source["audit_checkpoint_sha256"] = None
    with pytest.raises(RuntimeError, match="provenance"):
        _strict_v2_provenance(source)


def test_canary_strict_v2_rejects_parameter_column_mismatch() -> None:
    source = _strict_v2_source()
    source["parameters"]["catalog_manifest_sha256"] = "0" * 64

    with pytest.raises(RuntimeError, match="provenance"):
        _strict_v2_provenance(source)


def test_canary_strict_v2_rejects_self_hashed_non_exact_freshness_contract() -> None:
    source = _strict_v2_source()
    contract = source["parameters"]["freshness_contract"]
    contract["unexpected"] = True
    digest = _canonical_sha256(contract)
    source["freshness_contract_sha256"] = digest
    source["parameters"]["freshness_contract_sha256"] = digest

    with pytest.raises(RuntimeError, match="provenance"):
        _strict_v2_provenance(source)


def test_freeze_canary_rejects_strict_v1_source() -> None:
    source = _strict_v2_source()
    source["parameters"]["policy"] = "strict-v1"

    with pytest.raises(RuntimeError, match="provenance"):
        _strict_v2_provenance(source)


def test_freeze_canary_rejects_v1_selection_before_database_access(tmp_path) -> None:
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(_selection()), encoding="utf-8")

    class Connection:
        inserted = False

        @asynccontextmanager
        async def transaction(self, *, isolation=None):
            assert isolation == "serializable"
            yield

        async def fetchrow(self, sql, *_args):
            if "from module_c_eligibility_builds" in sql.lower():
                return {
                    "config_hash": MODULE_C_CONFIG_HASH,
                    "active_symbols": 20,
                    "disposition_rows": 100,
                    "parameters": {
                        "policy": "strict-v1",
                        "canonical_audit_run_id": "11111111-1111-1111-1111-111111111111",
                    },
                }
            if "from module_c_eligibility where" in sql.lower():
                return {
                    "row_count": 100,
                    "symbol_count": 20,
                    "unresolved_eligible": 0,
                    "timeframe_count": 5,
                }
            raise AssertionError(sql)

        async def fetchval(self, sql, *_args):
            assert "from kline_audit_runs" in sql.lower()
            return "completed"

        async def execute(self, *_args):
            self.inserted = True
            raise AssertionError("strict-v1 source must fail before insert")

    connection = Connection()
    args = Namespace(
        selection_manifest=selection_path,
        source_build_id="22222222-2222-2222-2222-222222222222",
    )

    with pytest.raises(RuntimeError, match="selection-v2"):
        asyncio.run(freeze_canary(connection, args))
    assert connection.inserted is False


def test_new_batch_rejects_strict_v1_build() -> None:
    class Connection:
        async def fetchval(self, *_args):
            raise AssertionError("strict-v1 must fail before audit lookup")

    build = {
        "parameters": {
            "policy": "strict-v1",
            "canonical_audit_run_id": "11111111-1111-1111-1111-111111111111",
        },
    }

    with pytest.raises(RuntimeError, match="strict canonical audit"):
        asyncio.run(
            validate_strict_build(
                Connection(), build, build_id="legacy", require_v2=True
            )
        )


def test_output_failure_rolls_back_frozen_canary(tmp_path, monkeypatch) -> None:
    selection = _selection()
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(selection), encoding="utf-8")
    source = {
        **_strict_v2_source(),
        "config_hash": MODULE_C_CONFIG_HASH,
        "active_universe_hash": "e" * 64,
        "manifest_hash": "9" * 64,
        "active_symbols": 20,
        "disposition_rows": 100,
    }
    levels = (5, 30, 1440, 10080, 43200)
    rows = [
        {
            "symbol_id": symbol_id,
            "symbol": entry["symbol"],
            "timeframe": level,
            "eligible": True,
            "reasons": [],
            "covered_until": datetime(2026, 7, 3, 7, tzinfo=timezone.utc),
            "unresolved_rows": 0,
        }
        for symbol_id, entry in enumerate(selection["symbols"], start=1)
        for level in levels
    ]

    class Connection:
        transaction_failed = False
        inserted_args = None
        source_returned = False

        @asynccontextmanager
        async def transaction(self, *, isolation=None):
            assert isolation == "serializable"
            try:
                yield
            except Exception:
                self.transaction_failed = True
                raise

        async def fetchrow(self, sql, *_args):
            normalized = " ".join(sql.lower().split())
            if "from module_c_eligibility_builds" in normalized and not self.source_returned:
                self.source_returned = True
                return source
            if "count(*)::integer row_count" in normalized:
                return {
                    "row_count": 100,
                    "symbol_count": 20,
                    "unresolved_eligible": 0,
                    "timeframe_count": 5,
                }
            if "from module_c_eligibility_builds" in normalized:
                values = self.inserted_args
                return {
                    "manifest_version": values[1],
                    "config_hash": values[2],
                    "active_universe_hash": values[3],
                    "manifest_hash": values[4],
                    "active_symbols": 20,
                    "disposition_rows": 100,
                    "parameters": json.loads(values[5]),
                    **{
                        field: values[index]
                        for index, field in enumerate(
                            (
                                "canonical_audit_run_id",
                                "audit_evidence_sha256",
                                "audit_checkpoint_sha256",
                                "freshness_contract_version",
                                "freshness_contract_sha256",
                                "catalog_generation_id",
                                "catalog_control_revision",
                                "catalog_manifest_sha256",
                                "audit_active_universe_sha256",
                            ),
                            start=7,
                        )
                    },
                }
            raise AssertionError(sql)

        async def fetchval(self, sql, *_args):
            if "from kline_audit_runs" in sql.lower():
                return "completed"
            if "from module_c_eligibility" in sql.lower():
                return 100
            raise AssertionError(sql)

        async def fetch(self, sql, *_args):
            assert "from module_c_eligibility" in sql.lower()
            return rows

        async def execute(self, sql, *args):
            assert "insert into module_c_eligibility_builds" in sql.lower()
            self.inserted_args = args
            return "INSERT 0 1"

        async def copy_records_to_table(self, *_args, **_kwargs):
            return None

    connection = Connection()

    v2_selection = {
        **selection,
        "contract_version": "module-c-canary-selection-v2",
    }

    def load_v2(_path):
        return tuple(entry["symbol"] for entry in selection["symbols"]), "a" * 64, v2_selection

    async def reproduce(*_args, **_kwargs):
        return v2_selection

    async def no_drift(*_args, **_kwargs):
        return None

    monkeypatch.setattr(batch_control, "load_selection", load_v2)
    monkeypatch.setattr(batch_control, "revalidate_strict_v2_build", no_drift)
    monkeypatch.setattr(
        "collector.module_c_canary_selection.validate_selection_source",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "collector.module_c_canary_selection.rebuild_selection_manifest",
        reproduce,
    )

    def fail_outputs(*_args):
        raise OSError("output unavailable")

    monkeypatch.setattr("collector.module_c_batch_control._write_outputs", fail_outputs)
    args = Namespace(
        selection_manifest=selection_path,
        source_build_id="33333333-3333-3333-3333-333333333333",
        build_id="44444444-4444-4444-4444-444444444444",
        manifest_version="canary-v2-test",
        output_dir=tmp_path / "outputs",
    )

    with pytest.raises(OSError, match="output unavailable"):
        asyncio.run(freeze_canary(connection, args))
    assert connection.transaction_failed is True
    assert connection.inserted_args is not None


@pytest.mark.parametrize(
    "parameters,active_symbols",
    [
        ({"scope": "canary", "selection_contract_version": "module-c-canary-selection-v1"}, 20),
        ({"scope": "canary", "selection_contract_version": "module-c-canary-selection-v2"}, 19),
        ({"scope": "baseline", "selection_contract_version": "module-c-canary-selection-v2"}, 20),
    ],
)
def test_prepare_canary_requires_deterministic_selection_v2(
    parameters, active_symbols
) -> None:
    with pytest.raises(RuntimeError, match="deterministic selection-v2"):
        validate_production_canary_selection(
            parameters=parameters, active_symbols=active_symbols
        )


class _PrepareDriftConnection:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.transaction_failed = False

    @asynccontextmanager
    async def transaction(self, *, isolation=None):
        assert isolation == "serializable"
        try:
            yield
        except Exception:
            self.transaction_failed = True
            raise

    async def execute(self, sql, *_args):
        self.execute_calls.append(" ".join(sql.lower().split()))
        return "SELECT 1"

    async def fetchrow(self, sql, *_args):
        assert "from module_c_eligibility_builds" in sql.lower()
        return {
            **_strict_v2_source(),
            "build_id": "33333333-3333-3333-3333-333333333333",
            "config_hash": MODULE_C_CONFIG_HASH,
            "manifest_hash": "9" * 64,
            "active_symbols": 20,
            "disposition_rows": 100,
        }


def test_prepare_batch_revalidates_provenance_before_state_writes(monkeypatch) -> None:
    connection = _PrepareDriftConnection()

    async def reject(*_args, **_kwargs):
        raise RuntimeError("provenance drift")

    monkeypatch.setattr(batch_control, "revalidate_strict_v2_build", reject)
    args = Namespace(
        batch_key="canary-20260718",
        eligibility_build_id="33333333-3333-3333-3333-333333333333",
    )

    with pytest.raises(RuntimeError, match="provenance drift"):
        asyncio.run(batch_control.prepare_batch(connection, args))

    assert all("insert into" not in sql and "update " not in sql for sql in connection.execute_calls)
    assert connection.transaction_failed is True


class _ActivateConnection:
    def __init__(
        self,
        *,
        parent_status: str = "planned",
        child_status: str = "pending",
        disposition_rows: int = 100,
        task_count: int = 100,
        update_results: tuple[str, ...] = ("UPDATE 1", "UPDATE 1"),
    ) -> None:
        self.row = {
            **_strict_v2_source(),
            "parent_status": parent_status,
            "batch_kind": "canary",
            "child_status": child_status,
            "child_disposition_rows": disposition_rows,
            "disposition_rows": disposition_rows,
            "task_count": task_count,
            "build_id": "33333333-3333-3333-3333-333333333333",
            "config_hash": MODULE_C_CONFIG_HASH,
            "manifest_hash": "9" * 64,
            "active_symbols": 20,
            "parent_config_hash": MODULE_C_CONFIG_HASH,
            "parent_manifest_hash": "9" * 64,
            "effective_config": {
                "contract": "module-c-native-five-level-v1",
                "levels": ["5f", "30f", "1d", "1w", "1m"],
                "modes": ["confirmed", "predictive"],
                "concurrency_per_worker": 1,
                "shard_count": 4,
                "eligibility_build_id": "33333333-3333-3333-3333-333333333333",
                "max_attempts": 3,
            },
            "parent_run_group_id": "group-1",
            "parent_publication_namespace": "production",
            "parent_profile_id": "module-c-native-5lvl",
            "child_active_symbols": 20,
            "child_shard_count": 4,
            "child_config_hash": MODULE_C_CONFIG_HASH,
            "child_run_group_id": "group-1",
            "child_publication_namespace": "production",
            "child_profile_id": "module-c-native-5lvl",
        }
        self.update_results = iter(update_results)
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_failed = False

    @asynccontextmanager
    async def transaction(self, *, isolation=None):
        assert isolation == "serializable"
        try:
            yield
        except Exception:
            self.transaction_failed = True
            raise

    async def fetchrow(self, sql, *args):
        normalized = " ".join(sql.lower().split())
        assert "for update of parent, child" in normalized
        assert args == (42,)
        return self.row

    async def execute(self, sql, *args):
        self.execute_calls.append((" ".join(sql.lower().split()), args))
        return next(self.update_results)


def _stub_activation_validations(monkeypatch) -> None:
    async def valid(*_args, **_kwargs):
        return None

    monkeypatch.setattr(batch_control, "revalidate_strict_v2_build", valid)
    monkeypatch.setattr(batch_control, "validate_pristine_task_manifest", valid)


@pytest.mark.parametrize(
    "field,value",
    [
        ("parent_manifest_hash", "0" * 64),
        ("child_config_hash", "wrong"),
        ("child_run_group_id", "wrong"),
        ("child_active_symbols", 19),
        ("child_disposition_rows", 99),
    ],
)
def test_activation_identity_rejects_parent_child_manifest_drift(field, value) -> None:
    connection = _ActivateConnection()
    connection.row[field] = value

    with pytest.raises(RuntimeError, match="batch identity"):
        validate_activation_identity(connection.row)


@pytest.mark.parametrize("max_attempts", [None, 0, -1, True, "3"])
def test_activation_identity_requires_exact_frozen_max_attempts(max_attempts) -> None:
    connection = _ActivateConnection()
    if max_attempts is None:
        connection.row["effective_config"].pop("max_attempts")
    else:
        connection.row["effective_config"]["max_attempts"] = max_attempts

    with pytest.raises(RuntimeError, match="batch identity"):
        validate_activation_identity(connection.row)


def test_activate_batch_atomically_transitions_parent_and_child(monkeypatch) -> None:
    _stub_activation_validations(monkeypatch)
    connection = _ActivateConnection()

    result = asyncio.run(activate_batch(connection, Namespace(batch_id=42)))

    assert result == {"batch_id": 42, "status": "running"}
    assert len(connection.execute_calls) == 2
    child_sql, child_args = connection.execute_calls[0]
    parent_sql, parent_args = connection.execute_calls[1]
    assert "update chan_c_full_recompute_batches" in child_sql
    assert "set status='running'" in child_sql
    assert "where batch_id=$1 and status='pending'" in child_sql
    assert child_args == (42,)
    assert "update chan_c_batches" in parent_sql
    assert "set status='running'" in parent_sql
    assert "where id=$1 and status='planned'" in parent_sql
    assert parent_args == (42,)
    assert connection.transaction_failed is False


def test_activate_batch_rejects_second_activation_without_writes(monkeypatch) -> None:
    _stub_activation_validations(monkeypatch)
    connection = _ActivateConnection(parent_status="running", child_status="running")

    with pytest.raises(RuntimeError, match="planned/pending"):
        asyncio.run(activate_batch(connection, Namespace(batch_id=42)))

    assert connection.execute_calls == []
    assert connection.transaction_failed is True


@pytest.mark.parametrize("update_results", [("UPDATE 0",), ("UPDATE 1", "UPDATE 0")])
def test_activate_batch_rolls_back_when_either_status_cas_misses(
    monkeypatch, update_results
) -> None:
    _stub_activation_validations(monkeypatch)
    connection = _ActivateConnection(update_results=update_results)

    with pytest.raises(RuntimeError, match="atomically activate"):
        asyncio.run(activate_batch(connection, Namespace(batch_id=42)))

    assert connection.transaction_failed is True


@pytest.mark.parametrize("gate", ["provenance", "manifest"])
def test_activate_batch_revalidates_before_status_writes(monkeypatch, gate) -> None:
    connection = _ActivateConnection()

    async def valid(*_args, **_kwargs):
        return None

    async def reject(*_args, **_kwargs):
        raise RuntimeError(f"{gate} drift")

    monkeypatch.setattr(
        batch_control,
        "revalidate_strict_v2_build",
        reject if gate == "provenance" else valid,
    )
    monkeypatch.setattr(
        batch_control,
        "validate_pristine_task_manifest",
        reject if gate == "manifest" else valid,
    )

    with pytest.raises(RuntimeError, match=f"{gate} drift"):
        asyncio.run(activate_batch(connection, Namespace(batch_id=42)))

    assert connection.execute_calls == []
    assert connection.transaction_failed is True
