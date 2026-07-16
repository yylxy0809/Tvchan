import asyncio
import json
from datetime import UTC, datetime

from collector.module_c_execution_report import (
    HEAD_COVERAGE_SQL,
    _json_object,
    build_report,
    parse_args,
    write_artifacts,
)


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
    ):
        self.batch_status = batch_status
        self.task_status = task_status
        self.missing_heads = missing_heads
        self.outbox_status = outbox_status
        self.lifecycle_mismatches = lifecycle_mismatches
        self.official_heads = official_heads
        self.baseline_historical = baseline_historical
        self.future_leaks = future_leaks

    async def fetchrow(self, sql, *args):
        if "report:batch" in sql:
            return {
                "batch_id": args[0],
                "eligibility_build_id": "build",
                "run_group_id": "group",
                "config_hash": "config",
                "publication_namespace": "production",
                "profile_id": "native-five-level",
                "shard_count": 4,
                "status": self.batch_status,
                "active_symbols": 1,
                "disposition_rows": 1,
                "created_at": datetime.now(UTC),
                "started_at": datetime.now(UTC),
                "finished_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
                "batch_key": "batch",
                "batch_kind": "baseline",
                "code_commit": "a" * 40,
                "image_digest": "sha256:test",
                "vendor_manifest_sha256": "b" * 64,
                "eligible_manifest_uri": "manifest.jsonl",
                "eligible_manifest_sha256": "c" * 64,
                "input_watermark": {},
                "audit_references": [],
                "manifest_version": "strict-v1",
                "active_universe_hash": "d" * 64,
                "manifest_hash": "e" * 64,
                "eligibility_parameters": {},
                "eligibility_summary": {},
            }
        if "report:canonical" in sql:
            return {
                "audit_run_id": "audit",
                "status": "completed",
                "summary": {"gate_pass": True},
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
    report = asyncio.run(build_report(Conn(), 7, resource_metrics={"cpu_percent": 25.0}))
    assert report["next_phase_decision"]["decision"] == "GO"
    assert report["recompute_summary"]["tasks"]["statuses"] == {"completed": 1}
    assert report["published_head_coverage"]["summary"]["missing"] == 0
    assert report["resource_metrics"][0]["cpu_percent"] == 25.0


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
    }
    assert {path.name for path in tmp_path.iterdir()} == expected
    assert json.loads((tmp_path / "next_phase_decision.json").read_text(encoding="utf-8"))["decision"] == "GO"
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
