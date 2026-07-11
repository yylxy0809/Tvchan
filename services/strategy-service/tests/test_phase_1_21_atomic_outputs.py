import asyncio
import hashlib
from pathlib import Path

import pytest

import app.engine.phase_1_21 as phase


def _write_required(directory: Path, marker: str) -> None:
    for name in ("source_artifact_manifest.json", "database_readonly_snapshot_before.json", "database_readonly_snapshot_after.json", "intraday_run_coverage_v3.json", "next_phase_decision.json", "phase_1_21_detailed_completion_report.md"):
        (directory / name).write_text(marker, encoding="utf-8")


def test_atomic_failure_preserves_existing_target(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"; target.mkdir(); (target / "old.txt").write_text("old", encoding="utf-8"); (target / "old.json").write_text('{"old":true}', encoding="utf-8")
    hashes_before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in target.iterdir()}
    async def fail(*, output_dir, **_):
        (output_dir / "partial.txt").write_text("partial", encoding="utf-8")
        raise RuntimeError("boom")
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", fail)
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(phase.run_phase_1_21(output_dir=target))
    assert {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in target.iterdir()} == hashes_before
    assert not list(tmp_path.glob(".out.staging-*"))
    assert not list(tmp_path.glob(".out.backup-*"))


def test_atomic_promote_replaces_old_target_and_rejects_lock(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"; target.mkdir(); (target / "old.txt").write_text("old", encoding="utf-8")
    async def complete(*, output_dir, **_):
        _write_required(output_dir, "new")
        return {"ok": True}
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", complete)
    assert asyncio.run(phase.run_phase_1_21(output_dir=target)) == {"ok": True}
    assert not (target / "old.txt").exists()
    assert (target / "next_phase_decision.json").read_text(encoding="utf-8") == "new"
    assert not list(tmp_path.glob(".out.staging-*"))
    assert not list(tmp_path.glob(".out.backup-*"))
    lock = tmp_path / ".out.lock"; lock.write_text("other", encoding="utf-8")
    with pytest.raises(RuntimeError, match="output lock exists"):
        asyncio.run(phase.run_phase_1_21(output_dir=target))


def test_second_replace_failure_restores_backup_releases_lock_and_allows_retry(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"; target.mkdir(); (target / "old.bin").write_bytes(b"old-bytes")
    async def complete(*, output_dir, **_):
        _write_required(output_dir, "new")
        return {"ok": True}
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", complete)
    original = phase.os.replace
    calls = 0
    def fail_promote(source, destination):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise PermissionError("injected promote failure")
        return original(source, destination)
    monkeypatch.setattr(phase.os, "replace", fail_promote)
    with pytest.raises(PermissionError, match="injected promote failure"):
        asyncio.run(phase.run_phase_1_21(output_dir=target))
    assert (target / "old.bin").read_bytes() == b"old-bytes"
    assert not list(tmp_path.glob(".out.staging-*"))
    assert not list(tmp_path.glob(".out.backup-*"))
    assert not (tmp_path / ".out.lock").exists()
    monkeypatch.setattr(phase.os, "replace", original)
    assert asyncio.run(phase.run_phase_1_21(output_dir=target)) == {"ok": True}


def test_lock_cleanup_permission_failure_keeps_promoted_output_recoverable(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"
    async def complete(*, output_dir, **_):
        _write_required(output_dir, "new")
        return {"ok": True}
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", complete)
    original_unlink = Path.unlink
    def deny_lock_unlink(path, *args, **kwargs):
        if path.name == ".out.lock":
            raise PermissionError("injected lock cleanup denial")
        return original_unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", deny_lock_unlink)
    with pytest.raises(RuntimeError, match="lock cleanup failed"):
        asyncio.run(phase.run_phase_1_21(output_dir=target))
    assert (target / "next_phase_decision.json").read_text(encoding="utf-8") == "new"
    lock = tmp_path / ".out.lock"
    assert lock.exists() and not list(tmp_path.glob(".out.staging-*"))
    monkeypatch.setattr(Path, "unlink", original_unlink)
    assert asyncio.run(phase.run_phase_1_21(output_dir=target)) == {"ok": True}
    assert not lock.exists()
