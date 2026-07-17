from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.cli import run_event_replay_timeline_audit
from app.config.strategy_execution import (
    DIAGNOSTIC_STRATEGY_CODES,
    require_diagnostic_strategy,
)
from app.config.strategy_params import STRATEGY_CODE


def test_audit_runner_reuses_shared_diagnostic_allowlist_guard() -> None:
    assert run_event_replay_timeline_audit.require_diagnostic_strategy is require_diagnostic_strategy
    assert len(DIAGNOSTIC_STRATEGY_CODES) == 5
    for strategy_code in DIAGNOSTIC_STRATEGY_CODES:
        assert require_diagnostic_strategy(strategy_code).strategy_code == strategy_code


@pytest.mark.parametrize("strategy_code", [STRATEGY_CODE, "", "unknown-strategy"])
def test_blocked_audit_fails_before_database_repository_output_or_audit_side_effects(
    monkeypatch,
    strategy_code: str,
) -> None:
    side_effects: list[str] = []

    async def create_pool(**_kwargs):
        side_effects.append("database")
        raise AssertionError("database pool must not be created for blocked execution")

    def create_repository(_pool):
        side_effects.append("repository")
        raise AssertionError("repository must not be created for blocked execution")

    async def build_audit(**_kwargs):
        side_effects.append("audit")
        raise AssertionError("audit must not run for blocked execution")

    def write_audit(*_args, **_kwargs):
        side_effects.append("output")
        raise AssertionError("audit output must not be written for blocked execution")

    monkeypatch.setattr(run_event_replay_timeline_audit, "create_pool", create_pool)
    monkeypatch.setattr(run_event_replay_timeline_audit, "ModuleCRepository", create_repository)
    monkeypatch.setattr(run_event_replay_timeline_audit, "KlineRepository", create_repository)
    monkeypatch.setattr(run_event_replay_timeline_audit, "build_event_replay_timeline_audit", build_audit)
    monkeypatch.setattr(run_event_replay_timeline_audit, "write_event_replay_timeline_audit", write_audit)

    with pytest.raises(ValueError, match="diagnostic|official"):
        asyncio.run(run_event_replay_timeline_audit._run(SimpleNamespace(strategy=strategy_code)))

    assert side_effects == []
