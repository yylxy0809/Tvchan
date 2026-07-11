from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import shutil
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.db import create_pool
from app.engine.expanded_gate_waterfall import build_gate_waterfall
from app.engine.intraday_cutoff_grid import build_expected_intraday_cutoff_grid
from app.engine.intraday_run_coverage_audit_v3 import audit_intraday_run_coverage_v3
from app.engine.micro_backfill_v4_planner import plan_micro_backfill_v4
from app.engine.observable_research_universe import build_observable_research_universe, build_reconstructed_episodes
from app.engine.phase_1_21_decision import decide_next_phase
from app.engine.signal_lifecycle_ledger import build_signal_lifecycle
from app.engine.trigger_window_semantics_v2 import bind_five_f_parent, evaluate_policy_matrix, select_valid_five_f_confirmation
from app.engine.multi_run_group_signal_ledger import ALLOWED_RUN_GROUPS
from app.engine.time_utils import iso_utc, utc_time
from app.repositories.module_c_repo import ModuleCRepository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase-1-21-intraday-grid-signal-lifecycle"
SOURCE_RELATIVE_PATHS = (
    "outputs/phase-1-20r-data-truth-contract-reconciliation/daily_setup_episodes.jsonl",
    "outputs/phase-1-20r-data-truth-contract-reconciliation/weekly_context_episodes.jsonl",
    "outputs/phase-1-20r-data-truth-contract-reconciliation/signal_event_ledger_v2_30f.jsonl",
    "outputs/phase-1-20r-data-truth-contract-reconciliation/signal_event_ledger_v2_5f.jsonl",
    "outputs/phase-1-20r-data-truth-contract-reconciliation/phase_1_20r_summary.json",
)
CONTRACT_VERSION = "weekly_daily_b2_official_v1.0"
TARGET_RUN_GROUP = "phase_1_22_targeted_entry_window_intraday_v1"
READ_ONLY_SQL = ["SELECT count(*) FROM scheme2_chan_c_published_heads", "SELECT coalesce(run_group_id, '<null>'), count(*) FROM chan_c_runs GROUP BY 1", "SELECT symbols, klines, chan_c_runs, chan_c_signals only; no DML"]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _default(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _parse_time(value: str) -> datetime:
    return utc_time(value)


def _partition_observable_symbols(universe: dict[str, Any]) -> tuple[list[str], list[str]]:
    observable = set(universe.get("observable_symbols", []))
    official = sorted(symbol for symbol in observable if universe.get("market_cap_status", {}).get(symbol) == "available")
    diagnostic = sorted(observable - set(official))
    return official, diagnostic


def _has_execution_bar(rows: list[dict[str, Any]], *, symbol: str, after: datetime, window_end: datetime) -> bool:
    return any(row["symbol"] == symbol and row["timeframe"] == 30 and after < utc_time(row["ts"]) <= window_end for row in rows)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_default) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=_default) + "\n" for row in rows), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) or ["no_rows"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, default=_default) if isinstance(value, (dict, list)) else value for key, value in row.items()})


def _md(title: str, values: dict[str, Any]) -> str:
    return "# " + title + "\n\n" + "\n".join(f"- {key}: `{json.dumps(value, ensure_ascii=False, default=_default)}`" for key, value in values.items()) + "\n"


def build_preflight_manifest(source_paths: list[Path]) -> dict[str, Any]:
    missing = [str(path.resolve()) for path in source_paths if not path.is_file()]
    if missing:
        raise FileNotFoundError("Missing required source artifacts:\n" + "\n".join(missing))
    artifacts = []
    for path in source_paths:
        stat, data = path.stat(), path.read_bytes()
        artifacts.append({"absolute_path": str(path.resolve()), "size_bytes": stat.st_size, "mtime_utc": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(), "sha256": hashlib.sha256(data).hexdigest()})
    return {"generated_at": _now(), "artifacts": artifacts, "source_run_groups": sorted(ALLOWED_RUN_GROUPS), "database_tables": ["symbols", "klines", "chan_c_runs", "chan_c_signals", "scheme2_chan_c_published_heads"], "readonly_sql_summary": READ_ONLY_SQL, "strategy_contract_version": CONTRACT_VERSION}


async def _snapshot(pool) -> dict[str, Any]:
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            groups = await conn.fetch("select coalesce(run_group_id, '<null>') as run_group_id, count(*) as count from chan_c_runs group by 1 order by 1")
            heads = await conn.fetchval("select count(*) from scheme2_chan_c_published_heads")
            target = await conn.fetchval("select count(*) from chan_c_runs where run_group_id=$1", TARGET_RUN_GROUP)
    return {"captured_at": _now(), "published_head_row_count": heads, "run_group_counts": {row["run_group_id"]: row["count"] for row in groups}, "target_run_group_count": target, "readonly_sql_summary": READ_ONLY_SQL}


async def _snapshot_conn(conn, *, transaction_snapshot: str | None = None) -> dict[str, Any]:
    groups = await conn.fetch("select coalesce(run_group_id, '<null>') as run_group_id, count(*) as count from chan_c_runs group by 1 order by 1")
    heads = await conn.fetchval("select count(*) from scheme2_chan_c_published_heads")
    target = await conn.fetchval("select count(*) from chan_c_runs where run_group_id=$1", TARGET_RUN_GROUP)
    return {"captured_at": _now(), "published_head_row_count": heads, "run_group_counts": {row["run_group_id"]: row["count"] for row in groups}, "target_run_group_count": target, "readonly_sql_summary": READ_ONLY_SQL, "transaction_snapshot": transaction_snapshot, "transaction_isolation": "repeatable_read", "transaction_readonly": True}


def _actual_grid(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: run.get(key) for key in ("run_id", "symbol", "level", "mode", "cutoff_bar_end", "run_group_id")} for run in runs if run.get("level") in {"30f", "5f"}]


def _lifecycle_runs_on_expected_grid(runs: list[dict[str, Any]], expected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected_keys = {(row["symbol"], row["level"], utc_time(row["cutoff_bar_end"])) for row in expected}
    return [row for row in runs if row.get("cutoff_bar_end") is not None and (row["symbol"], row["level"], utc_time(row["cutoff_bar_end"])) in expected_keys]


def _episode_recheck(episodes: list[dict[str, Any]], coverage: dict[str, Any], lifecycle: list[dict[str, Any]], bottom_events: list[dict[str, Any]], expected: list[dict[str, Any]], universe_by_symbol: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    results, policy_rows = [], []
    for episode in episodes:
        start, end, as_of = (_parse_time(episode[key]) for key in ("daily_setup_first_seen_time", "trigger_window_end", "as_of_time"))
        bottom = any(row.get("symbol") == episode["symbol"] and row.get("first_seen_time") and start < _parse_time(row["first_seen_time"]) <= as_of for row in bottom_events)
        crows = [row for row in coverage["rows"] if row["episode_id"] == episode["episode_id"]]
        mode_results = []
        session_cuts = [_parse_time(row["cutoff_bar_end"]) for row in expected if row["episode_id"] == episode["episode_id"] and row["level"] == "30f" and _parse_time(row["cutoff_bar_end"]) <= end]
        session_end = max(session_cuts, default=end)
        for mode in ("predictive", "confirmed"):
            choices = [row for row in lifecycle if row["symbol"] == episode["symbol"] and row["mode"] == mode]
            b1s = [row for row in choices if row["level"] == "30f" and row["side"] == "buy" and row["bsp_type"] == "1" and row["first_seen_time"]]
            b1 = next((row for row in b1s if start <= _parse_time(row["first_seen_time"]) <= end), None)
            late = next((row for row in b1s if _parse_time(row["first_seen_time"]) > end), None)
            one_p = next((row for row in choices if row["level"] == "30f" and row["side"] == "buy" and row["bsp_type"] == "1p" and row["first_seen_time"] and start <= _parse_time(row["first_seen_time"]) <= end), None)
            five_candidates = [row for row in choices if row["level"] == "5f" and row["side"] == "buy" and row["bsp_type"] in {"2", "2s"} and row["first_seen_time"] and start <= _parse_time(row["first_seen_time"]) <= end]
            five_f_b1_candidates = [row for row in choices if row["level"] == "5f" and row["side"] == "buy" and row["bsp_type"] == "1"]
            five, binding = select_valid_five_f_confirmation(b1=b1, five_candidates=five_candidates, five_f_b1_candidates=five_f_b1_candidates, trigger_window_end=end, as_of_time=as_of)
            matrix = evaluate_policy_matrix(as_of_time=as_of, trigger_window_end=end, trading_session_window_end=session_end, thirty_f_first_seen=b1 and b1["first_seen_time"], thirty_f_confirm_time=b1 and b1["confirm_time"], one_p_first_seen=one_p and one_p["first_seen_time"], bottom_visible=bottom, five_f_first_seen=five and five["first_seen_time"] if binding["valid"] else None, five_f_confirm_time=five and five["confirm_time"] if binding["valid"] else None, five_f_parent_valid=binding["valid"], has_1p=one_p is not None)
            for state in matrix:
                trace_signal = one_p if state["policy"] == "candidate_1p_research_only" else b1
                policy_rows.append({"episode_id": episode["episode_id"], "symbol": episode["symbol"], "universe_class": (universe_by_symbol or {}).get(episode["symbol"], "diagnostic_only"), "mode": mode, "run_id": trace_signal and trace_signal["first_seen_run_id"], "cutoff_bar_end": trace_signal and trace_signal["first_seen_time"], "point_time": trace_signal and trace_signal["point_time"], "first_seen_time": trace_signal and trace_signal["first_seen_time"], "confirm_time": trace_signal and trace_signal["confirm_time"], "disappear_time": trace_signal and trace_signal["disappear_time"], "source_signal_identity": trace_signal and trace_signal["identity"], "five_f_parent_binding": binding, **state})
            next_bar = next((row for row in expected if row["episode_id"] == episode["episode_id"] and row["level"] == "30f" and b1 and _parse_time(row["cutoff_bar_end"]) > _parse_time(b1["first_seen_time"])), None)
            reasons = []
            if any(row["mode"] == mode and row["coverage_classification"] in {"partial", "none", "unsupported_mode"} for row in crows): reasons.append("data_gap_or_unsupported_mode")
            if not b1: reasons.append("b1_too_late" if late else "no_b1")
            elif not b1["confirm_time"]: reasons.append("b1_unconfirmed")
            if not bottom: reasons.append("no_bottom")
            if not five: reasons.append("no_valid_5f_confirmation")
            if b1 and not next_bar: reasons.append("execution_bar_missing")
            if five and not binding["valid"]: reasons.append(binding["reason"])
            mode_results.append({"mode": mode, "b1": b1, "late_b1": late, "one_p": one_p, "five_f": five, "five_f_parent_binding": binding, "next_30f_execution_bar": next_bar, "policy_matrix": matrix, "root_causes": reasons or ["policy_blocked"]})
        results.append({"episode_id": episode["episode_id"], "symbol": episode["symbol"], "coverage": crows, "daily_bottom_fractal": bottom, "mode_results": mode_results})
    return results, policy_rows




async def _run_phase_1_21_impl(*, output_dir: Path, reconstruction_start: datetime | None = None, reconstruction_end: datetime | None = None) -> dict[str, Any]:
    started = _now(); output_dir.mkdir(parents=True, exist_ok=True); (output_dir / "traces").mkdir(exist_ok=True)
    paths = [PROJECT_ROOT / relative for relative in SOURCE_RELATIVE_PATHS]
    manifest = build_preflight_manifest(paths)
    _write_json(output_dir / "source_artifact_manifest.json", manifest); (output_dir / "source_artifact_manifest.md").write_text(_md("Source Artifact Manifest", manifest), encoding="utf-8")
    episodes, weekly_input, old_30f, old_5f = _read_jsonl(paths[0]), _read_jsonl(paths[1]), _read_jsonl(paths[2]), _read_jsonl(paths[3])
    pool = await create_pool()
    conn = await pool.acquire()
    transaction = conn.transaction(isolation="repeatable_read", readonly=True)
    try:
        await transaction.start()
        transaction_snapshot = await conn.fetchval("select txid_current_snapshot()")
        before = await _snapshot_conn(conn, transaction_snapshot=transaction_snapshot); _write_json(output_dir / "database_readonly_snapshot_before.json", before)
        repo = ModuleCRepository(pool)
        klines = await repo.fetch_intraday_klines_for_episodes(episodes, conn=conn)
        expected = build_expected_intraday_cutoff_grid(episodes, klines)
        intraday_runs, direct_pagination_metrics = await repo.fetch_historical_runs_with_signals_paged(
            symbols=sorted({row["symbol"] for row in episodes}),
            levels=("30f", "5f"),
            run_groups=tuple(sorted(ALLOWED_RUN_GROUPS)),
            conn=conn,
            start=min(_parse_time(row["daily_setup_first_seen_time"]) for row in episodes),
            end=max(_parse_time(row["trigger_window_end"]) for row in episodes),
            batch_size=2000,
        )
        coverage = audit_intraday_run_coverage_v3(expected, _actual_grid(intraday_runs), episodes=episodes)
        lifecycle_runs = _lifecycle_runs_on_expected_grid(intraday_runs, expected)
        lifecycle = build_signal_lifecycle(lifecycle_runs, expected)
        # Research universe must be discovered from the database, not limited to old episode symbols.
        symbol_rows = await conn.fetch("select distinct (s.code || '.' || s.exchange) as symbol from chan_c_runs r join symbols s on s.id=r.symbol_id where r.run_group_id='research_daily_close' and r.status='success' and r.run_kind='historical_backfill'")
        research_symbols = [row["symbol"] for row in symbol_rows]
        if (reconstruction_start is None) != (reconstruction_end is None):
            raise ValueError("reconstruction_start and reconstruction_end must be supplied together")
        scoped_audit = reconstruction_start is not None
        if scoped_audit:
            reconstruction_start, reconstruction_end = utc_time(reconstruction_start), utc_time(reconstruction_end)
        else:
            scope_rows = await conn.fetch("select min(coalesce(cutoff_bar_end, bar_until)) as start_time, max(coalesce(cutoff_bar_end, bar_until)) as end_time from chan_c_runs where run_group_id='research_daily_close' and status='success' and run_kind='historical_backfill'")
            if not scope_rows or scope_rows[0]["start_time"] is None or scope_rows[0]["end_time"] is None:
                raise RuntimeError("research_daily_close has no bounded historical run interval")
            reconstruction_start, reconstruction_end = utc_time(scope_rows[0]["start_time"]), utc_time(scope_rows[0]["end_time"])
        all_runs, pagination_metrics = await repo.fetch_historical_structure_runs_paged(symbols=research_symbols, levels=("1w", "1d", "30f", "5f"), run_groups=("research_daily_close",), conn=conn, start=reconstruction_start, end=reconstruction_end, batch_size=2000)
        # Lifecycle continuity is exact for the audited 8 episodes; expanded setup reconstruction
        # only needs lifecycle identities, then reads bounded historical Kline windows.
        expanded_lifecycle = build_signal_lifecycle(all_runs)
        setup_times = [_parse_time(row["first_seen_time"]) for row in expanded_lifecycle if row["level"] in {"1w", "1d"} and row["bsp_type"] in {"1", "2", "2s"} and row["first_seen_time"]]
        research_start = reconstruction_start
        research_end = reconstruction_end
        context_klines = await repo.fetch_complete_klines(symbols=research_symbols, levels=("1w", "1d"), start=research_start, end=research_end, conn=conn)
        universe = build_observable_research_universe(all_runs)
        weekly_episodes, daily_episodes = build_reconstructed_episodes(universe=universe, lifecycle=expanded_lifecycle, structure_runs=all_runs, klines=context_klines)
        intraday_start = min((_parse_time(row["daily_setup_first_seen_time"]) for row in daily_episodes), default=research_end)
        intraday_end = max((_parse_time(row["daily_setup_first_seen_time"]) + timedelta(days=5) for row in daily_episodes), default=research_end)
        intraday_klines = await repo.fetch_complete_klines(symbols=research_symbols, levels=("30f", "5f"), start=intraday_start, end=intraday_end, conn=conn)
        research_klines = context_klines + intraday_klines
        universe = build_observable_research_universe(all_runs, klines=research_klines)
        official_symbols, diagnostic_symbols = _partition_observable_symbols(universe)
        universe["official_symbols"] = official_symbols
        universe["diagnostic_only_symbols"] = diagnostic_symbols
        universe["official_eligible_count"] = len(official_symbols)
        universe["diagnostic_only_count"] = len(diagnostic_symbols)
        universe["official_eligible_symbol_count"] = len(official_symbols)
        universe["diagnostic_symbol_count"] = len(diagnostic_symbols)
        universe["observable_symbol_count"] = len(set(official_symbols) | set(diagnostic_symbols))
        universe["symbol_count"] = universe["observable_symbol_count"]
        expanded_audit_episodes = [{"episode_id": row["episode_id"], "symbol": row["symbol"], "daily_setup_first_seen_time": row["daily_setup_first_seen_time"], "trigger_window_end": (_parse_time(row["daily_setup_first_seen_time"]) + timedelta(days=5)).isoformat()} for row in daily_episodes]
        expanded_grid = build_expected_intraday_cutoff_grid(expanded_audit_episodes, intraday_klines)
        expanded_coverage = audit_intraday_run_coverage_v3(expanded_grid, _actual_grid(all_runs), episodes=expanded_audit_episodes)
        universe["intraday_coverage_auditable"] = len(expanded_coverage["rows"]) == len(expanded_audit_episodes) * 4
        universe["expanded_intraday_coverage"] = expanded_coverage["summary"]
        universe["pagination_metrics"] = pagination_metrics
        universe["direct_intraday_pagination_metrics"] = direct_pagination_metrics
        universe["audit_scope"] = {"start": iso_utc(reconstruction_start), "end": iso_utc(reconstruction_end), "scope_mode": "explicit_scoped" if scoped_audit else "full_research_daily_close_history", "scoped_audit_not_full_universe": scoped_audit}
        weekly_lookup = {row["episode_id"]: row for row in weekly_episodes}
        def intraday_for(entry, level, types):
            start_time = _parse_time(entry["daily_setup_first_seen_time"])
            end_time = start_time + timedelta(days=5)
            return next((row for row in expanded_lifecycle if row["symbol"] == entry["symbol"] and row["level"] == level and row["side"] == "buy" and row["bsp_type"] in types and row["first_seen_time"] and start_time <= _parse_time(row["first_seen_time"]) <= end_time), None)
        gates_input = []
        for entry in daily_episodes:
            context = weekly_lookup[entry["weekly_context_episode_id"]]
            b1 = intraday_for(entry, "30f", {"1"})
            window_start = _parse_time(entry["daily_setup_first_seen_time"])
            window_end = window_start + timedelta(days=5)
            five_candidates = [row for row in expanded_lifecycle if row["symbol"] == entry["symbol"] and row["level"] == "5f" and row["side"] == "buy" and row["mode"] == (b1 or {}).get("mode") and row["bsp_type"] in {"2", "2s"} and row["first_seen_time"] and window_start <= _parse_time(row["first_seen_time"]) <= window_end]
            five_f_b1_candidates = [row for row in expanded_lifecycle if row["symbol"] == entry["symbol"] and row["level"] == "5f" and row["side"] == "buy" and row["mode"] == (b1 or {}).get("mode") and row["bsp_type"] == "1"]
            five, parent_binding = select_valid_five_f_confirmation(b1=b1, five_candidates=five_candidates, five_f_b1_candidates=five_f_b1_candidates, trigger_window_end=window_end, as_of_time=window_end)
            parent_bound = parent_binding["valid"]
            setup_time = _parse_time(entry["daily_setup_first_seen_time"])
            daily_bars = [row for row in research_klines if row["symbol"] == entry["symbol"] and row["timeframe"] == 1440 and row["ts"] >= setup_time and row["ts"] <= setup_time + timedelta(days=5)]
            bottom = any(index > 0 and index + 1 < len(daily_bars) and daily_bars[index]["low_x1000"] < daily_bars[index - 1]["low_x1000"] and daily_bars[index]["low_x1000"] < daily_bars[index + 1]["low_x1000"] for index in range(len(daily_bars)))
            execution = bool(b1 and _has_execution_bar(research_klines, symbol=entry["symbol"], after=_parse_time(b1["first_seen_time"]), window_end=window_end))
            gates_input.append({**entry, "weekly_b1": bool(context.get("weekly_b1")), "weekly_b2": bool(context.get("weekly_b2")), "weekly_price_relation_valid": context["weekly_price_relation_valid"], "weekly_dif": context["weekly_dif"], "weekly_dif_status": context["weekly_dif_status"], "daily_first_up_strength_status": entry["daily_first_up_strength_status"], "entry_watch": bool(context.get("weekly_b1") and context.get("weekly_b2") and entry.get("daily_b1") and entry.get("daily_b2")), "fresh_30f_b1_appeared": bool(b1), "30f_b1_confirmed": bool(b1 and b1["confirm_time"]), "daily_bottom_fractal": bottom, "valid_5f_b2_b2s_confirmation": parent_bound, "five_f_parent_binding": parent_binding, "official_ge_70_trigger": bool(b1 and bottom and parent_bound), "next_30f_execution_bar_available": execution})
        waterfall = build_gate_waterfall(gates_input)
        bottom_path = PROJECT_ROOT / "outputs/phase-1-14-entry-confidence-v3/daily_bottom_fractal_event_ledger.jsonl"
        universe_by_symbol = {symbol: "official" for symbol in official_symbols} | {symbol: "diagnostic_only" for symbol in diagnostic_symbols}
        recheck, policy_rows = _episode_recheck(episodes, coverage, lifecycle, _read_jsonl(bottom_path) if bottom_path.is_file() else [], expected, universe_by_symbol)
        planner_expected = [{**row, "mode": "predictive"} for row in expected if row["level"] in {"30f", "5f"}]
        planner = plan_micro_backfill_v4(planner_expected, _actual_grid(intraday_runs))
        # PIT market-cap evidence is absent, so every proposed read-only plan row is diagnostic.
        planner["manifest"] = [{**row, "universe_class": "diagnostic_only"} for row in planner["manifest"]]
        official_policy_rows = [row for row in policy_rows if row["universe_class"] == "official"]
        diagnostic_policy_rows = [row for row in policy_rows if row["universe_class"] == "diagnostic_only"]
        official_count = sum(bool(row["entry_triggered"]) for row in official_policy_rows if row["policy_contract_official"])
        candidate_count = sum(bool(row["entry_triggered"]) for row in official_policy_rows if row["policy"] == "candidate_1p_research_only")
        diagnostic_candidate_count = sum(bool(row["entry_triggered"]) for row in diagnostic_policy_rows if row["policy"] == "candidate_1p_research_only")
        admission_valid = bool(planner["manifest"]) and all(planner["admission"].get(key) for key in ("expected_kline_cutoff_only", "no_existing_covered_cutoff", "exact_symbol_level_mode_cutoff_key", "execute_hardcoded_false"))
        resource_valid = planner["resource_estimate"]["planned_runs"] == len(planner["manifest"]) and planner["resource_estimate"]["estimated_kline_reads"] > 0
        semantic_blocker = any("blocked_unreconstructable" in row.get("gate_status", {}).values() for row in waterfall["rows"])
        official_missing_cutoff_count = sum(universe_by_symbol.get(row["symbol"]) == "official" for row in coverage["missing_cutoffs"])
        diagnostic_missing_cutoff_count = sum(universe_by_symbol.get(row["symbol"]) == "diagnostic_only" for row in coverage["missing_cutoffs"])
        official_daily_episode_count = sum(row["symbol"] in set(official_symbols) for row in daily_episodes)
        decision_inputs = {"official_eligible_symbol_count": universe["official_eligible_symbol_count"], "observable_symbol_count": universe["observable_symbol_count"], "diagnostic_symbol_count": universe["diagnostic_symbol_count"], "official_daily_episode_count": official_daily_episode_count, "official_trigger_count": official_count, "official_candidate_trigger_count": candidate_count, "official_missing_cutoff_count": official_missing_cutoff_count, "semantic_blocker": semantic_blocker}
        decision = "F_DATA_OR_SEMANTIC_BLOCKED" if scoped_audit else decide_next_phase(exact_missing_cutoff_count=official_missing_cutoff_count, official_trigger_count=official_count, candidate_trigger_count=candidate_count, daily_episode_count=official_daily_episode_count, symbol_count=universe["official_eligible_symbol_count"], semantic_blocker=semantic_blocker, backfill_admission_valid=admission_valid, resource_estimate_valid=resource_valid)
        in_transaction_after = await _snapshot_conn(conn, transaction_snapshot=transaction_snapshot)
        await transaction.commit()
    except Exception:
        await transaction.rollback()
        raise
    finally:
        await pool.release(conn)
        await pool.close()
    post_pool = await create_pool()
    post_conn = await post_pool.acquire()
    post_transaction = post_conn.transaction(isolation="repeatable_read", readonly=True)
    try:
        await post_transaction.start()
        post_snapshot = await post_conn.fetchval("select txid_current_snapshot()")
        post_commit_after = await _snapshot_conn(post_conn, transaction_snapshot=post_snapshot)
        await post_transaction.commit()
    except Exception:
        await post_transaction.rollback()
        raise
    finally:
        await post_pool.release(post_conn)
        await post_pool.close()
    after = {**post_commit_after, "consistency_scope": "post_commit_readonly_guard", "in_transaction_after": in_transaction_after}
    _write_json(output_dir / "database_readonly_snapshot_after.json", after)
    _write_jsonl(output_dir / "expected_intraday_cutoff_grid.jsonl", expected); _write_jsonl(output_dir / "actual_intraday_run_grid.jsonl", _actual_grid(intraday_runs))
    _write_json(output_dir / "intraday_run_coverage_v3.json", coverage); (output_dir / "intraday_run_coverage_v3.md").write_text(_md("Intraday Run Coverage V3", coverage["summary"]), encoding="utf-8"); _write_csv(output_dir / "intraday_run_coverage_by_episode.csv", coverage["rows"]); _write_jsonl(output_dir / "intraday_run_coverage_missing_cutoffs.jsonl", coverage["missing_cutoffs"]); _write_jsonl(output_dir / "intraday_run_duplicate_cutoffs.jsonl", coverage["duplicate_cutoffs"])
    filtered = {"30f_b1": [row for row in lifecycle if row["level"] == "30f" and row["side"] == "buy" and row["bsp_type"] == "1"], "30f_1p": [row for row in lifecycle if row["level"] == "30f" and row["side"] == "buy" and row["bsp_type"] == "1p"], "5f_b2_b2s": [row for row in lifecycle if row["level"] == "5f" and row["side"] == "buy" and row["bsp_type"] in {"2", "2s"}]}
    _write_jsonl(output_dir / "signal_lifecycle_30f_b1.jsonl", filtered["30f_b1"]); _write_jsonl(output_dir / "signal_lifecycle_30f_1p.jsonl", filtered["30f_1p"]); _write_jsonl(output_dir / "signal_lifecycle_5f_b2_b2s.jsonl", filtered["5f_b2_b2s"])
    lifecycle_summary = {"scope_policy": "A_only_runs_with_cutoffs_in_expected_episode_grid", "counts": {name: len(rows) for name, rows in filtered.items()}, "first_seen_confirm_disappear_separated": True, "cutoff_gap_signal_count": sum(row["cutoff_gap"] for row in lifecycle)}; _write_json(output_dir / "signal_lifecycle_summary.json", lifecycle_summary); (output_dir / "signal_lifecycle_summary.md").write_text(_md("Signal Lifecycle Summary", lifecycle_summary), encoding="utf-8")
    semantics = {"policies": ["official_calendar_window_first_seen", "official_calendar_window_confirm_time", "diagnostic_trading_session_window_first_seen", "candidate_1p_research_only"], "policy_contract_official_policy": "official_calendar_window_first_seen", "official_universe_result_is_separate": True, "rows": policy_rows}
    _write_json(output_dir / "trigger_window_semantics_v2.json", semantics)
    _write_jsonl(output_dir / "official_policy_rows.jsonl", official_policy_rows)
    _write_jsonl(output_dir / "diagnostic_policy_rows.jsonl", diagnostic_policy_rows)
    (output_dir / "trigger_window_semantics_v2.md").write_text(_md("Trigger Window Semantics V2", {"policy_contract_counts": dict(Counter(row["policy"] for row in policy_rows)), "official_universe_rows": len(official_policy_rows), "diagnostic_only_rows": len(diagnostic_policy_rows)}), encoding="utf-8")
    recheck_causes = Counter(reason for row in recheck for mode in row["mode_results"] for reason in mode["root_causes"])
    _write_json(output_dir / "phase_1_20r_8_episode_recheck.json", {"episodes": recheck}); (output_dir / "phase_1_20r_8_episode_recheck.md").write_text(_md("Phase 1.20R Eight Episode Recheck", {"episode_count": len(recheck), "root_causes": dict(recheck_causes)}), encoding="utf-8")
    _write_json(output_dir / "observable_research_universe.json", universe); (output_dir / "observable_research_universe.md").write_text(_md("Observable Research Universe", universe), encoding="utf-8"); _write_jsonl(output_dir / "weekly_context_episodes_v2.jsonl", weekly_episodes); _write_jsonl(output_dir / "daily_setup_episodes_v2.jsonl", daily_episodes)
    _write_json(output_dir / "expanded_gate_waterfall.json", waterfall)
    official_waterfall = {"rows": [row for row in waterfall["rows"] if row["symbol"] in set(universe["official_symbols"])], "official_eligible_count": universe["official_eligible_count"]}
    diagnostic_waterfall = {"rows": [row for row in waterfall["rows"] if row["symbol"] in set(universe["diagnostic_only_symbols"])], "diagnostic_only_count": universe["diagnostic_only_count"]}
    _write_json(output_dir / "official_gate_waterfall.json", official_waterfall)
    _write_json(output_dir / "diagnostic_gate_waterfall.json", diagnostic_waterfall)
    _write_jsonl(output_dir / "official_observable_symbols.jsonl", [{"symbol": symbol, "universe_class": "official"} for symbol in universe["official_symbols"]])
    _write_jsonl(output_dir / "diagnostic_observable_symbols.jsonl", [{"symbol": symbol, "universe_class": "diagnostic_only"} for symbol in universe["diagnostic_only_symbols"]])
    _write_jsonl(output_dir / "official_daily_setup_episodes.jsonl", [row for row in daily_episodes if row["symbol"] in set(universe["official_symbols"])])
    _write_jsonl(output_dir / "diagnostic_daily_setup_episodes.jsonl", [row for row in daily_episodes if row["symbol"] in set(universe["diagnostic_only_symbols"])])
    (output_dir / "expanded_gate_waterfall.md").write_text(_md("Expanded Gate Waterfall", {"gate_pass_counts": waterfall["gate_pass_counts"], "blocker_counts": waterfall["blocker_counts"]}), encoding="utf-8")
    _write_csv(output_dir / "expanded_gate_waterfall_by_symbol.csv", [{"symbol": symbol, "entry_episode_count": sum(row["symbol"] == symbol for row in waterfall["rows"]), "official_trigger_count": sum(row["official_ge_70_trigger"] is True for row in waterfall["rows"] if row["symbol"] == symbol)} for symbol in sorted({row["symbol"] for row in waterfall["rows"]})])
    _write_csv(output_dir / "expanded_gate_waterfall_by_year.csv", [{"year": year, "entry_episode_count": sum(row["year"] == year for row in waterfall["rows"])} for year in sorted({row["year"] for row in waterfall["rows"]})])
    matrix = {"official_universe_policy_contract_trigger_count": official_count, "official_universe_candidate_trigger_count": candidate_count, "diagnostic_policy_contract_trigger_count": sum(bool(row["entry_triggered"]) for row in diagnostic_policy_rows if row["policy_contract_official"]), "diagnostic_candidate_trigger_count": diagnostic_candidate_count, "diagnostic_trading_session_window_first_seen": sum(row["entry_triggered"] for row in diagnostic_policy_rows if row["policy"] == "diagnostic_trading_session_window_first_seen"), "candidate_1p_research_only": diagnostic_candidate_count, "official_eligible_symbol_count": universe["official_eligible_symbol_count"], "observable_symbol_count": universe["observable_symbol_count"], "diagnostic_symbol_count": universe["diagnostic_symbol_count"]}
    matrix["official_contract_unchanged"] = True; matrix["diagnostic_policies_not_official"] = True
    _write_json(output_dir / "policy_counterfactual_matrix.json", matrix); (output_dir / "policy_counterfactual_matrix.md").write_text(_md("Policy Counterfactual Matrix", matrix), encoding="utf-8")
    _write_json(output_dir / "micro_backfill_v4_admission.json", planner); (output_dir / "micro_backfill_v4_admission.md").write_text(_md("Micro Backfill V4 Admission", {key: planner[key] for key in ("execute", "decision", "raw_expected_episode_cutoff_rows", "deduplicated_rows", "planned_runs", "admission", "resource_estimate")}), encoding="utf-8"); _write_csv(output_dir / "micro_backfill_v4_manifest.csv", planner["manifest"])
    next_payload = {"next_phase_decision": decision, "decision_inputs": decision_inputs, "decision_rationale": "Decision thresholds use official_eligible_symbol_count and official_daily_episode_count only; observable and diagnostic counts are reported for scope transparency.", "exact_missing_cutoff_count": len(coverage["missing_cutoffs"]), "official_missing_cutoff_count": official_missing_cutoff_count, "diagnostic_missing_cutoff_count": diagnostic_missing_cutoff_count, "official_trigger_count": official_count, "official_candidate_trigger_count": candidate_count, "diagnostic_candidate_trigger_count": diagnostic_candidate_count, "official_eligible_symbol_count": universe["official_eligible_symbol_count"], "observable_symbol_count": universe["observable_symbol_count"], "diagnostic_symbol_count": universe["diagnostic_symbol_count"], "official_daily_episode_count": official_daily_episode_count, "backfill_admission_valid": admission_valid, "resource_estimate_valid": resource_valid, "semantic_blocker": semantic_blocker, "scoped_audit_not_full_universe": scoped_audit, "execute_backfill": False}; _write_json(output_dir / "next_phase_decision.json", next_payload); (output_dir / "next_phase_decision.md").write_text(_md("Next Phase Decision", next_payload), encoding="utf-8")
    def trace(sample: Any, *, gate_reason: Any = None, grid_rows: list[dict[str, Any]] | None = None, require_grid: bool = False) -> dict[str, Any]:
        if not isinstance(sample, dict):
            return {"available": False, "unavailable_reason": str(sample), "run_id": None, "cutoff": None, "point_time": None, "first_seen": None, "confirm": None, "disappear": None, "kline_grid": [], "gate_reason": gate_reason}
        data = sample
        payload = {"run_id": data.get("run_id") or data.get("first_seen_run_id"), "cutoff": data.get("cutoff_bar_end") or data.get("first_actual_cutoff"), "point_time": data.get("point_time"), "first_seen": data.get("first_seen_time"), "confirm": data.get("confirm_time"), "disappear": data.get("disappear_time")}
        missing = [name for name in ("run_id", "cutoff", "point_time", "first_seen") if payload[name] is None]
        source_grid = expected if grid_rows is None else grid_rows
        kline_grid = [row for row in source_grid if data.get("episode_id") and row["episode_id"] == data.get("episode_id")]
        if require_grid and not kline_grid:
            missing.append("kline_grid")
        if missing:
            return {"available": False, "unavailable_reason": "sample_missing_required_trace_fields:" + ",".join(missing), **payload, "kline_grid": kline_grid, "gate_reason": gate_reason or data.get("blocker") or data.get("root_causes"), "evidence": data}
        return {"available": True, **payload, "kline_grid": kline_grid, "gate_reason": gate_reason or data.get("blocker") or data.get("root_causes"), "evidence": data}
    expanded_confirmed_b1 = [row for row in expanded_lifecycle if row["level"] == "30f" and row["side"] == "buy" and row["bsp_type"] == "1" and row["confirm_time"]]
    candidate_trace = next((row for row in policy_rows if row["policy"] == "candidate_1p_research_only" and row["entry_triggered"]), "candidate_policy_has_zero_triggers")
    five_binding_trace = next(({**mode["five_f"], "binding": mode["five_f_parent_binding"]} for episode in recheck for mode in episode["mode_results"] if mode["five_f"]), "no_5f_b2_b2s_binding_sample_in_episode_scope")
    def coverage_evidence(sample: Any, trace_type: str) -> dict[str, Any]:
        if not isinstance(sample, dict):
            return {"available": False, "trace_type": trace_type, "unavailable_reason": str(sample)}
        sample_expected = [row for row in expected if row["episode_id"] == sample["episode_id"] and row["symbol"] == sample["symbol"] and row["level"] == sample["level"]]
        expected_cutoffs = {iso_utc(row["cutoff_bar_end"]) for row in sample_expected}
        cutoff_to_actual_runs: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in _actual_grid(intraday_runs):
            if run["symbol"] == sample["symbol"] and run["level"] == sample["level"] and run["mode"] == sample["mode"] and iso_utc(run["cutoff_bar_end"]) in expected_cutoffs:
                cutoff_to_actual_runs[iso_utc(run["cutoff_bar_end"])].append({"run_id": run["run_id"], "run_group_id": run["run_group_id"]})
        actual_runs = {cutoff: sorted(rows, key=lambda row: row["run_id"]) for cutoff, rows in sorted(cutoff_to_actual_runs.items())}
        return {
            "available": True,
            "trace_type": trace_type,
            "universe_class": universe_by_symbol.get(sample["symbol"], "diagnostic_only"),
            "episode_id": sample["episode_id"],
            "symbol": sample["symbol"],
            "level": sample["level"],
            "mode": sample["mode"],
            "coverage_classification": sample["coverage_classification"],
            "expected_cutoff_count": sample["expected_cutoff_count"],
            "covered_cutoff_count": len(actual_runs),
            "actual_cutoff_count": sample["covered_cutoff_count"],
            "first_expected_cutoff": sample["first_expected_cutoff"],
            "last_expected_cutoff": sample["last_expected_cutoff"],
            "first_actual_cutoff": sample["first_actual_cutoff"],
            "last_actual_cutoff": sample["last_actual_cutoff"],
            "expected_cutoffs": sorted(expected_cutoffs),
            "missing_cutoffs": sample.get("missing_cutoffs", []),
            "cutoff_to_actual_runs": actual_runs,
            "run_ids": sorted({row["run_id"] for rows in actual_runs.values() for row in rows}),
            "duplicate_provenance": [row for row in coverage["duplicate_cutoffs"] if row["episode_id"] == sample["episode_id"] and row["symbol"] == sample["symbol"] and row["level"] == sample["level"] and row["mode"] == sample["mode"]],
        }
    coverage_sample = next((row for row in coverage["rows"] if row["coverage_classification"] == "complete"), None)
    coverage_gap_sample = next((row for row in coverage["rows"] if row["coverage_classification"] in {"partial", "none"}), None)
    coverage_trace = coverage_evidence(coverage_sample, "coverage_complete")
    coverage_gap_trace = coverage_evidence(coverage_gap_sample, "coverage_gap")
    official_nearest = max(official_policy_rows, key=lambda row: row["confidence"], default=None)
    diagnostic_nearest = max(diagnostic_policy_rows, key=lambda row: row["confidence"], default=None)
    official_nearest_trace = trace(official_nearest or "no_official_candidate")
    official_nearest_trace["trace_type"] = "official_nearest_trigger"
    official_nearest_trace["universe_class"] = "official"
    diagnostic_nearest_trace = trace(diagnostic_nearest or "no_diagnostic_candidate")
    diagnostic_nearest_trace["trace_type"] = "diagnostic_nearest_trigger"
    diagnostic_nearest_trace["universe_class"] = "diagnostic_only"
    no_official_candidate_trace = {"available": False, "trace_type": "no_official_candidate", "universe_class": "official", "unavailable_reason": "no_official_candidate"} if official_nearest is None else {"available": False, "trace_type": "no_official_candidate", "universe_class": "official", "unavailable_reason": "official_candidate_available_in_closest_official_trigger"}
    gate_failure = next((row for row in waterfall["rows"] if row["blocker"]), None)
    gate_episode = next((row for row in daily_episodes if gate_failure and row["episode_id"] == gate_failure["episode_id"]), None)
    gate_signal = gate_episode and (gate_episode.get("daily_b2") or gate_episode.get("daily_b1"))
    gate_trace_sample = {**gate_failure, "run_id": gate_signal.get("first_seen_run_id"), "cutoff_bar_end": gate_signal.get("first_seen_time"), "point_time": gate_signal.get("point_time"), "first_seen_time": gate_signal.get("first_seen_time"), "confirm_time": gate_signal.get("confirm_time"), "disappear_time": gate_signal.get("disappear_time")} if gate_failure and gate_signal else "sample unavailable"
    traces = {"coverage_complete": coverage_trace, "coverage_gap": coverage_gap_trace, "b1_disappeared": trace(next((row for row in filtered["30f_b1"] if row["disappear_time"]), "no_disappeared_30f_b1_in_observable_lifecycle")), "b1_confirmed": trace(next(iter(expanded_confirmed_b1), "no_confirmed_30f_b1_in_expanded_observable_lifecycle")), "official_blocked_candidate": trace(candidate_trace, gate_reason="candidate_1p_research_only; official_30f_b1_not_present"), "five_f_parent_binding": trace(five_binding_trace, gate_reason=five_binding_trace.get("binding") if isinstance(five_binding_trace, dict) else None), "weekly_daily_gate_failure": trace(gate_trace_sample, gate_reason=gate_failure and gate_failure.get("blocker"), grid_rows=expanded_grid, require_grid=True), "closest_official_trigger": official_nearest_trace, "closest_diagnostic_trigger": diagnostic_nearest_trace, "no_official_candidate": no_official_candidate_trace}
    for trace in traces.values():
        if isinstance(trace, dict):
            trace.setdefault("universe_class", trace.get("evidence", {}).get("universe_class", "diagnostic_only"))
    for name, trace in traces.items(): (output_dir / "traces" / f"{name}.md").write_text(_md(name, trace if isinstance(trace, dict) else {"sample": trace}), encoding="utf-8")
    (output_dir / "trace_index.md").write_text("# Trace Index\n\n" + "\n".join(f"- `{name}`: `traces/{name}.md` available=`{trace['available']}`" for name, trace in traces.items()) + "\n", encoding="utf-8")
    unchanged = before["published_head_row_count"] == after["published_head_row_count"] and before["run_group_counts"] == after["run_group_counts"] and before["target_run_group_count"] == after["target_run_group_count"]
    finished = _now()
    report_output_dir = output_dir.parent / output_dir.name[1:].split(".staging-", 1)[0] if ".staging-" in output_dir.name else output_dir
    required_trace_names = ("coverage_complete", "coverage_gap", "b1_disappeared", "b1_confirmed", "official_blocked_candidate", "weekly_daily_gate_failure", "closest_official_trigger")
    unavailable_required_traces = [name for name in required_trace_names if not traces[name]["available"]]
    checklist = {"preflight": "completed", "real_kline_grid": "completed", "coverage_v3": "completed", "lifecycle": "completed", "contract_recheck": "completed", "observable_universe": "completed", "universe_counts": {key: universe[key] for key in ("official_eligible_symbol_count", "observable_symbol_count", "diagnostic_symbol_count")}, "waterfall": "completed_with_explicit_reconstructability_blockers", "backfill": "plan_only", "trace_task_status": "completed" if not unavailable_required_traces else "partial_required_samples_unavailable", "required_trace_unavailable": unavailable_required_traces, "in_transaction_consistency_only": "same repeatable-read snapshot; diagnostic consistency only", "post_commit_database_guard": "separate readonly repeatable-read connection compared with before snapshot", "database_unchanged": unchanged, "duration_started_at": started, "duration_finished_at": finished, "trace_availability": {name: row["available"] for name, row in traces.items()}}
    (output_dir / "phase_1_21_task_checklist_report.md").write_text(_md("Phase 1.21 Task Checklist", checklist), encoding="utf-8")
    report = {**checklist, "commands": ["python -m compileall app", "python -m pytest tests -q", "python -m app.cli.run_phase_1_21 --output-dir outputs/phase-1-21-intraday-grid-signal-lifecycle"], "readonly_sql": READ_ONLY_SQL, "old_cutoff_string_comparison_conclusion_obsolete": True, "current_missing_cutoff_count": len(coverage["missing_cutoffs"]), "current_manifest_rows": len(planner["manifest"]), "derived_parent_contract_source": "weekly_daily_b2_official_v1.0 §11.2 official reconstruction", "official_5f_scoring_contract": "Every official 5F score path requires buy-side B2/B2S, a buy-side 5F B1 after the 30F B1 and before that B2/B2S, and B2/B2S price >= 5F B1 price; direct 30F parent identity is supplementary only.", "next_phase_decision": decision, "all_coverage_ratios_bounded": coverage["summary"]["all_ratios_bounded"], "not_applicable_actual_sample_count": coverage["summary"]["not_applicable_sample_count"], "lifecycle_scope_policy": lifecycle_summary["scope_policy"], "precise_disappear_with_cutoff_gap_count": sum(row["disappear_time"] is not None and row["cutoff_gap"] for row in lifecycle), "expanded_confirmed_30f_b1_count": len(expanded_confirmed_b1), "b1_confirmed_trace_unavailable_reason": traces["b1_confirmed"].get("unavailable_reason"), "old_episode_count": len(episodes), "source_old_ledgers": {"30f": len(old_30f), "5f": len(old_5f)}, "weekly_input_episode_count": len(weekly_input), "output_dir": str(report_output_dir), "pagination_metrics": {"direct_intraday": direct_pagination_metrics, "expanded_structure": pagination_metrics}, "blockers": waterfall["blocker_counts"]}
    (output_dir / "phase_1_21_detailed_completion_report.md").write_text(_md("Phase 1.21 Detailed Completion Report", report), encoding="utf-8")
    return {"status": "DONE", **next_payload, "database_unchanged": unchanged}


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        return _windows_pid_alive(pid, kernel32.OpenProcess, ctypes.get_last_error, kernel32.CloseHandle)
    return _posix_pid_alive(pid, os.kill)


def _posix_pid_alive(pid: int, kill) -> bool:
    try:
        kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _windows_pid_alive(pid: int, open_process, get_last_error, close_handle) -> bool:
    handle = open_process(0x1000, False, pid)
    if not handle:
        # Only ERROR_INVALID_PARAMETER confirms a Windows PID is absent.
        return get_last_error() != 87
    close_handle(handle)
    return True


def _recoverable_lock(lock: Path) -> bool:
    try:
        owner = json.loads(lock.read_text(encoding="utf-8"))
        pid, token = int(owner.get("pid")), str(owner.get("token") or "")
    except (OSError, ValueError, TypeError):
        return False
    return bool(token) and (not _pid_alive(pid) or (pid == os.getpid() and owner.get("cleanup_deferred") is True))


def _is_windows() -> bool:
    return os.name == "nt"


def _acquire_posix_mutex(path: Path) -> int | None:
    import fcntl
    handle = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(handle)
        return None
    except Exception:
        os.close(handle)
        raise
    return handle


def _acquire_windows_mutex(path: Path) -> int | None:
    import msvcrt
    handle = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        if os.fstat(handle).st_size == 0:
            os.write(handle, b"\0")
        os.lseek(handle, 0, os.SEEK_SET)
        msvcrt.locking(handle, msvcrt.LK_NBLCK, 1)
    except OSError:
        os.close(handle)
        return None
    return handle


def _release_windows_mutex(handle: int) -> None:
    import msvcrt
    try:
        os.lseek(handle, 0, os.SEEK_SET)
        msvcrt.locking(handle, msvcrt.LK_UNLCK, 1)
    finally:
        os.close(handle)


def _release_posix_mutex(handle: int) -> None:
    import fcntl
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        os.close(handle)


class _OutputMutex:
    """Cross-process gate for lock inspection and stale-lock reclamation."""

    def __init__(self, target: Path) -> None:
        self._handle = None
        self._windows = _is_windows()
        digest = hashlib.sha256(str(target).encode("utf-8")).hexdigest()
        self._fallback = target.parent / f".phase1_21_mutex_{digest}"

    def acquire(self) -> bool:
        self._fallback.parent.mkdir(parents=True, exist_ok=True)
        self._handle = _acquire_windows_mutex(self._fallback) if self._windows else _acquire_posix_mutex(self._fallback)
        return self._handle is not None

    def release(self) -> None:
        if self._handle is None:
            return
        if self._windows:
            _release_windows_mutex(self._handle)
        else:
            _release_posix_mutex(self._handle)
        self._handle = None


async def _run_phase_1_21_with_file_lock(*, output_dir: Path = DEFAULT_OUTPUT_DIR, reconstruction_start: datetime | None = None, reconstruction_end: datetime | None = None) -> dict[str, Any]:
    """Run in a private staging directory and promote only a complete output set."""
    target = output_dir.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    lock = target.parent / f".{target.name}.lock"
    try:
        handle = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        if not _recoverable_lock(lock):
            raise RuntimeError(f"Phase 1.21 output lock exists: {lock}; refusing concurrent run") from error
        lock.unlink()
        handle = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    staging = target.parent / f".{target.name}.staging-{uuid.uuid4().hex}"
    backup: Path | None = None
    try:
        os.write(handle, json.dumps({"pid": os.getpid(), "started_at": _now(), "token": uuid.uuid4().hex, "cleanup_deferred": False}).encode("utf-8"))
        staging.mkdir()
        result = await _run_phase_1_21_impl(output_dir=staging, reconstruction_start=reconstruction_start, reconstruction_end=reconstruction_end)
        required = ("source_artifact_manifest.json", "database_readonly_snapshot_before.json", "database_readonly_snapshot_after.json", "intraday_run_coverage_v3.json", "next_phase_decision.json", "phase_1_21_detailed_completion_report.md")
        missing = [name for name in required if not (staging / name).is_file()]
        if missing:
            raise RuntimeError(f"Phase 1.21 staging validation failed: {missing}")
        if target.exists():
            backup = target.parent / f".{target.name}.backup-{uuid.uuid4().hex}"
            os.replace(target, backup)
        try:
            os.replace(staging, target)
        except Exception:
            if backup is not None and backup.exists():
                os.replace(backup, target)
            raise
        if backup is not None and backup.exists():
            shutil.rmtree(backup)
        return result
    finally:
        os.close(handle)
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        try:
            lock.unlink()
        except FileNotFoundError:
            pass
        except PermissionError as error:
            try:
                owner = json.loads(lock.read_text(encoding="utf-8"))
                owner["cleanup_deferred"] = True
                lock.write_text(json.dumps(owner), encoding="utf-8")
            except OSError:
                pass
            raise RuntimeError(f"Phase 1.21 output lock cleanup failed; promoted target remains intact at {target}: {lock}") from error


async def run_phase_1_21(*, output_dir: Path = DEFAULT_OUTPUT_DIR, reconstruction_start: datetime | None = None, reconstruction_end: datetime | None = None) -> dict[str, Any]:
    mutex = _OutputMutex(output_dir.resolve())
    if not mutex.acquire():
        raise RuntimeError(f"Phase 1.21 output mutex is held for {output_dir}; refusing concurrent run")
    try:
        return await _run_phase_1_21_with_file_lock(output_dir=output_dir, reconstruction_start=reconstruction_start, reconstruction_end=reconstruction_end)
    finally:
        mutex.release()


def run_phase_1_21_sync(*, output_dir: Path = DEFAULT_OUTPUT_DIR, reconstruction_start: datetime | None = None, reconstruction_end: datetime | None = None) -> dict[str, Any]:
    return asyncio.run(run_phase_1_21(output_dir=output_dir, reconstruction_start=reconstruction_start, reconstruction_end=reconstruction_end))
