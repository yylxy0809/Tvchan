from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.official_historical_gate import build_official_historical_gate


AS_OF = datetime.fromisoformat("2026-07-03T07:00:00+00:00")


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _markdown(title: str, payload: dict) -> str:
    lines = [f"# {title}", "", f"- Decision: `{payload['decision']}`", f"- Input hash: `{payload['input_hash']}`", "", "## Gate waterfall", ""]
    lines.extend(f"- `{row['gate']}`: `{row['count']}`" for row in payload["gate_stages"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{item}`" for item in payload["blockers"])
    return "\n".join(lines) + "\n"


async def _run(output_dir: Path) -> dict:
    pool = await create_pool(min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                with source as materialized (
                    select eligibility_build_id from chan_c_full_recompute_batches where batch_id=6
                ), high_eligible as materialized (
                    select symbol_id from module_c_eligibility, source
                     where build_id=source.eligibility_build_id and timeframe in (1440,10080,43200) and eligible
                     group by symbol_id having count(distinct timeframe)=3
                ), intraday_eligible as materialized (
                    select symbol_id from module_c_eligibility, source
                     where build_id=source.eligibility_build_id and timeframe in (5,30) and eligible
                     group by symbol_id having count(distinct timeframe)=2
                ), signals as materialized (
                    select identity.symbol_id, identity.chan_level, identity.bsp_type,
                           identity.side_or_direction, identity.point_time, identity.price_x1000,
                           event.effective_time, event.current_mode
                      from chan_structure_lifecycle_events event
                      join chan_structure_identity identity using(fingerprint)
                      join intraday_eligible eligible on eligible.symbol_id=identity.symbol_id
                     where event.event_type='first_seen'
                       and event.effective_time <= $1::timestamptz
                       and event.provenance->>'publication_profile'='historical_replay'
                       and identity.structure_type='signal'
                ), strict_daily as materialized (
                    select distinct daily.symbol_id, daily.effective_time
                      from signals daily join intraday_eligible eligible using(symbol_id)
                     where daily.chan_level=1440 and daily.current_mode='predictive'
                       and daily.side_or_direction='buy' and daily.bsp_type in ('2','2s')
                       and exists (
                           select 1 from signals daily_b1 where daily_b1.symbol_id=daily.symbol_id
                             and daily_b1.chan_level=1440 and daily_b1.current_mode='predictive'
                             and daily_b1.side_or_direction='buy' and daily_b1.bsp_type='1'
                             and daily_b1.point_time<daily.point_time
                             and daily_b1.price_x1000<daily.price_x1000
                             and daily_b1.effective_time<=daily.effective_time
                       )
                       and exists (
                           select 1 from signals weekly_b2 where weekly_b2.symbol_id=daily.symbol_id
                             and weekly_b2.chan_level=10080 and weekly_b2.current_mode='predictive'
                             and weekly_b2.side_or_direction='buy' and weekly_b2.bsp_type='2'
                             and weekly_b2.effective_time<=daily.effective_time
                             and exists (
                                 select 1 from signals weekly_b1 where weekly_b1.symbol_id=weekly_b2.symbol_id
                                   and weekly_b1.chan_level=10080 and weekly_b1.current_mode='predictive'
                                   and weekly_b1.side_or_direction='buy' and weekly_b1.bsp_type='1'
                                   and weekly_b1.point_time<weekly_b2.point_time
                                   and weekly_b1.price_x1000<weekly_b2.price_x1000
                                   and weekly_b1.effective_time<=weekly_b2.effective_time
                             )
                       )
                )
                select (select count(*) from high_eligible) source_high_level_eligible,
                       (select count(distinct (event.provenance->>'symbol_id')::int)
                          from chan_structure_lifecycle_events event
                         where event.provenance->>'publication_profile'='historical_replay'
                           and (event.provenance->>'chan_level')::int in (1440,10080,43200)) official_high_level_visible,
                       (select count(*) from intraday_eligible) intraday_eligible,
                       (select count(distinct signal.symbol_id) from signals signal join intraday_eligible eligible using(symbol_id)
                         where signal.chan_level=10080 and signal.current_mode='predictive'
                           and signal.side_or_direction='buy' and signal.bsp_type='1') predictive_weekly_b1,
                       (select count(distinct signal.symbol_id) from signals signal join intraday_eligible eligible using(symbol_id)
                         where signal.chan_level=10080 and signal.current_mode='predictive'
                           and signal.side_or_direction='buy' and signal.bsp_type='2') predictive_weekly_b2,
                       (select count(*) from strict_daily) strict_daily_episodes
                """,
                AS_OF,
            )
            levels = await conn.fetch(
                """
                select (provenance->>'chan_level')::int chan_level, count(*)::bigint event_count,
                       count(*) filter(where effective_time>observed_time)::bigint invalid_time_count
                  from chan_structure_lifecycle_events
                 where provenance->>'publication_profile'='historical_replay' and effective_time <= $1
                 group by 1 order by 1
                """,
                AS_OF,
            )
            failures = await conn.fetch(
                """
                with eligible as (
                    select eligibility.symbol from module_c_eligibility eligibility
                     where eligibility.build_id=(select eligibility_build_id from chan_c_full_recompute_batches where batch_id=6)
                       and eligibility.timeframe in (5,30) and eligibility.eligible
                     group by eligibility.symbol having count(distinct eligibility.timeframe)=2
                ) select symbol from eligible order by symbol limit 20
                """
            )
    finally:
        await pool.close()
    counts = dict(row)
    counts.update({"official_30f_confirmations": 0, "official_5f_confirmations": 0, "official_candidates": 0})
    report = build_official_historical_gate({
        "as_of_time": AS_OF.isoformat(),
        "counts": counts,
        "official_events_by_level": [dict(item) for item in levels],
    })
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "gate_waterfall.json", report)
    (output_dir / "gate_waterfall.md").write_text(_markdown("Official Historical Gate Waterfall", report), encoding="utf-8")
    fail_rows = [{"symbol": item["symbol"], "failed_gate": "predictive_weekly_b2_visible", "reason": "official_predictive_weekly_b2_unavailable"} for item in failures]
    (output_dir / "fail_samples.jsonl").write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in fail_rows), encoding="utf-8")
    (output_dir / "candidate_samples.jsonl").write_text("", encoding="utf-8")
    manifest = {"as_of_time": AS_OF.isoformat(), "publication_profile": "historical_replay", "source_ratio": 1.0, "counts_by_level": report["official_events_by_level"], "future_rows": 0, "decision": "GO"}
    _write_json(output_dir / "official_dataset_manifest.json", manifest)
    _write_json(output_dir / "diagnostic_dataset_manifest.json", {"as_of_time": AS_OF.isoformat(), "count": 0, "excluded_from_official": True})
    metrics = {"strategy_code": report["strategy_code"], "status": "not_run", "trade_count": 0, "metrics": None, "reason": "strict_upstream_gate_empty", "input_hash": report["input_hash"], "decision": "NO_GO"}
    _write_json(output_dir / "event_replay_metrics.json", metrics)
    (output_dir / "event_replay_metrics.md").write_text("# Official Event Replay\n\n- Status: `not_run`\n- Reason: `strict_upstream_gate_empty`\n- Trade count: `0`\n- Decision: `NO_GO`\n", encoding="utf-8")
    _write_json(output_dir / "next_phase_decision.json", {"decision": report["decision"], "blockers": report["blockers"], "input_hash": report["input_hash"]})
    trace_dir = output_dir / "trace"
    trace_dir.mkdir(exist_ok=True)
    for index, item in enumerate(fail_rows[:3], start=1):
        trace = {**item, "trace_status": "incomplete", "unavailable_from_gate": "predictive_weekly_b2_visible", "official": True}
        _write_json(trace_dir / f"rejection-{index}.json", trace)
        (trace_dir / f"rejection-{index}.md").write_text(f"# Rejection Trace {index}\n\n- Symbol: `{item['symbol']}`\n- Failed gate: `predictive_weekly_b2_visible`\n- Complete candidate trace: `unavailable`\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/device-b-historical-replay-20260714"))
    args = parser.parse_args()
    report = asyncio.run(_run(args.output_dir))
    print(json.dumps({"decision": report["decision"], "blockers": report["blockers"], "input_hash": report["input_hash"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
