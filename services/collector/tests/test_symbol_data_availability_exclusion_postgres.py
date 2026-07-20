"""Opt-in acceptance for migration 049 and audit-bound symbol deactivation."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from collector.kline_sql_gate import _manifest_sha256
from collector.symbol_data_availability_exclusion import (
    REQUIRED_TIMEFRAMES,
    active_universe_manifest_sha256,
    exclude_unavailable_symbols,
)


TEST_DATABASE_URL = os.getenv("SYMBOL_EXCLUSION_TEST_DATABASE_URL", "")
ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set SYMBOL_EXCLUSION_TEST_DATABASE_URL for a disposable database",
)
def test_migration_twice_deactivates_without_deleting_and_blocks_reactivation() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        conn = await asyncpg.connect(TEST_DATABASE_URL)
        schema = f"symbol_exclusion_{uuid4().hex}"
        try:
            await conn.execute(f'create schema "{schema}"')
            await conn.execute(f'set search_path to "{schema}"')
            await conn.execute(
                """
                create table symbols (
                    id integer generated always as identity primary key,
                    code text not null,
                    exchange text not null,
                    market text not null default 'A_SHARE',
                    is_active boolean not null default true,
                    updated_at timestamptz not null default clock_timestamp()
                );
                create table kline_audit_runs (
                    audit_run_id uuid primary key,
                    status text not null,
                    apply_mode boolean not null,
                    parameters jsonb not null,
                    summary jsonb not null
                );
                create table kline_audit_checkpoints (
                    audit_run_id uuid not null references kline_audit_runs,
                    symbol_id integer not null references symbols,
                    timeframe integer not null,
                    status text not null,
                    rows_scanned bigint not null,
                    metadata jsonb not null,
                    primary key(audit_run_id,symbol_id,timeframe)
                );
                create table kline_scope_catalog_generations (
                    generation_id uuid primary key,
                    status text not null
                );
                create table kline_scope_catalog_control (
                    control_key text primary key,
                    active_generation_id uuid not null
                        references kline_scope_catalog_generations,
                    revision bigint not null
                );
                create table kline_scope_catalog (
                    generation_id uuid not null,
                    symbol_id integer not null references symbols,
                    timeframe integer not null,
                    state text not null,
                    bounds_complete boolean not null,
                    min_ts timestamptz,
                    max_ts timestamptz,
                    updated_at timestamptz not null default clock_timestamp(),
                    primary key(generation_id,symbol_id,timeframe)
                );
                create table klines (
                    symbol_id integer not null references symbols,
                    timeframe integer not null,
                    ts timestamptz not null,
                    primary key(symbol_id,timeframe,ts)
                )
                """
            )
            migration = (
                ROOT / "db" / "sql" / "049_symbol_data_availability_exclusion.sql"
            ).read_text(encoding="utf-8")
            await conn.execute(migration)
            await conn.execute(migration)

            rows = await conn.fetch(
                """
                insert into symbols(code,exchange) values('600000','SH'),('920047','BJ')
                returning id as symbol_id,upper(code || '.' || exchange) as symbol
                """
            )
            symbols = [dict(row) for row in rows]
            audit_run_id = uuid4()
            generation_id = uuid4()
            await conn.execute(
                "insert into kline_scope_catalog_generations values($1,'complete')",
                generation_id,
            )
            await conn.execute(
                "insert into kline_scope_catalog_control values('active',$1,1)",
                generation_id,
            )
            parameters = {
                "contract_version": "module-c-strict-audit-v2",
                "timeframes": list(REQUIRED_TIMEFRAMES),
                "active_universe_count": 2,
                "active_universe_sha256": active_universe_manifest_sha256(symbols),
                "catalog_generation_id": str(generation_id),
                "catalog_control_revision": 1,
                "catalog_manifest_sha256": "0" * 64,
            }
            await conn.execute(
                "insert into kline_audit_runs values($1,'completed',false,$2::jsonb,$3::jsonb)",
                audit_run_id,
                json.dumps(parameters),
                json.dumps({"evidence_complete": True, "evidence_sha256": "a" * 64}),
            )
            for symbol in symbols:
                for timeframe in REQUIRED_TIMEFRAMES:
                    empty = symbol["symbol"] == "920047.BJ" and timeframe in (5, 30)
                    await conn.execute(
                        """
                        insert into kline_audit_checkpoints values(
                            $1,$2,$3,'completed',$4,$5::jsonb
                        )
                        """,
                        audit_run_id,
                        symbol["symbol_id"],
                        timeframe,
                        0 if empty else 100,
                        json.dumps({"disposition": "unresolved" if empty else "eligible"}),
                    )
                    await conn.execute(
                        """
                        insert into kline_scope_catalog values(
                            $1,$2,$3,$4,true,$5,$6
                        )
                        """,
                        generation_id,
                        symbol["symbol_id"],
                        timeframe,
                        "empty" if empty else "present",
                        None if empty else datetime(2020, 1, 1, tzinfo=timezone.utc),
                        None if empty else datetime(2026, 7, 17, 7, tzinfo=timezone.utc),
                    )
            catalog_rows = await conn.fetch(
                """
                select symbol_id,timeframe,state,bounds_complete,min_ts,max_ts,updated_at
                from kline_scope_catalog where generation_id=$1
                order by symbol_id,timeframe
                """,
                generation_id,
            )
            parameters["catalog_manifest_sha256"] = _manifest_sha256(
                [dict(row) for row in catalog_rows]
            )
            await conn.execute(
                "update kline_audit_runs set parameters=$2::jsonb where audit_run_id=$1",
                audit_run_id,
                json.dumps(parameters),
            )
            await conn.execute(
                "insert into klines values($1,5,'2026-07-17T07:00:00Z')",
                symbols[0]["symbol_id"],
            )
            before = await conn.fetchval("select count(*) from klines")

            await conn.execute(
                "update kline_scope_catalog_control set revision=2 where control_key='active'"
            )
            with pytest.raises(ValueError, match="catalog binding drifted"):
                await exclude_unavailable_symbols(
                    conn,
                    audit_run_id=UUID(str(audit_run_id)),
                    justification="disposable acceptance",
                    dry_run=True,
                )
            await conn.execute(
                "update kline_scope_catalog_control set revision=1 where control_key='active'"
            )

            result = await exclude_unavailable_symbols(
                conn,
                audit_run_id=UUID(str(audit_run_id)),
                justification="disposable acceptance",
                dry_run=False,
            )
            assert result["excluded_symbols"] == 1
            assert await conn.fetchval("select count(*) from klines") == before
            assert await conn.fetchval(
                "select is_active from symbols where code='920047'"
            ) is False

            await conn.execute("update symbols set is_active=true where code='920047'")
            assert await conn.fetchval(
                "select is_active from symbols where code='920047'"
            ) is False
            with pytest.raises(asyncpg.RaiseError, match="append-only"):
                await conn.execute("delete from symbol_data_availability_exclusions")
        finally:
            await conn.execute("reset search_path")
            await conn.execute(f'drop schema if exists "{schema}" cascade')
            await conn.close()

    asyncio.run(scenario())
