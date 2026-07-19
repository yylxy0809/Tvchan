"""Opt-in acceptance for migration 048 and exact supersession fencing."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest

from collector.kline_quarantine_supersession import create_supersessions
from collector.module_c_eligibility import _load_quarantine_inputs


TEST_DATABASE_URL = os.getenv("QUARANTINE_SUPERSESSION_TEST_DATABASE_URL", "")
ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set QUARANTINE_SUPERSESSION_TEST_DATABASE_URL for a disposable database",
)
def test_migration_twice_append_only_and_new_quarantine_fails_closed() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        conn = await asyncpg.connect(TEST_DATABASE_URL)
        schema = f"quarantine_supersession_{uuid4().hex}"
        try:
            await conn.execute(f'create schema "{schema}"')
            await conn.execute(f'set search_path to "{schema}"')
            await conn.execute(
                """
                create table symbols (
                    id integer generated always as identity primary key,
                    code text not null,
                    exchange text not null,
                    name text not null
                )
                """
            )
            for migration in (
                "024_kline_canonical_audit.sql",
                "029_kline_import_quarantine.sql",
                "048_kline_import_quarantine_supersession.sql",
                "048_kline_import_quarantine_supersession.sql",
            ):
                await conn.execute(
                    (ROOT / "db" / "sql" / migration).read_text(encoding="utf-8")
                )

            symbol_id = await conn.fetchval(
                "insert into symbols(code,exchange,name) values('688001','SH','x') returning id"
            )
            import_run_id = uuid4()
            audit_run_id = uuid4()
            await conn.execute(
                """
                insert into kline_import_runs(
                    import_run_id,source_name,started_at,completed_at,status
                ) values($1,'parquet_native','2026-07-10T00:00:00Z',
                         '2026-07-11T00:00:00Z','completed')
                """,
                import_run_id,
            )
            await conn.execute(
                """
                insert into kline_audit_runs(
                    audit_run_id,started_at,completed_at,status,apply_mode,parameters,summary
                ) values($1,'2026-07-19T08:00:00Z','2026-07-19T09:00:00Z',
                         'completed',false,$2::jsonb,$3::jsonb)
                """,
                audit_run_id,
                '{"contract_version":"module-c-strict-audit-v2",'
                '"observed_at":"2026-07-19T08:00:00Z"}',
                '{"evidence_complete":true,"evidence_sha256":"' + "a" * 64 + '"}',
            )
            await conn.execute(
                """
                insert into kline_audit_checkpoints(
                    audit_run_id,symbol_id,timeframe,shard_start,shard_end,
                    status,rows_scanned,metadata
                ) values($1,$2,5,'2020-01-01T00:00:00Z','2026-07-17T07:00:00Z',
                         'completed',100,'{"disposition":"eligible"}')
                """,
                audit_run_id,
                symbol_id,
            )

            async def add_quarantine(source_row: int) -> None:
                await conn.execute(
                    """
                    insert into kline_import_quarantine(
                        import_run_id,source_name,source_ref,source_row,
                        symbol_text,timeframe,reason,raw_payload
                    ) values($1,'parquet_native','member.parquet',$2,
                             '688001.SH','5f','missing_source_file','{}')
                    """,
                    import_run_id,
                    source_row,
                )

            await add_quarantine(1)
            result = await create_supersessions(
                conn,
                audit_run_id=audit_run_id,
                import_run_ids=[import_run_id],
                justification="disposable acceptance",
                dry_run=False,
            )
            assert result["superseded_groups"] == 1
            unresolved, missing = await _load_quarantine_inputs(
                conn,
                audit_run_id=str(audit_run_id),
                audit_evidence_sha256="a" * 64,
            )
            assert unresolved == {}
            assert missing == {}

            await add_quarantine(2)
            _, missing = await _load_quarantine_inputs(
                conn,
                audit_run_id=str(audit_run_id),
                audit_evidence_sha256="a" * 64,
            )
            assert missing == {("688001.SH", "5f"): 2}

            with pytest.raises(asyncpg.RaiseError, match="append-only"):
                await conn.execute(
                    "update kline_import_quarantine_supersessions set justification='x'"
                )
            with pytest.raises(asyncpg.RaiseError, match="append-only"):
                await conn.execute("delete from kline_import_quarantine_supersessions")
            with pytest.raises(asyncpg.RaiseError, match="append-only"):
                await conn.execute("delete from kline_import_quarantine")
        finally:
            await conn.execute("reset search_path")
            await conn.execute(f'drop schema if exists "{schema}" cascade')
            await conn.close()

    asyncio.run(scenario())
