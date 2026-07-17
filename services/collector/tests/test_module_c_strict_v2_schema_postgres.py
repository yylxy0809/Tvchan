"""Opt-in PostgreSQL acceptance for migration 043 on a dedicated test DB."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest


TEST_DATABASE_URL = os.getenv("MODULE_C_SCHEMA_TEST_DATABASE_URL", "")
ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set MODULE_C_SCHEMA_TEST_DATABASE_URL for a dedicated PostgreSQL verification database",
)
def test_migration_043_twice_and_strict_v2_insert_contract() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        conn = await asyncpg.connect(TEST_DATABASE_URL)
        schema = f"strict_v2_schema_{uuid4().hex}"
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
                "031_module_c_eligibility.sql",
                "040_kline_scope_catalog.sql",
                "043_module_c_strict_v2_eligibility_provenance.sql",
                "043_module_c_strict_v2_eligibility_provenance.sql",
            ):
                await conn.execute((ROOT / "db" / "sql" / migration).read_text(encoding="utf-8"))

            legacy_id = uuid4()
            await conn.execute(
                """
                insert into module_c_eligibility_builds (
                    build_id, manifest_version, config_hash, active_universe_hash,
                    manifest_hash, active_symbols, disposition_rows, parameters, summary
                ) values ($1, $2, 'config', 'universe', 'manifest', 0, 0, '{}', '{}')
                """,
                legacy_id,
                f"legacy-{legacy_id}",
            )

            incomplete_id = uuid4()
            with pytest.raises(asyncpg.CheckViolationError):
                async with conn.transaction():
                    await conn.execute(
                        """
                        insert into module_c_eligibility_builds (
                            build_id, manifest_version, config_hash, active_universe_hash,
                            manifest_hash, active_symbols, disposition_rows, parameters, summary
                        ) values ($1, $2, 'config', 'universe', 'manifest', 0, 0,
                                  '{"policy":"strict-v2"}', '{}')
                        """,
                        incomplete_id,
                        f"incomplete-{incomplete_id}",
                    )

            audit_id = uuid4()
            generation_id = uuid4()
            await conn.execute(
                """
                insert into kline_audit_runs (audit_run_id, status)
                values ($1, 'completed')
                """,
                audit_id,
            )
            await conn.execute(
                """
                insert into kline_scope_catalog_generations (
                    generation_id, status, expected_scope_count, symbol_ids,
                    timeframes, completed_at
                ) values ($1, 'complete', 1, array[1], array[5], clock_timestamp())
                """,
                generation_id,
            )

            sha = "a" * 64

            async def insert_strict(
                build_id: object,
                manifest_version: str,
                *,
                audit_run_id: object = audit_id,
                audit_evidence_sha256: str | None = sha,
                audit_checkpoint_sha256: str | None = sha,
                freshness_contract_sha256: str | None = sha,
                catalog_generation: object = generation_id,
                catalog_manifest_sha256: str | None = sha,
                audit_active_universe_sha256: str | None = sha,
            ) -> None:
                await conn.execute(
                    """
                    insert into module_c_eligibility_builds (
                        build_id, manifest_version, config_hash, active_universe_hash,
                        manifest_hash, active_symbols, disposition_rows, parameters, summary,
                        canonical_audit_run_id, audit_evidence_sha256,
                        audit_checkpoint_sha256, freshness_contract_version,
                        freshness_contract_sha256, catalog_generation_id,
                        catalog_control_revision, catalog_manifest_sha256,
                        audit_active_universe_sha256
                    ) values (
                        $1, $2, 'config', 'universe', 'manifest', 0, 0,
                        '{"policy":"strict-v2"}', '{}', $3, $4, $5,
                        'module-c-authoritative-freshness-v1', $6, $7, 0, $8, $9
                    )
                    """,
                    build_id,
                    manifest_version,
                    audit_run_id,
                    audit_evidence_sha256,
                    audit_checkpoint_sha256,
                    freshness_contract_sha256,
                    catalog_generation,
                    catalog_manifest_sha256,
                    audit_active_universe_sha256,
                )

            for missing_sha in (
                "audit_evidence_sha256",
                "audit_checkpoint_sha256",
                "freshness_contract_sha256",
                "catalog_manifest_sha256",
                "audit_active_universe_sha256",
            ):
                missing_id = uuid4()
                with pytest.raises(asyncpg.CheckViolationError):
                    async with conn.transaction():
                        await insert_strict(
                            missing_id,
                            f"missing-{missing_sha}-{missing_id}",
                            **{missing_sha: None},
                        )

            valid_id = uuid4()
            await insert_strict(valid_id, f"strict-v2-{valid_id}")

            invalid_fk_id = uuid4()
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                async with conn.transaction():
                    await insert_strict(
                        invalid_fk_id,
                        f"invalid-fk-{invalid_fk_id}",
                        audit_run_id=uuid4(),
                    )
        finally:
            await conn.execute("reset search_path")
            await conn.execute(f'drop schema if exists "{schema}" cascade')
            await conn.close()

    asyncio.run(scenario())
