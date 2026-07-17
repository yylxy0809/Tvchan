from pathlib import Path


def test_migration_043_adds_strict_v2_typed_provenance_idempotently() -> None:
    sql = (
        Path(__file__).parents[3]
        / "db"
        / "sql"
        / "043_module_c_strict_v2_eligibility_provenance.sql"
    ).read_text(encoding="utf-8").lower()

    for column in (
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
        assert f"add column if not exists {column}" in sql

    assert "references kline_audit_runs(audit_run_id)" in sql
    assert "references kline_scope_catalog_generations(generation_id)" in sql
    assert sql.count("on delete restrict") == 2
    assert "parameters ->> 'policy'" in sql
    assert "<> 'strict-v2'" in sql
    assert sql.count("~ '^[0-9a-f]{64}$'") == 5
    assert "freshness_contract_version = 'module-c-authoritative-freshness-v1'" in sql
    for sha_column in (
        "audit_evidence_sha256",
        "audit_checkpoint_sha256",
        "freshness_contract_sha256",
        "catalog_manifest_sha256",
        "audit_active_universe_sha256",
    ):
        assert f"{sha_column} is not null" in sql
    assert "catalog_control_revision >= 0" in sql
    assert "from pg_constraint" in sql
    assert "update module_c_eligibility_builds" not in sql
    assert "delete from module_c_eligibility_builds" not in sql
