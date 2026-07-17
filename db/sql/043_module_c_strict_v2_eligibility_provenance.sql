-- Strong, legacy-compatible provenance for strict-v2 Module C eligibility.
-- Existing builds remain valid because the new columns are nullable. A build
-- declaring parameters.policy=strict-v2 must provide the complete evidence set.

begin;

alter table module_c_eligibility_builds
    add column if not exists canonical_audit_run_id uuid,
    add column if not exists audit_evidence_sha256 varchar(64),
    add column if not exists audit_checkpoint_sha256 varchar(64),
    add column if not exists freshness_contract_version text,
    add column if not exists freshness_contract_sha256 varchar(64),
    add column if not exists catalog_generation_id uuid,
    add column if not exists catalog_control_revision bigint,
    add column if not exists catalog_manifest_sha256 varchar(64),
    add column if not exists audit_active_universe_sha256 varchar(64);

do $$
begin
    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'module_c_eligibility_builds'::regclass
           and conname = 'fk_module_c_eligibility_build_audit'
    ) then
        alter table module_c_eligibility_builds
            add constraint fk_module_c_eligibility_build_audit
            foreign key (canonical_audit_run_id)
            references kline_audit_runs(audit_run_id)
            on delete restrict;
    end if;

    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'module_c_eligibility_builds'::regclass
           and conname = 'fk_module_c_eligibility_build_catalog_generation'
    ) then
        alter table module_c_eligibility_builds
            add constraint fk_module_c_eligibility_build_catalog_generation
            foreign key (catalog_generation_id)
            references kline_scope_catalog_generations(generation_id)
            on delete restrict;
    end if;

    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'module_c_eligibility_builds'::regclass
           and conname = 'ck_module_c_eligibility_strict_v2_provenance'
    ) then
        alter table module_c_eligibility_builds
            add constraint ck_module_c_eligibility_strict_v2_provenance
            check (
                coalesce(parameters ->> 'policy', '') <> 'strict-v2'
                or (
                    canonical_audit_run_id is not null
                    and audit_evidence_sha256 is not null
                    and audit_evidence_sha256 ~ '^[0-9a-f]{64}$'
                    and audit_checkpoint_sha256 is not null
                    and audit_checkpoint_sha256 ~ '^[0-9a-f]{64}$'
                    and freshness_contract_version = 'module-c-authoritative-freshness-v1'
                    and freshness_contract_sha256 is not null
                    and freshness_contract_sha256 ~ '^[0-9a-f]{64}$'
                    and catalog_generation_id is not null
                    and catalog_control_revision is not null
                    and catalog_control_revision >= 0
                    and catalog_manifest_sha256 is not null
                    and catalog_manifest_sha256 ~ '^[0-9a-f]{64}$'
                    and audit_active_universe_sha256 is not null
                    and audit_active_universe_sha256 ~ '^[0-9a-f]{64}$'
                )
            );
    end if;
end
$$;

commit;
