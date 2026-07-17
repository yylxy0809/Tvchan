-- Fence K-line scope catalog creation and activation to one control revision.
-- Legacy building rows are an operator-visible error and are never rewritten.

begin;

do $$
declare
    has_base_revision boolean;
    has_legacy_building boolean;
begin
    select exists (
        select 1
          from information_schema.columns
         where table_schema = current_schema()
           and table_name = 'kline_scope_catalog_generations'
           and column_name = 'base_control_revision'
    ) into has_base_revision;

    if has_base_revision then
        execute $query$
            select exists (
                select 1
                  from kline_scope_catalog_generations
                 where status = 'building'
                   and base_control_revision is null
            )
        $query$ into has_legacy_building;
    else
        select exists (
            select 1
              from kline_scope_catalog_generations
             where status = 'building'
        ) into has_legacy_building;
    end if;

    if has_legacy_building then
        raise exception using
            errcode = '55000',
            message = 'migration 041 refuses legacy building kline scope catalog generations';
    end if;
end
$$;

alter table kline_scope_catalog_control
    add column if not exists revision bigint not null default 0;

alter table kline_scope_catalog_generations
    add column if not exists base_active_generation_id uuid,
    add column if not exists base_control_revision bigint;

do $$
begin
    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'kline_scope_catalog_generations'::regclass
           and conname = 'fk_kline_scope_catalog_generation_base_active'
    ) then
        alter table kline_scope_catalog_generations
            add constraint fk_kline_scope_catalog_generation_base_active
            foreign key (base_active_generation_id)
            references kline_scope_catalog_generations(generation_id)
            on delete restrict;
    end if;
    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'kline_scope_catalog_generations'::regclass
           and conname = 'ck_kline_scope_catalog_building_has_base_revision'
    ) then
        alter table kline_scope_catalog_generations
            add constraint ck_kline_scope_catalog_building_has_base_revision
            check (status <> 'building' or base_control_revision is not null);
    end if;
end
$$;

create unique index if not exists uq_kline_scope_catalog_one_building_generation
    on kline_scope_catalog_generations (status)
    where status = 'building';

create or replace function enforce_kline_scope_catalog_control_revision()
returns trigger
language plpgsql
as $$
begin
    if new.active_generation_id is distinct from old.active_generation_id then
        if new.revision <> old.revision + 1 then
            raise exception using
                errcode = '55000',
                message = 'kline scope catalog pointer changes require revision + 1';
        end if;
    elsif new.revision <> old.revision
          and new.revision <> old.revision + 1 then
        raise exception using
            errcode = '55000',
            message = 'kline scope catalog revision must stay unchanged or increment by 1';
    end if;
    return new;
end
$$;

revoke all on function enforce_kline_scope_catalog_control_revision() from public;

drop trigger if exists trg_kline_scope_catalog_control_revision
    on kline_scope_catalog_control;

create trigger trg_kline_scope_catalog_control_revision
before update of active_generation_id, revision
on kline_scope_catalog_control
for each row
execute function enforce_kline_scope_catalog_control_revision();

commit;
