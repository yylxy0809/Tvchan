create table if not exists runtime_config (
    key text primary key,
    value jsonb not null default '{}'::jsonb,
    version integer not null default 1,
    updated_at timestamptz not null default now(),
    constraint chk_runtime_config_version check (version >= 1)
);

comment on table runtime_config is
'Minimal runtime configuration store for API-served feature flags and other small JSON settings.';

comment on column runtime_config.value is
'JSON configuration payload. For frontend feature configuration, use key frontend.features.';

insert into runtime_config (key, value)
values ('frontend.features', '{}'::jsonb)
on conflict (key) do nothing;
