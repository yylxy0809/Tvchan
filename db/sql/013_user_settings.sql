create table if not exists user_settings (
    owner_token_hash char(64) not null,
    bucket varchar(64) not null,
    value jsonb not null default '{}'::jsonb,
    version integer not null default 1,
    updated_at timestamptz not null default now(),
    primary key (owner_token_hash, bucket),
    constraint chk_user_settings_owner_hash_sha256 check (owner_token_hash ~ '^[0-9a-f]{64}$'),
    constraint chk_user_settings_bucket check (
        bucket in ('theme', 'watchlist', 'layout', 'indicatorSettings')
    ),
    constraint chk_user_settings_version check (version >= 1)
);

comment on table user_settings is
'Per-token frontend user settings. Values are small JSON buckets scoped by the bearer token hash.';

comment on column user_settings.owner_token_hash is
'SHA-256 hash of the bearer token. Static admin/API tokens and database user tokens are isolated by token value.';

comment on column user_settings.bucket is
'Frontend settings bucket. Supported values: theme, watchlist, layout, indicatorSettings.';

create index if not exists idx_user_settings_updated_at
on user_settings (updated_at desc);
