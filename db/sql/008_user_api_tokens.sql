create table if not exists user_api_tokens (
    id integer generated always as identity primary key,
    token_hash char(64) not null,
    label varchar(128) not null,
    display_name varchar(128),
    role varchar(16) not null default 'user',
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    disabled_at timestamptz,
    last_used_at timestamptz,
    constraint chk_user_api_tokens_role check (role in ('user')),
    constraint chk_user_api_tokens_hash_sha256 check (token_hash ~ '^[0-9a-f]{64}$')
);

create unique index if not exists uq_user_api_tokens_token_hash
on user_api_tokens (token_hash);

create index if not exists idx_user_api_tokens_active_created
on user_api_tokens (is_active, created_at desc);
