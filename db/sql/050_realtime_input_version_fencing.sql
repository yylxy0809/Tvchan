alter table if exists scheme2_ingest_watermarks
    add column if not exists change_version bigint not null default 0;

alter table if exists scheme2_chan_c_tail_tasks
    add column if not exists target_input_version bigint not null default 0,
    add column if not exists claimed_input_version bigint;

alter table if exists scheme2_chan_c_published_heads
    add column if not exists consumed_input_version bigint not null default 0;

comment on column scheme2_ingest_watermarks.change_version is
'Monotonic canonical-input dirty version. Advances for any changed K-line in the written scope, including earlier bars.';

comment on column scheme2_chan_c_tail_tasks.claimed_input_version is
'Canonical input version frozen when this tail lease is claimed.';

comment on column scheme2_chan_c_published_heads.consumed_input_version is
'Canonical input version exactly consumed by the currently published head.';
