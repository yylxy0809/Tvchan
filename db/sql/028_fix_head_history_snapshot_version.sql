-- Preserve published migration 022 and upgrade its legacy numeric snapshot
-- identity to the string contract used by Module C and the API.

alter table if exists scheme2_chan_c_published_head_history
    alter column snapshot_version type varchar(255)
    using snapshot_version::text;
