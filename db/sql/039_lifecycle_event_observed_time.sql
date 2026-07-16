-- Preserve observation time separately from causal lifecycle event time.

alter table if exists chan_structure_lifecycle_events
    add column if not exists observed_time timestamptz;

update chan_structure_lifecycle_events event
   set observed_time = history.published_at
  from chan_c_head_history history
 where history.id = event.head_history_id
   and event.observed_time is null;

alter table if exists chan_structure_lifecycle_events
    alter column observed_time set not null;

alter table if exists chan_structure_lifecycle_events
    drop constraint if exists ck_chan_structure_lifecycle_event_time_order;

alter table if exists chan_structure_lifecycle_events
    add constraint ck_chan_structure_lifecycle_event_time_order
    check (effective_time <= observed_time);
