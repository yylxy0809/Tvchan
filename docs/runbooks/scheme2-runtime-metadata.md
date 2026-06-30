# Scheme 2 Runtime Metadata

This runbook defines the minimal runtime metadata layer for Scheme 2.

These tables are state tables only. They do not change the existing primary data
contracts of:

- `klines`
- `chan_runs`
- `chan_strokes`
- `chan_segments`
- `chan_centers`
- `chan_signals`

The canonical market-data source remains `5f`. Higher-period bars and the
three-level Chan structures are derived from canonical `5f` history.

## Tables

### `scheme2_source_member_checkpoints`

Purpose:

- Track archive/member import progress for parquet or zip based history loads.
- Provide idempotent resume at the member boundary.

Key fields:

- `root_path`
- `source_profile`
- `zip_path`
- `member_path`
- `member_crc32`
- `member_size_bytes`
- `timeframe`
- `status`
- `imported_rows`
- `error_message`
- `started_at`
- `completed_at`

Notes:

- This table tracks source processing state only.
- A successful member import means the member was fully consumed and its rows
  were committed into `klines`; it does not mean Chan was recomputed.

### `scheme2_ingest_watermarks`

Purpose:

- Track how far canonical bar ingestion has been durably committed.

Key fields:

- `symbol_id`
- `timeframe`
- `last_bar_end`
- `source`
- `updated_at`

Notes:

- Scheme 2 runtime should write only canonical `5f` rows here.
- `timeframe` remains in the schema so the table can stay explicit about the
  canonical scope instead of hiding it in code.
- This watermark is an ingest progress marker, not a Chan publish marker.

### `scheme2_chan_published_heads`

Purpose:

- Track the currently published Chan snapshot head per symbol/level/mode.
- Give bundle and websocket readers a stable published snapshot reference.

Key fields:

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `base_from_bar_end`
- `base_to_bar_end`
- `bar_count`
- `snapshot_version`
- `status`
- `published_at`

Notes:

- This table should be updated only after the underlying `chan_*` rows for the
  same `snapshot_version` are fully persisted.
- Readers should treat this table as the publish boundary.
- This table does not replace the detailed `chan_*` data; it points at the head
  that should be served.

### `scheme2_chan_recompute_watermarks`

Purpose:

- Track incremental recompute state per symbol/level/mode.
- Separate "earliest dirty point" from "last fully computed point".

Key fields:

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `dirty_from_bar_end`
- `last_computed_bar_end`
- `updated_at`
- `dirty_reason`
- `last_error`

Notes:

- `dirty_from_bar_end` is the earliest canonical `5f` `bar_end` that must be
  recomputed.
- `last_computed_bar_end` is the latest canonical `5f` `bar_end` that has been
  fully recomputed and written.
- This table is runtime state only; it does not say anything about which
  snapshot is currently published. That boundary stays in
  `scheme2_chan_published_heads`.

## Naming Rule

Scheme 2 runtime metadata uses `bar_end` as the time semantic for canonical
progress and recompute boundaries.

Examples:

- `last_bar_end`
- `base_from_bar_end`
- `base_to_bar_end`
- `dirty_from_bar_end`
- `last_computed_bar_end`

`bar_start` is intentionally not used as the primary runtime checkpoint
semantic.

## Bootstrap Phase

Bootstrap means:

1. import full historical canonical `5f`
2. compute full three-level Chan from that complete `5f` history
3. publish the first stable snapshot head

Recommended flow:

1. Discover parquet or zip members and insert or upsert rows in
   `scheme2_source_member_checkpoints` with `status='pending'`.
2. When a worker starts a member, set `status='running'` and `started_at`.
3. After the member's canonical `5f` rows are committed into `klines`, set:
   - `status='success'`
   - `imported_rows`
   - `completed_at`
4. Update `scheme2_ingest_watermarks` for each affected symbol at timeframe `5`
   with the latest committed `last_bar_end`.
5. After full `5f` history is present for a symbol, compute Chan across the
   full canonical range.
6. Persist the detailed results into existing `chan_runs` and `chan_*` tables.
7. Only after the detailed rows are complete, upsert the current
   `scheme2_chan_published_heads` row with:
   - `snapshot_version`
   - `base_from_bar_end`
   - `base_to_bar_end`
   - `bar_count`
   - `status='published'`
   - `published_at`
8. Mark `scheme2_chan_recompute_watermarks` for that symbol/level/mode as:
   - `dirty_from_bar_end = null`
   - `last_computed_bar_end = base_to_bar_end`

Bootstrap outcome:

- `klines` contains full canonical `5f` history
- `chan_*` contains the full precomputed structures
- `scheme2_chan_published_heads` points to the published head to serve

## Production Runtime Phase

Production runtime means:

- no full-history re-import
- no full-history recompute by default
- only resume from last committed point

Recommended flow:

1. Resume ingestion from source checkpoint state:
   - archive/member pipelines use `scheme2_source_member_checkpoints`
   - live or append-only pipelines resume from `scheme2_ingest_watermarks`
2. Commit new canonical `5f` rows into `klines`.
3. Advance `scheme2_ingest_watermarks.last_bar_end` only after the bar commit is
   durable.
4. If new bars, repaired gaps, or late revisions affect Chan continuity, set or
   move `scheme2_chan_recompute_watermarks.dirty_from_bar_end` backward to the
   earliest impacted canonical `bar_end`.
5. Incremental Chan updater reads the dirty watermark and recomputes forward
   from `dirty_from_bar_end`.
6. After detailed `chan_*` rows for the new snapshot are fully persisted:
   - update `scheme2_chan_recompute_watermarks.last_computed_bar_end`
   - clear `dirty_from_bar_end` when the dirty range is fully covered
   - atomically advance `scheme2_chan_published_heads.snapshot_version` and
     `published_at`

Production runtime outcome:

- ingest resumes from the last committed canonical `5f` boundary
- recompute resumes from the earliest dirty canonical `bar_end`
- serving can stay pinned to the published head until the next head is ready

## Contract Boundary

These metadata tables do not redefine:

- how `klines` rows are stored
- how Chan geometry is serialized in `chan_*`
- how existing bundle or frontend contracts interpret bars and Chan structures

They only answer runtime-state questions such as:

- Which source member is already imported?
- Up to which canonical `5f` `bar_end` is ingestion durable?
- Which `snapshot_version` is currently published?
- From which canonical `bar_end` must incremental Chan recomputation resume?

## Files

- `db/sql/010_scheme2_runtime.sql`
- `docs/runbooks/scheme2-runtime-metadata.md`
