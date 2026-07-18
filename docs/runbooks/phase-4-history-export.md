# Phase 4 History Export Runbook

## Scope

Phase 4 adds a minimal history export loop for local development:

- `POST /api/v1/history/export` accepts `bars`, optional `metadata`, and optional `chunk_size_bytes`.
- The API reads at most 16 MiB from the request stream, serializes `{request_id, created_at, metadata, bars}` as JSON, gzip-compresses it, stores it in bounded process memory, and returns a manifest.
- `GET /api/v1/history/export/{request_id}/chunks/{index}` returns the gzip bytes for a manifest chunk.

The current cache is process-local memory. Restarting the API clears previously created exports. Exports are private to the authenticated credential and expire after 15 minutes.

## Example

```bash
curl -s -X POST http://localhost:8000/api/v1/history/export \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {"symbol": "000001.SZ", "resolution": "1D"},
    "chunk_size_bytes": 65536,
    "bars": [
      {"time": 1717200000, "open": 10.1, "high": 10.4, "low": 9.9, "close": 10.2, "volume": 120000}
    ]
  }'
```

The response manifest includes `request_id`, `bar_count`, byte sizes, and chunk entries:

```json
{
  "request_id": "4f5c...",
  "format": "json",
  "compression": "gzip",
  "bar_count": 1,
  "chunk_count": 1,
  "chunks": [
    {
      "index": 0,
      "href": "/api/v1/history/export/4f5c.../chunks/0",
      "size_bytes": 154,
      "sha256": "..."
    }
  ]
}
```

Fetch a chunk:

```bash
curl -o chunk-0.json.gz \
  http://localhost:8000/api/v1/history/export/<request_id>/chunks/0
```

## Notes

- Chunks are byte slices of a single gzip stream, so concatenate them in index order before decompressing.
- `metadata` is intentionally open-ended for symbol, resolution, source, requested range, or other export context.
- Requests are limited to 16 MiB and 100,000 bars. Both declared and streamed body bytes are checked, so chunked requests cannot bypass the limit.
- Each export is limited to 16 MiB compressed/uncompressed, 1,024 chunks, and a 1 MiB chunk size. At most two builds run concurrently; each credential may retain four active exports. Process-wide storage is capped at 32 exports and 64 MiB.
- Capacity failures are fail-visible: request/export size returns `413`, a busy or per-credential quota returns `429`, and process storage exhaustion returns `507`. Unknown, expired, or foreign chunks all return `404`.
- This is a minimal closed loop, not durable storage. A later phase can replace the in-memory store with temp files, object storage, or database-backed export jobs without changing the manifest contract.
