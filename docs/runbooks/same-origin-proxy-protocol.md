# Same-Origin Proxy Friendly Protocol

This project now supports a deployment shape where the browser talks only to the
current website origin:

```text
browser -> https://your-domain.example
        -> /api/* and /ws/* on the same origin
        -> reverse proxy or tunnel
        -> NAS backend api:8001
```

## Frontend configuration

For local development, keep the default backend:

```powershell
$env:VITE_API_BASE_URL="http://127.0.0.1:8001"
```

For public website or same-origin reverse proxy mode:

```powershell
$env:VITE_API_BASE_URL="same-origin"
```

Runtime config can do the same without rebuilding:

```html
<script>
  window.__TV_APP_CONFIG__ = {
    apiBaseUrl: "same-origin",
    apiToken: "replace-with-user-token",
    chartDataTransport: "auto"
  };
</script>
```

When `apiBaseUrl` is `same-origin`, the frontend uses:

- HTTP: `/api/v3/chart/bundle` for chart data and `/api/v1/*` for supporting endpoints
- realtime WebSocket: `/ws/v1/realtime?token=...`
- chart request WebSocket: `/ws/v2/chart?token=...` with `get_chart_bundle`

If the page is served over HTTPS, WebSocket URLs automatically use `wss://`.

## Chart bundle HTTP protocol

`GET /api/v3/chart/bundle`

Query parameters:

- `symbol`: `000001.SZ`
- `timeframe`: chart timeframe such as `5f`, `15f`, `1d`
- `from`: optional ISO timestamp
- `to`: optional ISO timestamp
- `limit`: requested bar count
- `levels`: default `5f,30f,1d`
- `modes`: default `confirmed,predictive`

Response:

```json
{
  "schema_version": "chart-bundle.v3",
  "snapshot_id": "...",
  "symbol": "000001.SZ",
  "chart_timeframe": "5f",
  "base_timeframe": "5f",
  "bar_time_semantics": "bar_end",
  "range": { "from": null, "to": null, "limit": 300 },
  "bars": [],
  "chan": { "levels": {} }
}
```

The `bars` payload is the requested chart lens. `chan.levels` always carries the
three analysis levels `5f`, `30f`, and `1d`, all anchored by canonical 5f
`bar_end` timestamps.

## Chart WebSocket protocol

Connect to:

```text
/ws/v2/chart?token=<API_TOKEN>
```

Every request should include a `request_id`.

```json
{"type":"get_chart_bundle","request_id":"bundle_1","symbol":"000001.SZ","timeframe":"5f","limit":300}
```

Responses:

```json
{"type":"chart_bundle","request_id":"bundle_1","bundle":{}}
```

Errors keep the same `request_id`:

```json
{"type":"error","request_id":"bundle_1","error":"..."}
```

## Reverse proxy requirements

The proxy or tunnel must forward:

- `/api/` to the backend API service.
- `/ws/` to the backend API service with WebSocket upgrade headers.
- `Authorization` headers and query strings unchanged.

For split-origin development, configure backend CORS:

```env
CORS_ORIGINS=https://your-domain.example,http://127.0.0.1:5173
CORS_ORIGIN_REGEX=
PUBLIC_BASE_URL=https://your-domain.example
```

For true same-origin production, CORS is usually not involved because browser
requests do not cross origins.
