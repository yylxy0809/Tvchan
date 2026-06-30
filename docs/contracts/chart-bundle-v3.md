# Chart Bundle v3 Contract

## Status

Draft for frontend migration freeze.

## Goal

Freeze one canonical chart bundle contract for the web frontend so that:

- frontend consumes one HTTP bundle shape and one WebSocket bundle shape
- frontend stops mixing legacy `bars` and `chan` interfaces
- chart timeframe is treated as a display lens only
- Chan analysis is always derived from canonical `5f` history

This document is normative for the next migration of:

- `apps/web/src/api/marketData.ts`
- `apps/web/src/api/chartDataManager.ts`
- `apps/web/src/tradingview/datafeed.ts`
- `apps/web/src/tradingview/chanStudy.ts`

## Non-Goals

- This document does not define backend persistence schema details.
- This document does not require immediate endpoint path renaming.
- This document does not force TradingView-specific plot layout.

## Hard Invariants

### 1. Chart timeframe is a lens, not an analysis level

For any requested `chart_timeframe` in:

- `5f`
- `15f`
- `30f`
- `1h`
- `1d`
- `1w`
- `1m`

the response `chan` payload must always contain exactly these three analysis levels:

- `5f`
- `30f`
- `1d`

Frontend must not request or infer alternate Chan levels from chart timeframe.

### 2. Time semantics are unified

All K-line times and all Chan object times use:

- `epoch seconds`
- integer
- semantic meaning: `bar_end`

This applies to:

- `bars[].time`
- `strokes[].start.base_ts`
- `strokes[].end.base_ts`
- `strokes[].begin_base_ts`
- `strokes[].end_base_ts`
- `segments[].start.base_ts`
- `segments[].end.base_ts`
- `centers[].begin_base_ts`
- `centers[].end_base_ts`
- `signals[].base_ts`

If compatibility fields such as `start.time`, `end.time`, `start_time`, `end_time`, or `time` are still present during migration, they must mirror the same `bar_end epoch seconds` meaning. They must never contain chart-timeframe-projected timestamps.

### 3. Only `5f` is canonical storage input

Backend canonical history is the `5f` series only.

- real storage/import target: `5f`
- higher chart bars are generated from `5f`
- `30f` and `1d` Chan analysis is recursively derived from `5f`-anchored structure, not from independently stored `30f` or `1d` market bars

### 4. Buy/sell point visibility is a frontend display rule

Bundle always carries all available signal levels. Frontend filters by chart timeframe:

- when `chart_timeframe < 1d`: show `1d`, `30f`, `5f`
- when `chart_timeframe >= 1d`: show `1d`, `30f`

Backend should not pre-filter signal levels by view timeframe.

### 5. Centers must not render as one continuous rail

Center rails are helper geometry, not the primary visual.

- primary representation: filled area or rect
- default rail visibility: off
- whenever the active center changes, frontend must inject `NaN` breaks so separate centers never connect across empty space

Distinct center IDs and intervals are required so frontend can break continuity deterministically.

### 6. One symbol snapshot must be coherent across all view timeframes

For the same symbol and the same canonical source watermark:

- `snapshot_version` must be identical across `5f/15f/30f/1h/1d/1w/1m` bundle responses
- `bars` differ by `chart_timeframe`
- `chan` semantics do not differ by `chart_timeframe`

This is the key contract that lets frontend cache bars by view timeframe while treating Chan analysis as one symbol snapshot.

## Transport Surface

Frontend v3 consumes only bundle-style interfaces:

- HTTP bundle request
- WebSocket bundle request/subscription

Frontend v3 must stop relying on these legacy split interfaces:

- `/api/v1/bars`
- `/api/v1/chan/overlay`
- WS `get_bars`
- WS `get_chan`
- WS `subscribe_chan`

Compatibility aliases are allowed server-side during rollout. New frontend code
must treat `/api/v3/chart/bundle` as the contract source. `/api/v2/chart/bundle`
may keep the older v2 payload shape only as a fallback for transitional clients.

## HTTP Contract

### Request

Recommended logical endpoint:

`GET /api/v3/chart/bundle`

Compatibility alias during rollout may remain for old clients:

`GET /api/v2/chart/bundle`

### Query Parameters

Required:

- `symbol`
- `timeframe`
- `limit`

Optional:

- `from`
- `to`

Deprecated for frontend v3:

- `levels`
- `modes`

Reason:

- frontend always needs all three analysis levels
- frontend can filter confirmed/predictive locally
- removing per-request analysis knobs avoids cache fragmentation and split semantics

## WebSocket Contract

Recommended logical endpoint:

`/ws/v3/chart`

Compatibility alias during rollout may remain:

`/ws/v2/chart`

### Allowed frontend request types

- `get_chart_bundle`
- `subscribe_chart_bundle`
- `unsubscribe_chart_bundle`

### Forbidden frontend request types

- `get_bars`
- `get_chan`
- `subscribe_chan`
- any flow that reconstructs one screen from separate bar and chan messages

### WebSocket payload rule

WebSocket snapshot payload must embed the same bundle shape as HTTP.

Recommended response types:

- `chart_bundle`
- `chart_bundle_snapshot`

Recommended subscription push payload:

```json
{
  "type": "chart_bundle_snapshot",
  "request_id": "req_123",
  "symbol": "000001.SZ",
  "timeframe": "30f",
  "snapshot_version": "2026-06-24T02:30:00Z#000001.SZ#5f#abc123",
  "bundle": {}
}
```

The `bundle` object must be a full `chart-bundle.v3` payload. Frontend v3 does not depend on separate bar or chan deltas.

## Bundle Payload

### Top-Level Shape

```json
{
  "schema_version": "chart-bundle.v3",
  "snapshot_id": "000001.SZ|30f|1782600000|1783200000|300|sv_abc123",
  "snapshot_version": "sv_abc123",
  "symbol": "000001.SZ",
  "chart_timeframe": "30f",
  "base_timeframe": "5f",
  "bar_time_semantics": "bar_end",
  "range": {
    "from": 1782600000,
    "to": 1783200000,
    "limit": 300
  },
  "analysis_levels": ["5f", "30f", "1d"],
  "bars": [],
  "chan": {},
  "source_watermarks": {},
  "warnings": []
}
```

### Top-Level Fields

| Field | Required | Meaning |
|---|---|---|
| `schema_version` | yes | must be `chart-bundle.v3` |
| `snapshot_id` | yes | request-window-specific identifier; may differ by timeframe/range |
| `snapshot_version` | yes | canonical symbol analysis version; must be stable across view timeframes for same source watermark |
| `symbol` | yes | normalized symbol |
| `chart_timeframe` | yes | requested view timeframe for `bars` |
| `base_timeframe` | yes | currently always `5f` |
| `bar_time_semantics` | yes | currently always `bar_end` |
| `range` | yes | requested bar window metadata |
| `analysis_levels` | yes | fixed ordered list `["5f","30f","1d"]` |
| `bars` | yes | view timeframe K-lines |
| `chan` | yes | three-level Chan snapshot anchored to canonical `5f` bars |
| `source_watermarks` | yes | freshness and derivation watermarks |
| `warnings` | yes | non-fatal caveats for frontend display/debug |

## Bars Contract

### `bars[]`

```json
{
  "time": 1783200000,
  "open": 10.12,
  "high": 10.30,
  "low": 10.05,
  "close": 10.28,
  "volume": 123456,
  "amount": 1250000.0,
  "complete": true,
  "revision": 3
}
```

Rules:

- `time` is the view bar end time in `epoch seconds`
- `complete=false` is allowed only for the trailing still-building bar
- all higher-timeframe bars must be generated from canonical `5f` bars

### Session-aware aggregation requirements

For `15f`, `30f`, and `1h`:

- aggregation must respect China A-share split sessions
- no bucket may bridge the lunch gap
- bar end timestamps must land on the actual session-aware bucket end

For `1d`, `1w`, and `1m`:

- bars are aggregated from `5f`
- timestamps still mean bar end
- week/month alignment must be exchange-calendar aware, not naive 7-day or 30-day rolling buckets

Current backend implementation status:

- `15f`, `30f`, `1h`, and `1d` DB view bars are generated from canonical `5f`
- `1w` and `1m` calendar-aware aggregation is still pending; v3 responses emit `AGGREGATION_FALLBACK` if those view timeframes are requested

## Chan Contract

### Shape

```json
{
  "engine": "database:canonical-5f-recursive",
  "levels": {
    "5f": {
      "bar_count": 3000,
      "strokes": [],
      "segments": [],
      "centers": [],
      "signals": []
    },
    "30f": {
      "bar_count": 500,
      "strokes": [],
      "segments": [],
      "centers": [],
      "signals": []
    },
    "1d": {
      "bar_count": 120,
      "strokes": [],
      "segments": [],
      "centers": [],
      "signals": []
    }
  }
}
```

### Chan-level rules

- all three keys must exist: `5f`, `30f`, `1d`
- missing data is represented by empty arrays, not by omitting a level
- object arrays may contain both confirmed and predictive objects
- objects carry `mode` and `confirmed`

### Stroke / Segment Contract

```json
{
  "id": "000001.SZ:30f:confirmed:stroke:83",
  "level": "30f",
  "mode": "confirmed",
  "confirmed": true,
  "direction": "down",
  "start": {
    "base_ts": 1779847200,
    "base_seq": 547,
    "price": 10.85
  },
  "end": {
    "base_ts": 1779951300,
    "base_seq": 549,
    "price": 10.63
  },
  "begin_base_ts": 1779847200,
  "end_base_ts": 1779951300,
  "begin_base_seq": 547,
  "end_base_seq": 549
}
```

Rules:

- `start.base_ts` and `end.base_ts` are canonical endpoint times
- `base_seq` fields are optional but strongly recommended for debugging and deterministic tie-breaks
- if compatibility fields `start.time` or `end.time` exist, they must equal the canonical base timestamps

### Center Contract

```json
{
  "id": "000001.SZ:5f:confirmed:center:60",
  "level": "5f",
  "mode": "confirmed",
  "confirmed": true,
  "begin_base_ts": 1779674400,
  "end_base_ts": 1779763200,
  "begin_base_seq": 542,
  "end_base_seq": 545,
  "low": 10.71,
  "high": 10.76
}
```

Rules:

- `begin_base_ts` and `end_base_ts` are required canonical interval endpoints
- if compatibility fields `start_time` or `end_time` exist, they must mirror canonical bar-end seconds
- distinct center objects must remain distinct even when they share the same price band

### Signal Contract

```json
{
  "id": "000001.SZ:5f:confirmed:signal:90",
  "level": "5f",
  "mode": "confirmed",
  "confirmed": true,
  "base_ts": 1781746500,
  "base_seq": 703,
  "price": 100.17,
  "signal_type": "2s类买",
  "signal_key": "B2S"
}
```

Rules:

- one payload item represents one signal variant
- backend must not collapse multiple variants into one combined string such as `B2,3B`
- same bar may legitimately carry multiple signal items for the same side
- frontend must preserve those as separate projected marks or separate logical hits

`signal_key` is recommended as a normalized machine-readable field. `signal_type` remains the display label source.

## `analysis_levels`

`analysis_levels` exists so frontend does not need to infer which fixed levels belong to the bundle. It must:

- always exist
- always be ordered from low to high
- currently always equal `["5f","30f","1d"]`

Frontend may use this to drive level toggles, style groups, and profile summaries.

## `source_watermarks`

`source_watermarks` is required for freshness and cross-view consistency checks.

Recommended shape:

```json
{
  "canonical_5f_last_complete_end": 1783200000,
  "canonical_5f_last_seen_end": 1783200300,
  "view_last_complete_end": 1783200000,
  "analysis_generated_at": 1783200020,
  "analysis_source": "precomputed",
  "aggregation_source": "canonical-5f",
  "imported_5f_through": 1783200000
}
```

Required semantics:

- `canonical_5f_last_complete_end`: last complete canonical 5f bar used by analysis
- `canonical_5f_last_seen_end`: newest seen canonical 5f bar, possibly incomplete
- `view_last_complete_end`: last complete bar in the returned `bars` array
- `analysis_generated_at`: server-side generation time
- `aggregation_source`: currently expected to be `canonical-5f`

Frontend uses this for:

- stale badge/debug display
- understanding why the latest view bar may be incomplete
- detecting cross-timeframe mismatch during regression checks

## `warnings`

`warnings` is required and may be empty.

Recommended item shape:

```json
{
  "code": "VIEW_BAR_INCOMPLETE",
  "severity": "info",
  "message": "latest 30f bar is built from an incomplete trailing 5f bucket"
}
```

Recommended warning codes:

- `VIEW_BAR_INCOMPLETE`
- `CANONICAL_GAP_DETECTED`
- `ANALYSIS_PARTIAL`
- `AGGREGATION_FALLBACK`
- `SNAPSHOT_STALE`

Frontend must treat warnings as display/debug metadata only. Warnings must not require frontend to reinterpret geometry.

## Frontend Rendering Semantics

### `marketData.ts`

Must migrate to bundle-only reads.

- quotes may use `bars` from requested view timeframe
- Chan stroke state must come from the bundle `chan.levels`
- intraday classifier may use a `30f` bundle request or the `30f` bars path from a dedicated bundle request, but not legacy split interfaces

Important separation:

- market quote view timeframe may follow UI selection
- fixed Chan intelligence still depends on the canonical three-level analysis contract

### `chartDataManager.ts`

Must become the only chart transport adapter.

- one request in, one bundle out
- no bar/chan split reconstruction
- cache key may include request window and `chart_timeframe`
- `snapshot_version` must be preserved separately as canonical symbol analysis identity

Expected behavior:

- bundles for different view timeframes may have different `snapshot_id`
- bundles for the same symbol snapshot must share `snapshot_version`

### `datafeed.ts`

Must consume only `bundle.bars`.

- TradingView history and realtime refresh use the same bundle path
- no secondary overlay fetch is needed to understand current chart bars
- `chart_timeframe` from bundle is the authoritative bar series timeframe

### `chanStudy.ts`

Must consume only `bundle.chan`.

- `chart_timeframe` is used only for projection
- all endpoint projection starts from canonical `base_ts` / `begin_base_ts` / `end_base_ts`
- signal visibility uses the frontend timeframe rule from this document
- center rail breaks are enforced by projected object boundaries, not by ad hoc visual hacks

### `widget.ts`

Renderer orchestration should treat:

- `bars` as the visible bar sequence
- `chan` as an immutable three-level snapshot

It must not assume that `chan` was calculated at the same timeframe as `bars`.

## Deprecated Fields and Compatibility Rules

During migration, these compatibility fields may still be present:

- `start.time`
- `end.time`
- `start_time`
- `end_time`
- `time`

Rules:

- they may exist only as mirrors of canonical base timestamps
- frontend migration should prefer canonical fields first
- once all frontend consumers switch, these compatibility fields may be removed

Deprecated request knobs:

- `levels`
- `modes`

Deprecated semantic assumption:

- `overlay.chart_timeframe` means the Chan structure timeframe

New semantic rule:

- `chart_timeframe` refers only to the returned `bars` series

## Minimal Example

```json
{
  "schema_version": "chart-bundle.v3",
  "snapshot_id": "000001.SZ|1d|1782600000|1783200000|300|sv_abc123",
  "snapshot_version": "sv_abc123",
  "symbol": "000001.SZ",
  "chart_timeframe": "1d",
  "base_timeframe": "5f",
  "bar_time_semantics": "bar_end",
  "range": {
    "from": 1782600000,
    "to": 1783200000,
    "limit": 300
  },
  "analysis_levels": ["5f", "30f", "1d"],
  "bars": [
    {
      "time": 1783200000,
      "open": 10.12,
      "high": 10.30,
      "low": 10.05,
      "close": 10.28,
      "volume": 123456,
      "amount": 1250000.0,
      "complete": true,
      "revision": 3
    }
  ],
  "chan": {
    "engine": "database:canonical-5f-recursive",
    "levels": {
      "5f": { "bar_count": 3000, "strokes": [], "segments": [], "centers": [], "signals": [] },
      "30f": { "bar_count": 500, "strokes": [], "segments": [], "centers": [], "signals": [] },
      "1d": { "bar_count": 120, "strokes": [], "segments": [], "centers": [], "signals": [] }
    }
  },
  "source_watermarks": {
    "canonical_5f_last_complete_end": 1783200000,
    "canonical_5f_last_seen_end": 1783200300,
    "view_last_complete_end": 1783200000,
    "analysis_generated_at": 1783200020,
    "analysis_source": "precomputed",
    "aggregation_source": "canonical-5f",
    "imported_5f_through": 1783200000
  },
  "warnings": []
}
```

## Migration Acceptance Checklist

This contract is considered implemented on the frontend side only when all of the following are true:

- `marketData.ts` no longer depends on legacy `/bars` or `/chan` split semantics
- `chartDataManager.ts` exposes one bundle-first API to the rest of the app
- `datafeed.ts` uses bundle bars only
- `chanStudy.ts` projects only canonical `5f`-anchored endpoints
- signal display rules follow `<1d => 1d+30f+5f`, `>=1d => 1d+30f`
- center fills are primary and center rail transitions break with `NaN`
- frontend no longer requires `levels` or `modes` query parameters to function
