# Phase 6 Chan Overlay

## What is implemented

- `GET /api/v1/chan/overlay`
- Frontend loads Chan overlay data for every selected chart timeframe
- TradingView registers a local PineJS custom study through
  `custom_indicators_getter`
- TradingView draws:
  - Drawings API `trend_line` objects for strokes
  - Drawings API `trend_line` objects for segments
  - PineJS one-object-per-slot high/low bands for centers
  - PineJS shapes for buy/sell signals
  - Drawings API fallback for centers and signals only when the custom study
    cannot be created

The overlay is always generated from the three analysis levels:

- `5f`
- `30f`
- `1d`

This matches the product rule that the displayed chart timeframe and the Chan
analysis levels are separate concepts.

## Current engine

The API response currently uses one of these engine labels:

- `api-fake-overlay`: API-side placeholder fallback
- `chan-service:placeholder`: placeholder output from the dedicated Chan service
- `chan-service:chan.py`: real `chan.py` adapter output

Today, the local runtime is expected to be on `chan-service:chan.py` when the
bundled startup scripts can find the local `chan.py` checkout.

That means:

- bar loading is real from seed or PostgreSQL
- overlay structure is real and stable
- Chan geometry comes from the real `chan.py` adapter

The placeholder modes remain as a service-availability fallback only.

## Request window semantics

The overlay request `limit` means the number of K-lines loaded for this request.
It is not a cap on strokes, segments, centers, or buy/sell signals.

The preferred path is precomputed database data:

- market-fill writes the latest K-lines
- market-fill reloads all stored bars for each Chan level
- chan-service/`chan.py` calculates a continuous historical Chan chain from the
  earliest stored K-line to the latest stored K-line
- the API returns only the part of that chain that intersects the current
  requested K-line window

This keeps K-lines and Chan objects bound to the same historical timeline. When
the user pans to older K-lines, the frontend requests older bars and receives
the Chan strokes, segments, centers, and buy/sell points that intersect that
older window.

The response includes:

- `requested_bar_count`: requested K-line window size
- `bars_by_level`: actual K-line count used by each analysis level

If no precomputed run covers the requested window, the API can still fall back
to live `chan-service:chan.py` analysis for availability. That fallback is not
the target storage semantics; it is a development/runtime safety path.

## Verify locally

Start API:

```powershell
.\scripts\start-api-db.ps1
```

Check overlay API:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8001/api/v1/chan/overlay?symbol=000001.SZ&timeframe=5f&limit=60" `
  -Headers @{ Authorization = "Bearer dev-local-token" } |
  ConvertTo-Json -Depth 5
```

Expected fields:

- `levels=["5f","30f","1d"]`
- `engine` present and non-empty
- `requested_bar_count=60`
- `bars_by_level` has `5f`, `30f`, and `1d`
- `strokes`, `segments`, `centers`, `signals`

Start frontend:

```powershell
cd apps/web
$env:VITE_API_BASE_URL="http://127.0.0.1:8001"
$env:VITE_API_TOKEN="dev-local-token"
npm run dev -- --host 0.0.0.0 --port 5173
```

Open:

- `http://localhost:5173/`

Expected UI behavior:

- timeframe buttons switch the TradingView chart interval
- status panel shows Chan counts and overlay engine
- status panel shows requested/actual K-line window counts
- chart registers the `Chan Overlay Hybrid` PineJS custom study
- chart draws three-level overlay objects on top of K-lines
- side panel exposes grouped style presets for `5f`, `30f`, and daily strokes,
  segments, centers, and buy/sell signals
- chart container debug attributes show the hybrid renderer:
  - `data-chan-render-phase="rendered"`
  - `data-chan-study-ready="true"`
  - `data-chan-line-renderer="drawings"`
  - `data-chan-center-renderer="pinejs"`
  - `data-chan-pine-strokes="0"`
  - `data-chan-pine-segments="0"`
  - `data-chan-pine-centers` is non-zero when centers exist
  - `data-chan-stroke-drawings` and `data-chan-segment-drawings` are non-zero
    when corresponding objects exist

## Renderer Policy

The preferred renderer is hybrid for now:

- Drawings API is authoritative for strokes and segments because Chan strokes
  and segments are endpoint-to-endpoint geometric objects. TradingView PineJS
  line plots are sampled by bar and can visually bend when used for sparse
  cross-timeframe endpoint geometry.
- PineJS remains authoritative for centers and buy/sell signals, where the
  one-slot-per-object model avoids accidental connections between unrelated
  objects and keeps style controls inside the TradingView indicator panel.

Current split:

- PineJS custom study: registered as `Chan Overlay Hybrid` and loaded on the
  price chart.
- Drawings API renders strokes and segments as `trend_line` drawings.
- PineJS renders centers and buy/sell signals.
- Each PineJS center rail owns a single Chan object so TradingView does not
  connect unrelated centers.
- Completed objects use solid lines. Building or unfinished objects use dashed
  lines. The mode is derived from each object's `confirmed` flag.
- `5f`, `30f`, and `1d` use separate default colors and widths. Center and
  signal plots are exposed in the TradingView indicator Style panel. Stroke and
  segment style defaults are applied when creating their Drawing objects.
- The app-side style panel can tune grouped defaults per analysis level for
  Drawings API objects such as strokes, segments, and fallback centers. PineJS
  center/signal styles are exposed through TradingView's native indicator Style
  panel. The app does not batch-apply PineJS style overrides because this
  Advanced Charts build logs warnings when too many custom study plots are
  overridden programmatically.
- The API does not cap or drop overlay data. Any renderer slot budget applies
  only to the frontend rendering path and produces Drawing fallbacks where
  possible.
- Backend Chan storage is not slot-based. The database stores the full
  continuous Chan chain for each symbol/level/mode over the currently stored
  K-line history. Renderer slots are only an implementation detail for PineJS
  objects such as centers and markers.

Default PineJS slot layout:

- centers: 4 slots per level and completion mode, with separate high/low rails

The slot counts are renderer capacity, not API data limits. The API still
returns every Chan object from the full historical chain that intersects the
requested K-line window.

## Next replacement step

Keep pushing PineJS where it fits without regressing geometry:

- Continue PineJS center rendering by adding better center fill and visibility
  controls.
- Move more grouped style controls into TradingView's native study settings
  when the Advanced Charts API exposes a reliable path for dynamic group-level
  inputs.
- Investigate whether the current Advanced Charts build exposes a PineJS
  graphics/custom-renderer path that can draw true endpoint-to-endpoint lines.
  Only move strokes and segments back from Drawings API when that path can be
  visually verified as straight on `5f`, `30f`, and `1d` chart timeframes.
