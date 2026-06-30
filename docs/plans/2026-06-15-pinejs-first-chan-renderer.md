# PineJS-first Chan Renderer Plan

Date: 2026-06-15

## Decision

Prefer TradingView Charting Library custom studies through
`custom_indicators_getter(PineJS)` for Chan overlays where the object can be
represented as bar-indexed plots or shapes.

Keep Drawings API as the fallback renderer for objects that need arbitrary
two-point geometry, such as multi-level strokes, segments, and center rectangles.

## Why not raw PineScript

The local Advanced Charts package exposes PineJS custom indicators, not a local
runtime that accepts arbitrary TradingView PineScript text. The closest local
equivalent is a JavaScript custom study with:

- `metainfo`
- a PineJS constructor
- `main()` returning plot values for each bar

## Rendering split

PineJS custom study first:

- buy/sell signal markers
- per-level state plots
- center high/low bands when they can be represented as series
- confirmed/predictive state as separate plots

Drawings API fallback:

- stroke line from arbitrary start point to end point
- segment line from arbitrary start point to end point
- center rectangle spanning time and price
- any object that is easier to manage by explicit entity id

## Implementation path

1. Keep `/api/v1/chan/overlay` as the backend contract.
2. Add a frontend renderer abstraction:
   `pinejs-study`, `drawings`, `hybrid`.
3. Implement a PineJS custom study that consumes per-bar Chan series data.
4. Use Drawings API only for geometry that PineJS cannot express cleanly.
5. Keep the current Drawings implementation available until the PineJS study
   matches visual parity.

## Validation standard

- Switching chart timeframe still shows 5f/30f/1d Chan levels.
- Confirmed and predictive states remain visually distinct.
- Study rendering does not block chart pan, zoom, symbol switch, or realtime bars.
- Drawings fallback can be disabled without losing supported PineJS objects.
