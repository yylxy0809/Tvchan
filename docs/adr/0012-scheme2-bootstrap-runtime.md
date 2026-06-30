# ADR-0012: Scheme 2 Bootstrap and Runtime Boundary

## Status
Proposed

## Context
The production goal is to support 5-20 concurrent users dragging charts without making each
request trigger heavy market-data fetches or full-history Chan computation.

The historical source for bootstrap is `D:\5f数据\5m_price`, which contains yearly ZIP files
with daily parquet members. The parquet `trade_time` field is the canonical `5f` bar end time.

## Decision
Use a two-phase operating model:

1. **Bootstrap before deployment**
   - Import all available `5f` bars from the parquet ZIP source.
   - Treat every imported `trade_time` as `bar_end`; do not add five minutes and do not apply
     frontend timezone compensation.
   - Build all higher display bars from canonical `5f` bars.
   - Compute Chan data for `5f`, `30f`, and `1d` from the canonical `5f` history and publish a
     coherent snapshot head for each symbol.

2. **Runtime after deployment**
   - Detect the latest stored `5f` bar end for each symbol.
   - Continue collection only from the missing range.
   - Recompute or incrementally update Chan data only from the dirty range needed to publish a
     new coherent snapshot.
   - Serve frontend requests from the published snapshot and canonical `5f`-derived bars.

Frontend chart requests must never be the primary trigger for full-history Chan computation.

## Hard Rules

- `5f` is the only canonical market-data storage input for production serving.
- `15f`, `30f`, `1h`, `1d`, `1w`, and `1m` chart bars are derived from `5f`.
- `30f` and `1d` Chan analysis levels are recursive analysis levels derived from `5f` structure,
  not independent analysis of stored `30f` or `1d` market bars.
- All persisted and transported K-line/Chan timestamps use `bar_end` semantics.
- Published Chan heads are the read path for API and WebSocket bundle serving.
- The request path may report stale or partial status, but must not silently launch expensive
  full-history rebuilds for an interactive chart request.

## Required Runtime State

The database must track these independent facts:

- parquet source member checkpoints, for resumable import
- per-symbol `5f` ingest watermarks, for gap detection and continuation
- per-symbol Chan dirty ranges, for recompute/update scheduling
- per-symbol published Chan heads, for stable API/WS serving

These metadata tables do not replace the existing `klines` or `chan_*` data tables. They are
coordination state for bootstrap, runtime workers, and deployment gates.

## Consequences

### Positive
- Interactive users read already prepared data, which avoids request-path CPU spikes.
- Dragging the time axis only changes the returned bar window and projected Chan view.
- NAS deployment can recover from restarts by reading watermarks and dirty ranges.
- Historical import can be resumed without deleting completed work.

### Negative
- Bootstrap must finish before the system is considered production-ready.
- We need explicit operational checks before switching the public tunnel to a new backend.
- The first deployment depends on the quality and completeness of the local parquet source.

## Alternatives Considered

- **Request-time full Chan calculation**: rejected because it does not scale to concurrent users
  and causes unpredictable latency.
- **Directly using high-period bars for high-level Chan**: rejected because it violates the
  required recursive low-level-to-high-level Chan semantics.
- **Importing historical high-period files as canonical storage**: rejected for production
  serving; they may be used only as audit or reconciliation inputs.
