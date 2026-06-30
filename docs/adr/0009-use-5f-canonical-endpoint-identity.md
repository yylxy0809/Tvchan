# ADR-0009: Use 5f Canonical Endpoint Identity

## Status
Proposed

## Context
The current overlay path mixes display-timeframe timestamps with analysis-level timestamps. That creates endpoint drift, missing or ambiguous endpoint placement, and incorrect structure continuity on `30f` and `1d` charts.

## Decision
Anchor every Chan object endpoint to the `5f` base series and treat chart timeframe as a projection lens only.

## Consequences

### Positive
- One unambiguous source of truth for endpoint placement.
- `30f` and `1d` structures can be plotted on any chart timeframe without re-running Chan logic in the browser.
- Regression testing becomes data-driven.

### Negative
- Backend adapter and persistence schema must change.
- Frontend study becomes projection-heavy and must maintain more lookup tables.

## Alternatives Considered
- Keep per-timeframe overlay timestamps: rejected because this is the current failure mode.
- Compute separate overlays per chart timeframe: rejected because it breaks the "same Chan, different lens" requirement.
