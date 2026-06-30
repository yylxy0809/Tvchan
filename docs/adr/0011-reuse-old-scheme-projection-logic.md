# ADR-0011: Reuse Old-Scheme Projection Logic Instead of Re-Inventing It

## Status
Proposed

## Context
The old scheme already contains a working projection layer for:
- `buildLookupTablesFromWS`
- `buildStrokePointsForView`
- `buildBspMapForView`
- `sortedPivotsForView`
- `lastPointInRange`
- `findPivotOverlap`
- `applyPivotBreak`

## Decision
Port those mechanics into the current TypeScript `chanStudy.ts`, but feed them with the new canonical backend contract instead of the old ad hoc payloads.

## Consequences

### Positive
- Fastest path to a known-good rendering model.
- Limits exploratory frontend debugging.

### Negative
- Requires disciplined porting, not selective copy/paste.
- Existing current-study logic will likely be deleted rather than incrementally patched.
