# ADR-0010: Keep chan.py as the Only Chan Engine

## Status
Proposed

## Context
The user requirement is explicit: higher-level Chan structure must derive recursively from the low-level structure, not from directly aggregated higher-timeframe K-lines.

## Decision
Retain `Vespa314/chan.py` as the only Chan engine and use external projects only for implementation ideas, not as the primary semantic engine.

## Consequences

### Positive
- Preserves the required Chan semantics.
- Avoids mixing incompatible multi-period models.

### Negative
- We do not gain native-code acceleration immediately.
- We must harden our own serialization and snapshot pipeline.

## Alternatives Considered
- `YuYuKunKun/chanlun.py`: rejected as primary engine because its multi-period model is closer to "aggregate high-period bars and analyze per period".
- `YuYuKunKun/chanlun.c99`: rejected as primary engine for the same semantic reason, though its concurrency and performance patterns remain useful references.
