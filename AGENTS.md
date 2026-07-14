# Tvchan Project Instructions

## External project memory

- Use the MCP server `obsidian`, Vault `Obsidian`, for persistent context.
- For significant work, read `AI-Memory/INDEX.md`, `AI-Memory/projects/tradingview-chanlun.md`, and at most three task-specific notes under `Projects/tradingview-chanlun/`.
- Start with `overview.md`, `current-status-and-next-steps.md`, or `source-map.md`; do not scan the whole Vault.
- Repository files and the user's latest instructions override memory. Surface conflicts instead of silently choosing an older note.
- Never store secrets. Propose memory updates first unless the current request explicitly authorizes them.

## Current technical invariants

- The authoritative Chan path is collector-owned Module C using Vespa314 `chan.py` semantics. Do not restore Module B, namespace-B, `CHAN_SERVICE_URL`, or runtime fallback.
- The current computation contract is `native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false`: independently analyze native 5F, 30F, 1D, 1W, and 1M bars. Historical recursive-level plans are context, not the current contract.
- Keep the vendored Chan core unchanged unless the user explicitly authorizes an upstream-core change. Put integration behavior in adapters and verify the effective configuration.
- Preserve immutable run/head publication, stable identities, event-time fields, and no-lookahead behavior. A tail recomputation must publish a complete valid historical prefix.
- Keep the official `weekly_daily_b2_resonance_v1` path separate from diagnostic or relaxed research modes. Diagnostic results must never enter official outputs.

## Working rules

- This repository may have unrelated uncommitted work. Make surgical changes and do not discard or reformat user changes.
- Use the repository's existing tests and documentation as the source of truth. Add a focused regression test for behavior changes and run proportional verification before claiming completion.
- Code-coupled documentation remains in Git. Obsidian stores rationale, evolution, failed approaches, and cross-device handoff context—not copied source code.
