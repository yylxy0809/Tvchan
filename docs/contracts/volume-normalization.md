# Native Parquet volume normalization

Canonical `klines.volume` is always a number of shares. The native source may
label `vol` in either shares (`multiplier = 1`) or hundred-shares
(`multiplier = 100`). Imports must decide this per source row, never with a
global date range or unconditional multiplier.

`collector.volume_normalization.decide_volume_multiplier` first evaluates both
candidates with `amount / (raw_volume * multiplier)`. Exactly one implied
price must be within `[low, high]`, with a 0.1% relative rounding tolerance
and a 0.001 yuan absolute floor. If amount evidence is missing or ambiguous,
an independently complete 5f/30f daily aggregate in canonical shares may
select exactly one candidate. All other outcomes use the explicit
`ambiguous_volume_unit` quarantine path; the decision detail records why the
available evidence was insufficient.

The resulting `VolumeDecision.provenance()` payload preserves raw volume,
selected multiplier, normalized share count, decision basis, implied prices,
and aggregate source reference for the import audit record.
