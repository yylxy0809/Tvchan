# Device B BJ native 30f provider qualification

## Scope and guardrails

This is the bounded, read-only qualification requested for the existing approved provider set. It made ten endpoint calls only (two providers times five representative BJ symbols), wrote neither PostgreSQL nor the SMB share, and did not download a historical batch.

The provider implementation maps a BJ instrument to `bj{code}` for both HTTP endpoints. `pytdx` and `mootdx` are deliberately excluded: the current project contract states that TDX market encoding has no BJ market and these providers must never receive BJ symbols.

## Representatives

| Symbol | Listing date | Purpose |
| --- | ---: | --- |
| 920010.BJ | 2020-07-27 | early listed instrument |
| 920000.BJ | 2020-12-23 | early listed instrument |
| 920970.BJ | 2022-05-18 | established post-BSE instrument |
| 920002.BJ | 2024-05-30 | recent listing |
| 920003.BJ | 2025-11-07 | newest listing in the sample |

Each call requested at most 100 `30f` bars with a 12-second provider timeout and a 15-second enclosing timeout. Qualification requires a non-empty, strictly timestamp-sorted result that passes the project OHLCV and canonical 30-minute grid checks. The returned time range must also be reported before any provider can be approved for historical replacement.

## Results

Run completed on 2026-07-11 from Device B. No call met the qualification criteria; consequently no returned time-range, nine-label semantic evidence, or historical reach is available.

| Provider | Symbol(s) | Result | Latency / evidence |
| --- | --- | --- | --- |
| Tencent | 920010.BJ | connect timeout | 12,125 ms |
| Tencent | 920000.BJ, 920970.BJ, 920002.BJ, 920003.BJ | empty result | 6,208–7,140 ms |
| Baidu | 920010.BJ | connect timeout | 12,028 ms |
| Baidu | 920000.BJ, 920002.BJ, 920003.BJ | HTTP 403 | 7,737–8,054 ms |
| Baidu | 920970.BJ | response shape incompatible with current parser | `KeyError: 0`, 7,446 ms |

The Tencent endpoint did accept the BJ exchange mapping at transport level for four calls, but an empty K-line payload is not market-data coverage. The Baidu observations are not a licence to alter the parser: a 403 and one incompatible response do not establish a stable, permitted BJ source.

## Provider waterfall decision

| Priority | Provider | Decision for BJ native 30f | Reason |
| ---: | --- | --- | --- |
| 1 | Tencent | **not qualified** | no qualifying bars in four non-timeout samples; one timeout |
| 2 | Baidu | **not qualified** | 403, timeout, and incompatible response; no qualifying bars |
| — | pytdx / mootdx | **prohibited** | no valid BJ market mapping in the project TDX contract |

There is therefore no usable provider waterfall for BJ native 30f today. Do not make a bulk provider request, retry the five samples as a batch, or mark any BJ source coverage as repaired from this evidence.

## 325-symbol exception-manifest recommendation

Create a versioned manifest from all `F:\\data\\stock_30min\\*.BJ.parquet` (325 files), one row per `ts_code`, before any staged import. Its initial disposition for **every** row should be `native_30f_replacement_unqualified`, with these required fields:

```text
symbol,exchange,local_30f_path,local_profile_status,
native_30f_provider_qualified,provider_waterfall,provider_probe_run,
provider_probe_status,replacement_source_id,replacement_range_start,
replacement_range_end,approved_exception_id,disposition,notes
```

Initial values must be `exchange=BJ`, `native_30f_provider_qualified=false`, `provider_waterfall=tencent,baidu`, and `provider_probe_status=unqualified_2026-07-11`. Local row-level findings remain independent: the full profiler found 282 BJ files containing non-native session labels (including minute labels and `15:30`), plus negative-volume/amount records. Those rows require provenance-preserving quarantine even after a replacement-source decision. The remaining BJ files must not be treated as provider-qualified merely because they did not exhibit that particular local anomaly.

The only safe transitions from this initial manifest are: (1) a separately approved replacement provider/source with bounded qualification evidence, (2) an explicitly approved, versioned re-sampling contract, or (3) a documented coverage exception that keeps BJ native 30f out of the staged import and Module C eligibility calculation.
