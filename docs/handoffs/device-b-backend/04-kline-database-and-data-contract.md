# K 线数据库、导入与数据质量合同

## 1. 推荐数据库选型

继续使用 PostgreSQL 16 + TimescaleDB。原因：

- 当前 SQL、asyncpg 仓储、Timescale chunk 查询已经围绕它实现。
- 需要事务、外键、JSONB、窗口查询和并发任务租约，单纯 Parquet/SQLite 不适合作为在线主库。
- Parquet 适合作为历史导入源和冷备，不是 API 在线真相。

第一阶段使用一个 PostgreSQL 实例即可。K 线 hypertable、Module C 和策略表可以用 schema/tablespace 隔离，不建议一开始拆多个数据库服务，避免跨库事务和运维复杂度。

## 2. 核心表

### symbols

`id, code, exchange, name, asset_type, market, is_active, created_at, updated_at`

唯一键为 `(exchange, code)`。只对 `is_active=true` 的 A 股进行导入和重算；退市标的不继续补采。

### klines

| 字段 | 语义 |
|---|---|
| symbol_id | 标的外键 |
| timeframe | 分钟码：5/15/30/60/1440/10080/43200 |
| ts | 带时区的 bar 结束时间 |
| open/high/low/close_x1000 | 价格乘 1000 后整数 |
| volume | 成交量整数 |
| amount_x100 | 成交额乘 100，可空 |
| is_complete | 该周期是否闭合 |
| revision | 同一逻辑 bar 的修订号 |
| source | 数据源代码 |
| created_at/updated_at | 入库审计时间 |

当前物理主键是 `(symbol_id,timeframe,ts)`，意味着不同 source 不能长期保留同一个时间戳的多份版本；upsert 必须先按来源优先级决定是否覆盖。

## 3. 五级别要求

Module C 正式输入必须有：

- 5f：原生 5 分钟历史。
- 30f：原生 30 分钟历史。
- 1d：原生日线历史。
- 1w：从完整原生日线按自然周离线聚合，或可信原生周线。
- 1m：从完整原生日线按自然月离线聚合，或可信原生月线。

不允许在每次缠论计算时现场从 5f 聚合 30f/日线。`--prepare-native-bars` 仅是旧数据修复兼容开关，正式全量重算应关闭。

## 4. 时间合同

- 所有 Python datetime 必须带时区。
- 存库统一 `timestamptz`，比较统一 UTC。
- 日/周/月 bar 标签统一上海时区 15:00。
- 5f/15f/30f/1h 只接受 A 股交易时段合法 bar_end；代码额外允许 09:30 开盘快照。
- 周/月只使用已闭合周期。当前周、当前月不能进入 confirmed 历史重算。
- API 输出 Unix 秒，不输出毫秒。

## 5. 数据源代码和优先级

| source | code | 默认优先级 |
|---|---:|---:|
| parquet_native | 9 | 9 |
| parquet_5f | 4 | 8 |
| tdx_csv | 3 | 7 |
| pytdx | 2 | 6；超过 parquet 覆盖水位后提升到 10 |
| mootdx | 5 | 5 |
| tencent | 6 | 4 |
| baidu | 7 | 3 |
| derived_5f | 8 | 2 |
| seed | 1 | 1 |

同优先级下再比较 revision 和 complete。B 重建时应优先导入原生历史，再用实时源补历史末端，不要让低优先级聚合数据覆盖原生数据。

## 6. 配套表

- `scheme2_ingest_watermarks`: 每个 symbol/timeframe 的最后数据水位。
- `kline_source_coverage`: 每个来源的起止覆盖范围。
- `historical_backfill_tasks`: 历史补采任务。
- `scheme2_market_fetch_tasks/attempts/candidate_bars`: 实时多源采集任务和审计。
- `kline_audit_runs/checkpoints/quarantine`: 去重、缺失、异常值审计与隔离。
- `chart_period_bars`: 周/月图表读缓存，不能作为 Module C 真相替代。

## 7. 导入顺序

1. 刷新 `symbols`，确认活跃标的集合并冻结本轮 universe 水位。
2. 导入 5f、30f、1d 原生历史，逐文件记录 source coverage。
3. 做逻辑主键去重、OHLC 合法性、时间标签、成交量/成交额审计。
4. 从已验收日线批量生成闭合的 1w/1m。
5. 补采五级别末端缺口；不得静默忽略北交所或供应商空返回。
6. 生成 `scheme2_ingest_watermarks`。
7. 执行全市场水位报告后，才启动 Module C 全量计算。

## 8. 数据质量验收

每个活跃标的/周期至少检查：

- `min(ts), max(ts), count(*)`。
- 逻辑周期无重复。
- `low <= min(open,close) <= max(open,close) <= high`。
- volume/amount 非负，异常突变有来源证据。
- bar_end 合法，午休和非交易时间无伪 bar。
- 30f 每完整交易日应符合实际 session 数，停牌日除外。
- 日线与 5f/30f 的收盘价/高低范围抽样一致。
- 周/月仅闭合周期，末根时间对应周期最后交易日 15:00。

## 9. 容量控制

- 原始压缩包、解压 parquet、Postgres 三份数据不要永久共存。
- 导入成功并完成 checksum/水位后，将原始文件转移到外置盘或删除临时解压副本。
- 不保留多个来源的重复在线物理行；来源审计放 coverage/audit 表。
- Module C run 历史需要 GC 策略：published、生命周期所依赖版本和最近 N 个回滚版本保留，其余异步清理。

## 10. 禁止事项

- 不把 `chart_period_bars` 当作权威 K 线。
- 不在 API 请求链路扫描数万根 5f 临时生成日/周/月。
- 不把 naive datetime 当本地时间猜测。
- 不因供应商 403/空数据就把任务标记成功。
- 不在 K 线审计未通过前启动全量缠论重算。
