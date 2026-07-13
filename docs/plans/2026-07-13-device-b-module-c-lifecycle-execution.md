# 设备 B 下一阶段：Module C 五级别重算与策略生命周期同步执行任务单

## 1. 执行基线与目标

执行基线固定为 `origin/codex/device-b-lifecycle-foundation@5c5da96`。该提交链已具备：

- Module C eligibility gate；
- 原生时间与 batch provenance；
- production publication contract；
- head/outbox/lifecycle schema；
- durable outbox consumer。

本阶段目标是把这些基础设施投入真实数据执行：先证明 K 线 canonical 与发布合同满足启动条件，再完成全量活跃标的 `5f/30f/1d/1w/1m` Module C 重算，随后构建可恢复的生命周期 current projection 和策略生命周期数据集。

## 2. 不可变边界

1. 不修改 `chan.py` 的 K 线合并、分型、笔、线段、中枢、买卖点算法语义。
2. 笔计算必须保持 `allow_sub_peak=false`，次高点不得成笔。
3. 不恢复 Model B，不新增独立 `chan-service`；生产链路统一使用 Module C。
4. 仅处理 `symbols.is_active=true` 且通过 level-specific eligibility 的标的。
5. 不删除唯一有效 K 线；canonical unresolved 必须排除并保留证据。
6. 当前未闭合周、月不得进入 confirmed 结果。
7. baseline 只能表达“首次观察到当前状态”，不得把结构发生时间冒充历史 `first_seen_time`。
8. 未完成逐 cutoff historical replay 前，生命周期结果不得包装成正式无未来函数回测。
9. Module C 结果继续写入 G 盘 tablespace；G 盘可用空间低于 8 GB 时停止新任务。

## 3. Task B1：分支与数据库合同预检

### 实施

1. 记录代码 SHA、migration 版本、数据库实例、G 盘 tablespace 路径和 active universe 快照。
2. 空库执行 SQL 001 至 034，再执行第二遍验证幂等。
3. 在目标库检查 SQL 030、031、034 已应用，校验 `chan_c_runs.run_kind` 支持 `full_recompute`、`online`、`historical_replay`。
4. 运行 collector、API、strategy-service 全套测试和 `docker compose config --quiet`。
5. 确认只存在一套 Module C full/stream worker 和一套 lifecycle observer 配置。

### 验收

- 所有 migration 首次及幂等执行成功；
- 全套测试通过，无跳过关键时间、发布、outbox、fencing 测试；
- run manifest 能唯一复现代码、数据库、参数、active universe；
- 未发现 Model B 生产入口或双写链路。

## 4. Task B2：五级别 canonical 与 eligibility 启动门

### 实施

1. 对 active universe 执行只读 canonical gate，按 symbol/timeframe 输出：唯一性、OHLC 合法性、负量、session、完整性、首末水位。
2. 5f/30f/1d 必须来自已接受 canonical 数据；1w/1m 仅由完整日线派生，并排除当前周/月。
3. 运行 `module_c_eligibility`，生成版本化 manifest；每个 symbol/level 必须为 `eligible` 或带明确 reason 的 `excluded`。
4. unresolved、缺少本级 K 线、非法 session 不得以空结果进入全量重算。

### 验收

- accepted 集合 `(symbol_id,timeframe,ts)` 逻辑重复为 0；
- accepted 集合 OHLC/负量/session 异常为 0；
- active symbol × 5 levels disposition 覆盖率 100%；
- eligibility 计数可由 canonical 报告独立重算；
- 输出 `kline_canonical_gate.json/md`、`module_c_eligibility.jsonl`、`excluded_summary.json/md`。

### 停止条件

若 failures 或 unresolved 大于 0，只排除受影响 symbol/level 并汇报；不得静默放行，也不得自动删除数据。

## 5. Task B3：20 标的生产合同 canary

### 样本

至少包含主板、创业板、科创板、北交所、停牌稀疏、跳空、涨跌停及长历史样本，共 20 个 eligible 标的。

### 实施

1. 使用 `run_kind=full_recompute`、固定 config hash 和独立 batch ID 运行五级别 confirmed/predictive。
2. 每个 symbol/level/mode 写新 run，校验完成后通过 CAS 切换 published head；禁止半成品发布。
3. head CAS、head history、outbox 必须处于同一事务。
4. 启动 lifecycle observer 消费 canary outbox，重建 current projection。
5. 对 canary 结果与同版本 `chan.py` 直接全量计算做逐点 A/B。

### 验收

- canary failed=0；
- 笔端点、方向、顺序，中枢区间，买卖点逐点 A/B 零差异；
- 已知次高点样本不成笔；
- 每次 published head 都存在唯一 history、outbox 和 lifecycle baseline 证据；
- observer 重跑幂等，current projection 删除后可由事件完整重建。

## 6. Task B4：全量活跃标的五级别重算

### 实施

1. 停止旧 stream worker，确保不存在同 symbol/level 并发发布。
2. 初始使用 4 个静态候选 shard，每进程 `concurrency=1`；数据库任务仍以 claim/lease/fencing 控制所有权。
3. 每个 symbol 独立处理 `5f/30f/1d/1w/1m`，仅处理对应 level 的 eligible 集合。
4. 单进程 RSS 超过 1.2 GB、DB 活跃 Module C 查询超过 4、G 盘低于 8 GB、Postgres I/O 错误或日志 20 分钟无推进时立即停止异常 worker。
5. 每 5 分钟记录 shard 进度、最近 symbol、level、bars/strokes/segments/centers/signals、RSS、DB 活跃查询和失败数。
6. 失败写显式清单并保留旧 published head；不得用空 run 覆盖。

### 验收

- `sum(completed + failed + excluded) = active symbol × 5 levels`；
- eligible symbol/level 的 confirmed/predictive published head 覆盖率 100%；
- failed=0；若不为 0，本阶段不得宣告完成，必须有可恢复清单；
- published head 指向完整 run，不得只含尾部增量对象；
- `bars_by_level`、strokes、segments、centers、signals 计数和 native time 合同有效；
- G tablespace 路径、磁盘增长、WAL 和运行资源写入最终报告。

## 7. Task B5：生命周期同步与恢复证明

### 实施

1. lifecycle observer 以独立进程持续消费数据库 outbox，不使用 Redis 作为真相源。
2. 使用 claim/lease/fencing；worker 崩溃后从数据库水位继续。
3. baseline 事件写 `baseline_observed` provenance，不生成伪历史 first-seen。
4. 对 predictive -> confirmed、disappeared、reappeared 和 head superseded 建 current projection。
5. 保留 outbox lag、消费水位、失败与 dead-letter 可观测指标。

### 验收

- outbox backlog 最终为 0，失败消费为 0；
- 同一 outbox 重放不会重复事件；
- 旧 fencing token 无法覆盖新 current state；
- current projection 与 published heads 逐 symbol/level/mode 对账一致；
- UTC 等价时间只形成一个结构身份，naive datetime 被拒绝。

## 8. Task B6：恢复实时 Module C 与策略生命周期层

### 实施

1. 全量验收后启动 4 路 stream worker：5f 每根闭合 bar 触发；30f/1d 仅本级闭合触发；1w/1m 仅新闭合周/月触发。
2. 实时发布继续使用完整可见 run 或可证明完整的 append-only 版本；禁止 published head 指向仅含尾部对象的 run。
3. 策略层只消费 lifecycle event/current projection，不回读未来 head 补历史。
4. 构建 `official`、`observable`、`diagnostic` 物理分离的数据集。
5. 历史 first-seen 精度不足时只输出 coverage 缺口；正式历史回测等待逐 cutoff historical replay。

### 验收

- 交易时段 K 线采集+落库 p95 < 90 秒；
- Module C 增量计算+发布 p95 < 120 秒；
- outbox 消费 p95 < 30 秒，总链路在 5 分钟窗口内完成；
- 断网、数据库重启、worker 重启后不丢事件、不重复切 head；
- overlay 任意历史窗口返回完整对象，前端切周期不触发现场重算；
- 策略数据集任意 `as_of_time` 查询均不读取未来事件。

## 9. 必交付产物

写入 `outputs/device-b-module-c-lifecycle-20260713/`：

1. `run_manifest.json`
2. `kline_canonical_gate.json`、`kline_canonical_gate.md`
3. `module_c_eligibility.jsonl`、`excluded_summary.json`
4. `canary_ab_report.json`、`canary_ab_report.md`
5. `recompute_progress.jsonl`、`recompute_summary.json/md`
6. `published_head_coverage.json`
7. `lifecycle_reconciliation.json/md`
8. `resource_metrics.jsonl`
9. `failure_samples.jsonl`
10. `next_phase_decision.json`

## 10. 最终 Go/No-Go

只有以下条件同时满足才允许进入正式 historical replay 和策略回测：

- canonical/eligibility 启动门通过；
- canary A/B 零差异；
- 全量 eligible head 覆盖 100%、failed=0；
- outbox backlog=0，lifecycle 对账一致；
- 实时链路满足 5 分钟 SLA；
- official 数据集无 baseline 冒充 first-seen、无未来泄漏。

任一条件不满足时，`next_phase_decision.json` 必须为 `NO_GO`，并列出阻塞证据和最小修复范围。
