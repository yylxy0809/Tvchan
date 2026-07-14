# 设备 B 下一阶段：Module C 历史回放与正式策略回测任务单

## 1. 任务目标

设备 B 已完成批次 6 的五级别 Module C 全量重算、fenced lifecycle 发布和生命周期基础设施建设。当前证据为：

- eligible scope 的 published heads 为 `35080/35080`，缺失 `0`；
- lifecycle outbox 已完成 `35080`，阻塞、错配和缺失 history 均为 `0`；
- `module-c:native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false` 配置合同已生效；
- 但 official historical replay heads 为 `0/35080`；
- official strategy manifest 为 `0`，只能得到 diagnostic 数据；
- `next_phase_decision.json` 因 `official_historical_coverage_missing` 判定 `NO_GO`。

本阶段只解决一个核心问题：**以真实 cutoff 历史回放重建可追溯、无未来信息的 Module C 生命周期事件账本，并在此基础上生成正式策略数据集和正式 event replay 回测。**

不得把当前全量 head、当前 head history、结构 `point_time` 或 diagnostic 数据改名后当作历史 `first_seen_time`。

## 2. 不可变边界

1. 不修改 vendored `chan.py` 的 K 线合并、分型、笔、线段、中枢和买卖点语义。
2. 不恢复 Model B、namespace B、`CHAN_SERVICE_URL` 或任何运行时回退。
3. 五级别必须独立使用原生 `5f/30f/1d/1w/1m` K 线；不得由 5f 现场合成高级别输入。
4. 必须保持 `allow_sub_peak=false`，禁止次高点成笔。
5. official `weekly_daily_b2_resonance_v1` 与 sanity/diagnostic/research 模式必须物理和逻辑隔离。
6. 历史回放只能读取 cutoff 时刻已经闭合、已经可见且通过 canonical gate 的 K 线。
7. 每次尾部重算必须发布完整有效历史前缀，不能让 head 指向仅含尾部的增量 run。
8. 不删除唯一有效 K 线，不静默吸收 unresolved canonical 冲突。
9. 不以扩大样本、降低门槛或改变策略合同的方式修复 official 样本不足。
10. 本阶段不是实盘收益优化，不接 API、后台或前端。

## 3. 交付分支与执行顺序

- 基线：`origin/codex/device-b-module-c-lifecycle-execution@5f12465`
- 工作分支：`codex/device-b-historical-replay-official-backtest-20260714`
- 必须按 `H1 -> H2 -> H3 -> H4 -> H5 -> H6 -> H7` 顺序推进。
- H1/H2/H3 未通过前，只允许运行小样本 replay，不允许启动全市场历史回放。
- H5 未通过前，不允许生成 official backtest 结论。

## 4. Task H1：历史回放合同与 cutoff 网格

### 实施

1. 新增版本化 replay contract，至少包含：
   - `contract_version`；
   - `config_hash`；
   - `source_batch_id`；
   - `eligible_universe_snapshot_id`；
   - `canonical_gate_snapshot_id`；
   - `cutoff_time`；
   - `cutoff_policy`；
   - `run_group=historical_replay`；
   - `provenance=historical_replay`；
   - `timezone=UTC`。
2. cutoff 必须按交易所交易日和原生周期闭合时间生成：
   - 5f/30f：只使用该 cutoff 前已闭合的盘中 bar；
   - 1d：只使用已闭合交易日 15:00 bar；
   - 1w：只使用 cutoff 前已闭合周；
   - 1m：只使用 cutoff 前已闭合月。
3. 第一轮使用分层网格，不直接对每根历史 bar 全市场重算：
   - 周线和日线事件窗口必须完整；
   - 30f/5f 仅在正式策略需要的父子窗口内展开；
   - 网格必须由策略合同确定，不能用事后候选结果反向裁剪。
4. 每个 symbol/level/cutoff 生成稳定 replay identity；同一输入重复执行必须得到相同 identity。
5. naive datetime 一律拒绝；等价的 `+00:00` 和 `+08:00` 表示必须归一到同一 UTC 身份。

### 验收

- 给定固定交易日历可重复生成完全一致的 cutoff 清单和 hash；
- 当前未闭合周/月不会进入 confirmed replay；
- 任意 cutoff 查询不会读取 `bar_end > cutoff_time` 的 K 线；
- cutoff 前后边界、午休、节假日、停牌、BJ/SH/SZ 样本均有测试；
- 输出 `outputs/device-b-historical-replay-20260714/replay_contract.json` 和 `cutoff_grid_summary.json/md`。

## 5. Task H2：可恢复的 replay 调度、claim/lease/fencing

### 实施

1. 为 replay batch/task/checkpoint 建立 append-only 状态表或等价持久化模型。
2. task 唯一键至少覆盖 batch、symbol、level、mode、cutoff 和 contract version。
3. worker 使用 claim/lease/fencing token：
   - lease 过期后可被新 worker 接管；
   - 旧 worker 的迟到提交必须被 fencing 拒绝；
   - 重试不能重复发布 lifecycle 事件。
4. 支持 `--resume`，恢复时必须复用原 replay batch id，不得创建替代批次隐藏失败。
5. 提供资源限制：默认 2 路进程、每进程 concurrency=1；单进程 RSS 上限 1.2GB；数据库活跃 replay 查询不超过 2。
6. 所有异常必须写入结构化 failure manifest，不得只写日志。

### 验收

- 宕机、断网、DB 重启、lease 过期和旧 worker 慢写回测试通过；
- 同一 batch 重跑两次，run/head/history/event 数量不增长；
- checkpoint、task status、outbox 和 published head 可相互对账；
- 失败后 resume 从未完成 task 继续，已完成 task 不重算；
- 输出 `replay_batch_manifest.json`、`replay_failures.jsonl` 和 `resource_usage.json`。

## 6. Task H3：20 标的小样本逐 cutoff A/B

### 样本

至少覆盖：主板、创业板、科创板、北交所、停牌稀疏、跳空、涨跌停、长历史、短历史和当前 canonical unresolved 的排除样本。必须包含项目现有固定回归样本。

### 实施

1. 对每个 cutoff 仅加载当时可见的原生 K 线，调用现有 Module C/`chan.py` adapter。
2. 将 replay 输出与同一 cutoff 的直接离线全量计算逐点比较。
3. 比较字段至少包括：
   - 笔端点时间、价格、方向、序号、confirmed/predictive；
   - 线段端点和方向；
   - 中枢起止时间、上下沿；
   - 买卖点类型、位置、确认时间；
   - run config hash、base timeframe 和 source watermark。
4. 验证已知“次高点不得成笔”样本。
5. 验证增量尾部计算发布后，历史窗口仍返回完整 prefix。

### 验收

- 所有 eligible 样本逐点 A/B 零差异；
- 所有排除样本按明确 reason 被拒绝，而不是计算失败；
- 无 `point_time > cutoff_time`、`source_bar_end > cutoff_time` 或未来 confirmed 状态；
- published head 指向完整 run，overlay 历史前缀无断裂；
- 输出 `canary_ab_summary.json/md` 和逐点差异文件；差异非 0 时禁止进入 H4。

## 7. Task H4：全市场历史 replay

### 实施

1. 固定 H1 合同、eligible universe 和 canonical gate 快照后再启动。
2. 先 2 路执行，只有满足以下条件才允许升至 4 路：
   - 单进程 RSS p95 < 900MB；
   - DB active replay queries <= 2 时无 I/O/锁积压；
   - WAL、G 盘空间和 checkpoint 延迟稳定。
3. 只处理 eligible symbol/level/cutoff；排除项必须继承 source snapshot 的 reason。
4. 每个 cutoff 的 run 必须先完成校验，再原子发布 head/history/outbox。
5. worker 不得清理旧 run；GC 在 H7 后单独执行。

### 验收

- expected/completed/failed/excluded 数量可由固定输入重算；
- failures=0，或每个 failure 均在 manifest 中且阻止相关 scope 进入 official；
- head history、outbox、lifecycle event 和 current projection 对账差异为 0；
- 任意抽样 cutoff 可由原始 K 线和 contract 独立重现；
- 输出 `historical_replay_coverage.json/md`、`coverage_by_symbol.jsonl`、`exclusions.csv` 和 `reconciliation.json/md`。

## 8. Task H5：生命周期事件账本和 first_seen 真相

### 实施

1. 使用与在线链路相同的 observer 消费 replay outbox。
2. 生成 `first_seen`、`confirmed`、`disappeared`、`reappeared`，保留 event time、observed time、cutoff、run id、head version 和 provenance。
3. `first_seen_time` 必须来自首次 replay 可见 cutoff，不能来自结构 `point_time`、当前 head 创建时间或脚本运行时间。
4. lifecycle current projection 必须完全由 append-only events 重建。
5. baseline/current full recompute 事件不得混入 historical replay official ledger。

### 验收

- 删除 projection 后可由 events 完整重建，逐字段一致；
- predictive -> confirmed、消失、重现和同 cutoff 幂等测试通过；
- 任意 `as_of_time` 查询不读取未来 run/head/event；
- replay ledger 与 baseline/online ledger provenance 可物理区分；
- official historical heads 不再为 0；若仍为 0，必须输出数据证据并判定 NO_GO。

## 9. Task H6：正式策略数据集与 event replay 回测

### 实施

1. 仅使用 historical replay official ledger 构建 `weekly_daily_b2_resonance_v1` 数据集。
2. official、observable 和 diagnostic 分目录、分 manifest、分计数输出。
3. 运行 strict 全市场扫描和正式 event replay；sanity loose 只能作为诊断对照。
4. 输出 gate waterfall、失败样本、候选样本、至少 3 个完整 trace。
5. trace 必须覆盖周线上下文、日线 setup、30f 确认、5f 二确认、parent binding、价格约束和每层 first_seen/cutoff。
6. 回测必须使用下一可交易时点，计入项目现有手续费、滑点和停牌/涨跌停不可成交规则。
7. 不得因为 official 样本少而放宽合同；样本不足应输出 E/NO_GO 证据。

### 验收

- official manifest 中每一行均可追溯到 replay event/run/head/source bars；
- official 输出不含 baseline、diagnostic、research-only 或未来数据；
- strict waterfall 各层计数单调不增且可由样本文件复算；
- event replay 重跑结果 hash 一致；
- 输出正式 coverage、distribution、waterfall、trace、backtest metrics 和 `next_phase_decision.json`；
- 只有 official coverage 达标时才允许报告正式回测指标，否则明确 NO_GO。

## 10. Task H7：复核、资源报告和交付

### 实施

1. 运行 collector、API、strategy-service 相关测试和 migration 首次/二次执行测试。
2. 对新增查询给出 `EXPLAIN (ANALYZE, BUFFERS)` 前后对比；禁止无证据新增大索引。
3. 输出 replay 总耗时、单 task p50/p95、RSS p95、DB 查询数、WAL 增量和磁盘增量。
4. 对 H1-H6 每项要求生成“已完成/部分完成/未完成”的任务单对照报告。
5. 旧 run GC 只能在引用检查通过后执行，保留回滚窗口；本任务默认不删除有效历史数据。

### 最终验收门

- 所有自动化测试通过，无 xfail 掩盖时间、fencing、幂等或 no-lookahead 合同；
- canary A/B 零差异；
- replay reconciliation 差异为 0；
- official 数据集来源 100% 为 historical replay；
- official/diagnostic 交叉污染为 0；
- 产物可由固定代码、配置、输入快照和 batch id 重现；
- `next_phase_decision` 的计数与所有 JSON/Markdown/JSONL 产物一致；
- 未满足任一硬门时必须 NO_GO，不得包装为正式策略成功。

## 11. 必交付文件

统一放入 `outputs/device-b-historical-replay-20260714/`：

- `replay_contract.json`
- `cutoff_grid_summary.json` / `.md`
- `replay_batch_manifest.json`
- `replay_failures.jsonl`
- `resource_usage.json`
- `canary_ab_summary.json` / `.md`
- `historical_replay_coverage.json` / `.md`
- `coverage_by_symbol.jsonl`
- `exclusions.csv`
- `reconciliation.json` / `.md`
- `lifecycle_ledger_summary.json` / `.md`
- `official_dataset_manifest.json`
- `diagnostic_dataset_manifest.json`
- `gate_waterfall.json` / `.md`
- `fail_samples.jsonl`
- `candidate_samples.jsonl`
- `event_replay_metrics.json` / `.md`
- `trace/`（至少 3 个完整样本）
- `next_phase_decision.json`
- `task_completion_report.md`

## 12. 停止并请求决策的情形

出现以下情况时停止扩大执行范围并提交证据：

1. 必须修改 `chan.py` 语义才能实现 A/B 一致；
2. canonical unresolved 数据会改变 official 样本准入；
3. 需要放宽 `weekly_daily_b2_resonance_v1` 合同才能得到候选；
4. G 盘空间低于 15GB、数据库 I/O 错误或单进程 RSS 超过 1.2GB；
5. 需要删除有效 K 线或不可重建的历史 run；
6. official coverage 仍为 0 或无法达到可解释的最低覆盖；
7. 发现任何未来数据泄漏、旧 worker 覆盖新 head 或不完整 prefix 发布。
