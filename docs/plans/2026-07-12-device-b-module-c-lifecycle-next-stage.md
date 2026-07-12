# 设备 B 下一阶段：K 线真相收口、Module C 重算与策略生命周期建设任务单

## 1. 当前判断

基准提交为 `origin/master@91073a8`。设备 B 最新远端提交为
`origin/codex/device-b-daily-bulk-import@e38dbc7`，其提交链已经完成：

- 全量 5f/30f/1d Parquet 只读画像；
- 可恢复、静态分片的本地 Parquet 导入器；
- quarantine、deadlock 恢复和 import run finalization；
- 周线 bootstrap 聚合性能优化。

这些提交尚未提供“目标数据库已完成导入并通过复验”的持久证据。因此：

- **不得立即启动正式 Module C 全量重算**；
- **可以立即并行建设生命周期基础设施**；
- 生命周期 head history/outbox 必须先于首次正式 Module C 发布上线，否则首次全量结果只能事后补成不可靠历史。

## 2. 不可变边界

1. 不修改 `chan.py` 的 K 线合并、分型、笔、线段、中枢和买卖点语义。
2. 不恢复 Model B 或独立 `chan-service`。
3. 不把 30f 合成为 5f，不把异常 BJ 30f 强行归一化。
4. 不删除唯一有效 K 线；未决冲突必须 quarantine 并保留来源证据。
5. 当前周/月未闭合周期不得进入 confirmed 历史。
6. 首次全量重算只产生 `baseline_observed`，不得把历史 `point_time` 冒充 `first_seen_time`。
7. online、full-recompute、historical-replay 必须使用不同 run group/profile。

## 3. 工作流与依赖关系

```text
设备 B 导入分支合并
  -> K 线导入/复验/eligible universe
  -> 1w/1m 派生与复验
  -> Module C 正式全量重算
  -> baseline 生命周期消费
  -> 实时增量链路
  -> 策略数据集与正式回测

生命周期 schema + head outbox + observer
  ------------------------------------^  必须在 Module C 正式发布前完成
```

## 4. Task B1：设备 B 导入分支收口

### 实施

1. 将 `codex/device-b-daily-bulk-import` 建 PR 合入 `master`，不得 squash 掉需要追踪的导入修复证据。
2. 空库执行 SQL 001 至 029，再执行第二遍验证幂等。
3. 运行 collector、API、strategy-service 全套测试及 `docker compose config --quiet`。
4. 将导入镜像、源文件根目录、active universe 快照和 config hash 写入 run manifest。

### 验收

- 分支 HEAD 包含 `e38dbc7`；
- migrations 首次和二次执行均成功；
- 所有测试通过，无 xfail 掩盖导入/时间合同；
- manifest 可唯一复现源、代码、参数和 active universe。

## 5. Task B2：K 线数据库真相收口

### 实施

1. 使用固定 `import_run_id` 和静态 shard 导入 active-only 的 5f/30f/1d。
2. 恢复 unfinished shard 时必须复用原 run ID，不创建替代 run 隐藏失败。
3. quarantine：
   - 5f 的 4,310 条 OHLC 异常；
   - SH/SZ 30f 的 4,140 条 OHLC 异常；
   - BJ 30f 非法 session、负 volume/amount 和未认证 provider 数据。
4. 日线 volume 按 symbol/day 与 30f 和量比对：`*100`、原值或 unresolved 三分流。
5. 对缺失 native 5f 的 796 个 active symbol 和 BJ 30f 建版本化 exception manifest。
6. finalization 后运行全库 canonical dry-run，不在该轮自动 apply 新规则。

### 验收

- 每个 import run 状态与 checkpoint 数一致，无永久 `running`；
- `(symbol_id,timeframe,ts)` 逻辑重复为 0；
- 非法 session、OHLC、负量异常在 accepted 集合中为 0；
- quarantine 行均有 source path、raw key、reason 和 run ID；
- unresolved 不为 0 时明确阻止相应 symbol/timeframe 进入 Module C eligible universe；
- 输出 `kline_truth_summary.json/md`、`coverage_by_symbol.jsonl`、`exceptions.csv`。

## 6. Task B3：周线/月线派生和五级别 eligibility

### 实施

1. 仅从已接受且 `is_complete=true` 的日线派生 1w/1m。
2. 排除当前周、当前月；时间戳使用桶内最后一根合格日线的 15:00 bar end。
3. 建立 `module_c_eligible_universe` 物化结果或等价版本化 manifest，逐 symbol/level 给出 eligible、excluded、reason。
4. 回填并复验 `chart_period_bars`。

### 验收

- 1w/1m 无当前未闭合周期；
- OHLCV 聚合与日线逐桶抽样一致；
- 每个 active symbol 的 5f/30f/1d/1w/1m 均有明确 disposition；
- eligible 计数可由 coverage 和 exception manifest 重算得到；
- 不以“全市场 active 总数”替代“各 level eligible 总数”。

## 7. Task B4：生命周期基础设施（与 B2/B3 并行）

### 实施

1. 新增 append-only `chan_c_head_history`，`snapshot_version` 使用字符串合同。
2. head CAS 与 `chan_c_head_outbox` 插入同一数据库事务。
3. 新增稳定结构 fingerprint，身份至少包含 symbol、level、结构类型、方向/买卖点、结构时间、整数价格、config hash、identity version。
4. 新增 `chan_structure_lifecycle_events` 和可重建 current projection。
5. observer 使用 claim/lease/fencing token，保存消费水位，支持 outbox 重放。
6. baseline、online、historical replay 写入不同 provenance/run group。

### 验收

- 同一瞬间 `+00:00` 与 `+08:00` 只生成一个身份，naive datetime 被拒绝；
- head 发布成功而 outbox 缺失的情况不可构造；
- predictive -> confirmed、disappeared、reappeared 状态测试通过；
- worker 崩溃重试不重复写事件；
- current projection 删除后可由 events 完整重建并逐字段一致；
- baseline 只写 `baseline_observed`。

## 8. Task B5：Module C 五级别正式全量重算

### 启动门

只有 B2、B3、B4 全部通过后启动。先对 20 个跨市场样本执行 dry-run，确认 head/outbox/lifecycle 原子链路。

### 实施

1. 固定 config hash：`native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false`。
2. 明确 `allow_sub_peak=false`，禁止次高点成笔。
3. 停止 stream worker；初始 4 个进程、每进程 concurrency=1。
4. 监控单进程 RSS、DB active query、WAL、目标盘空间，再决定升至 6-8 路。
5. 每个 symbol/level/mode 独立完成 run、校验、head CAS 和 outbox。
6. 失败必须写清单，不得发布半成品 head。

### 验收

- eligible universe 的五级别 × confirmed/predictive head 覆盖率 100%；
- excluded 数量和原因与 B3 manifest 完全一致；
- config hash、base timeframe、run status、bar end 正确；
- 已知次高点样本不成笔；
- 固定样本与同版本 `chan.py` 直接全量计算逐点 A/B 零差异；
- 每次 head 切换均存在唯一 history/outbox/lifecycle baseline 证据。

## 9. Task B6：实时 Module C 与策略生命周期同步

### 实施

1. 启动 4 路 symbol shard stream worker。
2. 5f 每个闭合周期触发；30f/1d 仅本级闭合触发；1w/1m 仅新闭合周/月触发。
3. lifecycle observer 消费 outbox，生成 first_seen/confirmed/disappeared/reappeared。
4. Redis 只作通知和缓存，数据库 outbox 为恢复真相。
5. GC 旧 run 前检查 head history、lifecycle、strategy replay 引用。

### 验收

- K 线采集+落库 p95 < 90 秒；
- Module C 增量计算+发布 p95 < 120 秒；
- 总链路在 5 分钟窗口内完成；
- lease 过期 worker 不能覆盖新 head；
- 断网/重启后按数据库水位补消费，不丢生命周期事件；
- 历史窗口在增量发布后仍返回完整 overlay。

## 10. Task B7：策略生命周期数据集

### 实施

1. baseline 结果只供当前观察和诊断，不包装为历史 first_seen。
2. 正式历史回测使用独立 historical replay，逐 cutoff 运行 Module C 并由同一 observer 生成事件。
3. 重跑 coverage、weekly distribution、strict/sanity waterfall。
4. official、observable、diagnostic 物理分离，只有 official eligible 进入正式 event replay。

### 验收

- point/first_seen/confirm/disappear/provenance 可追溯；
- 任意 `as_of_time` 查询不读取未来 head/run/event；
- official trace 不含 diagnostic/research-only 数据；
- 样本不足时输出覆盖缺口，不放宽合同冒充正式收益。

## 11. 本地 A 设备统一行情侧栏代码的提交与合并方案

### 决策

本地代码应提交，但**不能把当前 dirty worktree 一次性提交到 master**。统一行情侧栏涉及后端，要求设备 B 重新实现会造成重复实现和合同漂移。正确做法是从 `origin/master` 建独立分支，按边界拆提交，再在设备 B 导入分支合并后 cherry-pick/rebase。

### 建议提交序列

1. `feat(web): add unified market sidebar UI and contracts`
   - 仅 `apps/web` 的侧栏、主题、新闻、今日最强和 market sidebar client/store；
   - 不混入 Module C overlay 或其它未关联前端改造。
2. `feat(api): add market sidebar snapshot and websocket aggregation`
   - `services/api/app/market_sidebar`、必要的 route/config/security/db 接线及测试。
3. `feat(collector): add local/westock/iwencai market provider`
   - `services/collector/collector/market_data*`、provider runtime、固定 fixtures/tests、WeStock bridge。
4. `chore(deploy): wire market-data-provider`
   - Dockerfile、compose、env example、Windows override；此提交在设备 B 分支上人工重放。

### 冲突判断

- 与设备 B 最新分支直接重叠的已知文件只有：
  - `deploy/backend.env.example`
  - `deploy/docker-compose.backend.yml`
- API market sidebar 和 collector market-data provider 主体与设备 B import 代码路径不同，可并存。
- `Dockerfile.collector` 虽不在当前 Git diff 交集内，仍应在设备 B Linux 镜像上重新跑安全与依赖测试。

### 合并验收

- 先合设备 B import 分支，再重放四个小提交；
- compose 冲突必须手工合并，不接受 `ours/theirs` 整文件覆盖；
- API、collector、strategy、frontend 全套测试通过；
- 空库 migration、provider 容器、导入 worker、Module C worker 可同时通过 compose config；
- market provider 不得改变 K 线 canonical 写入和 Module C 算法语义。

## 12. 需要用户决策才停止的边界

- BJ 30f 是继续排除，还是采购/批准新的历史源；
- 缺失 native 5f 的 796 个 active symbol 是否允许以 coverage exception 上线；
- 日线 volume unresolved 的容忍政策；
- 存储空间不足需要删除原始数据或缩短历史；
- 改变 `chan.py`、周日共振策略合同或 official 准入阈值。
