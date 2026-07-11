# 设备 B 启动、重建与后续开发任务单

## 阶段 0：冻结交接代码

### 操作

1. 等 A 当前 K 线重复/缺失、通信和前端加载工作完成。
2. 在 A 把 backend/protocol/sql/deploy/strategy 保存为明确 Git 提交。
3. 在 B 获取该提交，核对 commit hash。
4. 禁止 B 从旧基准提交手工补文件。

### 验收

- `git status` 只包含 B 自己明确产生的改动。
- `services/chan-service` 不存在。
- Module C adapter 位于 collector。
- SQL 014-027、strategy-service 均存在。

## 阶段 1：修复迁移合同并搭环境

### 必须先修

1. 将迁移 022 的 `snapshot_version bigint` 与当前字符串 snapshot 合同统一。
2. 确认迁移 027 删除的仅为 Model B 表。
3. 将 `timescaledb:latest-pg16` 锁定具体版本。
4. 给全量 Module C 重算增加 Compose batch profile。
5. 决定 `mootdx` 是正式依赖还是删除未启用 provider。

### 验收

- 空数据库从 001 到 027 一次执行成功。
- 第二次执行幂等成功。
- `docker compose config --quiet` 通过。
- API/collector/strategy 全套测试通过。

## 阶段 2：重建 K 线数据库

### 操作

1. 刷新全市场 symbol master，只保留活跃 A 股。
2. 下载/导入原生 5f、30f、1d。
3. 由已验收日线批量生成已闭合 1w、1m。
4. 多源补采历史末端。
5. 执行 canonical audit、重复/缺口/异常成交量审计。
6. 生成 source coverage 和 ingest watermarks。
7. 回填 `chart_period_bars`。

### 验收

- 全部目标活跃标的具有五级别水位；例外有明确清单和原因。
- 无逻辑重复 K 线。
- 2026-06-25 后成交量异常问题完成全市场核验。
- 周/月不包含当前未闭合周期。
- 随机抽样原生周期与供应商原始文件一致。

## 阶段 3：生命周期基础设施

### 操作

1. 修复 head history schema。
2. 给 published head 切换增加可靠历史写入或 outbox。
3. 定义结构 fingerprint 版本。
4. 建立 lifecycle ledger 的唯一键、状态转换和幂等规则。
5. baseline 事件明确标记 `baseline_observed`，不伪造历史 first_seen。

### 验收

- 同一时刻不同 UTC offset 归并同一身份。
- naive datetime 拒绝。
- predictive -> confirmed 产生 confirm_time。
- 消失和重新出现可区分。
- worker 崩溃重试不生成重复事件。

## 阶段 4：Module C 五级别全量重算

### 操作

1. 锁定 config_hash：`native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false`。
2. 关闭 stream worker。
3. 4 进程静态 shard 起跑，每进程 concurrency=1。
4. 按 CPU/内存/WAL 调整到 6-8 路；单进程内存设保护线。
5. 每个 symbol 五级别成功后写 published heads。
6. 失败标的落清单，不静默跳过。

### 验收

- 全部合格活跃标的五级别 × 两模式 head 完整。
- config_hash/base_timeframe/run status 正确。
- G/目标盘保留安全空间。
- 已知“次高成笔”样本符合新配置。
- 与 chan.py 直接计算 A/B 零差异。

## 阶段 5：实时链路

### 操作

1. 启动行情补采。
2. 启动 4 路 chan-c-stream symbol shard。
3. 5f 每 5 分钟更新；30f/1d 在本周期闭合后触发；1w/1m 仅新闭合周期触发。
4. 发布 Redis head update。
5. 后台 GC 旧 run，只删不再被 head/生命周期/回滚引用的版本。

### 验收

- 全市场采集+落库 <90s。
- Module C 增量计算+发布 <120s。
- 总流程在 5 分钟窗口内完成。
- worker lease 过期、旧 worker 慢写不能覆盖新 head。
- 任意历史窗口在增量发布后仍完整。

## 阶段 6：A/B 前后端联调

### 操作

1. B API 监听局域网地址，DB/Redis 不暴露。
2. A 前端配置 B `apiBaseUrl` 和普通 token。
3. 运行固定样本验收。
4. 采集 API p50/p95、SQL EXPLAIN 和前端加载时间。

### 验收

- K 线 <500ms，overlay <2s。
- 切周期先显示 K 线，不被 overlay 阻塞。
- 拖拽只补缺失窗口。
- 高级别端点精确落在低周期目标价 K 线，同价取最后一根。
- 标的信息与最新行情水位一致。

## 阶段 7：策略数据集与回测

### 操作

1. 从新 config 的 Module C run/head history 构建 lifecycle ledger。
2. 重跑 coverage、weekly distribution、strict/sanity waterfall。
3. 只有 official eligible 样本进入正式 event replay。
4. 数据覆盖仍不足时，继续扩大可回放区间，而不是放宽合同冒充正式结果。

### 验收

- official/observable/diagnostic 三类计数完全分离。
- point/first_seen/confirm/disappear 可追溯。
- 正式 trace 不含 diagnostic 信号。
- 有足够样本后才报告策略收益与风险。

## B 端 Codex 首轮指令模板

```text
先阅读 docs/handoffs/device-b-backend/00-README.md 及 01-08 全部文档。
不要修改 chan.py 算法语义，不恢复 Model B 或独立 chan-service。
先核对交接 commit、修复 migration 022 的 snapshot_version 合同，并在空库验证 001-027 幂等迁移。
随后按 08 的阶段 1-2 建立五级别 K 线真相和数据质量报告；数据审计通过前不要启动 Module C 全量重算。
所有失败标的必须落清单，禁止静默跳过；所有时间必须 timezone-aware UTC/Asia-Shanghai bar_end 合同一致。
```

## 需要用户决策的边界

以下情况停下讨论，不自行决定：

- B 的 500 GB 空间预计不足且需要删原始数据或缩短历史。
- 新数据源与原生历史价格/复权口径冲突。
- 要改变周日共振正式策略规则或阈值。
- 要改变 chan.py 算法语义，而不只是并发/存储实现。
- official 样本仍为 0，需要决定扩大历史还是调整策略合同。
