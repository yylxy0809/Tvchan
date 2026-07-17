# 设备 B 全面接管当前状态（2026-07-17）

## 1. 文档定位

本文是设备 B 全面接管完成后的最新状态入口。仓库代码、GitHub 合入记录和本文共同构成当前执行基线。

`13-device-b-takeover-execution-plan-2026-07-16.md` 中的 foundation -> execution -> replay/gate 顺序已经完成，只保留作审计记录，**不得重跑**。后续开发从最新 `origin/master` 建立新的、范围单一的分支和草稿 PR，不再重建旧 PR #3、#4、#6 的提交链。

## 2. 当前源码与合入状态

当前已验收的主干基线：

```text
origin/master = 379b26da159f3fc4356508ac079956d665a92924
short SHA     = 379b26d
```

| PR | 交付层 | 状态 |
| --- | --- | --- |
| #9 | lifecycle foundation rebased | 已合入 |
| #10 | Module C / lifecycle execution rebased | 已合入 |
| #11 | historical replay / official gate rebased | 已合入；official manifest 保持 fail-closed |
| #12 | Windows UTF-8 / TSX 合入后验收工具 | 已合入 |
| #13 | 空 timeframe bars fast-path | 已合入 |

三层 rebased 代码、验收工具与空周期查询修复均已进入主干。旧 PR 和旧分支仅用于审计，不应再次合并、rebase 或强推。

## 3. 接管验收结论

### 3.1 设备 B canary 与数据只读合同

- 设备 B 的小规模、多样化 canary 已通过，覆盖有 Head、空 Head、北交所、科创板和停牌/稀疏样本。
- 已 sealed 的批次保持 sealed，任务失败数为零；抽检 published head 的 identity 一致。
- canonical K 线在验收前后快照一致；未执行 K 线更新、删除、重建或全市场重算。
- Module C 当前权威合同仍为 `native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false`，五个原生周期独立计算。

### 3.2 Lifecycle 对账

- lifecycle reconciliation 已通过：projection mismatch 为零，published history missing 为零。
- outbox blocking 为零，未发现需要用数据删除或重建掩盖的失败。
- historical replay 的 `effective_time` 继续来自因果 cutoff，`observed_time` 不得早于 `effective_time`；baseline current state 与 historical replay 保持隔离。

### 3.3 真实 TradingView Advanced Charts 验收

合法授权的 `charting_library` 仅保存在本机 Git 忽略目录，未进入仓库。真实浏览器验收结果：

- `001220.SZ` 的 5 分钟 K 线、有 Head overlay 和结构绘制正常。
- `920047.BJ` 无 K 线时显示合法空态，不崩溃。
- `688001.SH` 与 `000017.SZ` 有 K 线但空 Head 时返回空 overlay，不残留旧标的图层。
- A -> B -> A 快速回切稳定；右侧新闻或外部资料无数据时不影响 K 线和 Module C 图层。

授权静态资源不得提交、打包进公开产物或记录其私有内容。

## 4. 正式策略状态

正式策略 `weekly_daily_b2_resonance_v1` 当前结论继续为 **`NO_GO`**。这是严格门禁的正确 fail-closed 结果，不是工程失败。

已审计的严格瀑布为：

```text
5529 -> 5525 -> 61 -> 4 -> 0 -> 0 -> 0
```

双级别覆盖宇宙中仍没有因果、official、predictive 的 weekly B2，因此不得用 confirmed、current head、point time、diagnostic、relaxed research 或近似数据生成伪正式交易。只有满足 `13` 第 6 节列出的数据条件后，才可新建独立 official-backtest 实现分支。

## 5. 当前禁止项

- 不修改或删除 canonical K 线，不迁移、复制或提交数据库目录、WAL、tablespace 或 Docker volume。
- 不重开 sealed 批次，不删除有效 run/head，不启动未经新任务明确限定和审计的全市场 Module C 重算或 historical replay。
- 不恢复 Module B、namespace-B、`CHAN_SERVICE_URL`、独立 `chan-service` 或运行时 fallback。
- 不修改 vendored `chan.py` 语义；集成行为继续放在外围 adapter、存储与 API。
- 不把 diagnostic/research 产物写入 official 输出，不把 `NO_GO` 改写为正式策略通过。
- 不提交密钥、token、cookie、日志、数据库文件、`outputs` 大型产物或 TradingView 授权资产。
- 数据库写任务必须先给出范围、dry-run、run/batch identity、fencing 和回滚方案；接管完成不等于默认授权无边界生产写入。

## 6. 已知工程缺口与优先级

### P0：建立端到端 v4 语义防火墙

当前 API overlay/screener 仍把旧 v3 Module C hash 视为兼容，旧 Strategy historical-backfill writer 也仍硬编码 v3 hash。若 v4 Head 缺失，生产读取可能静默退回不同语义；旧工具还可能写入错误标识的 run。

下一步应：

1. 让生产 API、健康状态和 screener 只接受当前 v4 hash；v3-only 必须 empty/degraded。
2. 退役或默认禁用 Strategy 中可写 Module C 的旧 v3 backfill 路径；official 查询显式限定 v4、publication profile 和允许的 run group。
3. 补 v3-only、v3/v4 混合与 official `NO_GO` 回归测试。

该项不需要数据库迁移，不改变现有历史审计数据，优先级最高。

### P0/P1：Lifecycle observer 产品化与可观测性

Lifecycle observer 已具备 lease、fencing、重试和 dead-letter 语义，但当前没有注册为标准 collector worker，也未由 Compose 与 `chan-c-stream` 一起持续启动；管理状态接口没有暴露 lifecycle outbox/DLQ。

下一步应：

1. 合并重复启动入口，注册标准 worker，并加入 realtime Compose profile。
2. 在管理状态中显示 pending/processing/failed/dead-letter、最旧积压和 observer watermark。
3. 增加 head publication -> outbox -> lifecycle event/current projection 的小型 PostgreSQL 集成测试。

部署后只写现有 lifecycle/outbox 表，不修改 K 线；异常积压必须 degraded/fail-closed。

### P1：可靠 scope catalog 消除空 timeframe 冷查询

PR #13 已避免无条件主查询，但缺 ingest watermark 时仍会直接探测大型 Timescale hypertable。真实冷/custom plan 仍约 4.2 至 4.5 秒，连接复用进入 generic plan 后才降至约 40 毫秒。不能简单把“缺 watermark”解释为“无 K 线”，因为旧数据可能存在 K 线但没有 watermark。

下一步应新增带完整性状态的轻量 `kline_scope_catalog`：

1. 由可恢复的只读扫描回填 `(symbol_id, timeframe)` 存在性和边界，整代完成后原子标记 complete。
2. 所有 K 线写路径在同一事务维护正向 scope；删除路径使对应 scope 失效或重建。
3. catalog 未完成或失效时继续正确但较慢的 fallback；只有 complete generation 中确实缺行才返回空。
4. 用全新数据库连接冷测空周期，并核对正向样本响应与 K 线快照完全不变。

数据库影响仅限新增小型 metadata 和一次只读回填扫描，不得改写 canonical K 线。

## 7. 后续开发顺序

推荐顺序：

1. P0 v4 语义防火墙：先消除静默语义回退和旧写路径。
2. P0/P1 lifecycle observer 产品化：保证后续增量发布不会让策略生命周期静默滞后。
3. P1 scope catalog：在不牺牲正确性的前提下消除空周期冷查询抖动。

每项使用独立分支和草稿 PR；先写回归测试，再做最小实现，运行相关 Web/API/Collector/Strategy 测试、迁移幂等验证、`git diff --check` 与敏感文件检查。任何一项都不得改变正式策略 `NO_GO`。

## 8. 下一次接管冷启动

```text
1. git fetch origin --prune，记录最新 origin/master；不要把 379b26d 当作永久固定 SHA。
2. 阅读 AGENTS.md、本文及新增任务单/审查意见。
3. 确认工作树干净，确认禁止项和 official NO_GO 未变化。
4. 从最新 master 建立单一范围分支；不得重跑 13 中已经完成的三层重建。
5. 写入数据库前先提交影响、dry-run、identity/fencing 与回滚说明。
```
