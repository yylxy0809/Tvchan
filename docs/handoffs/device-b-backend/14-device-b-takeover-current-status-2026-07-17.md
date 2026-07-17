# 设备 B 全面接管当前状态（2026-07-17）

## 1. 文档定位

本文是设备 B 全面接管完成后的最新状态入口。仓库代码、GitHub 合入记录和本文共同构成当前执行基线。

`13-device-b-takeover-execution-plan-2026-07-16.md` 中的 foundation -> execution -> replay/gate 顺序已经完成，只保留作审计记录，**不得重跑**。后续开发从最新 `origin/master` 建立新的、范围单一的分支和草稿 PR，不再重建旧 PR #3、#4、#6 的提交链。

## 2. 当前源码与合入状态

当前已验收的主干基线：

```text
origin/master = 1c790e359e663d20fe72c3747be6d087c047f814
short SHA     = 1c790e3
```

| PR | 交付层 | 状态 |
| --- | --- | --- |
| #9 | lifecycle foundation rebased | 已合入 |
| #10 | Module C / lifecycle execution rebased | 已合入 |
| #11 | historical replay / official gate rebased | 已合入；official manifest 保持 fail-closed |
| #12 | Windows UTF-8 / TSX 合入后验收工具 | 已合入 |
| #13 | 空 timeframe bars fast-path | 已合入 |
| #14 | 北交所 symbol / TradingView 元数据 | 已合入 |
| #15 | 设备 B 接管状态入口 | 已合入；本文随后由独立 PR 刷新 |
| #16 | Module C v4 运行时语义防火墙 | 已合入 |
| #17 | 侧栏 parser 故障隔离 | 已合入 |
| #18 | Strategy lifecycle `observed_time` as-of 防泄漏 | 已合入 |
| #19 | Lifecycle observer 标准 worker / Compose / 单实例退出 | 已合入 |
| #20 | Lifecycle observer 管理状态、API 与 Admin Console | 已合入 |
| #21 | generation-fenced `kline_scope_catalog` 正确性层 | 已合入 |
| #22 | API active-complete exact-empty 消费层 | 已合入 |
| #23 | 设备 B 接管状态刷新 | 已合入 |
| #24 | 通用 Strategy backtest runner diagnostic-only 防火墙 | 已合入；official/default/未知策略在连接数据库前 fail-closed |
| #25 | Admin Console durable token CRUD | 已合入；显式 admin token，无本地伪令牌或网络降级 |
| #26 | scope catalog generation fencing 加固 | 已合入；migration 041、单 building、revision/ABA 防护 |

三层 rebased 代码、合入后验收、v4/lifecycle 防火墙与 scope catalog 两阶段实现均已进入主干。旧 PR 和旧分支仅用于审计，不应再次合并、rebase 或强推。

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
- Strategy 的 `events_as_of` / `snapshot_as_of` 是观察时点安全视图：事件必须同时满足 `effective_time <= as_of` 和 `observed_time <= as_of`。需要仅按因果生效时间读取的调用方不得复用该接口，必须使用名称与语义明确分离的 causal API。

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

## 6. 工程缺口收口状态

原接管清单中的三个最高优先级缺口已经收口：

1. **v4 语义防火墙**：PR #16 使生产读取只接受当前 v4 合同，旧 writer 默认禁用，v3-only 必须 empty/degraded。
2. **Lifecycle observer 产品化与可观测性**：PR #18-#20 完成观察时点防泄漏、标准 worker、Compose 单实例运行、优雅退出、积压/DLQ/watermark 管理状态与前端展示。
3. **可靠 scope catalog**：PR #21 新增 migration 040、可恢复 generation/bootstrap/finalizer、全部 canonical K-line 写删路径同事务维护和 activation fencing；PR #22 仅在 active complete generation 的 exact-empty 证据存在时跳过冷 `klines` probe，其余情况保留正确 fallback。
4. **正式策略执行防火墙**：PR #24 将通用 Strategy backtest runner 永久限定为 diagnostic-only；official/default `weekly_daily_b2_resonance_v1` 与未知策略均在创建数据库连接前 fail-closed，不提供 bypass。
5. **管理令牌认证边界**：PR #25 使 Admin Console 的 token GET/POST/disable/DELETE 显式携带当前 admin token 调用 durable 后端 API；网络失败和 404 不再降级到 `localStorage` 或生成本地伪令牌。
6. **catalog generation 并发防护**：PR #26 的 migration 041 增加全局单一 building generation、base active pointer 与 control revision；writer/finalizer 对旧 revision、ABA 和 pointer-clear 变更均由数据库 fail-closed。

组合验证基线：Web contract `107/107` 且 production build 通过；Strategy `200/200`；PR #26 focused Collector `24/24`，Collector 全套 `369 passed`，另有一个仅因本机缺少可选 `notte_core` 依赖的环境失败。Disposable TimescaleDB 已验证 migration 001..041 与 041 幂等、legacy building 显式恢复、并发 create、旧 writer/finalizer fail-closed、新 finalizer、revision/ABA/pointer-clear 规则、active generation 原子切换和 exact-empty 零 K-line probe；canonical K-line 指纹不变。没有连接或写入生产数据库。

当前没有仓库任务单授权扩大为全市场重算、historical replay 或正式策略回测；official 仍为 `NO_GO`。

## 7. 后续开发方法

后续不再把本节已完成事项当作待办重复实现。每轮从最新 `origin/master`：

1. 检查新任务单、开放 PR、审查、评论与 CI。
2. 使用子代理分别只读审计前端、API/Strategy 和数据库/Collector，给出有文件与测试证据的候选缺口。
3. 只选择一个最高优先级、范围单一、可 TDD 验证且不触碰第 5 节禁止项的工作；没有充分证据时保持只读，不制造需求。
4. 每项使用干净 worktree、独立分支和草稿 PR；先写失败回归测试，再做最小实现。
5. 运行相关 Web/API/Collector/Strategy 测试、必要的 disposable 数据库验证、`git diff --check` 与敏感文件检查；设备 B 自审无阻塞后方可合入。

任何后续工程项都不得改变正式策略 `NO_GO`，不得将 diagnostic/research 结果包装为 official 结论。

## 8. 下一次接管冷启动

```text
1. git fetch origin --prune，记录最新 origin/master；不要把 1c790e3 当作永久固定 SHA。
2. 阅读 AGENTS.md、本文及新增任务单/审查意见。
3. 确认工作树干净，确认禁止项和 official NO_GO 未变化。
4. 从最新 master 建立单一范围分支；不得重跑 13 中已经完成的三层重建。
5. 写入数据库前先提交影响、dry-run、identity/fencing 与回滚说明。
```
