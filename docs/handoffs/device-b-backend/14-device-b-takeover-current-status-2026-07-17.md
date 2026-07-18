# 设备 B 全面接管当前状态（2026-07-17）

## 1. 文档定位

本文是设备 B 全面接管完成后的最新状态入口。仓库代码、GitHub 合入记录和本文共同构成当前执行基线。

`13-device-b-takeover-execution-plan-2026-07-16.md` 中的 foundation -> execution -> replay/gate 顺序已经完成，只保留作审计记录，**不得重跑**。后续开发从最新 `origin/master` 建立新的、范围单一的分支和草稿 PR，不再重建旧 PR #3、#4、#6 的提交链。

## 2. 当前源码与合入状态

当前已验收的主干基线：

```text
origin/master = 5595cca7e5d86ec5741b29fc19acdc281cc744e6
short SHA     = 5595cca
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
| #27-#41 | 接管状态、observer 新鲜度、Strategy fail-closed、管理认证、scope catalog 管理与 bulk bootstrap | 已合入；正式策略保持 `NO_GO` |
| #42 | strict-v2 canonical audit producer | 已合入；exact 五级快照、catalog/Kline 同快照交叉核验与双键 advisory fence |
| #43 | strict-v2 eligibility consumer A | 已合入；migration 043 与 audit/freshness/catalog/universe provenance 冻结 |
| #44 | strict-v2 execution consumer B | 已合入；prepare/activate drift 重验、原子激活与 running/running worker/publication fence |
| #45 | strict-v2 Module C 只读可观测性 | 已合入；Collector report、管理 API 与 Admin Console 全链路 fail-visible |
| #46 | deterministic Module C canary selection-v2 | 已合入；四板块各 5 标、每板块活动边界 2/1/2、稳定排序与 canonical hash |
| #47 | selection-v2 冻结证据与管理可观测性 | 已合入；完整 manifest、subset universe/source/配额 gate、旧证据缺失 fail-visible |
| #48 | 设备 B 接管状态刷新 | 已合入；记录 strict-v2 producer/consumer、selection-v2 与生产 freshness 阻塞 |
| #49 | selection-v2 共享纯合同 | 已合入；Collector/API 共用无数据库依赖的 schema、hash、quota 与 active-universe 验证 |
| #50 | Admin 管理认证失效边界 | 已合入；401/403 清 session，标准 HeadersInit 不得覆盖显式 Admin Bearer |
| #51 | Module C 活动批次只读可观测性 | 已合入；确定排序的 running batch IDs、bigint 字符串与前端请求 epoch fence |
| #52 | 浏览器登录权威收口 | 已合入；删除 frontend/local/public-health 伪认证，登录只接受后端权威响应 |
| #53 | durable 登录可用性语义 | 已合入；durable authority 不可用统一脱敏 503，static token 与既有 protected HTTP/WS 合同不变 |
| #54 | 设备 B 接管状态刷新 | 已合入；记录管理认证、浏览器登录与 durable 登录可用性收口 |
| #55 | Admin 认证错误正文故障隔离 | 已合入；401/403 正文不可读仍保留 status 并执行权威登出，5xx/network 保持局部降级 |
| #56 | 在线 Module C tail lease/publication fencing | 已合入；active lease 不被 normalize 撤销，publication 与 task completion 在同一事务内按 running/running、claim token/version、anchor/target/head identity fail-closed |
| #57 | protected HTTP/WebSocket durable authority 可用性 | 已合入；HTTP authority 故障脱敏 503，WebSocket 区分 1013/1008，conditional active touch 阻断 lookup 后 revoke/delete 竞态 |

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
7. **strict-v2 输入与执行闭环**：PR #42-#45 将 exact 五级 canonical audit、eligibility provenance、freshness/catalog/universe 绑定、prepare/activate drift 重验、running/running worker/publication fence 和只读管理状态连成 fail-closed 闭环。
8. **deterministic canary selection-v2**：PR #46-#49 固定四板块各 5 标与每板块低/中/高活动边界 2/1/2，冻结完整 selection manifest/hash，并由 Collector/API 共享同一无数据库纯合同。
9. **管理与浏览器认证收口**：PR #50-#52 统一 Admin 401/403 失效边界，移除所有浏览器伪认证路径，并保持迟到登录不能覆盖最新会话。
10. **durable 登录可用性**：PR #53 将 token store 缺失、lookup 或 usage touch 失败统一映射为不泄露凭据/数据库细节的 503；健康 authority 下的 invalid/disabled 仍为 `valid=false`，static token 不依赖数据库池。
11. **Admin 认证错误正文隔离**：PR #55 保证 401/403 的响应正文即使不可读取也不会丢失 status 或绕过登出；500/503/network 仍只局部降级，不伪造认证状态。
12. **在线 tail lease/publication fencing**：PR #56 统一 5f/30f/1d exact 与 1w/1m logical-period 状态机，禁止 normalize 撤销未过期 lease，并让旧 owner 在任何 run/structures/head/history/outbox/watermark/task completion 副作用前按 claim 与发布 identity fail-closed。
13. **protected durable authority 可用性**：PR #57 将非 static token 的 store/pool 缺失、lookup 与 usage touch 故障在 protected HTTP 映射为脱敏 503；WebSocket 在业务处理前用 1013 表达可重试故障、以 1008 保留 invalid/disabled 语义；active conditional touch 关闭查询后撤销竞态。

组合验证基线：Web contract `129/129` 且 production build 通过；API `259 passed / 8 skipped`；Collector `668 passed / 3 skipped`，另有一个仅因本机缺少可选 `notte_core` 依赖的既有环境失败。strict-v2 producer/consumer 已通过 focused、全套及 disposable PostgreSQL/TimescaleDB 的迁移、并发、回滚和 fencing 验证。selection-v2 共享合同已通过 shared/API/Collector/Web 三层回归；PR #55 通过 Web focused/full、production build、diff 检查与独立 P0/P1 复审；PR #56-#57 通过各自 focused/full、`compileall`、diff/security 检查与三路独立 P0/P1 复审，其中 PR #56 另通过 disposable TimescaleDB 验证。上述后续工程验证均未连接或写入生产库。

生产 `kline_scope_catalog` generation `2188f14c-0b35-416d-9671-fd3d227d1f75` 已 complete/active，control revision 为 `1`，`scope_count=expected_scope_count=38738`，unknown/incomplete 为零；bootstrap worker 已移除。canonical K-line 指纹保持不变，outbox blocking 为零，observer 健康。

当前硬阻塞是权威交易日历下的 canonical freshness：`5f/30f/1d/1w` 实际最大水位仍为 `2026-07-03T07:00:00Z`，`1m` 为 `2026-06-30T07:00:00Z`，已不满足当前 expected closed watermark。未经新的明确授权，不刷新或修改 canonical K 线，不生成生产 eligibility/canary，也不启动全市场 Module C 重算、historical replay 或正式策略回测；official 仍为 `NO_GO`。

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
1. git fetch origin --prune，记录最新 origin/master；不要把 5595cca 当作永久固定 SHA。
2. 阅读 AGENTS.md、本文及新增任务单/审查意见。
3. 确认工作树干净，确认禁止项和 official NO_GO 未变化。
4. 从最新 master 建立单一范围分支；不得重跑 13 中已经完成的三层重建。
5. 写入数据库前先提交影响、dry-run、identity/fencing 与回滚说明。
```
