# 设备 B 接管执行方案（2026-07-16）

## 0. 当前状态与唯一入口

本文件是设备 B 在 2026-07-16 起的执行入口。它取代 `12-device-b-full-project-takeover-2026-07-14.md` 中已过时的分支顺序与“设备 B 尚未完成 historical replay”的描述；旧文档仅保留作历史背景。

GitHub 与当前仓库代码是唯一源码事实。不同设备和不同 Codex 账号之间不共享会话记忆，禁止以 SMB、Obsidian 或旧工作树状态作为合并决策依据。

截至本文件提交时：

| 对象 | 状态 |
| --- | --- |
| `origin/master` | `27aa0a6`，尚未包含侧栏、lifecycle 或 replay 工作 |
| PR #5 | `codex/iwencai-sidebar-v2-rebased`，前端窗口化图表与侧栏 V2；设备 A 正在完成 Gate FE |
| PR #3 | `codex/device-b-lifecycle-foundation` @ `5c5da96`，旧 foundation |
| PR #4 | `codex/device-b-module-c-lifecycle-execution` @ `5f12465`，旧 execution；base 错误，不能直接合并 |
| PR #6 | `codex/device-b-historical-replay-execution-20260714` @ `2560c61`，整条 lifecycle/replay/gate 堆叠代码与执行证据 |

设备 B 已在自己的数据库中完成 historical replay、lifecycle/outbox 对账和 official strategy gate 审计。其正式策略结论是 **`NO_GO`**，这是 fail-closed 的正确结果，不能为了获得交易结果而放松 official 合同。

## 1. 不可改变的技术合同

- 权威缠论路径只有 collector-owned Module C；禁止恢复 Module B、`CHAN_SERVICE_URL`、独立 `chan-service` 或运行时 fallback。
- 有效计算合同为 `native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false`：原生 `5f/30f/1d/1w/1m` 分别计算。
- 不修改 vendored `chan.py` 语义；适配、发布、查询在外围实现。
- 维持 immutable run/head、稳定 identity、完整历史前缀、event-time、no-lookahead 与原子发布。
- `weekly_daily_b2_resonance_v1` 的 official、diagnostic 与 relaxed research 必须物理和逻辑隔离。
- canonical K 线只读，除非另有明确、可审计的 K 线修复授权。
- 侧栏的外部行情字段走当前 iWencai/AnythingAPI 聚合链；缠论状态与策略信号继续只读本地数据库。不得恢复 WeStock 运行时 fallback。

## 2. 当前硬门禁：设备 B 等待 Gate FE

设备 A 现在只在 `codex/iwencai-sidebar-v2-rebased` 上完成 PR #5 的测试、页面烟测与性能证据。

在设备 A 宣布 PR #5 已合并到 `master` 前，设备 B：

- 可以整理 PR #6 的可复现测试与精简审计文档；
- 可以检查本机 K 线审计、磁盘、TimescaleDB 与运行环境；
- 可以准备不进 Git 的运行资产清单；
- 不得合并 PR #3、PR #4 或 PR #6；
- 不得启动新的无审计全市场 Module C 重算；
- 不得改写旧共享分支历史；
- 不得把 `NO_GO` 改写为正式策略通过。

等待的原因是主干尚未包含前端/API 合同。即使侧栏和 B 栈当前没有同名文件冲突，迁移、published head、空 head、窗口化图表 bundle 的运行时合同仍必须以合入后的 `master` 为基线复验。

## 3. 正确的主干顺序

```text
Gate FE: PR #5 测试、烟测、性能证据 -> merge master
  -> foundation rebased -> 独立 PR -> merge master
  -> execution rebased -> 独立 PR -> merge master
  -> replay + official gate rebased -> 独立 PR -> merge master
  -> B API canary + A 前端端到端验收
  -> 满足数据条件后，才建立新的 official-backtest 实现分支
```

PR #6 是有价值的代码与证据来源，但不是可直接合入的最终交付：它包含 foundation、execution、replay 和 gate 整个堆叠链。直接合入会与 PR #3/#4 重复，损失审查和回滚边界。

## 4. 设备 B 合并执行步骤

只有在 PR #5 合入后才执行以下步骤。全部在干净的本地 clone 或 worktree 中完成；每一层合入后才开始下一层。

```powershell
git fetch origin --prune
git status --short
git rev-parse origin/master

$OLD_MASTER = "27aa0a6"
$FOUNDATION = "5c5da96"
$EXECUTION  = "5f12465"
$REPLAY     = "2560c61" # 执行前再次确认 PR #6 head

# 1. foundation：仅搬运旧 master 到 foundation tip 的 7 个提交
git switch -c codex/device-b-lifecycle-foundation-rebased origin/master
git rebase --onto origin/master $OLD_MASTER $FOUNDATION

# foundation 合并后更新 origin/master。
# 2. execution：仅搬运旧 foundation 之后的 6 个提交
git switch -c codex/device-b-module-c-lifecycle-execution-rebased origin/master
git rebase --onto origin/master $FOUNDATION $EXECUTION

# execution 合并后更新 origin/master。
# 3. replay/gate：仅搬运 execution 之后到 PR #6 tip 的增量
git switch -c codex/device-b-historical-replay-gate-rebased origin/master
git rebase --onto origin/master $EXECUTION $REPLAY
```

每层都必须：

1. 新建 `*-rebased` 分支和新 PR，base=`master`。
2. 保留旧分支和旧 PR 作审计记录，在旧 PR 说明其已 superseded。
3. 运行 `git diff --check`、本层对应测试、空库迁移、现库升级和 strategy-service 回归。
4. 逐文件解决冲突，不用 `ours`/`theirs` 粗暴覆盖。
5. 不对旧共享分支执行 `--force` 或 `--force-with-lease`。

即使上一层采用 squash merge，仍使用 `git rebase --onto`；不要依赖提交 SHA 自动去重。

## 5. 合入后的运行验收

合入 replay/gate 后，默认做 canary，不默认重跑 B 已完成的全市场任务。

| 验收面 | 必须证明 |
| --- | --- |
| K 线 | read-only；无非 canonical 写入；审计状态可追溯 |
| Module C | published head 有完整历史前缀，identity 稳定，原子发布 |
| 时间语义 | `effective_time`/`observed_time` 无未来数据和倒置 |
| lifecycle | projection mismatch=0、history missing=0，或有明确排除清单 |
| canary | 固定少量标的，含北交所、科创、停牌/稀疏样本；0 failed |
| 前端 | 指向 B API 后，空 head 合法不崩；有 head 时 overlay 完整 |
| 侧栏 | 外部数据失败不影响 K 线、Module C 或 strategy；bootstrap 不阻塞外部请求 |

只有 canary 证明旧运行产物与合入后的代码或迁移不兼容，才提出局部或全量重算计划，并先获得用户授权。

## 6. 官方策略的后续条件

当前 `NO_GO` 不能视为工程失败。PR #6 的严格瀑布为：

```text
5529 -> 5525 -> 61 -> 4 -> 0 -> 0 -> 0
```

只有同时满足以下条件，才创建新的 `official-backtest` 实现分支：

1. 双级别覆盖宇宙内存在至少一个因果、官方、predictive 的 weekly B2。
2. 下游 daily/intraday episodes 已按同一可见性合同重建。
3. 至少保留三条完整 official traces。
4. official 输出与 diagnostic 产物仍保持隔离。

旧 `7a413d2` 只是规划提交，不能作为 official-backtest 实现分支复用。

## 7. 设备 B 冷启动清单

```text
1. git fetch --prune，记录 origin/master 和 PR #5/#3/#4/#6 的 head/base。
2. 阅读 AGENTS.md、本文件和当前 master 的 STATUS（若存在）。
3. 输出当前 HEAD、工作树、当前 gate、下一步三项、阻塞项。
4. 任何数据库写操作前，说明影响范围、dry-run、run id 与回滚方式。
5. 禁止假设设备 A 的数据库、密钥、charting_library 或性能结果可迁移到设备 B。
```

真实密钥、cookie、数据库目录、tablespace、Docker volume 和 TradingView 授权资产均不进入 Git。通过单独的私密交接清单提供。

## 8. 接管完成定义

只有同时满足以下条件，设备 B 才进入“全面接管”状态：

- `master` 已包含 PR #5 和三层 rebased B PR；
- B 机 canary 通过且 K 线只读合同未被破坏；
- A 前端指向 B API 的空 head / 有 head 两套验收通过；
- 运行资产在 B 机可启动；
- 当前 gate、禁止项和已知问题已写入仓库状态文件或 GitHub issue。

在此之前，设备 B 的责任是保持已有 replay 证据、等待 Gate FE、按层重建主干；不是开启另一轮无边界的全量开发或重算。
