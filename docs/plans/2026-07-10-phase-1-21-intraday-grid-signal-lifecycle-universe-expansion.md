# Phase 1.21 盘中覆盖网格、信号生命周期与可观测样本扩展 Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Follow TDD. Do not change the frozen strategy contract or Module C semantics.

**Goal:** 修正 Phase 1.20R 的盘中覆盖率定义，按真实 K 线截止点重建 30F/5F 信号生命周期，并在现有数据库可精确回放的标的范围内扩展周线—日线共振二买样本，以确定“零入场”究竟来自数据缺口、信号时间语义还是冻结策略本身。

**Architecture:** 新增只读 Phase 1.21 研究流水线。以 `klines` 中完整的 30F/5F K 线截止点作为 expected grid，以 `chan_c_runs` 为 actual grid，按 `symbol × level × mode × cutoff_bar_end` 核对覆盖；再从每个历史 run 的完整信号集合构建 APPEARED/PERSISTED/CONFIRMED/DISAPPEARED 生命周期。先复核 Phase 1.20R 的 8 个 episode，再从 `research_daily_close` 已覆盖的可观测标的中重建更广的周线/日线 episode。所有候选口径只输出对照，不改变 `weekly_daily_b2_official_v1.0`。

**Tech Stack:** Python 3.11、asyncpg、PostgreSQL/TimescaleDB、pytest、JSON/JSONL/CSV/Markdown。

---

## 0. 已知事实与本阶段判断边界

Phase 1.20R 已证明：

- Micro V2 manifest 的 40 个 run_id 全部存在，失败 0；
- 当前 8 个独立 daily setup episode 全部来自 `000001.SZ`；
- Ledger V2 中 30F 事件 131 个、5F 事件 334 个，但仅覆盖 `000001.SZ`；
- 官方入场触发数为 0；
- 数据库审计前后 run-group 与 published-head 计数未变化；
- 现有测试为 `100 passed`。

但 Phase 1.20R 的 `intraday_run_coverage_audit_v2` 存在三个不能用于最终结论的问题：

1. `expected_cutoff_count` 固定为 2，没有按窗口内真实 30F/5F K 线计算；
2. `coverage_ratio = len(inside) / 2`，结果可大于 1，当前样本甚至达到 144；
3. `fully_covered` 只要求窗口内至少存在一个 30F run 和一个 5F run，不能证明整个触发窗口连续可见。

另外，`phase_1_20r.py` 调用 `evaluate_entry_state_v4()` 时未传 `trigger_window_end`。本次必须修复调用与测试，但不得改变冻结的窗口政策本身。

本阶段只回答以下问题：

- 每个 episode 的预期盘中截止点是否真的有对应历史 run？
- 30F B1/1p 在每个 run 中何时出现、确认、持续和消失？
- 官方冻结规则在 8 个 episode 以及更广的数据库可观测样本中分别通过多少？
- 是否存在可以精确定义的缺失 cutoff，值得下一阶段受控回填？

本阶段不回答收益率，不修改策略参数，不执行正式回测。

## 1. 强制边界

### 1.1 允许

- 修改 `services/strategy-service/app/**` 中 Phase 1.21 新增研究代码；
- 对 Phase 1.20R 的覆盖率算法和 `trigger_window_end` 漏传进行向后兼容修复；
- 新增/修改 `services/strategy-service/tests/**`；
- 只读查询 `symbols`、`klines`、`chan_c_runs`、`chan_c_signals`；
- 生成 `services/strategy-service/outputs/phase-1-21-intraday-grid-signal-lifecycle/` 下的研究产物。

### 1.2 禁止

- 不修改 `Vespa314/chan.py`、Module C 计算语义、`bi_strict` 或买卖点形成逻辑；
- 不更新 `scheme2_chan_c_published_heads`；
- 不插入、覆盖或删除任何 `chan_c_runs`/`chan_c_signals`；
- 不修改 `weekly_daily_b2_official_v1.0` 的类型、分值、阈值或 `require_30f_b1`；
- 不把 `1p` 升格为官方 B1；
- 不把 diagnostic 结果描述成正式回测；
- 不接 API、后台或前端；
- 不使用最终静态 published head 冒充历史 first_seen；
- 不运行 50 标的或全市场历史回填；
- 不执行 git reset/checkout/revert，不处理无关工作区改动。

## 2. 冻结策略合同

官方合同保持：

- 30F B1：`bsp_type=1`，40 分；
- 日线底分型：30 分；
- 5F B2/B2S 确认 30F B1：30 分；
- 入场至少 70 分且必须包含新鲜 30F B1；
- 成交价：触发后下一根 30F K 线开盘价；
- 历史触发使用 `first_seen_time`；`confirm_time` 只作为保守对照；
- `1p` 只属于 research-only candidate policy。

对 predictive/confirmed 的处理：

- `chan_c_runs.mode` 是快照模式；
- `chan_c_signals.is_confirmed` 是信号自身确认状态；
- 本阶段分别统计“首次出现”和“首次确认”，不得把两者混为一个时间；
- 若规范未明确冻结使用首次出现还是首次确认，输出并列结果并标记为未来决策点，不在代码中暗选。

## 3. 交付目录

固定输出目录：

`services/strategy-service/outputs/phase-1-21-intraday-grid-signal-lifecycle/`

必须包含：

- `source_artifact_manifest.json/md`
- `database_readonly_snapshot_before.json`
- `database_readonly_snapshot_after.json`
- `expected_intraday_cutoff_grid.jsonl`
- `actual_intraday_run_grid.jsonl`
- `intraday_run_coverage_v3.json/md`
- `intraday_run_coverage_by_episode.csv`
- `intraday_run_coverage_missing_cutoffs.jsonl`
- `intraday_run_duplicate_cutoffs.jsonl`
- `signal_lifecycle_30f_b1.jsonl`
- `signal_lifecycle_30f_1p.jsonl`
- `signal_lifecycle_5f_b2_b2s.jsonl`
- `signal_lifecycle_summary.json/md`
- `trigger_window_semantics_v2.json/md`
- `phase_1_20r_8_episode_recheck.json/md`
- `observable_research_universe.json/md`
- `weekly_context_episodes_v2.jsonl`
- `daily_setup_episodes_v2.jsonl`
- `expanded_gate_waterfall.json/md`
- `expanded_gate_waterfall_by_symbol.csv`
- `expanded_gate_waterfall_by_year.csv`
- `policy_counterfactual_matrix.json/md`
- `micro_backfill_v4_admission.json/md`
- `micro_backfill_v4_manifest.csv`（只生成计划，不执行）
- `next_phase_decision.json/md`
- `trace_index.md`
- `traces/*.md`
- `phase_1_21_detailed_completion_report.md`
- `phase_1_21_task_checklist_report.md`

---

### Task 1: Preflight、只读快照与固定输入清单

**Files:**

- Create: `services/strategy-service/app/engine/phase_1_21.py`
- Create: `services/strategy-service/app/cli/run_phase_1_21.py`
- Create: `services/strategy-service/tests/test_phase_1_21.py`

**Step 1: 写失败测试**

测试 preflight 必须记录：

- Phase 1.20R 关键输入文件的绝对路径、size、mtime、SHA256；
- 数据库表名与只读 SQL 摘要；
- run-group 白名单；
- 冻结合同版本；
- `database_before` 中 published-head 数量和各 run-group 数量。

缺任一必需文件必须失败，不能静默降级。

**Step 2: 运行失败测试**

Run: `python -m pytest tests/test_phase_1_21.py -q`

Expected: FAIL，Phase 1.21 尚未实现。

**Step 3: 最小实现**

固定源至少包括 Phase 1.20R 的：

- `daily_setup_episodes.jsonl`
- `weekly_context_episodes.jsonl`
- `signal_event_ledger_v2_30f.jsonl`
- `signal_event_ledger_v2_5f.jsonl`
- `phase_1_20r_summary.json`

**Step 4: 验证**

Expected: preflight 测试 PASS，数据库未写入。

### Task 2: 真实 expected cutoff grid

**Files:**

- Create: `services/strategy-service/app/engine/intraday_cutoff_grid.py`
- Modify: `services/strategy-service/app/repositories/module_c_repo.py`
- Create: `services/strategy-service/tests/test_intraday_cutoff_grid.py`

**Step 1: 写失败测试**

覆盖以下边界：

- expected cutoff 只能来自 `klines.is_complete=true`；
- 按 episode 的 `symbol`、窗口起止和原生 `timeframe in (5,30)` 读取；
- 周末、休市、停牌不生成虚假 cutoff；
- 同一 `(symbol, timeframe, ts)` 去重；
- 输出按时间升序；
- 结束边界包含规则固定并测试；
- `expected_30f_count` 和 `expected_5f_count` 不能写死。

**Step 2: 实现 set-based repository 查询**

一次性读取全部 episode 的相关 K 线，禁止 episode × SQL 的 N+1 查询。

**Step 3: 验证**

Expected: 所有 ratio 的分母来自真实 K 线，周末不计入。

### Task 3: Actual run grid 与 Coverage V3

**Files:**

- Create: `services/strategy-service/app/engine/intraday_run_coverage_audit_v3.py`
- Create: `services/strategy-service/tests/test_intraday_run_coverage_audit_v3.py`
- Modify: `services/strategy-service/app/engine/phase_1_21.py`

**Step 1: 写失败测试**

V3 必须按以下键去重：

`symbol × level × mode × cutoff_bar_end`

同时保留 duplicate run_id 列表和 run-group provenance。

每个 episode、level、mode 输出：

- expected cutoff count；
- covered cutoff count；
- missing cutoff count；
- duplicate cutoff count；
- `coverage_ratio = covered / expected`，必须处于 `[0,1]`；
- first/last expected cutoff；
- first/last actual cutoff；
- missing cutoff 明细。

分类冻结为：

- `complete`：missing=0 且 expected>0；
- `partial`：0<covered<expected；
- `none`：covered=0 且 expected>0；
- `not_applicable`：expected=0；
- `unsupported_mode`：数据库无该 mode 的历史 run，单独报告，不能算成普通缺失。

**Step 2: 兼容修复 V2**

保留 V2 文件供旧任务重现，但 Phase 1.21 禁止再使用其 `fully_covered` 作为决策依据。

**Step 3: 验证**

断言 Phase 1.20R 中大于 1 的 coverage ratio 在 V3 中不存在。

### Task 4: 30F/5F 信号生命周期账本

**Files:**

- Create: `services/strategy-service/app/engine/signal_lifecycle_ledger.py`
- Create: `services/strategy-service/tests/test_signal_lifecycle_ledger.py`
- Modify: `services/strategy-service/app/repositories/module_c_repo.py`

**Step 1: 写失败测试**

同一 mode 内，信号 identity 固定为：

`symbol|level|mode|side|bsp_type|signal_point_time|price_x1000`

生命周期事件：

- `APPEARED`：前一可见 run 不存在，当前 run 出现；
- `PERSISTED`：连续可见；
- `CONFIRMED`：第一次 `is_confirmed=true`；
- `DISAPPEARED`：前一 run 存在，当前 run 不存在；
- `REAPPEARED`：消失后再次出现。

必须分别保存：

- `point_time`
- `first_seen_time`
- `confirm_time`
- `disappear_time`
- `first_seen_run_id`
- `confirm_run_id`
- `last_seen_run_id`
- source run groups
- 是否存在 cutoff gap；若有 gap，禁止断言精确 disappear_time。

**Step 2: set-based 查询**

一次查询返回相关 run 及其完整 signal 集合；不能只查询有 signal 的 run，否则无法识别 disappearance。

**Step 3: 生成分类账本**

至少输出 30F buy `1`、30F buy `1p`、5F buy `2/2s`。

**Step 4: 验证**

测试连续、缺口、重复 run、首次即 confirmed、消失后重现等情况。

### Task 5: 触发窗口合同执行修复与并列语义审计

**Files:**

- Modify: `services/strategy-service/app/engine/phase_1_20r.py`
- Modify: `services/strategy-service/app/engine/entry_state_machine_v4.py`
- Create: `services/strategy-service/app/engine/trigger_window_semantics_v2.py`
- Modify: `services/strategy-service/tests/test_entry_state_machine_v4.py`
- Create: `services/strategy-service/tests/test_trigger_window_semantics_v2.py`

**Step 1: 写回归测试**

- Phase 1.20R 调用必须把 `trigger_window_end` 传给 state machine；
- 30F first_seen 在 as_of 前但在 window_end 后，必须 blocked；
- 5F confirmation 必须晚于对应 30F B1 first_seen；
- `five_f_counted=false` 时不能进入 5F 的 30 分；
- bottom + 5F 且无 30F B1 仍只能 60，不能触发。

**Step 2: 修复最小逻辑**

修正 `evaluate_entry_state_v4()`：评分必须使用 `five_f_counted`，不能使用未绑定到 30F B1 的任意 5F 可见信号。

**Step 3: 只读政策对照**

输出四组，均不改官方合同：

- `official_calendar_window_first_seen`
- `official_calendar_window_confirm_time`
- `diagnostic_trading_session_window_first_seen`
- `candidate_1p_research_only`

明确标记后三组不属于正式策略结果。

### Task 6: Phase 1.20R 八 episode 重检

**Files:**

- Modify: `services/strategy-service/app/engine/phase_1_21.py`
- Extend: `services/strategy-service/tests/test_phase_1_21.py`

**Step 1: 用 V3 覆盖重算**

对 8 个 episode 输出：

- 真实 cutoff grid coverage；
- B1/1p 生命周期；
- bottom fractal；
- 5F B2/B2S 与 parent 30F B1 的绑定；
- official 和三个 diagnostic policy 的状态转换。

**Step 2: 输出旧结论对照**

不得只写“仍为 0”。必须说明每个 episode 在哪一层失败：

- 数据缺口；
- 无 B1；
- B1 太晚；
- B1 未确认；
- 无 bottom；
- 无有效 5F confirmation；
- execution bar 缺失。

### Task 7: 构建数据库可观测研究样本宇宙

**Files:**

- Create: `services/strategy-service/app/engine/observable_research_universe.py`
- Create: `services/strategy-service/tests/test_observable_research_universe.py`
- Modify: `services/strategy-service/app/engine/phase_1_21.py`

**Step 1: 冻结来源**

只使用数据库已有 `research_daily_close` 和明确白名单历史 run-group。当前事实显示 `research_daily_close` 约覆盖 13 个标的；实际数量必须现场查询并写入报告，不能硬编码 13。

**Step 2: 纳入条件**

每个标的至少需要：

- 1W、1D、30F、5F 的 historical run；
- 可定位的 K 线截止点；
- 盘中 coverage 可审计；
- 不使用当前 published head 替代历史 run。

市值缺失时允许进入 `diagnostic_universe_without_market_cap`，但不得称为正式策略宇宙。

**Step 3: episode 重建**

从历史 run 的 first_seen/confirm lifecycle 重建：

- 周线 B1 -> B2/B2S 上下文；
- 日线 B1、第一笔上涨强度、D_B2/B2S setup；
- 独立 weekly context episode；
- 独立 daily setup episode。

同一结构重复出现在多个 run 中只能算一个 episode。

**Step 4: 验证**

测试不能把 observation count 当 episode count，不能跨 symbol 串联结构，不能未来泄漏。

### Task 8: 扩展 Gate Waterfall 与信号稀缺性诊断

**Files:**

- Create: `services/strategy-service/app/engine/expanded_gate_waterfall.py`
- Create: `services/strategy-service/tests/test_expanded_gate_waterfall.py`
- Modify: `services/strategy-service/app/engine/phase_1_21.py`

**Step 1: 固定 waterfall**

至少输出：

1. observable symbols；
2. weekly B1；
3. weekly B2/B2S with prior B1；
4. weekly price relation valid；
5. weekly DIF>0（若历史 DIF 不可重建，单独 blocker，不得默认通过）；
6. daily B1；
7. daily first-up strength >=70；
8. daily B2/B2S valid；
9. entry watch；
10. fresh 30F B1 appeared；
11. 30F B1 confirmed；
12. daily bottom fractal；
13. valid 5F B2/B2S confirmation；
14. official >=70 trigger；
15. next 30F execution bar available。

**Step 2: 分组输出**

按 symbol、year、policy 输出，不能只给总数。

**Step 3: 样本独立性**

报告 observation、weekly episode、daily episode、entry episode 四个基数，正式统计只使用 entry episode。

### Task 9: Micro Backfill V4 准入决策（只规划不执行）

**Files:**

- Create: `services/strategy-service/app/engine/micro_backfill_v4_planner.py`
- Create: `services/strategy-service/tests/test_micro_backfill_v4_planner.py`
- Modify: `services/strategy-service/app/engine/phase_1_21.py`

**Step 1: 准入条件**

只有同时满足才生成 manifest：

- 缺口来自 expected K-line cutoff；
- 不是周末、停牌或无 K 线；
- cutoff 未被任一白名单 run-group 覆盖；
- 精确到 symbol/level/mode/cutoff；
- 不覆盖已有 run；
- run-group 固定为 `phase_1_22_targeted_entry_window_intraday_v1`；
- 不更新 published heads。

**Step 2: 资源估算**

输出 planned runs、按 level 数量、预计 K 线读取量、预计数据库增量和预计运行时间。

本阶段 `execute=false` 必须硬编码并测试。

### Task 10: 下一阶段决策矩阵

**Files:**

- Create: `services/strategy-service/app/engine/phase_1_21_decision.py`
- Create: `services/strategy-service/tests/test_phase_1_21_decision.py`

输出且只能选择一个：

- `A_COVERAGE_GAP_BACKFILL_READY`：存在精确缺口且 manifest/资源估算有效；下一阶段执行受控回填；
- `B_OFFICIAL_TRIGGER_SAMPLE_READY`：覆盖完整且官方独立 entry episode >0；下一阶段补齐 execution/exit 后做小样本回测；
- `C_CANDIDATE_ONLY_REQUIRES_USER_DECISION`：官方为 0，但 `1p`、confirm-time 或 trading-session 窗口出现非零；停止自动循环，提交语义选择给用户；
- `D_STRATEGY_TOO_RESTRICTIVE_REQUIRES_USER_DECISION`：覆盖完整，至少 30 个独立 daily episode、至少 10 个标的，所有并列政策均为 0；停止自动循环，讨论策略假设；
- `E_SAMPLE_UNIVERSE_TOO_SMALL`：少于 30 个独立 daily episode或少于 10 个标的；下一阶段扩大历史可见样本，不改策略；
- `F_DATA_OR_SEMANTIC_BLOCKED`：历史 DIF、first_seen、K 线调整口径等关键事实不可重建；停止并给用户明确决策项。

不得使用模糊的 `mixed` 兜底。

### Task 11: Trace、报告与不可变性验证

**Files:**

- Modify: `services/strategy-service/app/engine/phase_1_21.py`
- Extend: `services/strategy-service/tests/test_phase_1_21.py`

**Step 1: Trace**

至少输出：

- 1 个 coverage complete episode；
- 1 个 coverage gap episode（若无则写 sample unavailable）；
- 1 个 B1 appeared then disappeared；
- 1 个 B1 confirmed；
- 1 个 official blocked but candidate passed；
- 1 个 weekly/daily gate failure；
- 1 个最接近 official trigger 的 episode。

每个 trace 必须含 run_id、cutoff、point_time、first_seen、confirm/disappear、K-line grid 和 gate reason。

**Step 2: 数据库不可变性**

运行前后比较：

- published-head row count；
- run-group counts；
- `phase_1_22_targeted_entry_window_intraday_v1` 必须不存在或计数不变。

**Step 3: 完成报告**

逐项标记已完成/部分完成/未完成，并附命令、SQL、耗时、产物、阻塞。

### Task 12: 全量验证

**Files:**

- Verify only.

运行：

```powershell
cd services/strategy-service
python -m compileall app
python -m pytest tests -q
python -m app.cli.run_phase_1_21 --output-dir outputs/phase-1-21-intraday-grid-signal-lifecycle
python -m pytest tests -q
```

验收：

- 原有 `100` 个测试全部保留通过，新增测试全部通过；
- coverage ratio 全部在 `[0,1]`；
- expected cutoff 不含非交易日伪点；
- 8 个旧 episode 均有逐 episode 根因；
- expanded universe 的 symbol/episode 基数明确；
- lifecycle 能区分 first_seen/confirm/disappear；
- official 与 candidate 输出物理隔离；
- Phase 1.21 数据库前后不变；
- 最终 decision 恰好一个；
- 未达到正式 backtest 条件时禁止输出胜率、收益率或“策略有效”。

## 4. 自动继续与停止条件

完成 Phase 1.21 后，控制代理必须读取 `next_phase_decision.json`：

- A：自动生成 Phase 1.22 受控微回填任务单并执行；
- B：自动生成 Phase 1.22 小样本 entry/exit event replay 任务单并执行；
- E：自动生成 Phase 1.22 样本宇宙扩展任务单，不修改策略；
- C、D、F：立即停止自动执行，向用户提交证据、选项、影响和推荐，不得替用户修改策略语义。

## 5. 最终成功标准

只有同时满足以下条件，才可以称为“策略模型搭建成功”：

- 周线、日线、30F、5F 规则均由无未来泄漏的历史事件账本驱动；
- 市值/上市状态/可交易性/复权口径可在历史时点重建；
- 至少 30 个独立交易样本完成含成本、滑点的 event replay；
- 入场和离场成交价使用下一根对应周期 K 线；
- 参数敏感性不依赖单点最优；
- 至少一次样本外或 walk-forward 验证；
- 所有正式结论与 diagnostic/candidate 结果严格隔离；
- 结果报告明确样本量、年度/市场状态分布、最大回撤、期望、胜率和失败原因。

