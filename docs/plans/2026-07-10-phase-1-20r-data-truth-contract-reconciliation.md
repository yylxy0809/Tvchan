# Phase 1.20R 数据真相修复与策略合同收口 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 重新以数据库中的 Module C 历史 run 为事实源，修复多 run-group 事件账本、盘中覆盖审计、独立样本定义和入场评分语义，明确当前 0 trigger 究竟来自数据覆盖缺口还是真实策略阻断。

**Architecture:** 保持 Module C、`chan.py` 和 published heads 完全不变，在 strategy-service 内新增只读的历史事件账本 V2、决策 episode 层和真实 run coverage 审计。旧 Phase 1.1-1.20 产物只作为不可变对照，新实现直接读取 PostgreSQL 的 `chan_c_runs/chan_c_signals`，并将所有结果写入独立的 Phase 1.20R 输出目录。

**Tech Stack:** Python 3.11、asyncpg、PostgreSQL、pytest、JSONL/JSON/Markdown/CSV、现有 strategy-service Module C repository 与 historical backfill 工具。

---

## 0. 阶段定位

Phase 1.20R 不是新策略阶段，而是对 Phase 1.11-1.20 历史回放链路的证据修复阶段。

本阶段必须回答三个问题：

1. Phase 1.17 已写入的 targeted intraday run 是否真正进入了 30F/5F 历史事件账本？
2. Phase 1.20 的 `waiting_for_post_daily_30f_refresh` 是真实信号缺失、历史 run 覆盖不足，还是旧产物没有消费新 run？
3. 当前 `confidence=70` 是否符合冻结设计中“30F B1=40、日线底分型=30、5F B2=30，且 30F B1 必须存在”的正式语义？

本阶段不评价收益，不调整策略阈值，不扩大正式样本，不进入产品化。

## 1. 已知基线与必须复现的事实

实现前先冻结以下输入事实，禁止在代码中硬编码这些数字；数字必须由输入产物或数据库重新统计得到：

- 默认研究标的：10 个。
- Phase 1.10 weekly-context observation：378 条，来自 2 个标的，约 6 个独立 weekly context episode。
- Phase 1.12 candidate observation：171 条，全部来自 `000001.SZ`，对应 8 个独立 daily setup signal episode。
- Phase 1.13 visible 30F observation：72 条。
- Phase 1.17 Micro-backfill V2：计划并成功写入 40 个 run，30F/5F 各 20 个，失败 0 个。
- Phase 1.17 targeted ledger：当前产物包含 4 个信号事件。
- Phase 1.19 post-daily 30F refresh 扫描记录：99 条。
- Phase 1.20 visible eligible refresh：0 条。
- Phase 1.20 当前 coverage audit 中最近 run 字段被固定为 `None`，不是真实数据库审计结果。

## 2. 冻结边界

### 2.1 严格禁止

1. 不修改 `Vespa314/chan.py`。
2. 不修改 Module C 的 K 线处理、分型、笔、线段、中枢、买卖点算法。
3. 不修改 `chan_c_*` 表结构和原始语义。
4. 不写入或切换 `scheme2_chan_c_published_heads`。
5. 不覆盖或删除 `research_daily_close`、Micro V1、Micro V2 历史 run。
6. 不修改旧 Phase 1.1-1.20 输出文件。
7. 不接 API、后台、前端、实时提醒或自动交易。
8. 不执行正式 `strategy_30f smoke`。
9. 不执行 50 标的或全市场历史结构回填。
10. 不使用 `signal_point_time` 替代 `first_seen_time` 触发历史交易。
11. 不因 0 trigger 放宽周线 MACD、日线强度、价格有效性或 stale 规则。
12. 不把 `1p`、B2S 或诊断价格策略静默提升为 official 规则。

### 2.2 允许的写入

默认只允许：

- 新增 strategy-service 源码和测试。
- 写入 `services/strategy-service/outputs/phase-1-20r-data-truth-contract-reconciliation/`。
- 在满足 Task 8 全部准入条件后，向隔离的 `phase_1_20r_targeted_entry_window_intraday_v3` run group 写入历史 run。

即使执行 V3，也必须：

- `run_kind=historical_backfill`；
- 不写 published heads；
- 不覆盖其它 run group；
- 不删除已有 run；
- 不改变 Module C 算法参数。

## 3. 输入与输出

### 3.1 固定输入产物

- `outputs/phase-1-10-daily-signal-visibility/weekly_context_daily_visibility_samples.jsonl`
- `outputs/phase-1-12-daily-setup-decision/daily_setup_sample_audit_v3.jsonl`
- `outputs/phase-1-13-30f-5f-confirmation-ledger/thirty_f_signal_event_ledger.jsonl`
- `outputs/phase-1-13-30f-5f-confirmation-ledger/five_f_signal_event_ledger.jsonl`
- `outputs/phase-1-14-entry-confidence-v3/daily_bottom_fractal_event_ledger.jsonl`
- `outputs/phase-1-17-trigger-window-microbackfill/micro_backfill_v2_manifest.csv`
- `outputs/phase-1-17-trigger-window-microbackfill/micro_backfill_v2_summary.json`
- `outputs/phase-1-17-trigger-window-microbackfill/signal_ledger_after_micro_v2_samples.jsonl`
- `outputs/phase-1-18-staleness-policy/candidate_universe_rebuild.json`
- `outputs/phase-1-19-post-daily-30f-refresh/post_daily_30f_refresh_samples.jsonl`
- `outputs/phase-1-20-30f-refresh-visibility-audit/post_daily_30f_refresh_visibility_gap_samples.jsonl`

### 3.2 数据库事实源

- `symbols`
- `klines`
- `chan_c_runs`
- `chan_c_signals`
- `scheme2_chan_c_published_heads`，只读且仅用于当前态对照

### 3.3 新输出目录

```text
services/strategy-service/outputs/phase-1-20r-data-truth-contract-reconciliation/
```

所有 JSON 文件必须包含：

```text
generated_at
source_artifact_paths
source_run_groups
source_database_tables
strategy_contract_version
future_leakage_detected
```

## 4. 时间与身份合同

### 4.1 时间字段

- `signal_point_time`：结构点所在 K 线，只能用于结构位置和价格检查。
- `first_seen_time`：该 fingerprint 第一次出现在合格历史 run 的 `cutoff_bar_end/bar_until`。
- `confirm_time`：同一 fingerprint 第一次以 confirmed 状态出现的 run cutoff。
- `as_of_time`：策略决策时间。
- `execution_time`：满足触发后下一根可交易 K 线时间。

历史可见性的唯一判断：

```text
signal_point_time <= first_seen_time <= as_of_time < execution_time
```

允许 `signal_point_time < first_seen_time`，但绝不允许用前者提前触发。

### 4.2 Signal fingerprint

```text
symbol_id | chan_level | mode | side | bsp_type |
signal_point_time | price_x1000
```

不同 fingerprint 不得因时间或价格接近而自动合并。

### 4.3 Episode identity

Weekly context episode：

```text
symbol | weekly_signal_fingerprint | weekly_context_first_seen_time
```

Daily setup episode：

```text
weekly_context_episode_id | daily_setup_signal_fingerprint
```

`as_of_time` 只是 episode 的观察点，不是新样本身份。

## 5. Official 与 Candidate 策略合同

### 5.1 Official V1.0

- Weekly：显式 prior B1 + B2，B2 在 B1 后，B2.price > B1.price，未跌破 B1，DIF > 0。
- Daily：D_B1 不早于 W_B1；第一上涨笔强度达到阈值；D_B2/B2S 在 D_B1 后且价格高于 D_B1。
- Entry：有效且 fresh 的 30F B1 必需；底分型和 5F B2/B2S 是附加确认。
- Confidence：30F B1=40，日线底分型=30，5F 确认=30。
- Trigger：confidence >= 70 且包含 fresh 30F B1。

### 5.2 Candidate 分支

允许单独研究：

- trusted weekly B2/B2S；
- self-contained daily B2/B2S；
- 30F `1p`；
- `signal_price_only`；
- extended trigger window。

每条 candidate 输出必须包含 `candidate_policy_id`，不得覆盖 official 字段或 official 汇总。

## 6. 成功标准总览

Phase 1.20R 完成必须同时满足：

1. 真实查询并核对所有相关 run group。
2. Phase 1.17 的 40 个 run 全部可追溯，失败数与 manifest 一致。
3. 多 run-group ledger V2 能解释 Micro V2 的 4 个信号事件。
4. 171 observation 被归并为独立 daily setup episodes，当前固定输入预期为 8 个。
5. 真实 coverage audit 不再输出占位 `None`。
6. `bottom + 5F` 的 confidence 必须为 60，不得为 70。
7. 没有 fresh 30F B1/候选 `1p` 时，不得触发 entry。
8. 5F 确认必须关联到具体 30F fingerprint，且 first-seen 不早于该 30F 信号。
9. 所有触发满足 `first_seen_time <= as_of_time`。
10. 明确给出 Micro-backfill V3 的 `execute / do_not_execute` 证据结论。
11. 若 trigger=0，不执行 candidate micro backtest。
12. 全量 strategy-service 测试通过。

---

### Task 1: 建立 Phase 1.20R 基线清单

**Files:**
- Create: `services/strategy-service/app/engine/phase_1_20r.py`
- Create: `services/strategy-service/app/cli/run_phase_1_20r.py`
- Test: `services/strategy-service/tests/test_phase_1_20r.py`

**Step 1: 写失败测试**

测试输入文件均存在，且旧产物摘要可读取；缺任何必需输入时必须失败并列出缺失路径。

**Step 2: 运行测试确认失败**

```powershell
cd services/strategy-service
python -m pytest tests/test_phase_1_20r.py::test_preflight_requires_all_source_artifacts -q
```

Expected: FAIL，原因是 `phase_1_20r` 尚不存在。

**Step 3: 最小实现**

新增：

```python
def build_preflight_manifest(source_paths: list[Path]) -> dict:
    ...
```

输出每个文件的绝对路径、大小、mtime、SHA-256、行数和 schema keys。

**Step 4: 验证**

输出：

- `source_artifact_manifest.json`
- `source_artifact_manifest.md`

验收：旧文件零改动；manifest 覆盖全部固定输入。

### Task 2: 固化策略语义合同

**Files:**
- Create: `services/strategy-service/app/contracts/weekly_daily_b2_contract.py`
- Create: `services/strategy-service/app/contracts/weekly_daily_b2_contract_v1.json`
- Test: `services/strategy-service/tests/test_weekly_daily_b2_contract.py`

**Step 1: 写失败测试**

至少覆盖：

```python
assert score(thirty_f=True, bottom=False, five_f=False) == 40
assert score(thirty_f=False, bottom=True, five_f=True) == 60
assert score(thirty_f=True, bottom=True, five_f=False) == 70
assert score(thirty_f=True, bottom=False, five_f=True) == 70
assert can_trigger(score=100, fresh_thirty_f=False) is False
```

**Step 2: 最小实现**

提供具名权重和 `require_30f_b1=true`，禁止使用 confirmation count 推导权重。

**Step 3: 验收**

- official/candidate 配置分别序列化。
- official 不接受 `1p` 替代 B1。
- candidate 可配置接受 `1p`，但输出明确标签。
- 现有 `EntryConfidenceEvaluator` 与合同测试一致。

### Task 3: Observation 归并为独立 Episode

**Files:**
- Create: `services/strategy-service/app/engine/strategy_episode_builder.py`
- Test: `services/strategy-service/tests/test_strategy_episode_builder.py`

**Step 1: 写失败测试**

覆盖：

- 同一 daily signal 在 90 个 `as_of_time` 出现，只生成 1 个 episode。
- 不同 daily signal fingerprint 生成不同 episode。
- 同一日线信号处于不同 weekly context 时不得误合并。
- observation 仍保留并通过 `episode_id` 关联。

**Step 2: 实现**

新增：

```python
def build_weekly_context_episodes(rows: list[dict]) -> list[dict]:
    ...

def build_daily_setup_episodes(rows: list[dict]) -> list[dict]:
    ...
```

**Step 3: 输出与验收**

- `weekly_context_episodes.jsonl`
- `daily_setup_episodes.jsonl`
- `observation_to_episode_map.csv`
- `episode_cardinality_audit.md/json`

当前固定输入验收：

- 378 weekly observations 全部有 episode_id。
- 171 candidate observations 全部有 episode_id。
- 171 candidate observations 归并为 8 个 daily setup episodes。
- 不在代码中硬编码 378、171、8。

### Task 4: 从数据库重建多 Run-group Signal Event Ledger V2

**Files:**
- Modify: `services/strategy-service/app/repositories/module_c_repo.py`
- Create: `services/strategy-service/app/engine/multi_run_group_signal_ledger.py`
- Test: `services/strategy-service/tests/test_multi_run_group_signal_ledger.py`

**Step 1: 写失败测试**

覆盖：

- 同 fingerprint 在多个 run group 出现时，`first_seen_time` 取最早合格 cutoff。
- `last_seen_time` 取最后观察 cutoff。
- source run ids 和 run groups 完整保留。
- 不合格 status、错误 mode、非 historical_backfill run 被排除。
- 同 point_time 但不同 price 不合并。
- `first_seen_time` 不得早于 run cutoff 所允许的市场时间。

**Step 2: 集合化查询**

一次查询读取目标 symbol、level、mode、run group 范围内的 run 与 signal，避免 per-sample 查询。

允许 run group：

```text
research_daily_close
phase_1_15_targeted_entry_window_intraday
phase_1_16_targeted_entry_window_intraday_v2
phase_1_20r_targeted_entry_window_intraday_v3（仅执行后）
```

**Step 3: 输出与验收**

- `signal_event_ledger_v2_30f.jsonl`
- `signal_event_ledger_v2_5f.jsonl`
- `signal_event_ledger_v2_summary.md/json`
- `run_group_contribution.csv`
- `ledger_v1_v2_diff.md/json`

必须核对：

- Micro V2 manifest 的 40 个 run 均能在数据库找到。
- targeted V2 run 中 30F/5F 各 20 个。
- 失败 run 数为 0，或与数据库真实状态一致并逐条报告。
- 当前固定输入中 4 个 targeted signal 均进入 Ledger V2。

### Task 5: 实现真实 Intraday Run Coverage Audit V2

**Files:**
- Create: `services/strategy-service/app/engine/intraday_run_coverage_audit_v2.py`
- Test: `services/strategy-service/tests/test_intraday_run_coverage_audit_v2.py`

**Step 1: 写失败测试**

覆盖：

- 已知 manifest run 必须返回真实 run_id 和 cutoff。
- 没有 run 时才允许 `None`，且必须带 `missing_reason`。
- 不能由 signal 是否可见反推 run 是否存在。
- 30F 和 5F 覆盖分别计算。
- coverage window 使用 episode，而不是每个 observation 重复计算。

**Step 2: 实现查询**

每个 episode 输出：

```text
episode_id
symbol
daily_setup_first_seen_time
trigger_window_start/end
nearest_30f_run_before_setup
first_30f_run_after_setup
last_30f_run_before_window_end
first_30f_run_after_window_end
30f_run_count_inside_window
5f_run_count_inside_window
covered_30f_cutoff_count
covered_5f_cutoff_count
expected_cutoff_count
coverage_ratio
run_groups_seen
coverage_classification
```

分类必须是：

```text
fully_covered
partially_covered
not_covered
covered_but_no_signal
signal_exists_but_first_seen_after_as_of
stale_signal_only
```

**Step 3: 输出与验收**

- `intraday_run_coverage_gap_audit_v2.md/json`
- `intraday_run_coverage_gap_samples_v2.jsonl`
- `intraday_run_coverage_by_episode.csv`
- `intraday_run_coverage_by_run_group.csv`

验收红线：已在 Micro V2 manifest 中存在的窗口不得全部显示为 `None`。

### Task 6: 重建 Post-daily 30F Refresh Visibility Audit V2

**Files:**
- Create: `services/strategy-service/app/engine/post_daily_refresh_visibility_v2.py`
- Test: `services/strategy-service/tests/test_post_daily_refresh_visibility_v2.py`

**Step 1: 写失败测试**

覆盖：

- refresh 必须满足 `30f.first_seen_time > daily_setup.first_seen_time`。
- refresh 必须满足 `30f.first_seen_time <= as_of_time` 才可见。
- point_time 在 setup 后但 first_seen 在 as_of 后，只能 diagnostic。
- setup 前的 30F 信号归类 stale，不可刷新。
- 同一 refresh 对同一 episode 只计一次。

**Step 2: 实现**

直接使用 Task 3 episode 和 Task 4 Ledger V2，不读取 Phase 1.13 旧 ledger 作为运行输入。

**Step 3: 输出与验收**

- `post_daily_30f_refresh_visibility_v2.md/json`
- `post_daily_30f_refresh_visibility_samples_v2.jsonl`
- `post_daily_30f_refresh_visibility_by_reason.csv`
- `phase_1_19_20_refresh_diff.md/json`

必须解释旧 99 条扫描记录中的每一条：

- 是否仍存在于 V2；
- 对应哪个 episode；
- 对应哪个 run/run group；
- 是否历史可见；
- 若不可见，具体原因是什么。

### Task 7: Entry State Machine V4 与 Confidence V7

**Files:**
- Create: `services/strategy-service/app/engine/entry_state_machine_v4.py`
- Test: `services/strategy-service/tests/test_entry_state_machine_v4.py`
- Modify: `services/strategy-service/app/engine/phase_1_20r.py`

**Step 1: 写失败测试**

最少覆盖：

1. `bottom + 5F` 得 60 分，不触发。
2. `fresh 30F + bottom` 得 70 分，可进入触发候选。
3. `fresh 30F + 5F` 得 70 分，可进入触发候选。
4. stale 30F + bottom + 5F 不触发。
5. 5F 信号早于其父 30F 信号，不计确认。
6. first_seen 晚于 as_of，不触发。
7. trigger window 结束后首次看到信号，不触发。
8. official 不接受 `1p`；candidate 可接受但必须标记。
9. 状态只能单向按时间推进，不能读取未来 transition。

**Step 2: 实现状态**

```text
WAIT_WEEKLY_CONTEXT
WEEKLY_CONTEXT_ACTIVE
DAILY_SETUP_ACTIVE
WAIT_FRESH_30F
FRESH_30F_VISIBLE
WAIT_SECOND_CONFIRMATION
ENTRY_ELIGIBLE
ENTRY_TRIGGERED
BLOCKED_STALE
EXPIRED
```

**Step 3: 输出与验收**

- `entry_state_machine_v4_spec.md`
- `entry_state_machine_v4_dry_run.md/json`
- `entry_state_machine_v4_samples.jsonl`
- `entry_state_machine_v4_transitions.csv`
- `entry_confidence_v7_distribution.md/json`

official 和 candidate 必须分别输出，禁止合并统计。

### Task 8: Micro-backfill V3 准入决策与 Dry-run

**Files:**
- Create: `services/strategy-service/app/engine/micro_backfill_v3_planner.py`
- Test: `services/strategy-service/tests/test_micro_backfill_v3_planner.py`

**Step 1: 准入规则测试**

只有全部满足才允许 `execute=true`：

1. Task 5 证明存在 `partially_covered/not_covered` episode。
2. 缺失窗口不是 stale 语义导致。
3. 目标按 episode 去重。
4. symbol、level、cutoff、预计 run 数明确。
5. run group 唯一隔离。
6. 不写 published heads。
7. 不覆盖已有 cutoff run。
8. 预计资源消耗在本机允许范围内。

**Step 2: 生成计划**

- `micro_backfill_v3_decision.md/json`
- `micro_backfill_v3_execution_plan.md/json`
- `micro_backfill_v3_manifest.csv`
- `micro_backfill_v3_resource_estimate.md/json`

manifest 必须按唯一 `symbol + level + cutoff_bar_end` 去重，不得按 171 observation 重复生成。

**Step 3: 阶段分支**

- `execute=false`：停止，不写数据库，进入 Task 10。
- `execute=true`：经计划验证后执行 Task 9。

### Task 9: 条件执行 Micro-backfill V3

**Files:**
- Reuse: `services/strategy-service/app/engine/module_c_history_backfill.py`
- Reuse: `services/strategy-service/app/cli/run_module_c_history_backfill.py`
- Modify only if required: corresponding CLI argument validation
- Test: existing backfill isolation tests plus new V3 manifest test

**Step 1: Dry-run 验证**

确认：

- 预计 run 数等于去重 manifest 行数；
- 没有 published-head 写操作；
- 没有其它 run-group 冲突；
- G 盘和数据库可用。

**Step 2: 执行**

```text
run_group_id=phase_1_20r_targeted_entry_window_intraday_v3
run_kind=historical_backfill
```

**Step 3: 验收**

- `written + skipped_existing + failed = planned`。
- `failed=0`；否则输出逐条失败并停止后续回测。
- published head 写入数为 0。
- research_daily_close 被覆盖数为 0。
- 重新执行 Task 4-7，不允许沿用 V3 前账本。

### Task 10: Candidate-only Micro Backtest 准入判断

**Files:**
- Create: `services/strategy-service/app/engine/candidate_micro_backtest_gate_v3.py`
- Test: `services/strategy-service/tests/test_candidate_micro_backtest_gate_v3.py`

只有同时满足才允许回测：

```text
independent_entry_episode_count > 0
future_leakage_detected = false
all_trigger_traces_complete = true
fresh_30f_required = true
official_candidate_isolation_passed = true
execution_bar_available = true
```

如果不满足，只输出：

- `candidate_micro_backtest_decision_v3.md/json`
- `candidate_micro_backtest_block_reasons.csv`

如果满足，只允许执行 candidate-only micro backtest，且必须标记：

```text
research_only=true
official_strategy_evaluation=false
sample_size_insufficient=true（独立交易少于30时）
```

任何 observation 数量都不能替代 independent episode 数量。

### Task 11: 股票池与正式扩容准备审计

**Files:**
- Create: `services/strategy-service/app/engine/formal_universe_readiness_audit.py`
- Test: `services/strategy-service/tests/test_formal_universe_readiness_audit.py`

审计但不补数据：

- 当前市值覆盖率；
- 历史时点市值可用性；
- 历史上市/退市状态可用性；
- K 线复权口径；
- 停牌、涨跌停、下一根开盘可成交性；
- 手续费与滑点模型；
- 当前 active universe 带来的幸存者偏差。

输出：

- `formal_universe_readiness_audit.md/json`
- `formal_backtest_blockers.csv`

只要历史时点股票池和市值条件未满足，不得称为正式全市场回测。

### Task 12: Trace、总报告与任务单对照报告

**Files:**
- Modify: `services/strategy-service/app/engine/phase_1_20r.py`

必须输出：

- `trace_index.md`
- `traces/*.md`
- `phase_1_20r_summary.md/json`
- `phase_1_20r_decision_report.md/json`
- `phase_1_20r_detailed_completion_report.md`
- `phase_1_20r_task_checklist_report.md`
- `phase_1_20r_old_vs_new_conclusion_matrix.md/json`

Trace 至少覆盖：

1. Micro V2 run 存在且信号被 Ledger V2 消费。
2. run 存在但没有新 30F 信号。
3. point_time 合格但 first_seen 晚于 as_of。
4. stale 30F 被正确阻断。
5. bottom+5F=60 被正确阻断。
6. fresh 30F+第二确认的样本；若不存在，明确写 `sample_not_available`。

最终决策必须是以下之一：

```text
A. coverage_gap_confirmed_execute_micro_v3
B. coverage_complete_no_fresh_30f_signal
C. mixed_coverage_and_time_semantics
D. fresh_candidate_trigger_found_allow_micro_backtest
E. implementation_defect_requires_repair
```

## 7. 测试与验证命令

### 7.1 定向测试

```powershell
cd services/strategy-service
python -m pytest tests/test_phase_1_20r.py -q
python -m pytest tests/test_weekly_daily_b2_contract.py -q
python -m pytest tests/test_strategy_episode_builder.py -q
python -m pytest tests/test_multi_run_group_signal_ledger.py -q
python -m pytest tests/test_intraday_run_coverage_audit_v2.py -q
python -m pytest tests/test_post_daily_refresh_visibility_v2.py -q
python -m pytest tests/test_entry_state_machine_v4.py -q
python -m pytest tests/test_micro_backfill_v3_planner.py -q
python -m pytest tests/test_candidate_micro_backtest_gate_v3.py -q
```

### 7.2 全量测试

```powershell
python -m compileall app
python -m pytest tests -q
```

### 7.3 运行 Phase 1.20R

```powershell
python -m app.cli.run_phase_1_20r `
  --output-dir outputs/phase-1-20r-data-truth-contract-reconciliation
```

### 7.4 数据库只读验收

执行前后记录：

- `scheme2_chan_c_published_heads` 行数与 head 指向；
- `research_daily_close` run 数；
- Micro V1/V2 run 数；
- V3 run 数（仅条件执行时变化）；
- 其它 run group 行数。

除隔离 V3 run 外，其余必须完全不变。

## 8. 阶段出口

### 8.1 可以进入受控 Micro-backfill V3

仅当真实 coverage audit 证明缺少必要 cutoff run，且目标窗口已按独立 episode 去重。

### 8.2 可以进入 Candidate-only Micro Backtest

仅当至少出现 1 个满足完整时间合同、fresh 30F 必需条件、可执行下一根 K 线的独立 entry episode。

少于 30 个独立交易时只能做链路冒烟，不得评价胜率或策略有效性；建议达到 100 个以上独立交易后再讨论统计表现。

### 8.3 不可以进入正式回测

以下任一未完成都不得进入：

- 历史时点市值与股票池；
- 足够的独立样本；
- 成交可实现性；
- 手续费和滑点；
- 无未来函数证明；
- official/candidate 语义隔离。

### 8.4 0 trigger 的处理

如果真实覆盖完整但仍没有 fresh 30F 信号，应接受为当前样本的真实结果，停止在这 10 个标的上继续叠加微回填。下一阶段应扩展候选宇宙或由策略所有者重新确认 official 口径，不能由开发 Agent自行放宽。

## 9. 最终完成定义

本任务不是以“产出交易”为完成标准，而是以“每个结论都能追溯到数据库 run、事件 fingerprint、episode 和 as_of 时间线”为完成标准。

Phase 1.20R 只有在以下全部成立时完成：

- 数据来源真实；
- 样本身份独立；
- first_seen 语义正确；
- 评分符合冻结合同；
- 覆盖缺口结论不是占位值；
- official 与 candidate 隔离；
- 所有测试通过；
- 阶段出口明确且没有越权执行。
