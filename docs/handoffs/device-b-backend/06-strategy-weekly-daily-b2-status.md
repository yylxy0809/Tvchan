# 缠论周线-日线共振二买策略与生命周期现状

## 1. 正式策略规则

当前策略代码为 `weekly_daily_b2_resonance_v1`。用通俗话概括：

1. 先在周线寻找明确的周线一买，再出现周线二买。
2. 周线 MACD DIF 必须大于 0。
3. 在周线背景之后，日线必须出现严格的日线一买。
4. 日线第一段向上力度评分至少 70。
5. 日线二买/类二买的位置不能破坏日线一买价格结构。
6. 进入观察后，必须出现新鲜的 30F 一买。
7. 入场信心分：30F 一买 40 分、日线底分型 30 分、验证过父关系的 5F 二买/类二买 30 分。
8. 总分至少 70，且正式合同要求存在 30F 一买。
9. 执行价使用下一根 30F K 线开盘价。

离场规则：日线收盘跌破日线一买参考价，或出现 30F 一卖、日线顶分型、周线顶分型。

## 2. 正式与诊断必须分开

项目有多种放宽口径，用于排查“为什么没有候选”，例如允许周线 2s、不要求 DIF>0、降低力度阈值或放宽 30F 条件。这些只能标记为 diagnostic/research_only，不能混入正式样本、正式 trace 或正式回测。

## 3. 三种时间

### point_time

缠论结构对应哪根 K 线。例如某个二买落在 2025-03-10。这是“结构位置”，不代表当时已经能确认。

### first_seen_time

系统第一次在当时的计算结果里看到这个结构。历史回测能否交易，主要看这个时间。

### confirm_time

结构从预测变成确认的时间。正式回测不能在 confirm_time 之前使用“已确认”身份。

另有 disappear_time，用于记录结构后来被修正消失。只有最新 Module C 结果无法恢复这些时间，必须保留 run/head 历史。

## 4. 现有数据库承载

- `chan_c_signals`: 保存结构位置、价格、side/bsp_type 和确认状态，不保存完整生命周期。
- `strategy_signal_events`: 已设计 point/first_seen/confirm/disappear 字段，用于策略事件账本。
- `scheme2_chan_c_published_head_history`: 计划记录每次 published head 变化，是重建生命周期的基础。
- Phase 1.21 的 JSONL ledger：当前研究性历史重建产物，不等于所有全市场数据都已永久入库。

迁移 022 中 `snapshot_version` 定义为 bigint，而当前 Module C snapshot 是字符串，必须在 B 新库迁移前改为 `varchar(255)` 或明确改存独立 sequence。否则 observer 可能写入失败或丢失身份。

## 5. Phase 1.21 最新结果

2026-07-10 真实只读审计产物显示：

- official eligible symbols：0。
- observable symbols：13。
- diagnostic symbols：13。
- official daily episodes：0。
- official triggers：0。
- diagnostic candidate triggers：1。
- 诊断缺失 cutoff：168，manifest 全部 `execute=false`。
- 下一决策：`E_SAMPLE_UNIVERSE_TOO_SMALL`。
- 最大上游阻塞：5 个周线样本全部未通过 `weekly_dif_gt_zero`。

这意味着策略软件链路已能解释失败原因，但还不能证明策略有效。当前没有足够的、满足正式数据可见性合同的样本做可信收益评估。

## 6. 为什么旧缠论数据无需因缺生命周期而全部删除

缺少 first_seen/confirm 并不代表笔、中枢、买卖点结构本身无用；它们仍可用于当前画图和截面分析。但旧结果不能直接作为精确历史回测证据。

本轮恰好因为 `bi_allow_sub_peak` 要从允许改为不允许而必须全量重算，所以最合理的做法是：新结构全量计算与生命周期观察同时上线，不再为旧结构补一套不一致的历史。

## 7. 推荐生命周期架构

### 结构身份

为每个 signal/stroke/center 生成稳定 fingerprint，至少包含：symbol、level、mode-independent 类型、方向/side、端点时间、整数价格、算法 config_hash。不要使用数据库自增 id 作为跨 run 身份。

### 发布观察

每次 published head 原子切换后，在同一事务或可靠 outbox 中记录：old_run、new_run、published_at、snapshot_version、config_hash。

### 差分事件

比较相邻 run：

- 新出现 -> first_seen。
- predictive 保持同身份后 confirmed -> confirm。
- 已存在后缺失 -> disappear。
- 价格/端点变化 -> 旧身份 disappear + 新身份 first_seen，不覆盖旧历史。

### 策略事件

策略层只消费生命周期 ledger 的“截至 as_of_time 已可见”事件，生成周线 context、日线 setup、30F/5F trigger 和 exit。不能直接查询最新 published head 来假装历史状态。

## 8. 重算与策略同步顺序

1. 冻结 effective Module C config 和 config_hash。
2. 完成五级别 K 线数据质量验收。
3. 建立全量 baseline run；baseline 的历史结构只知道 point_time，first_seen 应标记为 baseline_observed_at，不能伪装成历史精确可见时间。
4. 从 baseline 之后，所有 published head 变化实时记录精确 first_seen/confirm/disappear。
5. 若需要过去几年正式回测，必须按历史 cutoff 重放 K 线逐步计算，生成事件账本；不能仅依赖一次全量最终结果。
6. 策略正式回测只使用生命周期完整的区间；其余区间标记 diagnostic。

## 9. 策略有效性的验收标准

在满足以下条件前，不得宣布策略有效：

- 至少有足够多个年份、不同市场环境的官方可用样本。
- point/first_seen/confirm 无未来数据泄漏。
- 30F 与 5F 父子关系、价格约束均可追溯。
- 交易使用下一根可成交 K 线，含停牌/涨跌停处理。
- 输出交易数、胜率、平均收益、profit factor、最大回撤和持有期。
- 与简单基准和放宽口径对照，结果不由少数股票驱动。
- 样本外或滚动验证仍成立。

当前进度属于“策略规则和诊断基础设施较完整，正式数据集不足”，不是“策略已经成功”。
