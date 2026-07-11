# 模块 C 周线-日线共振二买策略数据契约

## 1. 只读数据源

本策略只允许读取以下模块 C 表：

- `chan_c_runs`
- `chan_c_strokes`
- `chan_c_segments`
- `chan_c_centers`
- `chan_c_signals`
- `scheme2_chan_c_published_heads`

以及基础 K 线/主数据表：

- `symbols`
- `klines`
- `scheme2_ingest_watermarks`
- `chan_level_state_snapshots`
- `chan_cross_level_states`

## 2. 周期编码

| 级别 | DB code |
|---|---:|
| `5f` | 5 |
| `30f` | 30 |
| `1d` | 1440 |
| `1w` | 10080 |
| `1m` | 43200 |

## 3. published head 语义

表：`scheme2_chan_c_published_heads`

关键字段：

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `base_from_bar_end`
- `base_to_bar_end`
- `snapshot_version`
- `run_id`
- `published_at`

约束：

- 当前只保留每个 `(symbol_id, chan_level, mode, base_timeframe)` 的当前头
- 不保留 head 历史链

策略读取规则：

- 当前扫描：直接读 `status='published'` 当前头
- 历史回放：不能只靠 head，必须回退到 `chan_c_runs`

## 4. `chan_c_runs` 语义

表：`chan_c_runs`

关键字段：

- `symbol_id`
- `chan_level`
- `bar_from`
- `bar_until`
- `status`
- `snapshot_version`
- `computed_at`

策略读取规则：

- `as_of_time is null`：优先用当前 published head 对应 `run_id`
- `as_of_time is not null`：使用 `status='success' and bar_until <= as_of_time` 的最新 run

## 5. 笔 / 线段 / 中枢 / 买卖点字段语义

### 5.1 `chan_c_strokes`

- `seq`：run 内顺序
- `start_ts/end_ts`：缠论端点时间
- `begin_base_ts/end_base_ts`：投影到当前基础 K 的端点 bar_end
- `start_price_x1000/end_price_x1000`
- `direction`
- `is_confirmed`

策略使用：

- 日线 `D_a/a` 直接来自日线 strokes
- 30f 内部复杂度评估可读取 30f segments/centers

### 5.2 `chan_c_centers`

- `seq`
- `start_ts/end_ts`
- `begin_base_ts/end_base_ts`
- `low_x1000/high_x1000`
- `is_confirmed`

策略使用：

- 作为最近邻日线中枢候选
- 若无日线中枢，再退化到线段重叠区

### 5.3 `chan_c_signals`

关键字段：

- `ts`
- `base_ts`
- `price_x1000`
- `signal_type`
- `is_confirmed`
- `extra`

`extra` 重要子字段：

- `side`: `buy` / `sell`
- `bsp_type`: `1` / `1p` / `2` / `2s` / `3a` / `3b`
- `features`: chan.py 输出特征字典

策略判断规范：

- 周线二买：`side='buy' and bsp_type='2'`
- 日线一买：`side='buy' and bsp_type='1'`
- 日线二买：`side='buy' and bsp_type='2'`
- 日线类二买：`side='buy' and bsp_type='2s'`
- 30f 一买：`side='buy' and bsp_type='1'`
- 30f 一卖：`side='sell' and bsp_type='1'`
- 5f 二买确认：`side='buy' and bsp_type in ('2','2s')`

## 6. 策略层新增表契约

### 6.1 `strategy_definitions`

用途：

- 存储策略版本和参数 JSON

主键/唯一：

- `UNIQUE(strategy_code, version)`

### 6.2 `strategy_signal_events`

用途：

- 存储候选、观察、触发、入场、退出等事件

最小事件类型：

- `WEEKLY_CONTEXT`
- `DAILY_SETUP`
- `ENTRY_WATCH`
- `ENTRY_TRIGGER`
- `ENTRY_FILLED`
- `EXIT_TRIGGER`
- `EXIT_FILLED`

### 6.3 `strategy_contexts`

用途：

- 存储周线二买上下文与日线 setup 上下文

### 6.4 `strategy_backtest_runs`

用途：

- 存储一次回测任务元信息和聚合指标

### 6.5 `strategy_backtest_trades`

用途：

- 存储逐笔回测交易

## 7. 时间语义契约

### 7.1 `point_time`

- 来源：`coalesce(base_ts, ts)`
- 语义：结构点所处 bar_end
- 用途：结构定位与报告展示
- 禁止直接作为成交时间

### 7.2 `first_seen_time`

当前实现分两种：

- 扫描模式：当前 `as_of_time`
- 回测模式：回放过程中策略条件第一次成立的评估时间

说明：

- 第一版 `event_replay_backtest` 的 `first_seen_time` 是按历史 run / bar 评估重放得到
- 不是来自独立 head 历史表

### 7.3 `confirm_time`

来源：

- 对应信号第一次以 `is_confirmed=true` 被策略层看到的评估时间

## 8. 市值契约

当前项目无现成市值表。

新增可选表：

- `symbol_fundamentals`

第一版策略处理：

- 若有 `symbol_fundamentals.market_cap_x100`
  - 应用 `market_cap_min`
- 若无
  - 标记 `market_cap_filter_applied=false`
  - 不静默假装已过滤

## 9. MACD 契约

输入：

- `klines.close`

参数：

- `fast=12`
- `slow=26`
- `signal=9`

输出：

- `dif`
- `dea`
- `histogram`

第一版不落库，现场计算。

## 10. 顶底分型契约

算法标识：

- `raw_3bar`

底分型：

- `k2.low < k1.low and k2.low < k3.low`

顶分型：

- `k2.high > k1.high and k2.high > k3.high`

确认时间：

- 第三根 K 的 `ts`

## 11. 回测模式契约

### 11.1 `exploratory_static_backtest`

- 使用最终结构做静态逻辑回放
- 用于调参数，不用于正式胜率

### 11.2 `event_replay_backtest`

- 按 30f 时间轴逐步评估
- 每个评估点仅读取 `bar_until <= as_of_time` 的模块 C run
- 该模式结果可作为正式策略评估基准
