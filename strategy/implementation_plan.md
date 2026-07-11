# 模块 C 周线-日线共振二买策略实现计划

## 1. 本轮实现范围

本轮严格按规范做后端离线能力：

1. 审计与数据契约
2. 策略数据库表
3. 模块 C 仓储层
4. 周线/日线/30f/5f 分析器
5. 离线扫描 CLI
6. 离线回测 CLI
7. CSV / JSON / Markdown 报告

本轮不做：

1. 实时提醒
2. 前端复杂交互
3. 自动交易

## 2. 代码目录

新增：

- `services/strategy-service/app/config/strategy_params.py`
- `services/strategy-service/app/domain/enums.py`
- `services/strategy-service/app/domain/models.py`
- `services/strategy-service/app/repositories/module_c_repo.py`
- `services/strategy-service/app/repositories/kline_repo.py`
- `services/strategy-service/app/repositories/strategy_repo.py`
- `services/strategy-service/app/analyzers/*.py`
- `services/strategy-service/app/engine/strategy_runner.py`
- `services/strategy-service/app/backtest/*.py`
- `services/strategy-service/app/cli/run_scan.py`
- `services/strategy-service/app/cli/run_backtest.py`
- `services/strategy-service/tests/*.py`

新增迁移：

- `db/sql/021_strategy_weekly_daily_b2.sql`

## 3. 实现顺序

### Step 1

先落数据库迁移和默认策略定义。

### Step 2

实现模块 C 仓储：

- 当前 published head 读取
- `as_of_time` 历史 run 读取
- signals / strokes / centers 查询

### Step 3

实现基础分析器：

- MACD
- raw_3bar 分型
- center query
- weekly context
- daily setup
- entry confidence
- exit evaluator

### Step 4

实现扫描器：

- 单 symbol 评估
- 多 symbol 扫描
- 结果写入 `strategy_signal_events` / `strategy_contexts`

### Step 5

实现回测器：

- `exploratory_static`
- `event_replay`
- 逐笔交易表落库
- 指标汇总

### Step 6

补测试和报告输出。

## 4. 关键实现约束

1. 只读模块 C
2. 不改 chan.py
3. 不改模块 C 计算逻辑
4. 不把策略字段塞进 `chan_c_signals`
5. 交易触发优先用回放时刻，不直接拿 `point_time` 成交

## 5. 验证顺序

1. 单元测试先跑分析器
2. 再跑仓储/报告测试
3. 最后用单 symbol CLI 冒烟
