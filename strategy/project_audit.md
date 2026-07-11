# 模块 C 周线-日线共振二买策略项目审计

日期：2026-07-07

## 1. 结论摘要

- 当前项目已经具备模块 C 五级别独立缠论结果、五级别 K 线、published head、状态快照、实时 tail 任务等基础设施。
- 当前默认 API/部署配置仍偏向模块 B，不满足“策略统一只读模块 C”的规范要求。
- `chan_c_signals` 已真实落库，`signal_type` 是中文标签，`extra.bsp_type` 才是原始 `1/1p/2/2s/3a/3b` 编码。
- 当前没有可直接用于策略硬过滤的市值主数据表，也没有独立的 MACD 或顶底分型表。
- 当前没有 published head 历史版本表，无法严格从 head 历史直接重建 `first_seen_time`；但可以基于 `chan_c_runs` 成功运行历史做近似事件重放。

## 2. 当前是否统一使用模块 C

结论：没有统一。

证据：

- [services/api/app/core/config.py](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/services/api/app/core/config.py) 默认 `CHAN_STORAGE_NAMESPACE=b`
- [deploy/backend.env](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/deploy/backend.env) 当前 `CHAN_ENGINE_MODE=module_b`
- [deploy/docker-compose.backend.yml](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/deploy/docker-compose.backend.yml) API 默认吃 `CHAN_ENGINE_MODE`

影响：

- 图表默认读 B 还是 C，取决于运行时环境变量，而不是代码硬锁到 C。
- 新策略服务不能依赖 API 默认行为，必须直接查询 `chan_c_*` 与 `scheme2_chan_c_published_heads`。

建议：

- 策略服务第一版完全绕开 API 默认 namespace，直接读模块 C 表。
- 前端/API 是否统一切 C，放到策略服务完成后再单独切换，不在本轮全局改默认行为。

## 3. 模块 C 数据契约核查

### 3.1 `chan_c_signals` 真实字段

来自 [db/sql/019_chan_module_c.sql](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/db/sql/019_chan_module_c.sql) 与数据库 `information_schema.columns`：

- `id bigint`
- `symbol_id integer`
- `chan_level integer`
- `mode smallint`
- `run_id bigint`
- `ts timestamptz`
- `price_x1000 integer`
- `signal_type varchar(32)`
- `is_confirmed boolean`
- `revision integer`
- `base_ts timestamptz`
- `base_seq integer`
- `extra jsonb`
- `created_at timestamptz`

### 3.2 `signal_type` / `bsp_type` 真实语义

代码来源：

- [services/chan-service/chan_service/vendor_chan_adapter.py](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/services/chan-service/chan_service/vendor_chan_adapter.py)

真实生成方式：

- `signal_type`：中文标签，如 `1类买`、`2类买`、`2s类买`
- `extra.side`：`buy` / `sell`
- `extra.bsp_type`：原始编码，如 `1`、`1p`、`2`、`2s`、`3a`、`3b`

数据库实测 top values：

- `2类买 / bsp_type=2`
- `2类卖 / bsp_type=2`
- `2s类买 / bsp_type=2s`
- `2s类卖 / bsp_type=2s`
- `1类买 / bsp_type=1`
- `1类卖 / bsp_type=1`

策略层建议：

- 业务判断统一使用 `extra.bsp_type + extra.side`
- `signal_type` 仅用于展示和报告

### 3.3 `is_confirmed` 语义

代码来源：

- [services/chan-service/chan_service/vendor_chan_adapter.py](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/services/chan-service/chan_service/vendor_chan_adapter.py)

结论：

- `is_confirmed` 来自 chan.py 中结构/买卖点的 `is_sure`
- 在信号层表示“该信号来自已确认结构”，不是“系统生命周期上永不变化”

### 3.4 `ts` 与 `base_ts`

当前模块 C 适配器中：

- `time = base_ts`
- `base_ts = 当前信号所在 K 线 bar_end`

见：

- [services/chan-service/chan_service/vendor_chan_adapter.py](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/services/chan-service/chan_service/vendor_chan_adapter.py)

结论：

- 当前模块 C 信号表里，`ts` 与 `base_ts` 语义在落库阶段基本一致，都是对应周期 K 线结束时间
- 因此第一版策略可将 `point_time` 取 `coalesce(base_ts, ts)`
- 后续若引入更细粒度结构点落位，再扩充事件层 `features_json`

## 4. published head 与历史可重放能力

### 4.1 当前 published head 表

- 当前表：[scheme2_chan_c_published_heads](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/db/sql/019_chan_module_c.sql)
- 唯一键：`(symbol_id, chan_level, mode, base_timeframe)`
- 这意味着它只保留“当前头”，不是历史 head 日志

### 4.2 当前覆盖情况

数据库实测：

- `5f confirmed/predictive`
- `30f confirmed/predictive`
- `1d confirmed/predictive`
- `1w confirmed/predictive`
- `1m confirmed/predictive`

五级别 published head 都已存在，覆盖约 5500 个活跃标的。

### 4.3 是否能重建 `first_seen_time`

严格结论：

- 不能从 `scheme2_chan_c_published_heads` 单表精确重建历史 `first_seen_time`

原因：

- head 行是 scope 内覆盖更新，不保留旧 head 版本

可行近似：

- 使用 `chan_c_runs` 成功运行历史
- 对每个 symbol/level 按 `bar_until` 升序回放
- 在每一版 run 的 `chan_c_signals` 中追踪信号 key 首次出现时间

因此：

- 可以实现 `event_replay_backtest`
- 但它是“按 run 历史近似重放”，不是“按 published head 历史精确回放”

## 5. 市值 / MACD / 分型现状

### 5.1 市值

当前没有现成表：

- `symbols` 只有 `code/exchange/name/asset_type/market/is_active`
- 数据库里没有 `market_cap` 或 `fundamentals` 表

因此：

- 当前无法严格执行“市值 > 100 亿”硬过滤
- 第一版策略服务必须把此能力设计为可选数据源，并在报告中标记是否实际应用

### 5.2 MACD

当前没有独立 MACD 存储表。

因此：

- 策略层需要基于 K 线现场计算 MACD(12,26,9)

### 5.3 顶底分型

当前没有独立顶底分型表。

因此：

- 第一版策略层按规范实现 `raw_3bar` 顶底分型
- 明确写入报告：`fractal_algo=raw_3bar`

## 6. 模块 C 五级别独立计算现状

代码来源：

- [services/chan-service/chan_service/module_c_adapter.py](C:/Users/yangyang/Documents/Codex/2026-06-13/tradingview-tradingview-a-5f-15f-30f/services/chan-service/chan_service/module_c_adapter.py)

关键结论：

- 5f 使用 5f bars 独立计算
- 30f 使用 30f bars 独立计算
- 1d 使用 1d bars 独立计算
- 1w 使用 1w bars 独立计算
- 1m 使用 1m bars 独立计算
- `bi_strict=False`
- 不再从 5f 递归聚合高级别

这与策略规范完全兼容。

## 7. 当前项目与规范的差距

必须补齐：

1. 策略层独立服务与 CLI
2. 策略事件表 / 上下文表 / 回测表
3. 模块 C 仓储层，支持 `as_of_time`
4. 周线上下文分析器
5. 日线第一笔上涨强度分析器
6. 30f/5f 入场确认分析器
7. 离线扫描 CLI
8. 离线回测 CLI
9. 报告输出

本轮不做：

1. 实时提醒
2. 自动交易
3. 前端复杂展示
4. 修改 chan.py
5. 修改模块 C 核心计算规则

## 8. 推荐执行顺序

1. 新增策略数据表与默认策略定义
2. 新增 `services/strategy-service`
3. 先实现 `exploratory_static_backtest`
4. 再实现基于 `chan_c_runs` 的 `event_replay_backtest`
5. 最后再考虑实时策略事件监听
