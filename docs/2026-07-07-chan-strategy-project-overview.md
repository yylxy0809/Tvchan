# 缠论选股入场策略前置说明（基于当前项目实际情况）

## 1. 文档目的

这份文档用于承接你在右侧内置浏览器里与 GPT 的讨论内容，并把那套“缠论选股入场策略”思路，落到**当前项目真实代码、真实数据库结构、真实缠论计算链路**上。

目标不是现在就把策略写完，而是先把下面几件事讲清楚：

1. 当前项目到底已经具备了什么。
2. 当前数据库里到底存了哪些和策略相关的数据。
3. 模块 B、模块 C 的缠论结果在语义上有什么差异。
4. 你想做的“自定义策略模型”应该接在哪一层。
5. 后续继续和 GPT / Codex 讨论时，哪些地方必须先统一口径。

说明范围：

- 以当前工作区代码为准。
- 以当前后端 Docker 配置为准。
- 以当前数据库 schema 和 collector / api / chan-service 实现为准。
- 结合你在浏览器中与 GPT 的对话大意进行归纳。

---

## 2. 右侧浏览器对话内容归纳

根据当前浏览器会话内容，你想做的不是“重写缠论算法”，而是在本项目现有缠论结果基础上，构建一个**自定义缠论选股/入场策略模型**。

目前讨论出来的策略方向，大意是：

1. 周线与日线共振优先。
   - 周线二买（B2）成立。
   - 日线二买（B2）成立。
   - 周线二买要偏强。

2. 周线二买时，周线 MACD DIF 需要在零轴上方。

3. 日线级别中：
   - 日线一买之后的第一笔向上要强。
   - 这笔上攻最好进入或突破最近相关中枢。
   - 日线二买可以是标准二买，也可以是类二买。
   - 但日线二买低点不能跌破对应日线一买。

4. 真正入场触发放到更低级别：
   - 在日线二买区域里，优先等 30f 一买。
   - 如果没有合适的 30f，再考虑 5f 一买。
   - 优先级：30f > 5f。

5. 离场思路：
   - 主离场：30f 走势完成后出现 30f 一卖。
   - 可选离场：周线顶分型，或日线二买后向上笔结束后的日线顶分型。
   - 失败止损：跌破日线一买。

6. 当前阶段目标：
   - 先做单次入场、单次出场。
   - 先验证胜率和策略可解释性。
   - 暂不追求复杂仓位管理和资金管理。

7. 讨论里还提到了一个非常关键的问题：
   - 策略模型里的“信号时间”到底怎么定义。
   - 至少要区分：
     - `point_time`：该买卖点对应结构点本身所在时间。
     - `first_seen_time`：该信号第一次在系统里出现的时间。
     - `confirm_time`：该信号真正稳定确认、可用于实盘的时间。

这件事对后续“回测是否作弊”“实盘是否前视”至关重要。

---

## 3. 当前项目总体架构

当前项目不是单体脚本，而是一个分层系统。可以简单理解为：

1. **前端**
   - TradingView 图表界面。
   - 问财选股、缠论选股、关注列表、后台配置等都在前端页面里。

2. **API 服务**
   - 对前端暴露 `/api/...`。
   - 负责读取数据库里的 K 线、缠论结果、选股状态、问财结果、LLM 配置等。

3. **chan-service**
   - 对 `Vespa314/chan.py` 做适配封装。
   - 负责把本项目内部 bar 数据喂给 `chan.py`，再把输出转成统一 JSON 结构。

4. **collector**
   - 负责行情采集、历史导入、增量补采、模块 B / 模块 C 缠论计算、实时 tail 计算等。

5. **Postgres / TimescaleDB**
   - 存 symbol 主数据、K 线、缠论结构、发布头、任务队列、水位、状态快照等。

6. **Redis**
   - 用于实时推送和事件分发。

---

## 4. 当前部署与运行配置要点

从当前代码和配置看，部署上主要有这些服务：

1. `tv_backend_timescaledb`
   - PostgreSQL + TimescaleDB。

2. `tv_backend_api`
   - API 服务，默认端口 `8001`。

3. `tv_backend_chan_service`
   - `chan.py` 适配服务，默认端口 `8002`。

4. `tv_backend_redis`
   - Redis。

5. `tv_backend_web_gateway`
   - 网关 / 静态网页。

当前 `deploy/backend.env` 里的关键配置事实：

1. `USE_SEED_DATA=false`
   - 说明 API 默认读数据库，不是 seed/mock。

2. `CHAN_ENGINE_MODE=module_b`
   - 当前 chan-service 默认 live 引擎还是模块 B。

3. `CHAN_STORAGE_NAMESPACE` 没有在 `deploy/backend.env` 显式设置。
   - API 配置默认值是 `b`。
   - 也就是说：**API 现在默认优先读取模块 B 的预计算缠论表，而不是模块 C。**

4. `POSTGRES_CHAN_C_TABLESPACE_HOST_PATH=G:/tv-a-share-db/postgres-tablespaces`
   - 模块 C 相关大表已经按你的要求迁到 G 盘 tablespace。

这意味着一个非常重要的现实问题：

### 当前项目里“模块 C 已经算出来”和“前端/API 正在读模块 C”不是同一件事

如果只是模块 C 在后台跑、把结果写进 `chan_c_*` 和 `scheme2_chan_c_*`，但 API 还是读 `storage_namespace=b`，那么：

- 前端图上看到的可能还是模块 B。
- 选股策略如果直接接 API，也可能读到模块 B。

这是后续策略落地时必须先统一的第一件事。

---

## 5. 当前数据库主干结构

## 5.1 `symbols`

基础证券主数据表。

主要字段：

- `id`
- `code`
- `exchange`
- `name`
- `asset_type`
- `market`
- `is_active`

用途：

- 全项目所有行情、缠论、任务队列、状态快照都以 `symbol_id` 为主键关联。
- 你已经明确要求：后续只保留和处理 `is_active=true` 的活跃标的。

---

## 5.2 `klines`

这是全项目所有 K 线的统一事实表。

主要字段：

- `symbol_id`
- `timeframe`
- `ts`
- `open_x1000`
- `high_x1000`
- `low_x1000`
- `close_x1000`
- `volume`
- `amount_x100`
- `is_complete`
- `revision`
- `source`

关键点：

1. `timeframe` 用分钟编码：
   - `5=5f`
   - `15=15f`
   - `30=30f`
   - `60=1h`
   - `1440=1d`
   - `10080=1w`
   - `43200=1m（月线，不是1分钟）`

2. `source` 表示来源优先级，当前代码里已支持：
   - `1=seed`
   - `2=pytdx`
   - `3=tdx_csv`
   - `4=parquet_5f`
   - `5=mootdx`
   - `6=tencent`
   - `7=baidu`
   - `8=derived_5f`
   - `9=parquet_native`

3. API 读 K 线时，会优先读真实来源：
   - `[2,3,4,5,6,7,8,9]`
   - 不够时才回落到 `seed`

4. 对策略模型来说，`klines` 是最底层事实源。
   - 不管策略最后基于 B 还是 C，最终都还是要回到这张表验证。

---

## 5.3 Scheme2 运行时水位与任务表

### `scheme2_ingest_watermarks`

作用：

- 记录每个标的、每个周期的 K 线已经补到哪里。

核心字段：

- `symbol_id`
- `timeframe`
- `last_bar_end`
- `source`
- `updated_at`

对策略的意义：

- 可以用来判断某个标的/周期数据是否新鲜。
- 做实盘策略前，必须先验证相关周期水位是否到位。

### `scheme2_market_fetch_tasks`

作用：

- 实时/补采行情抓取任务队列。

核心字段：

- `symbol_id`
- `timeframe`
- `status`
- `priority`
- `next_run_at`
- `target_bar_end`
- `claim_token`
- `lease_until`
- `shard_bucket`

说明：

- 这是多 worker 抢任务、续租、防旧 worker 回写的基础。

### `scheme2_market_fetch_attempts`

作用：

- 记录一次抓取尝试的来源、耗时、成败、胜出源。

### `scheme2_market_candidate_bars`

作用：

- 留存异常候选 bar。
- 主要用于多源冲突、fallback、质量失败排查。

对策略的意义：

- 如果某些策略结果很反常，后续可以用这些表追溯是不是底层行情源冲突导致的。

---

## 5.4 模块 B 缠论表

模块 B 仍然是当前项目里的旧主链之一。

核心表：

- `chan_runs`
- `chan_strokes`
- `chan_segments`
- `chan_centers`
- `chan_signals`
- `scheme2_chan_published_heads`
- `scheme2_chan_recompute_watermarks`
- `scheme2_chan_tail_tasks`

语义特点：

- 模块 B 本质上是以 `5f` 为基础，再递归构造 `30f`、`1d`。
- 它更接近你最早想要的“低级别递归出高级别”的路线。

---

## 5.5 模块 C 缠论表

模块 C 是你后来要求建立的新链路。

核心表：

- `chan_c_runs`
- `chan_c_strokes`
- `chan_c_segments`
- `chan_c_centers`
- `chan_c_signals`
- `scheme2_chan_c_published_heads`
- `scheme2_chan_c_recompute_watermarks`
- `scheme2_chan_c_tail_tasks`

这些表和模块 B 对应，但语义不同。

### `chan_c_runs`

记录模块 C 每次运行。

主要字段：

- `symbol_id`
- `chan_level`
- `mode`
- `input_signature`
- `config_hash`
- `bar_from`
- `bar_until`
- `bar_count`
- `status`
- `snapshot_version`
- `computed_at`

流式增量扩展字段：

- `run_kind`
- `parent_run_id`
- `expected_head_run_id`
- `run_group_id`
- `anchor_bar_end`
- `cutoff_bar_end`

### `chan_c_strokes`

存模块 C 的“笔”。

主要字段：

- `symbol_id`
- `chan_level`
- `mode`
- `run_id`
- `seq`
- `start_ts`
- `end_ts`
- `start_price_x1000`
- `end_price_x1000`
- `direction`
- `is_confirmed`
- `begin_base_ts`
- `end_base_ts`
- `begin_base_seq`
- `end_base_seq`
- `extra`

### `chan_c_segments`

结构与 `chan_c_strokes` 类似，表示线段。

### `chan_c_centers`

存中枢。

主要字段：

- `seq`
- `start_ts`
- `end_ts`
- `low_x1000`
- `high_x1000`
- `begin_base_ts`
- `end_base_ts`

### `chan_c_signals`

存买卖点信号。

主要字段：

- `ts`
- `price_x1000`
- `signal_type`
- `is_confirmed`
- `base_ts`
- `base_seq`
- `extra`

重点说明：

**是的，模块 C 已完成的缠论结果里包含买卖点数据，也就是 `signals`。**

### `scheme2_chan_c_published_heads`

这是模块 C 最重要的“发布头”表。

主要字段：

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `base_from_bar_end`
- `base_to_bar_end`
- `bar_count`
- `snapshot_version`
- `status`
- `run_id`
- `published_at`

意义：

- 前端/API 不应该直接“猜”最新 run。
- 应该通过 published head 找到当前对外可见版本。

### `scheme2_chan_c_recompute_watermarks`

增量计算水位。

主要字段：

- `dirty_from_bar_end`
- `last_computed_bar_end`
- `last_error`

### `scheme2_chan_c_tail_tasks`

模块 C 实时增量计算的任务队列表。

主要字段：

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `status`
- `priority`
- `queue_name`
- `schedule_interval_seconds`
- `next_run_at`
- `backoff_until`
- `pending_since`
- `shard_bucket`
- `worker_id`
- `claim_token`
- `lease_version`
- `anchor_bar_end`
- `target_bar_end`
- `claimed_target_bar_end`
- `expected_head_run_id`
- `expected_head_base_to_bar_end`
- `last_success_bar_end`

意义：

- 它是模块 C 高性能实时增量的调度基础。
- 以后如果要做实盘策略，策略本身也很可能要挂在这条“已发布结果推进”链路上。

---

## 5.6 选股/状态快照相关表

### `chan_level_state_snapshots`

这是当前缠论选股最直接可用的“状态表”。

它不是画线原始数据，而是把结构提炼成可筛选状态。

主要字段：

- `symbol_id`
- `chan_level`
- `mode`
- `base_timeframe`
- `snapshot_version`
- `run_id`
- `asof_base_ts`
- `source_bar_until`
- `latest_stroke_seq`
- `latest_stroke_direction`
- `latest_segment_seq`
- `latest_segment_direction`
- `has_active_center`
- `active_center_seq`
- `center_low_x1000`
- `center_high_x1000`
- `center_count`
- `structure_state`
- `structure_direction`
- `last_signal_type`
- `last_signal_side`
- `last_signal_bsp_type`
- `last_signal_base_ts`
- `is_complete`
- `definition_version`

它当前已经被 `/api/v1/screener/chan` 使用。

也就是说：

- 现在的“缠论选股”并不是直接逐条扫描 `chan_c_strokes` / `chan_c_signals`。
- 而是优先查 `chan_level_state_snapshots` 这种状态提炼表。

### `chan_cross_level_states`

作用：

- 表示父级结构和子级结构的嵌套关系。

这张表对你未来做“周线/日线/30f/5f 联动策略”非常关键，因为它天然适合描述：

- 某个父级笔 / 线段 / 中枢内，子级当前发展到了什么状态。

---

## 6. 当前缠论计算链路的真实情况

## 6.1 模块 B 的真实语义

模块 B 在 `services/chan-service/chan_service/vendor_chan_adapter.py` 中实现。

它直接调用 `Vespa314/chan.py`，但做法是：

1. 用 `5f` bar 构造基础 `CKLine_List`。
2. `5f` 层：
   - `bi_list` 作为笔
   - `seg_list` 作为线段
   - `zs_list` 作为中枢
   - `bs_point_lst` 作为买卖点
3. `30f` 层：
   - 用 `seg_list` 当作 30f 笔
   - 用 `segseg_list` 当作 30f 线段
   - 用 `segzs_list` 当作 30f 中枢
   - 用 `seg_bs_point_lst` 当作 30f 买卖点
4. `1d` 层：
   - 再在更高一层递归构造

它的核心配置是：

- `bi_algo = normal`
- `bi_strict = True`
- `bi_fx_check = strict`
- `seg_algo = chan`
- 以及一系列 BSP / MACD / 中枢相关参数

因此，模块 B 更符合“从 5f 递归出 30f、1d”的思路。

---

## 6.2 模块 C 的真实语义

模块 C 在 `services/chan-service/chan_service/module_c_adapter.py` 中实现。

它同样调用 `Vespa314/chan.py`，但实现方式不同：

1. **每个级别独立使用自己的原生周期 K 线**
   - `5f` 用 `5f` K 线
   - `30f` 用 `30f` K 线
   - `1d` 用日线
   - `1w` 用周线
   - `1m` 用月线

2. 每个级别独立构造一份 `CKLine_List`。

3. 每个级别分别取：
   - `bi_list` 作为该级别笔
   - `seg_list` 作为该级别线段
   - `zs_list` 作为该级别中枢
   - `bs_point_lst` 作为该级别买卖点

4. 模块 C 当前配置：

- `MODULE_C_CHAN_CONFIG = {**DEFAULT_CHAN_CONFIG, "bi_strict": False}`

也就是说：

- 模块 C 仍然沿用 `chan.py` 的核心算法。
- 但它改成了**原生周期独立计算**。
- 并且当前已按你的要求把 `bi_strict` 改为了 `False`。

模块 C 当前配置哈希：

- `module-c:native-5lvl-v3-bi-strict-false`

---

## 6.3 模块 C 不是“简化版缠论”

这一点要特别明确：

模块 C 不是手写几何线段，也不是前端自己拼图。

它仍然是：

1. 先把原生周期 K 线送进 `chan.py`
2. 由 `chan.py` 完成：
   - K 线处理
   - 分型构建
   - 笔构建
   - 线段构建
   - 中枢构建
   - 买卖点判断
3. 再把结果转成项目自己的数据库结构

所以它的本质不是“轻量替代算法”，而是“同一套 chan.py 语义，换了多周期输入模式”。

---

## 6.4 模块 C 的全量与实时两条链

### 全量链

入口：

- `services/collector/collector/chan_module_c_recompute.py`

特点：

- 面向全市场或大批量历史重算。
- 可以分片。
- 直接把某个 symbol 的多个 level 一次性算完并写入 `chan_c_*`。

### 实时增量链

入口：

- `services/collector/collector/chan_c_stream.py`

特点：

- 基于 `scheme2_chan_c_tail_tasks` 任务队列。
- 只处理“已发布头之后的新尾部”。
- 按 symbol + level + mode 做 claim / lease / fencing。

这条链是你后续做“5 分钟采集一次，5 分钟内更新完”的关键基础。

---

## 7. 当前高级别端点映射到低级别图上的真实实现

你现在最关心的一点之一，是：

> 高级别笔的端点，如何准确落到低级别 K 线上？

当前项目实际上已经做了这件事，并且思路与你提出的方案高度一致。

实现位置：

- `services/api/app/repositories/chan_postgres.py`

核心逻辑：

1. 如果当前展示图的周期比缠论级别低：
   - 例如在 5f 图上展示 30f / 1d / 1w / 1m 端点；
   - API 会做时间投影。

2. 投影时，会先确定一个候选时间窗口：
   - 优先用该高级别原生 bars 找到这一根 bar 对应的前后时间范围。
   - 再在当前低级别 bars 中寻找落点。

3. 价格匹配规则：
   - 优先找与目标价格**完全相同**的 bar。
   - 若同价多根，取**最后一根**。
   - 若没有完全相同价格，再按最接近价格的 bar 近似匹配。

4. 对笔端点还会区分方向：
   - 向上笔优先用 `high`
   - 向下笔优先用 `low`

这和你现在提出的规则基本一致：

- 通过“时间范围 + 目标价格”定位；
- 顶点找最高价、底点找最低价；
- 同价多根取最后一根。

所以后续如果你要把这条规则进一步固化为“策略标准口径”，不需要从零设计，只需要把现在的 API 投影逻辑进一步规范化、可追溯化即可。

---

## 8. 当前前端 / API 展示级别规则

当前后端 `routes/chan.py` 里，图表显示级别不是完全自由拼的，而是按当前图表周期决定：

1. 当前图周期 `< 30f`
   - 显示：`5f, 30f, 1d, 1w, 1m`

2. 当前图周期 `>= 30f 且 < 1d`
   - 显示：`30f, 1d, 1w, 1m`

3. 当前图周期 `>= 1d 且 < 1w`
   - 显示：`1d, 1w, 1m`

4. 当前图周期 `>= 1w 且 < 1m`
   - 显示：`1w, 1m`

5. 当前图周期 `>= 1m`
   - 显示：`1m`

这说明：

- 当前系统已经支持你后续做多级别策略，但显示层与策略层不能混为一谈。
- 策略模型要自己定义“决策所需级别”，不能完全沿用前端显示规则。

---

## 9. 当前项目已经具备、可直接用于策略模型的能力

## 9.1 已具备原始事实数据

已有：

- 活跃标的主数据 `symbols`
- 多周期 K 线 `klines`
- 采集水位 `scheme2_ingest_watermarks`

这意味着：

- 你做离线策略扫描、回测、验证，不需要再重新建一套行情底库。

## 9.2 已具备结构化缠论结果

已有：

- 笔 `strokes`
- 线段 `segments`
- 中枢 `centers`
- 买卖点 `signals`
- 发布头 `published_heads`

这意味着：

- 策略层不需要自己重新跑分型/笔/中枢/买卖点。
- 只需要围绕这些“已计算结果”做条件组合。

## 9.3 已具备可筛选状态层

已有：

- `chan_level_state_snapshots`
- `chan_cross_level_states`

这意味着：

- 很多策略条件可以先落成“状态条件”，而不是每次都全量扫细表。
- 例如：
   - 当前是趋势 / 盘整 / 无中枢
   - 最近一笔方向
   - 最近一段方向
   - 最近买卖点类型
   - 当前中枢数量

## 9.4 已具备实时任务与增量发布基础

已有：

- 多源补采任务表
- 模块 C tail 任务表
- claim / lease / backoff / shard 机制

这意味着：

- 未来实盘版策略可以直接挂在“新 published head 产生”之后，而不是从零搭一套调度系统。

---

## 10. 要实现“自定义缠论策略模型”，当前必须先协调的地方

下面这些问题，如果不先统一，后面和 GPT / Codex 再深入时很容易反复跑偏。

## 10.1 先确定策略到底基于模块 B 还是模块 C

这是第一优先级问题。

原因：

- 模块 B：`5f` 递归出 `30f/1d`
- 模块 C：`5f/30f/1d/1w/1m` 各自独立用原生 K 线算

两者的笔、线段、中枢、买卖点可能不同。

如果你后续的策略讨论基于“模块 C 的 5 级别原生周期独立计算”，那么至少要统一：

1. 策略研发使用模块 C。
2. API / 前端 / 选股接口读取模块 C。
3. 回测也读取模块 C。

否则会出现：

- 图上看的是 B
- 选股读的是 C
- 回测又用了 B

结果完全无法对齐。

### 现实协调点

当前默认 API `chan_storage_namespace = b`。

所以若后续正式切策略到模块 C，至少需要同步检查：

1. API 环境变量：
   - `CHAN_STORAGE_NAMESPACE=c`

2. 如果还需要 live chan-service 做即时分析/比对：
   - `CHAN_ENGINE_MODE=module_c`

---

## 10.2 先定义“策略时间语义”

这是第二优先级问题。

你和 GPT 已经谈到了这个点，但现在项目里还没有完全把这三个时间概念做成一等公民：

1. `point_time`
   - 结构点本身所落的 bar 时间。

2. `first_seen_time`
   - 系统第一次把这个点看见的时间。

3. `confirm_time`
   - 这个点真正可以用于交易决策的确认时间。

当前数据库里的现状：

- `chan_c_signals.ts / base_ts`
- `chan_level_state_snapshots.last_signal_base_ts`
- `published_heads.published_at`

这些字段还不等于一个完整的“事件时间模型”。

### 如果不补齐，会出现的问题

1. 回测时容易前视。
2. “当天盘中出现过、收盘后又消失”的 predictive 信号很难还原。
3. 无法回答：
   - 这个一买是什么时候第一次出现的？
   - 是什么时候才真正确认的？
   - 当时是否已经满足周线/日线/30f/5f 联立条件？

### 建议

后续策略模型必须明确增加一层事件化字段，至少补：

- `point_time`
- `first_seen_time`
- `confirm_time`
- `source_snapshot_version`
- `source_run_id`

---

## 10.3 先定义“策略层”是读细表还是读状态表

这个问题决定你后续实现复杂度。

### 方案 A：直接读 `chan_c_strokes / centers / signals`

优点：

- 信息最全。
- 可以做非常精细的结构判断。

缺点：

- SQL / Python 逻辑会复杂。
- 扫描全市场时更慢。

### 方案 B：优先读 `chan_level_state_snapshots`

优点：

- 快。
- 更适合选股。

缺点：

- 信息是抽象过的，不够细。
- 像“日线一买后的第一笔上攻是否进入最近相关中枢”这种条件，状态表可能不够表达。

### 对你当前这套策略的判断

你的策略已经不是简单状态筛选，而是：

- 周线 B2
- 日线 B2
- 日线向上第一笔强度
- 与最近中枢关系
- 30f / 5f 一买触发

所以大概率需要：

1. **状态表做第一层粗筛**
2. **细表做第二层精筛**

也就是：

- 先用 `chan_level_state_snapshots` 筛出候选标的
- 再读取候选标的的 `chan_c_strokes / chan_c_centers / chan_c_signals`
- 最后组合成策略信号

---

## 10.4 先统一“高级别端点映射到低级别图”的口径

虽然当前 API 已经做了投影，但如果它以后要成为策略一部分，就必须从“显示逻辑”升级成“策略标准”。

建议统一成下面这条正式口径：

1. 高级别端点先锁定所属高级别 bar 的时间范围。
2. 到低级别 K 线中找目标价格：
   - 顶点取最高价
   - 底点取最低价
3. 同价多根取最后一根
4. 若无完全同价，再取最接近价

原因：

- 这条规则已经和当前 API 实现基本一致。
- 直接拿来固化成本最低。
- 以后图形、回测、策略解释能统一。

---

## 10.5 先定义“策略模型”的输出形态

如果你要的是“缠论选股入场策略”，建议不要一开始就把它写成只有一个最终买点名单。

建议至少分三层输出：

1. **候选层**
   - 周线、日线满足大方向条件的标的。

2. **触发层**
   - 30f / 5f 真正出现入场信号的标的。

3. **交易层**
   - 入场价
   - 入场时间
   - 止损位
   - 离场条件
   - 当前状态

如果不分层，后面回测和实盘会很难解释。

---

## 10.6 先定义“买卖点”到底是直接复用 `signals`，还是再包一层策略事件

当前 `chan_c_signals` 里存的是 `chan.py` 原生买卖点结果。

这和“策略事件”不是一回事。

例如：

- `chan.py` 给出一个 30f 一买；
- 但你的策略要求它必须发生在“有效日线二买区域内”；
- 还要求周线二买、周线 DIF > 0；

这说明：

- `chan.py` 的 `signals` 是**结构级信号**
- 你的策略最终要输出的是**策略级信号**

建议后续单独设计策略结果表，而不是把策略结果直接覆盖 `chan_c_signals`。

---

## 11. 建议增加的策略层数据结构

这是后续实现时比较自然的一种落法。

## 11.1 策略定义表

例如：

- `strategy_definitions`

字段可包括：

- `strategy_code`
- `strategy_name`
- `version`
- `description`
- `rule_spec_json`
- `enabled`

作用：

- 允许你以后迭代多个策略版本，而不是把规则写死在代码里。

## 11.2 策略候选/触发表

例如：

- `strategy_signal_events`

字段可包括：

- `symbol_id`
- `strategy_code`
- `event_type`
  - `candidate`
  - `trigger`
  - `entry`
  - `exit`
  - `stop`
- `point_time`
- `first_seen_time`
- `confirm_time`
- `price`
- `source_level`
- `source_signal_type`
- `source_run_id`
- `source_snapshot_version`
- `features_json`
- `reason_json`

这样后续你要查：

- 某天出现了哪些候选
- 哪些最终触发了
- 为什么触发

都会很清楚。

## 11.3 持仓/回测结果表

例如：

- `strategy_positions`
- `strategy_backtest_runs`
- `strategy_backtest_trades`

这部分现在不一定马上做，但如果你已经明确要验证胜率，那么迟早要有。

---

## 12. 针对你当前这套策略，建议的最小落地路径

我建议分 4 步，不要一步到位。

## 第 1 步：先固定语义基准

先明确：

1. 策略基于模块 C。
2. 模块 C 使用五级别原生周期独立计算。
3. 周线、月线只在已完成周期更新后重新计算。
4. 高级别端点映射低级别，使用当前 API 的时间+价格规则。

这是策略口径基线。

## 第 2 步：先做离线扫描版，不接前端交易交互

先不要急着做“实时提醒 + UI + 自动选股联动”。

先做一个离线策略扫描器：

输入：

- 模块 C 已发布结果

输出：

- 符合周线/日线前置条件的候选标的
- 哪些标的在 30f / 5f 上真正触发了入场

这样最快。

## 第 3 步：补时间语义

当离线扫描逻辑跑通后，再补：

- `point_time`
- `first_seen_time`
- `confirm_time`

然后才能开始做可信回测。

## 第 4 步：再接实时流式计算链

最后再把策略挂到：

- `5f` 增量采集完成
- `chan_c_stream` 发布 head 成功

这条链路上，实现盘中候选/触发更新。

---

## 13. 当前项目里最值得注意的几个“现实约束”

## 13.1 当前默认展示/读取未必是模块 C

这是最大的现实偏差。

如果你后面和 GPT 继续讨论策略，但假设“系统现在前后端都已经统一使用模块 C”，这在当前代码里并不自动成立。

需要明确切换。

## 13.2 策略讨论不能只看画线

画线是显示结果，不等于策略事件。

后续如果只围绕图看“这是不是二买”，会陷入视觉判断。

真正落库、回测、自动选股，需要的是：

- 可查询的结构事件
- 可追溯的时间语义
- 可复现的策略判定过程

## 13.3 模块 C 当前更适合做“你想要的策略模型”

因为你现在已经明确要求：

- `5f/30f/1d/1w/1m` 五个级别各自用对应周期 K 线独立计算

这和模块 C 完全一致。

所以后续若策略继续深化，建议围绕模块 C，不要再让 B / C 混用。

---

## 14. 后续继续和 GPT / Codex 交流时，建议直接引用的统一口径

你后面可以直接把下面这段话作为统一背景发给 GPT / Codex：

> 当前项目已有完整 A 股主数据、五周期 K 线、模块 B 与模块 C 两套缠论结果。后续策略研发以模块 C 为准：5f、30f、1d、1w、1m 五个级别分别使用对应周期 K 线、调用 Vespa314/chan.py 原生逻辑独立计算，当前模块 C 配置为 bi_strict=False。策略不是重写缠论算法，而是在已发布的笔、中枢、买卖点结果之上构建多级别联立的入场/离场模型。后续请重点围绕：策略时间语义（point_time/first_seen_time/confirm_time）、模块 C published head 读取、状态表粗筛 + 细表精筛、以及策略事件表设计来讨论。高级别端点映射到低级别图表时，使用“时间范围 + 目标价格定位，同价多根取最后一根”的规则。当前 API 默认可能仍读取模块 B，需要把模块 C 作为正式策略基线。 

---

## 15. 建议你下一轮优先让我继续做的事情

如果你要继续推进，我建议按下面顺序：

1. 先让我把“模块 C 作为正式读取源”的链路核对并切干净。
2. 再让我设计策略事件表和时间语义字段。
3. 然后实现第一版“离线策略扫描器”。
4. 最后再接入实时增量链路和前端展示。

---

## 16. 结论

一句话概括：

当前项目已经具备了构建“缠论选股入场策略模型”的大部分基础能力，尤其是：

- 五周期 K 线底库
- 模块 C 原生周期独立计算
- 笔/线段/中枢/买卖点落库
- published head 发布机制
- 状态快照表
- 实时任务与增量计算框架

真正还没有统一好的，不是“能不能算”，而是：

1. 策略到底基于模块 B 还是 C；
2. 策略事件的时间语义怎么定义；
3. 策略层是怎么从结构信号组合成入场/出场信号；
4. 这些结果如何稳定落库并用于回测、选股和实盘。

这四件事一旦统一，后续就能比较顺地进入策略实现阶段。
