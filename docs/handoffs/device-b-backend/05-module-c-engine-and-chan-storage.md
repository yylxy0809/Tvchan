# Module C 缠论核心引擎与存储设计

## 1. 算法来源与边界

Module C 的生产语义基准是 vendored `Vespa314/chan.py`。本项目适配器只负责：

1. 把数据库 K 线转换为 chan.py 的 `CKLine_Unit`。
2. 为每个原生周期建立独立 `CKLine_List`。
3. 调用 chan.py 的 K 线合并、分型、笔、线段、中枢和买卖点逻辑。
4. 把对象转换为稳定 JSON，并写入 PostgreSQL。

不得用 `chanlun.rs` 或自写算法结果替换 chan.py 的结构判断。可以借鉴它的并发、状态机和流式架构，但算法结果必须以 chan.py 为准。

## 2. 五级别独立计算

| 缠论级别 | 输入 K 线 |
|---|---|
| 5f | 5f 原生 K 线 |
| 30f | 30f 原生 K 线 |
| 1d | 日线原生 K 线 |
| 1w | 已闭合周线 |
| 1m | 已闭合月线 |

每个级别独立调用一次 chan.py。高级别不是“递归使用低级别笔”计算，也不是 API 请求时从 5f 现场聚合。

## 3. 当前有效配置

语义合同位置：`libs/protocol/python/trading_protocol/module_c.py`。

```text
version/config_hash:
native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false

bi_algo=normal
bi_strict=false
bi_fx_check=strict
gap_as_kl=true
bi_end_is_peak=true
bi_allow_sub_peak=false
seg_algo=chan
zs_combine=false
bs_type=1,2,3a,3b
```

说明：adapter 的默认字典里仍写着 `bi_strict=True`，但创建 `CChanConfig` 前会被不可变语义合同覆盖为 `False`。最终运行值是 `False`。`bi_allow_sub_peak=False` 表示不允许次高/次低成笔，符合本轮全量重算要求。

设备 B 启动全量重算前必须打印并持久化最终 effective config，不只检查 adapter 默认字典。

## 4. 输出结构

- strokes：笔端点、方向、确认状态。
- segments：线段端点、方向、确认状态。
- centers：中枢起止、上下轨、确认状态。
- signals：买卖点 side、bsp_type、价格、features、确认状态。
- channels：chan.py trend 上下轨派生数据。

confirmed/predictive 是同一算法结果的状态视图。正式策略回测必须使用符合合同的可见时间，不得因 predictive 后来确认而把 point_time 当作当时已知时间。

## 5. Module C 表

### chan_c_runs

每次计算版本的元数据：symbol、level、输入签名、config_hash、bar 范围、bar_count、状态、snapshot_version、父 run/anchor 等流式字段。

### chan_c_strokes / chan_c_segments

保存 `start_ts/end_ts`、价格、方向、确认状态、`begin_base_ts/end_base_ts` 和 base_seq。

### chan_c_centers

保存起止时间、中枢上下轨、确认状态和 base 映射。

### chan_c_signals

保存信号时间、价格、signal_type、确认状态、`base_ts/base_seq`；side、bsp_type、features 和稳定 id 当前放在 `extra jsonb`。

### scheme2_chan_c_published_heads

每个 `(symbol,level,mode,base_timeframe)` 只指向一个前端可见 run。新 run 完整写入成功后才原子切换 head。

### watermarks / tail_tasks / head_history

- recompute watermarks：脏区间和最后计算水位。
- tail tasks：claim/lease/claim_token/fencing、重试和退避。
- head history：为生命周期 first_seen/confirm/disappear 提供发布序列。

## 6. 全量计算流程

`chan_module_c_recompute.py`：

1. 只选 `symbols.is_active=true` 且五级别 ingest watermark 都存在的标的。
2. 支持 shard-index/shard-count 和进程内 concurrency。
3. 每个 symbol 读取五级别完整 K 线。
4. 在工作线程调用 Module C adapter。
5. 校验所有时间落在本级 K 线范围内。
6. 各级别分别写 run 和明细，通过 COPY 批量入库。
7. run success 后更新 published head 和 recompute watermark。

建议 B 初始采用 4 个进程分片、每进程 concurrency=1、DB pool=1；观察 CPU、内存和 WAL 后再升到 6-8 路。不要用一个 Python 进程开大量线程，因为 chan.py 计算受 GIL 和对象内存影响。

## 7. 实时尾部流程

`chan_c_stream.py` 和 `chan_c_stream_postgres.py` 已实现：

- stale head 发现。
- symbol 分片。
- `FOR UPDATE SKIP LOCKED` claim。
- lease_version + claim_token fencing。
- 从最近确认笔端点附近加载 context + tail。
- 旧 run 前缀与新尾部合并。
- COPY 写新 run。
- CAS 更新 published head。
- Redis 发布 head update。

但这条链路仍必须解决/验证一个关键问题：published head 不能指向只包含尾部的 run。验收要随机抽取历史窗口，确认增量前后所有旧笔/中枢/信号仍可查询，而不是只看最新几十根。

## 8. 高级别端点投影到低周期

推荐规则与用户要求一致：

1. 先找到高级别端点所属的那一根高级别 K。
2. 取该高级别 K 覆盖的低周期时间区间。
3. 顶端点在区间内找 high 等于目标价格的低周期 K；底端点找 low。
4. 浮点比较使用数据库整数价 `x1000`，不能直接比较 float。
5. 同价多根取最后一根。
6. 找不到精确价格时返回“投影失败”证据，不应静默吸附到任意临近小高点。

当前 API 已有 `_project_point_timestamp`、native window 和 preferred high/low 逻辑，但前端仍观察到错位，因此 B 端要用 `000001.SZ` 等已知案例做端到端测试。

## 9. 生命周期时间不是当前结构表固有列

- point_time：可由 signal 的 `base_ts/ts` 得到；笔/中枢也有自身结构时间。
- first_seen_time：该身份第一次出现在 published run 的时间。
- confirm_time：同一身份第一次从 predictive 变成 confirmed 的可见时间。
- disappear_time：后续 published run 中该身份消失的时间。

后三者必须由 run/head 发布历史重建，不能仅看最新 `chan_c_signals`。因此全量重算时要同步记录每次 head 切换，并在策略层生成不可变生命周期事件账本。

## 10. B 端全量重算验收

- 五级别 × confirmed/predictive head 覆盖全部合格活跃标的。
- 每个 head 指向 success run，config_hash 精确等于 v4 合同。
- `base_timeframe == chan_level`。
- 抽样与直接 chan.py 离线结果逐点一致。
- `bi_allow_sub_peak=false` 的已知样本不再出现次高/次低成笔。
- 高级别端点投影已通过同价多根取最后一根测试。
- signals 不为空的样本验证 side/bsp_type/features 完整。
- 全量基线发布完成前不启动 stream worker。

另一个必须修复的代码质量问题：当前 `module_c_adapter.py` 中部分 `BSP_TYPE_CN` 和 `signal_type` 中文字面量已经出现乱码。结构化 `side/bsp_type` 仍可用，但 B 端应以 UTF-8 修复显示文案并补测试，不能让策略解析中文 `signal_type`。
