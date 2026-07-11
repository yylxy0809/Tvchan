# 策略信号历史生命周期推荐表结构

## 1. 设计目标

Module C 结构表回答“最终算出了什么”；生命周期层回答“在当时什么时候第一次看见、什么时候确认、什么时候消失”。两者必须分层，才能避免历史回测偷看未来。

推荐把生命周期表放在同一 PostgreSQL 实例的 `strategy` schema，结构明细仍放 `public.chan_c_*`。以下为推荐合同，不代表当前迁移已经完整实现。

## 2. 发布历史表

`chan_c_head_history`：每次 head 原子切换追加一行，不更新旧行。

| 字段 | 类型 | 说明 |
|---|---|---|
| id | bigint identity | 顺序主键 |
| symbol_id | int FK | 标的 |
| chan_level | int | 5/30/1440/10080/43200 |
| mode | text | confirmed/predictive |
| config_hash | varchar(128) | 算法语义身份 |
| old_run_id/new_run_id | bigint | 切换前后版本 |
| old_bar_end/new_bar_end | timestamptz | 数据水位 |
| snapshot_version | varchar(255) | 必须与 Module C 字符串合同一致 |
| published_at | timestamptz | 数据库原子发布时间 |
| worker_id/claim_token | text | 发布者与 fencing 证据 |
| source | text | full_recompute/stream/backfill |

唯一约束建议 `(symbol_id,chan_level,mode,new_run_id)`，保证崩溃重试幂等。`published_at` 必须由数据库 `clock_timestamp()` 生成。

## 3. 结构身份表

`chan_structure_identity`：为跨 run 的同一结构保存稳定身份。

| 字段 | 说明 |
|---|---|
| fingerprint | SHA-256/UUID，主键 |
| symbol_id, chan_level, structure_type | signal/stroke/segment/center |
| side_or_direction, bsp_type | 结构语义 |
| point/start/end time | UTC 结构时间 |
| price/start/end/low/high_x1000 | 整数价格身份 |
| config_hash | 算法版本隔离 |
| identity_version | fingerprint 规则版本 |
| payload_json | 非身份扩展字段 |

同一算法配置下，数据库自增 run_id、明细 id、mode 不应单独决定身份；predictive 到 confirmed 才能归并为同一个结构。

## 4. 生命周期事件表

`chan_structure_lifecycle_events`：append-only 事件账本。

| 字段 | 说明 |
|---|---|
| event_id | 主键 |
| fingerprint | 结构身份 FK |
| event_type | first_seen/confirmed/disappeared/reappeared/baseline_observed |
| effective_time | 当时系统可见时间 |
| point_time | 结构所在 K 线时间 |
| head_history_id/run_id | 来源版本 |
| previous_mode/current_mode | 状态变化 |
| provenance_json | 数据源、snapshot、推导证据 |
| created_at | 写入时间 |

唯一约束建议 `(fingerprint,event_type,head_history_id)`。同一事件重复消费不会重复插入。

## 5. 生命周期当前投影

`chan_structure_lifecycle_current` 可作为读模型：

- fingerprint 主键。
- point_time。
- first_seen_time。
- confirm_time。
- disappear_time。
- current_status/current_mode。
- first_seen_run_id/confirmed_run_id/last_seen_run_id。
- provenance/updated_at。

它可以从 append-only 事件重建，因此损坏时可删除后重算；正式历史证据仍以事件表为准。

## 6. 策略事件表

现有 `strategy_signal_events` 字段方向正确，建议补充：

- `source_fingerprint`：关联缠论结构身份。
- `visibility_policy_version`：说明 first_seen/confirm 使用规则。
- `event_identity` 唯一键：strategy_code/version + symbol + event_type + source_fingerprint + context identity。
- `is_official`、`research_only`：物理字段，不只放 JSON。
- `parent_event_id/parent_fingerprint`：30F 与 5F 父子绑定。
- `as_of_time`：该策略判断的截面时间。

继续保留 point/first_seen/confirm/disappear、source run/head/snapshot、confidence、strength、features 和 reason。

## 7. 策略上下文表

`strategy_contexts` 保存周线背景和日线 setup episode。建议增加稳定的 `context_identity` 唯一键，并把周 B1/B2、日 B1/B2 的引用从不稳定明细 id 改为 fingerprint 或 strategy event id。

上下文状态应 append event + current projection，而不是在一行上反复覆盖，避免失去历史失效原因。

## 8. Outbox 与消费水位

为保证 published head 与生命周期观察不丢事件，推荐：

1. head CAS 与 `chan_c_head_outbox` 插入同一事务。
2. lifecycle observer claim outbox，比较 old/new run。
3. 写 lifecycle events 后标记 outbox 完成。
4. 单独保存 `(observer_name,last_head_history_id)` 消费水位。

Redis 只用于加速通知；observer 重启后必须能从数据库 outbox 补读。

## 9. Baseline 与历史回放

第一次全量重算只能生成 `baseline_observed`：

- point_time 是历史结构位置。
- first_seen_time 只能是 baseline 发布时刻，除非执行逐 cutoff 历史重放。
- 不得把 point_time 填到 first_seen_time 冒充当时可见。

需要过去多年正式回测时，按 30F/日线闭合 cutoff 逐步重放 K 线，依次发布 run/head，再由同一 observer 生成生命周期。重放结果使用独立 run_group/profile，不能覆盖在线 head。

## 10. 索引与保留

建议索引：

- head history：`(symbol_id,chan_level,mode,published_at,id)`。
- lifecycle events：`(fingerprint,effective_time)`、`(symbol_id,chan_level,effective_time)`。
- strategy events：`(strategy_code,strategy_version,symbol_id,first_seen_time)`。
- official 筛选部分索引：`where is_official=true and disappear_time is null`。

run GC 前必须检查 head history、lifecycle provenance 和回测 run 是否引用该 run。不能只看当前 published head 就删除旧 run。

## 11. 上线验收

- 同一瞬间 `+00:00` 与 `+08:00` 只生成一个 fingerprint/event；naive 时间拒绝。
- predictive -> confirmed 顺序正确。
- 结构消失、重现和价格修正可区分。
- outbox 重放幂等。
- online 与 historical replay run_group 隔离。
- 策略查询 `as_of_time` 后的数据为 0。
- official trace 不含 research_only 数据。
- current projection 可从事件账本完全重建并逐字段一致。
