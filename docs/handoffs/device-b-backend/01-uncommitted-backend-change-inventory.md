# 当前未提交后端修改盘点

## 1. 盘点口径

本次只盘点和解释，不执行 `git reset`、`git checkout`、`git clean` 或文件删除。审计时工作树统计为：

- 跟踪文件修改：71 个。
- 跟踪文件删除：27 个。
- 未跟踪路径：77 个。
- 已跟踪差异：98 个文件，约 9,099 行新增、8,278 行删除。

这些数字会随其他工作树继续修改而变化。设备 B 应以最终交接提交为准，而不是按此数字逐个复制。

## 2. 架构级变更

### 2.1 独立 chan-service 被移除

已删除：

- `services/chan-service/**`
- `deploy/Dockerfile.chan-service`
- `services/api/app/services/chan_client.py`
- `scripts/start-chan-service.ps1`

含义：API 不再通过 HTTP 调另一个缠论微服务；Module C 适配器现在由 collector 直接加载。设备 B 不应重新部署旧 `chan-service`。

### 2.2 Model B 被退休

已删除旧 `chan_recompute.py`、`chan_state.py`、重算队列存储和相关 PowerShell 脚本。新增迁移 `027_retire_model_b.sql` 会删除旧 Model B 运行表。保留的是 Module C 的 `chan_c_*` 和 `scheme2_chan_c_*`。

注意：迁移 027 是不可逆的结构性动作。在 B 新库中没有历史 Model B 数据，风险较低；在任何已有数据库执行前仍应做 schema 备份。

### 2.3 Module C 归属 collector

新增核心文件：

- `services/collector/collector/module_c_adapter.py`
- `services/collector/collector/chan_module_c_recompute.py`
- `services/collector/collector/chan_c_stream.py`
- `services/collector/collector/storage/chan_c_stream_postgres.py`
- `libs/protocol/python/trading_protocol/module_c.py`

### 2.4 窗口化图表链路

新增或大改：

- `/api/v3/chart/bars`
- `/api/v3/chart/overlay`
- `/api/v1/chart/window`
- Module C 窗口索引迁移 025
- 周/月图表缓存迁移 026
- WebSocket delta 协议和前端 overlay manager

这是前端切周期、拖拽和缠论画线性能改造的主路径。

### 2.5 策略服务为未跟踪目录

`services/strategy-service/` 整体尚未提交，但包含 Phase 1.1 至 Phase 1.21 的实现、185 个测试和大量审计产物。若只复制 Git 已跟踪文件，该目录会完全丢失。

## 3. 新增 SQL 迁移

当前新增但未提交的迁移为 `014` 至 `027`：

| 迁移 | 作用 |
|---|---|
| 014-018 | 实时采集任务、候选数据、调度、租约、运维加固 |
| 019 | Module C run、笔、线段、中枢、买卖点、published head、水位 |
| 020 | Module C 流式尾部任务和 run 亲缘元数据 |
| 021 | 周日共振策略定义、事件、上下文、回测表 |
| 022 | published head 观察历史 |
| 023-024 | K 线来源覆盖、水位审计、隔离区 |
| 025 | 缠论窗口查询 GiST/前后邻接索引 |
| 026 | 周/月图表紧凑读缓存 |
| 027 | 删除 Model B 表 |

## 4. 当前不能直接交给 B 的原因

- 工作树包含后端、前端、策略和数据库迁移的混合变更。
- 独立服务删除与替代文件新增尚未形成同一提交。
- 本地 API 测试运行环境缺少 `anyio`，不能用本机 sandbox Python 完成 API 全套验证；Docker 构建会按 requirements 安装传递依赖，但尚需在 B 端实际验证。
- Module C 实时增量发布曾出现 published head 指向不完整尾部 run 的问题；代码已尝试合并旧 run 与尾部，但必须用真实数据库做完整性验收。
- 迁移 `022` 的 `snapshot_version bigint` 与 Module C 当前 `snapshot_version varchar(255)` 语义存在明显不一致，B 上必须先修复迁移设计。

## 5. 建议保存方式

最稳妥的是在 A 当前工作树完成相关修复后，至少拆成三次提交：

1. `backend-core`: protocol、db/sql、api、collector、deploy、后端 scripts。
2. `strategy-service`: 策略代码、合同、测试；大型历史 outputs 可单独决定是否入库。
3. `frontend-windowed-chart`: `apps/web` 的 API、缓存、TradingView 和 UI 改动。

不要在 B 上根据本盘点手工重写删除项；必须传递 Git 对象或统一补丁，才能保留文件删除、重命名和精确差异。
