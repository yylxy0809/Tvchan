# 工具链、依赖与设备 B 部署

## 1. 推荐环境

设备 B：12 核 24 线程、96 GB 内存、约 500 GB 可用磁盘，适合承担数据库和全量计算。推荐：

- Windows 11 + Docker Desktop + WSL2 后端。
- Docker Compose v2。
- Git，支持长路径。
- Python 3.11，仅用于本机脚本；生产服务优先在容器内运行。
- Node.js 仅在 B 需要构建 web-gateway 时安装，A 仍负责主要前端开发。

不要使用 Anaconda 跑长时导入或重算。项目 Dockerfile 均以 Python 3.11 slim 为基线。

## 2. 容器镜像

| 服务 | 镜像/构建 | 说明 |
|---|---|---|
| timescaledb | `timescale/timescaledb:latest-pg16` | 正式部署建议锁定验证过的具体 tag，不长期使用 latest |
| redis | `redis:7-alpine` | AOF 开启 |
| db-migrate | `postgres:16-alpine` | 顺序执行 `db/sql/*.sql` |
| api | `deploy/Dockerfile.api` | Python 3.11 + API requirements；还安装 Node/npm |
| collector | `deploy/Dockerfile.collector` | Python 3.11；包含 protocol、API 共享代码和 collector |
| web-gateway | nginx 1.27 alpine | 可选；A 前端可直接访问 B API |

## 3. Python 依赖

### API

- FastAPI 0.115.6
- Uvicorn 0.34.0
- asyncpg 0.30.0
- redis 5+
- httpx 0.28.1
- pywencai 0.13.1
- pytest 8.3.4

### Collector

- asyncpg、redis、httpx
- pytdx 1.72
- pyarrow 15+
- pytest

### Strategy

- asyncpg
- pytest

### 外部算法

Module C 不通过 PyPI 安装 `chan.py`，而是只读挂载 vendored `Vespa314/chan.py` 工作目录。Compose 默认容器路径 `/opt/vendor/chan.py-main`，由 `CHAN_PY_HOST_PATH` 指向 B 本机目录。

## 4. 当前 Compose 拓扑

基础服务：`timescaledb`、`redis`、`db-migrate`、`api`、`web-gateway`。

profile 服务：

- `manual-market-fill`: `market-fill-worker`
- `batch-history`: `history-backfill-worker`
- `workers/realtime-pipeline/realtime-chan-c`: `chan-c-stream-worker`
- `batch-chan-module-c-recompute`: 一次性 `chan-module-c-recompute-worker`
- `batch-tdx-csv-import`: `tdx-csv-import-worker`

全量 Module C 重算使用显式的一次性 Compose batch profile，不属于无人值守 profile。仅在 coverage/audit 门通过且已停止 realtime Chan stream worker 后启动；它固定使用原生 `5f,30f,1d,1w,1m`、双 mode、只读 chan.py 挂载，且不允许用聚合补齐替代原生周期。

## 5. B 端目录建议

```text
D:\tv-backend\repo                 代码
D:\tv-backend\vendor\chan.py-main chan.py
E:\tv-data\postgres               PostgreSQL 数据目录
E:\tv-data\tablespaces\chan-c     Module C tablespace（可选）
E:\tv-data\imports                历史 K 线原始文件
E:\tv-data\logs                   长任务日志
E:\tv-data\backups                schema/关键表备份
```

500 GB 对“原始文件 + PostgreSQL + WAL + 缠论多版本”并不宽裕。旧 A 数据库曾约 234 GB，旧 Module C 约 14 GB，仅作量级参考。建议至少保留 80-100 GB 空闲，不把重复原始压缩包和解压文件长期同时保留。

## 6. 环境变量重点

- `POSTGRES_DATA_HOST_PATH`: B 的 PostgreSQL 数据目录。
- `POSTGRES_CHAN_C_TABLESPACE_HOST_PATH`: 可选独立 Module C tablespace 根目录。
- `POSTGRES_BIND`: 仅 B 本机使用时 `127.0.0.1`；A 需要直连数据库做诊断时才设局域网 IP，并配置防火墙和强密码。通常 A 不应直连 DB。
- `API_BIND=0.0.0.0`、`API_PORT=8001`。
- `CORS_ORIGINS`: 加入 A 的前端 origin，例如 `http://192.168.5.x:5173`。
- `CHAN_PY_HOST_PATH`: B 上 vendored chan.py 目录。
- `API_TOKEN`/`ADMIN_API_TOKEN`: 必须改默认值，前端使用普通 token。
- `DATABASE_URL`、`REDIS_URL`: 容器内使用服务名，不使用 localhost。

## 7. 网络与安全

- A 前端通过 `http://192.168.5.<B>:8001` 或 B 的 `web-gateway:8080` 访问。
- 只开放 8001/8080 给局域网；5432 和 6379 默认绑定 `127.0.0.1`。
- Windows 防火墙只允许“专用网络”及 A 的 IP 段。
- 不把数据库密码、LLM key、问财 cookie 提交到 Git。

## 8. 当前验证记录

在 A 的 2026-07-11 工作树上：

- Python `compileall`：通过。
- Docker Compose `config --quiet`：通过。
- protocol：26 tests passed。
- collector：174 tests passed。
- strategy-service：185 tests passed。
- API 全套测试：未执行完成，本地 sandbox Python 缺少 `anyio`，在测试收集阶段失败；这属于本地依赖环境问题，不是测试断言失败。B 必须在容器或干净 venv 中 `pip install -r services/api/requirements.txt` 后重跑。

## 9. B 端必跑命令

```powershell
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml config --quiet
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml build
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml up -d timescaledb redis db-migrate api
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml ps
```

然后在对应镜像内运行 protocol/API/collector/strategy 全套测试，避免宿主机包污染。
