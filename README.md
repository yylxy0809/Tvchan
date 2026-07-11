# TradingView A Share Local

本项目是本地/私有部署的 A 股 TradingView Advanced Charts 终端，核心链路是：

- 前端：React/Vite + TradingView Charting Library。
- API：FastAPI，提供 K 线、缠论叠加层、问财选股、缠论选股、后台配置和实时 WebSocket。
- 数据库：PostgreSQL/TimescaleDB，保存 K 线、证券主数据、任务状态和已发布缠论结果。
- 实时 worker：`realtime-pipeline` 负责多源抓取 K 线、触发增量缠论发布。
- chan-service：独立封装 `chan.py` 分析引擎。

## 目录

```text
apps/web                 前端应用
services/api             FastAPI API gateway
services/collector       行情抓取、证券主数据、实时流水线 worker
services/chan-service    chan.py 服务封装
libs/protocol/python     共享协议类型
db/sql                   数据库迁移 SQL
deploy                   Docker/NAS 部署配置
docs/runbooks            运行手册
```

## 本地开发

启动数据库和 Redis：

```powershell
docker compose -f deploy/docker-compose.dev.yml up -d
```

启动 API：

```powershell
cd services/api
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH="../../libs/protocol/python;."
uvicorn app.main:app --reload --host 0.0.0.0 --port 8001
```

启动前端：

```powershell
cd apps/web
npm install
npm run dev
```

访问示例：

```text
http://127.0.0.1:5173/?apiBaseUrl=http%3A%2F%2F127.0.0.1%3A8001
```

## 部署入口

部署前先阅读：

- `docs/runbooks/backend-docker-nas.md`
- `docs/runbooks/local-runtime-checklist.md`
- `docs/runbooks/scheme2-verification-gates.md`

默认无人值守链路是 core backend + `realtime-pipeline` fetch/Chan worker。旧的
`market-fill`、独立 `chan-tail-publisher`、`chan-recompute`、history、tdx-csv
worker 只作为手动维护或回滚入口，不应和实时 tail 常驻并行运行。

## TradingView 授权库

将本地授权的 Charting Library 静态文件复制到：

```text
apps/web/public/charting_library
```

该目录被 `.gitignore` 和 `.dockerignore` 排除，不会提交到仓库或进入 Docker
构建上下文。生产 Docker 包使用 `apps/web/dist` 中已构建的前端产物。

## 验证

常用验证命令：

```powershell
pytest services/api/tests
pytest services/collector/tests
pytest services/chan-service/tests
cd apps/web
npm run build
npm run test:contract
```

部署健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8001/api/v1/health | ConvertTo-Json -Depth 8
```
