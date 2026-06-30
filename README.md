# TradingView A Share Local

私有部署的 A 股行情与 TradingView Advanced Charts 本地开发项目。

当前实现范围是 Phase 0/1：

- FastAPI API 服务骨架。
- 健康检查、股票搜索、K 线查询接口。
- 10 只 A 股样例标的。
- seed K 线数据路径，便于在 pytdx/数据库尚未接通前验证前后端闭环。
- pytdx 真实行情 provider，支持通过 collector backfill 写入数据库。
- pytdx-ready 的采集 Provider 抽象。
- React/Vite 前端与 TradingView Datafeed 壳。
- PostgreSQL + TimescaleDB、Redis 开发 Compose。

完整架构方案见：

- `outputs/tradingview-a-share-local-architecture-plan.md`
- `docs/plans/2026-06-14-phase-0-1-implementation.md`
- `docs/runbooks/local-runtime-checklist.md`

## 目录

```text
apps/web                 React/Vite 前端
services/api             FastAPI API Gateway
services/collector       行情采集服务骨架
services/chan-service    chan.py 独立服务预留目录
libs/protocol/python     Python 共享协议类型
db/sql                   数据库初始化 SQL
deploy                   本地开发 Docker Compose
docs/runbooks            运行手册
```

## 本地开发快速启动

1. 复制环境变量：

```powershell
Copy-Item .env.example .env
```

2. 启动数据库和 Redis：

```powershell
docker compose -f deploy/docker-compose.dev.yml up -d
```

3. 启动 API：

```powershell
cd services/api
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH="../../libs/protocol/python;."
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

4. 启动前端：

```powershell
cd apps/web
npm install
npm run dev
```

前端默认连接 `http://localhost:8000`，访问 token 默认读取 `VITE_API_TOKEN`。

## TradingView 授权库文件

将你本地授权目录：

```text
H:\OneDrive\OneDrive - long\文档\tradingview本地开发\TV
```

中的 Charting Library 静态文件复制到：

```text
apps/web/public/charting_library
```

该目录被 `.gitignore` 忽略，避免误提交授权文件。

如果还没复制，前端会显示 Phase 1 fallback 数据视图，API/Datafeed 仍可继续开发。

## Phase 1 验收

- `GET /api/v1/health` 返回 `ok`。
- `GET /api/v1/symbols?keyword=平安` 能找到 `000001.SZ`。
- `GET /api/v1/bars?symbol=000001.SZ&timeframe=5f` 返回 seed K 线。
- 前端可以看到 API 状态、样例标的、K 线数据。
- 复制 TradingView 库文件后，图表容器可加载 Advanced Charts。
