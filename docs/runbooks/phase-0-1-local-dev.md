# Phase 0/1 Local Dev Runbook

日期：2026-06-14

## 1. 环境要求

- Windows PowerShell。
- Python 3.11+。
- Node.js 20+。
- Docker Desktop。
- 已授权 TradingView Advanced Charts 文件。

## 2. 启动数据库和 Redis

在项目根目录执行：

```powershell
Copy-Item .env.example .env
docker compose -f deploy/docker-compose.dev.yml up -d
```

检查：

```powershell
docker ps
```

## 3. 启动 API

```powershell
cd services/api
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH="../../libs/protocol/python;."
$env:API_TOKEN="dev-local-token"
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

验证：

```powershell
Invoke-RestMethod http://localhost:8000/api/v1/health

Invoke-RestMethod `
  -Headers @{ Authorization = "Bearer dev-local-token" } `
  "http://localhost:8000/api/v1/symbols?keyword=平安"

Invoke-RestMethod `
  -Headers @{ Authorization = "Bearer dev-local-token" } `
  "http://localhost:8000/api/v1/bars?symbol=000001.SZ&timeframe=5f&limit=20"
```

## 4. 启动前端

```powershell
cd apps/web
npm install
$env:VITE_API_BASE_URL="http://localhost:8000"
$env:VITE_API_TOKEN="dev-local-token"
npm run dev
```

访问：

```text
http://localhost:5173
```

## 5. 复制 TradingView 授权文件

将本地授权目录：

```text
H:\OneDrive\OneDrive - long\文档\tradingview本地开发\TV
```

复制到：

```text
apps/web/public/charting_library
```

前端会优先尝试加载：

```text
/charting_library/charting_library.standalone.js
/charting_library/charting_library.js
```

如果这两个文件名与你本地库不同，需要在 `apps/web/src/tradingview/widget.ts` 中调整 `SCRIPT_CANDIDATES`。

## 6. Collector Seed 验证

```powershell
cd services/collector
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH="../../libs/protocol/python;../api;."
python collector/backfill.py --provider seed --symbols "000001.SZ" --timeframes "5f,1d" --limit 5
```

## 7. 测试

API：

```powershell
cd services/api
$env:PYTHONPATH="../../libs/protocol/python;."
python -m pytest -q
```

Collector：

```powershell
cd services/collector
$env:PYTHONPATH="../../libs/protocol/python;../api;."
python -m pytest -q
```

Frontend：

```powershell
cd apps/web
npm run build
```

## 8. Phase 1 验收标准

- `/api/v1/health` 返回 `status=ok`。
- `/api/v1/symbols?keyword=平安` 返回 `000001.SZ`。
- `/api/v1/bars?symbol=000001.SZ&timeframe=5f` 返回 OHLC 合法的 K 线。
- 前端显示 API 状态、样例标的列表和 fallback K 线。
- 复制 TradingView 授权文件后，图表容器加载 Advanced Charts。

## 9. 进入 Phase 2

Phase 2 的数据库落库和七周期 backfill 见：

```text
docs/runbooks/phase-2-database-backfill.md
```
