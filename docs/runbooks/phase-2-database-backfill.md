# Phase 2 Database Backfill Runbook

日期：2026-06-14

目标：

- 启动 PostgreSQL + TimescaleDB。
- 将 10 只 seed 股票的七周期 K 线写入数据库。
- 以 `USE_SEED_DATA=false` 启动 API，从数据库读取 symbols/bars。
- 运行空间测量脚本，为 30 股票样本外推做准备。

## 1. 启动数据库

在项目根目录执行：

```powershell
docker compose -f deploy/docker-compose.dev.yml up -d
```

确认：

```powershell
docker ps
```

## 2. 安装 Collector 依赖

```powershell
cd services/collector
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

## 3. 写入七周期样本数据

注意：PowerShell 下 `1d`、`1m` 等参数请加引号。

Seed 数据只用于验证链路，不是真实行情。

```powershell
$env:PYTHONPATH="../../libs/protocol/python;../api;."
$env:DATABASE_URL="postgresql://trader:trader@localhost:5432/tradingview_local"

python collector/backfill.py `
  --provider seed `
  --symbols "000001.SZ,000002.SZ,000063.SZ,000333.SZ,000651.SZ,600000.SH,600519.SH,600887.SH,601318.SH,601398.SH" `
  --timeframes "5f,15f,30f,1h,1d,1w,1m" `
  --limit 300 `
  --write-db `
  --replace-db
```

预期输出：

```json
{"provider":"seed","symbols":10,"deleted_bars":0,"bars":12730,"timeframes":["5f","15f","30f","1h","1d","1w","1m"],"database":"written"}
```

`1m` 在本项目中表示月线，数据库编码为 `43200`。

## 3.1 写入 pytdx 真实行情样本

安装依赖：

```powershell
pip install -r services/collector/requirements.txt
```

执行真实行情回填：

```powershell
cd services/collector
$env:PYTHONPATH="../../libs/protocol/python;../api;."
$env:DATABASE_URL="postgresql://trader:trader@localhost:5432/tradingview_local"

python collector/backfill.py `
  --provider pytdx `
  --symbols "000001.SZ,600519.SH" `
  --timeframes "5f,15f,30f,1h,1d,1w,1m" `
  --limit 300 `
  --write-db `
  --replace-db
```

如果默认通达信服务器不可达，可以指定：

```powershell
python collector/backfill.py `
  --provider pytdx `
  --tdx-host "47.103.48.45" `
  --tdx-port 7709 `
  --symbols "000001.SZ" `
  --timeframes "5f,1d" `
  --limit 100 `
  --write-db `
  --replace-db
```

API health 的 `data_source` 应显示 `database:pytdx`，或在混合数据时显示 `database:seed,pytdx`。

## 4. 以 DB mode 启动 API

如果已有 8000 端口 API 在跑，先结束旧进程，或换端口。

```powershell
cd services/api
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt

$env:PYTHONPATH="../../libs/protocol/python;."
$env:USE_SEED_DATA="false"
$env:DATABASE_URL="postgresql://trader:trader@localhost:5432/tradingview_local"
$env:API_TOKEN="dev-local-token"

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

验证：

```powershell
Invoke-RestMethod `
  -Headers @{ Authorization = "Bearer dev-local-token" } `
  "http://localhost:8000/api/v1/symbols?keyword=平安"

Invoke-RestMethod `
  -Headers @{ Authorization = "Bearer dev-local-token" } `
  "http://localhost:8000/api/v1/bars?symbol=000001.SZ&timeframe=1m&limit=5"
```

## 5. 空间测量

项目根目录执行：

```powershell
.\\scripts\\measure_storage.ps1
```

输出包括：

- 用户表总大小。
- 表大小。
- 索引大小。
- 各周期 K 线行数和时间范围。

Phase 2 的正式目标是扩到 30 只股票样本后再做全市场外推。
