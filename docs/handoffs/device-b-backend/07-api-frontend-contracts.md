# 设备 A 前端与设备 B 后端 API 合同

## 1. 连接方式

设备 A 前端启动后使用：

```text
http://127.0.0.1:5173/?apiBaseUrl=http://192.168.5.<B>:8001
```

生产 API 默认端口 8001。本机调试曾使用 8003，那只是手工启动端口，不应写死进合同。

HTTP 请求使用：

```http
Authorization: Bearer <API_TOKEN>
```

WebSocket 在 query string 传 token。B 的 `CORS_ORIGINS` 必须包含 A 前端实际 origin。

## 2. 主 K 线接口

```http
GET /api/v3/chart/bars
  ?symbol=000001.SZ
  &timeframe=1d
  &from=2026-01-01T00:00:00+08:00
  &to=2026-07-01T00:00:00+08:00
  &limit=300
```

v3 `to` 为 end-exclusive。响应：

```json
{
  "symbol": "000001.SZ",
  "timeframe": "1d",
  "bars": [
    {
      "time": 1782898800,
      "open": 10.5,
      "high": 10.57,
      "low": 10.41,
      "close": 10.47,
      "volume": 12345678,
      "amount": 123456789.0,
      "complete": true,
      "revision": 0
    }
  ]
}
```

前端必须把 `time` 当 Unix 秒和 bar_end，不得乘错 1000，也不得把 UTC 秒再次当本地秒偏移。

## 3. 主缠论窗口接口

```http
GET /api/v3/chart/overlay
  ?symbol=000001.SZ
  &timeframe=1d
  &from=...
  &to=...
  &limit=300
  &modes=confirmed,predictive
```

`from/to` 必须存在且带 UTC offset。生产显示级别由后端固定，前端不得自行请求其他组合：

| 图表周期 | 后端返回级别 |
|---|---|
| 5f | 5f, 30f, 1d |
| 15f | 5f, 30f, 1d |
| 30f | 30f, 1d |
| 1h | 30f, 1d |
| 1d | 1d, 1w |
| 1w | 1w, 1m |
| 1m | 1m |

说明：这张表是当前代码事实。它与更早“30f 及以上只显示 30f+日线”的旧要求不同，已经扩展为日/周/月层级。若产品要求要改，必须先更新后端合同和测试，不能仅在 Pine/前端隐藏。

## 4. Overlay 响应

顶层关键字段：

```json
{
  "symbol": "000001.SZ",
  "chart_timeframe": "1d",
  "levels": ["1d", "1w"],
  "modes": ["confirmed", "predictive"],
  "snapshot_version": "...",
  "base_timeframe": "native",
  "base_ts_semantics": "bar_end",
  "engine": "database:chan-module-c-precomputed",
  "requested_bar_count": 300,
  "bars_by_level": {"1d": 300, "1w": 60},
  "strokes": [],
  "segments": [],
  "centers": [],
  "signals": [],
  "channels": []
}
```

### 笔/线段

```json
{
  "id": "stable-id",
  "seq": 12,
  "level": "1d",
  "mode": "confirmed",
  "start": {"time": 1, "price": 10.432, "base_ts": 1, "base_seq": 20},
  "end": {"time": 2, "price": 11.57, "base_ts": 2, "base_seq": 42},
  "begin_base_ts": 1,
  "end_base_ts": 2,
  "direction": "up",
  "confirmed": true
}
```

### 中枢

`start_time/end_time, low/high, level, mode, confirmed`。矩形必须使用响应中的投影后时间，不能用数组序号猜测。

### 买卖点

`time/base_ts, price, side, bsp_type, signal_type, features, confirmed`。前端标签应优先使用结构化 `side+bsp_type`，不要解析乱码或中文 `signal_type`。

## 5. 空数据与错误

- symbol 不存在：bars 当前返回空列表，不一定 404。
- overlay 没有完整 published run：返回 `engine=database:chan-published-empty` 和空结构。
- 窗口过大：413。
- 缺 from/to 或缺时区：422。
- levels 与后端固定映射不一致：400。
- DB 未就绪：503。

前端不能把“空 overlay”永久缓存为该标的没有缠论；应按 snapshot/version 和窗口缓存，并在 head update 后失效。

## 6. WebSocket

### K 线

`/ws/v1/realtime?token=...`：subscribe/unsubscribe/ping，Redis 不可用时轮询回退。

### 图表缠论

`/ws/v2/chart?token=...`：订阅 symbol/timeframe/window，接收 snapshot、delta、resync_required。客户端发现 sequence gap 或 source head 不一致时必须重新拉 HTTP snapshot，不能在旧 snapshot 上盲合并。

## 7. 性能合同

- K 线与 overlay 分开请求；K 线先显示，缠论异步叠加。
- 切周期不得等待完整缠论包才显示 K 线。
- 拖拽只请求缺失窗口，不全量清空重建。
- 周/月优先读 `chart_period_bars`。
- overlay 查询必须走 run_id + 时间窗口索引。
- published head 切换后旧 snapshot 仍完整可读，前端再原子替换。

目标：局域网内 K 线首屏 <500ms，overlay <2s；当前实测曾约 10s，说明 DB 查询/缓存/published run 完整性仍未达标。

## 8. A/B 联调验收样本

至少固定：

- `000001.SZ`：日线 2026-03-23 10.606 至 2026-04-27 11.570 端点案例。
- 一个北交所代码 `.BJ`。
- 一个停牌稀疏标的。
- 一个长期历史月线标的。

每个样本验证：切标的、七种图表周期、拖拽历史、confirmed/predictive、笔/线段/中枢/买卖点、刷新后稳定 id、WS 断线重连。
