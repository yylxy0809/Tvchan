# Phase 3 Realtime Runbook

## Scope

This phase adds a minimal WebSocket channel:

- `ws://localhost:8001/ws/v1/realtime?token=dev-local-token`
- Supports `ping`, `subscribe`, `unsubscribe`.
- Emits `bar_update` messages for subscribed symbols/timeframes.

The current implementation prefers Redis fanout from collector events and falls
back to the development polling producer when Redis is unavailable.

## Runtime Flow

```text
collector.market_fill
  -> writes latest K-line rows to PostgreSQL/TimescaleDB
  -> publishes latest bar to Redis channel market:bar_updates
API /ws/v1/realtime
  -> subscribes to market:bar_updates
  -> filters by active WebSocket subscriptions
  -> sends bar_update to the frontend
```

Message shape sent to the browser:

```json
{
  "type": "bar_update",
  "seq": 1,
  "symbol": "000001.SZ",
  "timeframe": "5f",
  "bar": {
    "time": 1781558400,
    "open": 10.8,
    "high": 10.9,
    "low": 10.7,
    "close": 10.85,
    "volume": 123456
  },
  "source": "redis"
}
```

If Redis cannot be imported or reached, the API keeps the WebSocket open and
uses the older polling producer. In fallback mode the `source` field is omitted.

## Start Redis

Redis is included in the local compose file:

```powershell
docker compose -f deploy/docker-compose.dev.yml up -d redis
```

The default URL used by both API and collector scripts is:

```text
redis://127.0.0.1:6379/0
```

## Manual Probe

Use the frontend at:

```text
http://localhost:5173
```

The status panel shows `Realtime open` when connected and a short note when a
`bar_update` is received.

Trigger one update pass:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-market-fill-worker.ps1 `
  -Provider pytdx `
  -Symbols 000001.SZ `
  -Limit 300 `
  -SkipChan `
  -Sleep 0
```

The worker prints a `bar_published` event for each timeframe. A successful Redis
publish looks like:

```json
{"event":"bar_published","symbol":"000001.SZ","timeframe":"5f","published":true}
```
