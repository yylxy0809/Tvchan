# Phase 5 Chan Service Runbook

This phase keeps the Chan Analysis service contract stable while preparing the
real `Vespa314/chan.py` adapter.

Start it:

```powershell
cd services/chan-service
pip install -r requirements.txt
uvicorn chan_service.main:app --host 127.0.0.1 --port 8010
```

Health:

```text
GET http://127.0.0.1:8010/health
```

Analyze:

```text
POST http://127.0.0.1:8010/analyze
```

## Current Engine Modes

- default: placeholder fallback
- optional: real `chan.py` adapter via `CHAN_PY_PATH`

Health now returns:

```json
{
  "status": "ok",
  "engine": "placeholder",
  "mode": "placeholder",
  "status": "fallback"
}
```

This repo now includes a built-in adapter for a local open-source `chan.py`
checkout. By default the startup script points to:

- `C:\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f\work\vendor\chan.py-main`

If you want to use another path, set `CHAN_PY_PATH` before start:

```powershell
$env:CHAN_PY_PATH="D:\path\to\chan.py-main"
uvicorn chan_service.main:app --host 127.0.0.1 --port 8010
```

When the adapter is configured and loads successfully, `/health` reports
`engine=chan.py`, and `/analyze` includes `"engine": "chan.py"` in the response.

If the local checkout is missing or incompatible, the service falls back to the
placeholder analyzer so the frontend contract stays available.
