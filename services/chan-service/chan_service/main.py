from __future__ import annotations

from fastapi import FastAPI

from chan_service.analyzer import analyze, get_engine_metadata
from chan_service.models import ChanAnalyzeRequest, ChanAnalyzeResponse

app = FastAPI(title="Chan Analysis Service", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", **get_engine_metadata()}


@app.post("/analyze", response_model=ChanAnalyzeResponse)
def analyze_endpoint(request: ChanAnalyzeRequest) -> ChanAnalyzeResponse:
    return analyze(request)
