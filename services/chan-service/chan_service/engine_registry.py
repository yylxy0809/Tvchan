from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AnalyzerEngine:
    name: str
    mode: str
    module_path: str = ""


def resolve_engine() -> AnalyzerEngine:
    configured_mode = os.getenv("CHAN_ENGINE_MODE", "").strip().lower()
    if configured_mode in {"", "module_b", "chan_py", "chan.py"}:
        return AnalyzerEngine(
            name="module-b:chan.py",
            mode="chan_py",
            module_path=os.getenv("CHAN_PY_PATH", "").strip() or default_chan_py_path(),
        )
    return AnalyzerEngine(name=configured_mode, mode="unsupported")


def default_chan_py_path() -> str:
    return str(Path(__file__).resolve().parents[3] / "work" / "vendor" / "chan.py-main")
