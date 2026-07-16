from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.lifecycle_datasets import build_lifecycle_datasets
from app.engine.time_utils import utc_time
from app.repositories.lifecycle_repo import LifecycleRepository


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n" for row in rows)


async def _run(*, as_of_time: datetime, output_dir: Path) -> dict:
    pool = await create_pool(min_size=1, max_size=1)
    try:
        events, current = await LifecycleRepository(pool).snapshot_as_of(as_of_time)
    finally:
        await pool.close()
    payload = build_lifecycle_datasets(events=events, current=current, as_of_time=as_of_time)
    for name, rows in payload["datasets"].items():
        _atomic_write(output_dir / f"{name}.jsonl", _jsonl(rows))
    manifest = {key: value for key, value in payload.items() if key != "datasets"}
    _atomic_write(output_dir / "manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Build lifecycle-only strategy datasets")
    parser.add_argument("--as-of", type=utc_time, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = asyncio.run(_run(as_of_time=args.as_of, output_dir=args.output_dir))
    print(json.dumps({"decision": manifest["decision"], "blockers": manifest["blockers"]}, sort_keys=True))


if __name__ == "__main__":
    main()
