"""Compatibility entrypoint for the canonical lifecycle observer worker."""

from __future__ import annotations

import asyncio

from collector.lifecycle_observer_worker import main as worker_main


def main() -> int:
    return asyncio.run(worker_main())


if __name__ == "__main__":
    raise SystemExit(main())
