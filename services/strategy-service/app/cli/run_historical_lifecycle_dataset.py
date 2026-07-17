from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.db import create_pool
from app.engine.historical_lifecycle_dataset import export_historical_lifecycle_dataset
from app.engine.time_utils import utc_time
from app.repositories.historical_lifecycle_repo import HistoricalLifecycleRepository


async def _run(
    *,
    replay_batch_id: int,
    expected_contract_hash: str,
    effective_cutoff: datetime,
    output_dir: Path,
    prefetch: int = 1000,
) -> dict:
    pool = await create_pool(min_size=1, max_size=1)
    try:
        repository = HistoricalLifecycleRepository(pool)
        async with repository.open_snapshot(
            replay_batch_id=replay_batch_id,
            expected_contract_hash=expected_contract_hash,
            effective_cutoff=effective_cutoff,
        ) as snapshot:
            return await export_historical_lifecycle_dataset(
                snapshot=snapshot,
                effective_cutoff=effective_cutoff,
                output_dir=output_dir,
                prefetch=prefetch,
            )
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export one sealed historical replay lifecycle ledger by causal effective time"
    )
    parser.add_argument("--replay-batch-id", type=int, required=True)
    parser.add_argument("--expected-contract-hash", required=True)
    parser.add_argument("--effective-cutoff", type=utc_time, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefetch", type=int, default=1000)
    args = parser.parse_args()
    if args.prefetch < 1 or args.prefetch > 10000:
        parser.error("--prefetch must be between 1 and 10000")
    manifest = asyncio.run(
        _run(
            replay_batch_id=args.replay_batch_id,
            expected_contract_hash=args.expected_contract_hash,
            effective_cutoff=args.effective_cutoff,
            output_dir=args.output_dir,
            prefetch=args.prefetch,
        )
    )
    print(
        json.dumps(
            {
                "dataset_validation": manifest["dataset_validation"],
                "row_count": manifest["row_count"],
                "official_jsonl_sha256": manifest["official_jsonl_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
