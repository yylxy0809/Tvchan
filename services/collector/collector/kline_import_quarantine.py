"""Transactional raw-source quarantine helpers for native K-line imports.

The helper deliberately accepts a callback for canonical writes.  This keeps
the source failure table independent from the canonical ``klines`` shape while
making the commit ordering explicit: accepted rows, quarantines, then resume
checkpoint, all in one database transaction.
"""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class ImportCheckpoint:
    source_ref: str
    source_checksum: str
    last_source_row: int | None


@dataclass(frozen=True)
class QuarantineRecord:
    source_name: str
    source_ref: str
    source_row: int
    symbol_text: str | None
    timeframe: str | None
    raw_ts: str | None
    reason: str
    raw_payload: dict[str, Any]


AcceptedWriter = Callable[[Any], Awaitable[int]]


async def create_import_run(conn, *, import_run_id: UUID, source_name: str, parameters: dict[str, Any]) -> None:
    await conn.execute(
        """
        insert into kline_import_runs (import_run_id, source_name, status, parameters)
        values ($1, $2, 'running', $3::jsonb)
        on conflict (import_run_id) do nothing
        """,
        import_run_id,
        source_name,
        json.dumps(parameters, default=str, sort_keys=True),
    )


async def commit_import_batch(
    conn,
    *,
    import_run_id: UUID,
    checkpoint: ImportCheckpoint,
    quarantines: Iterable[QuarantineRecord],
    write_accepted: AcceptedWriter,
) -> int:
    """Atomically write valid bars, raw failures, and the completed checkpoint.

    If any step raises, the transaction is rolled back.  The caller can retry
    the same source identity: canonical upserts are idempotent and the stable
    quarantine key prevents duplicate forensic rows.
    """
    records = list(quarantines)
    async with conn.transaction():
        accepted_rows = await write_accepted(conn)
        if records:
            await conn.executemany(
                """
                insert into kline_import_quarantine (
                    import_run_id, source_name, source_ref, source_row,
                    symbol_text, timeframe, raw_ts, reason, raw_payload
                ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                on conflict (source_name, source_ref, source_row, reason) do nothing
                """,
                [
                    (
                        import_run_id,
                        item.source_name,
                        item.source_ref,
                        item.source_row,
                        item.symbol_text,
                        item.timeframe,
                        item.raw_ts,
                        item.reason,
                        json.dumps(item.raw_payload, default=str, sort_keys=True),
                    )
                    for item in records
                ],
            )
        # This statement is intentionally last.  A process crash before the
        # transaction commits leaves neither data writes nor checkpoint state.
        await conn.execute(
            """
            insert into kline_import_checkpoints (
                import_run_id, source_ref, source_checksum, status,
                accepted_rows, quarantined_rows, last_source_row, completed_at
            ) values ($1, $2, $3, 'completed', $4, $5, $6, now())
            on conflict (import_run_id, source_ref, source_checksum) do update
            set status = 'completed',
                accepted_rows = excluded.accepted_rows,
                quarantined_rows = excluded.quarantined_rows,
                last_source_row = excluded.last_source_row,
                error_message = null,
                updated_at = now(),
                completed_at = now()
            """,
            import_run_id,
            checkpoint.source_ref,
            checkpoint.source_checksum,
            accepted_rows,
            len(records),
            checkpoint.last_source_row,
        )
    return accepted_rows
