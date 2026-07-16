import asyncio

from collector.lifecycle_reconciliation import build_reconciliation, render_markdown


class Conn:
    def __init__(self, *, statuses, mismatch_count=0, missing_count=0):
        self.statuses = statuses
        self.mismatch_count = mismatch_count
        self.missing_count = missing_count

    async def fetch(self, sql):
        if "group by status" in sql:
            return [
                {"status": status, "count": count, "oldest_age_seconds": 3}
                for status, count in self.statuses.items()
            ]
        return [{"observer_name": "observer", "last_outbox_id": 4, "updated_at": None}]

    async def fetchrow(self, sql):
        if "event_state" in sql:
            return {"count": self.mismatch_count, "samples": ["fp"] if self.mismatch_count else []}
        return {"count": self.missing_count, "samples": [{"run_id": 9}] if self.missing_count else []}


def test_reconciliation_passes_only_when_outbox_projection_and_heads_agree() -> None:
    report = asyncio.run(build_reconciliation(Conn(statuses={"completed": 10})))
    assert report["decision"] == "PASS"
    assert report["blockers"] == []
    assert "Decision: `PASS`" in render_markdown(report)


def test_reconciliation_reports_all_blocking_classes() -> None:
    report = asyncio.run(
        build_reconciliation(
            Conn(statuses={"completed": 8, "dead_letter": 1}, mismatch_count=2, missing_count=1)
        )
    )
    assert report["decision"] == "FAIL"
    assert report["blockers"] == [
        "outbox_not_drained",
        "current_projection_differs_from_event_replay",
        "published_head_missing_history_or_outbox",
    ]
