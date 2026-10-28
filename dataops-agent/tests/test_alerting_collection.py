from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from agent.alerting import RunCursor, fetch_completed_pipeline_runs_since


def state(result="SUCCESS"):
    return SimpleNamespace(
        life_cycle_state="TERMINATED",
        result_state=result,
        state_message=None,
    )


def base_run(run_id: int, completed_second: int):
    return SimpleNamespace(run_id=run_id, end_time=completed_second * 1000)


def full_run(run_id: int, completed_second: int):
    return SimpleNamespace(
        run_id=run_id,
        run_name="daily-pipeline",
        state=state(),
        status=None,
        start_time=(completed_second - 1) * 1000,
        end_time=completed_second * 1000,
        run_duration=1000,
        run_page_url=f"https://example/{run_id}",
        tasks=[],
    )


class PaginatedFakeJobs:
    def __init__(self):
        self.calls = []
        self.get_calls = []
        self.pages_read = 0
        self._runs = [
            base_run(104, 4),
            base_run(103, 3),
            base_run(102, 2),
            base_run(101, 1),
        ]

    def list_runs(self, **kwargs):
        self.calls.append(kwargs)
        page_size = kwargs["limit"]
        for index in range(0, len(self._runs), page_size):
            self.pages_read += 1
            yield from self._runs[index : index + page_size]

    def get_run(self, run_id):
        self.get_calls.append(run_id)
        completed_second = run_id - 100
        return full_run(run_id, completed_second)


class FakeClient:
    def __init__(self):
        self.jobs = PaginatedFakeJobs()


def test_multiple_completed_runs_are_paginated_and_processed_chronologically():
    client = FakeClient()
    cursor = RunCursor(
        completed_at=datetime.fromtimestamp(2, tz=timezone.utc),
        run_id=102,
    )

    result = fetch_completed_pipeline_runs_since(
        cursor=cursor,
        client=client,
        job_id=123,
        page_size=2,
    )

    assert client.jobs.pages_read == 2
    assert client.jobs.calls == [
        {
            "job_id": 123,
            "completed_only": True,
            "expand_tasks": False,
            "limit": 2,
        }
    ]
    assert [run["run_id"] for run in result.runs] == [103, 104]
    assert client.jobs.get_calls == [103, 104]
    assert result.scanned_run_count == 4
    assert result.estimated_page_count == 2
    assert result.next_cursor.run_id == 104


def test_cursor_retry_returns_no_runs_and_preserves_cursor():
    client = FakeClient()
    cursor = RunCursor(
        completed_at=datetime.fromtimestamp(4, tz=timezone.utc),
        run_id=104,
    )

    result = fetch_completed_pipeline_runs_since(
        cursor=cursor,
        client=client,
        job_id=123,
        page_size=2,
    )

    assert result.runs == []
    assert result.next_cursor == cursor
    assert client.jobs.get_calls == []


def test_initial_baseline_can_select_only_latest_completed_run():
    client = FakeClient()

    result = fetch_completed_pipeline_runs_since(
        cursor=None,
        client=client,
        job_id=123,
        page_size=2,
        max_runs=1,
    )

    assert [run["run_id"] for run in result.runs] == [104]
    assert result.next_cursor.run_id == 104
    assert result.scanned_run_count == 1


def test_invalid_page_size_is_rejected_before_api_call():
    client = FakeClient()

    try:
        fetch_completed_pipeline_runs_since(
            cursor=None,
            client=client,
            job_id=123,
            page_size=25,
        )
    except ValueError as exc:
        assert "between 1 and 24" in str(exc)
    else:
        raise AssertionError("page_size=25 should fail")
    assert client.jobs.calls == []
