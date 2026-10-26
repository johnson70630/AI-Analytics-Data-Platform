from types import SimpleNamespace

import pytest

from agent.tools.jobs import (
    _resolve_job_id,
    fetch_latest_pipeline_run,
)


class FakeJobs:
    def __init__(self, runs=None, full_run=None):
        self._runs = runs or []
        self._full_run = full_run

    def list_runs(self, job_id, limit=1):
        return iter(self._runs)

    def get_run(self, run_id):
        return self._full_run


class FakeClient:
    def __init__(self, jobs):
        self.jobs = jobs


def state(
    lifecycle="TERMINATED",
    result="SUCCESS",
    message=None,
):
    return SimpleNamespace(
        life_cycle_state=lifecycle,
        result_state=result,
        state_message=message,
    )


def task(
    key,
    run_id,
    result="SUCCESS",
    message=None,
):
    return SimpleNamespace(
        task_key=key,
        run_id=run_id,
        state=state(result=result, message=message),
        status=None,
        start_time=1000,
        end_time=2000,
        run_duration=1000,
        run_page_url=f"https://example/{run_id}",
    )


def test_successful_pipeline_run():
    tasks = [
        task("run_daily_generator", 1),
        task("dbt_staging", 2),
        task("dbt_user_state", 3),
        task("dbt_snapshot", 4),
        task("dbt_dimensions", 5),
        task("dbt_silver_facts", 6),
        task("dbt_gold", 7),
        task("dbt_observability", 8),
        task("dbt_tests", 9),
    ]

    latest = SimpleNamespace(run_id=999)

    full_run = SimpleNamespace(
        run_id=999,
        run_name="production-pipeline",
        state=state(),
        status=None,
        start_time=1000,
        end_time=5000,
        run_duration=4000,
        run_page_url="https://example/999",
        tasks=tasks,
    )

    client = FakeClient(FakeJobs([latest], full_run))

    result = fetch_latest_pipeline_run(
        client=client,
        job_id=123,
    )

    assert result["found"] is True
    assert result["run_id"] == 999
    assert result["result_state"] == "SUCCESS"
    assert result["task_count"] == 9
    assert result["unsuccessful_tasks"] == []


def test_failed_task():
    tasks = [
        task("dbt_staging", 1),
        task(
            "dbt_silver_facts",
            2,
            result="FAILED",
            message="dbt model failed",
        ),
    ]

    latest = SimpleNamespace(run_id=999)

    full_run = SimpleNamespace(
        run_id=999,
        run_name="production-pipeline",
        state=state(result="FAILED"),
        status=None,
        start_time=1000,
        end_time=5000,
        run_duration=4000,
        run_page_url="https://example/999",
        tasks=tasks,
    )

    client = FakeClient(FakeJobs([latest], full_run))

    result = fetch_latest_pipeline_run(
        client=client,
        job_id=123,
    )

    assert result["result_state"] == "FAILED"
    assert result["unsuccessful_tasks"] == [
        "dbt_silver_facts"
    ]

    failed = result["tasks"][1]

    assert failed["result_state"] == "FAILED"
    assert failed["message"] == "dbt model failed"


def test_no_runs():
    client = FakeClient(FakeJobs([]))

    result = fetch_latest_pipeline_run(
        client=client,
        job_id=123,
    )

    assert result["found"] is False
    assert result["tasks"] == []


def test_missing_job_id(monkeypatch):
    monkeypatch.delenv(
        "DATABRICKS_PIPELINE_JOB_ID",
        raising=False,
    )

    with pytest.raises(ValueError):
        _resolve_job_id()


def test_invalid_job_id(monkeypatch):
    monkeypatch.setenv(
        "DATABRICKS_PIPELINE_JOB_ID",
        "not-a-number",
    )

    with pytest.raises(ValueError):
        _resolve_job_id()