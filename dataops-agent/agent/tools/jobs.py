from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from langchain_core.tools import tool


JOB_ID_ENV = "DATABRICKS_PIPELINE_JOB_ID"


def _enum_value(value: Any) -> str | None:
    """Convert Databricks SDK enums to plain strings."""
    if value is None:
        return None

    if hasattr(value, "value"):
        return str(value.value)

    return str(value)


def _to_iso(timestamp_ms: int | None) -> str | None:
    """Convert Databricks epoch milliseconds to UTC ISO timestamp."""
    if timestamp_ms is None:
        return None

    return datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=timezone.utc,
    ).isoformat()


def _duration_ms(item: Any) -> int | None:
    """Return run duration, falling back to end_time - start_time."""
    run_duration = getattr(item, "run_duration", None)

    if run_duration is not None:
        return run_duration

    start_time = getattr(item, "start_time", None)
    end_time = getattr(item, "end_time", None)

    if start_time is not None and end_time is not None:
        return end_time - start_time

    return None


def _extract_status(item: Any) -> dict[str, str | None]:
    """
    Normalize Databricks Jobs status.

    Newer Jobs APIs use `status`.
    Older SDK/API responses may still expose deprecated `state`.
    """

    status = getattr(item, "status", None)

    if status is not None:
        termination = getattr(status, "termination_details", None)

        return {
            "lifecycle_state": _enum_value(
                getattr(status, "state", None)
            ),
            "result_state": _enum_value(
                getattr(termination, "code", None)
                if termination is not None
                else None
            ),
            "message": (
                getattr(termination, "message", None)
                if termination is not None
                else None
            ),
        }

    # Backward-compatible fallback
    state = getattr(item, "state", None)

    if state is not None:
        return {
            "lifecycle_state": _enum_value(
                getattr(state, "life_cycle_state", None)
            ),
            "result_state": _enum_value(
                getattr(state, "result_state", None)
            ),
            "message": getattr(state, "state_message", None),
        }

    return {
        "lifecycle_state": None,
        "result_state": None,
        "message": None,
    }


def _resolve_job_id(job_id: int | None = None) -> int:
    if job_id is not None:
        return job_id

    raw_job_id = os.getenv(JOB_ID_ENV)

    if not raw_job_id:
        raise ValueError(
            f"{JOB_ID_ENV} is not configured"
        )

    try:
        return int(raw_job_id)
    except ValueError as exc:
        raise ValueError(
            f"{JOB_ID_ENV} must be an integer, got: {raw_job_id!r}"
        ) from exc


def _workspace_client() -> WorkspaceClient:
    """
    Use the named Databricks profile locally when configured.

    In deployed Agent Bricks, WorkspaceClient() can use the
    workload's Databricks authentication automatically.
    """
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE")

    if profile:
        return WorkspaceClient(profile=profile)

    return WorkspaceClient()


def fetch_latest_pipeline_run(
    *,
    client: WorkspaceClient | None = None,
    job_id: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the most recent run of the configured production pipeline.

    Strictly read-only.
    """
    resolved_job_id = _resolve_job_id(job_id)
    workspace = client or _workspace_client()

    runs = workspace.jobs.list_runs(
        job_id=resolved_job_id,
        limit=1,
    )

    latest = next(iter(runs), None)

    if latest is None:
        return {
            "found": False,
            "job_id": resolved_job_id,
            "message": "No runs found for this job.",
            "tasks": [],
        }

    if latest.run_id is None:
        raise RuntimeError(
            "Latest Databricks job run did not contain a run_id"
        )

    # get_run returns the complete run metadata and task collection.
    run = workspace.jobs.get_run(run_id=latest.run_id)

    run_status = _extract_status(run)

    tasks: list[dict[str, Any]] = []

    for task in run.tasks or []:
        task_status = _extract_status(task)

        tasks.append(
            {
                "task_key": task.task_key,
                "run_id": task.run_id,
                "lifecycle_state": task_status["lifecycle_state"],
                "result_state": task_status["result_state"],
                "message": task_status["message"],
                "start_time": _to_iso(task.start_time),
                "end_time": _to_iso(task.end_time),
                "duration_ms": _duration_ms(task),
                "run_page_url": getattr(task, "run_page_url", None),
            }
        )

    unsuccessful_tasks = [
        task["task_key"]
        for task in tasks
        if task["result_state"]
        not in (None, "SUCCESS")
    ]

    return {
        "found": True,
        "job_id": resolved_job_id,
        "run_id": run.run_id,
        "run_name": getattr(run, "run_name", None),
        "lifecycle_state": run_status["lifecycle_state"],
        "result_state": run_status["result_state"],
        "message": run_status["message"],
        "start_time": _to_iso(run.start_time),
        "end_time": _to_iso(run.end_time),
        "duration_ms": _duration_ms(run),
        "run_page_url": getattr(run, "run_page_url", None),
        "task_count": len(tasks),
        "unsuccessful_tasks": unsuccessful_tasks,
        "tasks": tasks,
    }


@tool
def get_latest_pipeline_run() -> dict[str, Any]:
    """
    Inspect the most recent run of the production Databricks
    Lakeflow pipeline.

    Returns the overall pipeline state and individual task states,
    including failure messages when available.

    This tool is read-only. It cannot start, cancel, repair,
    or modify a Databricks job.
    """
    return fetch_latest_pipeline_run()