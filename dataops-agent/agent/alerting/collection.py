"""Read-only collection of newly completed Lakeflow runs."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from agent.alerting.models import CompletedRunCollection, RunCursor
from agent.tools.jobs import (
    _duration_ms,
    _extract_status,
    _resolve_job_id,
    _to_iso,
    _workspace_client,
)


def _completed_at_ms(run: Any) -> int | None:
    value = getattr(run, "end_time", None)
    return int(value) if value is not None else None


def _cursor_key(cursor: RunCursor) -> tuple[int, int]:
    return (int(cursor.completed_at.timestamp() * 1000), cursor.run_id)


def _normalize_run(run: Any, *, job_id: int) -> dict[str, Any]:
    run_status = _extract_status(run)
    tasks: list[dict[str, Any]] = []
    for task in getattr(run, "tasks", None) or []:
        task_status = _extract_status(task)
        tasks.append(
            {
                "task_key": getattr(task, "task_key", None),
                "run_id": getattr(task, "run_id", None),
                "lifecycle_state": task_status["lifecycle_state"],
                "result_state": task_status["result_state"],
                "message": task_status["message"],
                "start_time": _to_iso(getattr(task, "start_time", None)),
                "end_time": _to_iso(getattr(task, "end_time", None)),
                "duration_ms": _duration_ms(task),
                "run_page_url": getattr(task, "run_page_url", None),
            }
        )
    return {
        "found": True,
        "job_id": job_id,
        "run_id": getattr(run, "run_id", None),
        "run_name": getattr(run, "run_name", None),
        "lifecycle_state": run_status["lifecycle_state"],
        "result_state": run_status["result_state"],
        "message": run_status["message"],
        "start_time": _to_iso(getattr(run, "start_time", None)),
        "end_time": _to_iso(getattr(run, "end_time", None)),
        "duration_ms": _duration_ms(run),
        "run_page_url": getattr(run, "run_page_url", None),
        "task_count": len(tasks),
        "unsuccessful_tasks": sorted(
            str(task["task_key"])
            for task in tasks
            if task["result_state"] not in (None, "SUCCESS")
        ),
        "tasks": tasks,
    }


def fetch_completed_pipeline_runs_since(
    *,
    cursor: RunCursor | None,
    client: WorkspaceClient | None = None,
    job_id: int | None = None,
    page_size: int = 20,
    max_runs: int | None = None,
) -> CompletedRunCollection:
    """Collect all completed runs after a cursor; the SDK follows page tokens."""

    if not 1 <= page_size <= 24:
        raise ValueError("page_size must be between 1 and 24")
    if max_runs is not None and max_runs < 1:
        raise ValueError("max_runs must be positive")
    workspace = client or _workspace_client()
    resolved_job_id = _resolve_job_id(job_id)
    cursor_key = _cursor_key(cursor) if cursor else None
    candidates: list[tuple[tuple[int, int], int]] = []
    scanned = 0

    for base_run in workspace.jobs.list_runs(
        job_id=resolved_job_id,
        completed_only=True,
        expand_tasks=False,
        limit=page_size,
    ):
        scanned += 1
        run_id = getattr(base_run, "run_id", None)
        completed_at_ms = _completed_at_ms(base_run)
        if run_id is None or completed_at_ms is None:
            continue
        key = (completed_at_ms, int(run_id))
        if cursor_key is None or key > cursor_key:
            candidates.append((key, int(run_id)))
        if max_runs is not None and cursor is None and len(candidates) >= max_runs:
            break

    candidates.sort(key=lambda item: item[0])
    runs = [
        _normalize_run(workspace.jobs.get_run(run_id=run_id), job_id=resolved_job_id)
        for _, run_id in candidates
    ]
    next_cursor = cursor
    if candidates:
        completed_at_ms, latest_run_id = candidates[-1][0]
        next_cursor = RunCursor(
            completed_at=datetime.fromtimestamp(completed_at_ms / 1000, tz=timezone.utc),
            run_id=latest_run_id,
        )
    return CompletedRunCollection(
        runs=runs,
        next_cursor=next_cursor,
        scanned_run_count=scanned,
        selected_run_count=len(runs),
        estimated_page_count=math.ceil(scanned / page_size) if scanned else 0,
        page_size=page_size,
    )
