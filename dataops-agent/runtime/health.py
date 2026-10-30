"""Read-only aggregation for the Daily Health Review UI."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

from agent.tools.jobs import fetch_latest_pipeline_run
from agent.tools.observability import fetch_latest_observability_summary

HealthFetcher = Callable[[], dict[str, Any]]

_CATEGORIES = {
    "freshness": "freshness_status",
    "row_counts": "volume_status",
    "quality": "quality_status",
    "reconciliation": "status",
}
_TERMINAL_FAILURES = {
    "CANCELED",
    "FAILED",
    "INTERNAL_ERROR",
    "SKIPPED",
    "TIMEDOUT",
    "UPSTREAM_FAILED",
}
_ACTIVE_STATES = {"BLOCKED", "PENDING", "QUEUED", "RUNNING", "TERMINATING", "WAITING_FOR_RETRY"}


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_error(source: str, error: BaseException) -> dict[str, str]:
    message = " ".join(str(error).split())[:500] or "The data source returned an error."
    message = re.sub(r"(?i)\bbearer\s+[^\s,;]+", "Bearer [REDACTED]", message)
    message = re.sub(
        r"(?i)\b(token|password|secret|authorization|credential)\b\s*[:=]\s*[^\s,;]+",
        r"\1=[REDACTED]",
        message,
    )
    return {"source": source, "type": type(error).__name__, "message": message}


def _pipeline_health(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload.get("found"):
        return {
            "available": True,
            "found": False,
            "status": "UNKNOWN",
            "job_id": payload.get("job_id"),
            "message": payload.get("message") or "No completed or active run was found.",
            "task_counts": {"total": 0, "passed": 0, "failed": 0, "active": 0, "unknown": 0},
            "tasks": [],
        }

    result_state = str(payload.get("result_state") or "").upper()
    lifecycle_state = str(payload.get("lifecycle_state") or "").upper()
    if result_state == "SUCCESS":
        status = "PASS"
    elif result_state in _TERMINAL_FAILURES or (result_state and result_state != "SUCCESS"):
        status = "FAIL"
    elif lifecycle_state in _ACTIVE_STATES:
        status = "RUNNING"
    else:
        status = "UNKNOWN"

    task_counts = {"total": 0, "passed": 0, "failed": 0, "active": 0, "unknown": 0}
    tasks = payload.get("tasks") if isinstance(payload.get("tasks"), list) else []
    task_counts["total"] = len(tasks)
    for task in tasks:
        task_result = str(task.get("result_state") or "").upper()
        task_lifecycle = str(task.get("lifecycle_state") or "").upper()
        if task_result == "SUCCESS":
            task_counts["passed"] += 1
        elif task_result:
            task_counts["failed"] += 1
        elif task_lifecycle in _ACTIVE_STATES:
            task_counts["active"] += 1
        else:
            task_counts["unknown"] += 1

    return {
        "available": True,
        "found": True,
        "status": status,
        "job_id": payload.get("job_id"),
        "run_id": payload.get("run_id"),
        "run_name": payload.get("run_name"),
        "lifecycle_state": payload.get("lifecycle_state"),
        "result_state": payload.get("result_state"),
        "message": payload.get("message"),
        "start_time": _iso(payload.get("start_time")),
        "end_time": _iso(payload.get("end_time")),
        "duration_ms": payload.get("duration_ms"),
        "run_page_url": payload.get("run_page_url"),
        "task_counts": task_counts,
        "unsuccessful_tasks": payload.get("unsuccessful_tasks") or [],
        "tasks": tasks,
    }


def _evidence_ref(category: str, row: dict[str, Any], index: int) -> str:
    identity = (
        row.get("entity_name")
        or row.get("reconciliation_name")
        or row.get("metric_name")
        or f"check-{index + 1}"
    )
    metric = row.get("metric_name")
    suffix = f":{metric}" if metric and metric != identity else ""
    return f"observability:{category}:{identity}{suffix}"


def _monitoring_health(
    payload: dict[str, Any], *, generated_at: datetime, stale_after_hours: float
) -> dict[str, Any]:
    counts = {"total": 0, "pass": 0, "warn": 0, "fail": 0, "unknown": 0}
    categories: dict[str, Any] = {}
    issues: list[dict[str, Any]] = []
    monitoring_runs: list[datetime] = []
    monitoring_references: list[datetime] = []
    synthetic_dates: set[str] = set()

    for category, status_column in _CATEGORIES.items():
        source = payload.get(category) if isinstance(payload.get(category), dict) else {}
        rows = source.get("rows") if isinstance(source.get("rows"), list) else []
        category_counts = {"total": len(rows), "pass": 0, "warn": 0, "fail": 0, "unknown": 0}
        for index, row in enumerate(rows):
            status = str(row.get(status_column) or "UNKNOWN").upper()
            bucket = status.lower() if status in {"PASS", "WARN", "FAIL"} else "unknown"
            category_counts[bucket] += 1
            counts[bucket] += 1
            if status != "PASS":
                issues.append(
                    {
                        "evidence_ref": _evidence_ref(category, row, index),
                        "category": category,
                        "status": status,
                        "entity_name": row.get("entity_name"),
                        "metric_name": row.get("metric_name"),
                        "reconciliation_name": row.get("reconciliation_name"),
                        "details": row,
                    }
                )

            run_at = _parse_datetime(row.get("monitoring_run_at"))
            reference_at = _parse_datetime(row.get("monitoring_reference_at"))
            if run_at:
                monitoring_runs.append(run_at)
            if reference_at:
                monitoring_references.append(reference_at)
            for field in ("latest_partition_date", "activity_date", "metric_date"):
                if row.get(field) is not None:
                    synthetic_dates.add(_iso(row[field]) or "")

        counts["total"] += len(rows)
        categories[category] = {
            "check_count": len(rows),
            "counts": category_counts,
            "issue_count": category_counts["warn"] + category_counts["fail"] + category_counts["unknown"],
        }

    monitoring_run_at = max(monitoring_runs) if monitoring_runs else None
    monitoring_reference_at = max(monitoring_references) if monitoring_references else None
    if monitoring_run_at:
        age_hours = max(0.0, (generated_at - monitoring_run_at).total_seconds() / 3600)
        staleness_status = "STALE" if age_hours > stale_after_hours else "CURRENT"
    else:
        age_hours = None
        staleness_status = "UNKNOWN"

    synthetic_reference_date = max(synthetic_dates) if synthetic_dates else None
    monitoring_status = (
        "UNKNOWN"
        if counts["total"] == 0
        else "FAIL"
        if counts["fail"]
        else "WARN"
        if counts["warn"] or counts["unknown"]
        else "PASS"
    )
    return {
        "available": True,
        "status": monitoring_status,
        "monitoring_run_at": _iso(monitoring_run_at),
        "monitoring_run_at_source": "dq_entity_freshness" if monitoring_run_at else None,
        "monitoring_reference_at": _iso(monitoring_reference_at),
        "synthetic_reference_date": synthetic_reference_date,
        "synthetic_reference_dates_observed": sorted(value for value in synthetic_dates if value),
        "synthetic_reference_date_consistent": len(synthetic_dates) <= 1,
        "pipeline_association_verified": False,
        "pipeline_association": "UNVERIFIED",
        "checks": counts,
        "categories": categories,
        "issues": issues,
        "staleness": {
            "status": staleness_status,
            "age_hours": round(age_hours, 2) if age_hours is not None and math.isfinite(age_hours) else None,
            "threshold_hours": stale_after_hours,
        },
    }


async def build_daily_health_summary(
    *,
    jobs_fetcher: HealthFetcher = fetch_latest_pipeline_run,
    observability_fetcher: HealthFetcher = fetch_latest_observability_summary,
    now: datetime | None = None,
    stale_after_hours: float = 36.0,
) -> dict[str, Any]:
    """Collect read-only health evidence without invoking an LLM or Genie."""
    generated_at = now or datetime.now(timezone.utc)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    generated_at = generated_at.astimezone(timezone.utc)

    results = await asyncio.gather(
        asyncio.to_thread(jobs_fetcher),
        asyncio.to_thread(observability_fetcher),
        return_exceptions=True,
    )
    source_errors: list[dict[str, str]] = []
    jobs_result, observability_result = results

    if isinstance(jobs_result, BaseException):
        source_errors.append(_safe_error("jobs", jobs_result))
        pipeline = {"available": False, "found": None, "status": "UNAVAILABLE", "task_counts": None, "tasks": []}
    else:
        pipeline = _pipeline_health(jobs_result)

    if isinstance(observability_result, BaseException):
        source_errors.append(_safe_error("observability", observability_result))
        monitoring = {
            "available": False,
            "status": "UNAVAILABLE",
            "monitoring_run_at": None,
            "monitoring_run_at_source": None,
            "monitoring_reference_at": None,
            "synthetic_reference_date": None,
            "pipeline_association_verified": False,
            "pipeline_association": "UNVERIFIED",
            "checks": None,
            "categories": {},
            "issues": [],
            "staleness": {"status": "UNKNOWN", "age_hours": None, "threshold_hours": stale_after_hours},
        }
    else:
        monitoring = _monitoring_health(
            observability_result,
            generated_at=generated_at,
            stale_after_hours=stale_after_hours,
        )

    available_count = int(pipeline["available"]) + int(monitoring["available"])
    partial = available_count == 1
    if available_count == 0:
        overall_status = "UNAVAILABLE"
    elif pipeline["status"] == "FAIL" or monitoring["status"] == "FAIL":
        overall_status = "FAIL"
    elif partial:
        overall_status = "PARTIAL"
    elif monitoring["status"] == "WARN" or monitoring["staleness"]["status"] == "STALE":
        overall_status = "WARN"
    elif monitoring["status"] == "UNKNOWN":
        overall_status = "UNKNOWN"
    elif pipeline["status"] in {"RUNNING", "UNKNOWN"}:
        overall_status = pipeline["status"]
    else:
        overall_status = "PASS"

    return {
        "schema_version": "1.0",
        "generated_at": generated_at.isoformat(),
        "overall_status": overall_status,
        "availability": {
            "jobs": pipeline["available"],
            "observability": monitoring["available"],
            "partial": partial,
        },
        "pipeline": pipeline,
        "monitoring": monitoring,
        "source_errors": source_errors,
        "limitations": [
            "Pipeline execution and monitoring evidence are reported separately; their association is unverified.",
            "This summary covers configured monitoring checks only and is not a claim of broader platform health.",
            "The reconciliation source is a current snapshot and does not expose its own monitoring timestamp.",
        ],
        "investigation": {
            "enabled": False,
            "status": "PLANNED_V2_6B",
            "action": "generate_dataops_investigation_report",
        },
    }
