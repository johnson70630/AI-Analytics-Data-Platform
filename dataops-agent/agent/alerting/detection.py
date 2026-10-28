"""Pure conversion of Jobs and observability evidence into stable observations."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

from agent.alerting.models import (
    DetectionResult,
    EvidenceAssociation,
    IncidentFingerprint,
    MonitoringObservation,
    MonitoringSourceError,
    ObservationStatus,
)


_OBSERVABILITY_CATEGORIES = {
    "freshness": "freshness_status",
    "row_counts": "volume_status",
    "quality": "quality_status",
    "reconciliation": "status",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_status(value: Any) -> ObservationStatus:
    status = str(value or "").upper()
    if status == "PASS" or status == "SUCCESS":
        return "PASS"
    if status == "WARN" or status == "WARNING":
        return "WARN"
    if status in {
        "FAIL",
        "FAILED",
        "ERROR",
        "INTERNAL_ERROR",
        "TIMED_OUT",
        "CANCELED",
        "CANCELLED",
    }:
        return "FAIL"
    if status == "UNAVAILABLE":
        return "UNAVAILABLE"
    return "UNKNOWN"


def incident_fingerprint(
    *,
    source: str,
    category: str,
    identity_parts: list[Any],
) -> IncidentFingerprint:
    """Build a stable fingerprint without status, run, or date fields."""

    normalized_parts = [str(part).strip().lower() for part in identity_parts if part not in (None, "")]
    identity = ":".join([source, category, *normalized_parts])
    return IncidentFingerprint(
        source=source,
        category=category,
        identity=identity,
        digest=_canonical_hash(identity),
    )


def _observation(
    *,
    fingerprint: IncidentFingerprint,
    status: ObservationStatus,
    observed_at: datetime,
    observation_token: Any,
    evidence: dict[str, Any],
    association: EvidenceAssociation,
    reference_time: datetime | None = None,
    synthetic_reference_date: str | None = None,
    execution_run_id: int | None = None,
) -> MonitoringObservation:
    return MonitoringObservation(
        observation_id=_canonical_hash(
            {
                "fingerprint": fingerprint.digest,
                "observation_token": observation_token,
            }
        ),
        fingerprint=fingerprint,
        status=status,
        observed_at=observed_at,
        reference_time=reference_time,
        synthetic_reference_date=synthetic_reference_date,
        execution_run_id=execution_run_id,
        association=association,
        evidence=evidence,
    )


def detect_execution_observations(
    run: dict[str, Any],
    *,
    collected_at: datetime | None = None,
) -> list[MonitoringObservation]:
    """Detect task-level execution observations without calling the Jobs API."""

    fallback_time = collected_at or _utc_now()
    observed_at = _parse_datetime(run.get("end_time")) or fallback_time
    run_id = run.get("run_id")
    job_id = run.get("job_id")
    association = EvidenceAssociation(
        status="NOT_APPLICABLE",
        reason="jobs_evidence_is_the_execution_record",
        execution_run_id=run_id,
    )
    observations: list[MonitoringObservation] = []
    tasks = run.get("tasks") or []
    for task in tasks:
        task_key = str(task.get("task_key") or "unknown_task")
        fingerprint = incident_fingerprint(
            source="jobs",
            category="execution",
            identity_parts=[job_id, "task", task_key],
        )
        observations.append(
            _observation(
                fingerprint=fingerprint,
                status=_normalize_status(task.get("result_state")),
                observed_at=_parse_datetime(task.get("end_time")) or observed_at,
                observation_token={"run_id": run_id, "task_key": task_key},
                evidence={"run": run, "task": task},
                association=association,
                execution_run_id=run_id,
            )
        )

    run_status = _normalize_status(run.get("result_state"))
    task_failures = any(item.status == "FAIL" for item in observations)
    if not tasks or (run_status == "FAIL" and not task_failures):
        fingerprint = incident_fingerprint(
            source="jobs",
            category="execution",
            identity_parts=[job_id, "pipeline"],
        )
        observations.append(
            _observation(
                fingerprint=fingerprint,
                status=run_status,
                observed_at=observed_at,
                observation_token={"run_id": run_id, "scope": "pipeline"},
                evidence={"run": run},
                association=association,
                execution_run_id=run_id,
            )
        )
    return observations


def assess_observability_association(
    summary: dict[str, Any],
    run: dict[str, Any] | None,
) -> EvidenceAssociation:
    """Associate a monitoring snapshot only when its execution timestamp fits the run."""

    monitoring_times = sorted(
        {
            value
            for row in (summary.get("freshness") or {}).get("rows") or []
            if (value := _parse_datetime(row.get("monitoring_run_at"))) is not None
        }
    )
    run_id = run.get("run_id") if run else None
    if run is None:
        return EvidenceAssociation(
            status="UNCERTAIN",
            reason="no_execution_evidence_was_available_for_association",
            monitoring_run_at=monitoring_times,
        )
    start = _parse_datetime(run.get("start_time"))
    end = _parse_datetime(run.get("end_time"))
    if not monitoring_times:
        return EvidenceAssociation(
            status="UNCERTAIN",
            reason="observability_snapshot_has_no_monitoring_execution_timestamp",
            execution_run_id=run_id,
        )
    if start is None or end is None:
        return EvidenceAssociation(
            status="UNCERTAIN",
            reason="pipeline_execution_window_is_incomplete",
            execution_run_id=run_id,
            monitoring_run_at=monitoring_times,
        )
    if all(start <= value <= end for value in monitoring_times):
        return EvidenceAssociation(
            status="UNCERTAIN",
            reason="timestamp_window_match_does_not_prove_execution_identity",
            execution_run_id=run_id,
            monitoring_run_at=monitoring_times,
        )
    return EvidenceAssociation(
        status="UNCERTAIN",
        reason="monitoring_execution_timestamp_is_outside_pipeline_run_window",
        execution_run_id=run_id,
        monitoring_run_at=monitoring_times,
    )


def _observability_identity(category: str, row: dict[str, Any]) -> list[Any]:
    if category == "quality":
        return [row.get("entity_name"), row.get("metric_name")]
    if category == "reconciliation":
        return [
            row.get("reconciliation_name"),
            row.get("source_layer"),
            row.get("target_layer"),
        ]
    return [row.get("entity_name")]


def _observability_token(
    category: str,
    row: dict[str, Any],
    association: EvidenceAssociation,
) -> Any:
    if category == "freshness":
        return row.get("monitoring_run_at") or row.get("monitoring_reference_at") or row
    if category == "row_counts":
        return {
            "activity_date": row.get("activity_date") or row.get("date_key"),
            "execution_run_id": association.execution_run_id,
        }
    if category == "quality":
        return {
            "metric_date": row.get("metric_date") or row.get("date_key"),
            "execution_run_id": association.execution_run_id,
        }
    return association.execution_run_id or row


def detect_observability_observations(
    summary: dict[str, Any],
    *,
    execution_run: dict[str, Any] | None = None,
    collected_at: datetime | None = None,
) -> list[MonitoringObservation]:
    """Detect PASS/WARN/FAIL observations while preserving date semantics."""

    fallback_time = collected_at or _utc_now()
    association = assess_observability_association(summary, execution_run)
    observations: list[MonitoringObservation] = []
    for category, status_column in _OBSERVABILITY_CATEGORIES.items():
        category_association = association
        if category != "freshness":
            category_association = association.model_copy(
                update={
                    "status": "UNCERTAIN",
                    "reason": "category_has_no_monitoring_execution_timestamp",
                }
            )
        category_result = summary.get(category) or {}
        rows = category_result.get("rows")
        if rows is None:
            rows = category_result.get("issues") or []
        for row in rows:
            fingerprint = incident_fingerprint(
                source="observability",
                category=category,
                identity_parts=_observability_identity(category, row),
            )
            monitoring_run_at = _parse_datetime(row.get("monitoring_run_at"))
            reference_time = _parse_datetime(row.get("monitoring_reference_at"))
            synthetic_date = (
                row.get("latest_partition_date")
                if category == "freshness"
                else row.get("activity_date")
                if category == "row_counts"
                else row.get("metric_date")
                if category == "quality"
                else None
            )
            observations.append(
                _observation(
                    fingerprint=fingerprint,
                    status=_normalize_status(row.get(status_column)),
                    observed_at=monitoring_run_at or fallback_time,
                    reference_time=reference_time,
                    synthetic_reference_date=str(synthetic_date) if synthetic_date is not None else None,
                    execution_run_id=category_association.execution_run_id,
                    observation_token=_observability_token(category, row, category_association),
                    evidence=row,
                    association=category_association,
                )
            )
    return observations


def detect_monitoring_observations(
    *,
    execution_run: dict[str, Any] | None,
    observability_summary: dict[str, Any] | None,
    jobs_error: Exception | None = None,
    observability_error: Exception | None = None,
    collected_at: datetime | None = None,
) -> DetectionResult:
    """Combine independent sources without converting missing evidence to PASS."""

    observations: list[MonitoringObservation] = []
    errors: list[MonitoringSourceError] = []
    if execution_run is not None:
        observations.extend(detect_execution_observations(execution_run, collected_at=collected_at))
    if observability_summary is not None:
        observations.extend(
            detect_observability_observations(
                observability_summary,
                execution_run=execution_run,
                collected_at=collected_at,
            )
        )
    if jobs_error is not None:
        errors.append(
            MonitoringSourceError(
                source="jobs",
                error_type=type(jobs_error).__name__,
                message=str(jobs_error),
            )
        )
    if observability_error is not None:
        errors.append(
            MonitoringSourceError(
                source="observability",
                error_type=type(observability_error).__name__,
                message=str(observability_error),
            )
        )
    return DetectionResult(
        observations=observations,
        source_availability={
            "jobs": execution_run is not None and jobs_error is None,
            "observability": observability_summary is not None and observability_error is None,
        },
        source_errors=errors,
    )
