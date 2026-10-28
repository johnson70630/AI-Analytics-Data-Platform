from __future__ import annotations

from datetime import datetime, timezone

from agent.alerting import (
    assess_observability_association,
    detect_monitoring_observations,
    detect_observability_observations,
    incident_fingerprint,
)


COLLECTED_AT = datetime(2026, 10, 27, 9, 15, tzinfo=timezone.utc)


def execution_run(*, result="SUCCESS", task_result="SUCCESS", run_id=456) -> dict:
    return {
        "found": True,
        "job_id": 123,
        "run_id": run_id,
        "run_name": "daily-pipeline",
        "result_state": result,
        "lifecycle_state": "TERMINATED",
        "start_time": "2026-10-27T09:00:00Z",
        "end_time": "2026-10-27T09:20:00Z",
        "tasks": [
            {
                "task_key": "dbt_observability",
                "result_state": task_result,
                "start_time": "2026-10-27T09:10:00Z",
                "end_time": "2026-10-27T09:15:00Z",
            }
        ],
    }


def observability_summary(
    *,
    freshness_status="PASS",
    quality_status="PASS",
    monitoring_run_at="2026-10-27T09:12:00Z",
    metric_date="2026-09-01",
) -> dict:
    freshness = {
        "entity_name": "subscriptions",
        "monitoring_run_at": monitoring_run_at,
        "monitoring_reference_at": "2026-09-01T23:59:59Z",
        "latest_partition_date": "2026-08-20",
        "freshness_status": freshness_status,
    }
    quality = {
        "entity_name": "payments",
        "metric_name": "failed_payment_rate",
        "metric_date": metric_date,
        "quality_status": quality_status,
    }
    return {
        "freshness": {"rows": [freshness], "issues": []},
        "row_counts": {"rows": [], "issues": []},
        "quality": {"rows": [quality], "issues": []},
        "reconciliation": {"rows": [], "issues": []},
    }


def test_fingerprint_uses_check_identity_not_status_or_dates():
    first = incident_fingerprint(
        source="observability",
        category="quality",
        identity_parts=["payments", "failed_payment_rate"],
    )
    repeated = incident_fingerprint(
        source="observability",
        category="quality",
        identity_parts=["payments", "failed_payment_rate"],
    )
    different_metric = incident_fingerprint(
        source="observability",
        category="quality",
        identity_parts=["payments", "missing_payment_id_rate"],
    )

    assert first.digest == repeated.digest
    assert first.digest != different_metric.digest
    assert "2026" not in first.identity


def test_different_entities_and_metrics_create_distinct_incident_identities():
    summary = observability_summary(freshness_status="WARN", quality_status="FAIL")
    observations = detect_observability_observations(
        summary,
        execution_run=execution_run(),
        collected_at=COLLECTED_AT,
    )

    assert len(observations) == 2
    assert len({item.fingerprint.digest for item in observations}) == 2
    assert {item.fingerprint.category for item in observations} == {"freshness", "quality"}


def test_pipeline_failure_detected_when_observability_is_unavailable():
    result = detect_monitoring_observations(
        execution_run=execution_run(result="FAILED", task_result="FAILED"),
        observability_summary=None,
        observability_error=RuntimeError("warehouse unavailable"),
        collected_at=COLLECTED_AT,
    )

    assert result.source_availability == {"jobs": True, "observability": False}
    assert len(result.observations) == 1
    assert result.observations[0].fingerprint.source == "jobs"
    assert result.observations[0].status == "FAIL"
    assert result.source_errors[0].source == "observability"
    assert all(item.status != "PASS" for item in result.observations)


def test_monitoring_error_does_not_fabricate_pass_observations():
    result = detect_monitoring_observations(
        execution_run=None,
        observability_summary=None,
        jobs_error=RuntimeError("jobs unavailable"),
        observability_error=RuntimeError("sql unavailable"),
        collected_at=COLLECTED_AT,
    )

    assert result.observations == []
    assert result.source_availability == {"jobs": False, "observability": False}
    assert {item.source for item in result.source_errors} == {"jobs", "observability"}


def test_monitoring_execution_time_is_not_confused_with_synthetic_reference_date():
    observations = detect_observability_observations(
        observability_summary(quality_status="WARN"),
        execution_run=execution_run(),
        collected_at=COLLECTED_AT,
    )
    freshness = next(item for item in observations if item.fingerprint.category == "freshness")
    quality = next(item for item in observations if item.fingerprint.category == "quality")

    assert freshness.observed_at.isoformat() == "2026-10-27T09:12:00+00:00"
    assert freshness.reference_time.isoformat() == "2026-09-01T23:59:59+00:00"
    assert freshness.synthetic_reference_date == "2026-08-20"
    assert quality.observed_at == COLLECTED_AT
    assert quality.synthetic_reference_date == "2026-09-01"
    assert freshness.association.status == "UNCERTAIN"
    assert freshness.association.reason == "timestamp_window_match_does_not_prove_execution_identity"
    assert quality.association.status == "UNCERTAIN"
    assert quality.association.reason == "category_has_no_monitoring_execution_timestamp"


def test_timestamp_window_match_remains_uncertain_without_run_identity():
    window_match = assess_observability_association(observability_summary(), execution_run())
    uncertain = assess_observability_association(
        observability_summary(monitoring_run_at="2026-10-28T09:12:00Z"),
        execution_run(),
    )

    assert window_match.status == "UNCERTAIN"
    assert window_match.execution_run_id == 456
    assert window_match.reason == "timestamp_window_match_does_not_prove_execution_identity"
    assert uncertain.status == "UNCERTAIN"
    assert "outside" in uncertain.reason


def test_repeated_snapshot_has_same_observation_id_but_new_metric_date_does_not():
    first = detect_observability_observations(
        observability_summary(), execution_run=execution_run(), collected_at=COLLECTED_AT
    )
    retry = detect_observability_observations(
        observability_summary(), execution_run=execution_run(), collected_at=COLLECTED_AT
    )
    next_date = detect_observability_observations(
        observability_summary(metric_date="2026-09-02"),
        execution_run=execution_run(),
        collected_at=COLLECTED_AT,
    )
    first_quality = next(item for item in first if item.fingerprint.category == "quality")
    retry_quality = next(item for item in retry if item.fingerprint.category == "quality")
    next_quality = next(item for item in next_date if item.fingerprint.category == "quality")

    assert first_quality.observation_id == retry_quality.observation_id
    assert first_quality.observation_id != next_quality.observation_id
    assert first_quality.fingerprint.digest == next_quality.fingerprint.digest


def test_new_execution_distinguishes_recomputed_same_date_from_transport_retry():
    first = detect_observability_observations(
        observability_summary(), execution_run=execution_run(run_id=456), collected_at=COLLECTED_AT
    )
    later_run = detect_observability_observations(
        observability_summary(), execution_run=execution_run(run_id=457), collected_at=COLLECTED_AT
    )
    first_quality = next(item for item in first if item.fingerprint.category == "quality")
    later_quality = next(item for item in later_run if item.fingerprint.category == "quality")

    assert first_quality.fingerprint.digest == later_quality.fingerprint.digest
    assert first_quality.observation_id != later_quality.observation_id
