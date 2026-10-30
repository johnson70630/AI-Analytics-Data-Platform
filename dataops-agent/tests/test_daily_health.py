from __future__ import annotations

from datetime import datetime, timezone

import pytest

from runtime.health import build_daily_health_summary


NOW = datetime(2026, 10, 29, 18, 0, tzinfo=timezone.utc)


def _jobs(*, result="SUCCESS", task_result="SUCCESS"):
    return {
        "found": True,
        "job_id": 123,
        "run_id": 456,
        "run_name": "daily",
        "lifecycle_state": "TERMINATED",
        "result_state": result,
        "start_time": "2026-10-29T15:00:00+00:00",
        "end_time": "2026-10-29T15:30:00+00:00",
        "duration_ms": 1_800_000,
        "unsuccessful_tasks": [] if task_result == "SUCCESS" else ["dbt_tests"],
        "tasks": [
            {
                "task_key": "dbt_tests",
                "lifecycle_state": "TERMINATED",
                "result_state": task_result,
            }
        ],
    }


def _observability(*, quality_status="PASS", monitoring_run_at="2026-10-29T16:00:00+00:00"):
    freshness_row = {
        "entity_name": "messages",
        "latest_partition_date": "2026-10-28",
        "monitoring_reference_at": "2026-10-29T15:00:00+00:00",
        "monitoring_run_at": monitoring_run_at,
        "freshness_status": "PASS",
    }
    quality_row = {
        "entity_name": "payments",
        "metric_name": "failed_payment_rate",
        "metric_date": "2026-10-28",
        "metric_rate": 0.12,
        "quality_status": quality_status,
    }
    return {
        "freshness": {"rows": [freshness_row], "issues": []},
        "row_counts": {
            "rows": [{"entity_name": "messages", "activity_date": "2026-10-28", "volume_status": "PASS"}],
            "issues": [],
        },
        "quality": {"rows": [quality_row], "issues": [] if quality_status == "PASS" else [quality_row]},
        "reconciliation": {
            "rows": [{"reconciliation_name": "bronze_to_silver", "status": "PASS"}],
            "issues": [],
        },
        "total_issue_count": 0 if quality_status == "PASS" else 1,
    }


@pytest.mark.asyncio
async def test_healthy_summary_keeps_execution_and_monitoring_evidence_separate():
    result = await build_daily_health_summary(
        jobs_fetcher=_jobs,
        observability_fetcher=_observability,
        now=NOW,
    )

    assert result["overall_status"] == "PASS"
    assert result["pipeline"]["status"] == "PASS"
    assert result["pipeline"]["task_counts"] == {
        "total": 1,
        "passed": 1,
        "failed": 0,
        "active": 0,
        "unknown": 0,
    }
    assert result["monitoring"]["checks"] == {
        "total": 4,
        "pass": 4,
        "warn": 0,
        "fail": 0,
        "unknown": 0,
    }
    assert result["monitoring"]["monitoring_run_at"] == "2026-10-29T16:00:00+00:00"
    assert result["monitoring"]["monitoring_run_at_source"] == "dq_entity_freshness"
    assert result["monitoring"]["monitoring_reference_at"] == "2026-10-29T15:00:00+00:00"
    assert result["monitoring"]["synthetic_reference_date"] == "2026-10-28"
    assert result["monitoring"]["pipeline_association_verified"] is False


@pytest.mark.asyncio
async def test_pipeline_success_does_not_hide_monitoring_warning():
    result = await build_daily_health_summary(
        jobs_fetcher=_jobs,
        observability_fetcher=lambda: _observability(quality_status="WARN"),
        now=NOW,
    )

    assert result["pipeline"]["status"] == "PASS"
    assert result["monitoring"]["status"] == "WARN"
    assert result["overall_status"] == "WARN"
    assert result["monitoring"]["issues"][0]["evidence_ref"] == (
        "observability:quality:payments:failed_payment_rate"
    )


@pytest.mark.asyncio
async def test_pipeline_failure_is_reported_independently():
    result = await build_daily_health_summary(
        jobs_fetcher=lambda: _jobs(result="FAILED", task_result="FAILED"),
        observability_fetcher=_observability,
        now=NOW,
    )

    assert result["overall_status"] == "FAIL"
    assert result["pipeline"]["status"] == "FAIL"
    assert result["pipeline"]["task_counts"]["failed"] == 1
    assert result["monitoring"]["status"] == "PASS"


@pytest.mark.asyncio
async def test_one_source_failure_returns_partial_evidence():
    def failed_jobs():
        raise PermissionError("Jobs evidence denied")

    result = await build_daily_health_summary(
        jobs_fetcher=failed_jobs,
        observability_fetcher=_observability,
        now=NOW,
    )

    assert result["overall_status"] == "PARTIAL"
    assert result["availability"] == {"jobs": False, "observability": True, "partial": True}
    assert result["pipeline"]["status"] == "UNAVAILABLE"
    assert result["source_errors"] == [
        {"source": "jobs", "type": "PermissionError", "message": "Jobs evidence denied"}
    ]


@pytest.mark.asyncio
async def test_known_failure_is_not_downgraded_when_other_source_is_unavailable():
    def failed_observability():
        raise RuntimeError("Observability unavailable")

    result = await build_daily_health_summary(
        jobs_fetcher=lambda: _jobs(result="FAILED", task_result="FAILED"),
        observability_fetcher=failed_observability,
        now=NOW,
    )

    assert result["availability"]["partial"] is True
    assert result["pipeline"]["status"] == "FAIL"
    assert result["overall_status"] == "FAIL"


@pytest.mark.asyncio
async def test_both_source_failures_are_unavailable_not_healthy():
    def fail():
        raise RuntimeError("unavailable")

    result = await build_daily_health_summary(
        jobs_fetcher=fail,
        observability_fetcher=fail,
        now=NOW,
    )

    assert result["overall_status"] == "UNAVAILABLE"
    assert result["availability"] == {"jobs": False, "observability": False, "partial": False}
    assert len(result["source_errors"]) == 2
    assert result["monitoring"]["checks"] is None


@pytest.mark.asyncio
async def test_source_errors_redact_credential_like_values():
    def fail():
        raise RuntimeError("authorization=super-secret Bearer abc123 password=hunter2")

    result = await build_daily_health_summary(
        jobs_fetcher=fail,
        observability_fetcher=_observability,
        now=NOW,
    )

    message = result["source_errors"][0]["message"]
    assert "super-secret" not in message
    assert "abc123" not in message
    assert "hunter2" not in message
    assert message.count("[REDACTED]") == 3


@pytest.mark.asyncio
async def test_empty_monitoring_result_is_unknown_not_pass():
    empty = {name: {"rows": [], "issues": []} for name in ("freshness", "row_counts", "quality", "reconciliation")}
    result = await build_daily_health_summary(
        jobs_fetcher=_jobs,
        observability_fetcher=lambda: empty,
        now=NOW,
    )

    assert result["monitoring"]["status"] == "UNKNOWN"
    assert result["monitoring"]["checks"]["total"] == 0
    assert result["overall_status"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_stale_monitoring_evidence_is_warned_without_changing_check_counts():
    result = await build_daily_health_summary(
        jobs_fetcher=_jobs,
        observability_fetcher=lambda: _observability(monitoring_run_at="2026-10-27T00:00:00+00:00"),
        now=NOW,
        stale_after_hours=36,
    )

    assert result["monitoring"]["status"] == "PASS"
    assert result["monitoring"]["staleness"]["status"] == "STALE"
    assert result["overall_status"] == "WARN"


@pytest.mark.asyncio
async def test_summary_is_read_only_and_future_investigation_is_disabled():
    result = await build_daily_health_summary(
        jobs_fetcher=_jobs,
        observability_fetcher=_observability,
        now=NOW,
    )

    assert result["investigation"] == {
        "enabled": False,
        "status": "PLANNED_V2_6B",
        "action": "generate_dataops_investigation_report",
    }
    assert "write" not in str(result).lower()
