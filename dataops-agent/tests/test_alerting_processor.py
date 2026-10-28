from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import pytest

from agent.alerting import (
    ConcurrentStateUpdateError,
    DetectionResult,
    EvidenceAssociation,
    InMemoryDurableIncidentRegistry,
    LifecycleSnapshot,
    MonitoringObservation,
    detect_monitoring_observations,
    incident_fingerprint,
    process_incidents,
)


NOW = datetime(2026, 10, 27, 10, 0, tzinfo=timezone.utc)
FINGERPRINT = incident_fingerprint(
    source="observability",
    category="quality",
    identity_parts=["payments", "failed_payment_rate"],
)


def observation(status: str, sequence: int) -> MonitoringObservation:
    return MonitoringObservation(
        observation_id=hashlib.sha256(f"processor-{sequence}".encode()).hexdigest(),
        fingerprint=FINGERPRINT,
        status=status,
        observed_at=NOW + timedelta(hours=sequence),
        synthetic_reference_date=f"2026-09-{sequence:02d}",
        association=EvidenceAssociation(
            status="UNCERTAIN",
            reason="category_has_no_monitoring_execution_timestamp",
            execution_run_id=100 + sequence,
        ),
        evidence={"quality_status": status, "metric_date": f"2026-09-{sequence:02d}"},
    )


def detection(item: MonitoringObservation) -> DetectionResult:
    return DetectionResult(
        observations=[item],
        source_availability={"jobs": True, "observability": True},
    )


def partial_report() -> dict:
    return {
        "schema_version": "1.1",
        "generated_at": "2026-10-27T10:00:00Z",
        "source_availability": {
            "jobs": {"available": True, "status": "PASS"},
            "observability": {"available": True, "status": "WARN"},
            "genie": {"available": False, "status": "partial"},
        },
        "pipeline_execution_health": {
            "status": "PASS",
            "pipeline_failed": False,
            "run_found": True,
            "result_state": "SUCCESS",
            "lifecycle_state": "TERMINATED",
            "run_id": 456,
            "run_started_at": "2026-10-27T09:00:00Z",
            "run_ended_at": "2026-10-27T09:20:00Z",
            "unsuccessful_tasks": [],
        },
        "monitoring_health": {
            "status": "WARN",
            "check_count": 1,
            "issue_count": 1,
            "warning_count": 1,
            "failure_count": 0,
            "monitoring_execution_timestamps": ["2026-10-27T09:12:00Z"],
            "monitoring_reference_timestamps": ["2026-09-01T23:59:59Z"],
            "synthetic_activity_dates": {"metric_dates": ["2026-09-01"]},
        },
        "detected_issues": [
            {
                "source": "observability",
                "category": "quality",
                "status": "WARN",
                "name": "payments.failed_payment_rate",
                "details": {"metric_date": "2026-09-01"},
            }
        ],
        "genie_findings": [
            {
                "investigation_id": "genie-1",
                "question": "Show bounded history.",
                "status": "partial",
                "answer": "One bounded result was returned.",
                "evidence": {"query_evidence": [{"result": {"rows": [["large raw row"]]}}]},
                "error": {"type": "partial_result"},
                "root_cause": {"status": "unknown", "explanation": "Unknown."},
                "impact": {"status": "unknown", "explanation": "Unknown."},
            }
        ],
        "source_errors": [
            {"source": "genie", "error_type": "PartialResult", "message": "bounded result"}
        ],
        "investigation_completeness": "partial",
        "genie_routing": {"selected_count": 1},
        "evidence": {"large_raw_report": ["do not email"] * 100},
        "source_references": {"quality": "latest"},
        "root_cause": {"status": "unknown", "explanation": "Unknown."},
        "impact": {"status": "unknown", "explanation": "Unknown."},
        "workflow": {"path": ["construct_report"]},
        "recommendations": [
            {
                "issue_id": "observability.quality.payments-failed-payment-rate",
                "priority": "P1",
                "category": "diagnostic",
                "action": "Compare the monitored metric with its bounded history.",
                "rationale": "The monitored check returned WARN.",
                "evidence_refs": ["detected_issues[0]", "genie_findings[0]"],
                "requires_human_approval": False,
                "verification_steps": ["Record the comparison."],
            }
        ],
        "recommendation_summary": {
            "status": "generated",
            "recommendation_count": 1,
            "unsupported_issue_count": 0,
            "message": "One recommendation generated.",
        },
    }


@pytest.mark.asyncio
async def test_new_incident_persists_once_and_invokes_investigation():
    registry = InMemoryDurableIncidentRegistry(LifecycleSnapshot(initialized=True))
    calls = 0

    async def investigate():
        nonlocal calls
        calls += 1
        return partial_report()

    result = await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 1)),
        mode="persist",
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW,
    )

    assert result.committed is True
    assert result.transition.decisions[0].decision == "NEW"
    assert calls == 1
    assert registry.persist_count == 1
    assert len(registry.observations) == 1
    assert len(registry.episodes) == 1
    assert len(registry.notifications) == 1

    retry = await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 1)),
        mode="persist",
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW,
    )
    assert retry.transition.decisions[0].decision == "DUPLICATE"
    assert retry.committed is False
    assert calls == 1
    assert registry.persist_count == 1
    assert len(registry.observations) == 1
    assert len(registry.notifications) == 1


@pytest.mark.asyncio
async def test_ongoing_warning_does_not_duplicate_alert():
    registry = InMemoryDurableIncidentRegistry(LifecycleSnapshot(initialized=True))
    calls = 0

    async def investigate():
        nonlocal calls
        calls += 1
        return partial_report()

    await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 1)),
        mode="persist",
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW,
    )
    ongoing = await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 2)),
        mode="persist",
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW + timedelta(hours=1),
    )

    assert ongoing.transition.decisions[0].decision == "ONGOING"
    assert ongoing.outbox_records == []
    assert calls == 1
    assert len(registry.notifications) == 1


@pytest.mark.asyncio
async def test_resolved_then_recurring_creates_new_episode_and_notification():
    registry = InMemoryDurableIncidentRegistry(LifecycleSnapshot(initialized=True))

    async def investigate():
        return partial_report()

    for status, sequence in [("WARN", 1), ("PASS", 2), ("PASS", 3), ("WARN", 4)]:
        result = await process_incidents(
            repository=registry,
            detection=detection(observation(status, sequence)),
            mode="persist",
            baseline_mode="process_existing",
            investigation_runner=investigate,
            now=NOW + timedelta(hours=sequence),
        )

    assert result.transition.decisions[0].decision == "RECURRING"
    assert result.transition.decisions[0].incident.episode_number == 2
    event_types = {record.event_type for record in registry.notifications.values()}
    assert event_types == {"NEW", "RESOLVED", "RECURRING"}
    assert len(registry.episodes) == 2


@pytest.mark.asyncio
async def test_partial_investigation_is_preserved_in_concise_outbox_payload():
    registry = InMemoryDurableIncidentRegistry(LifecycleSnapshot(initialized=True))

    async def investigate():
        return partial_report()

    result = await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 1)),
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW,
    )

    assert result.mode == "dry_run"
    assert result.committed is False
    payload = result.outbox_records[0].payload
    assert payload.investigation_completeness == "partial"
    assert payload.genie_findings[0].status == "partial"
    assert payload.recommendations[0].evidence_refs == [
        "detected_issues[0]",
        "genie_findings[0]",
    ]
    serialized = payload.model_dump_json()
    assert "large_raw_report" not in serialized
    assert "large raw row" not in serialized
    assert registry.persist_count == 0


@pytest.mark.asyncio
async def test_investigation_failure_still_creates_fact_only_outbox_record():
    registry = InMemoryDurableIncidentRegistry(LifecycleSnapshot(initialized=True))

    async def investigate():
        raise TimeoutError("bounded investigation timeout")

    result = await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 1)),
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW,
    )

    assert result.investigation_report is None
    assert result.investigation_error == "TimeoutError: bounded investigation timeout"
    payload = result.outbox_records[0].payload
    assert payload.investigation_completeness == "unavailable"
    assert payload.recommendations == []
    assert payload.source_errors == ["investigation:TimeoutError: bounded investigation timeout"]


@pytest.mark.asyncio
async def test_concurrent_state_conflict_retries_without_duplicate_investigation():
    class ConflictOnceRegistry(InMemoryDurableIncidentRegistry):
        def __init__(self):
            super().__init__(LifecycleSnapshot(initialized=True))
            self.conflicted = False

        def persist_transition(self, bundle, *, expected_version, lease_owner):
            if not self.conflicted:
                self.conflicted = True
                raise ConcurrentStateUpdateError("simulated concurrent processor")
            return super().persist_transition(
                bundle,
                expected_version=expected_version,
                lease_owner=lease_owner,
            )

    registry = ConflictOnceRegistry()
    calls = 0

    async def investigate():
        nonlocal calls
        calls += 1
        return partial_report()

    result = await process_incidents(
        repository=registry,
        detection=detection(observation("WARN", 1)),
        mode="persist",
        baseline_mode="process_existing",
        investigation_runner=investigate,
        now=NOW,
        max_persistence_retries=2,
    )

    assert result.committed is True
    assert registry.conflicted is True
    assert registry.persist_count == 1
    assert calls == 1


@pytest.mark.asyncio
async def test_multiple_completed_pipeline_runs_are_processed_in_order_without_duplicate_alert():
    registry = InMemoryDurableIncidentRegistry(LifecycleSnapshot(initialized=True))

    async def investigate():
        return partial_report()

    decisions = []
    for run_id, hour in [(700, 1), (701, 2)]:
        run = {
            "job_id": 123,
            "run_id": run_id,
            "result_state": "FAILED",
            "start_time": (NOW + timedelta(hours=hour)).isoformat(),
            "end_time": (NOW + timedelta(hours=hour, minutes=30)).isoformat(),
            "tasks": [
                {
                    "task_key": "dbt_tests",
                    "result_state": "FAILED",
                    "end_time": (NOW + timedelta(hours=hour, minutes=25)).isoformat(),
                }
            ],
        }
        result = await process_incidents(
            repository=registry,
            detection=detect_monitoring_observations(
                execution_run=run,
                observability_summary=None,
                collected_at=NOW + timedelta(hours=hour, minutes=30),
            ),
            mode="persist",
            baseline_mode="process_existing",
            investigation_runner=investigate,
            now=NOW + timedelta(hours=hour, minutes=30),
        )
        decisions.append(
            next(item.decision for item in result.transition.decisions if item.fingerprint is not None)
        )

    assert decisions == ["NEW", "ONGOING"]
    assert len(registry.episodes) == 1
    assert len(registry.notifications) == 1
