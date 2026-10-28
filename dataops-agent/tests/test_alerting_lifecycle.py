from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from agent.alerting import (
    DetectionResult,
    EvidenceAssociation,
    InMemoryIncidentStateRepository,
    LifecycleSnapshot,
    MonitoringObservation,
    apply_detection,
    incident_fingerprint,
    notification_candidates,
    run_alerting_dry_run,
    transition_lifecycle,
)


BASE_TIME = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
FINGERPRINT = incident_fingerprint(
    source="observability",
    category="quality",
    identity_parts=["payments", "failed_payment_rate"],
)


def observation(status: str, sequence: int) -> MonitoringObservation:
    return MonitoringObservation(
        observation_id=hashlib.sha256(f"observation-{sequence}".encode()).hexdigest(),
        fingerprint=FINGERPRINT,
        status=status,
        observed_at=BASE_TIME + timedelta(hours=sequence),
        synthetic_reference_date=f"2026-09-{sequence:02d}",
        association=EvidenceAssociation(
            status="MATCHED",
            reason="test evidence",
            execution_run_id=100 + sequence,
        ),
        evidence={"quality_status": status, "sequence": sequence},
    )


def detection(*observations: MonitoringObservation, observability=True) -> DetectionResult:
    return DetectionResult(
        observations=list(observations),
        source_availability={"jobs": True, "observability": observability},
    )


def apply(snapshot: LifecycleSnapshot, *observations: MonitoringObservation):
    return transition_lifecycle(
        snapshot,
        detection(*observations),
        baseline_mode="process_existing",
    )


def test_first_warn_creates_new_incident():
    result = apply(LifecycleSnapshot(), observation("WARN", 1))

    assert result.decisions[0].decision == "NEW"
    assert result.decisions[0].would_investigate is True
    assert result.decisions[0].would_notify is True
    incident = result.snapshot.incidents[FINGERPRINT.digest]
    assert incident.lifecycle_status == "OPEN"
    assert incident.episode_number == 1


def test_repeated_warn_remains_ongoing_without_duplicate_notification():
    first = apply(LifecycleSnapshot(), observation("WARN", 1))
    second = apply(first.snapshot, observation("WARN", 2))

    assert second.decisions[0].decision == "ONGOING"
    assert second.decisions[0].would_notify is False
    assert second.snapshot.incidents[FINGERPRINT.digest].episode_number == 1


def test_two_distinct_pass_observations_resolve_open_incident():
    opened = apply(LifecycleSnapshot(), observation("WARN", 1))
    first_pass = apply(opened.snapshot, observation("PASS", 2))
    second_pass = apply(first_pass.snapshot, observation("PASS", 3))

    assert first_pass.decisions[0].decision == "PENDING_RESOLUTION"
    assert first_pass.snapshot.incidents[FINGERPRINT.digest].consecutive_passes == 1
    assert second_pass.decisions[0].decision == "RESOLVED"
    assert second_pass.decisions[0].would_notify is True
    incident = second_pass.snapshot.incidents[FINGERPRINT.digest]
    assert incident.lifecycle_status == "RESOLVED"
    assert incident.consecutive_passes == 2


def test_repeated_stale_pass_observation_cannot_resolve_incident():
    opened = apply(LifecycleSnapshot(), observation("WARN", 1))
    first_pass = apply(opened.snapshot, observation("PASS", 2))
    repeated_pass = apply(first_pass.snapshot, observation("PASS", 2))

    assert repeated_pass.decisions[0].decision == "DUPLICATE"
    incident = repeated_pass.snapshot.incidents[FINGERPRINT.digest]
    assert incident.lifecycle_status == "OPEN"
    assert incident.consecutive_passes == 1


def test_warn_after_resolution_creates_recurring_episode():
    opened = apply(LifecycleSnapshot(), observation("WARN", 1))
    one_pass = apply(opened.snapshot, observation("PASS", 2))
    resolved = apply(one_pass.snapshot, observation("PASS", 3))
    recurring = apply(resolved.snapshot, observation("WARN", 4))

    assert recurring.decisions[0].decision == "RECURRING"
    assert recurring.decisions[0].would_investigate is True
    assert recurring.decisions[0].would_notify is True
    incident = recurring.snapshot.incidents[FINGERPRINT.digest]
    assert incident.lifecycle_status == "OPEN"
    assert incident.episode_number == 2
    assert incident.resolved_at is None


def test_unavailable_monitoring_never_counts_as_pass_and_breaks_pass_sequence():
    opened = apply(LifecycleSnapshot(), observation("WARN", 1))
    first_pass = apply(opened.snapshot, observation("PASS", 2))
    unavailable = transition_lifecycle(
        first_pass.snapshot,
        detection(observability=False),
        baseline_mode="process_existing",
    )
    final_pass = apply(unavailable.snapshot, observation("PASS", 3))

    assert unavailable.decisions[0].decision == "INSUFFICIENT_EVIDENCE"
    assert unavailable.snapshot.incidents[FINGERPRINT.digest].consecutive_passes == 0
    assert final_pass.decisions[0].decision == "PENDING_RESOLUTION"
    assert final_pass.snapshot.incidents[FINGERPRINT.digest].lifecycle_status == "OPEN"


def test_retrying_same_observation_is_idempotent():
    first = apply(LifecycleSnapshot(), observation("WARN", 1))
    retry = apply(first.snapshot, observation("WARN", 1))

    assert retry.decisions[0].decision == "DUPLICATE"
    incident = retry.snapshot.incidents[FINGERPRINT.digest]
    assert incident.observation_count == 1
    assert incident.episode_number == 1


def test_initial_baseline_suppresses_existing_alert_storm():
    result = transition_lifecycle(
        LifecycleSnapshot(),
        detection(observation("WARN", 1)),
        baseline_mode="suppress_existing",
    )

    assert result.decisions[0].decision == "BASELINED"
    assert result.decisions[0].would_notify is False
    assert result.decisions[0].would_investigate is False
    assert result.snapshot.initialized is True
    assert result.snapshot.incidents[FINGERPRINT.digest].lifecycle_status == "OPEN"

    repeated = apply(result.snapshot, observation("WARN", 2))
    assert repeated.decisions[0].decision == "ONGOING"
    assert repeated.decisions[0].would_notify is False


def test_initial_baseline_does_not_suppress_current_execution_failure():
    execution_fingerprint = incident_fingerprint(
        source="jobs",
        category="execution",
        identity_parts=[123, "task", "dbt_tests"],
    )
    failed_execution = MonitoringObservation(
        observation_id=hashlib.sha256(b"failed-execution").hexdigest(),
        fingerprint=execution_fingerprint,
        status="FAIL",
        observed_at=BASE_TIME,
        execution_run_id=456,
        association=EvidenceAssociation(
            status="NOT_APPLICABLE",
            reason="jobs evidence",
            execution_run_id=456,
        ),
        evidence={"task_key": "dbt_tests", "result_state": "FAILED"},
    )

    result = transition_lifecycle(
        LifecycleSnapshot(),
        detection(failed_execution),
        baseline_mode="suppress_existing",
    )

    assert result.decisions[0].decision == "NEW"
    assert result.decisions[0].would_notify is True
    assert result.decisions[0].would_investigate is True


def test_dry_run_reports_would_notify_without_saving_repository():
    repository = InMemoryIncidentStateRepository(LifecycleSnapshot(initialized=True))
    report = run_alerting_dry_run(
        repository=repository,
        detection=detection(observation("WARN", 1)),
        baseline_mode="process_existing",
        generated_at=BASE_TIME,
    )

    assert report.committed is False
    assert report.would_notify_count == 1
    assert report.would_investigate_count == 1
    assert report.decisions[0].decision == "NEW"
    assert notification_candidates(report.decisions) == [report.decisions[0]]
    assert repository.save_count == 0
    assert repository.load().incidents == {}


def test_apply_detection_uses_injected_repository_only():
    repository = InMemoryIncidentStateRepository(LifecycleSnapshot(initialized=True))
    result = apply_detection(
        repository=repository,
        detection=detection(observation("WARN", 1)),
        baseline_mode="process_existing",
    )

    assert result.decisions[0].decision == "NEW"
    assert repository.save_count == 1
    assert FINGERPRINT.digest in repository.load().incidents


def test_apply_detection_retries_optimistic_concurrency_conflict():
    class ConflictOnceRepository(InMemoryIncidentStateRepository):
        def __init__(self):
            super().__init__(LifecycleSnapshot(initialized=True))
            self.conflicted = False

        def save(self, snapshot, *, expected_version):
            if not self.conflicted:
                self.conflicted = True
                from agent.alerting.repository import ConcurrentStateUpdateError

                raise ConcurrentStateUpdateError("simulated concurrent update")
            return super().save(snapshot, expected_version=expected_version)

    repository = ConflictOnceRepository()
    result = apply_detection(
        repository=repository,
        detection=detection(observation("WARN", 1)),
        baseline_mode="process_existing",
        max_retries=2,
    )

    assert repository.conflicted is True
    assert repository.save_count == 1
    assert result.decisions[0].decision == "NEW"
