from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest

from agent.alerting import (
    EmailDeliveryConfig,
    InMemoryNotificationOutboxRepository,
    PermanentEmailDeliveryError,
    build_email_payload,
    deliver_notifications,
    render_notification_email,
)
from agent.alerting.models import (
    AlertDecision,
    EmailEvidenceAssessment,
    EmailGenieFinding,
    EmailRecommendation,
    IncidentFingerprint,
    IncidentRecord,
    NotificationEmailPayload,
    NotificationOutboxRecord,
)
from agent.alerting.outbox import notification_identity


NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
EPISODE_ID = "a" * 64


def rich_payload() -> NotificationEmailPayload:
    return NotificationEmailPayload(
        subject="[DataOps] WARN quality incident",
        event_type="NEW",
        episode_id=EPISODE_ID,
        observed_facts=["source=observability", "observed_status=WARN"],
        investigation_completeness="partial",
        genie_findings=[
            EmailGenieFinding(
                status="complete",
                question="Show history",
                answer="RAW_COMPLETE_GENIE_RESULT_MUST_NOT_RENDER",
            )
        ],
        recommendations=[
            EmailRecommendation(
                priority="P1",
                category="diagnostic",
                action="Compare the monitored metric across known layers.",
                rationale="The quality check returned WARN.",
                evidence_refs=["detected_issues[0]"],
                requires_human_approval=False,
                verification_steps=["Record the compared values."],
            ),
            EmailRecommendation(
                priority="P2",
                category="proposed_remediation",
                action="Correct the confirmed transformation only after review.",
                rationale="A correction is not supported until diagnosis is complete.",
                evidence_refs=["detected_issues[0]"],
                requires_human_approval=True,
                verification_steps=["Rerun the bounded quality check after approval."],
            ),
        ],
        evidence_references=["detected_issues[0]"],
        source_errors=["genie:PartialResult:bounded result"],
        severity="warning",
        incident_status="WARN",
        observed_evidence=[
            "observability.quality.failed_payment_rate: WARN <script>alert(1)</script>"
        ],
        monitoring_execution_timestamps=["2026-10-27T09:12:00Z"],
        monitoring_reference_timestamps=["2026-09-01T23:59:59Z"],
        synthetic_activity_dates={"metric_dates": ["2026-09-01"]},
        root_cause=EmailEvidenceAssessment(
            status="unknown",
            explanation="The available evidence does not identify the root cause.",
        ),
        impact=EmailEvidenceAssessment(
            status="unknown",
            explanation="The available evidence does not quantify the impact.",
        ),
    )


def outbox_record(payload: NotificationEmailPayload | None = None) -> NotificationOutboxRecord:
    email_payload = payload or rich_payload()
    return NotificationOutboxRecord(
        notification_id=notification_identity(email_payload.episode_id, email_payload.event_type),
        episode_id=email_payload.episode_id,
        event_type=email_payload.event_type,
        payload=email_payload,
        max_attempts=3,
        available_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )


def provider_config(provider: str) -> EmailDeliveryConfig:
    return EmailDeliveryConfig(
        provider=provider,
        sender_address="alerts@example.com",
        recipient_addresses=("operator@example.com",),
    )


def decision() -> AlertDecision:
    fingerprint = IncidentFingerprint(
        source="observability",
        category="quality",
        identity="payments.failed_payment_rate",
        digest=hashlib.sha256(b"quality-incident").hexdigest(),
    )
    incident = IncidentRecord(
        fingerprint=fingerprint,
        lifecycle_status="OPEN",
        episode_number=1,
        first_seen_at=NOW,
        episode_started_at=NOW,
        last_seen_at=NOW,
        last_status="WARN",
        last_observation_id=hashlib.sha256(b"observation").hexdigest(),
        observation_count=1,
        consecutive_passes=0,
    )
    return AlertDecision(
        decision="NEW",
        observation_id=incident.last_observation_id,
        fingerprint=fingerprint,
        incident=incident,
        would_investigate=True,
        would_notify=True,
        reason="new issue",
    )


def report() -> dict:
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
                "details": {"metric_date": "2026-09-01", "metric_rate": 0.21},
            }
        ],
        "genie_findings": [],
        "source_errors": [],
        "investigation_completeness": "partial",
        "genie_routing": {"selected_count": 0},
        "evidence": {"raw_rows": ["DO_NOT_EMAIL_RAW_REPORT"]},
        "source_references": {"quality": "latest"},
        "root_cause": {
            "status": "unknown",
            "explanation": "The retrieved evidence does not establish a root cause.",
        },
        "impact": {
            "status": "unknown",
            "explanation": "The retrieved evidence does not quantify impact.",
        },
        "workflow": {"path": ["construct_report"]},
        "recommendations": [
            {
                "issue_id": "observability.quality.payments-failed-payment-rate",
                "priority": "P1",
                "category": "diagnostic",
                "action": "Compare the monitored metric with its bounded history.",
                "rationale": "The monitored check returned WARN.",
                "evidence_refs": ["detected_issues[0]"],
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


def test_environment_configuration_defaults_to_mock_and_requires_live_routing_values():
    assert EmailDeliveryConfig.from_environment({}) == EmailDeliveryConfig.mock()

    with pytest.raises(ValueError, match="DATAOPS_EMAIL_FROM, DATAOPS_EMAIL_TO"):
        EmailDeliveryConfig.from_environment({"DATAOPS_EMAIL_PROVIDER": "ses"})

    config = EmailDeliveryConfig.from_environment(
        {
            "DATAOPS_EMAIL_PROVIDER": "ses",
            "DATAOPS_EMAIL_FROM": "alerts@example.com",
            "DATAOPS_EMAIL_TO": "primary@example.com,secondary@example.com",
        }
    )
    assert config.provider == "ses"
    assert config.recipient_addresses == ("primary@example.com", "secondary@example.com")


def test_legacy_outbox_payload_remains_readable_with_safe_defaults():
    payload = NotificationEmailPayload.model_validate(
        {
            "subject": "[DataOps] legacy notification",
            "event_type": "NEW",
            "episode_id": EPISODE_ID,
            "observed_facts": ["observed_status=WARN"],
            "investigation_completeness": "unavailable",
            "genie_findings": [],
            "recommendations": [],
            "evidence_references": [],
            "source_errors": [],
        }
    )

    assert payload.severity == "unknown"
    assert payload.root_cause.status == "unknown"
    assert payload.impact.status == "unknown"


def test_text_and_html_templates_are_bounded_grounded_and_html_safe():
    message = render_notification_email(
        notification_id="b" * 64,
        payload=rich_payload(),
        config=EmailDeliveryConfig.mock(),
    )

    for expected in (
        "Severity: WARNING",
        "2026-10-27T09:12:00Z",
        "2026-09-01T23:59:59Z",
        "Human approval required: Yes",
        "Record the compared values.",
        "does not identify the root cause",
        "does not quantify the impact",
    ):
        assert expected in message.text_body
    assert "<script>" not in message.html_body
    assert "&lt;script&gt;" in message.html_body
    assert "RAW_COMPLETE_GENIE_RESULT_MUST_NOT_RENDER" not in message.text_body
    assert "RAW_COMPLETE_GENIE_RESULT_MUST_NOT_RENDER" not in message.html_body


def test_outbox_payload_preserves_dates_assessments_and_excludes_raw_report():
    payload = build_email_payload(decision(), investigation_report=report())

    assert payload.severity == "warning"
    assert payload.monitoring_execution_timestamps == ["2026-10-27T09:12:00Z"]
    assert payload.monitoring_reference_timestamps == ["2026-09-01T23:59:59Z"]
    assert payload.synthetic_activity_dates == {"metric_dates": ["2026-09-01"]}
    assert payload.root_cause.status == "unknown"
    assert payload.impact.status == "unknown"
    assert "metric_rate" in payload.observed_evidence[0]
    assert "DO_NOT_EMAIL_RAW_REPORT" not in payload.model_dump_json()


def test_missing_investigation_evidence_remains_explicitly_unknown():
    payload = build_email_payload(
        decision(),
        investigation_report=None,
        investigation_error="TimeoutError: bounded investigation timeout",
    )
    message = render_notification_email(
        notification_id="c" * 64,
        payload=payload,
        config=EmailDeliveryConfig.mock(),
    )

    assert payload.investigation_completeness == "unavailable"
    assert payload.root_cause.status == "unknown"
    assert payload.impact.status == "unknown"
    assert "No supported recommendation" in message.text_body
    assert "investigation:TimeoutError" in message.text_body


def test_permanent_provider_error_is_terminal_without_exposing_provider_exception():
    class PermanentFailureSender:
        provider_name = "test-provider"

        def send(self, message):
            raise PermanentEmailDeliveryError(
                provider=self.provider_name,
                safe_message="sender identity is not verified",
            )

    item = outbox_record()
    repository = InMemoryNotificationOutboxRepository([item])
    result = deliver_notifications(
        repository=repository,
        sender=PermanentFailureSender(),
        worker_id="worker-1",
        now=NOW,
        email_config=provider_config("test-provider"),
    )

    assert result.failed_count == 1
    assert result.retryable_count == 0
    assert result.attempts[0].error == "sender identity is not verified"
    assert repository.records[item.notification_id].attempt_count == 1
    assert repository.records[item.notification_id].status == "FAILED"


def test_unclassified_provider_error_is_safely_logged_and_bounded_for_retry(caplog):
    class UnexpectedFailureSender:
        provider_name = "test-provider"

        def send(self, message):
            raise RuntimeError("SENSITIVE_PROVIDER_DETAIL")

    item = outbox_record()
    repository = InMemoryNotificationOutboxRepository([item])
    result = deliver_notifications(
        repository=repository,
        sender=UnexpectedFailureSender(),
        worker_id="worker-1",
        now=NOW,
        email_config=provider_config("test-provider"),
    )

    assert result.retryable_count == 1
    assert result.attempts[0].error == "unexpected email delivery failure (RuntimeError)"
    assert "SENSITIVE_PROVIDER_DETAIL" not in caplog.text
    assert "SENSITIVE_PROVIDER_DETAIL" not in repository.records[item.notification_id].last_error


def test_live_sender_requires_explicit_matching_routing_configuration():
    class LiveSender:
        provider_name = "ses"

        def send(self, message):
            raise AssertionError("configuration must fail before delivery")

    item = outbox_record()
    repository = InMemoryNotificationOutboxRepository([item])

    with pytest.raises(ValueError, match="explicit EmailDeliveryConfig"):
        deliver_notifications(
            repository=repository,
            sender=LiveSender(),
            worker_id="worker-1",
            now=NOW,
        )

    assert repository.records[item.notification_id].status == "PENDING"


def test_default_mock_delivery_is_no_network_and_duplicate_protected():
    item = outbox_record()
    repository = InMemoryNotificationOutboxRepository([item])

    first = deliver_notifications(
        repository=repository,
        worker_id="worker-1",
        now=NOW,
    )
    second = deliver_notifications(
        repository=repository,
        worker_id="worker-2",
        now=NOW,
    )

    assert first.sent_count == 1
    assert second.claimed_count == 0
    assert repository.records[item.notification_id].status == "SENT"
