from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError, NoCredentialsError, ReadTimeoutError

from agent.alerting import (
    EmailDeliveryConfig,
    InMemoryNotificationOutboxRepository,
    PermanentEmailDeliveryError,
    RetryableEmailDeliveryError,
    SesEmailSender,
    deliver_notifications,
)
from agent.alerting.delivery import RenderedEmail
from agent.alerting.models import NotificationEmailPayload, NotificationOutboxRecord
from agent.alerting.outbox import notification_identity


NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
EPISODE_ID = "d" * 64


class StubSesClient:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    def send_email(self, **kwargs):
        self.requests.append(kwargs)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def client_error(code: str, message: str = "provider detail must not escape") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "SendEmail",
    )


def rendered_email() -> RenderedEmail:
    return RenderedEmail(
        notification_id="a" * 64,
        provider="ses",
        sender_address="alerts@example.com",
        recipient_addresses=("operator@example.com",),
        reply_to="dataops@example.com",
        subject="[DataOps] WARN quality incident",
        text_body="Plain-text incident evidence",
        html_body="<html><body>HTML incident evidence</body></html>",
    )


def outbox_record() -> NotificationOutboxRecord:
    payload = NotificationEmailPayload(
        subject="[DataOps] WARN quality incident",
        event_type="NEW",
        episode_id=EPISODE_ID,
        observed_facts=["source=observability", "observed_status=WARN"],
        investigation_completeness="partial",
        genie_findings=[],
        recommendations=[],
        evidence_references=[],
        source_errors=[],
        severity="warning",
        incident_status="WARN",
    )
    return NotificationOutboxRecord(
        notification_id=notification_identity(EPISODE_ID, "NEW"),
        episode_id=EPISODE_ID,
        event_type="NEW",
        payload=payload,
        max_attempts=3,
        available_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )


def ses_config() -> EmailDeliveryConfig:
    return EmailDeliveryConfig(
        provider="ses",
        sender_address="alerts@example.com",
        recipient_addresses=("operator@example.com",),
    )


def test_successful_ses_acceptance_uses_v2_simple_content_and_returns_receipt():
    client = StubSesClient([{"MessageId": "ses-message-123"}])
    sender = SesEmailSender(client=client)

    receipt = sender.send(rendered_email())

    assert receipt.provider_name == "ses"
    assert receipt.provider_message_id == "ses-message-123"
    assert receipt.status == "accepted"
    request = client.requests[0]
    assert request["FromEmailAddress"] == "alerts@example.com"
    assert request["Destination"] == {"ToAddresses": ["operator@example.com"]}
    assert request["ReplyToAddresses"] == ["dataops@example.com"]
    assert request["Content"]["Simple"]["Subject"]["Charset"] == "UTF-8"
    assert request["Content"]["Simple"]["Body"]["Text"]["Data"] == (
        "Plain-text incident evidence"
    )
    assert request["Content"]["Simple"]["Body"]["Html"]["Data"].startswith("<html>")
    assert request["EmailTags"] == [
        {"Name": "dataops_notification_id", "Value": "a" * 64}
    ]


@pytest.mark.parametrize(
    "code",
    [
        "MessageRejected",
        "MailFromDomainNotVerifiedException",
        "SendingPausedException",
        "BadRequestException",
    ],
)
def test_sandbox_unverified_identity_and_permanent_rejections_are_not_retried(code):
    sender = SesEmailSender(client=StubSesClient([client_error(code)]))

    with pytest.raises(PermanentEmailDeliveryError) as error:
        sender.send(rendered_email())

    assert error.value.retryable is False
    assert code in error.value.safe_message
    assert "provider detail must not escape" not in error.value.safe_message


def test_missing_credentials_and_permission_failure_are_permanent():
    missing = SesEmailSender(client=StubSesClient([NoCredentialsError()]))
    denied = SesEmailSender(
        client=StubSesClient([client_error("AccessDeniedException")])
    )

    with pytest.raises(PermanentEmailDeliveryError, match="credentials are unavailable"):
        missing.send(rendered_email())
    with pytest.raises(PermanentEmailDeliveryError, match="AccessDeniedException"):
        denied.send(rendered_email())


@pytest.mark.parametrize(
    "failure",
    [
        client_error("TooManyRequestsException"),
        client_error("ThrottlingException"),
        ReadTimeoutError(endpoint_url="https://email.us-west-2.amazonaws.com"),
    ],
)
def test_rate_limits_and_transport_timeouts_are_retryable(failure):
    sender = SesEmailSender(client=StubSesClient([failure]))

    with pytest.raises(RetryableEmailDeliveryError) as error:
        sender.send(rendered_email())

    assert error.value.retryable is True
    assert "provider detail must not escape" not in error.value.safe_message


def test_unexpected_provider_error_is_sanitized_and_retryable():
    sender = SesEmailSender(
        client=StubSesClient([RuntimeError("SENSITIVE_PROVIDER_RESPONSE")])
    )

    with pytest.raises(RetryableEmailDeliveryError) as error:
        sender.send(rendered_email())

    assert error.value.safe_message == "unexpected SES delivery failure (RuntimeError)"
    assert "SENSITIVE_PROVIDER_RESPONSE" not in error.value.safe_message


def test_missing_message_id_is_retryable_because_acceptance_cannot_be_recorded():
    sender = SesEmailSender(client=StubSesClient([{}]))

    with pytest.raises(RetryableEmailDeliveryError, match="message identifier"):
        sender.send(rendered_email())


def test_outbox_retries_throttling_then_records_ses_acceptance_without_duplicates():
    item = outbox_record()
    repository = InMemoryNotificationOutboxRepository([item])
    client = StubSesClient(
        [client_error("TooManyRequestsException"), {"MessageId": "ses-message-456"}]
    )
    sender = SesEmailSender(client=client)

    first = deliver_notifications(
        repository=repository,
        sender=sender,
        email_config=ses_config(),
        worker_id="worker-1",
        now=NOW,
        retry_delay_seconds=10,
    )
    second = deliver_notifications(
        repository=repository,
        sender=sender,
        email_config=ses_config(),
        worker_id="worker-2",
        now=NOW + timedelta(seconds=11),
    )
    duplicate = deliver_notifications(
        repository=repository,
        sender=sender,
        email_config=ses_config(),
        worker_id="worker-3",
        now=NOW + timedelta(seconds=20),
    )

    stored = repository.records[item.notification_id]
    assert first.retryable_count == 1
    assert second.sent_count == 1
    assert second.attempts[0].provider_status == "accepted"
    assert duplicate.claimed_count == 0
    assert len(client.requests) == 2
    assert stored.status == "SENT"
    assert stored.provider_name == "ses"
    assert stored.provider_message_id == "ses-message-456"
    assert stored.provider_status == "accepted"


def test_permanent_ses_rejection_terminates_outbox_after_one_attempt():
    item = outbox_record()
    repository = InMemoryNotificationOutboxRepository([item])
    sender = SesEmailSender(
        client=StubSesClient([client_error("MailFromDomainNotVerifiedException")])
    )

    result = deliver_notifications(
        repository=repository,
        sender=sender,
        email_config=ses_config(),
        worker_id="worker-1",
        now=NOW,
    )

    assert result.failed_count == 1
    assert repository.records[item.notification_id].status == "FAILED"
    assert repository.records[item.notification_id].attempt_count == 1
