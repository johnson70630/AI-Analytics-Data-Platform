from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agent.alerting import (
    InMemoryNotificationOutboxRepository,
    MockEmailSender,
    deliver_notifications,
    notification_identity,
)
from agent.alerting.models import NotificationEmailPayload, NotificationOutboxRecord


NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
EPISODE_ID = "a" * 64


def record(*, max_attempts=3) -> NotificationOutboxRecord:
    payload = NotificationEmailPayload(
        subject="[DataOps] NEW quality incident",
        event_type="NEW",
        episode_id=EPISODE_ID,
        observed_facts=["source=observability", "observed_status=WARN"],
        investigation_completeness="partial",
        genie_findings=[],
        recommendations=[],
        evidence_references=[],
        source_errors=[],
    )
    return NotificationOutboxRecord(
        notification_id=notification_identity(EPISODE_ID, "NEW"),
        episode_id=EPISODE_ID,
        event_type="NEW",
        payload=payload,
        max_attempts=max_attempts,
        available_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )


def test_notification_identity_uses_episode_and_event_type():
    assert notification_identity(EPISODE_ID, "NEW") == notification_identity(EPISODE_ID, "NEW")
    assert notification_identity(EPISODE_ID, "NEW") != notification_identity(EPISODE_ID, "RESOLVED")
    assert notification_identity(EPISODE_ID, "NEW") != notification_identity("b" * 64, "NEW")


def test_lease_prevents_concurrent_workers_from_claiming_same_notification():
    repository = InMemoryNotificationOutboxRepository([record()])

    first = repository.claim(worker_id="worker-1", now=NOW, lease_seconds=60, limit=10)
    second = repository.claim(worker_id="worker-2", now=NOW, lease_seconds=60, limit=10)
    reclaimed = repository.claim(
        worker_id="worker-2",
        now=NOW + timedelta(seconds=61),
        lease_seconds=60,
        limit=10,
    )

    assert len(first) == 1
    assert second == []
    assert len(reclaimed) == 1
    assert reclaimed[0].attempt_count == 2
    assert reclaimed[0].claim_token != first[0].claim_token
    with pytest.raises(RuntimeError, match="claim is not owned"):
        repository.mark_sent(
            notification_id=first[0].notification_id,
            claim_token=first[0].claim_token,
            sent_at=NOW + timedelta(seconds=62),
        )


def test_expired_claim_cannot_mark_notification_sent_without_reclaim():
    repository = InMemoryNotificationOutboxRepository([record()])
    claimed = repository.claim(worker_id="worker-1", now=NOW, lease_seconds=60, limit=1)[0]

    with pytest.raises(RuntimeError, match="lease has expired"):
        repository.mark_sent(
            notification_id=claimed.notification_id,
            claim_token=claimed.claim_token or "",
            sent_at=NOW + timedelta(seconds=60),
        )

    assert repository.records[claimed.notification_id].status == "CLAIMED"


def test_mock_email_failure_becomes_retryable_then_sent():
    item = record()
    repository = InMemoryNotificationOutboxRepository([item])
    sender = MockEmailSender(failures_before_success=1)

    failed = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-1",
        now=NOW,
        retry_delay_seconds=30,
    )
    too_early = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-2",
        now=NOW + timedelta(seconds=20),
    )
    sent = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-2",
        now=NOW + timedelta(seconds=31),
    )

    assert failed.retryable_count == 1
    assert failed.attempts[0].status == "RETRYABLE"
    assert too_early.claimed_count == 0
    assert sent.sent_count == 1
    assert repository.records[item.notification_id].status == "SENT"
    assert len(sender.sent_payloads) == 1


def test_bounded_mock_failures_end_in_terminal_failed_state():
    item = record(max_attempts=2)
    repository = InMemoryNotificationOutboxRepository([item])
    sender = MockEmailSender(failures_before_success=10)

    first = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-1",
        now=NOW,
        retry_delay_seconds=1,
    )
    second = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-2",
        now=NOW + timedelta(seconds=2),
        retry_delay_seconds=1,
    )
    third = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-3",
        now=NOW + timedelta(seconds=4),
    )

    assert first.retryable_count == 1
    assert second.failed_count == 1
    assert second.attempts[0].status == "FAILED"
    assert third.claimed_count == 0
    assert repository.records[item.notification_id].attempt_count == 2
    assert sender.sent_payloads == []


def test_successful_delivery_is_not_claimed_again_but_not_claimed_as_exactly_once():
    item = record()
    repository = InMemoryNotificationOutboxRepository([item])
    sender = MockEmailSender()

    result = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-1",
        now=NOW,
    )
    repeated = deliver_notifications(
        repository=repository,
        sender=sender,
        worker_id="worker-2",
        now=NOW + timedelta(minutes=10),
    )

    assert result.sent_count == 1
    assert repeated.claimed_count == 0
    assert repository.records[item.notification_id].status == "SENT"
