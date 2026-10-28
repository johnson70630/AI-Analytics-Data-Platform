"""Notification outbox identity, leasing, and mock-only delivery."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol
from uuid import uuid4

from agent.alerting.models import (
    AlertDecision,
    DeliveryAttempt,
    DeliveryRunResult,
    EmailGenieFinding,
    EmailRecommendation,
    NotificationEmailPayload,
    NotificationEventType,
    NotificationOutboxRecord,
)
from agent.investigation.report import InvestigationReportV11


def _bounded(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    text = " ".join(value.split())
    return text[:limit]


def incident_episode_id(fingerprint_digest: str, episode_number: int) -> str:
    return hashlib.sha256(f"{fingerprint_digest}:{episode_number}".encode()).hexdigest()


def notification_identity(episode_id: str, event_type: str) -> str:
    """Stable outbox identity; retries cannot create a second logical notification."""

    return hashlib.sha256(f"{episode_id}:{event_type}".encode()).hexdigest()


def build_email_payload(
    decision: AlertDecision,
    *,
    investigation_report: dict | None,
    investigation_error: str | None = None,
) -> NotificationEmailPayload:
    """Build concise facts and recommendations without embedding raw report evidence."""

    if decision.incident is None or decision.fingerprint is None:
        raise ValueError("notification decisions require an incident and fingerprint")
    if decision.decision not in {"NEW", "RECURRING", "RESOLVED"}:
        raise ValueError(f"decision {decision.decision!r} is not notification eligible")
    event_type: NotificationEventType = decision.decision
    episode_id = incident_episode_id(
        decision.fingerprint.digest,
        decision.incident.episode_number,
    )
    observed_facts = [
        f"source={decision.fingerprint.source}",
        f"category={decision.fingerprint.category}",
        f"check_identity={decision.fingerprint.identity}",
        f"observed_status={decision.incident.last_status}",
        f"observed_at={decision.incident.last_seen_at.isoformat()}",
        f"episode={decision.incident.episode_number}",
    ]
    completeness = "unavailable"
    genie_findings: list[EmailGenieFinding] = []
    recommendations: list[EmailRecommendation] = []
    evidence_references: list[str] = []
    source_errors: list[str] = []
    if investigation_report is not None:
        report = InvestigationReportV11.model_validate(investigation_report)
        completeness = report.investigation_completeness
        genie_findings = [
            EmailGenieFinding(
                status=finding.status,
                question=_bounded(finding.question, 500) or "",
                answer=_bounded(finding.answer, 1500),
            )
            for finding in report.genie_findings[:2]
        ]
        recommendations = [
            EmailRecommendation(
                priority=item.priority,
                category=item.category,
                action=_bounded(item.action, 1000) or "",
                rationale=_bounded(item.rationale, 750) or "",
                evidence_refs=item.evidence_refs,
                requires_human_approval=item.requires_human_approval,
                verification_steps=[
                    _bounded(step, 500) or "" for step in item.verification_steps[:4]
                ],
            )
            for item in report.recommendations[:6]
        ]
        evidence_references = sorted(
            {
                reference
                for recommendation in recommendations
                for reference in recommendation.evidence_refs
            }
        )
        source_errors = [
            _bounded(f"{error.source}:{error.error_type}:{error.message}", 750) or ""
            for error in report.source_errors
        ]
    if investigation_error:
        source_errors.append(_bounded(f"investigation:{investigation_error}", 750) or "")
    return NotificationEmailPayload(
        subject=_bounded(
            f"[DataOps] {event_type} {decision.fingerprint.category} incident "
            f"{decision.fingerprint.identity}",
            200,
        )
        or "[DataOps] incident notification",
        event_type=event_type,
        episode_id=episode_id,
        observed_facts=observed_facts,
        investigation_completeness=completeness,
        genie_findings=genie_findings,
        recommendations=recommendations,
        evidence_references=evidence_references,
        source_errors=source_errors,
    )


def build_outbox_record(
    decision: AlertDecision,
    *,
    now: datetime,
    investigation_report: dict | None,
    investigation_error: str | None = None,
    max_attempts: int = 3,
) -> NotificationOutboxRecord:
    payload = build_email_payload(
        decision,
        investigation_report=investigation_report,
        investigation_error=investigation_error,
    )
    return NotificationOutboxRecord(
        notification_id=notification_identity(payload.episode_id, payload.event_type),
        episode_id=payload.episode_id,
        event_type=payload.event_type,
        payload=payload,
        max_attempts=max_attempts,
        available_at=now,
        created_at=now,
        updated_at=now,
    )


class EmailSender(Protocol):
    def send(self, payload: NotificationEmailPayload) -> None:
        """Send one bounded payload."""


class NotificationOutboxRepository(Protocol):
    def claim(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_seconds: int,
        limit: int,
    ) -> list[NotificationOutboxRecord]: ...

    def mark_sent(
        self,
        *,
        notification_id: str,
        claim_token: str,
        sent_at: datetime,
    ) -> None: ...

    def mark_failed(
        self,
        *,
        notification_id: str,
        claim_token: str,
        error: str,
        failed_at: datetime,
        retry_at: datetime,
    ) -> NotificationOutboxRecord: ...


class MockEmailSender:
    """Injectable test sender; it has no SMTP or external network implementation."""

    def __init__(self, *, failures_before_success: int = 0) -> None:
        self.failures_remaining = failures_before_success
        self.sent_payloads: list[NotificationEmailPayload] = []

    def send(self, payload: NotificationEmailPayload) -> None:
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("mock email delivery failure")
        self.sent_payloads.append(payload)


class InMemoryNotificationOutboxRepository:
    """Test-only outbox with lease and bounded retry semantics."""

    def __init__(self, records: Sequence[NotificationOutboxRecord] = ()) -> None:
        self.records = {item.notification_id: item.model_copy(deep=True) for item in records}

    def enqueue(self, record: NotificationOutboxRecord) -> None:
        self.records.setdefault(record.notification_id, record.model_copy(deep=True))

    def claim(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_seconds: int,
        limit: int,
    ) -> list[NotificationOutboxRecord]:
        if lease_seconds < 1 or limit < 1:
            raise ValueError("lease_seconds and limit must be positive")
        eligible = [
            item
            for item in self.records.values()
            if item.attempt_count < item.max_attempts
            and item.available_at <= now
            and (
                item.status in {"PENDING", "RETRYABLE"}
                or (
                    item.status == "CLAIMED"
                    and item.lease_expires_at is not None
                    and item.lease_expires_at <= now
                )
            )
        ]
        eligible.sort(key=lambda item: (item.available_at, item.created_at, item.notification_id))
        claimed: list[NotificationOutboxRecord] = []
        for item in eligible[:limit]:
            token = str(uuid4())
            updated = item.model_copy(
                update={
                    "status": "CLAIMED",
                    "claimed_by": worker_id,
                    "claim_token": token,
                    "lease_expires_at": now + timedelta(seconds=lease_seconds),
                    "attempt_count": item.attempt_count + 1,
                    "updated_at": now,
                }
            )
            self.records[item.notification_id] = updated
            claimed.append(updated.model_copy(deep=True))
        return claimed

    def _claimed(
        self,
        notification_id: str,
        claim_token: str,
        attempted_at: datetime,
    ) -> NotificationOutboxRecord:
        item = self.records[notification_id]
        if item.status != "CLAIMED" or item.claim_token != claim_token:
            raise RuntimeError("notification claim is not owned by this delivery attempt")
        if item.lease_expires_at is None or item.lease_expires_at <= attempted_at:
            raise RuntimeError("notification delivery lease has expired")
        return item

    def mark_sent(
        self,
        *,
        notification_id: str,
        claim_token: str,
        sent_at: datetime,
    ) -> None:
        item = self._claimed(notification_id, claim_token, sent_at)
        self.records[notification_id] = item.model_copy(
            update={
                "status": "SENT",
                "sent_at": sent_at,
                "updated_at": sent_at,
                "claimed_by": None,
                "claim_token": None,
                "lease_expires_at": None,
            }
        )

    def mark_failed(
        self,
        *,
        notification_id: str,
        claim_token: str,
        error: str,
        failed_at: datetime,
        retry_at: datetime,
    ) -> NotificationOutboxRecord:
        item = self._claimed(notification_id, claim_token, failed_at)
        status = "RETRYABLE" if item.attempt_count < item.max_attempts else "FAILED"
        updated = item.model_copy(
            update={
                "status": status,
                "available_at": retry_at,
                "last_error": error,
                "updated_at": failed_at,
                "claimed_by": None,
                "claim_token": None,
                "lease_expires_at": None,
            }
        )
        self.records[notification_id] = updated
        return updated.model_copy(deep=True)


def deliver_notifications(
    *,
    repository: NotificationOutboxRepository,
    sender: EmailSender,
    worker_id: str,
    now: datetime,
    limit: int = 10,
    lease_seconds: int = 300,
    retry_delay_seconds: int = 300,
) -> DeliveryRunResult:
    """Claim and deliver with at-least-once semantics; exactly-once is not guaranteed."""

    claimed = repository.claim(
        worker_id=worker_id,
        now=now,
        lease_seconds=lease_seconds,
        limit=limit,
    )
    attempts: list[DeliveryAttempt] = []
    sent_count = retryable_count = failed_count = 0
    for item in claimed:
        if item.claim_token is None:
            raise RuntimeError("claimed notification did not include a claim token")
        try:
            sender.send(item.payload)
        except Exception as exc:
            updated = repository.mark_failed(
                notification_id=item.notification_id,
                claim_token=item.claim_token,
                error=str(exc),
                failed_at=now,
                retry_at=now + timedelta(seconds=retry_delay_seconds),
            )
            retryable_count += updated.status == "RETRYABLE"
            failed_count += updated.status == "FAILED"
            attempts.append(
                DeliveryAttempt(
                    notification_id=item.notification_id,
                    status=updated.status,
                    error=str(exc),
                )
            )
        else:
            repository.mark_sent(
                notification_id=item.notification_id,
                claim_token=item.claim_token,
                sent_at=now,
            )
            sent_count += 1
            attempts.append(
                DeliveryAttempt(notification_id=item.notification_id, status="SENT")
            )
    return DeliveryRunResult(
        worker_id=worker_id,
        claimed_count=len(claimed),
        sent_count=sent_count,
        retryable_count=retryable_count,
        failed_count=failed_count,
        attempts=attempts,
    )
