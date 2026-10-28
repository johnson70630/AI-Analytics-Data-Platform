"""Provider-neutral rendering and delivery boundary for incident email."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from email.utils import parseaddr
from html import escape
from typing import Literal, Protocol

from agent.alerting.models import AlertDecision, NotificationEmailPayload


MOCK_SENDER = "dataops-alerts@example.invalid"
MOCK_RECIPIENT = "dataops-operator@example.invalid"


def notification_candidates(decisions: Sequence[AlertDecision]) -> list[AlertDecision]:
    """Select decisions eligible for outbox delivery without sending anything."""

    return [decision for decision in decisions if decision.would_notify]


def _valid_address(value: str) -> bool:
    name, address = parseaddr(value)
    return (
        not name
        and address == value
        and "@" in address
        and "\n" not in value
        and "\r" not in value
    )


@dataclass(frozen=True)
class EmailDeliveryConfig:
    """Non-secret routing configuration shared by all email providers."""

    provider: str
    sender_address: str
    recipient_addresses: tuple[str, ...]
    reply_to: str | None = None

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("email provider must not be empty")
        if not _valid_address(self.sender_address):
            raise ValueError("DATAOPS_EMAIL_FROM must be a single valid email address")
        if not self.recipient_addresses:
            raise ValueError("DATAOPS_EMAIL_TO must contain at least one recipient")
        if any(not _valid_address(value) for value in self.recipient_addresses):
            raise ValueError("DATAOPS_EMAIL_TO contains an invalid email address")
        if self.reply_to is not None and not _valid_address(self.reply_to):
            raise ValueError("DATAOPS_EMAIL_REPLY_TO must be a valid email address")

    @classmethod
    def mock(cls) -> EmailDeliveryConfig:
        """Safe default that cannot route to a real Internet mailbox."""

        return cls(
            provider="mock",
            sender_address=MOCK_SENDER,
            recipient_addresses=(MOCK_RECIPIENT,),
        )

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> EmailDeliveryConfig:
        """Load non-secret routing values; provider credentials remain adapter-owned."""

        values = os.environ if environment is None else environment
        provider = values.get("DATAOPS_EMAIL_PROVIDER", "mock").strip().lower()
        if provider == "mock":
            return cls(
                provider="mock",
                sender_address=values.get("DATAOPS_EMAIL_FROM", MOCK_SENDER).strip(),
                recipient_addresses=tuple(
                    item.strip()
                    for item in values.get("DATAOPS_EMAIL_TO", MOCK_RECIPIENT).split(",")
                    if item.strip()
                ),
                reply_to=values.get("DATAOPS_EMAIL_REPLY_TO") or None,
            )
        missing = [
            name
            for name in ("DATAOPS_EMAIL_FROM", "DATAOPS_EMAIL_TO")
            if not values.get(name, "").strip()
        ]
        if missing:
            raise ValueError(f"missing required email routing configuration: {', '.join(missing)}")
        return cls(
            provider=provider,
            sender_address=values["DATAOPS_EMAIL_FROM"].strip(),
            recipient_addresses=tuple(
                item.strip() for item in values["DATAOPS_EMAIL_TO"].split(",") if item.strip()
            ),
            reply_to=values.get("DATAOPS_EMAIL_REPLY_TO") or None,
        )


@dataclass(frozen=True)
class RenderedEmail:
    """Complete provider-neutral message passed to a delivery adapter."""

    notification_id: str
    provider: str
    sender_address: str
    recipient_addresses: tuple[str, ...]
    reply_to: str | None
    subject: str
    text_body: str
    html_body: str


@dataclass(frozen=True)
class EmailDeliveryReceipt:
    """Sanitized provider acceptance metadata; it is not proof of inbox delivery."""

    provider_name: str
    provider_message_id: str
    status: Literal["accepted"] = "accepted"

    def __post_init__(self) -> None:
        if not self.provider_name.strip():
            raise ValueError("email receipt provider_name must not be empty")
        if not self.provider_message_id.strip():
            raise ValueError("email receipt provider_message_id must not be empty")


class EmailDeliveryError(RuntimeError):
    """Safe provider error whose message is suitable for the durable outbox."""

    def __init__(self, *, provider: str, safe_message: str, retryable: bool) -> None:
        self.provider = provider
        self.safe_message = " ".join(safe_message.split())[:500]
        self.retryable = retryable
        super().__init__(self.safe_message)


class RetryableEmailDeliveryError(EmailDeliveryError):
    def __init__(self, *, provider: str, safe_message: str) -> None:
        super().__init__(provider=provider, safe_message=safe_message, retryable=True)


class PermanentEmailDeliveryError(EmailDeliveryError):
    def __init__(self, *, provider: str, safe_message: str) -> None:
        super().__init__(provider=provider, safe_message=safe_message, retryable=False)


class EmailSender(Protocol):
    """Provider adapter contract; implementations receive an already-rendered message."""

    provider_name: str

    def send(self, message: RenderedEmail) -> EmailDeliveryReceipt:
        """Return provider acceptance metadata or raise a classified delivery error."""


class MockEmailSender:
    """Default no-network provider used by tests and unconfigured runtimes."""

    provider_name = "mock"

    def __init__(self, *, failures_before_success: int = 0) -> None:
        if failures_before_success < 0:
            raise ValueError("failures_before_success must not be negative")
        self.failures_remaining = failures_before_success
        self.sent_messages: list[RenderedEmail] = []

    def send(self, message: RenderedEmail) -> EmailDeliveryReceipt:
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RetryableEmailDeliveryError(
                provider=self.provider_name,
                safe_message="mock email delivery failure",
            )
        self.sent_messages.append(message)
        return EmailDeliveryReceipt(
            provider_name=self.provider_name,
            provider_message_id=f"mock-{message.notification_id}",
        )


def _text_lines(title: str, values: Sequence[str], *, empty: str = "None reported") -> list[str]:
    lines = [title]
    lines.extend(f"- {value}" for value in values)
    if not values:
        lines.append(f"- {empty}")
    return lines


def _html_list(values: Sequence[str], *, empty: str = "None reported") -> str:
    items = values or [empty]
    return "<ul>" + "".join(f"<li>{escape(value)}</li>" for value in items) + "</ul>"


def _render_text(payload: NotificationEmailPayload) -> str:
    lines = [
        "DATAOPS INCIDENT NOTIFICATION",
        "",
        f"Event: {payload.event_type}",
        f"Severity: {payload.severity.upper()}",
        f"Incident status: {payload.incident_status}",
        f"Episode: {payload.episode_id}",
        f"Investigation completeness: {payload.investigation_completeness}",
        "",
    ]
    lines.extend(_text_lines("Observed evidence", payload.observed_evidence or payload.observed_facts))
    lines.extend(["", *_text_lines("Monitoring execution timestamps", payload.monitoring_execution_timestamps)])
    lines.extend(["", *_text_lines("Monitoring reference timestamps", payload.monitoring_reference_timestamps)])
    for label, dates in sorted(payload.synthetic_activity_dates.items()):
        lines.extend(["", *_text_lines(f"Synthetic activity dates — {label}", dates)])
    lines.extend(
        [
            "",
            "Root cause assessment",
            f"- Status: {payload.root_cause.status}",
            f"- {payload.root_cause.explanation}",
            "",
            "Impact assessment",
            f"- Status: {payload.impact.status}",
            f"- {payload.impact.explanation}",
        ]
    )
    lines.extend(["", "Top recommendations"])
    if not payload.recommendations:
        lines.append("- No supported recommendation is available from the retrieved evidence.")
    for index, recommendation in enumerate(payload.recommendations[:3], start=1):
        lines.extend(
            [
                f"{index}. [{recommendation.priority}] {recommendation.category}: {recommendation.action}",
                f"   Rationale: {recommendation.rationale}",
                "   Human approval required: "
                + ("Yes" if recommendation.requires_human_approval else "No"),
                "   Evidence: " + ", ".join(recommendation.evidence_refs),
                "   Verification:",
                *[f"   - {step}" for step in recommendation.verification_steps],
            ]
        )
    if payload.source_errors:
        lines.extend(["", *_text_lines("Unavailable or partial evidence", payload.source_errors)])
    lines.extend(
        [
            "",
            "This message reports retrieved evidence and proposed guidance only. No corrective action was executed.",
        ]
    )
    return "\n".join(lines)


def _render_recommendations_html(payload: NotificationEmailPayload) -> str:
    if not payload.recommendations:
        return (
            '<div class="empty">No supported recommendation is available from the retrieved '
            "evidence.</div>"
        )
    cards: list[str] = []
    for recommendation in payload.recommendations[:3]:
        approval = "Required" if recommendation.requires_human_approval else "Not required"
        cards.append(
            '<div class="recommendation">'
            f'<div class="tag">{escape(recommendation.priority)} · '
            f"{escape(recommendation.category)}</div>"
            f"<h3>{escape(recommendation.action)}</h3>"
            f"<p><strong>Why:</strong> {escape(recommendation.rationale)}</p>"
            f"<p><strong>Human approval:</strong> {approval}</p>"
            f"<p><strong>Evidence:</strong> {escape(', '.join(recommendation.evidence_refs))}</p>"
            "<p><strong>Verification:</strong></p>"
            f"{_html_list(recommendation.verification_steps)}"
            "</div>"
        )
    return "".join(cards)


def _render_html(payload: NotificationEmailPayload) -> str:
    evidence = payload.observed_evidence or payload.observed_facts
    activity_sections = "".join(
        f"<h3>Synthetic activity dates — {escape(label)}</h3>{_html_list(dates)}"
        for label, dates in sorted(payload.synthetic_activity_dates.items())
    )
    errors = (
        f'<section><h2>Unavailable or partial evidence</h2>{_html_list(payload.source_errors)}</section>'
        if payload.source_errors
        else ""
    )
    severity_class = escape(payload.severity)
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;background:#07111f;color:#dce9f7;font-family:Arial,sans-serif">
<div style="display:none;max-height:0;overflow:hidden">{escape(payload.subject)}</div>
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#07111f">
<tr><td align="center" style="padding:28px 12px">
<table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:680px;width:100%;background:#0e1b2c;border:1px solid #263a50;border-radius:14px;overflow:hidden">
<tr><td style="padding:28px;background:#10233b;border-bottom:1px solid #263a50">
<div style="color:#55d8ff;font-size:12px;font-weight:700;letter-spacing:1.5px">DATAOPS AI COMMAND CENTER</div>
<h1 style="margin:10px 0 12px;color:#ffffff;font-size:26px">{escape(payload.event_type)} incident notification</h1>
<span class="{severity_class}" style="display:inline-block;padding:6px 10px;border-radius:999px;background:#243952;color:#ffffff;font-size:12px;font-weight:700">{escape(payload.severity.upper())}</span>
</td></tr>
<tr><td style="padding:26px">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-bottom:22px"><tr>
<td style="padding:10px;background:#13253a;border-radius:8px"><strong>Status</strong><br>{escape(payload.incident_status)}</td>
<td style="width:10px"></td>
<td style="padding:10px;background:#13253a;border-radius:8px"><strong>Investigation</strong><br>{escape(payload.investigation_completeness)}</td>
</tr></table>
<section><h2 style="color:#55d8ff;font-size:18px">Observed evidence</h2>{_html_list(evidence)}</section>
<section><h2 style="color:#55d8ff;font-size:18px">Monitoring context</h2>
<h3>Monitoring execution timestamps</h3>{_html_list(payload.monitoring_execution_timestamps)}
<h3>Monitoring reference timestamps</h3>{_html_list(payload.monitoring_reference_timestamps)}
{activity_sections}</section>
<section><h2 style="color:#55d8ff;font-size:18px">Evidence assessments</h2>
<p><strong>Root cause ({escape(payload.root_cause.status)}):</strong> {escape(payload.root_cause.explanation)}</p>
<p><strong>Impact ({escape(payload.impact.status)}):</strong> {escape(payload.impact.explanation)}</p></section>
<section><h2 style="color:#55d8ff;font-size:18px">Top recommendations</h2>{_render_recommendations_html(payload)}</section>
{errors}
<p style="margin-top:28px;padding-top:18px;border-top:1px solid #263a50;color:#91a8bf;font-size:12px">This notification contains bounded evidence and proposed guidance only. No corrective action was executed.</p>
</td></tr></table>
</td></tr></table>
</body></html>"""


def render_notification_email(
    *,
    notification_id: str,
    payload: NotificationEmailPayload,
    config: EmailDeliveryConfig,
) -> RenderedEmail:
    """Render one bounded outbox payload into text and HTML alternatives."""

    return RenderedEmail(
        notification_id=notification_id,
        provider=config.provider,
        sender_address=config.sender_address,
        recipient_addresses=config.recipient_addresses,
        reply_to=config.reply_to,
        subject=payload.subject,
        text_body=_render_text(payload),
        html_body=_render_html(payload),
    )


class IncidentNotificationDelivery(Protocol):
    """Broad delivery boundary retained for decision-batch consumers."""

    def deliver(self, decisions: Sequence[AlertDecision]) -> None:
        """Deliver already-approved notification candidates."""
