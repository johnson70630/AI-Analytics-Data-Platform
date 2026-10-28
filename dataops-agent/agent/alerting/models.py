"""Versioned models for read-only incident detection and lifecycle decisions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


IncidentSource = Literal["jobs", "observability"]
ObservationStatus = Literal["PASS", "WARN", "FAIL", "UNKNOWN", "UNAVAILABLE"]
AssociationStatus = Literal["MATCHED", "UNCERTAIN", "NOT_APPLICABLE"]
IncidentLifecycleStatus = Literal["OPEN", "RESOLVED"]
AlertDecisionType = Literal[
    "BASELINED",
    "NEW",
    "ONGOING",
    "PENDING_RESOLUTION",
    "RESOLVED",
    "RECURRING",
    "HEALTHY_NO_INCIDENT",
    "INSUFFICIENT_EVIDENCE",
    "DUPLICATE",
]
BaselineMode = Literal["suppress_existing", "process_existing"]
NotificationStatus = Literal["PENDING", "CLAIMED", "SENT", "FAILED", "RETRYABLE"]
NotificationEventType = Literal["NEW", "RECURRING", "RESOLVED"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _require_aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class RunCursor(StrictModel):
    """Last completed run processed by the incident engine."""

    completed_at: datetime
    run_id: int

    @field_validator("completed_at")
    @classmethod
    def completed_at_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "completed_at")


class IncidentFingerprint(StrictModel):
    """Stable check identity that intentionally excludes observation timestamps."""

    source: IncidentSource
    category: str
    identity: str
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class EvidenceAssociation(StrictModel):
    """Whether observability evidence can be tied to one pipeline execution."""

    status: AssociationStatus
    reason: str
    execution_run_id: int | None = None
    monitoring_run_at: list[datetime] = Field(default_factory=list)

    @field_validator("monitoring_run_at")
    @classmethod
    def monitoring_times_must_be_aware(cls, values: list[datetime]) -> list[datetime]:
        return [_require_aware(value, "monitoring_run_at") for value in values]


class MonitoringObservation(StrictModel):
    """One check result at one execution or monitoring snapshot."""

    observation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    fingerprint: IncidentFingerprint
    status: ObservationStatus
    observed_at: datetime
    reference_time: datetime | None = None
    synthetic_reference_date: str | None = None
    execution_run_id: int | None = None
    association: EvidenceAssociation
    evidence: dict[str, Any]

    @field_validator("observed_at")
    @classmethod
    def observed_at_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "observed_at")

    @field_validator("reference_time")
    @classmethod
    def reference_time_must_be_aware(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _require_aware(value, "reference_time")


class MonitoringSourceError(StrictModel):
    source: IncidentSource
    error_type: str
    message: str


class DetectionResult(StrictModel):
    """Pure detection output; unavailable sources never become PASS observations."""

    observations: list[MonitoringObservation]
    source_availability: dict[IncidentSource, bool]
    source_errors: list[MonitoringSourceError] = Field(default_factory=list)


class IncidentRecord(StrictModel):
    fingerprint: IncidentFingerprint
    lifecycle_status: IncidentLifecycleStatus
    episode_number: int = Field(ge=1)
    first_seen_at: datetime
    episode_started_at: datetime
    last_seen_at: datetime
    last_status: ObservationStatus
    last_observation_id: str
    observation_count: int = Field(ge=1)
    consecutive_passes: int = Field(ge=0)
    resolved_at: datetime | None = None

    @field_validator("first_seen_at", "episode_started_at", "last_seen_at")
    @classmethod
    def incident_times_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "incident timestamp")

    @field_validator("resolved_at")
    @classmethod
    def resolved_at_must_be_aware(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _require_aware(value, "resolved_at")


class LifecycleSnapshot(StrictModel):
    """Repository-neutral lifecycle state with optimistic-concurrency versioning."""

    schema_version: Literal["1.0"] = "1.0"
    version: int = Field(default=0, ge=0)
    initialized: bool = False
    run_cursor: RunCursor | None = None
    incidents: dict[str, IncidentRecord] = Field(default_factory=dict)
    processed_observation_ids: list[str] = Field(default_factory=list)


class AlertDecision(StrictModel):
    decision: AlertDecisionType
    observation_id: str | None
    fingerprint: IncidentFingerprint | None
    incident: IncidentRecord | None
    would_investigate: bool
    would_notify: bool
    reason: str


class TransitionResult(StrictModel):
    snapshot: LifecycleSnapshot
    decisions: list[AlertDecision]


class CompletedRunCollection(StrictModel):
    """Chronologically ordered completed run evidence from the paginated Jobs API."""

    runs: list[dict[str, Any]]
    next_cursor: RunCursor | None
    scanned_run_count: int = Field(ge=0)
    selected_run_count: int = Field(ge=0)
    estimated_page_count: int = Field(ge=0)
    page_size: int = Field(ge=1, le=24)


class DryRunReport(StrictModel):
    """Non-persisting preview of lifecycle and future delivery decisions."""

    schema_version: Literal["1.0"] = "1.0"
    generated_at: datetime
    baseline_mode: BaselineMode
    committed: Literal[False] = False
    source_availability: dict[IncidentSource, bool]
    source_errors: list[MonitoringSourceError]
    observation_count: int = Field(ge=0)
    decisions: list[AlertDecision]
    would_notify_count: int = Field(ge=0)
    would_investigate_count: int = Field(ge=0)
    next_state: LifecycleSnapshot

    @field_validator("generated_at")
    @classmethod
    def generated_at_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "generated_at")


class IncidentEpisodeRecord(StrictModel):
    """Durable projection of one incident episode."""

    episode_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    fingerprint: IncidentFingerprint
    episode_number: int = Field(ge=1)
    lifecycle_status: IncidentLifecycleStatus
    first_seen_at: datetime
    episode_started_at: datetime
    last_seen_at: datetime
    resolved_at: datetime | None = None
    last_status: ObservationStatus
    last_observation_id: str
    observation_count: int = Field(ge=1)
    consecutive_passes: int = Field(ge=0)
    version: int = Field(ge=1)

    @field_validator("first_seen_at", "episode_started_at", "last_seen_at")
    @classmethod
    def episode_times_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "episode timestamp")

    @field_validator("resolved_at")
    @classmethod
    def episode_resolved_at_must_be_aware(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _require_aware(value, "resolved_at")


class PersistedObservation(StrictModel):
    observation: MonitoringObservation
    episode_id: str | None
    decision: AlertDecisionType
    would_investigate: bool
    would_notify: bool
    recorded_at: datetime

    @field_validator("recorded_at")
    @classmethod
    def recorded_at_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "recorded_at")


class MonitorCheckpoint(StrictModel):
    checkpoint_name: str
    snapshot_version: int = Field(ge=0)
    transition_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    run_cursor: RunCursor | None
    snapshot_json: str
    updated_at: datetime

    @field_validator("updated_at")
    @classmethod
    def checkpoint_updated_at_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "updated_at")


class EmailRecommendation(StrictModel):
    priority: str
    category: str
    action: str
    rationale: str
    evidence_refs: list[str]
    requires_human_approval: bool
    verification_steps: list[str]


class EmailGenieFinding(StrictModel):
    status: str
    question: str
    answer: str | None


class NotificationEmailPayload(StrictModel):
    """Bounded email content; deliberately excludes raw report JSON and query rows."""

    subject: str
    event_type: NotificationEventType
    episode_id: str
    observed_facts: list[str]
    investigation_completeness: str
    genie_findings: list[EmailGenieFinding]
    recommendations: list[EmailRecommendation]
    evidence_references: list[str]
    source_errors: list[str]


class NotificationOutboxRecord(StrictModel):
    notification_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    episode_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_type: NotificationEventType
    status: NotificationStatus = "PENDING"
    payload: NotificationEmailPayload
    attempt_count: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    available_at: datetime
    claimed_by: str | None = None
    claim_token: str | None = None
    lease_expires_at: datetime | None = None
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime
    sent_at: datetime | None = None

    @field_validator("available_at", "created_at", "updated_at")
    @classmethod
    def outbox_times_must_be_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "outbox timestamp")

    @field_validator("lease_expires_at", "sent_at")
    @classmethod
    def optional_outbox_times_must_be_aware(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _require_aware(value, "outbox timestamp")


class PersistenceBundle(StrictModel):
    """Idempotent records persisted before the checkpoint is advanced."""

    checkpoint: MonitorCheckpoint
    episodes: list[IncidentEpisodeRecord]
    observations: list[PersistedObservation]
    notifications: list[NotificationOutboxRecord]


class IncidentProcessingResult(StrictModel):
    mode: Literal["dry_run", "persist"]
    committed: bool
    transition: TransitionResult
    investigation_report: dict[str, Any] | None
    investigation_error: str | None
    outbox_records: list[NotificationOutboxRecord]


class DeliveryAttempt(StrictModel):
    notification_id: str
    status: NotificationStatus
    error: str | None = None


class DeliveryRunResult(StrictModel):
    worker_id: str
    claimed_count: int = Field(ge=0)
    sent_count: int = Field(ge=0)
    retryable_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    attempts: list[DeliveryAttempt]
