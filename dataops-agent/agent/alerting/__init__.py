"""Standalone DataOps incident detection and lifecycle engine."""

from agent.alerting.collection import fetch_completed_pipeline_runs_since
from agent.alerting.delta_repository import (
    DatabricksFixedStatementExecutor,
    DeltaIncidentRegistry,
    FixedStatementExecutor,
    SqlExecutionResult,
)
from agent.alerting.delivery import IncidentNotificationDelivery, notification_candidates
from agent.alerting.detection import (
    assess_observability_association,
    detect_execution_observations,
    detect_monitoring_observations,
    detect_observability_observations,
    incident_fingerprint,
)
from agent.alerting.lifecycle import transition_lifecycle
from agent.alerting.models import (
    AlertDecision,
    CompletedRunCollection,
    DetectionResult,
    DeliveryRunResult,
    DryRunReport,
    EvidenceAssociation,
    IncidentFingerprint,
    IncidentProcessingResult,
    IncidentRecord,
    LifecycleSnapshot,
    MonitoringObservation,
    NotificationOutboxRecord,
    RunCursor,
    TransitionResult,
)
from agent.alerting.outbox import (
    InMemoryNotificationOutboxRepository,
    MockEmailSender,
    build_email_payload,
    build_outbox_record,
    deliver_notifications,
    incident_episode_id,
    notification_identity,
)
from agent.alerting.processor import InvestigationRunner, process_incidents
from agent.alerting.repository import (
    ConcurrentStateUpdateError,
    DurableIncidentRegistry,
    IncidentStateRepository,
    InMemoryDurableIncidentRegistry,
    InMemoryIncidentStateRepository,
)
from agent.alerting.service import apply_detection, run_alerting_dry_run

__all__ = [
    "AlertDecision",
    "CompletedRunCollection",
    "ConcurrentStateUpdateError",
    "DatabricksFixedStatementExecutor",
    "DeltaIncidentRegistry",
    "DetectionResult",
    "DeliveryRunResult",
    "DryRunReport",
    "DurableIncidentRegistry",
    "EvidenceAssociation",
    "FixedStatementExecutor",
    "IncidentFingerprint",
    "IncidentProcessingResult",
    "IncidentRecord",
    "IncidentNotificationDelivery",
    "IncidentStateRepository",
    "InMemoryDurableIncidentRegistry",
    "InMemoryIncidentStateRepository",
    "InMemoryNotificationOutboxRepository",
    "InvestigationRunner",
    "LifecycleSnapshot",
    "MockEmailSender",
    "MonitoringObservation",
    "NotificationOutboxRecord",
    "RunCursor",
    "SqlExecutionResult",
    "TransitionResult",
    "apply_detection",
    "assess_observability_association",
    "build_email_payload",
    "build_outbox_record",
    "detect_execution_observations",
    "detect_monitoring_observations",
    "detect_observability_observations",
    "deliver_notifications",
    "fetch_completed_pipeline_runs_since",
    "incident_episode_id",
    "incident_fingerprint",
    "notification_identity",
    "notification_candidates",
    "process_incidents",
    "run_alerting_dry_run",
    "transition_lifecycle",
]
