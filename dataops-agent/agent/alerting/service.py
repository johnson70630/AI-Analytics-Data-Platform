"""Application services for state application and non-persisting dry runs."""

from __future__ import annotations

from datetime import datetime, timezone

from agent.alerting.lifecycle import transition_lifecycle
from agent.alerting.models import (
    BaselineMode,
    DetectionResult,
    DryRunReport,
    RunCursor,
    TransitionResult,
)
from agent.alerting.repository import (
    ConcurrentStateUpdateError,
    IncidentStateRepository,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_alerting_dry_run(
    *,
    repository: IncidentStateRepository,
    detection: DetectionResult,
    cursor: RunCursor | None = None,
    baseline_mode: BaselineMode = "suppress_existing",
    generated_at: datetime | None = None,
) -> DryRunReport:
    """Preview investigation/notification decisions without saving state."""

    transition = transition_lifecycle(
        repository.load(),
        detection,
        cursor=cursor,
        baseline_mode=baseline_mode,
    )
    return DryRunReport(
        generated_at=generated_at or _utc_now(),
        baseline_mode=baseline_mode,
        source_availability=detection.source_availability,
        source_errors=detection.source_errors,
        observation_count=len(detection.observations),
        decisions=transition.decisions,
        would_notify_count=sum(item.would_notify for item in transition.decisions),
        would_investigate_count=sum(item.would_investigate for item in transition.decisions),
        next_state=transition.snapshot,
    )


def apply_detection(
    *,
    repository: IncidentStateRepository,
    detection: DetectionResult,
    cursor: RunCursor | None = None,
    baseline_mode: BaselineMode = "suppress_existing",
    max_retries: int = 3,
) -> TransitionResult:
    """Apply to an injected repository with bounded optimistic-concurrency retries."""

    if max_retries < 1:
        raise ValueError("max_retries must be positive")
    last_error: ConcurrentStateUpdateError | None = None
    for _ in range(max_retries):
        current = repository.load()
        transition = transition_lifecycle(
            current,
            detection,
            cursor=cursor,
            baseline_mode=baseline_mode,
        )
        try:
            repository.save(transition.snapshot, expected_version=current.version)
            return transition
        except ConcurrentStateUpdateError as exc:
            last_error = exc
    raise last_error or ConcurrentStateUpdateError("state update failed")
