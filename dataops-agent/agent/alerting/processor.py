"""Dry-run-first orchestration for lifecycle, investigation, and persistence."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Literal, cast

from agent.alerting.lifecycle import transition_lifecycle
from agent.alerting.models import (
    BaselineMode,
    DetectionResult,
    IncidentProcessingResult,
    NotificationOutboxRecord,
    RunCursor,
)
from agent.alerting.outbox import build_outbox_record
from agent.alerting.persistence import build_persistence_bundle
from agent.alerting.repository import (
    ConcurrentStateUpdateError,
    DurableIncidentRegistry,
    IncidentStateRepository,
)
from agent.investigation.report import InvestigationReportV11


InvestigationRunner = Callable[[], Awaitable[dict]]
ProcessingMode = Literal["dry_run", "persist"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def process_incidents(
    *,
    repository: IncidentStateRepository,
    detection: DetectionResult,
    cursor: RunCursor | None = None,
    mode: ProcessingMode = "dry_run",
    baseline_mode: BaselineMode = "suppress_existing",
    investigation_runner: InvestigationRunner | None = None,
    checkpoint_name: str = "dataops-agent",
    lease_owner: str = "dataops-agent-worker",
    now: datetime | None = None,
    max_persistence_retries: int = 3,
) -> IncidentProcessingResult:
    """Process one detection batch; dry-run is the default and performs no writes."""

    if max_persistence_retries < 1:
        raise ValueError("max_persistence_retries must be positive")
    recorded_at = now or _utc_now()
    cached_investigation: dict | None = None
    cached_investigation_error: str | None = None
    investigation_attempted = False
    last_conflict: ConcurrentStateUpdateError | None = None

    for _ in range(max_persistence_retries):
        current = repository.load()
        transition = transition_lifecycle(
            current,
            detection,
            cursor=cursor,
            baseline_mode=baseline_mode,
        )
        eligible_for_investigation = any(
            decision.decision in {"NEW", "RECURRING"} and decision.would_investigate
            for decision in transition.decisions
        )
        if eligible_for_investigation and not investigation_attempted:
            investigation_attempted = True
            if investigation_runner is None:
                cached_investigation_error = "investigation runner was not configured"
            else:
                try:
                    raw_report = await investigation_runner()
                    cached_investigation = InvestigationReportV11.model_validate(raw_report).model_dump(
                        mode="json"
                    )
                except Exception as exc:
                    cached_investigation_error = f"{type(exc).__name__}: {exc}"

        outbox_records: list[NotificationOutboxRecord] = []
        for decision in transition.decisions:
            if not decision.would_notify or decision.decision not in {"NEW", "RECURRING", "RESOLVED"}:
                continue
            report = cached_investigation if decision.decision in {"NEW", "RECURRING"} else None
            error = (
                cached_investigation_error
                if decision.decision in {"NEW", "RECURRING"}
                else None
            )
            outbox_records.append(
                build_outbox_record(
                    decision,
                    now=recorded_at,
                    investigation_report=report,
                    investigation_error=error,
                )
            )

        if mode == "dry_run":
            return IncidentProcessingResult(
                mode="dry_run",
                committed=False,
                transition=transition,
                investigation_report=cached_investigation,
                investigation_error=cached_investigation_error,
                outbox_records=outbox_records,
            )

        if not hasattr(repository, "persist_transition"):
            raise TypeError("persist mode requires a DurableIncidentRegistry")
        duplicate_only = bool(transition.decisions) and all(
            decision.decision == "DUPLICATE" for decision in transition.decisions
        )
        if duplicate_only and (cursor is None or cursor == current.run_cursor):
            return IncidentProcessingResult(
                mode="persist",
                committed=False,
                transition=transition.model_copy(update={"snapshot": current}),
                investigation_report=cached_investigation,
                investigation_error=cached_investigation_error,
                outbox_records=[],
            )
        durable = cast(DurableIncidentRegistry, repository)
        bundle = build_persistence_bundle(
            transition=transition,
            detection=detection,
            notifications=outbox_records,
            checkpoint_name=checkpoint_name,
            cursor=cursor,
            recorded_at=recorded_at,
        )
        try:
            durable.persist_transition(
                bundle,
                expected_version=current.version,
                lease_owner=lease_owner,
            )
        except ConcurrentStateUpdateError as exc:
            last_conflict = exc
            continue
        return IncidentProcessingResult(
            mode="persist",
            committed=True,
            transition=transition,
            investigation_report=cached_investigation,
            investigation_error=cached_investigation_error,
            outbox_records=outbox_records,
        )
    raise last_conflict or ConcurrentStateUpdateError("persistence retries were exhausted")
