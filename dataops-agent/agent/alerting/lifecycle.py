"""Pure, idempotent incident lifecycle transitions."""

from __future__ import annotations

from collections.abc import Iterable

from agent.alerting.models import (
    AlertDecision,
    BaselineMode,
    DetectionResult,
    IncidentRecord,
    LifecycleSnapshot,
    MonitoringObservation,
    RunCursor,
    TransitionResult,
)


_ACTIVE_STATUSES = {"WARN", "FAIL"}


def _decision(
    name: str,
    observation: MonitoringObservation | None,
    incident: IncidentRecord | None,
    *,
    reason: str,
    would_investigate: bool = False,
    would_notify: bool = False,
) -> AlertDecision:
    return AlertDecision(
        decision=name,
        observation_id=observation.observation_id if observation else None,
        fingerprint=observation.fingerprint if observation else None,
        incident=incident,
        would_investigate=would_investigate,
        would_notify=would_notify,
        reason=reason,
    )


def _new_incident(observation: MonitoringObservation) -> IncidentRecord:
    return IncidentRecord(
        fingerprint=observation.fingerprint,
        lifecycle_status="OPEN",
        episode_number=1,
        first_seen_at=observation.observed_at,
        episode_started_at=observation.observed_at,
        last_seen_at=observation.observed_at,
        last_status=observation.status,
        last_observation_id=observation.observation_id,
        observation_count=1,
        consecutive_passes=0,
    )


def _ordered(observations: Iterable[MonitoringObservation]) -> list[MonitoringObservation]:
    return sorted(
        observations,
        key=lambda item: (
            item.observed_at,
            item.fingerprint.source,
            item.fingerprint.identity,
            item.observation_id,
        ),
    )


def _transition_observation(
    incidents: dict[str, IncidentRecord],
    observation: MonitoringObservation,
) -> AlertDecision:
    key = observation.fingerprint.digest
    incident = incidents.get(key)

    if observation.status in {"UNKNOWN", "UNAVAILABLE"}:
        return _decision(
            "INSUFFICIENT_EVIDENCE",
            observation,
            incident,
            reason="unknown or unavailable evidence cannot resolve or create an incident",
        )

    if observation.status in _ACTIVE_STATUSES:
        if incident is None:
            incident = _new_incident(observation)
            incidents[key] = incident
            return _decision(
                "NEW",
                observation,
                incident,
                reason="first non-PASS observation for this stable check identity",
                would_investigate=True,
                would_notify=True,
            )
        if incident.lifecycle_status == "RESOLVED":
            incident = incident.model_copy(
                update={
                    "lifecycle_status": "OPEN",
                    "episode_number": incident.episode_number + 1,
                    "episode_started_at": observation.observed_at,
                    "last_seen_at": observation.observed_at,
                    "last_status": observation.status,
                    "last_observation_id": observation.observation_id,
                    "observation_count": incident.observation_count + 1,
                    "consecutive_passes": 0,
                    "resolved_at": None,
                }
            )
            incidents[key] = incident
            return _decision(
                "RECURRING",
                observation,
                incident,
                reason="the same stable check identity recurred after confirmed resolution",
                would_investigate=True,
                would_notify=True,
            )
        incident = incident.model_copy(
            update={
                "last_seen_at": observation.observed_at,
                "last_status": observation.status,
                "last_observation_id": observation.observation_id,
                "observation_count": incident.observation_count + 1,
                "consecutive_passes": 0,
            }
        )
        incidents[key] = incident
        return _decision(
            "ONGOING",
            observation,
            incident,
            reason="repeated non-PASS observation for the open incident episode",
        )

    if incident is None or incident.lifecycle_status == "RESOLVED":
        return _decision(
            "HEALTHY_NO_INCIDENT",
            observation,
            incident,
            reason="PASS observed with no open incident for this check identity",
        )

    pass_count = incident.consecutive_passes + 1
    resolved = pass_count >= 2
    incident = incident.model_copy(
        update={
            "lifecycle_status": "RESOLVED" if resolved else "OPEN",
            "last_seen_at": observation.observed_at,
            "last_status": observation.status,
            "last_observation_id": observation.observation_id,
            "observation_count": incident.observation_count + 1,
            "consecutive_passes": pass_count,
            "resolved_at": observation.observed_at if resolved else None,
        }
    )
    incidents[key] = incident
    if resolved:
        return _decision(
            "RESOLVED",
            observation,
            incident,
            reason="two consecutive distinct PASS observations confirmed resolution",
            would_notify=True,
        )
    return _decision(
        "PENDING_RESOLUTION",
        observation,
        incident,
        reason="one PASS observation is insufficient to resolve the open incident",
    )


def _baseline_snapshot(
    snapshot: LifecycleSnapshot,
    detection: DetectionResult,
    cursor: RunCursor | None,
) -> TransitionResult:
    incidents = {key: value.model_copy(deep=True) for key, value in snapshot.incidents.items()}
    processed = set(snapshot.processed_observation_ids)
    decisions: list[AlertDecision] = []
    for observation in _ordered(detection.observations):
        if observation.observation_id in processed:
            decisions.append(
                _decision(
                    "DUPLICATE",
                    observation,
                    incidents.get(observation.fingerprint.digest),
                    reason="observation was already processed",
                )
            )
            continue
        processed.add(observation.observation_id)
        critical_execution_failure = (
            observation.fingerprint.source == "jobs"
            and observation.fingerprint.category == "execution"
            and observation.status == "FAIL"
        )
        if critical_execution_failure:
            decisions.append(_transition_observation(incidents, observation))
            continue
        if observation.status in _ACTIVE_STATUSES:
            incident = incidents.get(observation.fingerprint.digest) or _new_incident(observation)
            incidents[observation.fingerprint.digest] = incident
        else:
            incident = incidents.get(observation.fingerprint.digest)
        decisions.append(
            _decision(
                "BASELINED",
                observation,
                incident,
                reason="initial evidence was recorded without creating a notification storm",
            )
        )
    for source, available in detection.source_availability.items():
        if not available:
            decisions.append(
                _decision(
                    "INSUFFICIENT_EVIDENCE",
                    None,
                    None,
                    reason=f"{source} evidence was unavailable during initial baseline",
                )
            )
    return TransitionResult(
        snapshot=LifecycleSnapshot(
            version=snapshot.version + 1,
            initialized=True,
            run_cursor=cursor or snapshot.run_cursor,
            incidents=incidents,
            processed_observation_ids=sorted(processed),
        ),
        decisions=decisions,
    )


def transition_lifecycle(
    snapshot: LifecycleSnapshot,
    detection: DetectionResult,
    *,
    cursor: RunCursor | None = None,
    baseline_mode: BaselineMode = "suppress_existing",
) -> TransitionResult:
    """Apply observations without side effects and return the next snapshot."""

    if not snapshot.initialized and baseline_mode == "suppress_existing":
        return _baseline_snapshot(snapshot, detection, cursor)

    incidents = {key: value.model_copy(deep=True) for key, value in snapshot.incidents.items()}
    processed = set(snapshot.processed_observation_ids)
    decisions: list[AlertDecision] = []

    for source, available in detection.source_availability.items():
        if available:
            continue
        for key, incident in list(incidents.items()):
            if incident.lifecycle_status == "OPEN" and incident.fingerprint.source == source:
                incidents[key] = incident.model_copy(update={"consecutive_passes": 0})
        decisions.append(
            _decision(
                "INSUFFICIENT_EVIDENCE",
                None,
                None,
                reason=f"{source} evidence was unavailable; open incidents were not resolved",
            )
        )

    for observation in _ordered(detection.observations):
        if observation.observation_id in processed:
            decisions.append(
                _decision(
                    "DUPLICATE",
                    observation,
                    incidents.get(observation.fingerprint.digest),
                    reason="observation was already processed",
                )
            )
            continue
        processed.add(observation.observation_id)
        decisions.append(_transition_observation(incidents, observation))

    return TransitionResult(
        snapshot=LifecycleSnapshot(
            version=snapshot.version + 1,
            initialized=True,
            run_cursor=cursor or snapshot.run_cursor,
            incidents=incidents,
            processed_observation_ids=sorted(processed),
        ),
        decisions=decisions,
    )
