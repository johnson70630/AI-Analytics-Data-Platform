"""Pure projection of lifecycle state into normalized durable records."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from agent.alerting.models import (
    DetectionResult,
    IncidentEpisodeRecord,
    MonitorCheckpoint,
    NotificationOutboxRecord,
    PersistenceBundle,
    PersistedObservation,
    RunCursor,
    TransitionResult,
)
from agent.alerting.outbox import incident_episode_id


def _require_unique(values: list[str], identity_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"persistence input contains duplicate {identity_name} values")


def build_persistence_bundle(
    *,
    transition: TransitionResult,
    detection: DetectionResult,
    notifications: list[NotificationOutboxRecord],
    checkpoint_name: str,
    cursor: RunCursor | None,
    recorded_at: datetime,
) -> PersistenceBundle:
    """Create deterministic records; database adapters decide how to persist them."""

    _require_unique(
        [observation.observation_id for observation in detection.observations],
        "observation_id",
    )
    _require_unique(
        [
            decision.observation_id
            for decision in transition.decisions
            if decision.observation_id is not None
        ],
        "decision observation_id",
    )
    _require_unique(
        [notification.notification_id for notification in notifications],
        "notification_id",
    )

    snapshot = transition.snapshot
    episodes = [
        IncidentEpisodeRecord(
            episode_id=incident_episode_id(key, incident.episode_number),
            fingerprint=incident.fingerprint,
            episode_number=incident.episode_number,
            lifecycle_status=incident.lifecycle_status,
            first_seen_at=incident.first_seen_at,
            episode_started_at=incident.episode_started_at,
            last_seen_at=incident.last_seen_at,
            resolved_at=incident.resolved_at,
            last_status=incident.last_status,
            last_observation_id=incident.last_observation_id,
            observation_count=incident.observation_count,
            consecutive_passes=incident.consecutive_passes,
            version=snapshot.version,
        )
        for key, incident in sorted(snapshot.incidents.items())
    ]
    decisions = {
        decision.observation_id: decision
        for decision in transition.decisions
        if decision.observation_id is not None
    }
    observations: list[PersistedObservation] = []
    for observation in detection.observations:
        decision = decisions[observation.observation_id]
        incident = snapshot.incidents.get(observation.fingerprint.digest)
        episode_id = (
            incident_episode_id(observation.fingerprint.digest, incident.episode_number)
            if incident is not None
            else None
        )
        observations.append(
            PersistedObservation(
                observation=observation,
                episode_id=episode_id,
                decision=decision.decision,
                would_investigate=decision.would_investigate,
                would_notify=decision.would_notify,
                recorded_at=recorded_at,
            )
        )
    transition_material = {
        "checkpoint_name": checkpoint_name,
        "snapshot_version": snapshot.version,
        "snapshot_json": snapshot.model_dump_json(),
        "episodes": [item.model_dump(mode="json") for item in episodes],
        "observations": [item.model_dump(mode="json") for item in observations],
        "notifications": [item.model_dump(mode="json") for item in notifications],
    }
    transition_id = hashlib.sha256(
        json.dumps(transition_material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    checkpoint = MonitorCheckpoint(
        checkpoint_name=checkpoint_name,
        snapshot_version=snapshot.version,
        transition_id=transition_id,
        run_cursor=cursor or snapshot.run_cursor,
        snapshot_json=snapshot.model_dump_json(),
        updated_at=recorded_at,
    )
    return PersistenceBundle(
        checkpoint=checkpoint,
        episodes=episodes,
        observations=observations,
        notifications=notifications,
    )
