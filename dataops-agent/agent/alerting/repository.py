"""Dependency-injected incident state repository interfaces."""

from __future__ import annotations

from typing import Protocol

from agent.alerting.models import LifecycleSnapshot, PersistenceBundle


class ConcurrentStateUpdateError(RuntimeError):
    """Raised when optimistic state versioning detects a concurrent writer."""


class IncidentStateRepository(Protocol):
    """Minimal state contract for a future durable implementation."""

    def load(self) -> LifecycleSnapshot:
        """Return the current lifecycle snapshot."""

    def save(self, snapshot: LifecycleSnapshot, *, expected_version: int) -> None:
        """Atomically persist when the stored version matches expected_version."""


class DurableIncidentRegistry(IncidentStateRepository, Protocol):
    """Normalized durable persistence contract used only in explicit persist mode."""

    def persist_transition(
        self,
        bundle: PersistenceBundle,
        *,
        expected_version: int,
        lease_owner: str,
    ) -> None:
        """Persist observations, episodes, outbox, then advance the checkpoint."""


class InMemoryIncidentStateRepository:
    """Non-production repository used only by tests and local dry runs."""

    def __init__(self, snapshot: LifecycleSnapshot | None = None) -> None:
        self._snapshot = (snapshot or LifecycleSnapshot()).model_copy(deep=True)
        self.save_count = 0

    def load(self) -> LifecycleSnapshot:
        return self._snapshot.model_copy(deep=True)

    def save(self, snapshot: LifecycleSnapshot, *, expected_version: int) -> None:
        if self._snapshot.version != expected_version:
            raise ConcurrentStateUpdateError(
                f"state version changed: expected={expected_version}, actual={self._snapshot.version}"
            )
        self._snapshot = snapshot.model_copy(deep=True)
        self.save_count += 1


class InMemoryDurableIncidentRegistry(InMemoryIncidentStateRepository):
    """Test-only normalized registry; not a production persistence implementation."""

    def __init__(self, snapshot: LifecycleSnapshot | None = None) -> None:
        super().__init__(snapshot)
        self.episodes: dict[str, object] = {}
        self.observations: dict[str, object] = {}
        self.notifications: dict[str, object] = {}
        self.persist_count = 0

    def persist_transition(
        self,
        bundle: PersistenceBundle,
        *,
        expected_version: int,
        lease_owner: str,
    ) -> None:
        del lease_owner
        if self._snapshot.version != expected_version:
            raise ConcurrentStateUpdateError(
                f"state version changed: expected={expected_version}, actual={self._snapshot.version}"
            )
        for item in bundle.observations:
            self.observations.setdefault(item.observation.observation_id, item.model_copy(deep=True))
        for item in bundle.episodes:
            self.episodes[item.episode_id] = item.model_copy(deep=True)
        for item in bundle.notifications:
            self.notifications.setdefault(item.notification_id, item.model_copy(deep=True))
        self._snapshot = LifecycleSnapshot.model_validate_json(bundle.checkpoint.snapshot_json)
        self.persist_count += 1
