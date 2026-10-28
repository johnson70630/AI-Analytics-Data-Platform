"""Side-effect-free boundary for future notification delivery."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from agent.alerting.models import AlertDecision


class IncidentNotificationDelivery(Protocol):
    """Future delivery interface; V2.3A intentionally provides no implementation."""

    def deliver(self, decisions: Sequence[AlertDecision]) -> None:
        """Deliver approved notification candidates."""


def notification_candidates(decisions: Sequence[AlertDecision]) -> list[AlertDecision]:
    """Select decisions a future notifier may deliver without sending anything."""

    return [decision for decision in decisions if decision.would_notify]
