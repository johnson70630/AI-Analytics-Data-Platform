"""Deterministic Daily Health Review investigation action."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agent.investigation import InvestigationReportV11
from agent.tools.investigation import fetch_dataops_investigation_report


INVESTIGATION_ACTION = "generate_dataops_investigation_report"
InvestigationRunner = Callable[[], Awaitable[dict[str, Any]]]


class StrictActionModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PipelineSnapshot(StrictActionModel):
    status: str = Field(min_length=1, max_length=32)
    run_id: int | None = None
    result_state: str | None = Field(default=None, max_length=64)
    end_time: datetime | None = None

    @field_validator("end_time")
    @classmethod
    def end_time_must_be_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("end_time must be timezone-aware")
        return value


class MonitoringSnapshot(StrictActionModel):
    status: str = Field(min_length=1, max_length=32)
    monitoring_run_at: datetime | None = None
    monitoring_reference_at: datetime | None = None
    synthetic_reference_date: date | None = None
    pipeline_association_verified: Literal[False] = False

    @field_validator("monitoring_run_at", "monitoring_reference_at")
    @classmethod
    def monitoring_times_must_be_timezone_aware(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("monitoring timestamps must be timezone-aware")
        return value


class HealthSnapshotContext(StrictActionModel):
    schema_version: Literal["1.0"]
    generated_at: datetime
    overall_status: str = Field(min_length=1, max_length=32)
    pipeline: PipelineSnapshot
    monitoring: MonitoringSnapshot

    @field_validator("generated_at")
    @classmethod
    def generated_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        return value


class InvestigationActionRequest(StrictActionModel):
    action: Literal[INVESTIGATION_ACTION]
    health_snapshot: HealthSnapshotContext


def _snapshot_fingerprint(snapshot: HealthSnapshotContext) -> str:
    encoded = json.dumps(snapshot.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def execute_investigation_action(
    payload: dict[str, Any],
    *,
    runner: InvestigationRunner | None = None,
) -> dict[str, Any]:
    """Validate display context and execute the existing read-only workflow exactly once.

    ``health_snapshot`` is untrusted browser context used only to label the response. It is never
    passed into the investigation graph and cannot influence evidence, findings, or recommendations.
    """
    request = InvestigationActionRequest.model_validate(
        {
            "action": payload.get("action"),
            "health_snapshot": payload.get("health_snapshot"),
        }
    )
    report_data = await fetch_dataops_investigation_report(runner=runner)
    report = InvestigationReportV11.model_validate(report_data)
    snapshot = request.health_snapshot
    return {
        "output": [
            {
                "type": "investigation_report",
                "action": INVESTIGATION_ACTION,
                "snapshot_fingerprint": _snapshot_fingerprint(snapshot),
                "health_snapshot": snapshot.model_dump(mode="json"),
                "pipeline_monitoring_association": {
                    "verified": False,
                    "message": (
                        "The displayed health snapshot and the newly collected investigation "
                        "are separate evidence retrievals; run association is unverified."
                    ),
                },
                "report": report.model_dump(mode="json"),
            }
        ],
        "status": "completed",
    }
