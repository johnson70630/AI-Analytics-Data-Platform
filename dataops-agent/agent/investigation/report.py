"""Versioned Pydantic schema for DataOps investigation reports."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceStatus(StrictReportModel):
    available: bool
    status: str


class SourceAvailability(StrictReportModel):
    jobs: SourceStatus
    observability: SourceStatus
    genie: SourceStatus


class PipelineExecutionHealth(StrictReportModel):
    status: str
    pipeline_failed: bool | None
    run_found: bool | None
    result_state: str | None
    lifecycle_state: str | None
    run_id: int | None
    run_started_at: str | None
    run_ended_at: str | None
    unsuccessful_tasks: list[str]


class MonitoringHealth(StrictReportModel):
    status: str
    check_count: int | None
    issue_count: int | None
    warning_count: int | None
    failure_count: int | None
    monitoring_execution_timestamps: list[str]
    monitoring_reference_timestamps: list[str]
    synthetic_activity_dates: dict[str, list[str]]


class DetectedIssue(StrictReportModel):
    source: Literal["jobs", "observability"]
    category: str
    status: str
    name: str
    details: dict[str, Any]


class SourceError(StrictReportModel):
    source: Literal["jobs", "observability", "genie"]
    error_type: str
    message: str


class EvidenceAssessment(StrictReportModel):
    status: Literal["unknown", "supported"]
    explanation: str


class GenieFindingReport(StrictReportModel):
    investigation_id: str
    question: str
    status: Literal["complete", "partial", "unavailable"]
    answer: str | None
    evidence: dict[str, Any] | None
    error: dict[str, Any] | None
    root_cause: EvidenceAssessment
    impact: EvidenceAssessment


class InvestigationReportV1(StrictReportModel):
    """Stable output contract for the V2.1 investigation workflow."""

    schema_version: Literal["1.0"] = "1.0"
    generated_at: datetime
    source_availability: SourceAvailability
    pipeline_execution_health: PipelineExecutionHealth
    monitoring_health: MonitoringHealth
    detected_issues: list[DetectedIssue]
    genie_findings: list[GenieFindingReport]
    source_errors: list[SourceError]
    investigation_completeness: Literal["complete", "partial", "unavailable"]
    genie_routing: dict[str, Any]
    evidence: dict[str, Any]
    source_references: dict[str, Any]
    root_cause: EvidenceAssessment = Field(
        default_factory=lambda: EvidenceAssessment(
            status="unknown",
            explanation="The retrieved evidence does not establish a root cause.",
        )
    )
    impact: EvidenceAssessment = Field(
        default_factory=lambda: EvidenceAssessment(
            status="unknown",
            explanation="The retrieved evidence does not quantify impact.",
        )
    )
    workflow: dict[str, Any]

    @field_validator("generated_at")
    @classmethod
    def generated_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        return value
