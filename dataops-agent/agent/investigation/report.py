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


class Recommendation(StrictReportModel):
    """A read-only proposed action grounded in report evidence references."""

    issue_id: str
    priority: Literal["P0", "P1", "P2", "P3"]
    category: Literal["diagnostic", "proposed_remediation", "verification"]
    action: str
    rationale: str
    evidence_refs: list[str] = Field(min_length=1)
    requires_human_approval: bool
    verification_steps: list[str] = Field(min_length=1)


class RecommendationSummary(StrictReportModel):
    status: Literal["generated", "no_findings", "no_supported_recommendation"]
    recommendation_count: int = Field(ge=0)
    unsupported_issue_count: int = Field(ge=0)
    message: str


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


class InvestigationReportV11(InvestigationReportV1):
    """Additive V1.1 contract with deterministic recommendations."""

    schema_version: Literal["1.1"] = "1.1"
    recommendations: list[Recommendation]
    recommendation_summary: RecommendationSummary
