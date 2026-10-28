"""State and future-extension interfaces for the investigation graph."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypedDict


SourceName = Literal["jobs", "observability", "genie"]
HealthStatus = Literal["PASS", "WARN", "FAIL", "RUNNING", "UNKNOWN", "UNAVAILABLE"]


class InvestigationError(TypedDict):
    """A bounded error safe to include in the deterministic report."""

    source: SourceName
    error_type: str
    message: str


class InvestigationIssue(TypedDict):
    """A non-PASS observed condition from one evidence source."""

    source: Literal["jobs", "observability"]
    category: str
    status: str
    name: str
    details: dict[str, Any]


class ExecutionHealth(TypedDict):
    """Lakeflow execution health, kept separate from monitored data health."""

    status: HealthStatus
    pipeline_failed: bool | None
    run_found: bool | None
    result_state: str | None
    lifecycle_state: str | None
    run_id: int | None
    run_started_at: str | None
    run_ended_at: str | None
    unsuccessful_tasks: list[str]


class DataHealth(TypedDict):
    """Health of the latest observability checks, not the whole platform."""

    status: HealthStatus
    check_count: int | None
    issue_count: int | None
    warning_count: int | None
    failure_count: int | None
    monitoring_execution_timestamps: list[str]
    monitoring_reference_timestamps: list[str]
    data_reference_dates: dict[str, list[str]]


class GenieInvestigationRequest(TypedDict):
    """A bounded historical question selected from one observed anomaly."""

    investigation_id: str
    issue_name: str
    category: Literal["quality", "row_counts", "freshness"]
    status: Literal["WARN", "FAIL"]
    question: str
    source_reference: dict[str, Any]


class GenieSelectionDecision(TypedDict):
    """Why an observed issue was selected for or excluded from Genie."""

    issue_name: str
    category: str
    status: str
    selected: bool
    reason: str
    investigation_id: str | None


class GenieFinding(TypedDict):
    """One normalized Genie investigation outcome with original evidence."""

    investigation_id: str
    question: str
    status: Literal["complete", "partial", "unavailable"]
    answer: str | None
    evidence: dict[str, Any] | None
    error: dict[str, Any] | None
    root_cause: dict[str, str]
    impact: dict[str, str]


RecommendationPriority = Literal["P0", "P1", "P2", "P3"]
RecommendationCategory = Literal["diagnostic", "proposed_remediation", "verification"]


class RecommendationStateItem(TypedDict):
    """One deterministic, evidence-referenced recommendation."""

    issue_id: str
    priority: RecommendationPriority
    category: RecommendationCategory
    action: str
    rationale: str
    evidence_refs: list[str]
    requires_human_approval: bool
    verification_steps: list[str]


class RecommendationSummaryState(TypedDict):
    """Describe whether this evidence supports any recommendations."""

    status: Literal["generated", "no_findings", "no_supported_recommendation"]
    recommendation_count: int
    unsupported_issue_count: int
    message: str


class InvestigationState(TypedDict, total=False):
    """State shared by parallel evidence, evaluation, Genie, and report nodes."""

    jobs_evidence: dict[str, Any] | None
    observability_evidence: dict[str, Any] | None
    jobs_error: InvestigationError | None
    observability_error: InvestigationError | None
    errors: list[InvestigationError]
    execution_health: ExecutionHealth
    data_health: DataHealth
    issues: list[InvestigationIssue]
    genie_requests: list[GenieInvestigationRequest]
    genie_selection: list[GenieSelectionDecision]
    genie_findings: list[GenieFinding]
    genie_errors: list[InvestigationError]
    genie_route_reason: str
    recommendations: list[RecommendationStateItem]
    recommendation_summary: RecommendationSummaryState
    evaluation_status: Literal["completed", "skipped_no_evidence"]
    workflow_path: list[str]
    report: dict[str, Any]


GenieEvidenceFetcher = Callable[[str], Awaitable[dict[str, Any]]]
"""Async interface matching ``fetch_genie_observability(question)``."""
