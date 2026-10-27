"""Read-only LangGraph workflow for deterministic operational investigation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Literal

from databricks.sdk import WorkspaceClient
from langgraph.graph import END, START, StateGraph

from agent.investigation.report import InvestigationReportV1
from agent.investigation.state import (
    DataHealth,
    ExecutionHealth,
    GenieEvidenceFetcher,
    GenieFinding,
    GenieInvestigationRequest,
    GenieSelectionDecision,
    InvestigationError,
    InvestigationIssue,
    InvestigationState,
)
from agent.tools.genie import fetch_genie_observability
from agent.tools.jobs import fetch_latest_pipeline_run
from agent.tools.observability import fetch_latest_observability_summary


EvidenceFetcher = Callable[[], dict[str, Any]]
Clock = Callable[[], datetime]

MAX_GENIE_INVESTIGATIONS = 2
DEFAULT_GENIE_SERVICE_TIMEOUT_SECONDS = 120.0
# Allows the existing service's bounded answer wait plus up to five bounded result fetches.
DEFAULT_GENIE_NODE_TIMEOUT_SECONDS = 280.0

_OBSERVABILITY_CATEGORIES = {
    "freshness": "freshness_status",
    "row_counts": "volume_status",
    "quality": "quality_status",
    "reconciliation": "status",
}
_CATEGORY_PRIORITY = {"quality": 0, "row_counts": 1, "freshness": 2}
_STATUS_PRIORITY = {"FAIL": 0, "WARN": 1}
_UNKNOWN_ROOT_CAUSE = {
    "status": "unknown",
    "explanation": "The retrieved evidence does not establish a root cause.",
}
_UNKNOWN_IMPACT = {
    "status": "unknown",
    "explanation": "The retrieved evidence does not quantify impact.",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class InvestigationDependencies:
    """Injected read-only evidence functions used by the graph."""

    fetch_jobs: EvidenceFetcher
    fetch_observability: EvidenceFetcher
    fetch_genie: GenieEvidenceFetcher | None = None
    genie_timeout_seconds: float = DEFAULT_GENIE_NODE_TIMEOUT_SECONDS
    clock: Clock = _utc_now


def default_dependencies(
    *,
    jobs_client: WorkspaceClient | None = None,
    job_id: int | None = None,
    observability_client: WorkspaceClient | None = None,
    warehouse_id: str | None = None,
    genie_client: WorkspaceClient | None = None,
    genie_agent_id: str | None = None,
    genie_service_timeout_seconds: float = DEFAULT_GENIE_SERVICE_TIMEOUT_SECONDS,
    genie_node_timeout_seconds: float = DEFAULT_GENIE_NODE_TIMEOUT_SECONDS,
    clock: Clock = _utc_now,
) -> InvestigationDependencies:
    """Build lazy dependencies around the existing validated service functions."""

    async def fetch_genie(question: str) -> dict[str, Any]:
        return await fetch_genie_observability(
            question,
            client=genie_client,
            agent_id=genie_agent_id,
            timeout_seconds=genie_service_timeout_seconds,
        )

    return InvestigationDependencies(
        fetch_jobs=lambda: fetch_latest_pipeline_run(client=jobs_client, job_id=job_id),
        fetch_observability=lambda: fetch_latest_observability_summary(
            client=observability_client,
            warehouse_id=warehouse_id,
        ),
        fetch_genie=fetch_genie,
        genie_timeout_seconds=genie_node_timeout_seconds,
        clock=clock,
    )


def _error(
    source: Literal["jobs", "observability", "genie"],
    exc: Exception,
) -> InvestigationError:
    return {"source": source, "error_type": type(exc).__name__, "message": str(exc)}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _sorted_values(rows: list[dict[str, Any]], column: str) -> list[str]:
    values = {_text(row.get(column)) for row in rows}
    return sorted(value for value in values if value is not None)


def _unavailable_execution_health() -> ExecutionHealth:
    return {
        "status": "UNAVAILABLE",
        "pipeline_failed": None,
        "run_found": None,
        "result_state": None,
        "lifecycle_state": None,
        "run_id": None,
        "run_started_at": None,
        "run_ended_at": None,
        "unsuccessful_tasks": [],
    }


def _unavailable_data_health() -> DataHealth:
    return {
        "status": "UNAVAILABLE",
        "check_count": None,
        "issue_count": None,
        "warning_count": None,
        "failure_count": None,
        "monitoring_execution_timestamps": [],
        "monitoring_reference_timestamps": [],
        "data_reference_dates": {
            "latest_partition_dates": [],
            "activity_dates": [],
            "metric_dates": [],
        },
    }


def _evaluate_execution(
    evidence: dict[str, Any] | None,
) -> tuple[ExecutionHealth, list[InvestigationIssue]]:
    if evidence is None:
        return _unavailable_execution_health(), []

    found = bool(evidence.get("found"))
    result_state = _text(evidence.get("result_state"))
    lifecycle_state = _text(evidence.get("lifecycle_state"))
    normalized_result = (result_state or "").upper()
    normalized_lifecycle = (lifecycle_state or "").upper()
    unsuccessful_tasks = sorted(str(value) for value in evidence.get("unsuccessful_tasks") or [])

    if not found:
        status: Literal["PASS", "WARN", "FAIL", "RUNNING", "UNKNOWN", "UNAVAILABLE"] = "UNKNOWN"
    elif normalized_result == "SUCCESS" and not unsuccessful_tasks:
        status = "PASS"
    elif normalized_result or unsuccessful_tasks:
        status = "FAIL"
    elif normalized_lifecycle in {"BLOCKED", "PENDING", "QUEUED", "RUNNING", "TERMINATING"}:
        status = "RUNNING"
    else:
        status = "UNKNOWN"

    health: ExecutionHealth = {
        "status": status,
        "pipeline_failed": status == "FAIL" if found else None,
        "run_found": found,
        "result_state": result_state,
        "lifecycle_state": lifecycle_state,
        "run_id": evidence.get("run_id"),
        "run_started_at": _text(evidence.get("start_time")),
        "run_ended_at": _text(evidence.get("end_time")),
        "unsuccessful_tasks": unsuccessful_tasks,
    }

    issues: list[InvestigationIssue] = []
    failed_tasks = {
        str(task.get("task_key")): task
        for task in evidence.get("tasks") or []
        if str(task.get("result_state") or "").upper() not in {"", "SUCCESS"}
    }
    for task_name in unsuccessful_tasks:
        task = failed_tasks.get(task_name, {})
        issues.append(
            {
                "source": "jobs",
                "category": "execution",
                "status": str(task.get("result_state") or normalized_result or "FAIL"),
                "name": task_name,
                "details": task,
            }
        )
    if status == "FAIL" and not issues:
        issues.append(
            {
                "source": "jobs",
                "category": "execution",
                "status": normalized_result or "FAIL",
                "name": str(evidence.get("run_name") or evidence.get("run_id") or "pipeline"),
                "details": {
                    "message": evidence.get("message"),
                    "run_page_url": evidence.get("run_page_url"),
                },
            }
        )
    return health, issues


def _issue_name(category: str, row: dict[str, Any]) -> str:
    if category == "quality":
        values = (row.get("entity_name"), row.get("metric_name"))
        return ".".join(str(value) for value in values if value)
    return str(row.get("entity_name") or row.get("reconciliation_name") or category)


def _evaluate_data(
    evidence: dict[str, Any] | None,
) -> tuple[DataHealth, list[InvestigationIssue]]:
    if evidence is None:
        return _unavailable_data_health(), []

    issues: list[InvestigationIssue] = []
    check_count = 0
    warning_count = 0
    failure_count = 0

    for category, status_column in _OBSERVABILITY_CATEGORIES.items():
        category_result = evidence.get(category) or {}
        check_count += int(category_result.get("check_count") or 0)
        for row in category_result.get("issues") or []:
            status = str(row.get(status_column) or "UNKNOWN").upper()
            if status == "WARN":
                warning_count += 1
            else:
                failure_count += 1
            issues.append(
                {
                    "source": "observability",
                    "category": category,
                    "status": status,
                    "name": _issue_name(category, row),
                    "details": row,
                }
            )

    if failure_count:
        status: Literal["PASS", "WARN", "FAIL", "RUNNING", "UNKNOWN", "UNAVAILABLE"] = "FAIL"
    elif warning_count:
        status = "WARN"
    elif check_count:
        status = "PASS"
    else:
        status = "UNKNOWN"

    freshness_rows = (evidence.get("freshness") or {}).get("rows") or []
    row_count_rows = (evidence.get("row_counts") or {}).get("rows") or []
    quality_rows = (evidence.get("quality") or {}).get("rows") or []
    health: DataHealth = {
        "status": status,
        "check_count": check_count,
        "issue_count": len(issues),
        "warning_count": warning_count,
        "failure_count": failure_count,
        "monitoring_execution_timestamps": _sorted_values(freshness_rows, "monitoring_run_at"),
        "monitoring_reference_timestamps": _sorted_values(freshness_rows, "monitoring_reference_at"),
        "data_reference_dates": {
            "latest_partition_dates": _sorted_values(freshness_rows, "latest_partition_date"),
            "activity_dates": _sorted_values(row_count_rows, "activity_date"),
            "metric_dates": _sorted_values(quality_rows, "metric_date"),
        },
    }
    return health, issues


def _bounded(value: Any, limit: int = 96) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def _genie_question(
    issue: InvestigationIssue,
) -> tuple[str | None, str]:
    category = issue["category"]
    row = issue["details"]
    entity = _bounded(row.get("entity_name"))

    if issue["source"] != "observability":
        return None, "execution_issues_are_not_genie_data_investigations"
    if issue["status"] not in {"WARN", "FAIL"}:
        return None, "only_warn_and_fail_are_supported"
    if category == "reconciliation":
        return None, "reconciliation_is_a_current_snapshot_without_historical_dates"
    if category == "quality":
        metric = _bounded(row.get("metric_name"))
        metric_date = _bounded(row.get("metric_date"))
        if not all((entity, metric, metric_date)):
            return None, "quality_issue_lacks_entity_metric_or_metric_date"
        return (
            "Show the historical monitored values and statuses for quality metric "
            f"'{metric}' on entity '{entity}' around synthetic metric date '{metric_date}'. "
            "Return only retrieved evidence and keep monitoring timestamps separate from metric dates.",
            "supported_quality_metric_history",
        )
    if category == "row_counts":
        activity_date = _bounded(row.get("activity_date"))
        if not all((entity, activity_date)):
            return None, "row_count_issue_lacks_entity_or_activity_date"
        return (
            "Show the historical monitored row counts and volume statuses for entity "
            f"'{entity}' around synthetic activity date '{activity_date}'. "
            "Return only retrieved evidence and do not infer upstream source behavior.",
            "supported_row_count_history",
        )
    if category == "freshness":
        monitoring_run_at = _bounded(row.get("monitoring_run_at"))
        reference = _bounded(row.get("monitoring_reference_at") or row.get("latest_partition_date"))
        if not all((entity, monitoring_run_at, reference)):
            return None, "freshness_issue_lacks_monitoring_and_reference_structure"
        return (
            "Show the historical monitored freshness statuses for entity "
            f"'{entity}' around monitoring run '{monitoring_run_at}' and reference '{reference}'. "
            "Keep monitoring execution time separate from the monitored data reference.",
            "supported_freshness_history",
        )
    return None, "unsupported_anomaly_category"


def _select_genie_investigations(
    issues: list[InvestigationIssue],
) -> tuple[list[GenieInvestigationRequest], list[GenieSelectionDecision], str]:
    ordered = sorted(
        issues,
        key=lambda issue: (
            _STATUS_PRIORITY.get(issue["status"], 99),
            _CATEGORY_PRIORITY.get(issue["category"], 99),
            issue["name"],
        ),
    )
    candidates: list[tuple[InvestigationIssue, str, str]] = []
    decisions: list[GenieSelectionDecision] = []
    unsupported: list[tuple[InvestigationIssue, str]] = []

    for issue in ordered:
        question, reason = _genie_question(issue)
        if question is None:
            unsupported.append((issue, reason))
        else:
            candidates.append((issue, question, reason))

    requests: list[GenieInvestigationRequest] = []
    for index, (issue, question, reason) in enumerate(candidates):
        selected = index < MAX_GENIE_INVESTIGATIONS
        investigation_id = f"genie-{index + 1}" if selected else None
        decisions.append(
            {
                "issue_name": issue["name"],
                "category": issue["category"],
                "status": issue["status"],
                "selected": selected,
                "reason": reason if selected else "investigation_limit_reached",
                "investigation_id": investigation_id,
            }
        )
        if selected:
            requests.append(
                {
                    "investigation_id": investigation_id,
                    "issue_name": issue["name"],
                    "category": issue["category"],
                    "status": issue["status"],
                    "question": question,
                    "source_reference": issue["details"],
                }
            )

    decisions.extend(
        {
            "issue_name": issue["name"],
            "category": issue["category"],
            "status": issue["status"],
            "selected": False,
            "reason": reason,
            "investigation_id": None,
        }
        for issue, reason in unsupported
    )
    if requests:
        route_reason = f"selected_{len(requests)}_supported_anomalies"
    elif issues:
        route_reason = "no_supported_anomalies"
    else:
        route_reason = "no_anomalies"
    return requests, decisions, route_reason


def route_after_evaluation(
    state: InvestigationState,
) -> Literal["investigate_with_genie", "construct_report"]:
    if state.get("genie_requests"):
        return "investigate_with_genie"
    return "construct_report"


def _genie_finding(
    request: GenieInvestigationRequest,
    result: dict[str, Any],
) -> tuple[GenieFinding, InvestigationError | None]:
    query_issues = int(result.get("query_result_issue_count") or 0)
    truncated_results = sum(
        bool((query.get("result") or {}).get("truncated"))
        for query in result.get("query_evidence") or []
    )
    if result.get("ok") and query_issues == 0 and truncated_results == 0:
        status: Literal["complete", "partial", "unavailable"] = "complete"
        error = None
    elif result.get("ok"):
        status = "partial"
        error = {
            "source": "genie",
            "error_type": "partial_query_results",
            "message": (
                f"{query_issues} Genie query result set(s) were unavailable; "
                f"{truncated_results} were truncated by the bounded retrieval limit."
            ),
        }
    else:
        status = "unavailable"
        detail = result.get("error") or {}
        error = {
            "source": "genie",
            "error_type": str(detail.get("type") or "genie_failure"),
            "message": str(detail.get("message") or "Genie evidence was unavailable."),
        }

    finding: GenieFinding = {
        "investigation_id": request["investigation_id"],
        "question": request["question"],
        "status": status,
        "answer": result.get("answer"),
        "evidence": result,
        "error": None if error is None else {"type": error["error_type"], "message": error["message"]},
        "root_cause": dict(_UNKNOWN_ROOT_CAUSE),
        "impact": dict(_UNKNOWN_IMPACT),
    }
    return finding, error


def _genie_source_status(
    requests: list[GenieInvestigationRequest],
    findings: list[GenieFinding],
) -> dict[str, Any]:
    if not requests:
        return {"available": False, "status": "not_requested"}
    complete_count = sum(finding["status"] == "complete" for finding in findings)
    available_count = sum(finding["status"] != "unavailable" for finding in findings)
    if complete_count == len(requests):
        return {"available": True, "status": "complete"}
    if available_count:
        return {"available": True, "status": "partial"}
    return {"available": False, "status": "unavailable"}


def _source_references(state: InvestigationState) -> dict[str, Any]:
    jobs = state.get("jobs_evidence") or {}
    data_health = state.get("data_health") or _unavailable_data_health()
    genie = []
    for finding in state.get("genie_findings", []):
        evidence = finding.get("evidence") or {}
        genie.append(
            {
                "investigation_id": finding["investigation_id"],
                "metadata": evidence.get("metadata") or {},
                "queries": [
                    {
                        "attachment_id": query.get("attachment_id"),
                        "statement_id": query.get("statement_id"),
                        "result_available": (query.get("result") or {}).get("available"),
                        "truncated": (query.get("result") or {}).get("truncated"),
                    }
                    for query in evidence.get("query_evidence") or []
                ],
            }
        )
    return {
        "jobs": {
            "job_id": jobs.get("job_id"),
            "run_id": jobs.get("run_id"),
            "run_page_url": jobs.get("run_page_url"),
        },
        "observability": {
            "monitoring_execution_timestamps": data_health["monitoring_execution_timestamps"],
            "monitoring_reference_timestamps": data_health["monitoring_reference_timestamps"],
            "synthetic_activity_dates": data_health["data_reference_dates"],
        },
        "genie": genie,
    }


def build_investigation_graph(dependencies: InvestigationDependencies | None = None):
    """Compile the standalone read-only investigation graph."""

    deps = dependencies or default_dependencies()

    async def collect_jobs(_: InvestigationState) -> InvestigationState:
        try:
            evidence = await asyncio.to_thread(deps.fetch_jobs)
            if not isinstance(evidence, dict):
                raise TypeError("Jobs evidence must be a dictionary")
            return {"jobs_evidence": evidence, "jobs_error": None}
        except Exception as exc:  # noqa: BLE001 - partial reporting is the workflow contract
            return {"jobs_evidence": None, "jobs_error": _error("jobs", exc)}

    async def collect_observability(_: InvestigationState) -> InvestigationState:
        try:
            evidence = await asyncio.to_thread(deps.fetch_observability)
            if not isinstance(evidence, dict):
                raise TypeError("Observability evidence must be a dictionary")
            return {"observability_evidence": evidence, "observability_error": None}
        except Exception as exc:  # noqa: BLE001 - partial reporting is the workflow contract
            return {
                "observability_evidence": None,
                "observability_error": _error("observability", exc),
            }

    def join_evidence(state: InvestigationState) -> InvestigationState:
        errors = [
            error
            for error in (state.get("jobs_error"), state.get("observability_error"))
            if error is not None
        ]
        return {
            "errors": errors,
            "workflow_path": ["collect_jobs", "collect_observability", "join_evidence"],
        }

    def evaluate_issues(state: InvestigationState) -> InvestigationState:
        execution_health, execution_issues = _evaluate_execution(state.get("jobs_evidence"))
        data_health, data_issues = _evaluate_data(state.get("observability_evidence"))
        issues = [*execution_issues, *data_issues]
        requests, selection, route_reason = _select_genie_investigations(issues)
        return {
            "execution_health": execution_health,
            "data_health": data_health,
            "issues": issues,
            "genie_requests": requests,
            "genie_selection": selection,
            "genie_findings": [],
            "genie_errors": [],
            "genie_route_reason": route_reason,
            "evaluation_status": "completed",
            "workflow_path": [*state["workflow_path"], "evaluate_issues"],
        }

    async def investigate_with_genie(state: InvestigationState) -> InvestigationState:
        findings: list[GenieFinding] = []
        errors: list[InvestigationError] = []
        for request in state.get("genie_requests", [])[:MAX_GENIE_INVESTIGATIONS]:
            try:
                if deps.fetch_genie is None:
                    raise RuntimeError("Genie investigation dependency is not configured")
                result = await asyncio.wait_for(
                    deps.fetch_genie(request["question"]),
                    timeout=deps.genie_timeout_seconds,
                )
                if not isinstance(result, dict):
                    raise TypeError("Genie evidence must be a dictionary")
                finding, error = _genie_finding(request, result)
            except asyncio.TimeoutError:
                error = {
                    "source": "genie",
                    "error_type": "timeout",
                    "message": "Genie investigation exceeded the bounded workflow timeout.",
                }
                finding = {
                    "investigation_id": request["investigation_id"],
                    "question": request["question"],
                    "status": "unavailable",
                    "answer": None,
                    "evidence": None,
                    "error": {"type": error["error_type"], "message": error["message"]},
                    "root_cause": dict(_UNKNOWN_ROOT_CAUSE),
                    "impact": dict(_UNKNOWN_IMPACT),
                }
            except Exception as exc:  # noqa: BLE001 - partial reporting is the workflow contract
                error = _error("genie", exc)
                finding = {
                    "investigation_id": request["investigation_id"],
                    "question": request["question"],
                    "status": "unavailable",
                    "answer": None,
                    "evidence": None,
                    "error": {"type": error["error_type"], "message": error["message"]},
                    "root_cause": dict(_UNKNOWN_ROOT_CAUSE),
                    "impact": dict(_UNKNOWN_IMPACT),
                }
            findings.append(finding)
            if error is not None:
                errors.append(error)
        return {
            "genie_findings": findings,
            "genie_errors": errors,
            "workflow_path": [*state["workflow_path"], "investigate_with_genie"],
        }

    def construct_report(state: InvestigationState) -> InvestigationState:
        jobs_available = state.get("jobs_evidence") is not None
        observability_available = state.get("observability_evidence") is not None
        requests = state.get("genie_requests", [])
        findings = state.get("genie_findings", [])
        genie_status = _genie_source_status(requests, findings)
        errors = [*state.get("errors", []), *state.get("genie_errors", [])]

        if not jobs_available and not observability_available:
            completeness: Literal["complete", "partial", "unavailable"] = "unavailable"
        elif not jobs_available or not observability_available:
            completeness = "partial"
        elif requests and any(finding["status"] != "complete" for finding in findings):
            completeness = "partial"
        else:
            completeness = "complete"

        execution_health = state.get("execution_health") or _unavailable_execution_health()
        data_health = state.get("data_health") or _unavailable_data_health()
        path = [*state.get("workflow_path", []), "construct_report"]
        report = InvestigationReportV1(
            generated_at=deps.clock(),
            source_availability={
                "jobs": {
                    "available": jobs_available,
                    "status": execution_health["status"],
                },
                "observability": {
                    "available": observability_available,
                    "status": data_health["status"],
                },
                "genie": genie_status,
            },
            pipeline_execution_health=execution_health,
            monitoring_health={
                "status": data_health["status"],
                "check_count": data_health["check_count"],
                "issue_count": data_health["issue_count"],
                "warning_count": data_health["warning_count"],
                "failure_count": data_health["failure_count"],
                "monitoring_execution_timestamps": data_health[
                    "monitoring_execution_timestamps"
                ],
                "monitoring_reference_timestamps": data_health[
                    "monitoring_reference_timestamps"
                ],
                "synthetic_activity_dates": data_health["data_reference_dates"],
            },
            detected_issues=state.get("issues", []),
            genie_findings=findings,
            source_errors=errors,
            investigation_completeness=completeness,
            genie_routing={
                "reason": state.get("genie_route_reason", "not_evaluated"),
                "maximum_investigations": MAX_GENIE_INVESTIGATIONS,
                "selected_count": len(requests),
                "decisions": state.get("genie_selection", []),
            },
            evidence={
                "jobs": state.get("jobs_evidence"),
                "observability": state.get("observability_evidence"),
                "genie": findings,
            },
            source_references=_source_references(state),
            workflow={"path": path},
        ).model_dump(mode="json")
        return {"errors": errors, "workflow_path": path, "report": report}

    builder = StateGraph(InvestigationState)
    builder.add_node("collect_jobs", collect_jobs)
    builder.add_node("collect_observability", collect_observability)
    builder.add_node("join_evidence", join_evidence)
    builder.add_node("evaluate_issues", evaluate_issues)
    builder.add_node("investigate_with_genie", investigate_with_genie)
    builder.add_node("construct_report", construct_report)
    builder.add_edge(START, "collect_jobs")
    builder.add_edge(START, "collect_observability")
    builder.add_edge(["collect_jobs", "collect_observability"], "join_evidence")
    builder.add_edge("join_evidence", "evaluate_issues")
    builder.add_conditional_edges(
        "evaluate_issues",
        route_after_evaluation,
        {
            "investigate_with_genie": "investigate_with_genie",
            "construct_report": "construct_report",
        },
    )
    builder.add_edge("investigate_with_genie", "construct_report")
    builder.add_edge("construct_report", END)
    return builder.compile()


async def run_investigation(
    dependencies: InvestigationDependencies | None = None,
) -> dict[str, Any]:
    """Run the standalone workflow and return its validated versioned report."""

    graph = build_investigation_graph(dependencies)
    state = await graph.ainvoke({})
    return state["report"]
