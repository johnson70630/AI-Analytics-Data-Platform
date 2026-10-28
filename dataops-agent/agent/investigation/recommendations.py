"""Deterministic, evidence-grounded recommendation rules for investigation reports."""

from __future__ import annotations

import re
from typing import Any

from agent.investigation.state import (
    InvestigationIssue,
    InvestigationState,
    RecommendationCategory,
    RecommendationPriority,
    RecommendationStateItem,
    RecommendationSummaryState,
)


_SUPPORTED_CATEGORIES = {"execution", "freshness", "quality", "row_counts", "reconciliation"}
_PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
_CATEGORY_ORDER = {"diagnostic": 0, "proposed_remediation": 1, "verification": 2}


def _bounded(value: Any, default: str, limit: int = 96) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit] or default


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized[:64] or "unnamed"


def _issue_id(issue: InvestigationIssue, occurrence: int) -> str:
    base = f"{issue['source']}.{issue['category']}.{_slug(issue['name'])}"
    return base if occurrence == 1 else f"{base}.{occurrence}"


def _genie_ref(state: InvestigationState, issue: InvestigationIssue) -> str | None:
    selected = next(
        (
            decision
            for decision in state.get("genie_selection", [])
            if decision.get("selected")
            and decision.get("issue_name") == issue["name"]
            and decision.get("category") == issue["category"]
            and decision.get("status") == issue["status"]
        ),
        None,
    )
    investigation_id = selected.get("investigation_id") if selected else None
    for index, finding in enumerate(state.get("genie_findings", [])):
        if finding.get("investigation_id") == investigation_id:
            return f"genie_findings[{index}]"
    return None


def _evidence_refs(
    state: InvestigationState,
    issue: InvestigationIssue,
    issue_index: int,
) -> list[str]:
    refs = [f"detected_issues[{issue_index}]"]
    if issue["source"] == "jobs":
        refs.append("evidence.jobs")
    else:
        refs.append(f"evidence.observability.{issue['category']}")
    genie_ref = _genie_ref(state, issue)
    if genie_ref:
        refs.append(genie_ref)
    return refs


def _recommendation(
    *,
    issue_id: str,
    priority: RecommendationPriority,
    category: RecommendationCategory,
    action: str,
    rationale: str,
    evidence_refs: list[str],
    requires_human_approval: bool,
    verification_steps: list[str],
) -> RecommendationStateItem:
    return {
        "issue_id": issue_id,
        "priority": priority,
        "category": category,
        "action": action,
        "rationale": rationale,
        "evidence_refs": evidence_refs,
        "requires_human_approval": requires_human_approval,
        "verification_steps": verification_steps,
    }


def _issue_recommendations(
    state: InvestigationState,
    issue: InvestigationIssue,
    issue_index: int,
    issue_id: str,
) -> list[RecommendationStateItem]:
    details = issue["details"]
    status = _bounded(issue["status"], "UNKNOWN")
    name = _bounded(issue["name"], "unnamed issue")
    refs = _evidence_refs(state, issue, issue_index)
    diagnostic_priority = (
        "P0"
        if issue["category"] == "execution"
        or status in {"FAIL", "FAILED", "ERROR", "TIMED_OUT", "CANCELED", "CANCELLED"}
        else "P1"
    )

    if issue["category"] == "execution":
        run_id = _bounded(
            (state.get("execution_health") or {}).get("run_id"),
            "the reported run",
        )
        diagnostic_action = (
            f"Inspect Lakeflow run '{run_id}' and the reported task '{name}' using the returned "
            "run, task status, and message metadata; determine the failure condition before "
            "proposing an operator action."
        )
        rationale = f"The retrieved Jobs evidence reports task '{name}' with status '{status}'."
        remediation_action = (
            f"Only after the failure condition for task '{name}' is confirmed, prepare an "
            "operator-reviewed recovery or code/configuration change scoped to that condition."
        )
        verification_action = (
            f"After an authorized operator action, inspect a subsequent Lakeflow run and confirm "
            f"task '{name}' and the overall run report SUCCESS."
        )
        verification_steps = [
            "Compare the subsequent task result with the failed task evidence in this report.",
            "Confirm the subsequent overall Lakeflow result state is SUCCESS.",
        ]
    elif issue["category"] == "freshness":
        entity = _bounded(details.get("entity_name"), name)
        reference = _bounded(
            details.get("monitoring_reference_at") or details.get("latest_partition_date"),
            "the reported reference",
        )
        delay = _bounded(details.get("freshness_delay_hours"), "the reported delay")
        threshold = _bounded(
            details.get("freshness_warning_threshold_hours"),
            "the configured threshold",
        )
        diagnostic_action = (
            f"Validate the expected synthetic event cadence for entity '{entity}' at reference "
            f"'{reference}', compare its latest event, ingestion, and partition timestamps across "
            "Bronze and staging, and review the freshness calculation semantics."
        )
        rationale = (
            f"The freshness check for '{entity}' returned '{status}' with {delay} against "
            f"{threshold}; this evidence does not identify the cause."
        )
        remediation_action = (
            f"If diagnosis confirms a cadence, ingestion, or transformation defect for '{entity}', "
            "prepare a reviewed correction to the responsible synthetic generator, Bronze "
            "ingestion, or staging configuration."
        )
        verification_action = (
            f"After an approved change, recompute freshness for '{entity}' and confirm the latest "
            "event, ingestion, and partition timestamps are within the configured threshold."
        )
        verification_steps = [
            "Keep monitoring_run_at separate from monitoring_reference_at during comparison.",
            "Confirm the recomputed freshness_status is PASS for the same entity and reference.",
        ]
    elif issue["category"] == "quality":
        entity = _bounded(details.get("entity_name"), "the reported entity")
        metric = _bounded(details.get("metric_name"), name)
        metric_date = _bounded(details.get("metric_date"), "the reported metric date")
        diagnostic_action = (
            f"Compare numerator, denominator, metric rate, previous rate, rolling seven-day rate, "
            f"and warning delta for metric '{metric}' on '{entity}' at synthetic metric date "
            f"'{metric_date}', then inspect the relevant synthetic generator rule and staging "
            "representation."
        )
        rationale = (
            f"The latest quality check reports '{status}' for '{entity}.{metric}'; current and "
            "historical evidence must be compared before proposing a cause."
        )
        remediation_action = (
            f"If the comparison confirms a generator or transformation defect affecting "
            f"'{entity}.{metric}', prepare a reviewed correction to that confirmed component."
        )
        verification_action = (
            f"After an approved change, recompute '{metric}' for the affected synthetic date and "
            "compare it with its previous and rolling seven-day baselines."
        )
        verification_steps = [
            "Confirm numerator and denominator reconcile with the affected monitored records.",
            "Confirm the recomputed quality_status is PASS without suppressing affected records.",
        ]
    elif issue["category"] == "row_counts":
        entity = _bounded(details.get("entity_name"), name)
        activity_date = _bounded(details.get("activity_date"), "the reported activity date")
        diagnostic_action = (
            f"Compare row count, previous-day count, rolling seven-day average, and both change "
            f"percentages for entity '{entity}' at synthetic activity date '{activity_date}'; "
            "compare Bronze and staging counts and inspect the applicable synthetic generator rule."
        )
        rationale = (
            f"The volume check for '{entity}' returned '{status}'; the observed volume difference "
            "does not by itself establish missing or duplicated records."
        )
        remediation_action = (
            f"If count comparison confirms a generator, ingestion, or transformation defect for "
            f"'{entity}', prepare a reviewed correction to the confirmed component."
        )
        verification_action = (
            f"After an approved change, reconcile '{entity}' counts across Bronze and staging for "
            f"'{activity_date}' and recompute the volume check."
        )
        verification_steps = [
            "Confirm physical duplicates remain represented according to the established layer semantics.",
            "Confirm the recomputed volume_status and cross-layer counts agree.",
        ]
    else:  # reconciliation
        source_layer = _bounded(details.get("source_layer"), "the reported source layer")
        target_layer = _bounded(details.get("target_layer"), "the reported target layer")
        diagnostic_action = (
            f"Compare the reported counts and difference for reconciliation '{name}' between "
            f"'{source_layer}' and '{target_layer}', then inspect the established transformation "
            "between those layers at the same snapshot."
        )
        rationale = (
            f"The current reconciliation snapshot reports '{status}' for '{name}'; it does not "
            "contain historical dates or establish which transformation caused the difference."
        )
        remediation_action = (
            f"If the comparison confirms a counting or transformation defect for '{name}', prepare "
            "a reviewed correction to that confirmed transformation."
        )
        verification_action = (
            f"After an approved change, recompute reconciliation '{name}' and confirm its source "
            "count, target count, difference, and status meet the established expectation."
        )
        verification_steps = [
            "Use the same reconciliation snapshot and grain for source and target counts.",
            "Confirm the recomputed reconciliation status is PASS.",
        ]

    return [
        _recommendation(
            issue_id=issue_id,
            priority=diagnostic_priority,
            category="diagnostic",
            action=diagnostic_action,
            rationale=rationale,
            evidence_refs=refs,
            requires_human_approval=False,
            verification_steps=[
                "Record the observed comparison and preserve links to the referenced report evidence.",
                "Do not assign root cause or impact unless the comparison supports it.",
            ],
        ),
        _recommendation(
            issue_id=issue_id,
            priority="P2",
            category="proposed_remediation",
            action=remediation_action,
            rationale="A production correction is appropriate only after the diagnostic confirms a specific defect.",
            evidence_refs=refs,
            requires_human_approval=True,
            verification_steps=verification_steps,
        ),
        _recommendation(
            issue_id=issue_id,
            priority="P3",
            category="verification",
            action=verification_action,
            rationale="The affected check must be re-evaluated after any approved external change.",
            evidence_refs=refs,
            requires_human_approval=False,
            verification_steps=verification_steps,
        ),
    ]


def _unavailable_source_recommendations(
    state: InvestigationState,
) -> list[RecommendationStateItem]:
    recommendations: list[RecommendationStateItem] = []
    if state.get("jobs_evidence") is None:
        recommendations.append(
            _recommendation(
                issue_id="source.jobs.unavailable",
                priority="P1",
                category="diagnostic",
                action=(
                    "Confirm the configured Databricks authentication and pipeline job identifier, "
                    "then retry the read-only Jobs evidence collection."
                ),
                rationale="Jobs evidence is unavailable and must not be interpreted as a successful pipeline run.",
                evidence_refs=["source_availability.jobs", "source_errors"],
                requires_human_approval=False,
                verification_steps=["Confirm Jobs evidence returns a concrete run_found and execution status."],
            )
        )
    if state.get("observability_evidence") is None:
        recommendations.append(
            _recommendation(
                issue_id="source.observability.unavailable",
                priority="P1",
                category="diagnostic",
                action=(
                    "Confirm read access through the configured SQL warehouse to the established "
                    "ai_analytics.observability checks, then retry the fixed read-only collection."
                ),
                rationale="Monitoring evidence is unavailable and must not be treated as PASS or zero issues.",
                evidence_refs=["source_availability.observability", "source_errors"],
                requires_human_approval=False,
                verification_steps=[
                    "Confirm freshness, row-count, quality, and reconciliation check counts are returned."
                ],
            )
        )
    return recommendations


def _incomplete_genie_recommendations(
    state: InvestigationState,
) -> list[RecommendationStateItem]:
    recommendations: list[RecommendationStateItem] = []
    decisions = {
        decision.get("investigation_id"): decision
        for decision in state.get("genie_selection", [])
        if decision.get("investigation_id")
    }
    for index, finding in enumerate(state.get("genie_findings", [])):
        if finding.get("status") == "complete":
            continue
        investigation_id = str(finding.get("investigation_id") or f"genie-{index + 1}")
        decision = decisions.get(investigation_id) or {}
        issue_id = "genie." + _slug(str(decision.get("issue_name") or investigation_id))
        evidence = finding.get("evidence") or {}
        truncated = any(
            bool((query.get("result") or {}).get("truncated"))
            for query in evidence.get("query_evidence") or []
        )
        if truncated:
            action = (
                "Treat the retrieved Genie rows as partial evidence and do not infer values for "
                "unreturned rows; use the result metadata to formulate a narrower bounded historical question."
            )
            rationale = "At least one bounded Genie result was truncated."
        else:
            action = (
                "Treat historical Genie context as unavailable or partial; review the returned "
                "error metadata and retry the same bounded read-only question only after access or timeout is resolved."
            )
            rationale = "The selected Genie investigation did not return complete historical evidence."
        recommendations.append(
            _recommendation(
                issue_id=issue_id,
                priority="P1",
                category="diagnostic",
                action=action,
                rationale=rationale,
                evidence_refs=[f"genie_findings[{index}]", "source_errors"],
                requires_human_approval=False,
                verification_steps=[
                    "Confirm result availability, row count, and truncation metadata before using retrieved values."
                ],
            )
        )
    return recommendations


def build_recommendations(
    state: InvestigationState,
) -> tuple[list[RecommendationStateItem], RecommendationSummaryState]:
    """Generate ordered recommendations without executing any corrective action."""

    recommendations = _unavailable_source_recommendations(state)
    unsupported_issue_count = 0
    occurrences: dict[str, int] = {}
    for issue_index, issue in enumerate(state.get("issues", [])):
        if issue["category"] not in _SUPPORTED_CATEGORIES:
            unsupported_issue_count += 1
            continue
        key = f"{issue['source']}:{issue['category']}:{issue['name']}"
        occurrences[key] = occurrences.get(key, 0) + 1
        issue_id = _issue_id(issue, occurrences[key])
        recommendations.extend(
            _issue_recommendations(state, issue, issue_index, issue_id)
        )
    recommendations.extend(_incomplete_genie_recommendations(state))
    recommendations.sort(
        key=lambda item: (
            _PRIORITY_ORDER[item["priority"]],
            _CATEGORY_ORDER[item["category"]],
            item["issue_id"],
            item["action"],
        )
    )

    if recommendations:
        status = "generated"
        message = (
            "Deterministic recommendations were generated from retrieved evidence. Proposed "
            "production corrections require human approval and are not executed by this workflow."
        )
    elif not state.get("issues") and state.get("jobs_evidence") is not None and state.get(
        "observability_evidence"
    ) is not None:
        status = "no_findings"
        message = (
            "No non-PASS condition was found in the retrieved execution and latest monitoring "
            "evidence, so no recommendation was generated; this does not prove overall platform health."
        )
    else:
        status = "no_supported_recommendation"
        message = (
            "The retrieved evidence contains no anomaly category supported by the deterministic "
            "recommendation rules. No corrective action is proposed."
        )

    summary: RecommendationSummaryState = {
        "status": status,
        "recommendation_count": len(recommendations),
        "unsupported_issue_count": unsupported_issue_count,
        "message": message,
    }
    return recommendations, summary
