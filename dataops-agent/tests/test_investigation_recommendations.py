from __future__ import annotations

from copy import deepcopy

import pytest

from agent.investigation.recommendations import build_recommendations
from agent.investigation.report import InvestigationReportV1, InvestigationReportV11


def issue(category: str, status: str = "WARN") -> dict:
    details_by_category = {
        "freshness": {
            "entity_name": "messages",
            "latest_event_at": "2026-08-31T21:00:00Z",
            "latest_ingested_at": "2026-08-31T21:01:00Z",
            "latest_partition_date": "2026-08-31",
            "monitoring_reference_at": "2026-08-31T23:59:59Z",
            "monitoring_run_at": "2026-10-27T09:15:00Z",
            "freshness_delay_hours": "26.000000",
            "freshness_warning_threshold_hours": 24.0,
            "freshness_status": status,
        },
        "quality": {
            "entity_name": "messages",
            "metric_name": "missing_message_text_rate",
            "metric_date": "2026-08-31",
            "numerator": 12,
            "denominator": 1000,
            "metric_rate": 0.012,
            "previous_rate": 0.002,
            "rolling_7_day_rate": 0.003,
            "quality_warning_delta": 0.005,
            "quality_status": status,
        },
        "row_counts": {
            "entity_name": "messages",
            "activity_date": "2026-08-31",
            "row_count": 800,
            "previous_day_row_count": 1000,
            "rolling_7_day_average": 980.0,
            "previous_day_change_pct": -0.2,
            "rolling_7_day_change_pct": -0.1837,
            "volume_status": status,
        },
        "reconciliation": {
            "reconciliation_name": "staging_to_fact_message",
            "source_layer": "staging",
            "target_layer": "silver",
            "source_count": 1000,
            "target_count": 995,
            "difference": 5,
            "difference_pct": 0.005,
            "status": status,
        },
        "execution": {
            "task_key": "dbt_observability",
            "result_state": status,
            "message": "reported task failure",
            "run_page_url": "https://example/run/456",
        },
    }
    names = {
        "freshness": "messages",
        "quality": "messages.missing_message_text_rate",
        "row_counts": "messages",
        "reconciliation": "staging_to_fact_message",
        "execution": "dbt_observability",
    }
    return {
        "source": "jobs" if category == "execution" else "observability",
        "category": category,
        "status": status,
        "name": names[category],
        "details": details_by_category[category],
    }


def state(
    issues: list[dict] | None = None,
    *,
    jobs_available: bool = True,
    observability_available: bool = True,
    genie_finding: dict | None = None,
) -> dict:
    selected_issue = (issues or [None])[0]
    investigation_id = "genie-1"
    return {
        "jobs_evidence": {"run_id": 456, "result_state": "SUCCESS"}
        if jobs_available
        else None,
        "observability_evidence": {"total_issue_count": len(issues or [])}
        if observability_available
        else None,
        "execution_health": {
            "run_id": 456,
            "status": "FAIL" if any(item["category"] == "execution" for item in issues or []) else "PASS",
        },
        "issues": issues or [],
        "genie_selection": [
            {
                "issue_name": selected_issue["name"],
                "category": selected_issue["category"],
                "status": selected_issue["status"],
                "selected": True,
                "reason": "supported_history",
                "investigation_id": investigation_id,
            }
        ]
        if selected_issue and genie_finding
        else [],
        "genie_findings": [genie_finding] if genie_finding else [],
    }


def recommendations_for(category: str, status: str = "WARN"):
    recommendations, summary = build_recommendations(state([issue(category, status)]))
    return recommendations, summary


@pytest.mark.parametrize(
    ("category", "expected_text"),
    [
        ("freshness", "expected synthetic event cadence"),
        ("quality", "rolling seven-day rate"),
        ("row_counts", "compare bronze and staging counts"),
        ("reconciliation", "reported counts and difference"),
        ("execution", "inspect lakeflow run"),
    ],
)
def test_supported_anomaly_rules_generate_diagnostic_remediation_and_verification(
    category,
    expected_text,
):
    status = "FAIL" if category in {"reconciliation", "execution"} else "WARN"
    recommendations, summary = recommendations_for(category, status)

    assert summary == {
        "status": "generated",
        "recommendation_count": 3,
        "unsupported_issue_count": 0,
        "message": (
            "Deterministic recommendations were generated from retrieved evidence. Proposed "
            "production corrections require human approval and are not executed by this workflow."
        ),
    }
    assert [item["category"] for item in recommendations] == [
        "diagnostic",
        "proposed_remediation",
        "verification",
    ]
    assert expected_text in recommendations[0]["action"].lower()
    assert recommendations[0]["requires_human_approval"] is False
    assert recommendations[1]["requires_human_approval"] is True
    assert recommendations[2]["requires_human_approval"] is False
    assert all(item["evidence_refs"] for item in recommendations)
    assert all(item["verification_steps"] for item in recommendations)


def test_multiple_issues_have_deterministic_priority_ordering():
    recommendations, summary = build_recommendations(
        state(
            [
                issue("quality", "WARN"),
                issue("execution", "FAIL"),
                issue("row_counts", "WARN"),
                issue("reconciliation", "FAIL"),
            ]
        )
    )

    rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    assert [rank[item["priority"]] for item in recommendations] == sorted(
        rank[item["priority"]] for item in recommendations
    )
    assert [item["category"] for item in recommendations[:2]] == [
        "diagnostic",
        "diagnostic",
    ]
    assert all(
        item["category"] == "diagnostic"
        for item in recommendations
        if item["priority"] in {"P0", "P1"}
    )
    assert all(
        item["requires_human_approval"]
        for item in recommendations
        if item["category"] == "proposed_remediation"
    )
    assert summary["recommendation_count"] == 12


def test_no_findings_states_no_recommendation_without_claiming_platform_health():
    recommendations, summary = build_recommendations(state([]))

    assert recommendations == []
    assert summary["status"] == "no_findings"
    assert summary["recommendation_count"] == 0
    assert "does not prove overall platform health" in summary["message"]


def test_missing_observability_evidence_is_not_healthy():
    recommendations, summary = build_recommendations(
        state([], observability_available=False)
    )

    assert summary["status"] == "generated"
    assert recommendations[0]["issue_id"] == "source.observability.unavailable"
    assert recommendations[0]["priority"] == "P1"
    assert "must not be treated as PASS or zero issues" in recommendations[0]["rationale"]
    assert "ai_analytics.observability" in recommendations[0]["action"]


def genie_finding(status: str, *, truncated: bool = False) -> dict:
    result = {
        "available": status != "unavailable",
        "rows": [["2026-08-31", "WARN"]] if status != "unavailable" else None,
        "total_row_count": 150 if truncated else 1,
        "truncated": truncated,
        "error": None if status != "unavailable" else {"type": "timeout"},
    }
    return {
        "investigation_id": "genie-1",
        "question": "Show bounded history.",
        "status": status,
        "answer": "Retrieved evidence." if status != "unavailable" else None,
        "evidence": {
            "ok": status != "unavailable",
            "query_evidence": [{"result": result}],
        }
        if status != "unavailable"
        else None,
        "error": None if status == "complete" else {"type": "partial_or_unavailable"},
        "root_cause": {"status": "unknown", "explanation": "Unknown."},
        "impact": {"status": "unknown", "explanation": "Unknown."},
    }


def test_complete_genie_evidence_is_referenced_without_extra_claims():
    quality_issue = issue("quality")
    recommendations, _ = build_recommendations(
        state([quality_issue], genie_finding=genie_finding("complete"))
    )

    assert len(recommendations) == 3
    assert "genie_findings[0]" in recommendations[0]["evidence_refs"]
    assert all("root cause is" not in item["rationale"].lower() for item in recommendations)


@pytest.mark.parametrize("status", ["partial", "unavailable"])
def test_incomplete_genie_evidence_adds_diagnostic_guidance(status):
    recommendations, summary = build_recommendations(
        state([issue("quality")], genie_finding=genie_finding(status))
    )

    assert summary["recommendation_count"] == 4
    genie_guidance = next(item for item in recommendations if item["issue_id"].startswith("genie."))
    assert genie_guidance["category"] == "diagnostic"
    assert "unavailable or partial" in genie_guidance["action"]
    assert genie_guidance["requires_human_approval"] is False


def test_truncated_genie_results_never_infer_unreturned_rows():
    recommendations, _ = build_recommendations(
        state(
            [issue("row_counts")],
            genie_finding=genie_finding("partial", truncated=True),
        )
    )

    genie_guidance = next(item for item in recommendations if item["issue_id"].startswith("genie."))
    assert "do not infer values for unreturned rows" in genie_guidance["action"]
    assert "narrower bounded historical question" in genie_guidance["action"]


def test_unsupported_issue_can_state_no_supported_recommendation():
    unsupported = {
        "source": "observability",
        "category": "unknown_category",
        "status": "WARN",
        "name": "unknown",
        "details": {},
    }
    recommendations, summary = build_recommendations(state([unsupported]))

    assert recommendations == []
    assert summary["status"] == "no_supported_recommendation"
    assert summary["unsupported_issue_count"] == 1
    assert "No corrective action is proposed" in summary["message"]


def test_recommendations_do_not_claim_unsupported_systems_or_execution_abilities():
    recommendations, _ = build_recommendations(
        state(
            [
                issue("freshness"),
                issue("quality"),
                issue("row_counts"),
                issue("reconciliation", "FAIL"),
                issue("execution", "FAIL"),
            ]
        )
    )
    text = " ".join(
        part
        for item in recommendations
        for part in (
            item["action"],
            item["rationale"],
            *item["verification_steps"],
        )
    ).lower()

    for unsupported in (
        "payment gateway",
        "merchant",
        "webhook",
        "cdc",
        "authentication failure",
        "execute sql",
        "i repaired",
        "i reran",
        "i backfilled",
    ):
        assert unsupported not in text
    assert all(
        item["requires_human_approval"]
        for item in recommendations
        if item["category"] == "proposed_remediation"
    )


def test_v11_is_additive_and_v1_schema_remains_unchanged():
    assert "recommendations" not in InvestigationReportV1.model_fields
    assert "recommendation_summary" not in InvestigationReportV1.model_fields
    assert InvestigationReportV1.model_fields["schema_version"].default == "1.0"
    assert InvestigationReportV11.model_fields["schema_version"].default == "1.1"
    assert {
        "recommendations",
        "recommendation_summary",
    } <= set(InvestigationReportV11.model_fields)
