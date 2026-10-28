from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from agent.investigation import InvestigationReportV1, InvestigationReportV11
from agent.tools import all_tools
from agent.tools.investigation import (
    fetch_dataops_investigation_report,
    generate_dataops_investigation_report,
)


def complete_report() -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": datetime(2026, 10, 26, 16, 30, tzinfo=timezone.utc),
        "source_availability": {
            "jobs": {"available": True, "status": "PASS"},
            "observability": {"available": True, "status": "PASS"},
            "genie": {"available": False, "status": "not_requested"},
        },
        "pipeline_execution_health": {
            "status": "PASS",
            "pipeline_failed": False,
            "run_found": True,
            "result_state": "SUCCESS",
            "lifecycle_state": "TERMINATED",
            "run_id": 456,
            "run_started_at": "2026-10-26T08:00:00Z",
            "run_ended_at": "2026-10-26T08:20:00Z",
            "unsuccessful_tasks": [],
        },
        "monitoring_health": {
            "status": "PASS",
            "check_count": 14,
            "issue_count": 0,
            "warning_count": 0,
            "failure_count": 0,
            "monitoring_execution_timestamps": ["2026-10-26T08:18:00Z"],
            "monitoring_reference_timestamps": ["2026-10-26T00:00:00Z"],
            "synthetic_activity_dates": {
                "latest_partition_dates": ["2026-10-25"],
                "activity_dates": ["2026-10-25"],
                "metric_dates": ["2026-10-25"],
            },
        },
        "detected_issues": [],
        "genie_findings": [],
        "source_errors": [],
        "investigation_completeness": "complete",
        "genie_routing": {
            "reason": "no_anomalies",
            "maximum_investigations": 2,
            "selected_count": 0,
            "decisions": [],
        },
        "evidence": {
            "jobs": {"run_id": 456, "result_state": "SUCCESS"},
            "observability": {"total_issue_count": 0},
            "genie": [],
        },
        "source_references": {
            "jobs": {"job_id": 123, "run_id": 456},
            "observability": {
                "monitoring_execution_timestamps": ["2026-10-26T08:18:00Z"]
            },
            "genie": [],
        },
        "root_cause": {
            "status": "unknown",
            "explanation": "The retrieved evidence does not establish a root cause.",
        },
        "impact": {
            "status": "unknown",
            "explanation": "The retrieved evidence does not quantify impact.",
        },
        "workflow": {"path": ["collect_jobs", "collect_observability", "construct_report"]},
    }


def partial_report() -> dict:
    report = complete_report()
    report["source_availability"]["genie"] = {"available": True, "status": "partial"}
    report["monitoring_health"].update(
        {"status": "WARN", "issue_count": 1, "warning_count": 1}
    )
    report["detected_issues"] = [
        {
            "source": "observability",
            "category": "quality",
            "status": "WARN",
            "name": "payments.failed_payment_rate",
            "details": {
                "entity_name": "payments",
                "metric_name": "failed_payment_rate",
                "metric_date": "2026-10-25",
                "quality_status": "WARN",
            },
        }
    ]
    finding = {
        "investigation_id": "genie-1",
        "question": "Show historical monitored failed_payment_rate values.",
        "status": "partial",
        "answer": "A warning was reported, but one result set was unavailable.",
        "evidence": {
            "ok": True,
            "status": "COMPLETED",
            "query_result_issue_count": 1,
            "query_evidence": [
                {
                    "generated_sql": "SELECT metric_date, metric_rate FROM governed_history",
                    "statement_id": "statement-1",
                    "result": {
                        "available": False,
                        "columns": None,
                        "rows": None,
                        "total_row_count": None,
                        "truncated": None,
                        "error": {
                            "type": "result_retrieval_failure",
                            "message": "Result expired.",
                        },
                    },
                }
            ],
        },
        "error": {
            "type": "partial_query_results",
            "message": "One query result was unavailable.",
        },
        "root_cause": {
            "status": "unknown",
            "explanation": "The retrieved evidence does not establish a root cause.",
        },
        "impact": {
            "status": "unknown",
            "explanation": "The retrieved evidence does not quantify impact.",
        },
    }
    report["genie_findings"] = [finding]
    report["evidence"]["genie"] = [finding]
    report["source_errors"] = [
        {
            "source": "genie",
            "error_type": "partial_query_results",
            "message": "One query result was unavailable.",
        }
    ]
    report["investigation_completeness"] = "partial"
    report["genie_routing"].update(
        {"reason": "selected_1_supported_anomalies", "selected_count": 1}
    )
    return report


def unavailable_report() -> dict:
    report = complete_report()
    report["source_availability"] = {
        "jobs": {"available": False, "status": "UNAVAILABLE"},
        "observability": {"available": False, "status": "UNAVAILABLE"},
        "genie": {"available": False, "status": "not_requested"},
    }
    report["pipeline_execution_health"] = {
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
    report["monitoring_health"] = {
        "status": "UNAVAILABLE",
        "check_count": None,
        "issue_count": None,
        "warning_count": None,
        "failure_count": None,
        "monitoring_execution_timestamps": [],
        "monitoring_reference_timestamps": [],
        "synthetic_activity_dates": {
            "latest_partition_dates": [],
            "activity_dates": [],
            "metric_dates": [],
        },
    }
    report["source_errors"] = [
        {"source": "jobs", "error_type": "RuntimeError", "message": "Jobs unavailable."},
        {
            "source": "observability",
            "error_type": "RuntimeError",
            "message": "Monitoring unavailable.",
        },
    ]
    report["investigation_completeness"] = "unavailable"
    report["evidence"] = {"jobs": None, "observability": None, "genie": []}
    return report


def recommendation_report() -> dict:
    report = complete_report()
    report["schema_version"] = "1.1"
    report["recommendations"] = [
        {
            "issue_id": "observability.quality.messages-missing-message-text-rate",
            "priority": "P1",
            "category": "diagnostic",
            "action": "Compare the retrieved metric with its historical baseline.",
            "rationale": "The latest monitored metric returned WARN.",
            "evidence_refs": ["detected_issues[0]"],
            "requires_human_approval": False,
            "verification_steps": ["Record the comparison against cited evidence."],
        }
    ]
    report["recommendation_summary"] = {
        "status": "generated",
        "recommendation_count": 1,
        "unsupported_issue_count": 0,
        "message": "One diagnostic recommendation was generated.",
    }
    return report


def test_auto_discovery_finds_composite_once_and_preserves_v1_tools():
    tools = all_tools()
    names = [item.name for item in tools]

    assert names.count("generate_dataops_investigation_report") == 1
    assert {
        "get_latest_pipeline_run",
        "get_latest_observability_summary",
        "ask_genie_observability",
    } <= set(names)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("factory", "expected_completeness"),
    [
        (complete_report, "complete"),
        (partial_report, "partial"),
        (unavailable_report, "unavailable"),
        (recommendation_report, "complete"),
    ],
)
async def test_service_validates_complete_partial_and_unavailable_reports(
    factory,
    expected_completeness,
):
    original = factory()

    async def runner():
        return deepcopy(original)

    result = await fetch_dataops_investigation_report(runner=runner)

    if result["schema_version"] == "1.0":
        assert InvestigationReportV1.model_validate(result)
    else:
        assert InvestigationReportV11.model_validate(result)
    assert result["investigation_completeness"] == expected_completeness
    assert result["generated_at"] == "2026-10-26T16:30:00Z"


@pytest.mark.asyncio
async def test_tool_wrapper_invokes_workflow_and_returns_json(monkeypatch):
    calls = 0

    async def fake_workflow():
        nonlocal calls
        calls += 1
        return complete_report()

    monkeypatch.setattr("agent.tools.investigation.run_investigation", fake_workflow)

    result = await generate_dataops_investigation_report.ainvoke({})

    assert calls == 1
    assert result["schema_version"] == "1.0"
    assert isinstance(result["generated_at"], str)


@pytest.mark.asyncio
async def test_incomplete_genie_evidence_and_errors_are_not_removed():
    expected = partial_report()

    async def runner():
        return expected

    result = await fetch_dataops_investigation_report(runner=runner)
    query_result = result["genie_findings"][0]["evidence"]["query_evidence"][0]["result"]

    assert query_result["available"] is False
    assert query_result["rows"] is None
    assert query_result["error"]["type"] == "result_retrieval_failure"
    assert result["source_errors"][0]["error_type"] == "partial_query_results"
    assert result["monitoring_health"]["status"] == "WARN"
    assert result["investigation_completeness"] == "partial"


@pytest.mark.asyncio
async def test_graph_exception_is_explicit_and_not_converted_to_health():
    async def failing_runner():
        raise RuntimeError("investigation graph failed")

    with pytest.raises(RuntimeError, match="investigation graph failed"):
        await fetch_dataops_investigation_report(runner=failing_runner)


@pytest.mark.asyncio
async def test_invalid_graph_report_fails_schema_validation():
    async def invalid_runner():
        return {"schema_version": "1.0", "investigation_completeness": "complete"}

    with pytest.raises(ValidationError):
        await fetch_dataops_investigation_report(runner=invalid_runner)


def test_composite_tool_has_no_write_or_administrative_surface():
    schema = generate_dataops_investigation_report.args_schema.model_json_schema()
    description = generate_dataops_investigation_report.description.lower()

    assert schema["properties"] == {}
    assert generate_dataops_investigation_report.return_direct is True
    assert "read-only" in description
    for unsupported in ("repair", "rerun", "write data", "backfill"):
        assert unsupported in description
    assert "does not prove root cause" in description
    assert "overall platform health" in description
    assert "without adding hypotheses" in description
    assert "require human approval" in description
    assert "never executed by this tool" in description
    assert generate_dataops_investigation_report.name not in {
        "write",
        "repair",
        "backfill",
        "run_job",
    }
