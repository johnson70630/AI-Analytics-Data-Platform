from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from agent.investigation import workflow
from agent.investigation import (
    InvestigationDependencies,
    InvestigationReportV1,
    InvestigationReportV11,
    build_investigation_graph,
    run_investigation,
)


GENERATED_AT = datetime(2026, 10, 26, 9, 30, tzinfo=timezone.utc)


def pipeline_run(*, result_state: str = "SUCCESS", failed_task: str | None = None):
    tasks = [
        {
            "task_key": "dbt_observability",
            "result_state": result_state if failed_task == "dbt_observability" else "SUCCESS",
            "message": "observability task failed" if failed_task else None,
        }
    ]
    return {
        "found": True,
        "job_id": 123,
        "run_id": 456,
        "run_name": "daily-pipeline",
        "lifecycle_state": "TERMINATED",
        "result_state": result_state,
        "message": None,
        "start_time": "2026-10-25T08:00:00+00:00",
        "end_time": "2026-10-25T08:20:00+00:00",
        "run_page_url": "https://example/456",
        "unsuccessful_tasks": [failed_task] if failed_task else [],
        "tasks": tasks,
    }


def observability_summary(
    *,
    freshness_status: str = "PASS",
    volume_status: str = "PASS",
    quality_status: str = "PASS",
    reconciliation_status: str = "PASS",
):
    freshness_row = {
        "entity_name": "messages",
        "monitoring_run_at": "2026-10-25T08:18:00Z",
        "monitoring_reference_at": "2026-10-25T00:00:00Z",
        "latest_partition_date": "2026-10-24",
        "freshness_status": freshness_status,
    }
    row_count_row = {
        "entity_name": "messages",
        "activity_date": "2026-10-24",
        "row_count": 100,
        "volume_status": volume_status,
    }
    quality_row = {
        "entity_name": "messages",
        "metric_name": "missing_message_text_rate",
        "metric_date": "2026-10-24",
        "metric_rate": 0.02,
        "quality_status": quality_status,
    }
    reconciliation_row = {
        "reconciliation_name": "staging_to_fact_message",
        "status": reconciliation_status,
    }

    def category(row, status):
        issues = [] if status == "PASS" else [row]
        return {"check_count": 1, "issue_count": len(issues), "issues": issues, "rows": [row]}

    statuses = [freshness_status, volume_status, quality_status, reconciliation_status]
    return {
        "freshness": category(freshness_row, freshness_status),
        "row_counts": category(row_count_row, volume_status),
        "quality": category(quality_row, quality_status),
        "reconciliation": category(reconciliation_row, reconciliation_status),
        "total_issue_count": sum(status != "PASS" for status in statuses),
    }


def genie_success(*, query_issue_count: int = 0):
    result_available = query_issue_count == 0
    return {
        "ok": True,
        "status": "COMPLETED",
        "answer": "One historical warning was retrieved.",
        "text_evidence": [],
        "query_evidence": [
            {
                "attachment_id": "attachment-1",
                "generated_sql": "SELECT metric_date, quality_status FROM governed_history",
                "statement_id": "statement-1",
                "result": {
                    "available": result_available,
                    "status": {"state": "SUCCEEDED"} if result_available else None,
                    "columns": [
                        {"name": "metric_date", "type_name": "DATE"},
                        {"name": "quality_status", "type_name": "STRING"},
                    ]
                    if result_available
                    else None,
                    "rows": [["2026-10-24", "WARN"]] if result_available else None,
                    "total_row_count": 1 if result_available else None,
                    "truncated": False if result_available else None,
                    "error": None
                    if result_available
                    else {"type": "result_retrieval_failure", "message": "result expired"},
                },
            }
        ],
        "query_result_issue_count": query_issue_count,
        "suggested_questions": [],
        "metadata": {
            "conversation_id": "conversation-1",
            "message_id": "message-1",
            "deep_link": "https://example/genie/conversation-1",
        },
        "error": None,
    }


class RecordingFetcher:
    def __init__(self, result: dict[str, Any] | None = None, error: Exception | None = None):
        self.result = result
        self.error = error
        self.calls = 0

    def __call__(self) -> dict[str, Any]:
        self.calls += 1
        if self.error:
            raise self.error
        assert self.result is not None
        return self.result


class RecordingGenie:
    def __init__(self, results: list[dict[str, Any]] | None = None):
        self.results = list(results or [genie_success()])
        self.questions: list[str] = []

    async def __call__(self, question: str) -> dict[str, Any]:
        self.questions.append(question)
        return self.results.pop(0)


def dependencies(
    jobs: Callable[[], dict[str, Any]],
    observability: Callable[[], dict[str, Any]],
    *,
    genie: RecordingGenie | Callable[[str], Any] | None = None,
    genie_timeout_seconds: float = 1.0,
):
    return InvestigationDependencies(
        fetch_jobs=jobs,
        fetch_observability=observability,
        fetch_genie=genie,
        genie_timeout_seconds=genie_timeout_seconds,
        clock=lambda: GENERATED_AT,
    )


async def invoke(deps: InvestigationDependencies):
    return await build_investigation_graph(deps).ainvoke({})


@pytest.mark.asyncio
async def test_healthy_pipeline_and_monitoring_skip_genie():
    jobs = RecordingFetcher(pipeline_run())
    observability = RecordingFetcher(observability_summary())
    genie = RecordingGenie()

    state = await invoke(dependencies(jobs, observability, genie=genie))
    report = state["report"]

    assert report["schema_version"] == "1.1"
    assert report["investigation_completeness"] == "complete"
    assert report["pipeline_execution_health"]["status"] == "PASS"
    assert report["monitoring_health"]["status"] == "PASS"
    assert report["detected_issues"] == []
    assert report["genie_findings"] == []
    assert report["recommendations"] == []
    assert report["recommendation_summary"]["status"] == "no_findings"
    assert report["genie_routing"]["reason"] == "no_anomalies"
    assert report["source_availability"]["genie"] == {
        "available": False,
        "status": "not_requested",
    }
    assert genie.questions == []
    assert report["workflow"]["path"] == [
        "collect_jobs",
        "collect_observability",
        "join_evidence",
        "evaluate_issues",
        "generate_recommendations",
        "construct_report",
    ]


@pytest.mark.asyncio
async def test_pipeline_success_quality_warn_routes_to_genie():
    genie = RecordingGenie()
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary(quality_status="WARN")),
            genie=genie,
        )
    )

    assert report["pipeline_execution_health"]["status"] == "PASS"
    assert report["pipeline_execution_health"]["pipeline_failed"] is False
    assert report["monitoring_health"]["status"] == "WARN"
    assert len(genie.questions) == 1
    assert "quality metric 'missing_message_text_rate'" in genie.questions[0]
    assert "synthetic metric date '2026-10-24'" in genie.questions[0]
    assert report["genie_routing"]["selected_count"] == 1
    assert report["genie_findings"][0]["status"] == "complete"
    assert report["workflow"]["path"][-3:] == [
        "investigate_with_genie",
        "generate_recommendations",
        "construct_report",
    ]


@pytest.mark.asyncio
async def test_multiple_anomalies_are_limited_to_two_by_deterministic_priority():
    genie = RecordingGenie([genie_success(), genie_success()])
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(
                observability_summary(
                    freshness_status="WARN",
                    volume_status="FAIL",
                    quality_status="FAIL",
                    reconciliation_status="FAIL",
                )
            ),
            genie=genie,
        )
    )

    assert len(genie.questions) == 2
    assert "quality metric" in genie.questions[0]
    assert "row counts" in genie.questions[1]
    assert report["genie_routing"]["maximum_investigations"] == 2
    assert report["genie_routing"]["selected_count"] == 2
    decisions = report["genie_routing"]["decisions"]
    assert any(
        decision["category"] == "freshness"
        and decision["reason"] == "investigation_limit_reached"
        for decision in decisions
    )
    assert any(
        decision["category"] == "reconciliation"
        and decision["reason"]
        == "reconciliation_is_a_current_snapshot_without_historical_dates"
        for decision in decisions
    )


@pytest.mark.asyncio
async def test_pipeline_failure_does_not_hide_available_monitoring():
    report = await run_investigation(
        dependencies(
            RecordingFetcher(
                pipeline_run(result_state="FAILED", failed_task="dbt_observability")
            ),
            RecordingFetcher(observability_summary()),
        )
    )

    assert report["pipeline_execution_health"]["status"] == "FAIL"
    assert report["pipeline_execution_health"]["pipeline_failed"] is True
    assert report["monitoring_health"]["status"] == "PASS"
    assert report["genie_routing"]["reason"] == "no_supported_anomalies"
    execution_diagnostic = next(
        item
        for item in report["recommendations"]
        if item["issue_id"].startswith("jobs.execution")
        and item["category"] == "diagnostic"
    )
    assert execution_diagnostic["priority"] == "P0"
    assert "unsupported logs" not in execution_diagnostic["action"].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("jobs_error", "observability_error", "completeness", "jobs_status", "monitoring_status"),
    [
        (PermissionError("jobs denied"), None, "partial", "UNAVAILABLE", "PASS"),
        (None, RuntimeError("warehouse unavailable"), "partial", "PASS", "UNAVAILABLE"),
        (
            RuntimeError("jobs unavailable"),
            RuntimeError("observability unavailable"),
            "unavailable",
            "UNAVAILABLE",
            "UNAVAILABLE",
        ),
    ],
)
async def test_source_failures_preserve_partial_or_unavailable_reports(
    jobs_error,
    observability_error,
    completeness,
    jobs_status,
    monitoring_status,
):
    jobs = RecordingFetcher(pipeline_run(), error=jobs_error)
    observability = RecordingFetcher(observability_summary(), error=observability_error)

    report = await run_investigation(dependencies(jobs, observability))

    assert report["investigation_completeness"] == completeness
    assert report["pipeline_execution_health"]["status"] == jobs_status
    assert report["monitoring_health"]["status"] == monitoring_status
    assert len(report["source_errors"]) == int(jobs_error is not None) + int(
        observability_error is not None
    )
    assert report["root_cause"]["status"] == "unknown"
    assert report["impact"]["status"] == "unknown"


@pytest.mark.asyncio
async def test_genie_success_preserves_query_rows_schema_and_references():
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary(quality_status="WARN")),
            genie=RecordingGenie(),
        )
    )

    finding = report["genie_findings"][0]
    query = finding["evidence"]["query_evidence"][0]
    assert query["generated_sql"].startswith("SELECT")
    assert query["result"]["rows"] == [["2026-10-24", "WARN"]]
    assert query["result"]["columns"][1]["name"] == "quality_status"
    assert query["result"]["truncated"] is False
    assert report["source_references"]["genie"][0]["queries"] == [
        {
            "attachment_id": "attachment-1",
            "statement_id": "statement-1",
            "result_available": True,
            "truncated": False,
        }
    ]
    assert finding["root_cause"]["status"] == "unknown"
    assert finding["impact"]["status"] == "unknown"


@pytest.mark.asyncio
async def test_genie_timeout_preserves_base_report():
    questions = []

    async def slow_genie(question):
        questions.append(question)
        await asyncio.sleep(0.05)
        return genie_success()

    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary(quality_status="WARN")),
            genie=slow_genie,
            genie_timeout_seconds=0.001,
        )
    )

    assert len(questions) == 1
    assert report["investigation_completeness"] == "partial"
    assert report["pipeline_execution_health"]["status"] == "PASS"
    assert report["monitoring_health"]["status"] == "WARN"
    assert report["genie_findings"][0]["status"] == "unavailable"
    assert report["source_errors"][-1]["error_type"] == "timeout"
    assert report["source_availability"]["genie"]["available"] is False


@pytest.mark.asyncio
async def test_genie_query_result_partial_failure_is_not_zero_anomalies():
    genie = RecordingGenie([genie_success(query_issue_count=1)])
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary(quality_status="WARN")),
            genie=genie,
        )
    )

    finding = report["genie_findings"][0]
    result = finding["evidence"]["query_evidence"][0]["result"]
    assert finding["status"] == "partial"
    assert result["available"] is False
    assert result["rows"] is None
    assert report["investigation_completeness"] == "partial"
    assert report["source_errors"][-1]["error_type"] == "partial_query_results"


@pytest.mark.asyncio
async def test_genie_truncation_is_preserved_and_marks_investigation_partial():
    truncated = genie_success()
    truncated["query_evidence"][0]["result"]["truncated"] = True
    truncated["query_evidence"][0]["result"]["total_row_count"] = 150
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary(volume_status="WARN")),
            genie=RecordingGenie([truncated]),
        )
    )

    finding = report["genie_findings"][0]
    assert finding["status"] == "partial"
    assert finding["evidence"]["query_evidence"][0]["result"]["truncated"] is True
    assert finding["evidence"]["query_evidence"][0]["result"]["total_row_count"] == 150
    assert report["investigation_completeness"] == "partial"
    assert "1 were truncated" in report["source_errors"][-1]["message"]


@pytest.mark.asyncio
async def test_current_reconciliation_snapshot_is_not_sent_to_genie():
    genie = RecordingGenie()
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary(reconciliation_status="FAIL")),
            genie=genie,
        )
    )

    assert genie.questions == []
    assert report["genie_routing"]["selected_count"] == 0
    assert report["genie_routing"]["reason"] == "no_supported_anomalies"
    assert report["genie_routing"]["decisions"][0]["reason"] == (
        "reconciliation_is_a_current_snapshot_without_historical_dates"
    )


@pytest.mark.asyncio
async def test_freshness_requires_monitoring_and_reference_structure():
    summary = observability_summary(freshness_status="WARN")
    summary["freshness"]["issues"][0].pop("monitoring_run_at")
    genie = RecordingGenie()

    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(summary),
            genie=genie,
        )
    )

    assert genie.questions == []
    assert report["genie_routing"]["decisions"][0]["reason"] == (
        "freshness_issue_lacks_monitoring_and_reference_structure"
    )


@pytest.mark.asyncio
async def test_collectors_execute_in_parallel_and_join_before_evaluation():
    barrier = threading.Barrier(2)
    calls: list[str] = []
    lock = threading.Lock()

    def jobs_fetcher():
        with lock:
            calls.append("jobs_started")
        barrier.wait(timeout=2)
        return pipeline_run()

    def observability_fetcher():
        with lock:
            calls.append("observability_started")
        barrier.wait(timeout=2)
        return observability_summary()

    report = await run_investigation(dependencies(jobs_fetcher, observability_fetcher))

    assert set(calls) == {"jobs_started", "observability_started"}
    assert report["workflow"]["path"][:4] == [
        "collect_jobs",
        "collect_observability",
        "join_evidence",
        "evaluate_issues",
    ]


@pytest.mark.asyncio
async def test_report_schema_validation_and_timestamp_distinctions():
    report = await run_investigation(
        dependencies(
            RecordingFetcher(pipeline_run()),
            RecordingFetcher(observability_summary()),
        )
    )

    validated = InvestigationReportV11.model_validate(report)
    assert validated.schema_version == "1.1"
    assert validated.generated_at == GENERATED_AT
    assert validated.monitoring_health.monitoring_execution_timestamps == [
        "2026-10-25T08:18:00Z"
    ]
    assert validated.monitoring_health.monitoring_reference_timestamps == [
        "2026-10-25T00:00:00Z"
    ]
    assert validated.monitoring_health.synthetic_activity_dates == {
        "latest_partition_dates": ["2026-10-24"],
        "activity_dates": ["2026-10-24"],
        "metric_dates": ["2026-10-24"],
    }
    invalid = {**report, "schema_version": "2.0"}
    with pytest.raises(ValidationError):
        InvestigationReportV11.model_validate(invalid)

    legacy_payload = {
        key: value
        for key, value in report.items()
        if key not in {"recommendations", "recommendation_summary"}
    }
    legacy_payload["schema_version"] = "1.0"
    assert InvestigationReportV1.model_validate(legacy_payload).schema_version == "1.0"


@pytest.mark.asyncio
async def test_graph_exposes_only_bounded_read_only_dependencies():
    calls: list[str] = []

    def jobs_fetcher():
        calls.append("fetch_jobs")
        return pipeline_run()

    def observability_fetcher():
        calls.append("fetch_observability")
        return observability_summary(quality_status="WARN")

    async def genie_fetcher(question):
        calls.append("fetch_genie")
        assert "quality metric" in question
        return genie_success()

    deps = dependencies(jobs_fetcher, observability_fetcher, genie=genie_fetcher)
    report = await run_investigation(deps)
    graph = build_investigation_graph(deps).get_graph()

    assert calls == ["fetch_jobs", "fetch_observability", "fetch_genie"]
    assert set(graph.nodes) == {
        "__start__",
        "collect_jobs",
        "collect_observability",
        "join_evidence",
        "evaluate_issues",
        "investigate_with_genie",
        "generate_recommendations",
        "construct_report",
        "__end__",
    }
    assert "write" not in " ".join(graph.nodes)
    assert "repair" not in " ".join(graph.nodes)
    assert report["genie_routing"]["selected_count"] <= 2


@pytest.mark.asyncio
async def test_default_dependencies_delegate_to_existing_services(monkeypatch):
    jobs_client = object()
    observability_client = object()
    genie_client = object()
    calls = []

    def fetch_jobs(*, client, job_id):
        calls.append(("jobs", client, job_id))
        return pipeline_run()

    def fetch_observability(*, client, warehouse_id):
        calls.append(("observability", client, warehouse_id))
        return observability_summary()

    async def fetch_genie(question, *, client, agent_id, timeout_seconds):
        calls.append(("genie", question, client, agent_id, timeout_seconds))
        return genie_success()

    monkeypatch.setattr(workflow, "fetch_latest_pipeline_run", fetch_jobs)
    monkeypatch.setattr(workflow, "fetch_latest_observability_summary", fetch_observability)
    monkeypatch.setattr(workflow, "fetch_genie_observability", fetch_genie)

    deps = workflow.default_dependencies(
        jobs_client=jobs_client,
        job_id=123,
        observability_client=observability_client,
        warehouse_id="warehouse-123",
        genie_client=genie_client,
        genie_agent_id="agent-123",
        genie_service_timeout_seconds=12,
        clock=lambda: GENERATED_AT,
    )

    assert deps.fetch_jobs()["run_id"] == 456
    assert deps.fetch_observability()["total_issue_count"] == 0
    assert (await deps.fetch_genie("What changed?"))["ok"] is True
    assert calls == [
        ("jobs", jobs_client, 123),
        ("observability", observability_client, "warehouse-123"),
        ("genie", "What changed?", genie_client, "agent-123", 12),
    ]
