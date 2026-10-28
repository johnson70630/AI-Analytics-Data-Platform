from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

from agent.investigation.workflow import InvestigationDependencies


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def pipeline_run():
    return {
        "found": True,
        "job_id": 123,
        "run_id": 456,
        "run_name": "daily-pipeline",
        "lifecycle_state": "TERMINATED",
        "result_state": "SUCCESS",
        "start_time": "2026-10-26T08:00:00Z",
        "end_time": "2026-10-26T08:20:00Z",
        "unsuccessful_tasks": [],
        "tasks": [],
    }


def observability_warning():
    quality_issue = {
        "entity_name": "payments",
        "metric_name": "failed_payment_rate",
        "metric_date": "2026-08-31",
        "metric_rate": "0.10",
        "quality_status": "WARN",
    }
    return {
        "freshness": {
            "check_count": 0,
            "issue_count": 0,
            "issues": [],
            "rows": [],
        },
        "row_counts": {
            "check_count": 0,
            "issue_count": 0,
            "issues": [],
            "rows": [],
        },
        "quality": {
            "check_count": 1,
            "issue_count": 1,
            "issues": [quality_issue],
            "rows": [quality_issue],
        },
        "reconciliation": {
            "check_count": 0,
            "issue_count": 0,
            "issues": [],
            "rows": [],
        },
        "total_issue_count": 1,
    }


def genie_result():
    return {
        "ok": True,
        "status": "COMPLETED",
        "answer": "One historical warning was retrieved.",
        "text_evidence": [],
        "query_evidence": [
            {
                "attachment_id": "attachment-1",
                "generated_sql": "SELECT metric_date, metric_rate FROM governed_history",
                "statement_id": "statement-1",
                "result": {
                    "available": True,
                    "status": {"state": "SUCCEEDED"},
                    "columns": [
                        {"name": "metric_date", "type_name": "DATE"},
                        {"name": "metric_rate", "type_name": "DOUBLE"},
                    ],
                    "rows": [["2026-08-31", "0.10"]],
                    "total_row_count": 1,
                    "truncated": False,
                    "error": None,
                },
            }
        ],
        "query_result_issue_count": 0,
        "suggested_questions": [],
        "metadata": {},
        "error": None,
    }


def test_langgraph_config_targets_actual_investigation_graph():
    config = json.loads((PROJECT_ROOT / "langgraph.json").read_text())

    assert config == {
        "dependencies": ["."],
        "graphs": {
            "dataops_investigation": "./agent/investigation/studio.py:graph",
        },
        "env": "./.env",
        "python_version": "3.12",
    }


def test_studio_import_compiles_without_evidence_calls(monkeypatch):
    import agent.investigation.workflow as workflow

    calls: list[str] = []

    def unexpected_jobs(**_kwargs):
        calls.append("jobs")
        raise AssertionError("Studio import must not call Jobs")

    def unexpected_observability(**_kwargs):
        calls.append("observability")
        raise AssertionError("Studio import must not query observability")

    async def unexpected_genie(*_args, **_kwargs):
        calls.append("genie")
        raise AssertionError("Studio import must not call Genie")

    monkeypatch.setattr(workflow, "fetch_latest_pipeline_run", unexpected_jobs)
    monkeypatch.setattr(
        workflow,
        "fetch_latest_observability_summary",
        unexpected_observability,
    )
    monkeypatch.setattr(workflow, "fetch_genie_observability", unexpected_genie)
    monkeypatch.delenv("LANGSMITH_TRACING", raising=False)
    sys.modules.pop("agent.investigation.studio", None)

    studio = importlib.import_module("agent.investigation.studio")

    assert calls == []
    assert studio.graph is not None
    assert studio.os.environ["LANGSMITH_TRACING"] == "false"


def test_studio_preserves_explicit_tracing_choice(monkeypatch):
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    sys.modules.pop("agent.investigation.studio", None)

    studio = importlib.import_module("agent.investigation.studio")

    assert studio.os.environ["LANGSMITH_TRACING"] == "true"


def test_studio_graph_displays_all_nodes_and_conditional_edges():
    from agent.investigation.studio import create_studio_graph

    drawable = create_studio_graph(
        InvestigationDependencies(
            fetch_jobs=pipeline_run,
            fetch_observability=observability_warning,
        )
    ).get_graph()

    assert set(drawable.nodes) == {
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
    edges = {(edge.source, edge.target, edge.conditional) for edge in drawable.edges}
    assert ("__start__", "collect_jobs", False) in edges
    assert ("__start__", "collect_observability", False) in edges
    assert ("collect_jobs", "join_evidence", False) in edges
    assert ("collect_observability", "join_evidence", False) in edges
    assert ("evaluate_issues", "investigate_with_genie", True) in edges
    assert ("evaluate_issues", "generate_recommendations", True) in edges
    assert ("investigate_with_genie", "generate_recommendations", False) in edges
    assert ("generate_recommendations", "construct_report", False) in edges


@pytest.mark.asyncio
async def test_studio_graph_streams_node_updates_with_offline_evidence():
    from agent.investigation.studio import create_studio_graph

    calls: list[str] = []

    def jobs():
        calls.append("jobs")
        return pipeline_run()

    def observability():
        calls.append("observability")
        return observability_warning()

    async def genie(question: str):
        calls.append("genie")
        assert "failed_payment_rate" in question
        return genie_result()

    graph = create_studio_graph(
        InvestigationDependencies(
            fetch_jobs=jobs,
            fetch_observability=observability,
            fetch_genie=genie,
        )
    )
    updates = [update async for update in graph.astream({}, stream_mode="updates")]
    node_names = [next(iter(update)) for update in updates]
    result = await graph.ainvoke({})

    assert set(node_names[:2]) == {"collect_jobs", "collect_observability"}
    assert node_names[2:] == [
        "join_evidence",
        "evaluate_issues",
        "investigate_with_genie",
        "generate_recommendations",
        "construct_report",
    ]
    assert calls.count("jobs") == 2
    assert calls.count("observability") == 2
    assert calls.count("genie") == 2
    assert result["report"]["monitoring_health"]["status"] == "WARN"
    assert result["report"]["schema_version"] == "1.1"
    assert result["report"]["recommendations"]
    assert result["report"]["genie_routing"]["selected_count"] == 1
    assert result["report"]["source_errors"] == []
    assert result["report"]["investigation_completeness"] == "complete"
