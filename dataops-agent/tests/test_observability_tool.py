from types import SimpleNamespace

import pytest

from agent.tools import all_tools
from agent.tools.observability import (
    WAREHOUSE_ID_ENV,
    fetch_latest_observability_summary,
)


def _response(columns, rows, *, state="SUCCEEDED", error=None):
    return SimpleNamespace(
        status=SimpleNamespace(
            state=state,
            error=(
                SimpleNamespace(message=error)
                if error is not None
                else None
            ),
        ),
        manifest=SimpleNamespace(
            schema=SimpleNamespace(
                columns=[
                    SimpleNamespace(name=name)
                    for name in columns
                ]
            )
        ),
        result=SimpleNamespace(data_array=rows),
    )


class FakeStatementExecution:
    def __init__(self, responses):
        self._responses = iter(responses)
        self.calls = []

    def execute_statement(self, **kwargs):
        self.calls.append(kwargs)
        return next(self._responses)


class FakeClient:
    def __init__(self, responses):
        self.statement_execution = FakeStatementExecution(
            responses
        )


def _category_responses(
    *,
    freshness=None,
    row_counts=None,
    quality=None,
    reconciliation=None,
):
    return [
        _response(
            ["entity_name", "freshness_status"],
            freshness or [],
        ),
        _response(
            ["entity_name", "volume_status"],
            row_counts or [],
        ),
        _response(
            ["entity_name", "metric_name", "quality_status"],
            quality or [],
        ),
        _response(
            ["reconciliation_name", "status"],
            reconciliation or [],
        ),
    ]


def test_all_checks_pass(monkeypatch):
    monkeypatch.delenv(WAREHOUSE_ID_ENV, raising=False)
    client = FakeClient(
        _category_responses(
            freshness=[["messages", "PASS"]],
            row_counts=[["messages", "PASS"]],
            quality=[
                ["messages", "missing_message_text_rate", "PASS"]
            ],
            reconciliation=[
                ["staging_to_fact_message", "PASS"]
            ],
        )
    )

    summary = fetch_latest_observability_summary(
        client=client,
        warehouse_id="warehouse-123",
    )

    assert summary["total_issue_count"] == 0
    for category in (
        "freshness",
        "row_counts",
        "quality",
        "reconciliation",
    ):
        assert summary[category]["check_count"] == 1
        assert summary[category]["issue_count"] == 0
        assert summary[category]["issues"] == []

    calls = client.statement_execution.calls
    assert len(calls) == 4
    assert all(call["warehouse_id"] == "warehouse-123" for call in calls)
    assert all(call["catalog"] == "ai_analytics" for call in calls)
    assert all(call["schema"] == "observability" for call in calls)
    assert "MAX(monitoring_run_at)" in calls[0]["statement"]
    assert "MAX(activity_date)" in calls[1]["statement"]
    assert "MAX(metric_date)" in calls[2]["statement"]
    assert "dq_pipeline_reconciliation" in calls[3]["statement"]


def test_warn_and_fail_rows_are_returned_as_issues():
    client = FakeClient(
        _category_responses(
            freshness=[
                ["messages", "PASS"],
                ["payments", "WARN"],
            ],
            row_counts=[["messages", "FAIL"]],
            quality=[
                ["payments", "failed_payment_rate", "WARN"]
            ],
            reconciliation=[
                ["staging_to_fact_message", "PASS"],
                ["fact_payment_to_gold_finance", "FAIL"],
            ],
        )
    )

    summary = fetch_latest_observability_summary(
        client=client,
        warehouse_id="warehouse-123",
    )

    assert summary["freshness"]["issue_count"] == 1
    assert summary["freshness"]["issues"] == [
        {"entity_name": "payments", "freshness_status": "WARN"}
    ]
    assert summary["row_counts"]["issue_count"] == 1
    assert summary["quality"]["issue_count"] == 1
    assert summary["reconciliation"]["issue_count"] == 1
    assert summary["reconciliation"]["issues"][0]["status"] == "FAIL"
    assert summary["total_issue_count"] == 4


def test_missing_warehouse_id_fails_before_sql(monkeypatch):
    monkeypatch.delenv(WAREHOUSE_ID_ENV, raising=False)
    client = FakeClient([])

    with pytest.raises(
        ValueError,
        match=f"{WAREHOUSE_ID_ENV} is not configured",
    ):
        fetch_latest_observability_summary(client=client)

    assert client.statement_execution.calls == []


def test_failed_sql_statement_raises_clear_error():
    client = FakeClient(
        [
            _response(
                [],
                [],
                state="FAILED",
                error="permission denied",
            )
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="state=FAILED, message=permission denied",
    ):
        fetch_latest_observability_summary(
            client=client,
            warehouse_id="warehouse-123",
        )


def test_empty_results_return_zero_counts():
    client = FakeClient(_category_responses())

    summary = fetch_latest_observability_summary(
        client=client,
        warehouse_id="warehouse-123",
    )

    assert summary["total_issue_count"] == 0
    for category in (
        "freshness",
        "row_counts",
        "quality",
        "reconciliation",
    ):
        assert summary[category] == {
            "check_count": 0,
            "issue_count": 0,
            "issues": [],
            "rows": [],
        }


def test_tool_auto_discovery_includes_all_read_only_dataops_tools():
    names = {tool.name for tool in all_tools()}

    assert "get_latest_pipeline_run" in names
    assert "get_latest_observability_summary" in names
    assert "ask_genie_observability" in names
