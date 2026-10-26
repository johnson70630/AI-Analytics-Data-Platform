from unittest.mock import AsyncMock, Mock

import pytest
from databricks.sdk.errors import PermissionDenied

from agent.tools.genie import (
    GENIE_AGENT_ID_ENV,
    ask_genie_observability,
    fetch_genie_observability,
)


AGENT_ID = "0123456789abcdef0123456789abcdef"


class FakeGenieAgent:
    response = None
    error = None
    instances = []

    def __init__(self, space_id, **kwargs):
        self.space_id = space_id
        self.kwargs = kwargs
        self.ask = AsyncMock(side_effect=self.error, return_value=self.response)
        self.instances.append(self)


class FakeQueryResultResponse:
    def __init__(self, statement_response):
        self.statement_response = statement_response

    def as_dict(self):
        return {"statement_response": self.statement_response}


def query_response(*, columns=None, rows=None, total_row_count=0, truncated=False):
    return FakeQueryResultResponse(
        {
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {"columns": columns or []},
                "total_row_count": total_row_count,
                "truncated": truncated,
            },
            "result": {"data_array": rows or []},
        }
    )


class FakeClient:
    def __init__(self, side_effect=None):
        self.genie = Mock()
        self.genie.get_message_attachment_query_result = Mock(
            side_effect=side_effect,
            return_value=query_response(),
        )


@pytest.fixture(autouse=True)
def reset_fake_agent(monkeypatch):
    FakeGenieAgent.response = None
    FakeGenieAgent.error = None
    FakeGenieAgent.instances = []
    monkeypatch.delenv(GENIE_AGENT_ID_ENV, raising=False)


@pytest.mark.asyncio
async def test_grounded_answer_and_evidence_are_preserved():
    FakeGenieAgent.response = {
        "status": "COMPLETED",
        "space_id": AGENT_ID,
        "conversation_id": "1" * 32,
        "message_id": "2" * 32,
        "deep_link": "https://example.databricks.com/genie/conversation",
        "attachments": [
            {
                "attachment_id": "3" * 32,
                "text": {
                    "content": "Two dates had freshness warnings.",
                    "purpose": "TEXT_ATTACHMENT_PURPOSE_ANSWER",
                },
            },
            {
                "attachment_id": "4" * 32,
                "query": {
                    "title": "Warning dates",
                    "description": "Observed warning history",
                    "query": "SELECT metric_date FROM governed_history",
                    "statement_id": "statement-123",
                    "query_result_metadata": {"row_count": 2, "is_truncated": False},
                },
            },
            {"suggested_questions": {"questions": ["Which entities were affected?"]}},
        ],
    }
    client = FakeClient()

    result = await fetch_genie_observability(
        "Which dates had freshness warnings?",
        client=client,
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    assert result["ok"] is True
    assert result["answer"] == "Two dates had freshness warnings."
    evidence = result["query_evidence"][0]
    assert evidence["attachment_id"] == "4" * 32
    assert evidence["title"] == "Warning dates"
    assert evidence["description"] == "Observed warning history"
    assert evidence["generated_sql"] == "SELECT metric_date FROM governed_history"
    assert evidence["statement_id"] == "statement-123"
    assert evidence["result_metadata"] == {"row_count": 2, "is_truncated": False}
    assert evidence["result"] == {
        "available": True,
        "status": {"state": "SUCCEEDED"},
        "columns": [],
        "rows": [],
        "total_row_count": 0,
        "truncated": False,
        "error": None,
    }
    assert result["suggested_questions"] == ["Which entities were affected?"]
    assert result["metadata"]["conversation_id"] == "1" * 32
    instance = FakeGenieAgent.instances[0]
    assert instance.space_id == AGENT_ID
    assert instance.kwargs["auth"] == "app"
    assert instance.kwargs["workspace_client_for"]("app") is client
    instance.ask.assert_awaited_once_with("Which dates had freshness warnings?")
    client.genie.get_message_attachment_query_result.assert_called_once_with(
        AGENT_ID, "1" * 32, "2" * 32, "4" * 32
    )


@pytest.mark.asyncio
async def test_query_result_rows_and_column_schema_are_returned():
    FakeGenieAgent.response = {
        "status": "COMPLETED",
        "conversation_id": "1" * 32,
        "message_id": "2" * 32,
        "attachments": [
            {
                "attachment_id": "3" * 32,
                "query": {"query": "SELECT observed evidence"},
            }
        ],
    }
    client = FakeClient(
        [
            query_response(
                columns=[
                    {"name": "metric_date", "type_name": "DATE"},
                    {"name": "quality_status", "type_name": "STRING"},
                ],
                rows=[["2026-08-23", "WARN"]],
                total_row_count=1,
            )
        ]
    )

    result = await fetch_genie_observability(
        "Which dates had warnings?",
        client=client,
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    query_result = result["query_evidence"][0]["result"]
    assert query_result["available"] is True
    assert query_result["columns"] == [
        {"name": "metric_date", "type_name": "DATE"},
        {"name": "quality_status", "type_name": "STRING"},
    ]
    assert query_result["rows"] == [["2026-08-23", "WARN"]]
    assert query_result["total_row_count"] == 1
    assert query_result["truncated"] is False
    assert result["query_result_issue_count"] == 0


@pytest.mark.asyncio
async def test_query_result_is_capped_at_100_rows_and_marked_truncated():
    FakeGenieAgent.response = {
        "status": "COMPLETED",
        "conversation_id": "1" * 32,
        "message_id": "2" * 32,
        "attachments": [
            {"attachment_id": "3" * 32, "query": {"query": "SELECT evidence"}}
        ],
    }
    client = FakeClient(
        [
            query_response(
                columns=[{"name": "value", "type_name": "LONG"}],
                rows=[[index] for index in range(150)],
                total_row_count=150,
            )
        ]
    )

    result = await fetch_genie_observability(
        "Show bounded evidence.",
        client=client,
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    query_result = result["query_evidence"][0]["result"]
    assert len(query_result["rows"]) == 100
    assert query_result["rows"][-1] == [99]
    assert query_result["total_row_count"] == 150
    assert query_result["truncated"] is True


@pytest.mark.asyncio
async def test_partial_query_result_failure_is_not_zero_anomalies():
    FakeGenieAgent.response = {
        "status": "COMPLETED",
        "conversation_id": "1" * 32,
        "message_id": "2" * 32,
        "attachments": [
            {"attachment_id": "3" * 32, "query": {"query": "SELECT first evidence"}},
            {"attachment_id": "4" * 32, "query": {"query": "SELECT second evidence"}},
        ],
    }
    client = FakeClient(
        [
            query_response(
                columns=[{"name": "status", "type_name": "STRING"}],
                rows=[["WARN"]],
                total_row_count=1,
            ),
            RuntimeError("result expired"),
        ]
    )

    result = await fetch_genie_observability(
        "Show both evidence sets.",
        client=client,
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    assert result["ok"] is True
    assert result["query_result_issue_count"] == 1
    assert result["query_evidence"][0]["result"]["rows"] == [["WARN"]]
    unavailable = result["query_evidence"][1]["result"]
    assert unavailable["available"] is False
    assert unavailable["rows"] is None
    assert unavailable["total_row_count"] is None
    assert unavailable["truncated"] is None
    assert unavailable["error"]["type"] == "result_retrieval_failure"


@pytest.mark.asyncio
async def test_missing_agent_id_returns_configuration_error():
    result = await fetch_genie_observability(
        "Show historical quality warnings.", agent_factory=FakeGenieAgent
    )

    assert result["ok"] is False
    assert result["error"]["type"] == "configuration"
    assert GENIE_AGENT_ID_ENV in result["error"]["message"]
    assert FakeGenieAgent.instances == []


@pytest.mark.asyncio
async def test_authentication_error_is_structured():
    FakeGenieAgent.error = PermissionDenied("not authorized")

    result = await fetch_genie_observability(
        "Show historical anomalies.",
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    assert result["ok"] is False
    assert result["error"]["type"] == "authentication"
    assert "not authorized" in result["error"]["message"]


@pytest.mark.asyncio
async def test_failed_response_is_not_reported_as_evidence():
    FakeGenieAgent.response = {
        "status": "FAILED",
        "conversation_id": "1" * 32,
        "message_id": "2" * 32,
        "error": {"message": "query could not be completed"},
    }

    result = await fetch_genie_observability(
        "Show historical anomalies.",
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert result["error"] == {
        "type": "genie_failure",
        "message": "query could not be completed",
    }
    assert result["answer"] is None


@pytest.mark.asyncio
async def test_empty_completed_response_is_explicit():
    FakeGenieAgent.response = {"status": "COMPLETED", "attachments": []}

    result = await fetch_genie_observability(
        "Show historical anomalies.",
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    assert result["ok"] is False
    assert result["error"]["type"] == "empty_response"


@pytest.mark.asyncio
async def test_timeout_preserves_polling_metadata_without_resubmitting():
    FakeGenieAgent.response = {
        "status": "IN_PROGRESS",
        "timed_out": True,
        "conversation_id": "1" * 32,
        "message_id": "2" * 32,
        "warning": "Retain IDs and poll later; do not resubmit.",
    }

    result = await fetch_genie_observability(
        "Show historical anomalies.",
        agent_id=AGENT_ID,
        agent_factory=FakeGenieAgent,
    )

    assert result["ok"] is False
    assert result["error"]["type"] == "timeout"
    assert result["metadata"]["timed_out"] is True
    assert result["metadata"]["conversation_id"] == "1" * 32
    assert result["metadata"]["message_id"] == "2" * 32
    FakeGenieAgent.instances[0].ask.assert_awaited_once()


@pytest.mark.asyncio
async def test_tool_wrapper_uses_environment_configuration(monkeypatch):
    monkeypatch.setenv(GENIE_AGENT_ID_ENV, AGENT_ID)
    expected = {
        "ok": True,
        "status": "COMPLETED",
        "answer": "Grounded answer",
    }

    async def fake_fetch(question):
        assert question == "What changed?"
        return expected

    monkeypatch.setattr("agent.tools.genie.fetch_genie_observability", fake_fetch)

    result = await ask_genie_observability.ainvoke({"question": "What changed?"})

    assert result == expected
