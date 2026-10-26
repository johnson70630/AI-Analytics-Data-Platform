"""Read-only access to the configured DataOps Observability Genie Agent."""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    DatabricksError,
    DeadlineExceeded,
    PermissionDenied,
    Unauthenticated,
)
from databricks_agentkit.runtime.genie import GenieAgent
from databricks_agentkit.runtime.workspace import workspace_client
from langchain_core.tools import tool


GENIE_AGENT_ID_ENV = "DATABRICKS_GENIE_AGENT_ID"
DEFAULT_TIMEOUT_SECONDS = 120.0
QUERY_RESULT_TIMEOUT_SECONDS = 30.0
MAX_QUERY_ATTACHMENTS = 5
MAX_RESULT_ROWS = 100
_GENIE_ID = re.compile(r"^[a-f0-9]{32}$")
_SUCCESS_STATUS = "COMPLETED"
_FAILURE_STATUSES = {"FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}


def _resolve_genie_agent_id(agent_id: str | None = None) -> str:
    """Resolve and validate the configured Genie Agent/Space identifier."""
    value = (agent_id or os.getenv(GENIE_AGENT_ID_ENV, "")).strip()
    if not value:
        raise ValueError(f"{GENIE_AGENT_ID_ENV} is not configured")
    if not _GENIE_ID.fullmatch(value):
        raise ValueError(
            f"{GENIE_AGENT_ID_ENV} must be a 32-character lowercase hexadecimal ID"
        )
    return value


def _failure(
    error_type: str,
    message: str,
    *,
    status: str = "ERROR",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a stable, non-throwing failure result for the LangGraph tool."""
    return {
        "ok": False,
        "status": status,
        "answer": None,
        "text_evidence": [],
        "query_evidence": [],
        "query_result_issue_count": 0,
        "suggested_questions": [],
        "metadata": metadata or {},
        "error": {"type": error_type, "message": message},
    }


def _metadata(result: dict[str, Any]) -> dict[str, Any]:
    """Keep useful traceability fields without echoing configuration or credentials."""
    keys = (
        "space_id",
        "conversation_id",
        "message_id",
        "deep_link",
        "created_timestamp",
        "last_updated_timestamp",
        "timed_out",
        "indeterminate_submission",
    )
    return {key: result[key] for key in keys if result.get(key) is not None}


def _normalize_genie_result(result: dict[str, Any]) -> dict[str, Any]:
    """Return only grounded answer content and evidence present in Genie's response."""
    status = str(result.get("status") or "UNKNOWN")
    metadata = _metadata(result)

    if result.get("timed_out"):
        return _failure(
            "timeout",
            result.get("warning")
            or "Genie did not complete before the configured timeout; do not infer an answer.",
            status=status,
            metadata=metadata,
        )

    if status in _FAILURE_STATUSES:
        error = result.get("error")
        if isinstance(error, dict):
            detail = error.get("message") or error.get("error")
        else:
            detail = str(error) if error else None
        return _failure(
            "genie_failure",
            detail or f"Genie returned terminal status {status}.",
            status=status,
            metadata=metadata,
        )

    text_evidence: list[dict[str, Any]] = []
    query_evidence: list[dict[str, Any]] = []
    suggested_questions: list[str] = []

    for attachment in result.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        attachment_id = attachment.get("attachment_id")

        text = attachment.get("text")
        if isinstance(text, dict) and text.get("content"):
            text_evidence.append(
                {
                    "attachment_id": attachment_id,
                    "purpose": text.get("purpose"),
                    "content": text["content"],
                }
            )

        query = attachment.get("query")
        if isinstance(query, dict):
            query_evidence.append(
                {
                    "attachment_id": attachment_id,
                    "title": query.get("title"),
                    "description": query.get("description"),
                    "generated_sql": query.get("query"),
                    "statement_id": query.get("statement_id"),
                    "result_metadata": query.get("query_result_metadata"),
                }
            )

        suggestions = attachment.get("suggested_questions")
        if isinstance(suggestions, dict):
            suggested_questions.extend(suggestions.get("questions") or [])

    answers = [
        item["content"]
        for item in text_evidence
        if item.get("purpose") == "TEXT_ATTACHMENT_PURPOSE_ANSWER"
    ]
    if not answers:
        answers = [item["content"] for item in text_evidence]
    answer = "\n\n".join(answers) or None

    if status != _SUCCESS_STATUS:
        return _failure(
            "incomplete_response",
            f"Genie returned non-success status {status}; do not infer an answer.",
            status=status,
            metadata=metadata,
        )

    if answer is None and not query_evidence:
        return _failure(
            "empty_response",
            "Genie completed without answer text or query evidence.",
            status=status,
            metadata=metadata,
        )

    return {
        "ok": True,
        "status": status,
        "answer": answer,
        "text_evidence": text_evidence,
        "query_evidence": query_evidence,
        "query_result_issue_count": 0,
        "suggested_questions": suggested_questions,
        "metadata": metadata,
        "error": None,
    }


def _query_result_failure(error_type: str, message: str) -> dict[str, Any]:
    """Represent unavailable evidence distinctly from a successful zero-row result."""
    return {
        "available": False,
        "status": None,
        "columns": None,
        "rows": None,
        "total_row_count": None,
        "truncated": None,
        "error": {"type": error_type, "message": message},
    }


def _statement_state(status: Any) -> str:
    if isinstance(status, dict):
        return str(status.get("state") or "UNKNOWN")
    return str(status or "UNKNOWN")


async def _add_query_results(
    normalized: dict[str, Any],
    client: WorkspaceClient,
    *,
    space_id: str,
    timeout_seconds: float = QUERY_RESULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Attach bounded official query results without executing or accepting SQL."""
    metadata = normalized["metadata"]
    conversation_id = metadata.get("conversation_id")
    message_id = metadata.get("message_id")
    issues = 0

    for index, evidence in enumerate(normalized["query_evidence"]):
        attachment_id = evidence.get("attachment_id")
        if index >= MAX_QUERY_ATTACHMENTS:
            evidence["result"] = _query_result_failure(
                "attachment_limit",
                f"Result retrieval is limited to {MAX_QUERY_ATTACHMENTS} query attachments.",
            )
            issues += 1
            continue
        if not all((conversation_id, message_id, attachment_id)):
            evidence["result"] = _query_result_failure(
                "missing_reference",
                "Genie did not return all IDs required to retrieve this query result.",
            )
            issues += 1
            continue

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    client.genie.get_message_attachment_query_result,
                    space_id,
                    conversation_id,
                    message_id,
                    attachment_id,
                ),
                timeout=timeout_seconds,
            )
            payload = response.as_dict() if hasattr(response, "as_dict") else response
            statement = payload.get("statement_response") or {}
            manifest = statement.get("manifest") or {}
            raw_result = statement.get("result") or {}
            rows = list(raw_result.get("data_array") or [])
            total_row_count = manifest.get("total_row_count")
            result = {
                "status": statement.get("status") or {"state": "UNKNOWN"},
                "columns": (manifest.get("schema") or {}).get("columns") or [],
                "rows": rows,
                "total_row_count": total_row_count,
                "truncated": bool(
                    manifest.get("truncated")
                    or raw_result.get("next_chunk_index") is not None
                    or len(rows) > MAX_RESULT_ROWS
                    or (
                        total_row_count is not None
                        and total_row_count > len(rows[:MAX_RESULT_ROWS])
                    )
                ),
            }
            state = _statement_state(result.get("status"))
            if state != "SUCCEEDED":
                evidence["result"] = _query_result_failure(
                    "query_result_unavailable",
                    f"Genie query-result retrieval returned state {state}.",
                )
                evidence["result"]["status"] = result.get("status")
                issues += 1
                continue

            rows = result["rows"]
            total_row_count = result.get("total_row_count")
            evidence["result"] = {
                "available": True,
                "status": result.get("status"),
                "columns": result.get("columns") or [],
                "rows": rows[:MAX_RESULT_ROWS],
                "total_row_count": total_row_count,
                "truncated": bool(
                    result.get("truncated")
                    or len(rows) > MAX_RESULT_ROWS
                    or (
                        total_row_count is not None
                        and total_row_count > len(rows[:MAX_RESULT_ROWS])
                    )
                ),
                "error": None,
            }
        except asyncio.TimeoutError:
            evidence["result"] = _query_result_failure(
                "timeout",
                "Genie query-result retrieval exceeded the bounded timeout.",
            )
            issues += 1
        except (Unauthenticated, PermissionDenied) as exc:
            evidence["result"] = _query_result_failure(
                "authentication",
                f"Databricks rejected query-result access: {exc}",
            )
            issues += 1
        except DatabricksError as exc:
            evidence["result"] = _query_result_failure(
                "databricks_api",
                f"Genie query-result retrieval failed: {exc}",
            )
            issues += 1
        except Exception as exc:
            evidence["result"] = _query_result_failure(
                "result_retrieval_failure",
                f"Genie query-result retrieval failed ({type(exc).__name__}): {exc}",
            )
            issues += 1

    normalized["query_result_issue_count"] = issues
    return normalized


async def fetch_genie_observability(
    question: str,
    *,
    client: WorkspaceClient | None = None,
    agent_id: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    agent_factory: Callable[..., GenieAgent] = GenieAgent,
) -> dict[str, Any]:
    """Ask the configured Genie Agent and return its grounded, read-only response evidence."""
    if not isinstance(question, str) or not question.strip():
        return _failure("invalid_request", "question must not be empty")

    try:
        resolved_agent_id = _resolve_genie_agent_id(agent_id)
        resolved_client = client

        def workspace_client_for(_auth: str) -> WorkspaceClient:
            nonlocal resolved_client
            if resolved_client is None:
                resolved_client = workspace_client()
            return resolved_client

        genie = agent_factory(
            resolved_agent_id,
            wait_seconds=timeout_seconds,
            auth="app",
            workspace_client_for=workspace_client_for,
        )
        result = await genie.ask(question.strip())
    except ValueError as exc:
        return _failure("configuration", str(exc))
    except (Unauthenticated, PermissionDenied) as exc:
        return _failure(
            "authentication",
            f"Databricks rejected Genie authentication or authorization: {exc}",
        )
    except (DeadlineExceeded, TimeoutError) as exc:
        return _failure("timeout", f"Genie request timed out: {exc}")
    except DatabricksError as exc:
        return _failure("databricks_api", f"Genie API request failed: {exc}")
    except Exception as exc:
        return _failure(
            "request_failure",
            f"Genie request failed ({type(exc).__name__}): {exc}",
        )

    if not isinstance(result, dict):
        return _failure("invalid_response", "Genie returned an unsupported response format.")
    normalized = _normalize_genie_result(result)
    if not normalized["ok"] or not normalized["query_evidence"]:
        return normalized
    if resolved_client is None:
        return _failure(
            "client_unavailable",
            "The authenticated Databricks client was unavailable for query-result retrieval.",
            status=normalized["status"],
            metadata=normalized["metadata"],
        )
    return await _add_query_results(
        normalized,
        resolved_client,
        space_id=resolved_agent_id,
    )


@tool
async def ask_genie_observability(question: str) -> dict[str, Any]:
    """Ask the read-only DataOps Observability Genie Agent a flexible historical data-health question.

    Use this for historical stale-data, volume-anomaly, data-quality, and cross-layer
    reconciliation questions that the fixed latest-health tool does not answer. Treat returned
    answer text, SQL, metadata, and links only as retrieved Genie evidence. Do not infer underlying
    rows or root causes that Genie did not return, and distinguish monitoring execution timestamps
    from the data/reference dates being monitored. A query result with available=false is missing
    evidence, not proof of zero rows or zero anomalies.
    """
    return await fetch_genie_observability(question)
