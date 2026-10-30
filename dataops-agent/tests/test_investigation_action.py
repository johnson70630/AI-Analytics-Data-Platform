from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from agent.investigation.workflow import InvestigationDependencies, run_investigation
from databricks_agentkit import DurableAgentServer
from databricks_agentkit.runtime.auth import InvocationAuthPolicy
from databricks_agentkit.runtime.store import InMemoryRuntimeStore
from runtime import adapter
from runtime.investigation import INVESTIGATION_ACTION, execute_investigation_action


def _snapshot() -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": "2026-10-29T18:00:00+00:00",
        "overall_status": "WARN",
        "pipeline": {
            "status": "PASS",
            "run_id": 456,
            "result_state": "SUCCESS",
            "end_time": "2026-10-29T17:00:00+00:00",
        },
        "monitoring": {
            "status": "WARN",
            "monitoring_run_at": "2026-10-29T17:05:00+00:00",
            "monitoring_reference_at": "2026-10-29T16:00:00+00:00",
            "synthetic_reference_date": "2026-10-28",
            "pipeline_association_verified": False,
        },
    }


def _jobs() -> dict:
    return {
        "found": True,
        "job_id": 123,
        "run_id": 456,
        "run_name": "daily",
        "result_state": "SUCCESS",
        "lifecycle_state": "TERMINATED",
        "start_time": "2026-10-29T16:00:00Z",
        "end_time": "2026-10-29T17:00:00Z",
        "unsuccessful_tasks": [],
        "tasks": [],
    }


def _observability() -> dict:
    freshness = {
        "entity_name": "messages",
        "latest_partition_date": "2026-10-28",
        "monitoring_reference_at": "2026-10-29T16:00:00Z",
        "monitoring_run_at": "2026-10-29T17:05:00Z",
        "freshness_status": "PASS",
    }
    return {
        "freshness": {"check_count": 1, "issue_count": 0, "issues": [], "rows": [freshness]},
        "row_counts": {"check_count": 0, "issue_count": 0, "issues": [], "rows": []},
        "quality": {"check_count": 0, "issue_count": 0, "issues": [], "rows": []},
        "reconciliation": {"check_count": 0, "issue_count": 0, "issues": [], "rows": []},
        "total_issue_count": 0,
    }


async def _report(*, observability_available: bool = True) -> dict:
    def failed_observability():
        raise RuntimeError("Monitoring unavailable")

    dependencies = InvestigationDependencies(
        fetch_jobs=_jobs,
        fetch_observability=_observability if observability_available else failed_observability,
        fetch_genie=None,
        clock=lambda: datetime(2026, 10, 29, 18, 30, tzinfo=timezone.utc),
    )
    return await run_investigation(dependencies)


@pytest.mark.asyncio
async def test_action_executes_existing_workflow_and_preserves_structured_report():
    async def runner():
        return await _report()

    result = await execute_investigation_action(
        {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
        runner=runner,
    )

    item = result["output"][0]
    assert result["status"] == "completed"
    assert item["type"] == "investigation_report"
    assert item["report"]["schema_version"] == "1.1"
    assert item["report"]["root_cause"]["status"] == "unknown"
    assert item["report"]["impact"]["status"] == "unknown"
    assert item["report"]["pipeline_execution_health"]["status"] == "PASS"
    assert item["report"]["monitoring_health"]["status"] == "PASS"
    assert item["health_snapshot"]["pipeline"]["run_id"] == 456
    assert item["health_snapshot"]["monitoring"]["synthetic_reference_date"] == "2026-10-28"
    assert item["pipeline_monitoring_association"]["verified"] is False
    assert len(item["snapshot_fingerprint"]) == 64


@pytest.mark.asyncio
async def test_action_preserves_partial_source_errors():
    async def runner():
        return await _report(observability_available=False)

    result = await execute_investigation_action(
        {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
        runner=runner,
    )
    report = result["output"][0]["report"]

    assert report["investigation_completeness"] == "partial"
    assert report["source_availability"]["observability"]["available"] is False
    assert report["source_errors"][0]["source"] == "observability"


@pytest.mark.asyncio
async def test_action_rejects_v1_report_instead_of_silently_downgrading_contract():
    report = await _report()
    report["schema_version"] = "1.0"
    report.pop("recommendations")
    report.pop("recommendation_summary")

    async def runner():
        return report

    with pytest.raises(ValidationError):
        await execute_investigation_action(
            {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
            runner=runner,
        )


@pytest.mark.asyncio
async def test_action_rejects_invalid_or_claimed_linkage_context():
    snapshot = _snapshot()
    snapshot["monitoring"]["pipeline_association_verified"] = True

    with pytest.raises(ValidationError):
        await execute_investigation_action(
            {"action": INVESTIGATION_ACTION, "health_snapshot": snapshot},
            runner=_report,
        )


@pytest.mark.asyncio
async def test_action_failure_remains_explicit():
    async def failed_runner():
        raise RuntimeError("investigation failed")

    with pytest.raises(RuntimeError, match="investigation failed"):
        await execute_investigation_action(
            {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
            runner=failed_runner,
        )


@pytest.mark.asyncio
async def test_adapter_routes_fixed_action_without_calling_agent(monkeypatch):
    calls = []

    async def action(payload):
        calls.append(payload)
        return {"output": [], "status": "completed"}

    async def forbidden_agent(*_args, **_kwargs):
        raise AssertionError("LLM agent must not be invoked")

    monkeypatch.setattr(adapter, "execute_investigation_action", action)
    monkeypatch.setattr(adapter, "_invoke_agent", forbidden_agent)
    context = SimpleNamespace(session_id="session-1", invocation_id="inv-1")
    payload = {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()}

    result = await adapter.invoke(payload, context)

    assert result["status"] == "completed"
    assert calls == [payload]


@pytest.mark.asyncio
async def test_snapshot_fingerprint_is_deterministic_for_retry_idempotency():
    async def runner():
        return await _report()

    payload = {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()}
    first = await execute_investigation_action(payload, runner=runner)
    second = await execute_investigation_action(payload, runner=runner)

    assert first["output"][0]["snapshot_fingerprint"] == second["output"][0]["snapshot_fingerprint"]


def test_existing_invocation_endpoint_enforces_deployed_user_auth(monkeypatch):
    async def handler(request, context):
        return {"output": request, "session_id": context.session_id}

    monkeypatch.setenv("DATABRICKS_APP_NAME", "test-app")
    app = DurableAgentServer(
        runtime_store=InMemoryRuntimeStore(),
        auth_policy=InvocationAuthPolicy(user_required=True),
    )
    app.invoke(handler)
    app.recover(handler)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/api/invocations",
            json={
                "id": "00000000-0000-4000-8000-000000000001",
                "session_id": "session-1",
                "input": {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
            },
        )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "MCP_USER_AUTH_REQUIRED"


def test_runtime_store_deduplicates_repeated_background_invocation_id():
    calls = 0

    async def handler(request, context):
        nonlocal calls
        calls += 1
        return {"output": request, "session_id": context.session_id}

    app = DurableAgentServer(runtime_store=InMemoryRuntimeStore())
    app.invoke(handler)
    app.recover(handler)
    request = {
        "id": "00000000-0000-4000-8000-000000000002",
        "session_id": "session-1",
        "input": {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
        "background": True,
    }
    with TestClient(app) as client:
        first = client.post("/api/invocations", json=request)
        second = client.post("/api/invocations", json=request)
        completed = client.get(f"/api/invocations/{request['id']}")

    assert first.status_code == 202
    assert second.status_code == 202
    assert completed.json()["status"] == "completed"
    assert calls == 1


def test_authenticated_invocation_status_is_namespaced_to_user(monkeypatch):
    async def handler(request, context):
        return {"output": request, "session_id": context.session_id}

    monkeypatch.setenv("DATABRICKS_APP_NAME", "test-app")
    app = DurableAgentServer(
        runtime_store=InMemoryRuntimeStore(),
        auth_policy=InvocationAuthPolicy(user_required=True),
    )
    app.invoke(handler)
    app.recover(handler)
    invocation_id = "00000000-0000-4000-8000-000000000003"
    request = {
        "id": invocation_id,
        "session_id": "session-1",
        "input": {"action": INVESTIGATION_ACTION, "health_snapshot": _snapshot()},
        "background": True,
    }
    alice = {
        "X-Forwarded-Access-Token": "alice-token",
        "X-Forwarded-User": "alice",
    }
    bob = {
        "X-Forwarded-Access-Token": "bob-token",
        "X-Forwarded-User": "bob",
    }
    with TestClient(app, raise_server_exceptions=False) as client:
        started = client.post("/api/invocations", json=request, headers=alice)
        alice_result = client.get(f"/api/invocations/{invocation_id}", headers=alice)
        bob_result = client.get(f"/api/invocations/{invocation_id}", headers=bob)

    assert started.status_code == 202
    assert alice_result.status_code == 200
    assert alice_result.json()["status"] == "completed"
    assert bob_result.status_code == 404
