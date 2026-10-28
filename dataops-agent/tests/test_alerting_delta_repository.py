from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.alerting import (
    ConcurrentStateUpdateError,
    DatabricksFixedStatementExecutor,
    DeltaIncidentRegistry,
    DetectionResult,
    EvidenceAssociation,
    LifecycleSnapshot,
    MonitoringObservation,
    SqlExecutionResult,
    incident_fingerprint,
    transition_lifecycle,
)
from agent.alerting.outbox import build_outbox_record
from agent.alerting.persistence import build_persistence_bundle
from agent.alerting.delta_repository import _is_delta_concurrency_error


NOW = datetime(2026, 10, 27, 13, 0, tzinfo=timezone.utc)


class SqlError:
    def __init__(self, error_code, message):
        self.error_code = error_code
        self.message = message


class StatementApi:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def execute_statement(self, **kwargs):
        self.requests.append(kwargs)
        return self.response


class Workspace:
    def __init__(self, response):
        self.statement_execution = StatementApi(response)


def persistence_bundle():
    fingerprint = incident_fingerprint(
        source="observability",
        category="quality",
        identity_parts=["payments", "failed_payment_rate"],
    )
    observation = MonitoringObservation(
        observation_id=hashlib.sha256(b"delta-observation").hexdigest(),
        fingerprint=fingerprint,
        status="WARN",
        observed_at=NOW,
        synthetic_reference_date="2026-09-01",
        association=EvidenceAssociation(
            status="UNCERTAIN",
            reason="category_has_no_monitoring_execution_timestamp",
            execution_run_id=456,
        ),
        evidence={"quality_status": "WARN", "metric_name": "failed_payment_rate"},
    )
    detection = DetectionResult(
        observations=[observation],
        source_availability={"jobs": True, "observability": True},
    )
    transition = transition_lifecycle(
        LifecycleSnapshot(initialized=True),
        detection,
        baseline_mode="process_existing",
    )
    outbox = build_outbox_record(
        transition.decisions[0],
        now=NOW,
        investigation_report=None,
        investigation_error="investigation unavailable",
    )
    return build_persistence_bundle(
        transition=transition,
        detection=detection,
        notifications=[outbox],
        checkpoint_name="dataops-agent",
        cursor=None,
        recorded_at=NOW,
    )


class FakeExecutor:
    def __init__(self, *, fail_once=None, wrong_lease=False, expired_lease=False):
        self.calls = []
        self.fail_once = fail_once
        self.failed = False
        self.wrong_lease = wrong_lease
        self.expired_lease = expired_lease
        self.lease_token = None
        self.lease_owner = None
        self.lease_expires_at = None

    def execute(self, *, name, statement, parameters):
        self.calls.append({"name": name, "statement": statement, "parameters": parameters})
        if name == "claim_checkpoint":
            self.lease_token = parameters["lease_token"]
            self.lease_owner = parameters["lease_owner"]
            self.lease_expires_at = parameters["lease_expires_at"]
        if name == self.fail_once and not self.failed:
            self.failed = True
            raise RuntimeError("simulated crash")
        if name == "verify_checkpoint_lease":
            token = "wrong-token" if self.wrong_lease else self.lease_token
            expires_at = NOW if self.expired_lease else self.lease_expires_at
            return SqlExecutionResult(
                rows=[
                    {
                        "lease_owner": self.lease_owner,
                        "lease_token": token,
                        "lease_expires_at": expires_at,
                    }
                ]
            )
        if name == "finalize_checkpoint":
            return SqlExecutionResult(rows=[], affected_row_count=1)
        if name == "select_checkpoint":
            return SqlExecutionResult(rows=[])
        return SqlExecutionResult(rows=[])


def test_delta_registry_is_write_disabled_by_default():
    executor = FakeExecutor()
    registry = DeltaIncidentRegistry(executor)

    with pytest.raises(RuntimeError, match="persistence is disabled"):
        registry.persist_transition(
            persistence_bundle(),
            expected_version=0,
            lease_owner="worker-1",
        )
    assert executor.calls == []


def test_delta_concurrency_errors_are_classified_for_processor_retry():
    assert _is_delta_concurrency_error(
        SqlError("DELTA_CONCURRENT_WRITE", "ConcurrentAppendException")
    )
    assert not _is_delta_concurrency_error(SqlError("PERMISSION_DENIED", "missing MODIFY"))


def test_statement_executor_uses_dev_schema_and_ignores_result_row_count_for_dml():
    response = SimpleNamespace(
        status=SimpleNamespace(state="SUCCEEDED", error=None),
        manifest=SimpleNamespace(
            schema=SimpleNamespace(columns=[SimpleNamespace(name="value")])
        ),
        result=SimpleNamespace(data_array=[["one"]], row_count=999),
    )
    workspace = Workspace(response)
    executor = DatabricksFixedStatementExecutor(
        client=workspace,
        warehouse_id="warehouse-123",
    )

    result = executor.execute(
        name="fixed_select",
        statement="SELECT :value",
        parameters={"value": 1},
    )

    assert result.rows == [{"value": "one"}]
    assert result.affected_row_count is None
    request = workspace.statement_execution.requests[0]
    assert request["catalog"] == "ai_analytics"
    assert request["schema"] == "dataops_alerting_dev"
    assert request["on_wait_timeout"].value == "CANCEL"


def test_statement_executor_maps_delta_conflict_to_retryable_error():
    response = SimpleNamespace(
        status=SimpleNamespace(
            state="FAILED",
            error=SqlError("DELTA_CONCURRENT_WRITE", "ConcurrentAppendException"),
        )
    )
    executor = DatabricksFixedStatementExecutor(
        client=Workspace(response),
        warehouse_id="warehouse-123",
    )

    with pytest.raises(ConcurrentStateUpdateError, match="ConcurrentAppendException"):
        executor.execute(name="fixed_update", statement="SELECT 1", parameters={})


def test_fixed_parameterized_statements_persist_checkpoint_last():
    executor = FakeExecutor()
    registry = DeltaIncidentRegistry(executor, write_enabled=True, clock=lambda: NOW)
    bundle = persistence_bundle()

    registry.persist_transition(bundle, expected_version=0, lease_owner="worker-1")

    names = [call["name"] for call in executor.calls]
    assert names == [
        "ensure_checkpoint",
        "activate_committed_outbox",
        "claim_checkpoint",
        "verify_checkpoint_lease",
        "merge_observation",
        "merge_episode",
        "merge_outbox",
        "finalize_checkpoint",
        "activate_committed_outbox",
    ]
    observation_call = next(call for call in executor.calls if call["name"] == "merge_observation")
    assert ":observation_id" in observation_call["statement"]
    assert bundle.observations[0].observation.observation_id not in observation_call["statement"]
    assert observation_call["parameters"]["evidence_json"]
    assert "lease_expires_at > CAST(:fence_time AS TIMESTAMP)" in observation_call["statement"]
    assert "ai_analytics.dataops_alerting_dev" in observation_call["statement"]
    outbox_call = next(call for call in executor.calls if call["name"] == "merge_outbox")
    assert outbox_call["parameters"]["transition_id"] == bundle.checkpoint.transition_id
    assert "target.committed = FALSE" in outbox_call["statement"]
    assert "transition_id, committed" in outbox_call["statement"]


def test_production_target_must_be_selected_explicitly():
    executor = FakeExecutor()
    registry = DeltaIncidentRegistry(
        executor,
        write_enabled=True,
        clock=lambda: NOW,
        target="production",
    )

    registry.persist_transition(persistence_bundle(), expected_version=0, lease_owner="worker-1")

    assert all(
        "ai_analytics.dataops_alerting_dev" not in call["statement"]
        for call in executor.calls
    )
    assert any(
        "ai_analytics.dataops_alerting.incident_observations" in call["statement"]
        for call in executor.calls
    )


def test_checkpoint_recovery_replays_idempotent_records_before_advancing():
    executor = FakeExecutor(fail_once="merge_episode")
    registry = DeltaIncidentRegistry(executor, write_enabled=True, clock=lambda: NOW)
    bundle = persistence_bundle()

    with pytest.raises(RuntimeError, match="simulated crash"):
        registry.persist_transition(bundle, expected_version=0, lease_owner="worker-1")
    assert "finalize_checkpoint" not in [call["name"] for call in executor.calls]

    registry.persist_transition(bundle, expected_version=0, lease_owner="worker-2")
    names = [call["name"] for call in executor.calls]
    assert names.count("merge_observation") == 2
    assert names[-2:] == ["finalize_checkpoint", "activate_committed_outbox"]
    assert names.count("finalize_checkpoint") == 1


def test_concurrent_processor_losing_checkpoint_lease_writes_no_incident_records():
    executor = FakeExecutor(wrong_lease=True)
    registry = DeltaIncidentRegistry(executor, write_enabled=True, clock=lambda: NOW)

    with pytest.raises(ConcurrentStateUpdateError, match="lease was not acquired"):
        registry.persist_transition(
            persistence_bundle(),
            expected_version=0,
            lease_owner="worker-1",
        )

    names = [call["name"] for call in executor.calls]
    assert names == [
        "ensure_checkpoint",
        "activate_committed_outbox",
        "claim_checkpoint",
        "verify_checkpoint_lease",
    ]


def test_expired_checkpoint_lease_writes_no_incident_records():
    executor = FakeExecutor(expired_lease=True)
    registry = DeltaIncidentRegistry(executor, write_enabled=True, clock=lambda: NOW)

    with pytest.raises(ConcurrentStateUpdateError, match="lease was not acquired"):
        registry.persist_transition(
            persistence_bundle(),
            expected_version=0,
            lease_owner="worker-1",
        )

    assert [call["name"] for call in executor.calls] == [
        "ensure_checkpoint",
        "activate_committed_outbox",
        "claim_checkpoint",
        "verify_checkpoint_lease",
    ]


def test_duplicate_bundle_identity_is_rejected_before_sql():
    executor = FakeExecutor()
    registry = DeltaIncidentRegistry(executor, write_enabled=True, clock=lambda: NOW)
    bundle = persistence_bundle()
    duplicate = bundle.model_copy(
        update={"observations": [bundle.observations[0], bundle.observations[0]]}
    )

    with pytest.raises(ValueError, match="duplicate observation_id"):
        registry.persist_transition(duplicate, expected_version=0, lease_owner="worker-1")

    assert executor.calls == []


def test_oversized_checkpoint_is_rejected_before_sql():
    executor = FakeExecutor()
    registry = DeltaIncidentRegistry(
        executor,
        write_enabled=True,
        clock=lambda: NOW,
        max_checkpoint_bytes=10,
    )

    with pytest.raises(ValueError, match="checkpoint snapshot"):
        registry.persist_transition(
            persistence_bundle(),
            expected_version=0,
            lease_owner="worker-1",
        )

    assert executor.calls == []


def test_outbox_claim_is_bounded_and_requires_committed_checkpoint():
    executor = FakeExecutor()
    registry = DeltaIncidentRegistry(executor, write_enabled=True, clock=lambda: NOW)

    assert registry.claim(
        worker_id="worker-1",
        now=NOW,
        lease_seconds=60,
        limit=10,
    ) == []

    activation_statement = executor.calls[0]["statement"]
    claim_statement = executor.calls[1]["statement"]
    assert "checkpoint.last_transition_id = outbox.transition_id" in activation_statement
    assert "checkpoint.snapshot_version >= candidate.checkpoint_version" in claim_statement
    assert "candidate.committed = TRUE" in claim_statement
    assert "candidate.checkpoint_name = :checkpoint_name" in claim_statement
    assert "ai_analytics.dataops_alerting_dev.notification_outbox" in claim_statement

    with pytest.raises(ValueError, match="between 1 and 100"):
        registry.claim(
            worker_id="worker-1",
            now=NOW,
            lease_seconds=60,
            limit=101,
        )


def test_migration_defines_all_tables_and_non_enforced_identifiers():
    migration = (
        Path(__file__).parents[1]
        / "agent"
        / "alerting"
        / "migrations"
        / "001_create_dataops_alerting.sql"
    ).read_text()

    for table in (
        "incident_episodes",
        "incident_observations",
        "monitor_checkpoints",
        "notification_outbox",
    ):
        assert f"ai_analytics.dataops_alerting.{table}" in migration
    assert migration.count("PRIMARY KEY") == 4
    assert migration.count("NOT ENFORCED") >= 4
    assert "GRANT " not in migration.upper()
    assert "checkpoint_name STRING NOT NULL" in migration
    assert "checkpoint_version BIGINT NOT NULL" in migration
    assert "last_transition_id STRING" in migration
    assert "transition_id STRING NOT NULL" in migration
    assert "committed BOOLEAN NOT NULL" in migration


def test_development_migration_is_isolated_and_contains_no_grants():
    migration = (
        Path(__file__).parents[1]
        / "agent"
        / "alerting"
        / "migrations"
        / "000_create_dataops_alerting_dev.sql"
    ).read_text()

    assert migration.count("ai_analytics.dataops_alerting_dev.") == 4
    assert "CREATE SCHEMA IF NOT EXISTS ai_analytics.dataops_alerting_dev" in migration
    assert "ai_analytics.dataops_alerting." not in migration
    assert "GRANT " not in migration.upper()
