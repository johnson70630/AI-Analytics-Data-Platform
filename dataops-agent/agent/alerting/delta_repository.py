"""Unity Catalog Delta adapter using only fixed parameterized statements."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Protocol
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementParameterListItem,
)

from agent.alerting.models import (
    LifecycleSnapshot,
    NotificationEmailPayload,
    NotificationOutboxRecord,
    PersistenceBundle,
)
from agent.alerting.repository import (
    ConcurrentStateUpdateError,
    DurableIncidentRegistry,
)
from agent.tools.observability import _resolve_warehouse_id, _workspace_client


CATALOG = "ai_analytics"
PRODUCTION_SCHEMA = "dataops_alerting"
DEVELOPMENT_SCHEMA = "dataops_alerting_dev"
SCHEMA = PRODUCTION_SCHEMA
CHECKPOINT_TABLE = f"{CATALOG}.{SCHEMA}.monitor_checkpoints"
EPISODE_TABLE = f"{CATALOG}.{SCHEMA}.incident_episodes"
OBSERVATION_TABLE = f"{CATALOG}.{SCHEMA}.incident_observations"
OUTBOX_TABLE = f"{CATALOG}.{SCHEMA}.notification_outbox"


_SELECT_CHECKPOINT = f"""
SELECT snapshot_json
FROM {CHECKPOINT_TABLE}
WHERE checkpoint_name = :checkpoint_name
LIMIT 1
"""

_ENSURE_CHECKPOINT = f"""
MERGE INTO {CHECKPOINT_TABLE} AS target
USING (SELECT :checkpoint_name AS checkpoint_name) AS source
ON target.checkpoint_name = source.checkpoint_name
WHEN NOT MATCHED THEN INSERT (
  checkpoint_name, snapshot_version, snapshot_json, updated_at
) VALUES (
  :checkpoint_name, 0, :snapshot_json, CAST(:updated_at AS TIMESTAMP)
)
"""

_CLAIM_CHECKPOINT = f"""
UPDATE {CHECKPOINT_TABLE}
SET lease_owner = :lease_owner,
    lease_token = :lease_token,
    lease_expires_at = CAST(:lease_expires_at AS TIMESTAMP),
    updated_at = CAST(:updated_at AS TIMESTAMP)
WHERE checkpoint_name = :checkpoint_name
  AND snapshot_version = CAST(:expected_version AS BIGINT)
  AND (lease_expires_at IS NULL OR lease_expires_at <= CAST(:updated_at AS TIMESTAMP))
"""

_VERIFY_CHECKPOINT_LEASE = f"""
SELECT lease_owner, lease_token, lease_expires_at
FROM {CHECKPOINT_TABLE}
WHERE checkpoint_name = :checkpoint_name
  AND snapshot_version = CAST(:expected_version AS BIGINT)
LIMIT 1
"""

_MERGE_OBSERVATION = f"""
MERGE INTO {OBSERVATION_TABLE} AS target
USING (
  SELECT :observation_id AS observation_id
  WHERE EXISTS (
    SELECT 1 FROM {CHECKPOINT_TABLE}
    WHERE checkpoint_name = :checkpoint_name
      AND snapshot_version = CAST(:expected_version AS BIGINT)
      AND lease_token = :lease_token
      AND lease_expires_at > CAST(:fence_time AS TIMESTAMP)
  )
) AS source
ON target.observation_id = source.observation_id
WHEN NOT MATCHED THEN INSERT (
  observation_id, fingerprint_digest, episode_id, source, category, check_identity,
  observation_status, observed_at, reference_time, synthetic_reference_date,
  execution_run_id, association_status, association_reason, decision,
  would_investigate, would_notify, evidence_json, recorded_at
) VALUES (
  :observation_id, :fingerprint_digest, :episode_id, :source, :category, :check_identity,
  :observation_status, CAST(:observed_at AS TIMESTAMP), CAST(:reference_time AS TIMESTAMP),
  :synthetic_reference_date, CAST(:execution_run_id AS BIGINT), :association_status,
  :association_reason, :decision, CAST(:would_investigate AS BOOLEAN),
  CAST(:would_notify AS BOOLEAN), :evidence_json, CAST(:recorded_at AS TIMESTAMP)
)
"""

_MERGE_EPISODE = f"""
MERGE INTO {EPISODE_TABLE} AS target
USING (
  SELECT :episode_id AS episode_id
  WHERE EXISTS (
    SELECT 1 FROM {CHECKPOINT_TABLE}
    WHERE checkpoint_name = :checkpoint_name
      AND snapshot_version = CAST(:expected_version AS BIGINT)
      AND lease_token = :lease_token
      AND lease_expires_at > CAST(:fence_time AS TIMESTAMP)
  )
) AS source
ON target.episode_id = source.episode_id
WHEN MATCHED AND target.version <= CAST(:version AS BIGINT) THEN UPDATE SET
  lifecycle_status = :lifecycle_status,
  last_seen_at = CAST(:last_seen_at AS TIMESTAMP),
  resolved_at = CAST(:resolved_at AS TIMESTAMP),
  last_status = :last_status,
  last_observation_id = :last_observation_id,
  observation_count = CAST(:observation_count AS BIGINT),
  consecutive_passes = CAST(:consecutive_passes AS INT),
  version = CAST(:version AS BIGINT),
  updated_at = CAST(:updated_at AS TIMESTAMP)
WHEN NOT MATCHED THEN INSERT (
  episode_id, fingerprint_digest, source, category, check_identity, episode_number,
  lifecycle_status, first_seen_at, episode_started_at, last_seen_at, resolved_at,
  last_status, last_observation_id, observation_count, consecutive_passes,
  version, created_at, updated_at
) VALUES (
  :episode_id, :fingerprint_digest, :source, :category, :check_identity,
  CAST(:episode_number AS INT), :lifecycle_status, CAST(:first_seen_at AS TIMESTAMP),
  CAST(:episode_started_at AS TIMESTAMP), CAST(:last_seen_at AS TIMESTAMP),
  CAST(:resolved_at AS TIMESTAMP), :last_status, :last_observation_id,
  CAST(:observation_count AS BIGINT), CAST(:consecutive_passes AS INT),
  CAST(:version AS BIGINT), CAST(:created_at AS TIMESTAMP), CAST(:updated_at AS TIMESTAMP)
)
"""

_MERGE_OUTBOX = f"""
MERGE INTO {OUTBOX_TABLE} AS target
USING (
  SELECT :notification_id AS notification_id
  WHERE EXISTS (
    SELECT 1 FROM {CHECKPOINT_TABLE}
    WHERE checkpoint_name = :checkpoint_name
      AND snapshot_version = CAST(:expected_version AS BIGINT)
      AND lease_token = :lease_token
      AND lease_expires_at > CAST(:fence_time AS TIMESTAMP)
  )
) AS source
ON target.notification_id = source.notification_id
WHEN MATCHED AND target.committed = FALSE THEN UPDATE SET
  episode_id = :episode_id,
  checkpoint_name = :checkpoint_name,
  checkpoint_version = CAST(:checkpoint_version AS BIGINT),
  transition_id = :transition_id,
  committed = FALSE,
  event_type = :event_type,
  status = :status,
  payload_json = :payload_json,
  attempt_count = CAST(:attempt_count AS INT),
  max_attempts = CAST(:max_attempts AS INT),
  available_at = CAST(:available_at AS TIMESTAMP),
  claimed_by = NULL,
  claim_token = NULL,
  lease_expires_at = NULL,
  last_error = NULL,
  created_at = CAST(:created_at AS TIMESTAMP),
  updated_at = CAST(:updated_at AS TIMESTAMP),
  sent_at = NULL,
  provider_name = NULL,
  provider_message_id = NULL,
  provider_status = NULL
WHEN NOT MATCHED THEN INSERT (
  notification_id, episode_id, checkpoint_name, checkpoint_version, transition_id, committed,
  event_type, status, payload_json, attempt_count,
  max_attempts, available_at, created_at, updated_at
) VALUES (
  :notification_id, :episode_id, :checkpoint_name, CAST(:checkpoint_version AS BIGINT),
  :transition_id, FALSE,
  :event_type, :status, :payload_json,
  CAST(:attempt_count AS INT), CAST(:max_attempts AS INT),
  CAST(:available_at AS TIMESTAMP), CAST(:created_at AS TIMESTAMP),
  CAST(:updated_at AS TIMESTAMP)
)
"""

_FINALIZE_CHECKPOINT = f"""
UPDATE {CHECKPOINT_TABLE}
SET snapshot_version = CAST(:snapshot_version AS BIGINT),
    last_transition_id = :transition_id,
    run_completed_at = CAST(:run_completed_at AS TIMESTAMP),
    run_id = CAST(:run_id AS BIGINT),
    snapshot_json = :snapshot_json,
    lease_owner = NULL,
    lease_token = NULL,
    lease_expires_at = NULL,
    updated_at = CAST(:updated_at AS TIMESTAMP)
WHERE checkpoint_name = :checkpoint_name
  AND snapshot_version = CAST(:expected_version AS BIGINT)
  AND lease_token = :lease_token
  AND lease_expires_at > CAST(:fence_time AS TIMESTAMP)
"""

_VERIFY_FINALIZED_CHECKPOINT = f"""
SELECT snapshot_version, snapshot_json, last_transition_id, lease_token
FROM {CHECKPOINT_TABLE}
WHERE checkpoint_name = :checkpoint_name
LIMIT 1
"""

_ACTIVATE_COMMITTED_OUTBOX = f"""
UPDATE {OUTBOX_TABLE} AS outbox
SET committed = TRUE,
    updated_at = CAST(:activated_at AS TIMESTAMP)
WHERE outbox.checkpoint_name = :checkpoint_name
  AND outbox.committed = FALSE
  AND EXISTS (
    SELECT 1 FROM {CHECKPOINT_TABLE} AS checkpoint
    WHERE checkpoint.checkpoint_name = outbox.checkpoint_name
      AND checkpoint.snapshot_version = outbox.checkpoint_version
      AND checkpoint.last_transition_id = outbox.transition_id
  )
"""

_CLAIM_OUTBOX = f"""
MERGE INTO {OUTBOX_TABLE} AS target
USING (
  SELECT notification_id
  FROM {OUTBOX_TABLE} AS candidate
  WHERE candidate.checkpoint_name = :checkpoint_name
    AND candidate.attempt_count < candidate.max_attempts
    AND candidate.committed = TRUE
    AND candidate.available_at <= CAST(:claimed_at AS TIMESTAMP)
    AND EXISTS (
      SELECT 1 FROM {CHECKPOINT_TABLE} AS checkpoint
      WHERE checkpoint.checkpoint_name = candidate.checkpoint_name
        AND checkpoint.snapshot_version >= candidate.checkpoint_version
    )
    AND (
      candidate.status IN ('PENDING', 'RETRYABLE')
      OR (
        candidate.status = 'CLAIMED'
        AND candidate.lease_expires_at <= CAST(:claimed_at AS TIMESTAMP)
      )
    )
  ORDER BY candidate.available_at, candidate.created_at, candidate.notification_id
  LIMIT CAST(:claim_limit AS INT)
) AS source
ON target.notification_id = source.notification_id
WHEN MATCHED AND target.checkpoint_name = :checkpoint_name
  AND target.attempt_count < target.max_attempts
  AND target.committed = TRUE
  AND target.available_at <= CAST(:claimed_at AS TIMESTAMP)
  AND (
    target.status IN ('PENDING', 'RETRYABLE')
    OR (target.status = 'CLAIMED' AND target.lease_expires_at <= CAST(:claimed_at AS TIMESTAMP))
  )
THEN UPDATE SET
  status = 'CLAIMED',
  claimed_by = :worker_id,
  claim_token = :claim_token,
  lease_expires_at = CAST(:lease_expires_at AS TIMESTAMP),
  attempt_count = target.attempt_count + 1,
  updated_at = CAST(:claimed_at AS TIMESTAMP)
"""

_SELECT_CLAIMED_OUTBOX = f"""
SELECT notification_id, episode_id, event_type, status, payload_json, attempt_count,
       max_attempts, available_at, claimed_by, claim_token, lease_expires_at,
       last_error, created_at, updated_at, sent_at,
       provider_name, provider_message_id, provider_status
FROM {OUTBOX_TABLE}
WHERE claim_token = :claim_token AND claimed_by = :worker_id AND status = 'CLAIMED'
ORDER BY notification_id
"""

_MARK_OUTBOX_SENT = f"""
UPDATE {OUTBOX_TABLE}
SET status = 'SENT', sent_at = CAST(:sent_at AS TIMESTAMP),
    provider_name = :provider_name, provider_message_id = :provider_message_id,
    provider_status = :provider_status,
    claimed_by = NULL, claim_token = NULL, lease_expires_at = NULL,
    updated_at = CAST(:sent_at AS TIMESTAMP)
WHERE notification_id = :notification_id AND claim_token = :claim_token AND status = 'CLAIMED'
  AND lease_expires_at > CAST(:sent_at AS TIMESTAMP)
"""

_MARK_OUTBOX_FAILED = f"""
UPDATE {OUTBOX_TABLE}
SET status = CASE
      WHEN CAST(:retryable AS BOOLEAN) AND attempt_count < max_attempts THEN 'RETRYABLE'
      ELSE 'FAILED'
    END,
    available_at = CAST(:retry_at AS TIMESTAMP), last_error = :error,
    claimed_by = NULL, claim_token = NULL, lease_expires_at = NULL,
    updated_at = CAST(:failed_at AS TIMESTAMP)
WHERE notification_id = :notification_id AND claim_token = :claim_token AND status = 'CLAIMED'
  AND lease_expires_at > CAST(:failed_at AS TIMESTAMP)
"""

_SELECT_OUTBOX_BY_ID = f"""
SELECT notification_id, episode_id, event_type, status, payload_json, attempt_count,
       max_attempts, available_at, claimed_by, claim_token, lease_expires_at,
       last_error, created_at, updated_at, sent_at,
       provider_name, provider_message_id, provider_status
FROM {OUTBOX_TABLE}
WHERE notification_id = :notification_id
LIMIT 1
"""


@dataclass(frozen=True)
class SqlExecutionResult:
    rows: list[dict[str, Any]]
    affected_row_count: int | None = None


class FixedStatementExecutor(Protocol):
    def execute(self, *, name: str, statement: str, parameters: dict[str, Any]) -> SqlExecutionResult:
        """Execute one adapter-owned statement; callers cannot supply SQL."""


def _parameter_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _is_delta_concurrency_error(error: object) -> bool:
    text = " ".join(
        str(value)
        for value in (
            getattr(error, "error_code", None),
            getattr(error, "message", None),
            error,
        )
        if value
    ).lower()
    return any(
        marker in text
        for marker in (
            "concurrent",
            "metadatachangedexception",
            "protocolchangedexception",
        )
    )


TargetEnvironment = Literal["development", "production"]


def _target_schema(target: TargetEnvironment) -> str:
    if target == "development":
        return DEVELOPMENT_SCHEMA
    if target == "production":
        return PRODUCTION_SCHEMA
    raise ValueError(f"unsupported alerting target: {target!r}")


def _target_statement(statement: str, target: TargetEnvironment) -> str:
    schema = _target_schema(target)
    return statement.replace(
        f"{CATALOG}.{PRODUCTION_SCHEMA}.",
        f"{CATALOG}.{schema}.",
    )


class DatabricksFixedStatementExecutor:
    """Thin Statement Execution API adapter for fixed SQL owned by this module."""

    def __init__(
        self,
        *,
        client: WorkspaceClient | None = None,
        warehouse_id: str | None = None,
        target: TargetEnvironment = "development",
    ) -> None:
        self._client = client or _workspace_client()
        self._warehouse_id = _resolve_warehouse_id(warehouse_id)
        self.target = target
        self._schema = _target_schema(target)

    def execute(self, *, name: str, statement: str, parameters: dict[str, Any]) -> SqlExecutionResult:
        del name
        response = self._client.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            catalog=CATALOG,
            schema=self._schema,
            statement=statement,
            parameters=[
                StatementParameterListItem(name=key, type="STRING", value=_parameter_value(value))
                for key, value in parameters.items()
            ],
            wait_timeout="30s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
            row_limit=1000,
        )
        state = getattr(getattr(response, "status", None), "state", None)
        state_value = getattr(state, "value", state)
        if str(state_value) != "SUCCEEDED":
            error = getattr(getattr(response, "status", None), "error", None)
            if _is_delta_concurrency_error(error):
                raise ConcurrentStateUpdateError(
                    getattr(error, "message", None) or "Delta concurrent write conflict"
                )
            raise RuntimeError(
                f"Databricks SQL statement failed: state={state_value}, "
                f"message={getattr(error, 'message', None)}"
            )
        columns = [
            column.name
            for column in getattr(getattr(getattr(response, "manifest", None), "schema", None), "columns", []) or []
        ]
        result = getattr(response, "result", None)
        rows = [dict(zip(columns, row)) for row in getattr(result, "data_array", []) or []]
        # Statement Execution row_count describes result-chunk rows, not DML rows affected.
        # Mutation callers verify the resulting state with a separate fixed SELECT.
        return SqlExecutionResult(rows=rows, affected_row_count=None)


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class DeltaIncidentRegistry(DurableIncidentRegistry):
    """Explicitly enabled Delta registry; construction alone performs no SQL."""

    def __init__(
        self,
        executor: FixedStatementExecutor,
        *,
        checkpoint_name: str = "dataops-agent",
        write_enabled: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        checkpoint_lease_seconds: int = 120,
        max_checkpoint_bytes: int = 1_000_000,
        target: TargetEnvironment = "development",
    ) -> None:
        self._executor = executor
        self._checkpoint_name = checkpoint_name
        self._write_enabled = write_enabled
        self._clock = clock
        self._checkpoint_lease_seconds = checkpoint_lease_seconds
        self._max_checkpoint_bytes = max_checkpoint_bytes
        self._target = target
        executor_target = getattr(executor, "target", target)
        if executor_target != target:
            raise ValueError(
                f"executor target {executor_target!r} does not match registry target {target!r}"
            )
        _target_schema(target)

    def _sql(self, statement: str) -> str:
        return _target_statement(statement, self._target)

    def _require_writes(self) -> None:
        if not self._write_enabled:
            raise RuntimeError("Delta persistence is disabled; use explicit persist mode to enable it")

    @staticmethod
    def _validate_bundle_uniqueness(bundle: PersistenceBundle) -> None:
        identities = {
            "observation_id": [item.observation.observation_id for item in bundle.observations],
            "episode_id": [item.episode_id for item in bundle.episodes],
            "notification_id": [item.notification_id for item in bundle.notifications],
        }
        for identity_name, values in identities.items():
            if len(values) != len(set(values)):
                raise ValueError(f"persistence bundle contains duplicate {identity_name} values")

    def load(self) -> LifecycleSnapshot:
        result = self._executor.execute(
            name="select_checkpoint",
            statement=self._sql(_SELECT_CHECKPOINT),
            parameters={"checkpoint_name": self._checkpoint_name},
        )
        if not result.rows:
            return LifecycleSnapshot()
        return LifecycleSnapshot.model_validate_json(result.rows[0]["snapshot_json"])

    def save(self, snapshot: LifecycleSnapshot, *, expected_version: int) -> None:
        raise NotImplementedError("use persist_transition so the checkpoint advances last")

    def persist_transition(
        self,
        bundle: PersistenceBundle,
        *,
        expected_version: int,
        lease_owner: str,
    ) -> None:
        self._require_writes()
        if bundle.checkpoint.checkpoint_name != self._checkpoint_name:
            raise ValueError("bundle checkpoint_name does not match repository checkpoint")
        if bundle.checkpoint.snapshot_version != expected_version + 1:
            raise ValueError("bundle snapshot version must advance exactly once")
        checkpoint_size = len(bundle.checkpoint.snapshot_json.encode("utf-8"))
        if checkpoint_size > self._max_checkpoint_bytes:
            raise ValueError(
                f"checkpoint snapshot is {checkpoint_size} bytes; limit is {self._max_checkpoint_bytes}"
            )
        self._validate_bundle_uniqueness(bundle)
        now = self._clock()
        lease_token = str(uuid4())
        empty_snapshot = LifecycleSnapshot().model_dump_json()
        common = {
            "checkpoint_name": self._checkpoint_name,
            "updated_at": now,
        }
        self._executor.execute(
            name="ensure_checkpoint",
            statement=self._sql(_ENSURE_CHECKPOINT),
            parameters={**common, "snapshot_json": empty_snapshot},
        )
        self._activate_committed_outbox()
        self._executor.execute(
            name="claim_checkpoint",
            statement=self._sql(_CLAIM_CHECKPOINT),
            parameters={
                **common,
                "lease_owner": lease_owner,
                "lease_token": lease_token,
                "lease_expires_at": now + timedelta(seconds=self._checkpoint_lease_seconds),
                "expected_version": expected_version,
            },
        )
        verification = self._executor.execute(
            name="verify_checkpoint_lease",
            statement=self._sql(_VERIFY_CHECKPOINT_LEASE),
            parameters={
                "checkpoint_name": self._checkpoint_name,
                "expected_version": expected_version,
            },
        )
        lease_row = verification.rows[0] if verification.rows else None
        lease_expires_at = _parse_timestamp(
            lease_row.get("lease_expires_at") if lease_row else None
        )
        if (
            not lease_row
            or lease_row.get("lease_token") != lease_token
            or lease_row.get("lease_owner") != lease_owner
            or lease_expires_at is None
            or lease_expires_at <= now
        ):
            raise ConcurrentStateUpdateError("checkpoint lease was not acquired")

        for item in bundle.observations:
            observation = item.observation
            self._executor.execute(
                name="merge_observation",
                statement=self._sql(_MERGE_OBSERVATION),
                parameters={
                    "checkpoint_name": self._checkpoint_name,
                    "expected_version": expected_version,
                    "lease_token": lease_token,
                    "fence_time": self._clock(),
                    "observation_id": observation.observation_id,
                    "fingerprint_digest": observation.fingerprint.digest,
                    "episode_id": item.episode_id,
                    "source": observation.fingerprint.source,
                    "category": observation.fingerprint.category,
                    "check_identity": observation.fingerprint.identity,
                    "observation_status": observation.status,
                    "observed_at": observation.observed_at,
                    "reference_time": observation.reference_time,
                    "synthetic_reference_date": observation.synthetic_reference_date,
                    "execution_run_id": observation.execution_run_id,
                    "association_status": observation.association.status,
                    "association_reason": observation.association.reason,
                    "decision": item.decision,
                    "would_investigate": item.would_investigate,
                    "would_notify": item.would_notify,
                    "evidence_json": json.dumps(observation.evidence, sort_keys=True, default=str),
                    "recorded_at": item.recorded_at,
                },
            )
        for item in bundle.episodes:
            self._executor.execute(
                name="merge_episode",
                statement=self._sql(_MERGE_EPISODE),
                parameters={
                    "checkpoint_name": self._checkpoint_name,
                    "expected_version": expected_version,
                    "lease_token": lease_token,
                    "fence_time": self._clock(),
                    "episode_id": item.episode_id,
                    "fingerprint_digest": item.fingerprint.digest,
                    "source": item.fingerprint.source,
                    "category": item.fingerprint.category,
                    "check_identity": item.fingerprint.identity,
                    "episode_number": item.episode_number,
                    "lifecycle_status": item.lifecycle_status,
                    "first_seen_at": item.first_seen_at,
                    "episode_started_at": item.episode_started_at,
                    "last_seen_at": item.last_seen_at,
                    "resolved_at": item.resolved_at,
                    "last_status": item.last_status,
                    "last_observation_id": item.last_observation_id,
                    "observation_count": item.observation_count,
                    "consecutive_passes": item.consecutive_passes,
                    "version": item.version,
                    "created_at": item.first_seen_at,
                    "updated_at": bundle.checkpoint.updated_at,
                },
            )
        for item in bundle.notifications:
            self._executor.execute(
                name="merge_outbox",
                statement=self._sql(_MERGE_OUTBOX),
                parameters={
                    "notification_id": item.notification_id,
                    "episode_id": item.episode_id,
                    "checkpoint_name": self._checkpoint_name,
                    "checkpoint_version": bundle.checkpoint.snapshot_version,
                    "transition_id": bundle.checkpoint.transition_id,
                    "expected_version": expected_version,
                    "lease_token": lease_token,
                    "fence_time": self._clock(),
                    "event_type": item.event_type,
                    "status": item.status,
                    "payload_json": item.payload.model_dump_json(),
                    "attempt_count": item.attempt_count,
                    "max_attempts": item.max_attempts,
                    "available_at": item.available_at,
                    "created_at": item.created_at,
                    "updated_at": item.updated_at,
                },
            )
        finalized = self._executor.execute(
            name="finalize_checkpoint",
            statement=self._sql(_FINALIZE_CHECKPOINT),
            parameters={
                "checkpoint_name": self._checkpoint_name,
                "snapshot_version": bundle.checkpoint.snapshot_version,
                "transition_id": bundle.checkpoint.transition_id,
                "run_completed_at": (
                    bundle.checkpoint.run_cursor.completed_at
                    if bundle.checkpoint.run_cursor
                    else None
                ),
                "run_id": bundle.checkpoint.run_cursor.run_id if bundle.checkpoint.run_cursor else None,
                "snapshot_json": bundle.checkpoint.snapshot_json,
                "updated_at": bundle.checkpoint.updated_at,
                "expected_version": expected_version,
                "lease_token": lease_token,
                "fence_time": self._clock(),
            },
        )
        if finalized.affected_row_count == 0:
            raise ConcurrentStateUpdateError("checkpoint finalization lost its lease")
        if finalized.affected_row_count is None:
            verification = self._executor.execute(
                name="verify_finalized_checkpoint",
                statement=self._sql(_VERIFY_FINALIZED_CHECKPOINT),
                parameters={"checkpoint_name": self._checkpoint_name},
            )
            if (
                not verification.rows
                or int(verification.rows[0]["snapshot_version"])
                != bundle.checkpoint.snapshot_version
                or verification.rows[0].get("snapshot_json")
                != bundle.checkpoint.snapshot_json
                or verification.rows[0].get("last_transition_id")
                != bundle.checkpoint.transition_id
                or verification.rows[0].get("lease_token") not in (None, "")
            ):
                raise ConcurrentStateUpdateError("checkpoint finalization could not be verified")
        self._activate_committed_outbox()

    def _activate_committed_outbox(self) -> None:
        self._executor.execute(
            name="activate_committed_outbox",
            statement=self._sql(_ACTIVATE_COMMITTED_OUTBOX),
            parameters={
                "checkpoint_name": self._checkpoint_name,
                "activated_at": self._clock(),
            },
        )

    def claim(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_seconds: int,
        limit: int,
    ) -> list[NotificationOutboxRecord]:
        self._require_writes()
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if not 1 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 1 and 3600")
        self._activate_committed_outbox()
        claim_token = str(uuid4())
        self._executor.execute(
            name="claim_outbox",
            statement=self._sql(_CLAIM_OUTBOX),
            parameters={
                "checkpoint_name": self._checkpoint_name,
                "worker_id": worker_id,
                "claim_token": claim_token,
                "claimed_at": now,
                "lease_expires_at": now + timedelta(seconds=lease_seconds),
                "claim_limit": limit,
            },
        )
        result = self._executor.execute(
            name="select_claimed_outbox",
            statement=self._sql(_SELECT_CLAIMED_OUTBOX),
            parameters={"worker_id": worker_id, "claim_token": claim_token},
        )
        return [self._outbox_record(row) for row in result.rows]

    def mark_sent(
        self,
        *,
        notification_id: str,
        claim_token: str,
        sent_at: datetime,
        provider_name: str | None = None,
        provider_message_id: str | None = None,
        provider_status: str | None = None,
    ) -> None:
        self._require_writes()
        result = self._executor.execute(
            name="mark_outbox_sent",
            statement=self._sql(_MARK_OUTBOX_SENT),
            parameters={
                "notification_id": notification_id,
                "claim_token": claim_token,
                "sent_at": sent_at,
                "provider_name": provider_name,
                "provider_message_id": provider_message_id,
                "provider_status": provider_status,
            },
        )
        if result.affected_row_count == 0:
            raise ConcurrentStateUpdateError("notification delivery lease was lost before SENT")
        if result.affected_row_count is None:
            verification = self._executor.execute(
                name="select_outbox_by_id",
                statement=self._sql(_SELECT_OUTBOX_BY_ID),
                parameters={"notification_id": notification_id},
            )
            if not verification.rows or verification.rows[0].get("status") != "SENT":
                raise ConcurrentStateUpdateError("notification SENT state could not be verified")

    def mark_failed(
        self,
        *,
        notification_id: str,
        claim_token: str,
        error: str,
        failed_at: datetime,
        retry_at: datetime,
        retryable: bool,
    ) -> NotificationOutboxRecord:
        self._require_writes()
        update = self._executor.execute(
            name="mark_outbox_failed",
            statement=self._sql(_MARK_OUTBOX_FAILED),
            parameters={
                "notification_id": notification_id,
                "claim_token": claim_token,
                "error": error,
                "failed_at": failed_at,
                "retry_at": retry_at,
                "retryable": retryable,
            },
        )
        if update.affected_row_count == 0:
            raise ConcurrentStateUpdateError("notification delivery lease was lost before failure update")
        result = self._executor.execute(
            name="select_outbox_by_id",
            statement=self._sql(_SELECT_OUTBOX_BY_ID),
            parameters={"notification_id": notification_id},
        )
        if not result.rows:
            raise RuntimeError("notification disappeared after failure update")
        record = self._outbox_record(result.rows[0])
        if record.status not in {"RETRYABLE", "FAILED"}:
            raise ConcurrentStateUpdateError("notification failure state could not be verified")
        return record

    @staticmethod
    def _outbox_record(row: dict[str, Any]) -> NotificationOutboxRecord:
        return NotificationOutboxRecord(
            notification_id=row["notification_id"],
            episode_id=row["episode_id"],
            event_type=row["event_type"],
            status=row["status"],
            payload=NotificationEmailPayload.model_validate_json(row["payload_json"]),
            attempt_count=int(row["attempt_count"]),
            max_attempts=int(row["max_attempts"]),
            available_at=_parse_timestamp(row["available_at"]),
            claimed_by=row.get("claimed_by"),
            claim_token=row.get("claim_token"),
            lease_expires_at=_parse_timestamp(row.get("lease_expires_at")),
            last_error=row.get("last_error"),
            created_at=_parse_timestamp(row["created_at"]),
            updated_at=_parse_timestamp(row["updated_at"]),
            sent_at=_parse_timestamp(row.get("sent_at")),
            provider_name=row.get("provider_name"),
            provider_message_id=row.get("provider_message_id"),
            provider_status=row.get("provider_status"),
        )
