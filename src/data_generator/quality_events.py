"""Sparse feedback and operational error generation with source lineage."""

import random
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .inference_data import MetricHistogram, choose_model, load_existing_models
from .ingestion import (
    ingestion_timestamp,
    iter_local_partitioned_records,
    iter_s3_partitioned_records,
    remove_local_event_outputs,
)
from .utils import generate_uuid
from .writers import PartitionedNDJSONWriter


FEEDBACK_FIELDS = {
    "feedback_id",
    "completion_id",
    "user_id",
    "feedback_type",
    "feedback_score",
    "created_at",
    "ingested_at",
}
ERROR_FIELDS = {
    "error_id",
    "user_id",
    "conversation_id",
    "message_id",
    "completion_id",
    "inference_id",
    "model_id",
    "error_source",
    "error_type",
    "error_code",
    "severity",
    "occurred_at",
    "ingested_at",
}
FEEDBACK_TYPES = ("THUMBS_UP", "THUMBS_DOWN", "RATING")
ERROR_SOURCES = ("MODEL", "APPLICATION", "NETWORK", "DATABASE", "BILLING", "OTHER")
ERROR_SOURCE_WEIGHTS = (40, 25, 15, 10, 5, 5)
SEVERITIES = ("INFO", "WARNING", "ERROR", "CRITICAL")

ERROR_TYPE_CODES = {
    "MODEL": {
        "MODEL_TIMEOUT": "MDL-504",
        "MODEL_RATE_LIMIT": "MDL-429",
        "MODEL_UNAVAILABLE": "MDL-503",
        "INVALID_MODEL_RESPONSE": "MDL-502",
    },
    "APPLICATION": {
        "REQUEST_PROCESSING_ERROR": "APP-422",
        "INTERNAL_APPLICATION_ERROR": "APP-500",
        "VALIDATION_ERROR": "APP-400",
    },
    "NETWORK": {
        "CONNECTION_TIMEOUT": "NET-408",
        "CONNECTION_RESET": "NET-104",
        "CLIENT_DISCONNECT": "NET-499",
    },
    "DATABASE": {
        "DB_CONNECTION_ERROR": "DB-08001",
        "DB_TIMEOUT": "DB-57014",
        "WRITE_FAILURE": "DB-40001",
    },
    "BILLING": {
        "PAYMENT_PROVIDER_ERROR": "BILL-502",
        "BILLING_SERVICE_UNAVAILABLE": "BILL-503",
    },
    "OTHER": {"UNKNOWN_ERROR": "OTH-000"},
}

MODEL_POSITIVE_RATES = {
    "model_001": 0.72,
    "model_002": 0.77,
    "model_003": 0.75,
    "model_004": 0.73,
    "model_005": 0.70,
    "model_006": 0.80,
    "model_007": 0.72,
    "model_008": 0.78,
}


def _generation_end(partition_date: date) -> datetime:
    return datetime.combine(partition_date, time(23, 59, 59), timezone.utc)


def _timestamp_us(value: datetime | None) -> int | None:
    return None if value is None else round(value.timestamp() * 1_000_000)


def _from_us(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1_000_000, timezone.utc)


def _insert_batches(
    connection: sqlite3.Connection,
    statement: str,
    rows: Iterable[tuple[Any, ...]],
) -> int:
    count = 0
    batch = []
    try:
        for row in rows:
            batch.append(row)
            if len(batch) == 10_000:
                connection.executemany(statement, batch)
                count += len(batch)
                batch.clear()
        if batch:
            connection.executemany(statement, batch)
            count += len(batch)
    except sqlite3.IntegrityError as exc:
        raise ValueError(f"Source/output ID uniqueness failed: {exc}") from exc
    return count


@contextmanager
def source_lineage_index(
    source_root: str | Path,
) -> Iterator[tuple[sqlite3.Connection, list[dict[str, Any]], dict[str, int]]]:
    """Build a temporary disk-backed lineage index from serialized source data."""
    models, _ = load_existing_models(source_root)
    with tempfile.TemporaryDirectory() as temporary_directory:
        connection = sqlite3.connect(
            Path(temporary_directory) / "quality_source.sqlite"
        )
        connection.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;
            CREATE TABLE users (
                user_id TEXT PRIMARY KEY,
                signup_us INTEGER NOT NULL
            );
            CREATE TABLE conversations (
                conversation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_us INTEGER NOT NULL
            );
            CREATE TABLE messages (
                message_id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_us INTEGER NOT NULL
            );
            CREATE TABLE completions (
                completion_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL UNIQUE,
                conversation_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                requested_us INTEGER NOT NULL,
                completed_us INTEGER NOT NULL
            );
            CREATE TABLE inferences (
                inference_id TEXT PRIMARY KEY,
                completion_id TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL,
                model_id TEXT NOT NULL,
                request_us INTEGER NOT NULL,
                response_us INTEGER,
                latency_ms INTEGER,
                status TEXT NOT NULL
            );
            """
        )

        counts = {}
        counts["users"] = _insert_batches(
            connection,
            "INSERT INTO users VALUES (?, ?)",
            (
                (row["user_id"], _timestamp_us(row["signup_at"]))
                for row, _ in iter_local_partitioned_records("users", source_root)
            ),
        )
        counts["conversations"] = _insert_batches(
            connection,
            "INSERT INTO conversations VALUES (?, ?, ?)",
            (
                (
                    row["conversation_id"],
                    row["user_id"],
                    _timestamp_us(row["created_at"]),
                )
                for row, _ in iter_local_partitioned_records(
                    "conversations", source_root
                )
            ),
        )
        counts["messages"] = _insert_batches(
            connection,
            "INSERT INTO messages VALUES (?, ?, ?, ?)",
            (
                (
                    row["message_id"],
                    row["conversation_id"],
                    row["user_id"],
                    _timestamp_us(row["created_at"]),
                )
                for row, _ in iter_local_partitioned_records("messages", source_root)
            ),
        )
        counts["completions"] = _insert_batches(
            connection,
            "INSERT INTO completions VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    row["completion_id"],
                    row["message_id"],
                    row["conversation_id"],
                    row["user_id"],
                    row["completion_status"],
                    _timestamp_us(row["requested_at"]),
                    _timestamp_us(row["completed_at"]),
                )
                for row, _ in iter_local_partitioned_records(
                    "completions", source_root
                )
            ),
        )
        counts["model_inferences"] = _insert_batches(
            connection,
            "INSERT INTO inferences VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    row["inference_id"],
                    row["completion_id"],
                    row["user_id"],
                    row["model_id"],
                    _timestamp_us(row["request_at"]),
                    _timestamp_us(row["response_at"]),
                    row["latency_ms"],
                    row["inference_status"],
                )
                for row, _ in iter_local_partitioned_records(
                    "model_inferences", source_root
                )
            ),
        )
        connection.executescript(
            """
            CREATE INDEX completion_status_idx ON completions(status);
            CREATE INDEX inference_status_idx ON inferences(status);
            CREATE INDEX inference_completion_idx ON inferences(completion_id);
            CREATE INDEX message_user_idx ON messages(user_id);
            """
        )
        connection.commit()
        try:
            yield connection, models, counts
        finally:
            connection.close()


def _bounded_delay(
    timestamp: datetime,
    generation_end: datetime,
    delay_seconds: int,
) -> datetime:
    return timestamp + timedelta(
        seconds=min(
            delay_seconds,
            max(0, int((generation_end - timestamp).total_seconds())),
        )
    )


def _feedback_delay_seconds() -> int:
    group = random.choices(
        ("QUICK", "SAME_DAY", "LATER"),
        weights=(85, 12, 3),
        k=1,
    )[0]
    if group == "QUICK":
        return random.randint(1, 300)
    if group == "SAME_DAY":
        return random.randint(301, 21_600)
    return random.randint(21_601, 259_200)


def _feedback_propensity(user_id: str, propensities: dict[str, float]) -> float:
    if user_id not in propensities:
        propensities[user_id] = random.choices(
            (0.45, 1.40, 2.80),
            weights=(70, 25, 5),
            k=1,
        )[0]
    return propensities[user_id]


def should_generate_feedback(
    user_id: str,
    latency_ms: int,
    propensities: dict[str, float],
) -> bool:
    """Apply sparse, persistent user-level feedback propensity."""
    propensity = _feedback_propensity(user_id, propensities)
    latency_modifier = 1.08 if latency_ms >= 2_750 else 1.0
    return random.random() < 0.085 * propensity * latency_modifier


def build_feedback_event(
    completion_id: str,
    user_id: str,
    completed_at: datetime,
    model_id: str,
    latency_ms: int,
    generation_end: datetime,
) -> dict[str, Any]:
    """Build one sparse, model-aware feedback event after a successful response."""
    positive_rate = MODEL_POSITIVE_RATES[model_id]
    if latency_ms >= 2_750:
        positive_rate -= 0.08
    elif latency_ms >= 1_800:
        positive_rate -= 0.035
    if random.random() < 0.10:
        feedback_type = "RATING"
    elif random.random() < positive_rate:
        feedback_type = "THUMBS_UP"
    else:
        feedback_type = "THUMBS_DOWN"
    if feedback_type == "THUMBS_UP":
        score = 1
    elif feedback_type == "THUMBS_DOWN":
        score = -1
    elif random.random() < positive_rate:
        score = random.choices((4, 5), weights=(45, 55), k=1)[0]
    else:
        score = random.choices((1, 2, 3), weights=(25, 35, 40), k=1)[0]
    created_at = _bounded_delay(
        completed_at,
        generation_end,
        _feedback_delay_seconds(),
    )
    return {
        "feedback_id": generate_uuid(),
        "completion_id": completion_id,
        "user_id": user_id,
        "feedback_type": feedback_type,
        "feedback_score": score,
        "created_at": created_at,
        "ingested_at": ingestion_timestamp(created_at, generation_end),
    }


def _error_type(source: str) -> tuple[str, str]:
    types = tuple(ERROR_TYPE_CODES[source])
    weights = {
        "MODEL": (48, 25, 17, 10),
        "APPLICATION": (45, 35, 20),
        "NETWORK": (48, 27, 25),
        "DATABASE": (32, 43, 25),
        "BILLING": (60, 40),
        "OTHER": (100,),
    }[source]
    error_type = random.choices(types, weights=weights, k=1)[0]
    return error_type, ERROR_TYPE_CODES[source][error_type]


def _severity(error_type: str) -> str:
    weights = (10, 30, 55, 5)
    if error_type in {"MODEL_UNAVAILABLE", "DB_CONNECTION_ERROR", "WRITE_FAILURE"}:
        weights = (4, 22, 62, 12)
    return random.choices(SEVERITIES, weights=weights, k=1)[0]


def _empty_error_context() -> dict[str, str | None]:
    return {
        "user_id": None,
        "conversation_id": None,
        "message_id": None,
        "completion_id": None,
        "inference_id": None,
        "model_id": None,
    }


def build_linked_model_error(
    failed_lineage: tuple[Any, ...],
    generation_end: datetime,
) -> dict[str, Any]:
    """Build an error with full lineage to one existing failed inference."""
    (
        inference_id,
        completion_id,
        user_id,
        model_id,
        request_us,
        message_id,
        conversation_id,
        completed_us,
    ) = failed_lineage
    occurred_at = _from_us(completed_us) or _from_us(request_us)
    error_type, error_code = _error_type("MODEL")
    return {
        "error_id": generate_uuid(),
        "user_id": user_id,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "completion_id": completion_id,
        "inference_id": inference_id,
        "model_id": model_id,
        "error_source": "MODEL",
        "error_type": error_type,
        "error_code": error_code,
        "severity": _severity(error_type),
        "occurred_at": occurred_at,
        "ingested_at": ingestion_timestamp(occurred_at, generation_end),
    }


def _traffic_samples(connection: sqlite3.Connection) -> tuple[list[tuple], list[tuple]]:
    messages = list(
        connection.execute(
            """
            SELECT message_id, conversation_id, user_id, created_us
            FROM messages
            WHERE rowid % 19 = 0
            LIMIT 60000
            """
        )
    )
    inferences = list(
        connection.execute(
            """
            SELECT i.inference_id, i.completion_id, i.user_id, i.model_id,
                   i.request_us, i.response_us, c.message_id, c.conversation_id,
                   c.completed_us
            FROM inferences i
            JOIN completions c ON c.completion_id = i.completion_id
            WHERE i.rowid % 31 = 0
            LIMIT 40000
            """
        )
    )
    return messages, inferences


def build_independent_error(
    source: str,
    message_contexts: list[tuple],
    inference_contexts: list[tuple],
    models: list[dict[str, Any]],
    generation_end: datetime,
) -> dict[str, Any]:
    """Build one source-specific error with only logically available context."""
    message_id, conversation_id, user_id, message_us = random.choice(
        message_contexts
    )
    anchor = _from_us(message_us)
    occurred_at = _bounded_delay(
        anchor,
        generation_end,
        random.randint(0, 300),
    )
    context = _empty_error_context()

    if source == "MODEL":
        context["model_id"] = choose_model(models, occurred_at)["model_id"]
    elif source == "APPLICATION":
        pattern = random.choices(
            ("MESSAGE", "USER", "SYSTEM"),
            weights=(65, 20, 15),
            k=1,
        )[0]
        if pattern == "MESSAGE":
            context.update(
                user_id=user_id,
                conversation_id=conversation_id,
                message_id=message_id,
            )
        elif pattern == "USER":
            context["user_id"] = user_id
    elif source == "DATABASE":
        if random.random() < 0.55:
            context.update(
                user_id=user_id,
                conversation_id=conversation_id,
                message_id=message_id,
            )
    elif source == "NETWORK":
        pattern = random.choices(
            ("MESSAGE", "INFERENCE", "USER", "SYSTEM"),
            weights=(45, 35, 10, 10),
            k=1,
        )[0]
        if pattern == "MESSAGE":
            context.update(
                user_id=user_id,
                conversation_id=conversation_id,
                message_id=message_id,
            )
        elif pattern == "INFERENCE":
            (
                inference_id,
                completion_id,
                inference_user_id,
                model_id,
                request_us,
                response_us,
                inference_message_id,
                inference_conversation_id,
                completed_us,
            ) = random.choice(inference_contexts)
            context.update(
                user_id=inference_user_id,
                conversation_id=inference_conversation_id,
                message_id=inference_message_id,
                completion_id=completion_id,
                inference_id=inference_id,
                model_id=model_id,
            )
            occurred_at = _from_us(response_us or request_us or completed_us)
        elif pattern == "USER":
            context["user_id"] = user_id
    elif source == "BILLING":
        if random.random() < 0.85:
            context["user_id"] = user_id
    elif source == "OTHER" and random.random() < 0.35:
        context["user_id"] = user_id

    error_type, error_code = _error_type(source)
    return {
        "error_id": generate_uuid(),
        **context,
        "error_source": source,
        "error_type": error_type,
        "error_code": error_code,
        "severity": _severity(error_type),
        "occurred_at": occurred_at,
        "ingested_at": ingestion_timestamp(occurred_at, generation_end),
    }


def generate_quality_events_locally(
    partition_date: date,
    output_root: str | Path,
    *,
    error_target: int | None = None,
) -> dict[str, int]:
    """Generate feedback and errors from preserved serialized source lineage."""
    generation_end = _generation_end(partition_date)
    removed = remove_local_event_outputs(("feedback", "errors"), output_root)
    feedback_count = error_count = 0
    propensities: dict[str, float] = {}
    with source_lineage_index(output_root) as (connection, models, source_counts):
        message_contexts, inference_contexts = _traffic_samples(connection)
        with PartitionedNDJSONWriter(
            output_root,
            "feedback",
        ) as feedback_writer, PartitionedNDJSONWriter(
            output_root,
            "errors",
        ) as error_writer:
            for (
                completion_id,
                user_id,
                completed_us,
                model_id,
                latency_ms,
            ) in connection.execute(
                """
                SELECT c.completion_id, c.user_id, c.completed_us,
                       i.model_id, i.latency_ms
                FROM completions c
                JOIN inferences i ON i.completion_id = c.completion_id
                WHERE c.status = 'SUCCESS' AND i.status = 'SUCCESS'
                """
            ):
                if should_generate_feedback(
                    user_id,
                    latency_ms,
                    propensities,
                ):
                    feedback = build_feedback_event(
                        completion_id,
                        user_id,
                        _from_us(completed_us),
                        model_id,
                        latency_ms,
                        generation_end,
                    )
                    feedback_writer.write(
                        feedback,
                        feedback["ingested_at"].date(),
                    )
                    feedback_count += 1

            target = error_target or random.randint(20_000, 25_000)
            sources = random.choices(
                ERROR_SOURCES,
                weights=ERROR_SOURCE_WEIGHTS,
                k=target,
            )
            model_slots = sources.count("MODEL")
            linked_target = round(model_slots * 0.82)
            failed_lineages = list(
                connection.execute(
                    """
                    SELECT i.inference_id, i.completion_id, i.user_id, i.model_id,
                           i.request_us, c.message_id, c.conversation_id,
                           c.completed_us
                    FROM inferences i
                    JOIN completions c ON c.completion_id = i.completion_id
                    WHERE i.status = 'FAILED'
                    """
                )
            )
            random.shuffle(failed_lineages)
            linked_index = 0
            for source in sources:
                if source == "MODEL" and linked_index < linked_target:
                    error = build_linked_model_error(
                        failed_lineages[linked_index],
                        generation_end,
                    )
                    linked_index += 1
                else:
                    error = build_independent_error(
                        source,
                        message_contexts,
                        inference_contexts,
                        models,
                        generation_end,
                    )
                error_writer.write(error, error["ingested_at"].date())
                error_count += 1
            feedback_partitions = len(feedback_writer.paths)
            error_partitions = len(error_writer.paths)

    print(f"Preserved source messages: {source_counts['messages']:,}")
    print(f"Preserved source completions: {source_counts['completions']:,}")
    print(
        "Preserved source model inferences: "
        f"{source_counts['model_inferences']:,}"
    )
    print(f"Removed prior Milestone 6 local files: {len(removed)}")
    print(
        f"Wrote {feedback_count:,} feedback records across "
        f"{feedback_partitions} daily partitions"
    )
    print(
        f"Wrote {error_count:,} errors across "
        f"{error_partitions} daily partitions"
    )
    return {
        "feedback": feedback_count,
        "errors": error_count,
        "feedback_partitions": feedback_partitions,
        "error_partitions": error_partitions,
    }


class QualityIngestionMetrics:
    def __init__(self) -> None:
        self.delays = MetricHistogram()
        self.categories: Counter[str] = Counter()
        self.partitions = {"feedback": set(), "errors": set()}

    def add(
        self,
        entity_name: str,
        event_at: datetime,
        ingested_at: datetime,
        physical_date: date,
    ) -> None:
        delay = (ingested_at - event_at).total_seconds()
        self.delays.add(delay)
        self.partitions[entity_name].add(physical_date)
        if delay < 60:
            self.categories["same_minute"] += 1
        if ingested_at.date() == event_at.date():
            self.categories["same_day"] += 1
        elif ingested_at.date() == event_at.date() + timedelta(days=1):
            self.categories["next_day"] += 1
        if delay > 86_400:
            self.categories["over_one_day"] += 1


def _validate_ingestion(
    entity_name: str,
    event_at: datetime,
    ingested_at: Any,
    physical_date: date,
    generation_end: datetime,
    metrics: QualityIngestionMetrics,
) -> None:
    if (
        not isinstance(ingested_at, datetime)
        or ingested_at < event_at
        or ingested_at > generation_end
        or physical_date != ingested_at.date()
    ):
        raise ValueError(f"{entity_name} has invalid ingestion metadata")
    metrics.add(entity_name, event_at, ingested_at, physical_date)


def _is_positive(feedback_type: str, score: int) -> bool:
    return feedback_type == "THUMBS_UP" or (
        feedback_type == "RATING" and score >= 4
    )


def _is_negative(feedback_type: str, score: int) -> bool:
    return feedback_type == "THUMBS_DOWN" or (
        feedback_type == "RATING" and score <= 2
    )


def validate_serialized_quality_events(
    connection: sqlite3.Connection,
    models: list[dict[str, Any]],
    feedback_records: Iterable[tuple[dict[str, Any], date]],
    error_records: Iterable[tuple[dict[str, Any], date]],
    generation_end: datetime,
    *,
    production_scale: bool = True,
) -> dict[str, Any]:
    """Validate serialized Milestone 6 records against the source lineage index."""
    model_ids = {model["model_id"] for model in models}
    model_details = {model["model_id"]: model for model in models}
    feedback_types: Counter[str] = Counter()
    ratings: Counter[int] = Counter()
    positive = negative = neutral = 0
    error_sources: Counter[str] = Counter()
    error_types: Counter[str] = Counter()
    severities: Counter[str] = Counter()
    ingestion = QualityIngestionMetrics()

    connection.executescript(
        """
        CREATE TABLE feedback_output (
            feedback_id TEXT PRIMARY KEY,
            completion_id TEXT NOT NULL UNIQUE,
            user_id TEXT NOT NULL,
            feedback_type TEXT NOT NULL,
            feedback_score INTEGER NOT NULL,
            created_us INTEGER NOT NULL
        );
        CREATE TABLE error_output (
            error_id TEXT PRIMARY KEY,
            user_id TEXT,
            conversation_id TEXT,
            message_id TEXT,
            completion_id TEXT,
            inference_id TEXT,
            model_id TEXT,
            error_source TEXT NOT NULL,
            error_type TEXT NOT NULL,
            error_code TEXT NOT NULL,
            severity TEXT NOT NULL,
            occurred_us INTEGER NOT NULL
        );
        """
    )

    def feedback_rows() -> Iterator[tuple[Any, ...]]:
        nonlocal positive, negative, neutral
        for record, physical_date in feedback_records:
            if set(record) != FEEDBACK_FIELDS:
                raise ValueError("Feedback schema is invalid")
            feedback_id = record.get("feedback_id")
            created_at = record.get("created_at")
            if not isinstance(feedback_id, str) or not feedback_id:
                raise ValueError("Feedback ID is invalid")
            if not isinstance(created_at, datetime) or created_at > generation_end:
                raise ValueError(f"Feedback {feedback_id} has invalid created_at")
            feedback_type = record.get("feedback_type")
            score = record.get("feedback_score")
            if feedback_type not in FEEDBACK_TYPES:
                raise ValueError(f"Feedback {feedback_id} has invalid type")
            valid_score = (
                (feedback_type == "THUMBS_UP" and score == 1)
                or (feedback_type == "THUMBS_DOWN" and score == -1)
                or (
                    feedback_type == "RATING"
                    and isinstance(score, int)
                    and 1 <= score <= 5
                )
            )
            if not valid_score:
                raise ValueError(f"Feedback {feedback_id} has invalid score")
            _validate_ingestion(
                "feedback",
                created_at,
                record.get("ingested_at"),
                physical_date,
                generation_end,
                ingestion,
            )
            feedback_types[feedback_type] += 1
            if feedback_type == "RATING":
                ratings[score] += 1
            if _is_positive(feedback_type, score):
                positive += 1
            elif _is_negative(feedback_type, score):
                negative += 1
            else:
                neutral += 1
            yield (
                feedback_id,
                record["completion_id"],
                record["user_id"],
                feedback_type,
                score,
                _timestamp_us(created_at),
            )

    feedback_count = _insert_batches(
        connection,
        "INSERT INTO feedback_output VALUES (?, ?, ?, ?, ?, ?)",
        feedback_rows(),
    )

    def error_rows() -> Iterator[tuple[Any, ...]]:
        for record, physical_date in error_records:
            if set(record) != ERROR_FIELDS:
                raise ValueError("Error schema is invalid")
            error_id = record.get("error_id")
            occurred_at = record.get("occurred_at")
            source = record.get("error_source")
            error_type = record.get("error_type")
            if not isinstance(error_id, str) or not error_id:
                raise ValueError("Error ID is invalid")
            if not isinstance(occurred_at, datetime) or occurred_at > generation_end:
                raise ValueError(f"Error {error_id} has invalid occurred_at")
            if source not in ERROR_SOURCES:
                raise ValueError(f"Error {error_id} has invalid source")
            if error_type not in ERROR_TYPE_CODES[source]:
                raise ValueError(f"Error {error_id} has invalid source/type pair")
            if record.get("error_code") != ERROR_TYPE_CODES[source][error_type]:
                raise ValueError(f"Error {error_id} has invalid error_code")
            if record.get("severity") not in SEVERITIES:
                raise ValueError(f"Error {error_id} has invalid severity")
            if record.get("model_id") not in model_ids | {None}:
                raise ValueError(f"Error {error_id} has invalid model_id")
            _validate_ingestion(
                "errors",
                occurred_at,
                record.get("ingested_at"),
                physical_date,
                generation_end,
                ingestion,
            )
            error_sources[source] += 1
            error_types[error_type] += 1
            severities[record["severity"]] += 1
            yield (
                error_id,
                record["user_id"],
                record["conversation_id"],
                record["message_id"],
                record["completion_id"],
                record["inference_id"],
                record["model_id"],
                source,
                error_type,
                record["error_code"],
                record["severity"],
                _timestamp_us(occurred_at),
            )

    error_count = _insert_batches(
        connection,
        "INSERT INTO error_output VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        error_rows(),
    )
    connection.commit()

    success_count = connection.execute(
        "SELECT COUNT(*) FROM completions WHERE status = 'SUCCESS'"
    ).fetchone()[0]
    feedback_rate = feedback_count / success_count
    if not 0.05 <= feedback_rate <= 0.10:
        raise ValueError(f"Feedback rate {feedback_rate:.3%} is outside 5%-10%")
    if production_scale and not 60_000 <= feedback_count <= 90_000:
        raise ValueError(f"Feedback count {feedback_count} is outside 60K-90K")
    if production_scale and not 15_000 <= error_count <= 30_000:
        raise ValueError(f"Error count {error_count} is outside 15K-30K")

    feedback_lineage_errors = connection.execute(
        """
        SELECT COUNT(*)
        FROM feedback_output f
        LEFT JOIN completions c ON c.completion_id = f.completion_id
        WHERE c.completion_id IS NULL
           OR c.status != 'SUCCESS'
           OR c.user_id != f.user_id
           OR f.created_us < c.completed_us
        """
    ).fetchone()[0]
    if feedback_lineage_errors:
        raise ValueError(f"Found {feedback_lineage_errors} invalid feedback links")

    feedback_users, max_feedback, average_feedback = connection.execute(
        """
        SELECT COUNT(*), MAX(feedback_count), AVG(feedback_count)
        FROM (
            SELECT user_id, COUNT(*) AS feedback_count
            FROM feedback_output
            GROUP BY user_id
        )
        """
    ).fetchone()
    total_users = connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if feedback_users >= total_users or max_feedback <= average_feedback * 2:
        raise ValueError("Feedback behavior is too uniform across users")

    fk_errors = connection.execute(
        """
        SELECT COUNT(*)
        FROM error_output e
        LEFT JOIN users u ON u.user_id = e.user_id
        LEFT JOIN conversations v ON v.conversation_id = e.conversation_id
        LEFT JOIN messages m ON m.message_id = e.message_id
        LEFT JOIN completions c ON c.completion_id = e.completion_id
        LEFT JOIN inferences i ON i.inference_id = e.inference_id
        WHERE (e.user_id IS NOT NULL AND u.user_id IS NULL)
           OR (e.conversation_id IS NOT NULL AND v.conversation_id IS NULL)
           OR (e.message_id IS NOT NULL AND m.message_id IS NULL)
           OR (e.completion_id IS NOT NULL AND c.completion_id IS NULL)
           OR (e.inference_id IS NOT NULL AND i.inference_id IS NULL)
        """
    ).fetchone()[0]
    if fk_errors:
        raise ValueError(f"Found {fk_errors} invalid error foreign keys")

    lineage_errors = connection.execute(
        """
        SELECT COUNT(*)
        FROM error_output e
        LEFT JOIN messages m ON m.message_id = e.message_id
        LEFT JOIN completions c ON c.completion_id = e.completion_id
        LEFT JOIN inferences i ON i.inference_id = e.inference_id
        WHERE (
            e.message_id IS NOT NULL AND (
                e.user_id != m.user_id OR e.conversation_id != m.conversation_id
            )
        ) OR (
            e.completion_id IS NOT NULL AND (
                e.user_id != c.user_id
                OR (e.message_id IS NOT NULL AND e.message_id != c.message_id)
                OR (
                    e.conversation_id IS NOT NULL
                    AND e.conversation_id != c.conversation_id
                )
            )
        ) OR (
            e.inference_id IS NOT NULL AND (
                e.completion_id IS NULL OR e.completion_id != i.completion_id
                OR e.user_id IS NULL OR e.user_id != i.user_id
                OR e.model_id IS NULL OR e.model_id != i.model_id
            )
        )
        """
    ).fetchone()[0]
    if lineage_errors:
        raise ValueError(f"Found {lineage_errors} inconsistent error lineages")

    context_errors = connection.execute(
        """
        SELECT COUNT(*) FROM error_output
        WHERE (
            error_source IN ('APPLICATION', 'DATABASE')
            AND (
                completion_id IS NOT NULL OR inference_id IS NOT NULL
                OR model_id IS NOT NULL
            )
        ) OR (
            error_source = 'BILLING'
            AND (
                conversation_id IS NOT NULL OR message_id IS NOT NULL
                OR completion_id IS NOT NULL OR inference_id IS NOT NULL
                OR model_id IS NOT NULL
            )
        ) OR (
            error_source = 'MODEL' AND (
                model_id IS NULL
                OR (
                    inference_id IS NULL AND (
                        user_id IS NOT NULL OR conversation_id IS NOT NULL
                        OR message_id IS NOT NULL OR completion_id IS NOT NULL
                    )
                )
            )
        )
        """
    ).fetchone()[0]
    if context_errors:
        raise ValueError(f"Found {context_errors} illogical error contexts")

    source_tolerances = {
        "MODEL": (0.35, 0.45),
        "APPLICATION": (0.20, 0.30),
        "NETWORK": (0.10, 0.20),
        "DATABASE": (0.07, 0.13),
        "BILLING": (0.03, 0.08),
        "OTHER": (0.03, 0.08),
    }
    if production_scale:
        for source, (lower, upper) in source_tolerances.items():
            ratio = error_sources[source] / error_count
            if not lower <= ratio <= upper:
                raise ValueError(f"Error source {source} ratio is outside tolerance")

    feedback_timing = MetricHistogram()
    for delay_us, in connection.execute(
        """
        SELECT f.created_us - c.completed_us
        FROM feedback_output f
        JOIN completions c ON c.completion_id = f.completion_id
        """
    ):
        feedback_timing.add(delay_us / 1_000_000)

    feedback_by_model = {}
    for model_id, model in model_details.items():
        success_model_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM completions c
            JOIN inferences i ON i.completion_id = c.completion_id
            WHERE c.status = 'SUCCESS' AND i.model_id = ?
            """,
            (model_id,),
        ).fetchone()[0]
        count, model_positive = connection.execute(
            """
            SELECT COUNT(*), SUM(
                CASE WHEN f.feedback_type = 'THUMBS_UP'
                       OR (f.feedback_type = 'RATING' AND f.feedback_score >= 4)
                     THEN 1 ELSE 0 END
            )
            FROM feedback_output f
            JOIN inferences i ON i.completion_id = f.completion_id
            WHERE i.model_id = ?
            """,
            (model_id,),
        ).fetchone()
        feedback_by_model[model_id] = {
            "model_name": model["model_name"],
            "model_version": model["model_version"],
            "count": count,
            "feedback_rate": count / success_model_count * 100,
            "positive_rate": (model_positive or 0) / count * 100 if count else 0,
        }

    model_errors = {
        model["model_id"]: {
            "model_name": model["model_name"],
            "model_version": model["model_version"],
            "count": connection.execute(
                """
                SELECT COUNT(*) FROM error_output
                WHERE error_source = 'MODEL' AND model_id = ?
                """,
                (model["model_id"],),
            ).fetchone()[0],
        }
        for model in models
    }
    failed_inferences = connection.execute(
        "SELECT COUNT(*) FROM inferences WHERE status = 'FAILED'"
    ).fetchone()[0]
    linked_failed = connection.execute(
        """
        SELECT COUNT(DISTINCT e.inference_id)
        FROM error_output e
        JOIN inferences i ON i.inference_id = e.inference_id
        WHERE e.error_source = 'MODEL' AND i.status = 'FAILED'
        """
    ).fetchone()[0]
    if linked_failed == 0:
        raise ValueError("No model errors link to failed inferences")

    context_counts = {
        field: connection.execute(
            f"SELECT COUNT(*) FROM error_output WHERE {field} IS NOT NULL"
        ).fetchone()[0]
        for field in (
            "user_id",
            "conversation_id",
            "message_id",
            "completion_id",
            "inference_id",
            "model_id",
        )
    }
    partition_report = {
        entity_name: {
            "count": len(partition_dates),
            "earliest": min(partition_dates),
            "latest": max(partition_dates),
        }
        for entity_name, partition_dates in ingestion.partitions.items()
    }
    if any(report["count"] < 2 for report in partition_report.values()):
        raise ValueError("Milestone 6 output does not span multiple partitions")
    if production_scale and ingestion.categories["over_one_day"] == 0:
        raise ValueError("Milestone 6 output has no genuinely late arrivals")

    return {
        "feedback": feedback_count,
        "feedback_rate": feedback_rate * 100,
        "feedback_types": feedback_types,
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "ratings": ratings,
        "feedback_by_model": feedback_by_model,
        "feedback_timing": feedback_timing.summary(),
        "errors": error_count,
        "error_sources": error_sources,
        "error_types": error_types,
        "severities": severities,
        "model_errors": model_errors,
        "failed_inferences": failed_inferences,
        "linked_failed_inferences": linked_failed,
        "error_context_counts": context_counts,
        "partitions": partition_report,
        "ingestion_delay": ingestion.delays.summary(),
        "arrival_categories": ingestion.categories,
        "duplicate_lineage_validation": "PASS",
    }


def validate_local_quality_events(
    partition_date: date,
    source_root: str | Path,
    *,
    production_scale: bool = True,
) -> dict[str, Any]:
    with source_lineage_index(source_root) as (connection, models, _):
        return validate_serialized_quality_events(
            connection,
            models,
            iter_local_partitioned_records("feedback", source_root),
            iter_local_partitioned_records("errors", source_root),
            _generation_end(partition_date),
            production_scale=production_scale,
        )


def validate_s3_quality_events(
    client: Any,
    bucket: str,
    partition_date: date,
    source_root: str | Path,
) -> dict[str, Any]:
    with source_lineage_index(source_root) as (connection, models, _):
        return validate_serialized_quality_events(
            connection,
            models,
            iter_s3_partitioned_records(client, bucket, "feedback"),
            iter_s3_partitioned_records(client, bucket, "errors"),
            _generation_end(partition_date),
        )


def print_quality_summary(summary: dict[str, Any]) -> None:
    print("\nFEEDBACK:")
    print(f"total: {summary['feedback']:,}")
    print(f"rate among successful completions: {summary['feedback_rate']:.4f}%")
    for feedback_type in FEEDBACK_TYPES:
        print(f"{feedback_type}: {summary['feedback_types'][feedback_type]:,}")
    print(
        f"positive: {summary['positive']:,}; negative: {summary['negative']:,}; "
        f"neutral: {summary['neutral']:,}"
    )
    print("ratings: " + ", ".join(
        f"{score}={summary['ratings'][score]:,}" for score in range(1, 6)
    ))
    print("\nFEEDBACK BY MODEL:")
    for model_id, values in summary["feedback_by_model"].items():
        print(
            f"{model_id} {values['model_name']} {values['model_version']}: "
            f"{values['count']:,}, rate={values['feedback_rate']:.4f}%, "
            f"positive={values['positive_rate']:.4f}%"
        )
    print("\nFEEDBACK TIMING SECONDS:")
    print(f"median: {summary['feedback_timing']['median']:,}")
    print(f"p95: {summary['feedback_timing']['p95']:,}")
    print("\nERRORS:")
    print(f"total: {summary['errors']:,}")
    print("sources: " + ", ".join(
        f"{source}={summary['error_sources'][source]:,}"
        for source in ERROR_SOURCES
    ))
    print("types:")
    for error_type, count in summary["error_types"].most_common():
        print(f"{error_type}: {count:,}")
    print("severity: " + ", ".join(
        f"{severity}={summary['severities'][severity]:,}"
        for severity in SEVERITIES
    ))
    print("\nMODEL ERRORS:")
    for model_id, values in summary["model_errors"].items():
        print(
            f"{model_id} {values['model_name']} {values['model_version']}: "
            f"{values['count']:,}"
        )
    print(
        "failed inference linkage: "
        f"{summary['linked_failed_inferences']:,}/"
        f"{summary['failed_inferences']:,} "
        f"({summary['linked_failed_inferences'] / summary['failed_inferences']:.4%})"
    )
    print("\nERROR CONTEXT:")
    for field_name, count in summary["error_context_counts"].items():
        print(f"{field_name}: {count:,} ({count / summary['errors']:.4%})")
    print("\nINGESTION DELAY SECONDS:")
    for metric in ("median", "p95", "p99", "max"):
        print(f"{metric}: {summary['ingestion_delay'][metric]:,.0f}")
    print("arrivals:")
    total_events = summary["feedback"] + summary["errors"]
    for category in ("same_minute", "same_day", "next_day", "over_one_day"):
        count = summary["arrival_categories"][category]
        print(f"{category}: {count:,} ({count / total_events:.4%})")
    print("\nDAILY PARTITIONS:")
    for entity_name, values in summary["partitions"].items():
        print(
            f"{entity_name}: {values['count']} "
            f"({values['earliest']} to {values['latest']})"
        )
