"""Completion and model-inference generation from existing user messages."""

import json
import math
import random
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .ingestion import (
    ingestion_timestamp,
    iter_local_partitioned_records,
    iter_s3_partitioned_records,
    primary_event_timestamp,
    remove_local_event_outputs,
)
from .reference_data import validate_models
from .utils import generate_uuid
from .writers import PartitionedNDJSONWriter


COMPLETION_COVERAGE = 0.99
COMPLETION_STATUSES = ("SUCCESS", "FAILED", "CANCELLED")
COMPLETION_STATUS_WEIGHTS = (94, 4, 2)
INFERENCE_STATUSES = {"SUCCESS", "FAILED"}
COMPLETION_FIELDS = {
    "completion_id",
    "message_id",
    "conversation_id",
    "user_id",
    "completion_status",
    "requested_at",
    "completed_at",
    "response_text",
    "ingested_at",
}
INFERENCE_FIELDS = {
    "inference_id",
    "completion_id",
    "user_id",
    "model_id",
    "request_at",
    "response_at",
    "latency_ms",
    "input_tokens",
    "output_tokens",
    "inference_status",
    "ingested_at",
}

# peak weight, adoption-ramp days, decline date, decline half-life days
MODEL_TRAFFIC_PROFILES = {
    "model_001": (8.0, 30, date(2025, 9, 1), 100),
    "model_002": (34.0, 30, date(2026, 2, 12), 180),
    "model_003": (44.0, 40, None, 365),
    "model_004": (24.0, 45, date(2026, 2, 12), 150),
    "model_005": (5.0, 30, date(2025, 8, 1), 120),
    "model_006": (58.0, 60, None, 365),
    "model_007": (10.0, 45, date(2025, 8, 19), 100),
    "model_008": (24.0, 45, None, 365),
}

# Median milliseconds and log-normal sigma. Values intentionally differ.
MODEL_LATENCY_PROFILES = {
    "model_001": (1_150, 0.48),
    "model_002": (850, 0.42),
    "model_003": (1_650, 0.50),
    "model_004": (1_050, 0.44),
    "model_005": (1_900, 0.52),
    "model_006": (680, 0.40),
    "model_007": (1_350, 0.47),
    "model_008": (920, 0.43),
}

RESPONSE_SENTENCES = (
    "A practical way to approach this is to start with the smallest testable unit.",
    "First, define the expected outcome and the constraints that matter most.",
    "Then separate the workflow into clear stages with observable inputs and outputs.",
    "Use representative examples so the behavior is easy to verify and explain.",
    "The main tradeoff is between simplicity now and flexibility for later changes.",
    "For production use, add validation, monitoring, and an explicit failure path.",
    "A useful implementation should be deterministic where repeatability matters.",
    "Measure the result with a small set of metrics before adding more complexity.",
    "This keeps the design understandable while still leaving room to scale.",
    "Be careful around boundaries, missing values, and assumptions about ordering.",
    "A concise test case can confirm both the normal path and the important "
    "edge cases.",
    "Once that works, expand incrementally and compare each change against "
    "the baseline.",
    "Document the decision in code through clear names and focused validation rules.",
    "The final shape should make incorrect states difficult to produce accidentally.",
)


def _generation_end(partition_date: date) -> datetime:
    return datetime.combine(partition_date, time(23, 59, 59), timezone.utc)


def load_existing_models(
    source_root: str | Path,
) -> tuple[list[dict[str, Any]], Path]:
    """Load the newest valid local model snapshot without regenerating it."""
    paths = sorted(
        (Path(source_root) / "raw" / "models").glob("dt=*/*.json"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    if not paths:
        raise RuntimeError("No existing local model reference data found")
    failures = []
    for path in paths:
        try:
            models = [
                json.loads(line)
                for line in path.read_text().splitlines()
                if line
            ]
            for model in models:
                model["release_date"] = date.fromisoformat(model["release_date"])
            validate_models(models)
            return models, path
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            failures.append(f"{path}: {exc}")
    raise RuntimeError("No valid model snapshot found: " + "; ".join(failures))


def load_conversation_owners(source_root: str | Path) -> dict[str, str]:
    """Load existing conversation ownership for source-message verification."""
    owners = {}
    for conversation, _ in iter_local_partitioned_records(
        "conversations",
        source_root,
    ):
        conversation_id = conversation["conversation_id"]
        if conversation_id in owners:
            raise ValueError(f"Duplicate source conversation_id {conversation_id}")
        owners[conversation_id] = conversation["user_id"]
    return owners


def _model_weight(model: dict[str, Any], timestamp: datetime) -> float:
    if model["release_date"] > timestamp.date():
        return 0.0
    peak, ramp_days, decline_date, half_life = MODEL_TRAFFIC_PROFILES[
        model["model_id"]
    ]
    age_days = max(0, (timestamp.date() - model["release_date"]).days)
    adoption = 0.10 + 0.90 * min(1.0, age_days / ramp_days)
    decline = 1.0
    if decline_date and timestamp.date() > decline_date:
        decline_days = (timestamp.date() - decline_date).days
        decline = 0.5 ** (decline_days / half_life)
    return peak * adoption * decline


def choose_model(
    models: list[dict[str, Any]],
    request_at: datetime,
) -> dict[str, Any]:
    """Choose a released model using gradual adoption and decline curves."""
    weights = [_model_weight(model, request_at) for model in models]
    if not any(weights):
        raise ValueError(f"No model released by {request_at.isoformat()}")
    return random.choices(models, weights=weights, k=1)[0]


def _sample_latency_ms(model_id: str) -> int:
    median_ms, sigma = MODEL_LATENCY_PROFILES[model_id]
    latency = random.lognormvariate(math.log(median_ms), sigma)
    if random.random() < 0.005:
        latency *= random.uniform(2.0, 4.5)
    return max(50, min(20_000, round(latency)))


def _input_tokens(message_text: str) -> int:
    return max(1, round(len(message_text) / 4 * random.uniform(0.88, 1.12)))


def _response_text(message_text: str) -> str:
    category = random.choices(
        ("SHORT", "MEDIUM", "LONG", "VERY_LONG"),
        weights=(20, 55, 23, 2),
        k=1,
    )[0]
    bounds = {
        "SHORT": (70, 145),
        "MEDIUM": (170, 360),
        "LONG": (420, 820),
        "VERY_LONG": (900, 1_600),
    }[category]
    target = random.randint(*bounds)
    if random.random() < 0.78:
        target = max(target, len(message_text) + random.randint(20, 120))
    sentences = []
    while sum(len(sentence) + 1 for sentence in sentences) < target:
        sentences.append(random.choice(RESPONSE_SENTENCES))
    return " ".join(sentences)


def _output_tokens(response_text: str) -> int:
    return max(1, round(len(response_text) / 4 * random.uniform(0.90, 1.10)))


def generate_completion_and_inference(
    message: dict[str, Any],
    models: list[dict[str, Any]],
    generation_end: datetime,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Generate a causal zero/one completion and its single inference."""
    message_at = message["created_at"]
    if message_at >= generation_end or random.random() >= COMPLETION_COVERAGE:
        return None

    available_ms = int((generation_end - message_at).total_seconds() * 1_000)
    request_delay_ms = min(random.randint(15, 350), available_ms)
    requested_at = message_at + timedelta(milliseconds=request_delay_ms)
    model = choose_model(models, requested_at)
    completion_status = random.choices(
        COMPLETION_STATUSES,
        weights=COMPLETION_STATUS_WEIGHTS,
        k=1,
    )[0]
    input_tokens = _input_tokens(message["message_text"])
    pre_inference_ms = random.randint(5, 120)
    post_inference_ms = random.randint(8, 220)
    latency_ms = _sample_latency_ms(model["model_id"])
    required_ms = pre_inference_ms + latency_ms + post_inference_ms
    remaining_ms = int((generation_end - requested_at).total_seconds() * 1_000)
    if completion_status == "SUCCESS" and remaining_ms < required_ms:
        completion_status = random.choice(("FAILED", "CANCELLED"))

    completion_id = generate_uuid()
    inference_id = generate_uuid()
    if completion_status == "SUCCESS":
        inference_request_at = requested_at + timedelta(
            milliseconds=pre_inference_ms
        )
        inference_response_at = inference_request_at + timedelta(
            milliseconds=latency_ms
        )
        completed_at = inference_response_at + timedelta(
            milliseconds=post_inference_ms
        )
        response_text = _response_text(message["message_text"])
        output_tokens = _output_tokens(response_text)
        inference_status = "SUCCESS"
    else:
        detection_ms = min(
            max(pre_inference_ms, random.randint(25, 2_000)),
            max(0, remaining_ms),
        )
        inference_request_at = requested_at + timedelta(
            milliseconds=min(pre_inference_ms, detection_ms)
        )
        completed_at = requested_at + timedelta(milliseconds=detection_ms)
        inference_response_at = None
        latency_ms = None
        response_text = None
        output_tokens = None
        inference_status = "FAILED"

    completion = {
        "completion_id": completion_id,
        "message_id": message["message_id"],
        "conversation_id": message["conversation_id"],
        "user_id": message["user_id"],
        "completion_status": completion_status,
        "requested_at": requested_at,
        "completed_at": completed_at,
        "response_text": response_text,
        "ingested_at": ingestion_timestamp(completed_at, generation_end),
    }
    inference = {
        "inference_id": inference_id,
        "completion_id": completion_id,
        "user_id": message["user_id"],
        "model_id": model["model_id"],
        "request_at": inference_request_at,
        "response_at": inference_response_at,
        "latency_ms": latency_ms,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "inference_status": inference_status,
        "ingested_at": ingestion_timestamp(
            inference_response_at or inference_request_at,
            generation_end,
        ),
    }
    return completion, inference


def generate_inference_records(
    messages: Iterable[dict[str, Any]],
    models: list[dict[str, Any]],
    generation_end: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Materialize completion/inference pairs for tests and small samples."""
    completions = []
    inferences = []
    for message in messages:
        generated = generate_completion_and_inference(
            message,
            models,
            generation_end,
        )
        if generated is not None:
            completion, inference = generated
            completions.append(completion)
            inferences.append(inference)
    return completions, inferences


class MetricHistogram:
    """Exact integer distribution metrics without retaining each observation."""

    def __init__(self) -> None:
        self.counts: Counter[int] = Counter()
        self.count = 0
        self.total = 0

    def add(self, value: int | float) -> None:
        normalized = int(round(value))
        self.counts[normalized] += 1
        self.count += 1
        self.total += normalized

    def percentile(self, probability: float) -> int:
        target = max(1, math.ceil(self.count * probability))
        cumulative = 0
        for value, count in sorted(self.counts.items()):
            cumulative += count
            if cumulative >= target:
                return value
        raise ValueError("Cannot calculate percentile for an empty metric")

    def summary(self) -> dict[str, float | int]:
        if not self.count:
            return {"average": 0, "median": 0, "p95": 0, "p99": 0, "max": 0}
        return {
            "average": self.total / self.count,
            "median": self.percentile(0.50),
            "p95": self.percentile(0.95),
            "p99": self.percentile(0.99),
            "max": max(self.counts),
        }


class CorrelationAccumulator:
    def __init__(self) -> None:
        self.n = 0
        self.sum_x = self.sum_y = 0.0
        self.sum_xx = self.sum_yy = self.sum_xy = 0.0

    def add(self, x: float, y: float) -> None:
        self.n += 1
        self.sum_x += x
        self.sum_y += y
        self.sum_xx += x * x
        self.sum_yy += y * y
        self.sum_xy += x * y

    def value(self) -> float:
        numerator = self.n * self.sum_xy - self.sum_x * self.sum_y
        denominator = math.sqrt(
            (self.n * self.sum_xx - self.sum_x**2)
            * (self.n * self.sum_yy - self.sum_y**2)
        )
        return numerator / denominator if denominator else 0.0


class IngestionMetrics:
    def __init__(self) -> None:
        self.delays = MetricHistogram()
        self.categories: Counter[str] = Counter()
        self.partition_dates: dict[str, set[date]] = {
            "completions": set(),
            "model_inferences": set(),
        }

    def add(
        self,
        entity_name: str,
        event_at: datetime,
        ingested_at: datetime,
        physical_date: date,
    ) -> None:
        delay = (ingested_at - event_at).total_seconds()
        self.delays.add(delay)
        self.partition_dates[entity_name].add(physical_date)
        if delay < 60:
            self.categories["same_minute"] += 1
        if ingested_at.date() == event_at.date():
            self.categories["same_day"] += 1
        elif ingested_at.date() == event_at.date() + timedelta(days=1):
            self.categories["next_day"] += 1
        if delay > 86_400:
            self.categories["over_one_day"] += 1


def generate_inference_data_locally(
    partition_date: date,
    output_root: str | Path,
) -> dict[str, Any]:
    """Stream Milestone 5 records from preserved local source messages."""
    generation_end = _generation_end(partition_date)
    models, model_path = load_existing_models(output_root)
    owners = load_conversation_owners(output_root)
    removed = remove_local_event_outputs(
        ("completions", "model_inferences"),
        output_root,
    )
    source_count = completion_count = inference_count = 0
    with PartitionedNDJSONWriter(
        output_root,
        "completions",
    ) as completion_writer, PartitionedNDJSONWriter(
        output_root,
        "model_inferences",
    ) as inference_writer:
        for message, physical_date in iter_local_partitioned_records(
            "messages",
            output_root,
        ):
            source_count += 1
            if message["ingested_at"].date() != physical_date:
                raise ValueError(
                    f"Source message {message['message_id']} has a dt mismatch"
                )
            if owners.get(message["conversation_id"]) != message["user_id"]:
                raise ValueError(
                    f"Source message {message['message_id']} has invalid ownership"
                )
            generated = generate_completion_and_inference(
                message,
                models,
                generation_end,
            )
            if generated is not None:
                completion, inference = generated
                completion_writer.write(
                    completion,
                    completion["ingested_at"].date(),
                )
                inference_writer.write(
                    inference,
                    inference["ingested_at"].date(),
                )
                completion_count += 1
                inference_count += 1
            if source_count % 100_000 == 0:
                print(f"Processed {source_count:,} preserved source messages")
        completion_paths = dict(completion_writer.paths)
        inference_paths = dict(inference_writer.paths)

    if source_count < 1_000_000:
        raise ValueError(f"Expected at least 1M source messages, found {source_count}")
    print(f"Loaded model snapshot: {model_path}")
    print(f"Loaded existing conversations: {len(owners):,}")
    print(f"Preserved source messages: {source_count:,}")
    print(f"Removed prior Milestone 5 local files: {len(removed)}")
    print(
        f"Wrote {completion_count:,} completions across "
        f"{len(completion_paths)} daily partitions"
    )
    print(
        f"Wrote {inference_count:,} model inferences across "
        f"{len(inference_paths)} daily partitions"
    )
    return {
        "source_messages": source_count,
        "completions": completion_count,
        "model_inferences": inference_count,
        "completion_partitions": len(completion_paths),
        "inference_partitions": len(inference_paths),
    }


def _timestamp_us(value: datetime | None) -> int | None:
    return None if value is None else round(value.timestamp() * 1_000_000)


def _ensure_datetime(value: Any, field_name: str, record_id: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{record_id} has invalid {field_name}")
    return value


def _validate_ingestion_row(
    entity_name: str,
    record: dict[str, Any],
    physical_date: date,
    generation_end: datetime,
    metrics: IngestionMetrics,
) -> None:
    event_at = primary_event_timestamp(entity_name, record)
    ingested_at = _ensure_datetime(
        record.get("ingested_at"),
        "ingested_at",
        record.get("completion_id") or record.get("inference_id") or "record",
    )
    if ingested_at < event_at or ingested_at > generation_end:
        raise ValueError(f"{entity_name} has an invalid ingestion timestamp")
    if physical_date != ingested_at.date():
        raise ValueError(f"{entity_name} physical dt does not match ingested_at")
    metrics.add(entity_name, event_at, ingested_at, physical_date)


def validate_serialized_inference_data(
    source_messages: Iterable[dict[str, Any]],
    completions: Iterable[tuple[dict[str, Any], date]],
    inferences: Iterable[tuple[dict[str, Any], date]],
    models: list[dict[str, Any]],
    generation_end: datetime,
) -> dict[str, Any]:
    """Fully validate serialized Milestone 5 streams with a disk-backed index."""
    models_by_id = {model["model_id"]: model for model in models}
    status_counts: Counter[str] = Counter()
    model_counts: Counter[str] = Counter()
    latency = MetricHistogram()
    input_tokens = MetricHistogram()
    output_tokens = MetricHistogram()
    response_lengths = MetricHistogram()
    latency_by_model = {model_id: MetricHistogram() for model_id in models_by_id}
    response_token_correlation = CorrelationAccumulator()
    ingestion = IngestionMetrics()

    with tempfile.TemporaryDirectory() as temporary_directory:
        database_path = Path(temporary_directory) / "milestone5_validation.sqlite"
        connection = sqlite3.connect(database_path)
        connection.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;
            CREATE TABLE messages (
                message_id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_us INTEGER NOT NULL,
                text_length INTEGER NOT NULL
            );
            CREATE TABLE completions (
                completion_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL UNIQUE,
                conversation_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                requested_us INTEGER NOT NULL,
                completed_us INTEGER NOT NULL,
                response_length INTEGER
            );
            CREATE TABLE inferences (
                inference_id TEXT PRIMARY KEY,
                completion_id TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL,
                model_id TEXT NOT NULL,
                request_us INTEGER NOT NULL,
                response_us INTEGER,
                latency_ms INTEGER,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER,
                status TEXT NOT NULL
            );
            """
        )

        def insert_batches(statement: str, rows: Iterator[tuple[Any, ...]]) -> int:
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
                raise ValueError(f"Duplicate Milestone 5/source ID: {exc}") from exc
            return count

        def message_rows() -> Iterator[tuple[Any, ...]]:
            for message in source_messages:
                message_id = message.get("message_id")
                if not isinstance(message_id, str) or not message_id:
                    raise ValueError("Source message has an invalid message_id")
                created_at = _ensure_datetime(
                    message.get("created_at"),
                    "created_at",
                    message_id,
                )
                yield (
                    message_id,
                    message["conversation_id"],
                    message["user_id"],
                    _timestamp_us(created_at),
                    len(message["message_text"]),
                )

        source_count = insert_batches(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?)",
            message_rows(),
        )

        def completion_rows() -> Iterator[tuple[Any, ...]]:
            for completion, physical_date in completions:
                if set(completion) != COMPLETION_FIELDS:
                    raise ValueError("Completion schema is invalid")
                completion_id = completion.get("completion_id")
                if not isinstance(completion_id, str) or not completion_id:
                    raise ValueError("Completion has an invalid completion_id")
                requested_at = _ensure_datetime(
                    completion.get("requested_at"),
                    "requested_at",
                    completion_id,
                )
                completed_at = _ensure_datetime(
                    completion.get("completed_at"),
                    "completed_at",
                    completion_id,
                )
                status = completion.get("completion_status")
                response_text = completion.get("response_text")
                if status not in COMPLETION_STATUSES:
                    raise ValueError(f"{completion_id} has invalid completion status")
                if requested_at > generation_end or completed_at > generation_end:
                    raise ValueError(f"{completion_id} exceeds generation horizon")
                if completed_at < requested_at:
                    raise ValueError(f"{completion_id} completes before request")
                if status == "SUCCESS":
                    if completed_at <= requested_at:
                        raise ValueError(f"{completion_id} has no successful duration")
                    if not isinstance(response_text, str) or not response_text.strip():
                        raise ValueError(f"{completion_id} has no successful response")
                    response_lengths.add(len(response_text))
                elif response_text is not None:
                    raise ValueError(f"{completion_id} failure response must be NULL")
                _validate_ingestion_row(
                    "completions",
                    completion,
                    physical_date,
                    generation_end,
                    ingestion,
                )
                status_counts[status] += 1
                yield (
                    completion_id,
                    completion["message_id"],
                    completion["conversation_id"],
                    completion["user_id"],
                    status,
                    _timestamp_us(requested_at),
                    _timestamp_us(completed_at),
                    len(response_text) if response_text else None,
                )

        completion_count = insert_batches(
            "INSERT INTO completions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            completion_rows(),
        )

        def inference_rows() -> Iterator[tuple[Any, ...]]:
            for inference, physical_date in inferences:
                if set(inference) != INFERENCE_FIELDS:
                    raise ValueError("Model inference schema is invalid")
                inference_id = inference.get("inference_id")
                if not isinstance(inference_id, str) or not inference_id:
                    raise ValueError("Model inference has an invalid inference_id")
                request_at = _ensure_datetime(
                    inference.get("request_at"),
                    "request_at",
                    inference_id,
                )
                response_at = inference.get("response_at")
                if response_at is not None:
                    response_at = _ensure_datetime(
                        response_at,
                        "response_at",
                        inference_id,
                    )
                model = models_by_id.get(inference.get("model_id"))
                if model is None:
                    raise ValueError(f"{inference_id} has invalid model_id")
                if model["release_date"] > request_at.date():
                    raise ValueError(f"{inference_id} predates model release")
                status = inference.get("inference_status")
                if status not in INFERENCE_STATUSES:
                    raise ValueError(f"{inference_id} has invalid inference status")
                current_input_tokens = inference.get("input_tokens")
                if (
                    not isinstance(current_input_tokens, int)
                    or current_input_tokens <= 0
                ):
                    raise ValueError(f"{inference_id} has invalid input_tokens")
                input_tokens.add(current_input_tokens)
                current_latency = inference.get("latency_ms")
                current_output_tokens = inference.get("output_tokens")
                if status == "SUCCESS":
                    if (
                        response_at is None
                        or response_at <= request_at
                        or not isinstance(current_latency, int)
                        or current_latency <= 0
                        or not isinstance(current_output_tokens, int)
                        or current_output_tokens <= 0
                    ):
                        raise ValueError(f"{inference_id} has invalid success metrics")
                    measured_ms = round(
                        (response_at - request_at).total_seconds() * 1_000
                    )
                    if abs(measured_ms - current_latency) > 1:
                        raise ValueError(
                            f"{inference_id} latency does not match timestamps"
                        )
                    latency.add(current_latency)
                    latency_by_model[model["model_id"]].add(current_latency)
                    output_tokens.add(current_output_tokens)
                elif any(
                    value is not None
                    for value in (response_at, current_latency, current_output_tokens)
                ):
                    raise ValueError(f"{inference_id} failure metrics must be NULL")
                if request_at > generation_end or (
                    response_at is not None and response_at > generation_end
                ):
                    raise ValueError(f"{inference_id} exceeds generation horizon")
                _validate_ingestion_row(
                    "model_inferences",
                    inference,
                    physical_date,
                    generation_end,
                    ingestion,
                )
                model_counts[model["model_id"]] += 1
                yield (
                    inference_id,
                    inference["completion_id"],
                    inference["user_id"],
                    model["model_id"],
                    _timestamp_us(request_at),
                    _timestamp_us(response_at),
                    current_latency,
                    current_input_tokens,
                    current_output_tokens,
                    status,
                )

        inference_count = insert_batches(
            "INSERT INTO inferences VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            inference_rows(),
        )
        connection.commit()

        coverage = completion_count / source_count
        if not 0.98 <= coverage <= 0.995:
            raise ValueError(f"Completion coverage {coverage:.4%} is out of range")
        if inference_count != completion_count:
            raise ValueError("Every completion must have exactly one inference")
        for status, lower, upper in (
            ("SUCCESS", 0.92, 0.96),
            ("FAILED", 0.03, 0.05),
            ("CANCELLED", 0.01, 0.03),
        ):
            ratio = status_counts[status] / completion_count
            if not lower <= ratio <= upper:
                raise ValueError(f"Completion {status} ratio {ratio:.3%} is invalid")

        relationship_errors = connection.execute(
            """
            SELECT COUNT(*)
            FROM completions c
            LEFT JOIN messages m ON m.message_id = c.message_id
            WHERE m.message_id IS NULL
               OR c.conversation_id != m.conversation_id
               OR c.user_id != m.user_id
               OR c.requested_us < m.created_us
            """
        ).fetchone()[0]
        if relationship_errors:
            raise ValueError(f"Found {relationship_errors} invalid completion links")

        inference_errors = connection.execute(
            """
            SELECT COUNT(*)
            FROM inferences i
            LEFT JOIN completions c ON c.completion_id = i.completion_id
            WHERE c.completion_id IS NULL
               OR i.user_id != c.user_id
               OR (c.status = 'SUCCESS' AND i.status != 'SUCCESS')
               OR (c.status != 'SUCCESS' AND i.status != 'FAILED')
            """
        ).fetchone()[0]
        if inference_errors:
            raise ValueError(f"Found {inference_errors} inconsistent inference links")

        causal_errors = connection.execute(
            """
            SELECT COUNT(*)
            FROM messages m
            JOIN completions c ON c.message_id = m.message_id
            JOIN inferences i ON i.completion_id = c.completion_id
            WHERE c.status = 'SUCCESS'
              AND NOT (
                  m.created_us <= c.requested_us
                  AND c.requested_us <= i.request_us
                  AND i.request_us < i.response_us
                  AND i.response_us <= c.completed_us
              )
            """
        ).fetchone()[0]
        if causal_errors:
            raise ValueError(f"Found {causal_errors} broken successful causal chains")

        prompt_token_correlation = CorrelationAccumulator()
        for prompt_length, tokens in connection.execute(
            """
            SELECT m.text_length, i.input_tokens
            FROM messages m
            JOIN completions c ON c.message_id = m.message_id
            JOIN inferences i ON i.completion_id = c.completion_id
            """
        ):
            prompt_token_correlation.add(prompt_length, tokens)
        for response_length, tokens in connection.execute(
            """
            SELECT c.response_length, i.output_tokens
            FROM completions c
            JOIN inferences i ON i.completion_id = c.completion_id
            WHERE c.status = 'SUCCESS'
            """
        ):
            response_token_correlation.add(response_length, tokens)
        if prompt_token_correlation.value() < 0.85:
            raise ValueError("Prompt length and input tokens are weakly correlated")
        if response_token_correlation.value() < 0.85:
            raise ValueError("Response length and output tokens are weakly correlated")

        model_medians = {
            model_id: metric.summary()["median"]
            for model_id, metric in latency_by_model.items()
            if metric.count
        }
        if len(model_medians) < 6 or max(model_medians.values()) < 1.5 * min(
            model_medians.values()
        ):
            raise ValueError("Model latency profiles are not measurably different")

        active_model_ids = {
            model["model_id"] for model in models if model["active_flag"]
        }
        recent_start_us = _timestamp_us(generation_end - timedelta(days=90))
        recent_total, recent_active = connection.execute(
            """
            SELECT COUNT(*), SUM(CASE WHEN model_id IN (?, ?, ?, ?) THEN 1 ELSE 0 END)
            FROM inferences
            WHERE request_us >= ?
            """,
            (*sorted(active_model_ids), recent_start_us),
        ).fetchone()
        if not recent_total or recent_active / recent_total < 0.75:
            raise ValueError("Current models do not dominate recent traffic")

        newest_model = max(models, key=lambda model: model["release_date"])
        release_at = datetime.combine(
            newest_model["release_date"],
            time.min,
            timezone.utc,
        )
        early_end_us = _timestamp_us(release_at + timedelta(days=60))
        release_us = _timestamp_us(release_at)
        latest_start_us = _timestamp_us(generation_end - timedelta(days=60))
        early_total, early_newest = connection.execute(
            """
            SELECT COUNT(*), SUM(CASE WHEN model_id = ? THEN 1 ELSE 0 END)
            FROM inferences
            WHERE request_us >= ? AND request_us < ?
            """,
            (newest_model["model_id"], release_us, early_end_us),
        ).fetchone()
        latest_total, latest_newest = connection.execute(
            """
            SELECT COUNT(*), SUM(CASE WHEN model_id = ? THEN 1 ELSE 0 END)
            FROM inferences
            WHERE request_us >= ?
            """,
            (newest_model["model_id"], latest_start_us),
        ).fetchone()
        early_share = early_newest / early_total if early_total else 0
        latest_share = latest_newest / latest_total if latest_total else 0
        if latest_share <= early_share * 1.5:
            raise ValueError("Newest-model adoption does not increase over time")

        for entity_name, partition_dates in ingestion.partition_dates.items():
            if len(partition_dates) < 2:
                raise ValueError(f"{entity_name} does not span multiple partitions")
        if ingestion.categories["over_one_day"] == 0:
            raise ValueError("No genuinely late-arriving Milestone 5 records exist")

        connection.close()

    model_traffic = {
        model_id: {
            "model_name": models_by_id[model_id]["model_name"],
            "model_version": models_by_id[model_id]["model_version"],
            "count": model_counts[model_id],
            "percentage": model_counts[model_id] / inference_count * 100,
        }
        for model_id in models_by_id
    }
    partition_report = {
        entity_name: {
            "count": len(partition_dates),
            "earliest": min(partition_dates),
            "latest": max(partition_dates),
        }
        for entity_name, partition_dates in ingestion.partition_dates.items()
    }
    return {
        "source_messages": source_count,
        "completions": completion_count,
        "missing_completions": source_count - completion_count,
        "coverage_percentage": coverage * 100,
        "completion_status": status_counts,
        "model_inferences": inference_count,
        "model_traffic": model_traffic,
        "latency": latency.summary(),
        "latency_by_model": {
            model_id: {
                "median": metric.summary()["median"],
                "p95": metric.summary()["p95"],
            }
            for model_id, metric in latency_by_model.items()
            if metric.count
        },
        "input_tokens": input_tokens.summary(),
        "output_tokens": output_tokens.summary(),
        "response_lengths": response_lengths.summary(),
        "prompt_input_correlation": prompt_token_correlation.value(),
        "response_output_correlation": response_token_correlation.value(),
        "ingestion_delay": ingestion.delays.summary(),
        "arrival_categories": ingestion.categories,
        "partitions": partition_report,
        "duplicate_verification": "PASS",
        "causal_validation": "PASS",
        "model_migration_validation": "PASS",
    }


def local_source_messages(output_root: str | Path) -> Iterator[dict[str, Any]]:
    for message, _ in iter_local_partitioned_records("messages", output_root):
        yield message


def validate_local_inference_data(
    partition_date: date,
    output_root: str | Path,
) -> dict[str, Any]:
    models, _ = load_existing_models(output_root)
    return validate_serialized_inference_data(
        local_source_messages(output_root),
        iter_local_partitioned_records("completions", output_root),
        iter_local_partitioned_records("model_inferences", output_root),
        models,
        _generation_end(partition_date),
    )


def validate_s3_inference_data(
    client: Any,
    bucket: str,
    partition_date: date,
    source_root: str | Path,
) -> dict[str, Any]:
    """Validate the full downloaded S3 population against local source messages."""
    models, _ = load_existing_models(source_root)
    return validate_serialized_inference_data(
        local_source_messages(source_root),
        iter_s3_partitioned_records(client, bucket, "completions"),
        iter_s3_partitioned_records(client, bucket, "model_inferences"),
        models,
        _generation_end(partition_date),
    )


def print_inference_summary(summary: dict[str, Any]) -> None:
    print("\nSOURCE MESSAGES:")
    print(f"total: {summary['source_messages']:,}")
    print("\nCOMPLETION COVERAGE:")
    print(f"with completion: {summary['completions']:,}")
    print(f"without completion: {summary['missing_completions']:,}")
    print(f"coverage: {summary['coverage_percentage']:.4f}%")
    print("\nCOMPLETION STATUS:")
    for status in COMPLETION_STATUSES:
        print(f"{status}: {summary['completion_status'][status]:,}")
    print(f"\nMODEL INFERENCES: {summary['model_inferences']:,}")
    print("\nMODEL TRAFFIC:")
    for model_id, traffic in summary["model_traffic"].items():
        print(
            f"{model_id} {traffic['model_name']} {traffic['model_version']}: "
            f"{traffic['count']:,} ({traffic['percentage']:.4f}%)"
        )
    print("\nLATENCY MS:")
    for name in ("average", "median", "p95", "p99", "max"):
        print(f"{name}: {summary['latency'][name]:,.2f}")
    print("\nLATENCY BY MODEL (median / p95):")
    for model_id, metrics in summary["latency_by_model"].items():
        print(f"{model_id}: {metrics['median']:,} / {metrics['p95']:,}")
    for label, key in (
        ("INPUT TOKENS", "input_tokens"),
        ("OUTPUT TOKENS", "output_tokens"),
        ("RESPONSE LENGTH CHARACTERS", "response_lengths"),
    ):
        print(f"\n{label}:")
        for name in ("average", "median", "p95", "max"):
            print(f"{name}: {summary[key][name]:,.2f}")
    print("\nCORRELATIONS:")
    print(f"prompt/input tokens: {summary['prompt_input_correlation']:.4f}")
    print(f"response/output tokens: {summary['response_output_correlation']:.4f}")
    print("\nINGESTION DELAY SECONDS:")
    for name in ("median", "p95", "p99", "max"):
        print(f"{name}: {summary['ingestion_delay'][name]:,.0f}")
    print("\nARRIVALS:")
    total_events = summary["completions"] + summary["model_inferences"]
    for category in ("same_minute", "same_day", "next_day", "over_one_day"):
        count = summary["arrival_categories"][category]
        print(f"{category}: {count:,} ({count / total_events:.4%})")
    print("\nDAILY PARTITIONS:")
    for entity_name, metrics in summary["partitions"].items():
        print(
            f"{entity_name}: {metrics['count']} "
            f"({metrics['earliest']} to {metrics['latest']})"
        )
