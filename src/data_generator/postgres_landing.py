"""Stream S3 Bronze NDJSON records into PostgreSQL landing tables."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Sequence

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


LANDING_SCHEMA = "landing"
LINEAGE_COLUMNS = (
    ("source_partition_date", "DATE"),
    ("source_file", "TEXT"),
    ("loaded_at", "TIMESTAMPTZ"),
)
EXPECTED_BRONZE_COUNTS = {
    "models": 8,
    "devices": 12,
    "subscription_plans": 5,
    "users": 10_000,
    "user_updates": 1_750,
    "conversations": 116_701,
    "messages": 1_134_771,
    "completions": 1_118_882,
    "model_inferences": 1_118_623,
    "feedback": 69_606,
    "errors": 24_736,
    "subscriptions": 13_153,
    "purchases": 12_236,
    "payments": 13_202,
}
EXPECTED_MISSING_COUNTS = {
    ("users", "email"): 44,
    ("messages", "message_text"): 4_797,
    ("completions", "user_id"): 5_142,
    ("model_inferences", "model_id"): 4_237,
    ("feedback", "feedback_type"): 371,
    ("purchases", "purchase_type"): 51,
    ("payments", "payment_method"): 45,
}
EXPECTED_DUPLICATE_INCREASES = {
    "messages": 2_053,
    "completions": 1_914,
    "model_inferences": 1_655,
    "feedback": 70,
    "payments": 32,
}
EXPECTED_TEMPORAL_VIOLATIONS = 1_552
COPY_BATCH_SIZE = 10_000
S3_OBJECT_READ_ATTEMPTS = 3
_PARTITION_RE = re.compile(
    r"^bronze/(?P<entity>[a-z_]+)/dt=(?P<date>\d{4}-\d{2}-\d{2})/[^/]+\.json$"
)


@dataclass(frozen=True)
class EntitySpec:
    """Explicit landing table definition for one Bronze entity."""

    name: str
    id_field: str
    columns: tuple[tuple[str, str], ...]

    @property
    def source_column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    @property
    def all_columns(self) -> tuple[tuple[str, str], ...]:
        return self.columns + LINEAGE_COLUMNS

    @property
    def all_column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.all_columns)


def _spec(
    name: str,
    id_field: str,
    *columns: tuple[str, str],
) -> EntitySpec:
    return EntitySpec(name=name, id_field=id_field, columns=columns)


ENTITY_SPECS = {
    spec.name: spec
    for spec in (
        _spec(
            "models",
            "model_id",
            ("model_id", "TEXT"),
            ("model_name", "TEXT"),
            ("model_version", "TEXT"),
            ("provider", "TEXT"),
            ("release_date", "DATE"),
            ("active_flag", "BOOLEAN"),
        ),
        _spec(
            "devices",
            "device_id",
            ("device_id", "TEXT"),
            ("device_type", "TEXT"),
            ("operating_system", "TEXT"),
            ("browser", "TEXT"),
            ("app_platform", "TEXT"),
        ),
        _spec(
            "subscription_plans",
            "plan_id",
            ("plan_id", "TEXT"),
            ("plan_name", "TEXT"),
            ("monthly_price", "NUMERIC(18, 2)"),
            ("currency", "TEXT"),
            ("active_flag", "BOOLEAN"),
        ),
        _spec(
            "users",
            "user_id",
            ("user_id", "TEXT"),
            ("email", "TEXT"),
            ("name", "TEXT"),
            ("country_code", "TEXT"),
            ("account_status", "TEXT"),
            ("signup_source", "TEXT"),
            ("signup_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "user_updates",
            "update_id",
            ("update_id", "TEXT"),
            ("user_id", "TEXT"),
            ("field_name", "TEXT"),
            ("old_value", "TEXT"),
            ("new_value", "TEXT"),
            ("updated_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "conversations",
            "conversation_id",
            ("conversation_id", "TEXT"),
            ("user_id", "TEXT"),
            ("created_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "messages",
            "message_id",
            ("message_id", "TEXT"),
            ("conversation_id", "TEXT"),
            ("user_id", "TEXT"),
            ("device_id", "TEXT"),
            ("sequence_number", "BIGINT"),
            ("message_text", "TEXT"),
            ("created_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "completions",
            "completion_id",
            ("completion_id", "TEXT"),
            ("message_id", "TEXT"),
            ("conversation_id", "TEXT"),
            ("user_id", "TEXT"),
            ("completion_status", "TEXT"),
            ("requested_at", "TIMESTAMPTZ"),
            ("completed_at", "TIMESTAMPTZ"),
            ("response_text", "TEXT"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "model_inferences",
            "inference_id",
            ("inference_id", "TEXT"),
            ("completion_id", "TEXT"),
            ("user_id", "TEXT"),
            ("model_id", "TEXT"),
            ("request_at", "TIMESTAMPTZ"),
            ("response_at", "TIMESTAMPTZ"),
            ("latency_ms", "BIGINT"),
            ("input_tokens", "BIGINT"),
            ("output_tokens", "BIGINT"),
            ("inference_status", "TEXT"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "feedback",
            "feedback_id",
            ("feedback_id", "TEXT"),
            ("completion_id", "TEXT"),
            ("user_id", "TEXT"),
            ("feedback_type", "TEXT"),
            ("feedback_score", "INTEGER"),
            ("created_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "errors",
            "error_id",
            ("error_id", "TEXT"),
            ("user_id", "TEXT"),
            ("conversation_id", "TEXT"),
            ("message_id", "TEXT"),
            ("completion_id", "TEXT"),
            ("inference_id", "TEXT"),
            ("model_id", "TEXT"),
            ("error_source", "TEXT"),
            ("error_type", "TEXT"),
            ("error_code", "TEXT"),
            ("severity", "TEXT"),
            ("occurred_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "subscriptions",
            "subscription_id",
            ("subscription_id", "TEXT"),
            ("user_id", "TEXT"),
            ("plan_id", "TEXT"),
            ("started_at", "TIMESTAMPTZ"),
            ("ended_at", "TIMESTAMPTZ"),
            ("subscription_status", "TEXT"),
            ("actual_monthly_price", "NUMERIC(18, 2)"),
            ("updated_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "purchases",
            "purchase_id",
            ("purchase_id", "TEXT"),
            ("user_id", "TEXT"),
            ("subscription_id", "TEXT"),
            ("plan_id", "TEXT"),
            ("purchase_type", "TEXT"),
            ("purchase_status", "TEXT"),
            ("subtotal_amount", "NUMERIC(18, 2)"),
            ("discount_amount", "NUMERIC(18, 2)"),
            ("tax_amount", "NUMERIC(18, 2)"),
            ("total_amount", "NUMERIC(18, 2)"),
            ("purchase_created_at", "TIMESTAMPTZ"),
            ("updated_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
        _spec(
            "payments",
            "payment_id",
            ("payment_id", "TEXT"),
            ("purchase_id", "TEXT"),
            ("user_id", "TEXT"),
            ("payment_status", "TEXT"),
            ("payment_method", "TEXT"),
            ("payment_amount", "NUMERIC(18, 2)"),
            ("refund_amount", "NUMERIC(18, 2)"),
            ("processed_at", "TIMESTAMPTZ"),
            ("refunded_at", "TIMESTAMPTZ"),
            ("updated_at", "TIMESTAMPTZ"),
            ("ingested_at", "TIMESTAMPTZ"),
        ),
    )
}


@dataclass(frozen=True)
class LoadResult:
    entity: str
    files: int
    rows: int
    elapsed_seconds: float
    representative_checks: int
    partitions: int = 0
    partition_dates: tuple[date, ...] = ()
    reprocessed: bool = False


def partition_date_from_key(key: str, entity: str | None = None) -> date:
    """Extract and validate the physical partition date from a Bronze key."""
    match = _PARTITION_RE.fullmatch(key)
    if not match or (entity is not None and match.group("entity") != entity):
        raise ValueError(f"Invalid Bronze S3 key: {key}")
    try:
        return date.fromisoformat(match.group("date"))
    except ValueError as exc:
        raise ValueError(f"Invalid Bronze S3 key: {key}") from exc


def record_to_postgres_row(
    spec: EntitySpec,
    record: dict[str, Any],
    *,
    partition_date: date,
    source_file: str,
    loaded_at: datetime,
) -> tuple[Any, ...]:
    """Map one record without cleaning, deduplication, or timestamp repair."""
    unexpected = set(record).difference(spec.source_column_names)
    if unexpected:
        raise ValueError(
            f"Unexpected {spec.name} source columns: {sorted(unexpected)}"
        )
    return tuple(record.get(name) for name in spec.source_column_names) + (
        partition_date,
        source_file,
        loaded_at,
    )


def list_bronze_keys(client: Any, bucket: str, entity: str) -> list[str]:
    """Return all NDJSON keys for an entity using paginated S3 listing."""
    keys: list[str] = []
    prefix = f"bronze/{entity}/dt="
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(
                item["Key"]
                for item in page.get("Contents", [])
                if item["Key"].endswith(".json")
            )
    except (BotoCoreError, ClientError, OSError) as exc:
        raise RuntimeError(f"Failed to list s3://{bucket}/{prefix}: {exc}") from exc
    return sorted(keys)


def list_bronze_partition_dates(
    client: Any,
    bucket: str,
    entity: str,
) -> list[date]:
    """Discover entity partition folders without downloading their objects."""
    prefix = f"bronze/{entity}/"
    dates = set()
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
        ):
            for item in page.get("CommonPrefixes", []):
                partition_prefix = item.get("Prefix", "")
                if not partition_prefix.startswith(prefix + "dt="):
                    continue
                value = partition_prefix.removeprefix(prefix + "dt=").rstrip("/")
                try:
                    dates.add(date.fromisoformat(value))
                except ValueError as exc:
                    raise ValueError(
                        f"Invalid Bronze partition prefix: {partition_prefix}"
                    ) from exc
    except (BotoCoreError, ClientError, OSError) as exc:
        raise RuntimeError(f"Failed to list s3://{bucket}/{prefix}: {exc}") from exc
    return sorted(dates)


def list_bronze_partition_keys(
    client: Any,
    bucket: str,
    entity: str,
    partition_date: date,
) -> list[str]:
    """List only the objects in one exact entity/date partition."""
    prefix = f"bronze/{entity}/dt={partition_date.isoformat()}/"
    keys = []
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(
                item["Key"]
                for item in page.get("Contents", [])
                if item["Key"].endswith(".json")
            )
    except (BotoCoreError, ClientError, OSError) as exc:
        raise RuntimeError(f"Failed to list s3://{bucket}/{prefix}: {exc}") from exc
    return sorted(keys)


def select_incremental_dates(
    available_dates: Sequence[date],
    watermark: date | None,
    *,
    lookback_days: int = 0,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[list[date], bool]:
    """Select append dates or a bounded transactional reprocessing window."""
    available = sorted(set(available_dates))
    if start_date is not None or end_date is not None:
        lower = start_date or date.min
        upper = end_date or date.max
        return [value for value in available if lower <= value <= upper], True
    if lookback_days:
        if watermark is None:
            return available, False
        anchor = max(watermark, max(available, default=watermark))
        lower = anchor - timedelta(days=lookback_days)
        return [value for value in available if value >= lower], True
    if watermark is None:
        return available, False
    return [value for value in available if value > watermark], False


def get_entity_watermark(cursor: Any, entity: str) -> date | None:
    """Use the landing table's physical partition date as its watermark."""
    cursor.execute(
        f'SELECT MAX(source_partition_date) '
        f'FROM "{LANDING_SCHEMA}"."{entity}"'
    )
    return cursor.fetchone()[0]


def create_table_sql(spec: EntitySpec) -> str:
    """Build the explicit unconstrained landing table DDL."""
    definitions = ",\n    ".join(
        f'"{name}" {sql_type}' for name, sql_type in spec.all_columns
    )
    return (
        f'CREATE TABLE IF NOT EXISTS "{LANDING_SCHEMA}"."{spec.name}" (\n'
        f"    {definitions}\n"
        ")"
    )


def _copy_sql(spec: EntitySpec) -> str:
    columns = ", ".join(f'"{name}"' for name in spec.all_column_names)
    return (
        f'COPY "{LANDING_SCHEMA}"."{spec.name}" ({columns}) '
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    )


def _write_copy_value(value: Any) -> Any:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _flush_copy_batch(cursor: Any, spec: EntitySpec, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerows(tuple(_write_copy_value(value) for value in row) for row in rows)
    stream.seek(0)
    cursor.copy_expert(_copy_sql(spec), stream)
    rows.clear()


def _read_s3_object_rows(
    s3_client: Any,
    bucket: str,
    key: str,
    spec: EntitySpec,
    loaded_at: datetime,
) -> list[tuple[Any, ...]]:
    """Buffer and retry one modest daily object before copying any of its rows."""
    partition_date = partition_date_from_key(key, spec.name)
    source_file = f"s3://{bucket}/{key}"
    for attempt in range(1, S3_OBJECT_READ_ATTEMPTS + 1):
        body = None
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            object_rows: list[tuple[Any, ...]] = []
            for line_number, line in enumerate(body.iter_lines(), start=1):
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        f"Invalid NDJSON at {source_file}:{line_number}"
                    ) from exc
                object_rows.append(
                    record_to_postgres_row(
                        spec,
                        record,
                        partition_date=partition_date,
                        source_file=source_file,
                        loaded_at=loaded_at,
                    )
                )
            return object_rows
        except (BotoCoreError, ClientError, OSError) as exc:
            if attempt == S3_OBJECT_READ_ATTEMPTS:
                raise RuntimeError(
                    f"Failed reading {source_file} after {attempt} attempts: {exc}"
                ) from exc
            print(
                f"RETRY entity={spec.name} source_file={key} "
                f"attempt={attempt + 1}/{S3_OBJECT_READ_ATTEMPTS}"
            )
        finally:
            if body is not None:
                body.close()


def _normalize_expected(value: Any, sql_type: str) -> Any:
    if value is None:
        return None
    if sql_type == "DATE":
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    if sql_type == "TIMESTAMPTZ":
        if isinstance(value, datetime):
            parsed = value
        else:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    if sql_type.startswith("NUMERIC"):
        return Decimal(str(value))
    if sql_type in {"INTEGER", "BIGINT"}:
        return int(value)
    if sql_type == "BOOLEAN":
        return bool(value)
    return str(value)


def _validate_representatives(
    cursor: Any,
    spec: EntitySpec,
    representatives: Sequence[tuple[Any, ...]],
) -> int:
    selected_columns = ", ".join(
        f'"{name}"' for name in spec.all_column_names
    )
    id_index = spec.all_column_names.index(spec.id_field)
    source_file_index = spec.all_column_names.index("source_file")
    checked = 0
    for expected_row in representatives:
        cursor.execute(
            f'SELECT {selected_columns} FROM "{LANDING_SCHEMA}"."{spec.name}" '
            f'WHERE "{spec.id_field}" = %s AND "source_file" = %s',
            (expected_row[id_index], expected_row[source_file_index]),
        )
        candidates = cursor.fetchall()
        normalized = tuple(
            _normalize_expected(value, sql_type)
            for value, (_, sql_type) in zip(expected_row, spec.all_columns)
        )
        if normalized not in candidates:
            raise ValueError(
                f"Deterministic content check failed for {spec.name} "
                f"{spec.id_field}={expected_row[id_index]}"
            )
        checked += 1
    return checked


def _validate_partition_physical_shape(
    cursor: Any,
    spec: EntitySpec,
    partition_dates: Sequence[date],
    expected_null_counts: Counter[str],
    expected_source_files: Counter[str],
) -> None:
    """Reconcile physical NULL and source-file counts for loaded partitions."""
    cursor.execute(
        f'SELECT source_file, COUNT(*) '
        f'FROM "{LANDING_SCHEMA}"."{spec.name}" '
        "WHERE source_partition_date = ANY(%s) "
        "GROUP BY source_file",
        (list(partition_dates),),
    )
    actual_source_files = Counter(dict(cursor.fetchall()))
    if actual_source_files != expected_source_files:
        raise ValueError(
            f"{spec.name} source_file reconciliation failed: "
            f"PostgreSQL={dict(actual_source_files)}, "
            f"S3={dict(expected_source_files)}"
        )

    null_expressions = ", ".join(
        f'SUM(CASE WHEN "{name}" IS NULL THEN 1 ELSE 0 END)'
        for name in spec.source_column_names
    )
    cursor.execute(
        f'SELECT {null_expressions} '
        f'FROM "{LANDING_SCHEMA}"."{spec.name}" '
        "WHERE source_partition_date = ANY(%s)",
        (list(partition_dates),),
    )
    actual_values = cursor.fetchone()
    actual_null_counts = Counter(
        {
            name: int(value or 0)
            for name, value in zip(spec.source_column_names, actual_values)
        }
    )
    if actual_null_counts != expected_null_counts:
        raise ValueError(
            f"{spec.name} NULL reconciliation failed: "
            f"PostgreSQL={dict(actual_null_counts)}, "
            f"S3={dict(expected_null_counts)}"
        )


def load_entity(
    connection: Any,
    s3_client: Any,
    bucket: str,
    entity: str,
    *,
    batch_size: int = COPY_BATCH_SIZE,
) -> LoadResult:
    """Full-refresh one landing table atomically from S3 Bronze."""
    if entity not in ENTITY_SPECS:
        raise ValueError(f"Unsupported entity: {entity}")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    spec = ENTITY_SPECS[entity]
    keys = list_bronze_keys(s3_client, bucket, entity)
    if not keys:
        raise RuntimeError(f"No S3 Bronze files found for {entity}")

    started = time.monotonic()
    loaded_at = datetime.now(timezone.utc)
    representatives: list[tuple[Any, ...]] = []
    batch: list[tuple[Any, ...]] = []
    rows_loaded = 0

    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{LANDING_SCHEMA}"')
            cursor.execute(create_table_sql(spec))
            cursor.execute(f'TRUNCATE TABLE "{LANDING_SCHEMA}"."{entity}"')
            sample_key_indexes = {0, len(keys) // 2, len(keys) - 1}
            for key_index, key in enumerate(keys):
                object_rows = _read_s3_object_rows(
                    s3_client, bucket, key, spec, loaded_at
                )
                if key_index in sample_key_indexes and object_rows:
                    representatives.append(object_rows[len(object_rows) // 2])
                for row in object_rows:
                    batch.append(row)
                    rows_loaded += 1
                    if len(batch) >= batch_size:
                        _flush_copy_batch(cursor, spec, batch)
            _flush_copy_batch(cursor, spec, batch)
            cursor.execute(
                f'SELECT COUNT(*) FROM "{LANDING_SCHEMA}"."{entity}"'
            )
            database_count = cursor.fetchone()[0]
            if database_count != rows_loaded:
                raise ValueError(
                    f"{entity} count mismatch: S3={rows_loaded:,}, "
                    f"PostgreSQL={database_count:,}"
                )
            representative_checks = _validate_representatives(
                cursor, spec, representatives
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    elapsed = time.monotonic() - started
    result = LoadResult(
        entity=entity,
        files=len(keys),
        rows=rows_loaded,
        elapsed_seconds=elapsed,
        representative_checks=representative_checks,
    )
    print(
        f"PASS entity={entity} files={result.files:,} rows={result.rows:,} "
        f"seconds={result.elapsed_seconds:.2f} "
        f"content_checks={result.representative_checks}"
    )
    return result


def load_entity_incremental(
    connection: Any,
    s3_client: Any,
    bucket: str,
    entity: str,
    *,
    batch_size: int = COPY_BATCH_SIZE,
    lookback_days: int = 0,
    start_date: date | None = None,
    end_date: date | None = None,
) -> LoadResult:
    """Append or transactionally reprocess selected Bronze partitions."""
    if entity not in ENTITY_SPECS:
        raise ValueError(f"Unsupported entity: {entity}")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if lookback_days < 0:
        raise ValueError("lookback_days cannot be negative")
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    spec = ENTITY_SPECS[entity]
    available_dates = list_bronze_partition_dates(s3_client, bucket, entity)
    started = time.monotonic()
    loaded_at = datetime.now(timezone.utc)
    rows_loaded = 0
    representatives: list[tuple[Any, ...]] = []
    files = 0
    expected_null_counts: Counter[str] = Counter(
        {name: 0 for name in spec.source_column_names}
    )
    expected_source_files: Counter[str] = Counter()

    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{LANDING_SCHEMA}"')
            cursor.execute(create_table_sql(spec))
            watermark = get_entity_watermark(cursor, entity)
            selected_dates, reprocessed = select_incremental_dates(
                available_dates,
                watermark,
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
            )
            if not selected_dates:
                connection.commit()
                result = LoadResult(
                    entity=entity,
                    files=0,
                    rows=0,
                    elapsed_seconds=time.monotonic() - started,
                    representative_checks=0,
                    partitions=0,
                    partition_dates=(),
                    reprocessed=False,
                )
                print(
                    f"NOOP entity={entity} watermark={watermark} "
                    "partitions=0 files=0 rows=0"
                )
                return result

            keys_by_date = {
                partition: list_bronze_partition_keys(
                    s3_client, bucket, entity, partition
                )
                for partition in selected_dates
            }
            missing_objects = [
                partition
                for partition, keys in keys_by_date.items()
                if not keys
            ]
            if missing_objects:
                raise RuntimeError(
                    f"No Bronze objects found for {entity} partitions: "
                    + ", ".join(map(str, missing_objects))
                )

            if reprocessed:
                cursor.execute(
                    f'DELETE FROM "{LANDING_SCHEMA}"."{entity}" '
                    "WHERE source_partition_date = ANY(%s)",
                    (selected_dates,),
                )

            batch: list[tuple[Any, ...]] = []
            for partition in selected_dates:
                for key in keys_by_date[partition]:
                    object_rows = _read_s3_object_rows(
                        s3_client,
                        bucket,
                        key,
                        spec,
                        loaded_at,
                    )
                    files += 1
                    if object_rows:
                        representatives.append(object_rows[len(object_rows) // 2])
                    for row in object_rows:
                        expected_source_files[row[-2]] += 1
                        for index, name in enumerate(spec.source_column_names):
                            if row[index] is None:
                                expected_null_counts[name] += 1
                        batch.append(row)
                        rows_loaded += 1
                        if len(batch) >= batch_size:
                            _flush_copy_batch(cursor, spec, batch)
            _flush_copy_batch(cursor, spec, batch)

            cursor.execute(
                f'SELECT COUNT(*) FROM "{LANDING_SCHEMA}"."{entity}" '
                "WHERE source_partition_date = ANY(%s)",
                (selected_dates,),
            )
            database_count = cursor.fetchone()[0]
            if database_count != rows_loaded:
                raise ValueError(
                    f"{entity} partition count mismatch: S3={rows_loaded:,}, "
                    f"PostgreSQL={database_count:,}"
                )
            _validate_partition_physical_shape(
                cursor,
                spec,
                selected_dates,
                expected_null_counts,
                expected_source_files,
            )
            representative_checks = _validate_representatives(
                cursor,
                spec,
                representatives,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    result = LoadResult(
        entity=entity,
        files=files,
        rows=rows_loaded,
        elapsed_seconds=time.monotonic() - started,
        representative_checks=representative_checks,
        partitions=len(selected_dates),
        partition_dates=tuple(selected_dates),
        reprocessed=reprocessed,
    )
    action = "REPROCESS" if reprocessed else "APPEND"
    print(
        f"PASS action={action} entity={entity} "
        f"partitions={result.partitions:,} files={result.files:,} "
        f"rows={result.rows:,} seconds={result.elapsed_seconds:.2f} "
        f"content_checks={result.representative_checks}"
    )
    return result


def validate_quality_preservation(connection: Any) -> dict[str, Any]:
    """Validate known missing, duplicate, and temporal Bronze defects."""
    missing: dict[str, int] = {}
    duplicates: dict[str, int] = {}
    with connection.cursor() as cursor:
        for (entity, field), expected in EXPECTED_MISSING_COUNTS.items():
            cursor.execute(
                f'SELECT COUNT(*) FROM "{LANDING_SCHEMA}"."{entity}" '
                f'WHERE "{field}" IS NULL'
            )
            actual = cursor.fetchone()[0]
            if actual != expected:
                raise ValueError(
                    f"Missing-value mismatch for {entity}.{field}: "
                    f"actual={actual:,}, expected={expected:,}"
                )
            missing[f"{entity}.{field}"] = actual

        for entity, expected in EXPECTED_DUPLICATE_INCREASES.items():
            spec = ENTITY_SPECS[entity]
            cursor.execute(
                f'SELECT COUNT(*) - COUNT(DISTINCT "{spec.id_field}") '
                f'FROM "{LANDING_SCHEMA}"."{entity}"'
            )
            actual = cursor.fetchone()[0]
            if actual != expected:
                raise ValueError(
                    f"Duplicate mismatch for {entity}: "
                    f"actual={actual:,}, expected={expected:,}"
                )
            duplicates[entity] = actual

        temporal_queries = {
            "message_before_conversation": """
                SELECT COUNT(DISTINCT m.message_id)
                FROM landing.messages AS m
                JOIN landing.conversations AS c USING (conversation_id)
                WHERE m.created_at < c.created_at
            """,
            "completion_order": """
                SELECT COUNT(DISTINCT completion_id)
                FROM landing.completions
                WHERE completed_at < requested_at
            """,
            "inference_order": """
                SELECT COUNT(DISTINCT inference_id)
                FROM landing.model_inferences
                WHERE response_at < request_at
            """,
            "payment_before_purchase": """
                SELECT COUNT(DISTINCT p.payment_id)
                FROM landing.payments AS p
                JOIN landing.purchases AS o USING (purchase_id)
                WHERE p.processed_at < o.purchase_created_at
            """,
        }
        temporal: dict[str, int] = {}
        for category, query in temporal_queries.items():
            cursor.execute(query)
            temporal[category] = cursor.fetchone()[0]
        temporal_total = sum(temporal.values())
        if temporal_total != EXPECTED_TEMPORAL_VIOLATIONS:
            raise ValueError(
                "Temporal-defect mismatch: "
                f"actual={temporal_total:,}, "
                f"expected={EXPECTED_TEMPORAL_VIOLATIONS:,}"
            )

    return {
        "missing": missing,
        "duplicates": duplicates,
        "temporal": temporal,
        "temporal_total": temporal_total,
    }


def validate_landing_schema(connection: Any) -> dict[str, list[tuple[Any, ...]]]:
    """Return and validate the landing table and column inventory."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (LANDING_SCHEMA,),
        )
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = sorted(ENTITY_SPECS)
        if tables != expected_tables:
            raise ValueError(
                f"Landing table inventory mismatch: {tables} != {expected_tables}"
            )
        cursor.execute(
            """
            SELECT table_name, column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
            """,
            (LANDING_SCHEMA,),
        )
        columns = cursor.fetchall()
    return {"tables": tables, "columns": columns}


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_s3_client() -> tuple[Any, str]:
    """Create an S3 client through the standard AWS credential provider chain."""
    bucket = _required_environment("S3_BUCKET")
    client = boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            connect_timeout=10,
            read_timeout=300,
            retries={"max_attempts": 10, "mode": "standard"},
        ),
    )
    return client, bucket


def create_postgres_connection() -> Any:
    """Connect using the existing warehouse environment variables."""
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2 is required to load PostgreSQL") from exc
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_POSTGRES_HOST", "postgres-warehouse"),
        port=int(os.getenv("WAREHOUSE_POSTGRES_PORT", "5432")),
        user=_required_environment("WAREHOUSE_POSTGRES_USER"),
        password=_required_environment("WAREHOUSE_POSTGRES_PASSWORD"),
        dbname=_required_environment("WAREHOUSE_POSTGRES_DB"),
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load S3 Bronze data into PostgreSQL landing tables."
    )
    parser.add_argument("--entity", choices=tuple(ENTITY_SPECS))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--incremental",
        action="store_true",
        help="Append only new partitions, or reprocess a bounded requested window.",
    )
    mode.add_argument(
        "--all",
        action="store_true",
        help="Recovery mode: full-refresh all landing tables.",
    )
    mode.add_argument(
        "--full-refresh",
        action="store_true",
        help="Recovery mode: full-refresh --entity or all entities.",
    )
    parser.add_argument("--partition-date", type=date.fromisoformat)
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--lookback-days", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=COPY_BATCH_SIZE)
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    if args.lookback_days < 0:
        parser.error("--lookback-days cannot be negative")
    if args.all and args.entity:
        parser.error("--all cannot be combined with --entity")
    if not (args.incremental or args.all or args.full_refresh or args.entity):
        parser.error("choose --incremental, --all, --full-refresh, or --entity")
    date_options = sum(
        value is not None
        for value in (args.partition_date, args.start_date, args.end_date)
    )
    if date_options and not args.incremental:
        parser.error("partition/date filters require --incremental")
    if args.lookback_days and not args.incremental:
        parser.error("--lookback-days requires --incremental")
    if args.partition_date and (args.start_date or args.end_date):
        parser.error("--partition-date cannot be combined with a date range")
    if args.lookback_days and date_options:
        parser.error("--lookback-days cannot be combined with date filters")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("--start-date cannot be after --end-date")
    if args.partition_date:
        args.start_date = args.partition_date
        args.end_date = args.partition_date
    return args


def run(argv: Sequence[str] | None = None) -> list[LoadResult]:
    """CLI entry point for incremental daily or recovery full-refresh loads."""
    load_dotenv()
    args = parse_args(argv)
    entities: Iterable[str] = (
        (args.entity,)
        if args.entity
        else ENTITY_SPECS
    )
    incremental = args.incremental
    s3_client, bucket = create_s3_client()
    connection = create_postgres_connection()
    total_started = time.monotonic()
    results: list[LoadResult] = []
    try:
        for entity in entities:
            try:
                if incremental:
                    results.append(
                        load_entity_incremental(
                            connection,
                            s3_client,
                            bucket,
                            entity,
                            batch_size=args.batch_size,
                            lookback_days=args.lookback_days,
                            start_date=args.start_date,
                            end_date=args.end_date,
                        )
                    )
                else:
                    results.append(
                        load_entity(
                            connection,
                            s3_client,
                            bucket,
                            entity,
                            batch_size=args.batch_size,
                        )
                    )
            except Exception:
                print(f"FAIL entity={entity}")
                raise
        if not incremental and not args.entity:
            schema = validate_landing_schema(connection)
            print("PASS landing_tables=" + str(len(schema["tables"])))
    finally:
        connection.close()
    elapsed = time.monotonic() - total_started
    total_rows = sum(result.rows for result in results)
    total_files = sum(result.files for result in results)
    print(
        f"PASS total entities={len(results)} files={total_files:,} "
        f"rows={total_rows:,} seconds={elapsed:.2f}"
    )
    return results


if __name__ == "__main__":
    run()
