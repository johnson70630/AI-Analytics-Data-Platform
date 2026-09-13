"""Stream S3 Bronze NDJSON records into PostgreSQL landing tables."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
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
    expected_count = EXPECTED_BRONZE_COUNTS[entity]
    sample_indexes = {0, expected_count // 2, expected_count - 1}
    representatives: list[tuple[Any, ...]] = []
    batch: list[tuple[Any, ...]] = []
    rows_loaded = 0

    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{LANDING_SCHEMA}"')
            cursor.execute(create_table_sql(spec))
            cursor.execute(f'TRUNCATE TABLE "{LANDING_SCHEMA}"."{entity}"')
            for key in keys:
                object_rows = _read_s3_object_rows(
                    s3_client, bucket, key, spec, loaded_at
                )
                for row in object_rows:
                    if rows_loaded in sample_indexes:
                        representatives.append(row)
                    batch.append(row)
                    rows_loaded += 1
                    if len(batch) >= batch_size:
                        _flush_copy_batch(cursor, spec, batch)
            _flush_copy_batch(cursor, spec, batch)
            cursor.execute(
                f'SELECT COUNT(*) FROM "{LANDING_SCHEMA}"."{entity}"'
            )
            database_count = cursor.fetchone()[0]
            if rows_loaded != expected_count or database_count != expected_count:
                raise ValueError(
                    f"{entity} count mismatch: S3={rows_loaded:,}, "
                    f"PostgreSQL={database_count:,}, expected={expected_count:,}"
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
    """Create an S3 client from the project's existing environment settings."""
    bucket = _required_environment("S3_BUCKET")
    client = boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=_required_environment("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_required_environment("AWS_SECRET_ACCESS_KEY"),
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
        description="Full-refresh S3 Bronze data into PostgreSQL landing tables."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--entity", choices=tuple(ENTITY_SPECS))
    selection.add_argument("--all", action="store_true")
    parser.add_argument("--batch-size", type=int, default=COPY_BATCH_SIZE)
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    return args


def run(argv: Sequence[str] | None = None) -> list[LoadResult]:
    """CLI entry point for one-entity or all-entity loading."""
    load_dotenv()
    args = parse_args(argv)
    entities: Iterable[str] = ENTITY_SPECS if args.all else (args.entity,)
    s3_client, bucket = create_s3_client()
    connection = create_postgres_connection()
    total_started = time.monotonic()
    results: list[LoadResult] = []
    try:
        for entity in entities:
            try:
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
        if args.all:
            quality = validate_quality_preservation(connection)
            schema = validate_landing_schema(connection)
            print("PASS landing_tables=" + str(len(schema["tables"])))
            print("PASS missing_values=" + json.dumps(quality["missing"], sort_keys=True))
            print("PASS duplicates=" + json.dumps(quality["duplicates"], sort_keys=True))
            print("PASS temporal=" + json.dumps(quality["temporal"], sort_keys=True))
            print(f"PASS temporal_total={quality['temporal_total']:,}")
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
