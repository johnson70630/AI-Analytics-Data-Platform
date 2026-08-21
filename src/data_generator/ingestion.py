"""Shared event ingestion timestamps, daily partitioning, and validation."""

import json
import random
from collections import Counter, defaultdict
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .writers import write_records_locally, write_records_to_s3


EVENT_TIMESTAMP_FIELDS = {
    "users": ("signup_at",),
    "user_updates": ("updated_at",),
    "conversations": ("created_at",),
    "messages": ("created_at",),
    "completions": ("completed_at", "requested_at"),
    "model_inferences": ("response_at", "request_at"),
    "feedback": ("created_at",),
    "errors": ("occurred_at",),
    "subscriptions": ("updated_at",),
    "purchases": ("updated_at",),
    "payments": ("updated_at",),
}
RECORD_TIMESTAMP_FIELDS = {
    "users": ("signup_at", "ingested_at"),
    "user_updates": ("updated_at", "ingested_at"),
    "conversations": ("created_at", "ingested_at"),
    "messages": ("created_at", "ingested_at"),
    "completions": ("requested_at", "completed_at", "ingested_at"),
    "model_inferences": ("request_at", "response_at", "ingested_at"),
    "feedback": ("created_at", "ingested_at"),
    "errors": ("occurred_at", "ingested_at"),
    "subscriptions": (
        "started_at",
        "ended_at",
        "updated_at",
        "ingested_at",
    ),
    "purchases": (
        "purchase_created_at",
        "updated_at",
        "ingested_at",
    ),
    "payments": (
        "processed_at",
        "refunded_at",
        "updated_at",
        "ingested_at",
    ),
}
EVENT_ID_FIELDS = {
    "users": "user_id",
    "user_updates": "update_id",
    "conversations": "conversation_id",
    "messages": "message_id",
    "completions": "completion_id",
    "model_inferences": "inference_id",
    "feedback": "feedback_id",
    "errors": "error_id",
    "subscriptions": "subscription_id",
    "purchases": "purchase_id",
    "payments": "payment_id",
}
S3_REPLACEMENT_KEYS = {
    "users": "raw/users/dt=2026-08-20/2437a9a9-b719-4b22-8e77-1fa844e6e475.json",
    "user_updates": "raw/user_updates/dt=2026-08-20/0afd5e5d-f627-4f95-90f5-de46ef78ce91.json",
    "conversations": "raw/conversations/dt=2026-08-20/7513c7c0-eb18-4357-8c3b-b3318a1bf77f.json",
    "messages": "raw/messages/dt=2026-08-20/6b9b18dc-3160-41fa-af9e-e1c344920729.json",
}


def _ingestion_delay_seconds() -> int:
    delay_group = random.choices(
        ("FAST", "MINUTES", "HOURS", "LATE"),
        weights=(95, 4, 0.8, 0.2),
        k=1,
    )[0]
    if delay_group == "FAST":
        return random.randint(0, 30)
    if delay_group == "MINUTES":
        return random.randint(31, 1_800)
    if delay_group == "HOURS":
        return random.randint(1_801, 21_600)
    return random.randint(21_601, 172_800)


def ingestion_timestamp(
    event_timestamp: datetime,
    generation_end: datetime,
) -> datetime:
    """Return a bounded ingestion timestamp using the shared delay profile."""
    available_seconds = max(
        0,
        int((generation_end - event_timestamp).total_seconds()),
    )
    delay = min(_ingestion_delay_seconds(), available_seconds)
    return event_timestamp + timedelta(seconds=delay)


def primary_event_timestamp(
    entity_name: str,
    record: dict[str, Any],
) -> datetime:
    """Select the first available primary event timestamp for an entity."""
    for field_name in EVENT_TIMESTAMP_FIELDS[entity_name]:
        value = record.get(field_name)
        if isinstance(value, datetime):
            return value
    raise ValueError(f"{entity_name} record has no primary event timestamp")


def add_ingestion_timestamps(
    records: list[dict[str, Any]],
    event_timestamp_field: str,
    generation_end: datetime,
) -> list[dict[str, Any]]:
    """Copy event records and add a bounded, strongly skewed ingested_at."""
    enriched = []
    for record in records:
        event_timestamp = record[event_timestamp_field]
        enriched.append(
            {
                **record,
                "ingested_at": ingestion_timestamp(
                    event_timestamp,
                    generation_end,
                ),
            }
        )
    return enriched


def partition_by_ingestion_date(
    records: list[dict[str, Any]],
) -> dict[date, list[dict[str, Any]]]:
    partitions: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        partitions[record["ingested_at"].date()].append(record)
    return dict(sorted(partitions.items()))


def remove_local_event_outputs(
    entity_names: tuple[str, ...],
    output_root: str | Path,
) -> list[Path]:
    """Remove only prior generated files for explicitly named event entities."""
    removed = []
    for entity_name in entity_names:
        entity_root = Path(output_root) / "raw" / entity_name
        if not entity_root.exists():
            continue
        files = sorted(entity_root.glob("dt=*/*.json"))
        for path in files:
            path.unlink()
            removed.append(path)
        for directory in sorted(entity_root.glob("dt=*"), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
    return removed


def write_partitioned_records_locally(
    records: list[dict[str, Any]],
    entity_name: str,
    output_root: str | Path,
) -> list[Path]:
    partitions = partition_by_ingestion_date(records)
    paths = [
        write_records_locally(
            partition_records,
            entity_name,
            partition_date,
            output_root,
            verbose=False,
        )
        for partition_date, partition_records in partitions.items()
    ]
    print(
        f"Wrote {len(records):,} {entity_name} records across "
        f"{len(paths)} local daily partitions"
    )
    return paths


def write_partitioned_records_to_s3(
    records: list[dict[str, Any]],
    entity_name: str,
    *,
    bucket: str,
    region: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    replacement_key: str | None = None,
) -> list[str]:
    partitions = partition_by_ingestion_date(records)
    client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    keys = [
        write_records_to_s3(
            partition_records,
            entity_name,
            partition_date,
            bucket=bucket,
            region=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            client=client,
            verbose=False,
            object_key=(
                replacement_key
                if replacement_key
                and partition_date == max(partitions)
                else None
            ),
        )
        for partition_date, partition_records in partitions.items()
    ]
    print(
        f"Wrote {len(records):,} {entity_name} records across "
        f"{len(keys)} S3 daily partitions"
    )
    return keys


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_record_timestamps(
    entity_name: str,
    record: dict[str, Any],
) -> dict[str, Any]:
    for field_name in RECORD_TIMESTAMP_FIELDS[entity_name]:
        value = record.get(field_name)
        if isinstance(value, str):
            record[field_name] = _parse_utc(value)
    return record


def iter_local_partitioned_records(
    entity_name: str,
    output_root: str | Path,
    *,
    prefix: str = "raw",
) -> Iterator[tuple[dict[str, Any], date]]:
    """Yield parsed NDJSON records with their physical local partition date."""
    paths = sorted(
        (Path(output_root) / prefix / entity_name).glob("dt=*/*.json")
    )
    if not paths:
        raise RuntimeError(f"No local partitioned files found for {entity_name}")
    for path in paths:
        partition_date = date.fromisoformat(path.parent.name.removeprefix("dt="))
        try:
            with path.open(encoding="utf-8") as source:
                for line in source:
                    if line.strip():
                        yield (
                            _parse_record_timestamps(
                                entity_name,
                                json.loads(line),
                            ),
                            partition_date,
                        )
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Failed to reload {path}: {exc}") from exc


def load_local_partitioned_records(
    entity_name: str,
    output_root: str | Path,
    *,
    prefix: str = "raw",
) -> tuple[list[dict[str, Any]], list[date], list[Path]]:
    """Reload daily NDJSON files and retain each row's physical partition date."""
    paths = sorted(
        (Path(output_root) / prefix / entity_name).glob("dt=*/*.json")
    )
    if not paths:
        raise RuntimeError(f"No local partitioned files found for {entity_name}")
    records = []
    physical_dates = []
    for path in paths:
        partition_date = date.fromisoformat(path.parent.name.removeprefix("dt="))
        try:
            file_records = [
                json.loads(line)
                for line in path.read_text().splitlines()
                if line
            ]
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Failed to reload {path}: {exc}") from exc
        for record in file_records:
            _parse_record_timestamps(entity_name, record)
        records.extend(file_records)
        physical_dates.extend([partition_date] * len(file_records))
    return records, physical_dates, paths


def load_s3_partitioned_records(
    client: Any,
    bucket: str,
    entity_name: str,
) -> tuple[list[dict[str, Any]], list[date], list[str]]:
    """Download all daily entity objects with pagination and partition metadata."""
    paginator = client.get_paginator("list_objects_v2")
    keys = sorted(
        item["Key"]
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=f"raw/{entity_name}/dt=",
        )
        for item in page.get("Contents", [])
    )
    if not keys:
        raise RuntimeError(f"No S3 partitioned objects found for {entity_name}")
    records = []
    physical_dates = []
    for key in keys:
        response = client.get_object(Bucket=bucket, Key=key)
        if response.get("ContentType") != "application/x-ndjson":
            raise ValueError(
                f"Unexpected content type for s3://{bucket}/{key}: "
                f"{response.get('ContentType')}"
            )
        partition_date = date.fromisoformat(
            key.split("/dt=", 1)[1].split("/", 1)[0]
        )
        file_records = [
            json.loads(line)
            for line in response["Body"].read().decode("utf-8").splitlines()
            if line
        ]
        for record in file_records:
            _parse_record_timestamps(entity_name, record)
        records.extend(file_records)
        physical_dates.extend([partition_date] * len(file_records))
    return records, physical_dates, keys


def iter_s3_partitioned_records(
    client: Any,
    bucket: str,
    entity_name: str,
    *,
    prefix: str = "raw",
) -> Iterator[tuple[dict[str, Any], date]]:
    """Yield parsed S3 NDJSON records with their physical partition date."""
    paginator = client.get_paginator("list_objects_v2")
    keys = sorted(
        item["Key"]
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=f"{prefix}/{entity_name}/dt=",
        )
        for item in page.get("Contents", [])
    )
    if not keys:
        raise RuntimeError(f"No S3 partitioned objects found for {entity_name}")
    for key in keys:
        response = client.get_object(Bucket=bucket, Key=key)
        if response.get("ContentType") != "application/x-ndjson":
            raise ValueError(
                f"Unexpected content type for s3://{bucket}/{key}: "
                f"{response.get('ContentType')}"
            )
        partition_date = date.fromisoformat(
            key.split("/dt=", 1)[1].split("/", 1)[0]
        )
        try:
            for raw_line in response["Body"].iter_lines():
                if raw_line:
                    yield (
                        _parse_record_timestamps(
                            entity_name,
                            json.loads(raw_line.decode("utf-8")),
                        ),
                        partition_date,
                    )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Failed to reload s3://{bucket}/{key}: {exc}"
            ) from exc


def upload_local_partitioned_files(
    entity_name: str,
    output_root: str | Path,
    *,
    bucket: str,
    region: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    prefix: str = "raw",
) -> list[str]:
    """Upload validated local daily files to identical, idempotent S3 keys."""
    root = Path(output_root)
    paths = sorted((root / prefix / entity_name).glob("dt=*/*.json"))
    if not paths:
        raise RuntimeError(f"No local partitioned files found for {entity_name}")
    client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    keys = [path.relative_to(root).as_posix() for path in paths]
    existing = {
        item["Key"]
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=f"{prefix}/{entity_name}/dt=",
        )
        for item in page.get("Contents", [])
    }
    unexpected = existing.difference(keys)
    if unexpected:
        raise RuntimeError(
            f"Refusing to upload {entity_name}: found {len(unexpected)} "
            "unexpected existing S3 object(s)"
        )
    for path, key in zip(paths, keys):
        try:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=path.read_bytes(),
                ContentType="application/x-ndjson",
            )
        except (OSError, BotoCoreError, ClientError) as exc:
            raise RuntimeError(
                f"Failed to upload {path} to s3://{bucket}/{key}: {exc}"
            ) from exc
    print(
        f"Uploaded {len(paths)} validated {entity_name} daily files to S3"
    )
    return keys


def validate_ingestion_records(
    entity_name: str,
    records: list[dict[str, Any]],
    generation_end: datetime,
    physical_partition_dates: list[date] | None = None,
) -> None:
    """Validate ingestion ordering, horizon, partitions, and ID uniqueness."""
    id_field = EVENT_ID_FIELDS[entity_name]
    seen_ids = set()
    for index, record in enumerate(records):
        record_id = record.get(id_field)
        if not record_id or record_id in seen_ids:
            raise ValueError(
                f"{entity_name} ingestion validation failed: duplicate {id_field} "
                f"{record_id}"
            )
        seen_ids.add(record_id)
        event_timestamp = primary_event_timestamp(entity_name, record)
        ingested_at = record.get("ingested_at")
        if (
            not isinstance(ingested_at, datetime)
            or ingested_at < event_timestamp
            or ingested_at > generation_end
        ):
            raise ValueError(
                f"{entity_name} ingestion validation failed for {record_id}"
            )
        if (
            physical_partition_dates is not None
            and physical_partition_dates[index] != ingested_at.date()
        ):
            raise ValueError(
                f"{entity_name} ingestion validation failed: physical dt mismatch "
                f"for {record_id}"
            )
    if len({record["ingested_at"].date() for record in records}) < 2:
        raise ValueError(
            f"{entity_name} ingestion validation failed: expected multiple dates"
        )


def ingestion_report(records_by_entity: dict[str, list[dict[str, Any]]]) -> dict:
    delays = []
    categories = Counter()
    partitions = {}
    for entity_name, records in records_by_entity.items():
        entity_dates = [record["ingested_at"].date() for record in records]
        partitions[entity_name] = {
            "count": len(set(entity_dates)),
            "earliest": min(entity_dates),
            "latest": max(entity_dates),
        }
        for record in records:
            event_timestamp = primary_event_timestamp(entity_name, record)
            ingested_at = record["ingested_at"]
            delay = (ingested_at - event_timestamp).total_seconds()
            delays.append(delay)
            if delay < 60:
                categories["same_minute"] += 1
            if ingested_at.date() == event_timestamp.date():
                categories["same_day"] += 1
            elif ingested_at.date() == event_timestamp.date() + timedelta(days=1):
                categories["next_day"] += 1
            if delay > 86_400:
                categories["over_one_day"] += 1
    ordered = sorted(delays)
    percentile = lambda value: ordered[max(0, int(value * len(ordered)) - 1)]
    return {
        "total": len(ordered),
        "median": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "maximum": max(ordered),
        "categories": categories,
        "partitions": partitions,
    }
