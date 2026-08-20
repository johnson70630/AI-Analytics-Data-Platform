"""NDJSON serialization and Bronze-layer output writers."""

import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import boto3
from botocore.exceptions import BotoCoreError, ClientError

Record = Mapping[str, Any]


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def serialize_ndjson(records: Iterable[Record]) -> str:
    """Serialize records as one compact JSON object per line."""
    lines = [
        json.dumps(record, default=_json_default, separators=(",", ":"))
        for record in records
    ]
    return "\n".join(lines) + ("\n" if lines else "")


def _partition_value(partition_date: date | str) -> str:
    if isinstance(partition_date, datetime):
        return partition_date.date().isoformat()
    if isinstance(partition_date, date):
        return partition_date.isoformat()
    try:
        return date.fromisoformat(partition_date).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError("partition_date must use YYYY-MM-DD format") from exc


def build_object_key(entity_name: str, partition_date: date | str) -> str:
    """Build a raw Bronze object path for an operational entity."""
    if not entity_name or any(part in entity_name for part in ("/", "\\", "..")):
        raise ValueError("entity_name must be a non-empty path-safe name")
    return (
        f"raw/{entity_name}/dt={_partition_value(partition_date)}/"
        f"{uuid.uuid4()}.json"
    )


def write_records_locally(
    records: Iterable[Record],
    entity_name: str,
    partition_date: date | str,
    output_root: str | Path = "data",
) -> Path | None:
    """Write records to a local path matching the S3 key layout."""
    materialized_records = list(records)
    if not materialized_records:
        print(f"No {entity_name} records to write; skipping local output.")
        return None

    output_path = Path(output_root) / build_object_key(
        entity_name, partition_date
    )
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            serialize_ndjson(materialized_records), encoding="utf-8"
        )
    except OSError as exc:
        raise RuntimeError(
            f"Failed to write {len(materialized_records)} records locally "
            f"to {output_path}: {exc}"
        ) from exc

    print(f"Wrote {len(materialized_records)} records locally to {output_path}")
    return output_path


def write_records_to_s3(
    records: Iterable[Record],
    entity_name: str,
    partition_date: date | str,
    *,
    bucket: str,
    region: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
) -> str | None:
    """Serialize records as NDJSON and upload them to the configured S3 bucket."""
    materialized_records = list(records)
    if not materialized_records:
        print(f"No {entity_name} records to write; skipping S3 output.")
        return None

    key = build_object_key(entity_name, partition_date)
    try:
        client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=serialize_ndjson(materialized_records).encode("utf-8"),
            ContentType="application/x-ndjson",
        )
    except (BotoCoreError, ClientError, OSError) as exc:
        raise RuntimeError(
            f"Failed to upload {len(materialized_records)} records "
            f"to s3://{bucket}/{key}: {exc}"
        ) from exc

    print(
        f"Wrote {len(materialized_records)} records "
        f"to bucket {bucket} with key {key}"
    )
    return key
