"""Copy-only S3 materialization for source entities missing from Bronze."""

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from itertools import zip_longest
from typing import Any, Iterable

from botocore.exceptions import BotoCoreError, ClientError

from .ingestion import RECORD_TIMESTAMP_FIELDS
from .messy_data import BRONZE_ENTITIES as DIRTY_BRONZE_ENTITIES


COPY_ONLY_BRONZE_ENTITIES = (
    "models",
    "devices",
    "subscription_plans",
    "user_updates",
    "conversations",
    "errors",
    "subscriptions",
)
ALL_BRONZE_ENTITIES = (
    "users",
    "user_updates",
    "conversations",
    "messages",
    "completions",
    "model_inferences",
    "feedback",
    "errors",
    "models",
    "devices",
    "subscription_plans",
    "subscriptions",
    "purchases",
    "payments",
)
ID_FIELDS = {
    "models": "model_id",
    "devices": "device_id",
    "subscription_plans": "plan_id",
    "user_updates": "update_id",
    "conversations": "conversation_id",
    "errors": "error_id",
    "subscriptions": "subscription_id",
}
NDJSON_CONTENT_TYPE = "application/x-ndjson"
_MISSING = object()


@dataclass(frozen=True)
class S3InventoryItem:
    key: str
    size: int
    etag: str


def list_s3_inventory(
    client: Any,
    bucket: str,
    prefix: str,
) -> dict[str, S3InventoryItem]:
    """Return stable object metadata for one S3 prefix."""
    inventory = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".json"):
                continue
            inventory[key] = S3InventoryItem(
                key=key,
                size=int(item.get("Size", 0)),
                etag=str(item.get("ETag", "")),
            )
    return dict(sorted(inventory.items()))


def capture_protected_s3_inventory(
    client: Any,
    bucket: str,
) -> dict[str, dict[str, S3InventoryItem]]:
    """Snapshot all raw data, dirty Bronze data, and the quality manifest."""
    prefixes = [
        *(f"raw/{entity_name}/" for entity_name in ALL_BRONZE_ENTITIES),
        *(
            f"bronze/{entity_name}/"
            for entity_name in DIRTY_BRONZE_ENTITIES
        ),
        "quality/injection_manifest/",
    ]
    return {
        prefix: list_s3_inventory(client, bucket, prefix)
        for prefix in prefixes
    }


def assert_inventory_unchanged(
    before: dict[str, dict[str, S3InventoryItem]],
    after: dict[str, dict[str, S3InventoryItem]],
) -> None:
    changed = [prefix for prefix in before if before[prefix] != after.get(prefix)]
    if changed:
        raise ValueError(
            "Protected S3 inventory changed: " + ", ".join(sorted(changed))
        )


def _partition_date(key: str) -> date:
    try:
        return date.fromisoformat(key.split("/dt=", 1)[1].split("/", 1)[0])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"Invalid partitioned S3 key: {key}") from exc


def _destination_key(source_key: str) -> str:
    if not source_key.startswith("raw/"):
        raise ValueError(f"Raw source key expected: {source_key}")
    return "bronze/" + source_key.removeprefix("raw/")


def materialize_missing_bronze_entities(
    client: Any,
    bucket: str,
) -> dict[str, Any]:
    """Create only absent Bronze objects using byte-preserving S3 copies."""
    source_inventory = {}
    expected_destinations = {}
    existing_destinations = {}
    for entity_name in COPY_ONLY_BRONZE_ENTITIES:
        raw = list_s3_inventory(client, bucket, f"raw/{entity_name}/dt=")
        if not raw:
            raise RuntimeError(f"No raw S3 objects found for {entity_name}")
        bronze = list_s3_inventory(client, bucket, f"bronze/{entity_name}/dt=")
        expected = {_destination_key(key): item for key, item in raw.items()}
        unexpected = set(bronze).difference(expected)
        if unexpected:
            raise RuntimeError(
                f"Refusing to materialize {entity_name}: found "
                f"{len(unexpected)} unexpected Bronze object(s)"
            )
        source_inventory[entity_name] = raw
        expected_destinations[entity_name] = expected
        existing_destinations[entity_name] = bronze

    copied = Counter()
    skipped = Counter()
    copied_keys = []
    for entity_name in COPY_ONLY_BRONZE_ENTITIES:
        existing = existing_destinations[entity_name]
        for destination_key, source in expected_destinations[entity_name].items():
            if destination_key in existing:
                destination = existing[destination_key]
                if (
                    destination.size != source.size
                    or destination.etag != source.etag
                ):
                    raise RuntimeError(
                        f"Refusing to overwrite non-matching {destination_key}"
                    )
                skipped[entity_name] += 1
                continue
            try:
                client.copy_object(
                    Bucket=bucket,
                    Key=destination_key,
                    CopySource={"Bucket": bucket, "Key": source.key},
                    CopySourceIfMatch=source.etag,
                    IfNoneMatch="*",
                    ContentType=NDJSON_CONTENT_TYPE,
                    MetadataDirective="REPLACE",
                )
            except (BotoCoreError, ClientError, OSError) as exc:
                raise RuntimeError(
                    f"Failed to copy s3://{bucket}/{source.key} to "
                    f"s3://{bucket}/{destination_key}: {exc}"
                ) from exc
            copied[entity_name] += 1
            copied_keys.append(destination_key)

    return {
        "copied_objects": copied,
        "skipped_objects": skipped,
        "copied_keys": copied_keys,
        "source_inventory": source_inventory,
    }


def _iter_lines(body: Any) -> Iterable[bytes]:
    try:
        return body.iter_lines(chunk_size=1024 * 1024)
    except TypeError:
        return body.iter_lines()


def _parse_ingested_date(value: Any) -> date:
    if not isinstance(value, str):
        raise ValueError("ingested_at must be an ISO-8601 string")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def validate_materialized_bronze_entities(
    client: Any,
    bucket: str,
) -> dict[str, Any]:
    """Stream corresponding raw/Bronze objects and prove copy fidelity."""
    row_counts = Counter()
    object_counts = Counter()
    ids_preserved = Counter()
    timestamps_preserved = Counter()
    schemas_preserved = Counter()
    partition_checks = Counter()
    reference_partition_checks = Counter()

    for entity_name in COPY_ONLY_BRONZE_ENTITIES:
        raw = list_s3_inventory(client, bucket, f"raw/{entity_name}/dt=")
        bronze = list_s3_inventory(client, bucket, f"bronze/{entity_name}/dt=")
        expected_keys = {_destination_key(key) for key in raw}
        if set(bronze) != expected_keys:
            raise ValueError(f"Bronze object inventory mismatch for {entity_name}")
        id_field = ID_FIELDS[entity_name]
        timestamp_fields = RECORD_TIMESTAMP_FIELDS.get(entity_name, ())
        for source_key, source_metadata in raw.items():
            destination_key = _destination_key(source_key)
            destination_metadata = bronze[destination_key]
            if (
                source_metadata.size != destination_metadata.size
                or source_metadata.etag != destination_metadata.etag
                or _partition_date(source_key) != _partition_date(destination_key)
            ):
                raise ValueError(f"Bronze copy metadata mismatch for {destination_key}")
            raw_response = client.get_object(Bucket=bucket, Key=source_key)
            bronze_response = client.get_object(Bucket=bucket, Key=destination_key)
            if raw_response.get("ContentType") != NDJSON_CONTENT_TYPE:
                raise ValueError(f"Unexpected raw content type for {source_key}")
            if bronze_response.get("ContentType") != NDJSON_CONTENT_TYPE:
                raise ValueError(
                    f"Unexpected Bronze content type for {destination_key}"
                )
            for raw_line, bronze_line in zip_longest(
                _iter_lines(raw_response["Body"]),
                _iter_lines(bronze_response["Body"]),
                fillvalue=_MISSING,
            ):
                if raw_line is _MISSING or bronze_line is _MISSING:
                    raise ValueError(f"Bronze row count mismatch for {entity_name}")
                if not raw_line and not bronze_line:
                    continue
                if raw_line != bronze_line:
                    raise ValueError(f"Bronze payload changed for {entity_name}")
                try:
                    raw_record = json.loads(raw_line)
                    bronze_record = json.loads(bronze_line)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        f"Invalid NDJSON while validating {entity_name}: {exc}"
                    ) from exc
                if not isinstance(raw_record, dict) or not isinstance(
                    bronze_record, dict
                ):
                    raise ValueError(f"Non-object row found for {entity_name}")
                if raw_record.get(id_field) != bronze_record.get(id_field):
                    raise ValueError(f"ID changed for {entity_name}")
                ids_preserved[entity_name] += 1
                if set(raw_record) != set(bronze_record):
                    raise ValueError(f"Schema changed for {entity_name}")
                schemas_preserved[entity_name] += 1
                for field_name in timestamp_fields:
                    if raw_record.get(field_name) != bronze_record.get(field_name):
                        raise ValueError(
                            f"Timestamp {field_name} changed for {entity_name}"
                        )
                    timestamps_preserved[entity_name] += 1
                physical_date = _partition_date(destination_key)
                if "ingested_at" in bronze_record:
                    if _parse_ingested_date(bronze_record["ingested_at"]) != physical_date:
                        raise ValueError(
                            f"Bronze {entity_name} partition does not match ingested_at"
                        )
                    partition_checks[entity_name] += 1
                else:
                    reference_partition_checks[entity_name] += 1
                row_counts[entity_name] += 1
            object_counts[entity_name] += 1

    return {
        "raw_row_counts": Counter(row_counts),
        "bronze_row_counts": Counter(row_counts),
        "object_counts": object_counts,
        "ids_preserved": ids_preserved,
        "timestamps_preserved": timestamps_preserved,
        "schemas_preserved": schemas_preserved,
        "partition_checks": partition_checks,
        "reference_partition_checks": reference_partition_checks,
        "status": "PASS",
    }
