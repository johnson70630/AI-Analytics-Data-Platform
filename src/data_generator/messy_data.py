"""Deterministic controlled quality-issue injection for isolated Bronze data."""

import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from .ingestion import (
    EVENT_ID_FIELDS,
    iter_local_partitioned_records,
    iter_s3_partitioned_records,
)
from .writers import PartitionedNDJSONWriter


MISSING_FIELD = "MISSING_FIELD"
DUPLICATE_RECORD = "DUPLICATE_RECORD"
OUT_OF_ORDER_TIMESTAMP = "OUT_OF_ORDER_TIMESTAMP"
ISSUE_TYPES = (MISSING_FIELD, DUPLICATE_RECORD, OUT_OF_ORDER_TIMESTAMP)
BRONZE_ENTITIES = (
    "users",
    "messages",
    "completions",
    "model_inferences",
    "feedback",
    "purchases",
    "payments",
)
MANIFEST_FIELDS = {
    "injection_id",
    "entity_name",
    "source_record_id",
    "issue_type",
    "field_name",
    "original_value",
    "injected_value",
    "original_ingested_at",
    "dirty_ingested_at",
    "violation_type",
}
INJECTION_CONFIG = {
    "users": {
        "missing_field": "email",
        "missing_rate": 0.0048,
        "duplicate_rate": 0.0,
        "timestamp_rate": 0.0,
    },
    "messages": {
        "missing_field": "message_text",
        "missing_rate": 0.0042,
        "duplicate_rate": 0.0018,
        "timestamp_rate": 0.0005,
    },
    "completions": {
        "missing_field": "user_id",
        "missing_rate": 0.0045,
        "duplicate_rate": 0.0017,
        "timestamp_rate": 0.0005,
    },
    "model_inferences": {
        "missing_field": "model_id",
        "missing_rate": 0.0038,
        "duplicate_rate": 0.0015,
        "timestamp_rate": 0.0004,
    },
    "feedback": {
        "missing_field": "feedback_type",
        "missing_rate": 0.0052,
        "duplicate_rate": 0.0012,
        "timestamp_rate": 0.0,
    },
    "purchases": {
        "missing_field": "purchase_type",
        "missing_rate": 0.0046,
        "duplicate_rate": 0.0,
        "timestamp_rate": 0.0,
    },
    "payments": {
        "missing_field": "payment_method",
        "missing_rate": 0.0040,
        "duplicate_rate": 0.0020,
        "timestamp_rate": 0.0007,
    },
}


def _normalized(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _stable_digest(seed: int, entity_name: str, record_id: str) -> bytes:
    return hashlib.sha256(
        f"{seed}:{entity_name}:{record_id}".encode("utf-8")
    ).digest()


def _stable_fraction(digest: bytes, offset: int = 0) -> float:
    return int.from_bytes(digest[offset : offset + 8], "big") / 2**64


def _injection_id(
    seed: int,
    entity_name: str,
    record_id: str,
    issue_type: str,
) -> str:
    value = hashlib.sha256(
        f"{seed}:{entity_name}:{record_id}:{issue_type}".encode("utf-8")
    ).hexdigest()
    return f"inj_{value[:24]}"


def _manifest_row(
    seed: int,
    entity_name: str,
    record_id: str,
    issue_type: str,
    field_name: str,
    original_value: Any,
    injected_value: Any,
    original_ingested_at: datetime,
    dirty_ingested_at: datetime,
    violation_type: str,
) -> dict[str, Any]:
    return {
        "injection_id": _injection_id(
            seed,
            entity_name,
            record_id,
            issue_type,
        ),
        "entity_name": entity_name,
        "source_record_id": record_id,
        "issue_type": issue_type,
        "field_name": field_name,
        "original_value": _normalized(original_value),
        "injected_value": _normalized(injected_value),
        "original_ingested_at": _normalized(original_ingested_at),
        "dirty_ingested_at": _normalized(dirty_ingested_at),
        "violation_type": violation_type,
    }


def _inject_missing(
    record: dict[str, Any],
    entity_name: str,
    seed: int,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    field_name = INJECTION_CONFIG[entity_name]["missing_field"]
    original_value = record.get(field_name)
    if original_value is None:
        return None
    dirty = dict(record)
    dirty[field_name] = None
    record_id = record[EVENT_ID_FIELDS[entity_name]]
    manifest = _manifest_row(
        seed,
        entity_name,
        record_id,
        MISSING_FIELD,
        field_name,
        original_value,
        None,
        record["ingested_at"],
        record["ingested_at"],
        "NULL_REQUIRED_FIELD",
    )
    return dirty, manifest


def _inject_duplicate(
    record: dict[str, Any],
    entity_name: str,
    seed: int,
    digest: bytes,
    generation_end: datetime,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    original_ingested_at = record["ingested_at"]
    if original_ingested_at >= generation_end:
        return None
    offset_seconds = 300 + int(_stable_fraction(digest, 8) * 129_300)
    dirty_ingested_at = min(
        generation_end,
        original_ingested_at + timedelta(seconds=offset_seconds),
    )
    duplicate = dict(record)
    duplicate["ingested_at"] = dirty_ingested_at
    replay_type = (
        "REPLAY_SAME_DAY"
        if dirty_ingested_at.date() == original_ingested_at.date()
        else "REPLAY_CROSS_DAY"
    )
    record_id = record[EVENT_ID_FIELDS[entity_name]]
    manifest = _manifest_row(
        seed,
        entity_name,
        record_id,
        DUPLICATE_RECORD,
        "ingested_at",
        original_ingested_at,
        dirty_ingested_at,
        original_ingested_at,
        dirty_ingested_at,
        replay_type,
    )
    return duplicate, manifest


def _timestamp_injection(
    record: dict[str, Any],
    entity_name: str,
    digest: bytes,
    contexts: dict[str, dict[str, datetime]],
) -> tuple[str, datetime, str] | None:
    offset_seconds = 1 + int(_stable_fraction(digest, 16) * 3_600)
    if entity_name == "messages":
        parent_at = contexts["conversations"].get(record["conversation_id"])
        if parent_at is None:
            return None
        return (
            "created_at",
            parent_at - timedelta(seconds=offset_seconds),
            "MESSAGE_BEFORE_CONVERSATION",
        )
    if entity_name == "completions" and record.get("completed_at") is not None:
        return (
            "completed_at",
            record["requested_at"] - timedelta(seconds=offset_seconds),
            "COMPLETION_BEFORE_REQUEST",
        )
    if entity_name == "model_inferences" and record.get("response_at") is not None:
        return (
            "response_at",
            record["request_at"] - timedelta(seconds=offset_seconds),
            "INFERENCE_RESPONSE_BEFORE_REQUEST",
        )
    if entity_name == "payments":
        parent_at = contexts["purchases"].get(record["purchase_id"])
        if parent_at is None:
            return None
        return (
            "processed_at",
            parent_at - timedelta(seconds=offset_seconds),
            "PAYMENT_BEFORE_PURCHASE",
        )
    return None


def _inject_timestamp(
    record: dict[str, Any],
    entity_name: str,
    seed: int,
    digest: bytes,
    contexts: dict[str, dict[str, datetime]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    injection = _timestamp_injection(record, entity_name, digest, contexts)
    if injection is None:
        return None
    field_name, injected_value, violation_type = injection
    dirty = dict(record)
    original_value = dirty[field_name]
    dirty[field_name] = injected_value
    record_id = record[EVENT_ID_FIELDS[entity_name]]
    manifest = _manifest_row(
        seed,
        entity_name,
        record_id,
        OUT_OF_ORDER_TIMESTAMP,
        field_name,
        original_value,
        injected_value,
        record["ingested_at"],
        record["ingested_at"],
        violation_type,
    )
    return dirty, manifest


def inject_record(
    record: dict[str, Any],
    entity_name: str,
    seed: int,
    generation_end: datetime,
    contexts: dict[str, dict[str, datetime]],
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    """Return original-or-dirty row, optional replay row, and one manifest row."""
    config = INJECTION_CONFIG[entity_name]
    record_id = record[EVENT_ID_FIELDS[entity_name]]
    digest = _stable_digest(seed, entity_name, record_id)
    selection = _stable_fraction(digest)
    missing_end = config["missing_rate"]
    duplicate_end = missing_end + config["duplicate_rate"]
    timestamp_end = duplicate_end + config["timestamp_rate"]

    if selection < missing_end:
        injected = _inject_missing(record, entity_name, seed)
        if injected is not None:
            dirty, manifest = injected
            return dirty, None, manifest
    elif selection < duplicate_end:
        injected = _inject_duplicate(
            record,
            entity_name,
            seed,
            digest,
            generation_end,
        )
        if injected is not None:
            duplicate, manifest = injected
            return record, duplicate, manifest
    elif selection < timestamp_end:
        injected = _inject_timestamp(
            record,
            entity_name,
            seed,
            digest,
            contexts,
        )
        if injected is not None:
            dirty, manifest = injected
            return dirty, None, manifest
    return record, None, None


def _file_stats(path: Path) -> tuple[int, int, str]:
    digest = hashlib.sha256()
    row_count = 0
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            row_count += chunk.count(b"\n")
    return path.stat().st_size, row_count, digest.hexdigest()


def snapshot_clean_baseline(
    source_root: str | Path,
    entities: Iterable[str] = BRONZE_ENTITIES,
) -> dict[str, Any]:
    """Capture byte-level clean raw signatures and row totals."""
    root = Path(source_root)
    files = {}
    rows = Counter()
    bytes_by_entity = Counter()
    for entity_name in entities:
        paths = sorted((root / "raw" / entity_name).glob("dt=*/*.json"))
        if not paths:
            raise RuntimeError(f"No clean raw files found for {entity_name}")
        for path in paths:
            size, row_count, digest = _file_stats(path)
            relative_path = path.relative_to(root).as_posix()
            files[relative_path] = (size, row_count, digest)
            rows[entity_name] += row_count
            bytes_by_entity[entity_name] += size
    return {
        "files": files,
        "rows": dict(rows),
        "bytes": dict(bytes_by_entity),
    }


def assert_clean_baseline_unchanged(
    before: dict[str, Any],
    after: dict[str, Any],
) -> None:
    if before != after:
        raise ValueError("Clean raw baseline files changed during Bronze generation")


def _load_contexts(source_root: str | Path) -> dict[str, dict[str, datetime]]:
    return {
        "conversations": {
            row["conversation_id"]: row["created_at"]
            for row, _ in iter_local_partitioned_records(
                "conversations",
                source_root,
            )
        },
        "purchases": {
            row["purchase_id"]: row["purchase_created_at"]
            for row, _ in iter_local_partitioned_records(
                "purchases",
                source_root,
            )
        },
    }


def _remove_generated_tree(
    source_root: str | Path,
    prefix: str,
    entity_names: Iterable[str],
) -> int:
    root = Path(source_root)
    removed = 0
    for entity_name in entity_names:
        entity_root = root / prefix / entity_name
        if not entity_root.exists():
            continue
        for path in sorted(entity_root.glob("dt=*/*.json")):
            path.unlink()
            removed += 1
        for directory in sorted(entity_root.glob("dt=*"), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
    return removed


def generate_messy_bronze_locally(
    partition_date: date,
    source_root: str | Path,
    seed: int,
) -> dict[str, Any]:
    """Stream clean raw data into isolated Bronze with controlled issues."""
    generation_end = datetime.combine(
        partition_date,
        datetime.max.time().replace(microsecond=0),
        timezone.utc,
    )
    contexts = _load_contexts(source_root)
    removed = _remove_generated_tree(
        source_root,
        "bronze",
        BRONZE_ENTITIES,
    )
    removed += _remove_generated_tree(
        source_root,
        "quality",
        ("injection_manifest",),
    )
    print(f"Removed {removed} obsolete controlled-Bronze files")

    clean_counts = Counter()
    bronze_counts = Counter()
    issue_counts = Counter()
    object_counts = Counter()
    with PartitionedNDJSONWriter(
        source_root,
        "injection_manifest",
        prefix="quality",
    ) as manifest_writer:
        for entity_name in BRONZE_ENTITIES:
            with PartitionedNDJSONWriter(
                source_root,
                entity_name,
                prefix="bronze",
            ) as bronze_writer:
                for record, _ in iter_local_partitioned_records(
                    entity_name,
                    source_root,
                ):
                    clean_counts[entity_name] += 1
                    dirty, duplicate, manifest = inject_record(
                        record,
                        entity_name,
                        seed,
                        generation_end,
                        contexts,
                    )
                    bronze_writer.write(dirty, dirty["ingested_at"].date())
                    bronze_counts[entity_name] += 1
                    if duplicate is not None:
                        bronze_writer.write(
                            duplicate,
                            duplicate["ingested_at"].date(),
                        )
                        bronze_counts[entity_name] += 1
                    if manifest is not None:
                        manifest_writer.write(manifest, partition_date)
                        issue_counts[manifest["issue_type"]] += 1
            object_counts[entity_name] = len(bronze_writer.paths)
            print(
                f"Wrote {bronze_counts[entity_name]:,} Bronze {entity_name} "
                f"rows across {object_counts[entity_name]} daily partitions"
            )
    return {
        "clean_counts": clean_counts,
        "bronze_counts": bronze_counts,
        "issue_counts": issue_counts,
        "object_counts": object_counts,
        "manifest_objects": len(manifest_writer.paths),
    }


def _load_local_manifest(source_root: str | Path) -> list[dict[str, Any]]:
    paths = sorted(
        (Path(source_root) / "quality" / "injection_manifest").glob(
            "dt=*/*.json"
        )
    )
    if not paths:
        raise RuntimeError("No local injection manifest found")
    rows = []
    for path in paths:
        try:
            rows.extend(
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Failed to reload {path}: {exc}") from exc
    return rows


def _validate_timestamp_violation(
    record: dict[str, Any],
    violation_type: str,
    contexts: dict[str, dict[str, datetime]],
) -> bool:
    if violation_type == "MESSAGE_BEFORE_CONVERSATION":
        return record["created_at"] < contexts["conversations"][
            record["conversation_id"]
        ]
    if violation_type == "COMPLETION_BEFORE_REQUEST":
        return record["completed_at"] < record["requested_at"]
    if violation_type == "INFERENCE_RESPONSE_BEFORE_REQUEST":
        return record["response_at"] < record["request_at"]
    if violation_type == "PAYMENT_BEFORE_PURCHASE":
        return record["processed_at"] < contexts["purchases"][
            record["purchase_id"]
        ]
    return False


def _manifest_indexes(
    manifest: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], set[str]]:
    indexes = {}
    injection_ids = set()
    for row in manifest:
        if set(row) != MANIFEST_FIELDS:
            raise ValueError("Injection manifest schema is invalid")
        injection_id = row.get("injection_id")
        key = (row.get("entity_name"), row.get("source_record_id"))
        if (
            not injection_id
            or injection_id in injection_ids
            or key in indexes
            or row.get("entity_name") not in BRONZE_ENTITIES
            or row.get("issue_type") not in ISSUE_TYPES
        ):
            raise ValueError("Injection manifest uniqueness/vocabulary is invalid")
        injection_ids.add(injection_id)
        indexes[key] = row
    return indexes, injection_ids


def _validate_bronze(
    source_root: str | Path,
    manifest: list[dict[str, Any]],
    bronze_iterators: dict[
        str,
        Callable[[], Iterator[tuple[dict[str, Any], date]]],
    ],
    object_counts: dict[str, int],
) -> dict[str, Any]:
    indexes, _ = _manifest_indexes(manifest)
    contexts = _load_contexts(source_root)
    affected_keys = set(indexes)
    clean_records = {}
    clean_occurrences = Counter()
    clean_counts = Counter()
    for entity_name in BRONZE_ENTITIES:
        id_field = EVENT_ID_FIELDS[entity_name]
        for record, _ in iter_local_partitioned_records(
            entity_name,
            source_root,
        ):
            clean_counts[entity_name] += 1
            key = (entity_name, record[id_field])
            if key in affected_keys:
                clean_occurrences[key] += 1
                clean_records[key] = record

    bronze_counts = Counter()
    bronze_records: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    partitions: dict[str, set[date]] = defaultdict(set)
    for entity_name in BRONZE_ENTITIES:
        id_field = EVENT_ID_FIELDS[entity_name]
        for record, physical_date in bronze_iterators[entity_name]():
            if record["ingested_at"].date() != physical_date:
                raise ValueError(
                    f"Bronze {entity_name} physical dt does not match ingested_at"
                )
            bronze_counts[entity_name] += 1
            partitions[entity_name].add(physical_date)
            key = (entity_name, record[id_field])
            if key in affected_keys:
                bronze_records[key].append(record)

    missing_by_entity = Counter()
    missing_by_field = Counter()
    duplicate_by_entity = Counter()
    duplicate_replay_type = Counter()
    timestamp_by_entity = Counter()
    violation_types = Counter()
    issue_counts = Counter()
    for key, manifest_row in indexes.items():
        clean = clean_records.get(key)
        dirty_rows = bronze_records.get(key, [])
        if clean is None or clean_occurrences[key] != 1:
            raise ValueError(f"Manifest source {key} is absent/duplicated in raw")
        if _normalized(clean["ingested_at"]) != manifest_row["original_ingested_at"]:
            raise ValueError(f"Manifest source ingestion mismatch for {key}")
        issue_type = manifest_row["issue_type"]
        field_name = manifest_row["field_name"]
        issue_counts[issue_type] += 1
        if issue_type == MISSING_FIELD:
            if (
                len(dirty_rows) != 1
                or clean.get(field_name) is None
                or dirty_rows[0].get(field_name) is not None
                or _normalized(clean[field_name]) != manifest_row["original_value"]
                or manifest_row["injected_value"] is not None
            ):
                raise ValueError(
                    f"Missing-field injection does not reconcile for {key}"
                )
            missing_by_entity[key[0]] += 1
            missing_by_field[f"{key[0]}.{field_name}"] += 1
        elif issue_type == DUPLICATE_RECORD:
            if len(dirty_rows) != 2:
                raise ValueError(
                    f"Duplicate injection does not create two rows for {key}"
                )
            expected_original = manifest_row["original_ingested_at"]
            expected_dirty = manifest_row["dirty_ingested_at"]
            ingestions = sorted(_normalized(row["ingested_at"]) for row in dirty_rows)
            if ingestions != sorted((expected_original, expected_dirty)):
                raise ValueError(
                    f"Duplicate ingestion timestamps do not reconcile for {key}"
                )
            for row in dirty_rows:
                comparable = dict(row)
                comparable["ingested_at"] = clean["ingested_at"]
                if comparable != clean:
                    raise ValueError(f"Duplicate payload changed for {key}")
            duplicate_by_entity[key[0]] += 1
            duplicate_replay_type[manifest_row["violation_type"]] += 1
        else:
            if (
                len(dirty_rows) != 1
                or _normalized(clean[field_name]) != manifest_row["original_value"]
                or _normalized(dirty_rows[0][field_name])
                != manifest_row["injected_value"]
                or not _validate_timestamp_violation(
                    dirty_rows[0],
                    manifest_row["violation_type"],
                    contexts,
                )
            ):
                raise ValueError(f"Timestamp injection does not reconcile for {key}")
            timestamp_by_entity[key[0]] += 1
            violation_types[manifest_row["violation_type"]] += 1

    for entity_name in BRONZE_ENTITIES:
        expected = clean_counts[entity_name] + duplicate_by_entity[entity_name]
        if bronze_counts[entity_name] != expected:
            raise ValueError(
                f"Bronze {entity_name} count {bronze_counts[entity_name]} "
                f"does not equal clean plus duplicates {expected}"
            )
        if len(partitions[entity_name]) != object_counts[entity_name]:
            raise ValueError(
                f"Bronze {entity_name} does not use one file per daily partition"
            )

    total_issues = sum(issue_counts.values())
    if total_issues != len(manifest):
        raise ValueError("Manifest row count does not reconcile to injected issues")
    eligible_records = sum(clean_counts.values())
    return {
        "clean_counts": clean_counts,
        "bronze_counts": bronze_counts,
        "missing_total": issue_counts[MISSING_FIELD],
        "missing_by_entity": missing_by_entity,
        "missing_by_field": missing_by_field,
        "duplicate_total": issue_counts[DUPLICATE_RECORD],
        "duplicate_by_entity": duplicate_by_entity,
        "duplicate_replay_type": duplicate_replay_type,
        "timestamp_total": issue_counts[OUT_OF_ORDER_TIMESTAMP],
        "timestamp_by_entity": timestamp_by_entity,
        "violation_types": violation_types,
        "total_issues": total_issues,
        "unique_records": len(indexes),
        "affected_percentage": len(indexes) / eligible_records * 100,
        "manifest_rows": len(manifest),
        "manifest_reconciliation": "PASS",
        "partitions": {
            entity_name: {
                "count": len(values),
                "earliest": min(values),
                "latest": max(values),
            }
            for entity_name, values in partitions.items()
        },
        "object_counts": Counter(object_counts),
        "examples": {
            issue_type: next(
                row for row in manifest if row["issue_type"] == issue_type
            )
            for issue_type in ISSUE_TYPES
        },
    }


def validate_local_messy_bronze(
    source_root: str | Path,
) -> dict[str, Any]:
    manifest = _load_local_manifest(source_root)
    object_counts = {
        entity_name: len(
            list(
                (Path(source_root) / "bronze" / entity_name).glob(
                    "dt=*/*.json"
                )
            )
        )
        for entity_name in BRONZE_ENTITIES
    }
    iterators = {
        entity_name: (
            lambda entity_name=entity_name: iter_local_partitioned_records(
                entity_name,
                source_root,
                prefix="bronze",
            )
        )
        for entity_name in BRONZE_ENTITIES
    }
    return _validate_bronze(source_root, manifest, iterators, object_counts)


def _load_s3_manifest(client: Any, bucket: str) -> tuple[list[dict[str, Any]], int]:
    keys = sorted(
        item["Key"]
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix="quality/injection_manifest/dt=",
        )
        for item in page.get("Contents", [])
    )
    if not keys:
        raise RuntimeError("No S3 injection manifest found")
    rows = []
    for key in keys:
        response = client.get_object(Bucket=bucket, Key=key)
        if response.get("ContentType") != "application/x-ndjson":
            raise ValueError(f"Unexpected content type for s3://{bucket}/{key}")
        rows.extend(
            json.loads(line)
            for line in response["Body"].read().decode("utf-8").splitlines()
            if line
        )
    return rows, len(keys)


def validate_s3_messy_bronze(
    client: Any,
    bucket: str,
    source_root: str | Path,
) -> dict[str, Any]:
    manifest, manifest_objects = _load_s3_manifest(client, bucket)
    object_counts = {
        entity_name: sum(
            1
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket,
                Prefix=f"bronze/{entity_name}/dt=",
            )
            for _ in page.get("Contents", [])
        )
        for entity_name in BRONZE_ENTITIES
    }
    iterators = {
        entity_name: (
            lambda entity_name=entity_name: iter_s3_partitioned_records(
                client,
                bucket,
                entity_name,
                prefix="bronze",
            )
        )
        for entity_name in BRONZE_ENTITIES
    }
    summary = _validate_bronze(
        source_root,
        manifest,
        iterators,
        object_counts,
    )
    summary["manifest_objects"] = manifest_objects
    return summary


def s3_raw_inventory(
    client: Any,
    bucket: str,
) -> dict[str, tuple[int, str]]:
    """Capture immutable raw object sizes and ETags for selected entities."""
    inventory = {}
    for entity_name in BRONZE_ENTITIES:
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=f"raw/{entity_name}/dt=",
        ):
            for item in page.get("Contents", []):
                inventory[item["Key"]] = (item["Size"], item["ETag"])
    return inventory


def verify_s3_raw_row_counts(
    client: Any,
    bucket: str,
    expected_counts: dict[str, int],
) -> Counter[str]:
    counts = Counter()
    for entity_name in BRONZE_ENTITIES:
        for _record, _physical_date in iter_s3_partitioned_records(
            client,
            bucket,
            entity_name,
        ):
            counts[entity_name] += 1
        if counts[entity_name] != expected_counts[entity_name]:
            raise ValueError(
                f"Clean raw S3 {entity_name} row count changed: "
                f"{counts[entity_name]} != {expected_counts[entity_name]}"
            )
    return counts


def print_messy_summary(summary: dict[str, Any]) -> None:
    print("\nClean vs Bronze row counts:")
    for entity_name in BRONZE_ENTITIES:
        print(
            f"{entity_name}: {summary['clean_counts'][entity_name]:,} -> "
            f"{summary['bronze_counts'][entity_name]:,}"
        )
    print(f"\nMISSING_FIELD: {summary['missing_total']:,}")
    for entity_name, count in summary["missing_by_entity"].items():
        print(f"{entity_name}: {count:,}")
    print("Missing fields:")
    for field_name, count in summary["missing_by_field"].items():
        print(f"{field_name}: {count:,}")
    print(f"\nDUPLICATE_RECORD: {summary['duplicate_total']:,}")
    for entity_name, count in summary["duplicate_by_entity"].items():
        print(f"{entity_name}: {count:,}")
    print(
        "same-day replays: "
        f"{summary['duplicate_replay_type']['REPLAY_SAME_DAY']:,}"
    )
    print(
        "cross-day replays: "
        f"{summary['duplicate_replay_type']['REPLAY_CROSS_DAY']:,}"
    )
    print(
        f"\nOUT_OF_ORDER_TIMESTAMP: {summary['timestamp_total']:,}"
    )
    for entity_name, count in summary["timestamp_by_entity"].items():
        print(f"{entity_name}: {count:,}")
    print("Violation types:")
    for violation_type, count in summary["violation_types"].items():
        print(f"{violation_type}: {count:,}")
    print(f"\nTotal injected issues: {summary['total_issues']:,}")
    print(f"Unique affected records: {summary['unique_records']:,}")
    print(f"Affected percentage: {summary['affected_percentage']:.3f}%")
    print(f"Manifest rows: {summary['manifest_rows']:,}")
    print(f"Manifest reconciliation: {summary['manifest_reconciliation']}")
    print("\nDaily Bronze partitions:")
    for entity_name, report in summary["partitions"].items():
        print(
            f"{entity_name}: {report['count']} "
            f"({report['earliest']} to {report['latest']})"
        )
