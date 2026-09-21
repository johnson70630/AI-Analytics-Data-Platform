"""Centralized, read-only S3 data-lake profiling utility."""

import argparse
import csv
import hashlib
import json
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import boto3

from src.data_generator.config import DEFAULT_AWS_REGION, load_config


PROFILER_VERSION = "1.1"
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_PROGRESS_EVERY = 100_000
DOMAIN_ENTITIES = {
    "REFERENCE": ("models", "devices", "subscription_plans"),
    "USER": ("users", "user_updates"),
    "PRODUCT": ("conversations", "messages"),
    "AI_ML": ("completions", "model_inferences", "feedback", "errors"),
    "FINANCE": ("subscriptions", "purchases", "payments"),
}
RELATIONSHIPS = {
    "logical_flow": [
        "users -> user_updates",
        "users -> conversations -> messages -> completions -> model_inferences",
        "successful completions -> feedback",
        "system/activity -> errors",
        "users -> subscriptions -> purchases -> payments",
    ],
    "cardinality_assumptions": [
        "user: many conversations",
        "conversation: many messages",
        "message: 0..1 completion",
        "completion: 1 model inference in V1",
        "completion: 0..1 feedback in current generated source",
        "user: many subscription lifecycle periods",
        "subscription: many purchases possible",
        "purchase: one or more payment attempts possible",
    ],
}
ENTITY_CSV_FIELDS = (
    "domain",
    "entity_name",
    "clean_exists",
    "bronze_exists",
    "clean_rows",
    "bronze_rows",
    "row_difference",
    "clean_objects",
    "bronze_objects",
    "clean_partitions",
    "bronze_partitions",
    "clean_earliest_date",
    "clean_latest_date",
    "bronze_earliest_date",
    "bronze_latest_date",
    "clean_size_bytes",
    "bronze_size_bytes",
    "total_injected_issues",
    "affected_records",
    "affected_pct",
    "missing_count",
    "missing_pct",
    "duplicate_count",
    "duplicate_pct",
    "timestamp_issue_count",
    "timestamp_issue_pct",
    "objects_scanned",
    "rows_processed",
    "processing_time_seconds",
    "rows_per_second",
)


@dataclass(frozen=True)
class S3Object:
    key: str
    size: int
    partition_date: date | None


def _prefix(value: str) -> str:
    return value.strip("/") + "/"


def parse_partition_date(key: str) -> date:
    """Extract and validate a physical dt=YYYY-MM-DD partition from a key."""
    for part in key.split("/"):
        if part.startswith("dt="):
            return date.fromisoformat(part.removeprefix("dt="))
    raise ValueError(f"S3 key has no dt partition: {key}")


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def human_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:,.2f} {unit}"
        amount /= 1024
    return f"{amount:,.2f} TiB"


def discover_layer_objects(
    client: Any,
    bucket: str,
    prefix: str,
) -> tuple[dict[str, list[S3Object]], list[str]]:
    """Discover partitioned entity objects under one S3 layer prefix."""
    normalized = _prefix(prefix)
    discovered: dict[str, list[S3Object]] = defaultdict(list)
    warnings = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=normalized):
        for item in page.get("Contents", []):
            key = item["Key"]
            relative = key.removeprefix(normalized)
            parts = relative.split("/")
            if len(parts) < 3 or not key.endswith(".json"):
                continue
            entity_name = parts[0]
            try:
                partition_date = parse_partition_date(key)
            except ValueError as exc:
                warnings.append(str(exc))
                continue
            discovered[entity_name].append(
                S3Object(key, int(item.get("Size", 0)), partition_date)
            )
    for objects in discovered.values():
        objects.sort(key=lambda item: item.key)
    return dict(discovered), warnings


def discover_manifest_objects(
    client: Any,
    bucket: str,
    prefix: str,
) -> list[S3Object]:
    normalized = _prefix(prefix)
    objects = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket,
        Prefix=normalized,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith(".json"):
                partition = None
                try:
                    partition = parse_partition_date(key)
                except ValueError:
                    pass
                objects.append(
                    S3Object(key, int(item.get("Size", 0)), partition)
                )
    return sorted(objects, key=lambda item: item.key)


class FieldProfiler:
    def __init__(self) -> None:
        self.row_count = 0
        self.present = Counter()
        self.nulls = Counter()
        self.types: dict[str, Counter[str]] = defaultdict(Counter)

    def add(self, record: dict[str, Any]) -> None:
        self.row_count += 1
        for field_name, value in record.items():
            self.present[field_name] += 1
            if value is None:
                self.nulls[field_name] += 1
            self.types[field_name][_json_type(value)] += 1

    def summary(self) -> dict[str, Any]:
        fields = {}
        for field_name in sorted(self.present):
            present = self.present[field_name]
            fields[field_name] = {
                "present_count": present,
                "absent_count": self.row_count - present,
                "null_count": self.nulls[field_name],
                "observed_types": sorted(self.types[field_name]),
                "type_counts": dict(sorted(self.types[field_name].items())),
            }
        return {"row_count": self.row_count, "fields": fields}

    def state(self) -> dict[str, Any]:
        """Return compact JSON-safe counters for an object-boundary checkpoint."""
        return {
            "row_count": self.row_count,
            "present": dict(self.present),
            "nulls": dict(self.nulls),
            "types": {
                field_name: dict(type_counts)
                for field_name, type_counts in self.types.items()
            },
        }

    @classmethod
    def from_state(cls, state: dict[str, Any] | None) -> "FieldProfiler":
        profiler = cls()
        if not state:
            return profiler
        profiler.row_count = int(state.get("row_count", 0))
        profiler.present.update(state.get("present", {}))
        profiler.nulls.update(state.get("nulls", {}))
        for field_name, type_counts in state.get("types", {}).items():
            profiler.types[field_name].update(type_counts)
        return profiler


class CheckpointStore:
    """Thread-safe local checkpoint storage; never writes to S3."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.path = self.root / "profile_checkpoint.json"
        self.lock = threading.Lock()
        self.data: dict[str, Any] = {
            "profiler_version": PROFILER_VERSION,
            "groups": {},
        }
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if loaded.get("profiler_version") == PROFILER_VERSION:
                    self.data = loaded
            except (OSError, json.JSONDecodeError):
                pass

    def load_group(self, group_id: str, signature: str) -> dict[str, Any] | None:
        with self.lock:
            saved = self.data.get("groups", {}).get(group_id)
            if saved and saved.get("signature") == signature:
                return saved.get("state")
        return None

    def save_group(
        self,
        group_id: str,
        signature: str,
        state: dict[str, Any],
        *,
        completed: bool,
    ) -> None:
        with self.lock:
            groups = self.data.setdefault("groups", {})
            groups[group_id] = {
                "signature": signature,
                "completed": completed,
                "state": state,
            }
            self.data["completed_entities"] = sorted(
                name for name, value in groups.items() if value.get("completed")
            )
            self.data["updated_at"] = datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )
            self.root.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(
                json.dumps(self.data, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            temporary.replace(self.path)


def _object_signature(objects: Iterable[S3Object]) -> str:
    digest = hashlib.sha256()
    for item in objects:
        digest.update(f"{item.key}\0{item.size}\n".encode("utf-8"))
    return digest.hexdigest()


def _iter_lines(body: Any, chunk_size: int) -> Iterable[bytes]:
    """Use large SDK chunks while retaining compatibility with simple test bodies."""
    try:
        return body.iter_lines(chunk_size=chunk_size)
    except TypeError:
        return body.iter_lines()


def profile_object_group(
    client: Any,
    bucket: str,
    objects: list[S3Object],
    *,
    on_record: Callable[[dict[str, Any]], None] | None = None,
    label: str = "entity",
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    initial_state: dict[str, Any] | None = None,
    checkpoint_callback: Callable[[dict[str, Any], bool], None] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Stream an NDJSON object group once for counts and schema statistics."""
    initial_state = initial_state or {}
    fields = FieldProfiler.from_state(initial_state.get("fields"))
    warnings = list(initial_state.get("warnings", []))
    malformed_json = int(initial_state.get("malformed_json_count", 0))
    non_object_rows = int(initial_state.get("non_object_row_count", 0))
    processed_keys = set(initial_state.get("processed_objects", []))
    checkpoint_rows = fields.row_count
    prior_runtime = float(initial_state.get("scan_runtime_seconds", 0.0))
    started = time.monotonic()
    next_progress = (
        (fields.row_count // progress_every + 1) * progress_every
        if progress_every > 0
        else 0
    )
    if processed_keys:
        print(
            f"Resuming {label}: {len(processed_keys)} objects / "
            f"{fields.row_count:,} rows restored",
            flush=True,
        )
    print(f"Scanning entity: {label}", flush=True)
    for item in objects:
        if item.key in processed_keys:
            continue
        response = client.get_object(Bucket=bucket, Key=item.key)
        content_type = response.get("ContentType")
        if content_type != "application/x-ndjson":
            warnings.append(
                f"Unexpected content type {content_type!r} for {item.key}"
            )
        for line_number, raw_line in enumerate(
            _iter_lines(response["Body"], chunk_size),
            start=1,
        ):
            if not raw_line:
                continue
            try:
                record = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                malformed_json += 1
                warnings.append(
                    f"Malformed JSON {item.key}:{line_number}: {exc}"
                )
                continue
            if not isinstance(record, dict):
                non_object_rows += 1
                warnings.append(
                    f"Non-object JSON {item.key}:{line_number}"
                )
                continue
            fields.add(record)
            if on_record is not None:
                on_record(record)
            if progress_every > 0 and fields.row_count >= next_progress:
                print(f"Processed: {fields.row_count:,} rows ({label})", flush=True)
                while next_progress <= fields.row_count:
                    next_progress += progress_every
        processed_keys.add(item.key)
        if checkpoint_callback is not None:
            checkpoint_callback(
                {
                    "fields": fields.state(),
                    "warnings": warnings,
                    "malformed_json_count": malformed_json,
                    "non_object_row_count": non_object_rows,
                    "processed_objects": sorted(processed_keys),
                    "scan_runtime_seconds": prior_runtime
                    + time.monotonic()
                    - started,
                },
                len(processed_keys) == len(objects),
            )
    partitions = sorted(
        {item.partition_date for item in objects if item.partition_date}
    )
    size_bytes = sum(item.size for item in objects)
    current_runtime = time.monotonic() - started
    scan_runtime = prior_runtime + current_runtime
    summary = {
        "exists": bool(objects),
        "row_count": fields.row_count,
        "object_count": len(objects),
        "partition_count": len(partitions),
        "earliest_partition": partitions[0].isoformat() if partitions else None,
        "latest_partition": partitions[-1].isoformat() if partitions else None,
        "size_bytes": size_bytes,
        "average_object_bytes": size_bytes / len(objects) if objects else 0,
        "malformed_json_count": malformed_json,
        "non_object_row_count": non_object_rows,
        "schema": fields.summary(),
        "processed_object_count": len(processed_keys),
        "checkpoint_reused_object_count": len(
            set(initial_state.get("processed_objects", []))
        ),
        "checkpoint_reused_row_count": checkpoint_rows,
        "scan_runtime_seconds": scan_runtime,
        "current_runtime_seconds": current_runtime,
        "rows_per_second": fields.row_count / scan_runtime if scan_runtime else 0.0,
    }
    print(
        f"Completed {label}: objects scanned: {len(objects):,}; "
        f"rows: {fields.row_count:,}; runtime: {current_runtime:.2f} seconds; "
        f"rows/sec: {summary['rows_per_second']:,.0f}",
        flush=True,
    )
    return summary, warnings


class ManifestProfiler:
    def __init__(self) -> None:
        self.rows = 0
        self.issue_types = Counter()
        self.by_entity = Counter()
        self.by_entity_issue = Counter()
        self.by_field = Counter()
        self.missing_by_field = Counter()
        self.by_violation = Counter()
        self.by_entity_classifier = Counter()
        self.unique_records: dict[str, set[str]] = defaultdict(set)

    def add(self, record: dict[str, Any]) -> None:
        self.rows += 1
        entity = str(record.get("entity_name"))
        issue_type = str(record.get("issue_type"))
        source_id = str(record.get("source_record_id"))
        field_name = record.get("field_name")
        violation_type = record.get("violation_type")
        self.issue_types[issue_type] += 1
        self.by_entity[entity] += 1
        self.by_entity_issue[(entity, issue_type)] += 1
        self.unique_records[entity].add(source_id)
        if field_name:
            self.by_field[(entity, str(field_name))] += 1
            if issue_type == "MISSING_FIELD":
                self.missing_by_field[(entity, str(field_name))] += 1
        if violation_type:
            self.by_violation[(entity, str(violation_type))] += 1
        classifier = (
            str(field_name)
            if issue_type == "MISSING_FIELD"
            else str(violation_type or issue_type)
        )
        self.by_entity_classifier[(entity, issue_type, classifier)] += 1

    def summary(self) -> dict[str, Any]:
        return {
            "row_count": self.rows,
            "unique_affected_records": sum(
                len(values) for values in self.unique_records.values()
            ),
            "issue_count_by_type": dict(sorted(self.issue_types.items())),
            "issue_count_by_entity": dict(sorted(self.by_entity.items())),
            "issue_count_by_entity_and_type": [
                {
                    "entity_name": entity,
                    "issue_type": issue_type,
                    "issue_count": count,
                }
                for (entity, issue_type), count in sorted(
                    self.by_entity_issue.items()
                )
            ],
            "issue_count_by_field": [
                {
                    "entity_name": entity,
                    "field_name": field_name,
                    "issue_count": count,
                }
                for (entity, field_name), count in sorted(self.by_field.items())
            ],
            "issue_count_by_violation_type": [
                {
                    "entity_name": entity,
                    "violation_type": violation_type,
                    "issue_count": count,
                }
                for (entity, violation_type), count in sorted(
                    self.by_violation.items()
                )
            ],
        }


def _entity_order(discovered: Iterable[str]) -> list[str]:
    known = [entity for entities in DOMAIN_ENTITIES.values() for entity in entities]
    extras = sorted(set(discovered).difference(known))
    return known + extras


def _domain_for(entity_name: str) -> str:
    for domain, entities in DOMAIN_ENTITIES.items():
        if entity_name in entities:
            return domain
    return "UNCLASSIFIED"


def _pct(numerator: int, denominator: int) -> float:
    return numerator / denominator * 100 if denominator else 0.0


def _check(name: str, passed: bool, details: str) -> dict[str, str]:
    return {
        "name": name,
        "status": "PASS" if passed else "FAIL",
        "details": details,
    }


def build_profile(
    client: Any,
    bucket: str,
    *,
    raw_prefix: str = "raw/",
    bronze_prefix: str = "bronze/",
    manifest_prefix: str = "quality/injection_manifest/",
    checkpoint_root: str | Path | None = None,
    workers: int = 1,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
) -> dict[str, Any]:
    """Build a complete current-state profile by streaming S3 read-only."""
    started = time.monotonic()
    raw_objects, warnings = discover_layer_objects(
        client, bucket, raw_prefix
    )
    bronze_objects, bronze_warnings = discover_layer_objects(
        client, bucket, bronze_prefix
    )
    warnings.extend(bronze_warnings)
    manifest_objects = discover_manifest_objects(
        client, bucket, manifest_prefix
    )
    entity_names = _entity_order(set(raw_objects) | set(bronze_objects))

    print("Profiling S3 injection manifest...", flush=True)
    manifest = ManifestProfiler()
    manifest_layer, layer_warnings = profile_object_group(
        client,
        bucket,
        manifest_objects,
        on_record=manifest.add,
        label="quality/injection_manifest",
        progress_every=progress_every,
        chunk_size=chunk_size,
    )
    warnings.extend(layer_warnings)
    manifest_summary = manifest.summary()
    checkpoint = CheckpointStore(checkpoint_root) if checkpoint_root else None

    def scan_group(
        layer_name: str,
        entity_name: str,
        objects: list[S3Object],
    ) -> tuple[dict[str, Any], list[str]]:
        group_id = f"{layer_name}/{entity_name}"
        signature = _object_signature(objects)
        initial = checkpoint.load_group(group_id, signature) if checkpoint else None

        def save(state: dict[str, Any], completed: bool) -> None:
            if checkpoint is not None:
                checkpoint.save_group(
                    group_id,
                    signature,
                    state,
                    completed=completed,
                )

        return profile_object_group(
            client,
            bucket,
            objects,
            label=group_id,
            progress_every=progress_every,
            chunk_size=chunk_size,
            initial_state=initial,
            checkpoint_callback=save if checkpoint else None,
        )

    raw_profiles: dict[str, dict[str, Any]] = {}
    bronze_profiles: dict[str, dict[str, Any] | None] = {}
    jobs = [
        ("raw", entity_name, raw_objects.get(entity_name, []))
        for entity_name in entity_names
    ] + [
        ("bronze", entity_name, bronze_objects.get(entity_name, []))
        for entity_name in entity_names
        if bronze_objects.get(entity_name)
    ]
    results: dict[tuple[str, str], tuple[dict[str, Any], list[str]]] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_jobs = {
            executor.submit(scan_group, layer, entity, objects): (layer, entity)
            for layer, entity, objects in jobs
        }
        for future in as_completed(future_jobs):
            results[future_jobs[future]] = future.result()

    for entity_name in entity_names:
        raw_profile, layer_warnings = results[("raw", entity_name)]
        raw_profiles[entity_name] = raw_profile
        warnings.extend(layer_warnings)
        bronze_result = results.get(("bronze", entity_name))
        if bronze_result is None:
            bronze_profiles[entity_name] = None
        else:
            bronze_profiles[entity_name] = bronze_result[0]
            warnings.extend(bronze_result[1])

    entities = {}
    schema_summary = {"raw": {}, "bronze": {}}
    null_profile_rows = []
    dirty_rows = []
    total_clean_rows = 0
    total_bronze_rows = 0
    for entity_name in entity_names:
        raw = raw_profiles[entity_name]
        bronze = bronze_profiles[entity_name]
        clean_rows = raw["row_count"]
        bronze_rows = bronze["row_count"] if bronze else None
        total_clean_rows += clean_rows
        if bronze_rows is not None:
            total_bronze_rows += bronze_rows
        missing_count = manifest.by_entity_issue[(entity_name, "MISSING_FIELD")]
        duplicate_count = manifest.by_entity_issue[(entity_name, "DUPLICATE_RECORD")]
        timestamp_count = manifest.by_entity_issue[
            (entity_name, "OUT_OF_ORDER_TIMESTAMP")
        ]
        affected = len(manifest.unique_records[entity_name])
        observed_fields = sorted(
            set(raw["schema"]["fields"])
            | (set(bronze["schema"]["fields"]) if bronze else set())
        )
        entity_profile = {
            "domain": _domain_for(entity_name),
            "entity_name": entity_name,
            "clean_raw_exists": raw["exists"],
            "bronze_exists": bronze is not None and bronze["exists"],
            "clean_row_count": clean_rows,
            "bronze_row_count": bronze_rows,
            "row_difference": (
                bronze_rows - clean_rows if bronze_rows is not None else None
            ),
            "clean_object_count": raw["object_count"],
            "bronze_object_count": bronze["object_count"] if bronze else None,
            "clean_partition_count": raw["partition_count"],
            "bronze_partition_count": bronze["partition_count"] if bronze else None,
            "earliest_clean_partition": raw["earliest_partition"],
            "latest_clean_partition": raw["latest_partition"],
            "earliest_bronze_partition": (
                bronze["earliest_partition"] if bronze else None
            ),
            "latest_bronze_partition": (
                bronze["latest_partition"] if bronze else None
            ),
            "clean_size_bytes": raw["size_bytes"],
            "bronze_size_bytes": bronze["size_bytes"] if bronze else None,
            "observed_fields": observed_fields,
            "total_injected_issues": manifest.by_entity[entity_name],
            "unique_affected_records": affected,
            "affected_pct": _pct(affected, clean_rows),
            "missing_count": missing_count,
            "missing_pct": _pct(missing_count, clean_rows),
            "duplicate_count": duplicate_count,
            "duplicate_pct": _pct(duplicate_count, clean_rows),
            "timestamp_issue_count": timestamp_count,
            "timestamp_issue_pct": _pct(timestamp_count, clean_rows),
            "duplicate_observation": {
                "source": "quality/injection_manifest",
                "excess_duplicate_rows": duplicate_count,
            },
            "objects_scanned": raw["object_count"]
            + (bronze["object_count"] if bronze else 0),
            "rows_processed": raw["row_count"]
            + (bronze["row_count"] if bronze else 0),
            "processing_time_seconds": raw["scan_runtime_seconds"]
            + (bronze["scan_runtime_seconds"] if bronze else 0.0),
        }
        entity_runtime = entity_profile["processing_time_seconds"]
        entity_profile["rows_per_second"] = (
            entity_profile["rows_processed"] / entity_runtime
            if entity_runtime
            else 0.0
        )
        entities[entity_name] = entity_profile
        schema_summary["raw"][entity_name] = raw["schema"]
        if bronze:
            schema_summary["bronze"][entity_name] = bronze["schema"]
        for layer_name, layer_profile in (("raw", raw), ("bronze", bronze)):
            if layer_profile is None:
                continue
            for field_name, field in layer_profile["schema"]["fields"].items():
                injected_missing = (
                    manifest.missing_by_field[(entity_name, field_name)]
                    if layer_name == "bronze"
                    else 0
                )
                null_profile_rows.append(
                    {
                        "layer": layer_name,
                        "entity_name": entity_name,
                        "field_name": field_name,
                        "row_count": layer_profile["row_count"],
                        "field_present_count": field["present_count"],
                        "field_absent_count": field["absent_count"],
                        "null_count": field["null_count"],
                        "null_pct": _pct(
                            field["null_count"], layer_profile["row_count"]
                        ),
                        "injected_missing_count": injected_missing,
                    }
                )
        for (group_entity, issue_type, classifier), count in sorted(
            manifest.by_entity_classifier.items()
        ):
            if group_entity != entity_name:
                continue
            dirty_rows.append(
                {
                    "entity_name": entity_name,
                    "issue_type": issue_type,
                    "field_or_violation_type": classifier,
                    "issue_count": count,
                    "clean_row_count": clean_rows,
                    "issue_pct": _pct(count, clean_rows),
                }
            )

    domains = {}
    for domain, domain_entities in DOMAIN_ENTITIES.items():
        available = [entities[name] for name in domain_entities if name in entities]
        domains[domain] = {
            "entity_count": len(available),
            "clean_rows": sum(row["clean_row_count"] for row in available),
            "bronze_rows": sum(
                row["bronze_row_count"] or 0 for row in available
            ),
            "clean_size_bytes": sum(row["clean_size_bytes"] for row in available),
            "bronze_size_bytes": sum(
                row["bronze_size_bytes"] or 0 for row in available
            ),
            "dirty_issues": sum(
                row["total_injected_issues"] for row in available
            ),
        }

    raw_storage = {
        "object_count": sum(row["object_count"] for row in raw_profiles.values()),
        "size_bytes": sum(row["size_bytes"] for row in raw_profiles.values()),
    }
    bronze_storage = {
        "object_count": sum(
            row["object_count"] for row in bronze_profiles.values() if row
        ),
        "size_bytes": sum(
            row["size_bytes"] for row in bronze_profiles.values() if row
        ),
    }
    manifest_storage = {
        "object_count": manifest_layer["object_count"],
        "size_bytes": manifest_layer["size_bytes"],
        "row_count": manifest_layer["row_count"],
    }

    checks = []
    checks.append(
        _check(
            "manifest_rows_equal_issue_sum",
            manifest.rows == sum(manifest.issue_types.values()),
            f"manifest={manifest.rows}, grouped={sum(manifest.issue_types.values())}",
        )
    )
    checks.append(
        _check(
            "entity_issue_totals",
            sum(manifest.by_entity.values()) == manifest.rows,
            f"entity_grouped={sum(manifest.by_entity.values())}",
        )
    )
    row_differences_pass = all(
        profile["row_difference"] is None
        or profile["row_difference"] == profile["duplicate_count"]
        for profile in entities.values()
    )
    checks.append(
        _check(
            "bronze_clean_duplicate_delta",
            row_differences_pass,
            "Bronze-clean row differences compared with manifest duplicates",
        )
    )
    checks.append(
        _check(
            "manifest_duplicate_counts_reconciled",
            row_differences_pass,
            "Manifest duplicates compared with Bronze-clean row differences",
        )
    )
    expected_missing = manifest.issue_types["MISSING_FIELD"]
    classified_missing = sum(manifest.missing_by_field.values())
    checks.append(
        _check(
            "manifest_missing_counts_reconciled",
            classified_missing == expected_missing,
            f"classified={classified_missing}, expected={expected_missing}",
        )
    )
    expected_timestamps = manifest.issue_types["OUT_OF_ORDER_TIMESTAMP"]
    classified_timestamps = sum(
        count
        for (_, issue_type, _), count in manifest.by_entity_classifier.items()
        if issue_type == "OUT_OF_ORDER_TIMESTAMP"
    )
    checks.append(
        _check(
            "manifest_timestamp_counts_reconciled",
            classified_timestamps == expected_timestamps,
            f"classified={classified_timestamps}, expected={expected_timestamps}",
        )
    )
    checks.append(
        _check(
            "manifest_unique_affected_records",
            manifest_summary["unique_affected_records"]
            == sum(len(values) for values in manifest.unique_records.values()),
            f"unique={manifest_summary['unique_affected_records']}",
        )
    )
    checks.append(
        _check(
            "storage_byte_reconciliation",
            raw_storage["size_bytes"]
            == sum(profile["clean_size_bytes"] for profile in entities.values())
            and bronze_storage["size_bytes"]
            == sum(
                profile["bronze_size_bytes"] or 0
                for profile in entities.values()
            ),
            "Layer storage bytes compared with per-entity totals",
        )
    )
    partition_ranges_pass = all(
        profile["clean_partition_count"] == 0
        or profile["earliest_clean_partition"]
        <= profile["latest_clean_partition"]
        for profile in entities.values()
    ) and all(
        not profile["bronze_exists"]
        or profile["earliest_bronze_partition"]
        <= profile["latest_bronze_partition"]
        for profile in entities.values()
    )
    checks.append(
        _check(
            "partition_date_ranges",
            partition_ranges_pass,
            "All discovered physical partition ranges are internally consistent",
        )
    )
    malformed_count = sum(
        profile["malformed_json_count"] + profile["non_object_row_count"]
        for profile in raw_profiles.values()
    ) + sum(
        profile["malformed_json_count"] + profile["non_object_row_count"]
        for profile in bronze_profiles.values()
        if profile
    ) + manifest_layer["malformed_json_count"]
    checks.append(
        _check(
            "stream_parse_errors",
            malformed_count == 0,
            f"malformed_or_non_object_rows={malformed_count}",
        )
    )
    statuses = {check["status"] for check in checks}
    overall_status = (
        "FAIL" if "FAIL" in statuses else "WARNING" if "WARNING" in statuses else "PASS"
    )

    affected_denominator = sum(
        profile["clean_row_count"]
        for profile in entities.values()
        if profile["bronze_exists"]
    )
    total_runtime = time.monotonic() - started
    runtime_per_entity = {
        entity_name: {
            "runtime_seconds": profile["processing_time_seconds"],
            "objects_scanned": profile["objects_scanned"],
            "rows_processed": profile["rows_processed"],
            "rows_per_second": profile["rows_per_second"],
        }
        for entity_name, profile in entities.items()
    }
    all_layer_profiles = list(raw_profiles.values()) + [
        profile for profile in bronze_profiles.values() if profile
    ]
    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "source": "s3",
            "bucket": bucket,
            "raw_prefix": _prefix(raw_prefix),
            "bronze_prefix": _prefix(bronze_prefix),
            "manifest_prefix": _prefix(manifest_prefix),
            "profiler_version": PROFILER_VERSION,
            "dirty_percentage_denominator": "clean raw rows per entity",
            "quality_issue_source": "quality/injection_manifest",
            "runtime_seconds": total_runtime,
            "total_runtime_seconds": total_runtime,
            "runtime_per_entity": runtime_per_entity,
            "total_entities_scanned": len(entities),
            "objects_scanned": sum(
                profile["object_count"] for profile in all_layer_profiles
            )
            + manifest_layer["object_count"],
            "rows_processed": sum(
                profile["row_count"] for profile in all_layer_profiles
            )
            + manifest_layer["row_count"],
            "workers": max(1, workers),
            "stream_chunk_bytes": chunk_size,
            "checkpoint_root": str(checkpoint_root) if checkpoint_root else None,
            "checkpoint_reused_objects": sum(
                profile["checkpoint_reused_object_count"]
                for profile in all_layer_profiles
            ),
        },
        "domains": domains,
        "entities": entities,
        "schemas": schema_summary,
        "dirty_data": {
            **manifest_summary,
            "overall_affected_pct": _pct(
                manifest_summary["unique_affected_records"],
                affected_denominator,
            ),
            "overall_affected_denominator": affected_denominator,
            "detail_rows": dirty_rows,
        },
        "null_profile_summary": null_profile_rows,
        "storage": {
            "raw": raw_storage,
            "bronze": bronze_storage,
            "manifest": manifest_storage,
        },
        "relationships": RELATIONSHIPS,
        "reconciliation": {
            "overall_status": overall_status,
            "checks": checks,
        },
        "totals": {
            "clean_rows": total_clean_rows,
            "materialized_bronze_rows": total_bronze_rows,
        },
        "warnings": warnings,
    }


def entity_summary_rows(profile: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for entity_name, entity in profile["entities"].items():
        rows.append(
            {
                "domain": entity["domain"],
                "entity_name": entity_name,
                "clean_exists": entity["clean_raw_exists"],
                "bronze_exists": entity["bronze_exists"],
                "clean_rows": entity["clean_row_count"],
                "bronze_rows": entity["bronze_row_count"],
                "row_difference": entity["row_difference"],
                "clean_objects": entity["clean_object_count"],
                "bronze_objects": entity["bronze_object_count"],
                "clean_partitions": entity["clean_partition_count"],
                "bronze_partitions": entity["bronze_partition_count"],
                "clean_earliest_date": entity["earliest_clean_partition"],
                "clean_latest_date": entity["latest_clean_partition"],
                "bronze_earliest_date": entity["earliest_bronze_partition"],
                "bronze_latest_date": entity["latest_bronze_partition"],
                "clean_size_bytes": entity["clean_size_bytes"],
                "bronze_size_bytes": entity["bronze_size_bytes"],
                "total_injected_issues": entity["total_injected_issues"],
                "affected_records": entity["unique_affected_records"],
                "affected_pct": entity["affected_pct"],
                "missing_count": entity["missing_count"],
                "missing_pct": entity["missing_pct"],
                "duplicate_count": entity["duplicate_count"],
                "duplicate_pct": entity["duplicate_pct"],
                "timestamp_issue_count": entity["timestamp_issue_count"],
                "timestamp_issue_pct": entity["timestamp_issue_pct"],
                "objects_scanned": entity["objects_scanned"],
                "rows_processed": entity["rows_processed"],
                "processing_time_seconds": entity["processing_time_seconds"],
                "rows_per_second": entity["rows_per_second"],
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: Iterable[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def write_profile_outputs(
    profile: dict[str, Any],
    output_root: str | Path,
) -> list[Path]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "profile": root / "data_profile.json",
        "entities": root / "entity_summary.csv",
        "dirty": root / "dirty_data_summary.csv",
        "nulls": root / "null_profile.csv",
        "schemas": root / "schema_summary.json",
    }
    paths["profile"].write_text(
        json.dumps(profile, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    paths["schemas"].write_text(
        json.dumps(profile["schemas"], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(paths["entities"], entity_summary_rows(profile), ENTITY_CSV_FIELDS)
    _write_csv(
        paths["dirty"],
        profile["dirty_data"]["detail_rows"],
        (
            "entity_name",
            "issue_type",
            "field_or_violation_type",
            "issue_count",
            "clean_row_count",
            "issue_pct",
        ),
    )
    _write_csv(
        paths["nulls"],
        profile["null_profile_summary"],
        (
            "layer",
            "entity_name",
            "field_name",
            "row_count",
            "field_present_count",
            "field_absent_count",
            "null_count",
            "null_pct",
            "injected_missing_count",
        ),
    )
    return list(paths.values())


def print_console_profile(profile: dict[str, Any], output_root: str | Path) -> None:
    print("\n" + "=" * 60)
    print("AI CHATBOT DATA LAKE PROFILE")
    print("SOURCE: S3")
    print("=" * 60)
    print("\nCLEAN RAW")
    print("-" * 60)
    for entity_name, entity in profile["entities"].items():
        print(f"{entity_name:<28}{entity['clean_row_count']:>14,}")
    print(f"\nTOTAL CLEAN ROWS: {profile['totals']['clean_rows']:,}")
    print("\nBRONZE QUALITY SUMMARY")
    print("-" * 60)
    print(f"{'Entity':<22}{'Missing':>12}{'Duplicate':>12}{'Bad Time':>12}")
    for entity_name, entity in profile["entities"].items():
        if entity["bronze_exists"]:
            print(
                f"{entity_name:<22}{entity['missing_count']:>12,}"
                f"{entity['duplicate_count']:>12,}"
                f"{entity['timestamp_issue_count']:>12,}"
            )
    dirty = profile["dirty_data"]
    print(f"\nTOTAL INJECTED ISSUES: {dirty['row_count']:,}")
    print(f"UNIQUE RECORDS AFFECTED: {dirty['unique_affected_records']:,}")
    print(f"OVERALL AFFECTED RATE: {dirty['overall_affected_pct']:.3f}%")
    print("\nS3 STORAGE")
    print("-" * 60)
    for label in ("raw", "bronze", "manifest"):
        storage = profile["storage"][label]
        print(
            f"{label.title():<12}{storage['object_count']:>6,} objects / "
            f"{human_bytes(storage['size_bytes'])}"
        )
    print(
        "\nSELF-CHECK: "
        f"{profile['reconciliation']['overall_status']}"
    )
    print(f"\nOUTPUT:\n{output_root}")
    print("=" * 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile the AI chatbot data lake directly from S3."
    )
    parser.add_argument("--bucket", help="Override the configured S3 bucket.")
    parser.add_argument("--raw-prefix", default="raw/")
    parser.add_argument("--bronze-prefix", default="bronze/")
    parser.add_argument(
        "--manifest-prefix",
        default="quality/injection_manifest/",
    )
    parser.add_argument(
        "--output-root",
        default="data/profile/latest/",
    )
    parser.add_argument(
        "--checkpoint-root",
        default="data/profile/checkpoints/",
        help="Local directory for resumable object-boundary checkpoints.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent entity scans (default: 4).",
    )
    parser.add_argument(
        "--chunk-size-mib",
        type=int,
        default=1,
        help="S3 streaming line-buffer chunk size in MiB (default: 1).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Print a progress line after this many rows (default: 100000).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(require_s3=False)
    bucket = args.bucket or config["s3_bucket"]
    if not bucket:
        raise RuntimeError("S3 bucket is required via --bucket or S3_BUCKET")
    client_kwargs = {
        "region_name": config["aws_default_region"] or DEFAULT_AWS_REGION,
    }
    client = boto3.client("s3", **client_kwargs)
    profile = build_profile(
        client,
        str(bucket),
        raw_prefix=args.raw_prefix,
        bronze_prefix=args.bronze_prefix,
        manifest_prefix=args.manifest_prefix,
        checkpoint_root=args.checkpoint_root,
        workers=args.workers,
        chunk_size=max(1, args.chunk_size_mib) * 1024 * 1024,
        progress_every=max(0, args.progress_every),
    )
    paths = write_profile_outputs(profile, args.output_root)
    if profile["reconciliation"]["overall_status"] == "FAIL":
        raise RuntimeError("S3 data profile self-checks failed")
    print_console_profile(profile, args.output_root)
    print(f"Generated {len(paths)} profile output files")


if __name__ == "__main__":
    main()
