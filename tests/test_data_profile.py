import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from src.data_quality.profile_data import (
    FieldProfiler,
    build_profile,
    discover_layer_objects,
    entity_summary_rows,
    parse_partition_date,
    write_profile_outputs,
)


class FakeBody:
    def __init__(self, payload):
        self.payload = payload

    def iter_lines(self):
        return iter(self.payload.splitlines())

    def read(self):
        return self.payload


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, *, Bucket, Prefix):
        del Bucket
        contents = [
            {"Key": key, "Size": len(value)}
            for key, value in sorted(self.objects.items())
            if key.startswith(Prefix)
        ]
        return [{"Contents": contents}] if contents else [{}]


class FakeS3Client:
    def __init__(self, records_by_key):
        self.operations = []
        self.objects = {
            key: (
                "\n".join(
                    json.dumps(record, separators=(",", ":"))
                    for record in records
                )
                + "\n"
            ).encode("utf-8")
            for key, records in records_by_key.items()
        }

    def get_paginator(self, name):
        self.operations.append(("get_paginator", name))
        if name != "list_objects_v2":
            raise AssertionError(name)
        return FakePaginator(self.objects)

    def get_object(self, *, Bucket, Key):
        self.operations.append(("get_object", Key))
        del Bucket
        return {
            "Body": FakeBody(self.objects[Key]),
            "ContentType": "application/x-ndjson",
        }


class DataProfileTests(unittest.TestCase):
    def setUp(self):
        self.raw_user_1 = {
            "user_id": "u1",
            "email": "u1@example.test",
            "name": "User One",
            "ingested_at": "2026-01-01T10:00:00Z",
        }
        self.raw_user_2 = {
            "user_id": "u2",
            "email": "u2@example.test",
            "ingested_at": "2026-01-01T11:00:00Z",
        }
        duplicate = {
            **self.raw_user_2,
            "ingested_at": "2026-01-01T11:10:00Z",
        }
        raw_completion = {
            "completion_id": "c1",
            "requested_at": "2026-01-01T12:00:00Z",
            "completed_at": "2026-01-01T12:00:05Z",
            "ingested_at": "2026-01-01T12:00:10Z",
        }
        dirty_completion = {
            **raw_completion,
            "completed_at": "2026-01-01T11:59:59Z",
        }
        manifest = [
            {
                "injection_id": "i1",
                "entity_name": "users",
                "source_record_id": "u1",
                "issue_type": "MISSING_FIELD",
                "field_name": "email",
                "violation_type": "NULL_REQUIRED_FIELD",
            },
            {
                "injection_id": "i2",
                "entity_name": "users",
                "source_record_id": "u2",
                "issue_type": "DUPLICATE_RECORD",
                "field_name": "ingested_at",
                "violation_type": "REPLAY_SAME_DAY",
            },
            {
                "injection_id": "i3",
                "entity_name": "completions",
                "source_record_id": "c1",
                "issue_type": "OUT_OF_ORDER_TIMESTAMP",
                "field_name": "completed_at",
                "violation_type": "COMPLETION_BEFORE_REQUEST",
            },
        ]
        self.records_by_key = {
            "raw/models/dt=2026-01-01/models.json": [
                {"model_id": "m1", "active_flag": True}
            ],
            "raw/users/dt=2026-01-01/users.json": [
                self.raw_user_1,
                self.raw_user_2,
            ],
            "bronze/users/dt=2026-01-01/users.json": [
                {**self.raw_user_1, "email": None},
                self.raw_user_2,
                duplicate,
            ],
            "raw/completions/dt=2026-01-01/completions.json": [raw_completion],
            "bronze/completions/dt=2026-01-01/completions.json": [
                dirty_completion
            ],
            "quality/injection_manifest/dt=2026-01-02/manifest.json": manifest,
        }
        self.client = FakeS3Client(self.records_by_key)

    def _profile(self):
        return build_profile(self.client, "test-bucket")

    def test_s3_key_partition_parsing_and_discovery(self):
        self.assertEqual(
            parse_partition_date("raw/users/dt=2026-01-02/file.json").isoformat(),
            "2026-01-02",
        )
        with self.assertRaises(ValueError):
            parse_partition_date("raw/users/file.json")
        discovered, warnings = discover_layer_objects(
            self.client,
            "test-bucket",
            "raw/",
        )
        self.assertEqual(sorted(discovered), ["completions", "models", "users"])
        self.assertFalse(warnings)

    def test_field_profiler_distinguishes_absent_null_and_types(self):
        profiler = FieldProfiler()
        profiler.add({"a": None, "b": 1})
        profiler.add({"b": 2.5})
        summary = profiler.summary()
        self.assertEqual(summary["fields"]["a"]["present_count"], 1)
        self.assertEqual(summary["fields"]["a"]["absent_count"], 1)
        self.assertEqual(summary["fields"]["a"]["null_count"], 1)
        self.assertEqual(
            summary["fields"]["b"]["observed_types"],
            ["integer", "number"],
        )

    def test_counts_percentages_manifest_and_duplicate_reconciliation(self):
        profile = self._profile()
        users = profile["entities"]["users"]
        self.assertEqual(users["clean_row_count"], 2)
        self.assertEqual(users["bronze_row_count"], 3)
        self.assertEqual(users["row_difference"], 1)
        self.assertEqual(users["missing_count"], 1)
        self.assertEqual(users["missing_pct"], 50.0)
        self.assertEqual(users["duplicate_count"], 1)
        self.assertEqual(
            users["duplicate_observation"]["excess_duplicate_rows"], 1
        )
        self.assertEqual(profile["dirty_data"]["row_count"], 3)
        self.assertEqual(profile["dirty_data"]["unique_affected_records"], 3)
        self.assertEqual(profile["reconciliation"]["overall_status"], "PASS")

    def test_timestamp_grouping_storage_and_entity_without_bronze(self):
        profile = self._profile()
        completion = profile["entities"]["completions"]
        self.assertEqual(completion["timestamp_issue_count"], 1)
        violations = profile["dirty_data"]["issue_count_by_violation_type"]
        self.assertIn(
            {
                "entity_name": "completions",
                "violation_type": "COMPLETION_BEFORE_REQUEST",
                "issue_count": 1,
            },
            violations,
        )
        self.assertFalse(profile["entities"]["models"]["bronze_exists"])
        expected_raw_bytes = sum(
            len(value)
            for key, value in self.client.objects.items()
            if key.startswith("raw/")
        )
        self.assertEqual(profile["storage"]["raw"]["size_bytes"], expected_raw_bytes)

    def test_observed_nulls_are_separate_from_injected_missing(self):
        profile = self._profile()
        row = next(
            row
            for row in profile["null_profile_summary"]
            if row["layer"] == "bronze"
            and row["entity_name"] == "users"
            and row["field_name"] == "email"
        )
        self.assertEqual(row["null_count"], 1)
        self.assertEqual(row["injected_missing_count"], 1)
        name = next(
            row
            for row in profile["null_profile_summary"]
            if row["layer"] == "bronze"
            and row["entity_name"] == "users"
            and row["field_name"] == "name"
        )
        self.assertEqual(name["field_absent_count"], 2)

    def test_structured_outputs_have_deterministic_headers_and_order(self):
        profile = self._profile()
        rows = entity_summary_rows(profile)
        self.assertEqual(rows[0]["entity_name"], "models")
        with tempfile.TemporaryDirectory() as output_root:
            paths = write_profile_outputs(profile, output_root)
            self.assertEqual(len(paths), 5)
            for path in paths:
                self.assertTrue(path.exists())
            loaded = json.loads(
                (Path(output_root) / "data_profile.json").read_text()
            )
            self.assertEqual(loaded["metadata"]["source"], "s3")
            with (Path(output_root) / "entity_summary.csv").open() as source:
                csv_rows = list(csv.DictReader(source))
            self.assertEqual(csv_rows[0]["entity_name"], "models")
            dirty_header = (
                Path(output_root) / "dirty_data_summary.csv"
            ).read_text().splitlines()[0]
            self.assertEqual(
                dirty_header,
                "entity_name,issue_type,field_or_violation_type,issue_count,"
                "clean_row_count,issue_pct",
            )

    def test_profiler_is_read_only_and_logical_results_are_deterministic(self):
        first = self._profile()
        second = self._profile()
        self.assertTrue(
            all(
                operation in {"get_paginator", "get_object"}
                for operation, _ in self.client.operations
            )
        )
        for entity_name in first["entities"]:
            for field_name in (
                "clean_row_count",
                "bronze_row_count",
                "missing_count",
                "duplicate_count",
                "timestamp_issue_count",
                "observed_fields",
            ):
                self.assertEqual(
                    first["entities"][entity_name][field_name],
                    second["entities"][entity_name][field_name],
                )
        self.assertEqual(first["schemas"], second["schemas"])
        self.assertEqual(first["dirty_data"], second["dirty_data"])

    def test_checkpoint_reuses_completed_s3_objects(self):
        with tempfile.TemporaryDirectory() as checkpoint_root:
            first = build_profile(
                self.client,
                "test-bucket",
                checkpoint_root=checkpoint_root,
            )
            resumed_client = FakeS3Client(self.records_by_key)
            resumed = build_profile(
                resumed_client,
                "test-bucket",
                checkpoint_root=checkpoint_root,
            )
            fetched = [
                key
                for operation, key in resumed_client.operations
                if operation == "get_object"
            ]
            self.assertEqual(
                fetched,
                ["quality/injection_manifest/dt=2026-01-02/manifest.json"],
            )
            self.assertEqual(first["schemas"], resumed["schemas"])
            self.assertEqual(first["dirty_data"], resumed["dirty_data"])
            self.assertEqual(resumed["metadata"]["checkpoint_reused_objects"], 5)
            checkpoint = json.loads(
                (Path(checkpoint_root) / "profile_checkpoint.json").read_text()
            )
            self.assertEqual(len(checkpoint["completed_entities"]), 5)


if __name__ == "__main__":
    unittest.main()
