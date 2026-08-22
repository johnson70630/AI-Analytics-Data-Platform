import hashlib
import json
import unittest

from src.data_generator.bronze_materialization import (
    COPY_ONLY_BRONZE_ENTITIES,
    assert_inventory_unchanged,
    capture_protected_s3_inventory,
    materialize_missing_bronze_entities,
    validate_materialized_bronze_entities,
)


class FakeBody:
    def __init__(self, payload):
        self.payload = payload

    def iter_lines(self, chunk_size=None):
        del chunk_size
        return iter(self.payload.splitlines())


class FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, *, Bucket, Prefix):
        del Bucket
        contents = [
            {
                "Key": key,
                "Size": len(item["body"]),
                "ETag": item["etag"],
            }
            for key, item in sorted(self.client.objects.items())
            if key.startswith(Prefix)
        ]
        return [{"Contents": contents}] if contents else [{}]


class FakeS3Client:
    def __init__(self, records_by_key):
        self.objects = {}
        self.operations = []
        for key, records in records_by_key.items():
            body = (
                "\n".join(
                    json.dumps(record, separators=(",", ":"))
                    for record in records
                )
                + "\n"
            ).encode()
            self._store(key, body)

    def _store(self, key, body, content_type="application/x-ndjson"):
        self.objects[key] = {
            "body": body,
            "content_type": content_type,
            "etag": f'"{hashlib.md5(body).hexdigest()}"',  # nosec B324
        }

    def get_paginator(self, name):
        self.operations.append(("get_paginator", name))
        if name != "list_objects_v2":
            raise AssertionError(name)
        return FakePaginator(self)

    def get_object(self, *, Bucket, Key):
        del Bucket
        self.operations.append(("get_object", Key))
        item = self.objects[Key]
        return {
            "Body": FakeBody(item["body"]),
            "ContentType": item["content_type"],
        }

    def copy_object(
        self,
        *,
        Bucket,
        Key,
        CopySource,
        CopySourceIfMatch,
        IfNoneMatch,
        ContentType,
        MetadataDirective,
    ):
        del Bucket
        self.operations.append(("copy_object", Key))
        if Key in self.objects:
            raise AssertionError(f"overwrite attempted: {Key}")
        source = self.objects[CopySource["Key"]]
        self.assert_equal(CopySourceIfMatch, source["etag"])
        self.assert_equal(IfNoneMatch, "*")
        self.assert_equal(ContentType, "application/x-ndjson")
        self.assert_equal(MetadataDirective, "REPLACE")
        self._store(Key, source["body"], ContentType)
        return {"CopyObjectResult": {"ETag": source["etag"]}}

    @staticmethod
    def assert_equal(actual, expected):
        if actual != expected:
            raise AssertionError(f"{actual!r} != {expected!r}")


class BronzeMaterializationTests(unittest.TestCase):
    def setUp(self):
        day = "2026-08-20"
        ingested = f"{day}T12:00:00Z"
        source_records = {
            "models": [{"model_id": "m1", "release_date": "2026-01-01"}],
            "devices": [{"device_id": "d1", "device_type": "DESKTOP"}],
            "subscription_plans": [
                {"plan_id": "p1", "monthly_price": 20.0}
            ],
            "user_updates": [
                {
                    "update_id": "uu1",
                    "user_id": "u1",
                    "updated_at": f"{day}T11:59:00Z",
                    "ingested_at": ingested,
                }
            ],
            "conversations": [
                {
                    "conversation_id": "c1",
                    "created_at": f"{day}T11:58:00Z",
                    "ingested_at": ingested,
                }
            ],
            "errors": [
                {
                    "error_id": "e1",
                    "occurred_at": f"{day}T11:57:00Z",
                    "ingested_at": ingested,
                }
            ],
            "subscriptions": [
                {
                    "subscription_id": "s1",
                    "started_at": f"{day}T10:00:00Z",
                    "ended_at": None,
                    "updated_at": f"{day}T11:56:00Z",
                    "ingested_at": ingested,
                }
            ],
        }
        records_by_key = {
            f"raw/{entity}/dt={day}/{entity}.json": records
            for entity, records in source_records.items()
        }
        records_by_key.update(
            {
                f"bronze/users/dt={day}/dirty.json": [
                    {"user_id": "u1", "email": None}
                ],
                f"quality/injection_manifest/dt={day}/manifest.json": [
                    {"injection_id": "i1", "entity_name": "users"}
                ],
            }
        )
        self.client = FakeS3Client(records_by_key)

    def test_materializes_and_validates_copy_only_entities(self):
        protected_before = capture_protected_s3_inventory(
            self.client, "bucket"
        )
        result = materialize_missing_bronze_entities(self.client, "bucket")
        validation = validate_materialized_bronze_entities(
            self.client, "bucket"
        )
        protected_after = capture_protected_s3_inventory(self.client, "bucket")
        assert_inventory_unchanged(protected_before, protected_after)

        self.assertEqual(sum(result["copied_objects"].values()), 7)
        self.assertEqual(validation["status"], "PASS")
        for entity_name in COPY_ONLY_BRONZE_ENTITIES:
            self.assertEqual(validation["raw_row_counts"][entity_name], 1)
            self.assertEqual(validation["bronze_row_counts"][entity_name], 1)
            self.assertEqual(validation["ids_preserved"][entity_name], 1)
            self.assertEqual(validation["schemas_preserved"][entity_name], 1)
        self.assertTrue(
            all(
                operation in {"get_paginator", "get_object", "copy_object"}
                for operation, _ in self.client.operations
            )
        )

    def test_rerun_skips_identical_objects_without_overwrite(self):
        materialize_missing_bronze_entities(self.client, "bucket")
        first_copy_count = sum(
            operation == "copy_object" for operation, _ in self.client.operations
        )
        result = materialize_missing_bronze_entities(self.client, "bucket")
        second_copy_count = sum(
            operation == "copy_object" for operation, _ in self.client.operations
        )
        self.assertEqual(first_copy_count, 7)
        self.assertEqual(second_copy_count, 7)
        self.assertEqual(sum(result["skipped_objects"].values()), 7)

    def test_refuses_to_overwrite_nonmatching_bronze_object(self):
        source_key = next(key for key in self.client.objects if key.startswith("raw/models/"))
        destination_key = "bronze/" + source_key.removeprefix("raw/")
        self.client._store(destination_key, b'{"model_id":"changed"}\n')
        with self.assertRaisesRegex(RuntimeError, "Refusing to overwrite"):
            materialize_missing_bronze_entities(self.client, "bucket")

    def test_validation_rejects_partition_ingestion_mismatch(self):
        materialize_missing_bronze_entities(self.client, "bucket")
        key = next(
            key for key in self.client.objects if key.startswith("bronze/errors/")
        )
        record = json.loads(self.client.objects[key]["body"])
        record["ingested_at"] = "2026-08-19T23:59:59Z"
        self.client._store(key, (json.dumps(record) + "\n").encode())
        with self.assertRaisesRegex(ValueError, "metadata mismatch|payload changed"):
            validate_materialized_bronze_entities(self.client, "bucket")


if __name__ == "__main__":
    unittest.main()
