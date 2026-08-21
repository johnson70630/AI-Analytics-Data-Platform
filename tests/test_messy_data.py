import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

from src.data_generator.ingestion import EVENT_ID_FIELDS
from src.data_generator.messy_data import (
    BRONZE_ENTITIES,
    DUPLICATE_RECORD,
    MISSING_FIELD,
    OUT_OF_ORDER_TIMESTAMP,
    assert_clean_baseline_unchanged,
    inject_record,
    snapshot_clean_baseline,
    validate_local_messy_bronze,
)
from src.data_generator.writers import PartitionedNDJSONWriter


class MessyDataTests(unittest.TestCase):
    def setUp(self):
        self.seed = 42
        self.generation_end = datetime(
            2026,
            8,
            20,
            23,
            59,
            59,
            tzinfo=timezone.utc,
        )
        self.event_at = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)
        self.contexts = {
            "conversations": {"conversation-1": self.event_at},
            "purchases": {"purchase-1": self.event_at},
        }

    def _template(self, entity_name):
        ingested_at = self.event_at + timedelta(seconds=20)
        templates = {
            "users": {
                "user_id": "user",
                "email": "person@example.com",
                "signup_at": self.event_at,
                "ingested_at": ingested_at,
            },
            "messages": {
                "message_id": "message",
                "conversation_id": "conversation-1",
                "user_id": "user-1",
                "message_text": "Synthetic message",
                "created_at": self.event_at + timedelta(seconds=10),
                "ingested_at": ingested_at,
            },
            "completions": {
                "completion_id": "completion",
                "user_id": "user-1",
                "requested_at": self.event_at,
                "completed_at": self.event_at + timedelta(seconds=5),
                "ingested_at": ingested_at,
            },
            "model_inferences": {
                "inference_id": "inference",
                "model_id": "model-1",
                "request_at": self.event_at,
                "response_at": self.event_at + timedelta(seconds=4),
                "ingested_at": ingested_at,
            },
            "feedback": {
                "feedback_id": "feedback",
                "feedback_type": "THUMBS_UP",
                "created_at": self.event_at,
                "ingested_at": ingested_at,
            },
            "purchases": {
                "purchase_id": "purchase",
                "purchase_type": "RENEWAL",
                "purchase_created_at": self.event_at,
                "updated_at": self.event_at + timedelta(seconds=10),
                "ingested_at": ingested_at,
            },
            "payments": {
                "payment_id": "payment",
                "purchase_id": "purchase-1",
                "payment_method": "CARD",
                "processed_at": self.event_at + timedelta(seconds=5),
                "refunded_at": None,
                "updated_at": self.event_at + timedelta(seconds=5),
                "ingested_at": ingested_at,
            },
        }
        return templates[entity_name]

    def _find_injection(self, entity_name, issue_type, template=None):
        template = dict(template or self._template(entity_name))
        id_field = EVENT_ID_FIELDS[entity_name]
        for index in range(100_000):
            record = dict(template)
            record[id_field] = f"{entity_name}-{index}"
            result = inject_record(
                record,
                entity_name,
                self.seed,
                self.generation_end,
                self.contexts,
            )
            if result[2] is not None and result[2]["issue_type"] == issue_type:
                return record, result
        self.fail(f"Could not find deterministic {issue_type} for {entity_name}")

    def test_same_seed_selects_identical_records(self):
        first = self._find_injection("messages", DUPLICATE_RECORD)
        second = self._find_injection("messages", DUPLICATE_RECORD)
        self.assertEqual(first, second)
        self.assertEqual(first[1][2]["injection_id"], second[1][2]["injection_id"])

    def test_missing_field_preserves_clean_original(self):
        clean, (dirty, duplicate, manifest) = self._find_injection(
            "users",
            MISSING_FIELD,
        )
        self.assertEqual(clean["email"], "person@example.com")
        self.assertIsNone(dirty["email"])
        self.assertIsNone(duplicate)
        self.assertEqual(manifest["original_value"], clean["email"])
        self.assertIsNone(manifest["injected_value"])

    def test_duplicate_replay_preserves_id_payload_and_partition(self):
        clean, (original, duplicate, manifest) = self._find_injection(
            "payments",
            DUPLICATE_RECORD,
        )
        self.assertEqual(original, clean)
        self.assertEqual(duplicate["payment_id"], clean["payment_id"])
        self.assertGreater(duplicate["ingested_at"], clean["ingested_at"])
        comparable = dict(duplicate)
        comparable["ingested_at"] = clean["ingested_at"]
        self.assertEqual(comparable, clean)
        self.assertEqual(
            duplicate["ingested_at"].date(),
            date.fromisoformat(manifest["dirty_ingested_at"][:10]),
        )

    def test_timestamp_injections_create_real_causal_violations(self):
        _, (completion, _, completion_manifest) = self._find_injection(
            "completions",
            OUT_OF_ORDER_TIMESTAMP,
        )
        self.assertLess(completion["completed_at"], completion["requested_at"])
        self.assertEqual(
            completion_manifest["violation_type"],
            "COMPLETION_BEFORE_REQUEST",
        )
        _, (payment, _, payment_manifest) = self._find_injection(
            "payments",
            OUT_OF_ORDER_TIMESTAMP,
        )
        self.assertLess(
            payment["processed_at"],
            self.contexts["purchases"][payment["purchase_id"]],
        )
        self.assertEqual(
            payment_manifest["violation_type"],
            "PAYMENT_BEFORE_PURCHASE",
        )

    def test_serialized_manifest_reconciles_and_raw_baseline_is_unchanged(self):
        desired_issues = {
            "users": MISSING_FIELD,
            "messages": DUPLICATE_RECORD,
            "completions": OUT_OF_ORDER_TIMESTAMP,
            "model_inferences": DUPLICATE_RECORD,
            "feedback": MISSING_FIELD,
            "purchases": MISSING_FIELD,
            "payments": OUT_OF_ORDER_TIMESTAMP,
        }
        clean_records = {}
        injected_records = {}
        manifests = []
        for entity_name in BRONZE_ENTITIES:
            clean, result = self._find_injection(
                entity_name,
                desired_issues[entity_name],
            )
            clean_records[entity_name] = [clean]
            dirty, duplicate, manifest = result
            injected_records[entity_name] = [dirty] + (
                [duplicate] if duplicate is not None else []
            )
            manifests.append(manifest)

        context_purchase = self._template("purchases")
        context_purchase["purchase_id"] = "purchase-1"
        clean_records["purchases"].append(context_purchase)
        injected_records["purchases"].append(context_purchase)

        with tempfile.TemporaryDirectory() as output_root:
            conversation = {
                "conversation_id": "conversation-1",
                "created_at": self.event_at,
                "ingested_at": self.event_at + timedelta(seconds=1),
            }
            for entity_name, records in {
                **clean_records,
                "conversations": [conversation],
            }.items():
                with PartitionedNDJSONWriter(
                    output_root,
                    entity_name,
                ) as writer:
                    for record in records:
                        writer.write(record, record["ingested_at"].date())
            before = snapshot_clean_baseline(output_root)
            for entity_name, records in injected_records.items():
                with PartitionedNDJSONWriter(
                    output_root,
                    entity_name,
                    prefix="bronze",
                ) as writer:
                    for record in records:
                        writer.write(record, record["ingested_at"].date())
            with PartitionedNDJSONWriter(
                output_root,
                "injection_manifest",
                prefix="quality",
            ) as writer:
                for manifest in manifests:
                    writer.write(manifest, date(2026, 8, 20))
            after = snapshot_clean_baseline(output_root)
            assert_clean_baseline_unchanged(before, after)
            summary = validate_local_messy_bronze(output_root)
            self.assertEqual(summary["manifest_rows"], len(manifests))
            self.assertEqual(summary["manifest_reconciliation"], "PASS")
            self.assertEqual(summary["total_issues"], len(manifests))
            self.assertEqual(summary["unique_records"], len(manifests))
            self.assertEqual(summary["duplicate_total"], 2)
            self.assertEqual(summary["timestamp_total"], 2)
            self.assertEqual(summary["missing_total"], 3)


if __name__ == "__main__":
    unittest.main()
