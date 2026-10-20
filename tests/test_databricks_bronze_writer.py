import unittest
from datetime import date

from databricks.ingestion.bronze_writer import (
    SUPPORTED_DAILY_ENTITIES,
    TARGET_SCHEMA,
    _align_records,
    append_bronze_day,
    append_bronze_entity,
    target_table_name,
    transaction_app_id,
    transaction_version,
    validate_entity,
)


class FailOnSparkAccess:
    def __getattr__(self, name):
        raise AssertionError(f"Spark must not be accessed for an empty batch: {name}")


class BronzeWriterTests(unittest.TestCase):
    def test_supported_entity_validation(self):
        for entity in SUPPORTED_DAILY_ENTITIES:
            self.assertEqual(validate_entity(entity), entity)

    def test_unexpected_entity_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported daily Bronze entity"):
            validate_entity("users")
        with self.assertRaisesRegex(ValueError, "unsupported entities"):
            append_bronze_day(
                FailOnSparkAccess(),
                {"users": []},
                date(2026, 8, 23),
            )

    def test_transaction_identity_is_deterministic(self):
        self.assertEqual(
            transaction_app_id("model_inferences"),
            "ai-analytics-bronze-model_inferences",
        )
        self.assertEqual(transaction_version(date(2026, 8, 23)), 20260823)
        self.assertEqual(transaction_version(date(2026, 8, 23)), 20260823)

    def test_transaction_version_rejects_non_date(self):
        with self.assertRaisesRegex(TypeError, "datetime.date"):
            transaction_version("2026-08-23")

    def test_target_table_uses_managed_bronze_schema(self):
        self.assertEqual(TARGET_SCHEMA, "ai_analytics.bronze")
        self.assertEqual(
            target_table_name("messages"),
            "ai_analytics.bronze.messages",
        )

    def test_empty_entity_skips_spark_and_reports_zero_rows(self):
        summary = append_bronze_entity(
            FailOnSparkAccess(),
            "subscriptions",
            [],
            date(2026, 8, 23),
        )
        self.assertEqual(summary["input_physical_row_count"], 0)
        self.assertEqual(summary["validated_physical_row_count"], 0)
        self.assertEqual(summary["submitted_physical_row_count"], 0)
        self.assertFalse(summary["write_attempted"])
        self.assertEqual(summary["status"], "SKIPPED_EMPTY")
        self.assertEqual(summary["transaction_version"], 20260823)

    def test_empty_day_returns_one_stable_summary_per_entity(self):
        summaries = append_bronze_day(
            FailOnSparkAccess(),
            {},
            date(2026, 8, 23),
        )
        self.assertEqual(
            [summary["entity"] for summary in summaries],
            list(SUPPORTED_DAILY_ENTITIES),
        )
        self.assertTrue(
            all(summary["status"] == "SKIPPED_EMPTY" for summary in summaries)
        )

    def test_alignment_preserves_duplicates_and_null_fills_missing_fields(self):
        source = {"message_id": "message-1"}
        aligned = _align_records(
            "messages",
            [source, source],
            date(2026, 8, 23),
            ("message_id", "message_text", "dt"),
        )
        self.assertEqual(len(aligned), 2)
        self.assertEqual(aligned[0], aligned[1])
        self.assertIsNone(aligned[0]["message_text"])
        self.assertEqual(aligned[0]["dt"], date(2026, 8, 23))
        self.assertNotIn("dt", source)

    def test_alignment_rejects_fields_absent_from_target_schema(self):
        with self.assertRaisesRegex(ValueError, "unexpected_field"):
            _align_records(
                "messages",
                [{"message_id": "message-1", "unexpected_field": 1}],
                date(2026, 8, 23),
                ("message_id", "dt"),
            )


if __name__ == "__main__":
    unittest.main()
