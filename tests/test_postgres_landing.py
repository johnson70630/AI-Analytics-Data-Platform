import unittest
from datetime import date, datetime, timezone

from src.data_generator.postgres_landing import (
    ENTITY_SPECS,
    LINEAGE_COLUMNS,
    create_table_sql,
    parse_args,
    partition_date_from_key,
    record_to_postgres_row,
)


class PostgresLandingTests(unittest.TestCase):
    def test_partition_date_is_parsed_from_matching_bronze_key(self):
        key = "bronze/messages/dt=2026-08-20/records.json"
        self.assertEqual(
            partition_date_from_key(key, "messages"),
            date(2026, 8, 20),
        )

    def test_partition_parser_rejects_wrong_entity_and_invalid_date(self):
        with self.assertRaises(ValueError):
            partition_date_from_key(
                "bronze/users/dt=2026-08-20/records.json", "messages"
            )
        with self.assertRaises(ValueError):
            partition_date_from_key(
                "bronze/messages/dt=2026-02-30/records.json", "messages"
            )

    def test_mapping_defines_all_fourteen_unconstrained_tables(self):
        self.assertEqual(len(ENTITY_SPECS), 14)
        self.assertEqual(
            set(ENTITY_SPECS),
            {
                "models",
                "devices",
                "subscription_plans",
                "users",
                "user_updates",
                "conversations",
                "messages",
                "completions",
                "model_inferences",
                "feedback",
                "errors",
                "subscriptions",
                "purchases",
                "payments",
            },
        )
        for spec in ENTITY_SPECS.values():
            self.assertEqual(spec.all_columns[-3:], LINEAGE_COLUMNS)
            ddl = create_table_sql(spec)
            self.assertNotIn("NOT NULL", ddl)
            self.assertNotIn("UNIQUE", ddl)
            self.assertNotIn("REFERENCES", ddl)

    def test_record_mapping_preserves_null_and_timestamp_source_values(self):
        spec = ENTITY_SPECS["completions"]
        invalid_order_record = {
            "completion_id": "completion-1",
            "message_id": "message-1",
            "conversation_id": "conversation-1",
            "user_id": None,
            "completion_status": "COMPLETED",
            "requested_at": "2026-08-20T12:00:10Z",
            "completed_at": "2026-08-20T12:00:00Z",
            "response_text": "unchanged",
            "ingested_at": "2026-08-20T12:00:11Z",
        }
        loaded_at = datetime(2026, 9, 10, tzinfo=timezone.utc)
        row = record_to_postgres_row(
            spec,
            invalid_order_record,
            partition_date=date(2026, 8, 20),
            source_file="s3://example/bronze/completions/file.json",
            loaded_at=loaded_at,
        )
        values = dict(zip(spec.all_column_names, row))
        self.assertIsNone(values["user_id"])
        self.assertEqual(values["requested_at"], "2026-08-20T12:00:10Z")
        self.assertEqual(values["completed_at"], "2026-08-20T12:00:00Z")
        self.assertEqual(values["loaded_at"], loaded_at)

    def test_mapping_rejects_unmapped_source_columns(self):
        with self.assertRaisesRegex(ValueError, "Unexpected models source columns"):
            record_to_postgres_row(
                ENTITY_SPECS["models"],
                {"model_id": "model-1", "unknown": "would-be-lost"},
                partition_date=date(2026, 8, 20),
                source_file="s3://example/key.json",
                loaded_at=datetime.now(timezone.utc),
            )

    def test_object_read_retry_does_not_return_partial_rows(self):
        from unittest.mock import Mock, patch

        from botocore.exceptions import ReadTimeoutError

        from src.data_generator.postgres_landing import _read_s3_object_rows

        failed_body = Mock()

        def failed_iteration():
            yield b'{"model_id":"partial"}'
            raise ReadTimeoutError(endpoint_url="s3")

        failed_body.iter_lines.side_effect = failed_iteration
        good_body = Mock()
        good_body.iter_lines.return_value = iter(
            [
                b'{"model_id":"model-1","model_name":"A",'
                b'"model_version":"1","provider":"P",'
                b'"release_date":"2026-01-01","active_flag":true}'
            ]
        )
        client = Mock()
        client.get_object.side_effect = [
            {"Body": failed_body},
            {"Body": good_body},
        ]
        with patch("builtins.print"):
            rows = _read_s3_object_rows(
                client,
                "bucket",
                "bronze/models/dt=2026-08-20/records.json",
                ENTITY_SPECS["models"],
                datetime(2026, 9, 10, tzinfo=timezone.utc),
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "model-1")
        self.assertEqual(client.get_object.call_count, 2)
        failed_body.close.assert_called_once()
        good_body.close.assert_called_once()

    def test_cli_accepts_one_entity_or_all(self):
        self.assertEqual(parse_args(["--entity", "messages"]).entity, "messages")
        self.assertTrue(parse_args(["--all"]).all)
        with self.assertRaises(SystemExit):
            parse_args(["--entity", "unknown"])
        with self.assertRaises(SystemExit):
            parse_args([])
        with self.assertRaises(SystemExit):
            parse_args(["--all", "--batch-size", "0"])


if __name__ == "__main__":
    unittest.main()
