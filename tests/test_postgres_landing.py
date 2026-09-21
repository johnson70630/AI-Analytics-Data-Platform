import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from src.data_generator.postgres_landing import (
    ENTITY_SPECS,
    LINEAGE_COLUMNS,
    create_s3_client,
    create_table_sql,
    list_bronze_partition_dates,
    load_entity_incremental,
    parse_args,
    partition_date_from_key,
    record_to_postgres_row,
    select_incremental_dates,
)


class PostgresLandingTests(unittest.TestCase):
    @patch("src.data_generator.postgres_landing.boto3.client")
    @patch.dict(
        "os.environ",
        {"S3_BUCKET": "example-bucket", "AWS_DEFAULT_REGION": "us-west-2"},
        clear=True,
    )
    def test_s3_client_uses_standard_credential_chain(self, boto3_client):
        client, bucket = create_s3_client()
        self.assertIs(client, boto3_client.return_value)
        self.assertEqual(bucket, "example-bucket")
        kwargs = boto3_client.call_args.kwargs
        self.assertEqual(kwargs["region_name"], "us-west-2")
        self.assertNotIn("aws_access_key_id", kwargs)
        self.assertNotIn("aws_secret_access_key", kwargs)
        self.assertNotIn("aws_session_token", kwargs)

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

    def test_cli_incremental_modes_are_unambiguous(self):
        args = parse_args(["--incremental", "--lookback-days", "2"])
        self.assertTrue(args.incremental)
        self.assertEqual(args.lookback_days, 2)
        args = parse_args(
            ["--incremental", "--entity", "messages", "--partition-date", "2026-08-21"]
        )
        self.assertEqual(args.start_date, date(2026, 8, 21))
        self.assertEqual(args.end_date, date(2026, 8, 21))
        self.assertTrue(parse_args(["--full-refresh"]).full_refresh)
        with self.assertRaises(SystemExit):
            parse_args(["--incremental", "--partition-date", "2026-08-21", "--lookback-days", "1"])

    def test_partition_discovery_uses_common_prefixes(self):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "bronze/messages/dt=2026-08-20/"},
                    {"Prefix": "bronze/messages/dt=2026-08-21/"},
                ]
            }
        ]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        self.assertEqual(
            list_bronze_partition_dates(client, "bucket", "messages"),
            [date(2026, 8, 20), date(2026, 8, 21)],
        )
        paginator.paginate.assert_called_once_with(
            Bucket="bucket", Prefix="bronze/messages/", Delimiter="/"
        )

    def test_per_entity_watermark_selects_new_dates_and_allows_noop(self):
        available = [date(2026, 8, 20), date(2026, 8, 21)]
        selected, reprocess = select_incremental_dates(
            available, date(2026, 8, 20)
        )
        self.assertEqual(selected, [date(2026, 8, 21)])
        self.assertFalse(reprocess)
        selected, reprocess = select_incremental_dates(
            available, date(2026, 8, 21)
        )
        self.assertEqual(selected, [])
        self.assertFalse(reprocess)

    def test_empty_entity_and_lookback_date_selection(self):
        available = [date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21)]
        self.assertEqual(select_incremental_dates([], date(2026, 8, 20))[0], [])
        selected, reprocess = select_incremental_dates(
            available, date(2026, 8, 21), lookback_days=1
        )
        self.assertEqual(selected, [date(2026, 8, 20), date(2026, 8, 21)])
        self.assertTrue(reprocess)
        selected, _ = select_incremental_dates(
            [*available, date(2026, 8, 22)],
            date(2026, 8, 21),
            lookback_days=1,
        )
        self.assertEqual(selected, [date(2026, 8, 21), date(2026, 8, 22)])
        selected, reprocess = select_incremental_dates(
            available,
            date(2026, 8, 21),
            start_date=date(2026, 8, 21),
            end_date=date(2026, 8, 21),
        )
        self.assertEqual(selected, [date(2026, 8, 21)])
        self.assertTrue(reprocess)

    def _connection(self, *fetchone_values):
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.side_effect = fetchone_values
        connection = MagicMock()
        connection.cursor.return_value = cursor
        return connection, cursor

    @patch("src.data_generator.postgres_landing._validate_representatives", return_value=1)
    @patch("src.data_generator.postgres_landing._validate_partition_physical_shape")
    @patch("src.data_generator.postgres_landing._flush_copy_batch")
    @patch("src.data_generator.postgres_landing._read_s3_object_rows")
    @patch("src.data_generator.postgres_landing.list_bronze_partition_keys")
    @patch("src.data_generator.postgres_landing.list_bronze_partition_dates")
    def test_new_partition_append_preserves_physical_rows(
        self, list_dates, list_keys, read_rows, flush, validate_shape, validate
    ):
        target = date(2026, 8, 21)
        list_dates.return_value = [date(2026, 8, 20), target]
        list_keys.return_value = ["bronze/messages/dt=2026-08-21/a.json"]
        duplicate = tuple(["same-id"] + [None] * 10)
        read_rows.return_value = [duplicate, duplicate]
        connection, cursor = self._connection((date(2026, 8, 20),), (2,))
        result = load_entity_incremental(
            connection, MagicMock(), "bucket", "messages"
        )
        self.assertEqual(result.rows, 2)
        self.assertEqual(result.partition_dates, (target,))
        self.assertFalse(result.reprocessed)
        self.assertEqual(read_rows.return_value[0], read_rows.return_value[1])
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertFalse(any("TRUNCATE" in sql or "DELETE" in sql for sql in statements))
        connection.commit.assert_called_once()
        connection.rollback.assert_not_called()

    @patch("src.data_generator.postgres_landing.list_bronze_partition_dates")
    def test_second_incremental_run_is_noop(self, list_dates):
        target = date(2026, 8, 21)
        list_dates.return_value = [target]
        connection, _ = self._connection((target,))
        result = load_entity_incremental(
            connection, MagicMock(), "bucket", "subscriptions"
        )
        self.assertEqual(result.rows, 0)
        self.assertEqual(result.partitions, 0)
        connection.commit.assert_called_once()

    @patch("src.data_generator.postgres_landing._validate_representatives", return_value=1)
    @patch("src.data_generator.postgres_landing._validate_partition_physical_shape")
    @patch("src.data_generator.postgres_landing._flush_copy_batch")
    @patch("src.data_generator.postgres_landing._read_s3_object_rows")
    @patch("src.data_generator.postgres_landing.list_bronze_partition_keys")
    @patch("src.data_generator.postgres_landing.list_bronze_partition_dates")
    def test_explicit_partition_reprocess_deletes_only_affected_date(
        self, list_dates, list_keys, read_rows, flush, validate_shape, validate
    ):
        target = date(2026, 8, 21)
        list_dates.return_value = [date(2026, 8, 20), target]
        list_keys.return_value = ["bronze/feedback/dt=2026-08-21/a.json"]
        read_rows.return_value = [tuple(["feedback-1"] + [None] * 9)]
        connection, cursor = self._connection((target,), (1,))
        result = load_entity_incremental(
            connection,
            MagicMock(),
            "bucket",
            "feedback",
            start_date=target,
            end_date=target,
        )
        self.assertTrue(result.reprocessed)
        delete_calls = [
            call
            for call in cursor.execute.call_args_list
            if "DELETE FROM" in call.args[0]
        ]
        self.assertEqual(len(delete_calls), 1)
        self.assertEqual(delete_calls[0].args[1], ([target],))
        self.assertNotIn("TRUNCATE", delete_calls[0].args[0])

    @patch("src.data_generator.postgres_landing._read_s3_object_rows")
    @patch("src.data_generator.postgres_landing.list_bronze_partition_keys")
    @patch("src.data_generator.postgres_landing.list_bronze_partition_dates")
    def test_incremental_failure_rolls_back_transaction(
        self, list_dates, list_keys, read_rows
    ):
        target = date(2026, 8, 21)
        list_dates.return_value = [target]
        list_keys.return_value = ["bronze/errors/dt=2026-08-21/a.json"]
        read_rows.side_effect = RuntimeError("read failure")
        connection, _ = self._connection((date(2026, 8, 20),))
        with self.assertRaisesRegex(RuntimeError, "read failure"):
            load_entity_incremental(
                connection, MagicMock(), "bucket", "errors"
            )
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
