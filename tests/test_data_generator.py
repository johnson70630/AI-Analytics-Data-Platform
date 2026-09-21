import json
import random
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.data_generator.config import load_config
from src.data_generator.main import generate_test_records
from src.data_generator.utils import add_random_delay, initialize_randomness
from src.data_generator.writers import (
    serialize_ndjson,
    write_records_locally,
    write_records_to_s3,
)


class DataGeneratorFoundationTests(unittest.TestCase):
    def test_seed_reproduces_test_records(self):
        first = generate_test_records(
            initialize_randomness(42), date(2026, 8, 20)
        )
        second = generate_test_records(
            initialize_randomness(42), date(2026, 8, 20)
        )
        self.assertEqual(first, second)

    def test_ndjson_serializes_supported_values(self):
        content = serialize_ndjson(
            [
                {
                    "text": "hello",
                    "number": 3,
                    "enabled": True,
                    "missing": None,
                    "day": date(2026, 8, 20),
                    "time": datetime(2026, 8, 20, tzinfo=timezone.utc),
                }
            ]
        )
        lines = content.splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["time"], "2026-08-20T00:00:00Z")

    def test_random_delay_never_precedes_input(self):
        random.seed(42)
        start = datetime(2026, 8, 20, tzinfo=timezone.utc)
        self.assertGreaterEqual(add_random_delay(start, 1, 10), start)

    def test_local_writer_uses_bronze_path_and_ndjson(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = write_records_locally(
                [{"id": "1"}, {"id": "2"}],
                "generator_test",
                date(2026, 8, 20),
                temporary_directory,
            )
            self.assertIsNotNone(output_path)
            path = Path(output_path)
            self.assertIn("raw/generator_test/dt=2026-08-20", path.as_posix())
            self.assertEqual(len(path.read_text().splitlines()), 2)

    def test_empty_local_write_is_skipped(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            self.assertIsNone(
                write_records_locally(
                    [], "generator_test", date(2026, 8, 20), temporary_directory
                )
            )

    @patch("src.data_generator.config.load_dotenv")
    @patch.dict("os.environ", {}, clear=True)
    def test_s3_config_lists_every_missing_value(self, _load_dotenv):
        with self.assertRaisesRegex(
            RuntimeError,
            "S3_BUCKET",
        ):
            load_config(require_s3=True)

    @patch("src.data_generator.config.load_dotenv")
    @patch.dict("os.environ", {"S3_BUCKET": "example-bucket"}, clear=True)
    def test_s3_config_uses_aws_provider_chain(self, _load_dotenv):
        config = load_config(require_s3=True)
        self.assertEqual(config["s3_bucket"], "example-bucket")
        self.assertNotIn("aws_access_key_id", config)
        self.assertNotIn("aws_secret_access_key", config)

    @patch("src.data_generator.writers.boto3.client")
    def test_s3_writer_uploads_ndjson(self, boto3_client):
        initialize_randomness(42)
        key = write_records_to_s3(
            [{"id": "1"}],
            "generator_test",
            date(2026, 8, 20),
            bucket="example-bucket",
            region="us-east-1",
        )
        self.assertTrue(key.startswith("raw/generator_test/dt=2026-08-20/"))
        boto3_client.return_value.put_object.assert_called_once()
        request = boto3_client.return_value.put_object.call_args.kwargs
        self.assertEqual(request["Bucket"], "example-bucket")
        self.assertEqual(request["Body"], b'{"id":"1"}\n')
        boto3_client.assert_called_once_with("s3", region_name="us-east-1")


if __name__ == "__main__":
    unittest.main()
