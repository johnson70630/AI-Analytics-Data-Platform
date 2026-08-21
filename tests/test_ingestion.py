import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from src.data_generator.ingestion import (
    add_ingestion_timestamps,
    ingestion_report,
    load_local_partitioned_records,
    validate_ingestion_records,
    write_partitioned_records_locally,
)
from src.data_generator.utils import initialize_randomness


class IngestionTests(unittest.TestCase):
    def setUp(self):
        initialize_randomness(42)
        self.generation_end = datetime(2026, 8, 20, 23, 59, 59, tzinfo=timezone.utc)

    def test_ingestion_preserves_business_fields_and_bounds_delays(self):
        records = [
            {
                "message_id": f"message-{index}",
                "created_at": self.generation_end - timedelta(days=30, seconds=index),
                "value": index,
            }
            for index in range(20_000)
        ]
        enriched = add_ingestion_timestamps(
            records,
            "created_at",
            self.generation_end,
        )
        self.assertEqual(
            [{key: value for key, value in row.items() if key != "ingested_at"} for row in enriched],
            records,
        )
        validate_ingestion_records("messages", enriched, self.generation_end)
        report = ingestion_report({"messages": enriched})
        self.assertGreater(report["maximum"], 21_600)
        self.assertGreater(report["categories"]["over_one_day"], 0)

    def test_partition_writer_and_reload_match_ingestion_dates(self):
        records = [
            {
                "message_id": "message-1",
                "created_at": datetime(2026, 8, 18, 23, 59, 58, tzinfo=timezone.utc),
                "ingested_at": datetime(2026, 8, 19, 0, 0, 4, tzinfo=timezone.utc),
            },
            {
                "message_id": "message-2",
                "created_at": datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc),
                "ingested_at": datetime(2026, 8, 20, 10, 0, 10, tzinfo=timezone.utc),
            },
        ]
        with tempfile.TemporaryDirectory() as output_root:
            paths = write_partitioned_records_locally(
                records,
                "messages",
                output_root,
            )
            self.assertEqual(len(paths), 2)
            reloaded, physical_dates, _ = load_local_partitioned_records(
                "messages",
                output_root,
            )
            validate_ingestion_records(
                "messages",
                reloaded,
                self.generation_end,
                physical_dates,
            )
            self.assertEqual(len(reloaded), 2)


if __name__ == "__main__":
    unittest.main()
