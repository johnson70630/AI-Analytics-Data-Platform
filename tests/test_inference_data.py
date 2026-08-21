import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

from src.data_generator.inference_data import (
    generate_inference_records,
    validate_serialized_inference_data,
)
from src.data_generator.ingestion import iter_local_partitioned_records
from src.data_generator.reference_data import generate_models
from src.data_generator.utils import initialize_randomness
from src.data_generator.writers import PartitionedNDJSONWriter


class InferenceDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        initialize_randomness(42)
        cls.generation_end = datetime(
            2026,
            8,
            20,
            23,
            59,
            59,
            tzinfo=timezone.utc,
        )
        history_start = datetime(2025, 8, 21, tzinfo=timezone.utc)
        history_seconds = int((cls.generation_end - history_start).total_seconds())
        cls.messages = [
            {
                "message_id": f"message-{index}",
                "conversation_id": f"conversation-{index // 8}",
                "user_id": f"user-{index % 500}",
                "message_text": (
                    "Explain a practical data engineering pattern with testing "
                    + "and operational tradeoffs " * (1 + index % 8)
                ),
                "created_at": history_start
                + timedelta(seconds=history_seconds * index // 20_000),
            }
            for index in range(20_000)
        ]
        cls.models = generate_models()
        cls.completions, cls.inferences = generate_inference_records(
            cls.messages,
            cls.models,
            cls.generation_end,
        )

    def test_coverage_uniqueness_and_status_distribution(self):
        coverage = len(self.completions) / len(self.messages)
        self.assertGreaterEqual(coverage, 0.98)
        self.assertLessEqual(coverage, 0.995)
        self.assertEqual(
            len({row["completion_id"] for row in self.completions}),
            len(self.completions),
        )
        self.assertEqual(
            len({row["message_id"] for row in self.completions}),
            len(self.completions),
        )
        status_counts = {
            status: sum(
                row["completion_status"] == status for row in self.completions
            )
            for status in ("SUCCESS", "FAILED", "CANCELLED")
        }
        self.assertGreater(status_counts["SUCCESS"], status_counts["FAILED"])
        self.assertGreater(status_counts["FAILED"], status_counts["CANCELLED"])

    def test_causal_status_token_and_release_rules(self):
        messages = {row["message_id"]: row for row in self.messages}
        completions = {row["completion_id"]: row for row in self.completions}
        releases = {row["model_id"]: row["release_date"] for row in self.models}
        self.assertEqual(len(self.inferences), len(self.completions))
        for inference in self.inferences:
            completion = completions[inference["completion_id"]]
            message = messages[completion["message_id"]]
            self.assertGreaterEqual(completion["requested_at"], message["created_at"])
            self.assertLessEqual(completion["completed_at"], self.generation_end)
            self.assertGreater(inference["input_tokens"], 0)
            self.assertLessEqual(
                releases[inference["model_id"]],
                inference["request_at"].date(),
            )
            if completion["completion_status"] == "SUCCESS":
                self.assertEqual(inference["inference_status"], "SUCCESS")
                self.assertLess(inference["request_at"], inference["response_at"])
                self.assertLessEqual(
                    inference["response_at"],
                    completion["completed_at"],
                )
                measured = round(
                    (
                        inference["response_at"] - inference["request_at"]
                    ).total_seconds()
                    * 1_000
                )
                self.assertEqual(measured, inference["latency_ms"])
                self.assertGreater(inference["output_tokens"], 0)
                self.assertTrue(completion["response_text"].strip())
            else:
                self.assertEqual(inference["inference_status"], "FAILED")
                self.assertIsNone(completion["response_text"])
                self.assertIsNone(inference["response_at"])
                self.assertIsNone(inference["latency_ms"])
                self.assertIsNone(inference["output_tokens"])

    def test_serialized_reload_full_validation(self):
        with tempfile.TemporaryDirectory() as output_root:
            with PartitionedNDJSONWriter(
                output_root,
                "completions",
            ) as completion_writer, PartitionedNDJSONWriter(
                output_root,
                "model_inferences",
            ) as inference_writer:
                for completion in self.completions:
                    completion_writer.write(
                        completion,
                        completion["ingested_at"].date(),
                    )
                for inference in self.inferences:
                    inference_writer.write(
                        inference,
                        inference["ingested_at"].date(),
                    )
            report = validate_serialized_inference_data(
                iter(self.messages),
                iter_local_partitioned_records("completions", output_root),
                iter_local_partitioned_records("model_inferences", output_root),
                self.models,
                self.generation_end,
            )
            self.assertEqual(report["causal_validation"], "PASS")
            self.assertEqual(report["model_migration_validation"], "PASS")
            self.assertEqual(report["duplicate_verification"], "PASS")
            self.assertGreater(report["prompt_input_correlation"], 0.85)
            self.assertGreater(report["response_output_correlation"], 0.85)
            self.assertGreater(report["arrival_categories"]["over_one_day"], 0)
            self.assertGreater(report["partitions"]["completions"]["count"], 300)
            self.assertGreater(
                report["partitions"]["model_inferences"]["count"],
                300,
            )


if __name__ == "__main__":
    unittest.main()
