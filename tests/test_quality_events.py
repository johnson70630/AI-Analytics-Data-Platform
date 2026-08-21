import sqlite3
import tempfile
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone

from src.data_generator.ingestion import iter_local_partitioned_records
from src.data_generator.quality_events import (
    ERROR_FIELDS,
    ERROR_SOURCES,
    ERROR_TYPE_CODES,
    FEEDBACK_FIELDS,
    SEVERITIES,
    build_feedback_event,
    build_independent_error,
    build_linked_model_error,
    should_generate_feedback,
    validate_serialized_quality_events,
)
from src.data_generator.reference_data import generate_models
from src.data_generator.utils import initialize_randomness
from src.data_generator.writers import PartitionedNDJSONWriter


class QualityEventTests(unittest.TestCase):
    def setUp(self):
        initialize_randomness(42)
        self.generation_end = datetime(
            2026,
            8,
            20,
            23,
            59,
            59,
            tzinfo=timezone.utc,
        )
        self.models = generate_models()

    def test_feedback_rate_is_sparse_and_user_propensity_is_not_uniform(self):
        propensities = {}
        counts = Counter()
        opportunities = 0
        for user_index in range(1_000):
            user_id = f"user-{user_index}"
            for _ in range(30):
                opportunities += 1
                if should_generate_feedback(user_id, 900, propensities):
                    counts[user_id] += 1
        rate = sum(counts.values()) / opportunities
        self.assertGreaterEqual(rate, 0.05)
        self.assertLessEqual(rate, 0.10)
        self.assertGreater(max(counts.values()), 5)
        self.assertGreater(len(set(counts.values())), 3)
        self.assertLess(len(counts), 1_000)

    def test_feedback_semantics_timing_and_model_quality_pattern(self):
        completed_at = self.generation_end - timedelta(days=30)
        fast_positive = 0
        slow_positive = 0
        for index in range(5_000):
            fast = build_feedback_event(
                f"fast-{index}",
                "user-1",
                completed_at,
                "model_006",
                650,
                self.generation_end,
            )
            slow = build_feedback_event(
                f"slow-{index}",
                "user-2",
                completed_at,
                "model_005",
                4_000,
                self.generation_end,
            )
            for feedback in (fast, slow):
                self.assertEqual(set(feedback), FEEDBACK_FIELDS)
                self.assertGreaterEqual(feedback["created_at"], completed_at)
                self.assertGreaterEqual(
                    feedback["ingested_at"],
                    feedback["created_at"],
                )
                self.assertLessEqual(feedback["ingested_at"], self.generation_end)
                if feedback["feedback_type"] == "THUMBS_UP":
                    self.assertEqual(feedback["feedback_score"], 1)
                elif feedback["feedback_type"] == "THUMBS_DOWN":
                    self.assertEqual(feedback["feedback_score"], -1)
                else:
                    self.assertIn(feedback["feedback_score"], range(1, 6))
            fast_positive += fast["feedback_type"] == "THUMBS_UP" or (
                fast["feedback_type"] == "RATING"
                and fast["feedback_score"] >= 4
            )
            slow_positive += slow["feedback_type"] == "THUMBS_UP" or (
                slow["feedback_type"] == "RATING"
                and slow["feedback_score"] >= 4
            )
        self.assertGreater(fast_positive, slow_positive)

    def test_failed_inference_error_has_complete_matching_lineage(self):
        failed_lineage = (
            "inference-1",
            "completion-1",
            "user-1",
            "model_003",
            int((self.generation_end - timedelta(days=1)).timestamp() * 1_000_000),
            "message-1",
            "conversation-1",
            int((self.generation_end - timedelta(hours=23)).timestamp() * 1_000_000),
        )
        error = build_linked_model_error(failed_lineage, self.generation_end)
        self.assertEqual(set(error), ERROR_FIELDS)
        self.assertEqual(error["inference_id"], "inference-1")
        self.assertEqual(error["completion_id"], "completion-1")
        self.assertEqual(error["user_id"], "user-1")
        self.assertEqual(error["model_id"], "model_003")
        self.assertEqual(error["error_source"], "MODEL")
        self.assertGreaterEqual(error["ingested_at"], error["occurred_at"])

    def test_independent_error_contexts_are_source_specific(self):
        event_at = self.generation_end - timedelta(days=30)
        event_us = int(event_at.timestamp() * 1_000_000)
        message_contexts = [("message-1", "conversation-1", "user-1", event_us)]
        inference_contexts = [
            (
                "inference-1",
                "completion-1",
                "user-1",
                "model_003",
                event_us,
                event_us + 1_000_000,
                "message-1",
                "conversation-1",
                event_us + 2_000_000,
            )
        ]
        for source in ERROR_SOURCES:
            for _ in range(100):
                error = build_independent_error(
                    source,
                    message_contexts,
                    inference_contexts,
                    self.models,
                    self.generation_end,
                )
                self.assertEqual(set(error), ERROR_FIELDS)
                self.assertIn(error["error_type"], ERROR_TYPE_CODES[source])
                self.assertEqual(
                    error["error_code"],
                    ERROR_TYPE_CODES[source][error["error_type"]],
                )
                self.assertIn(error["severity"], SEVERITIES)
                if source == "BILLING":
                    self.assertTrue(
                        all(
                            error[field] is None
                            for field in (
                                "conversation_id",
                                "message_id",
                                "completion_id",
                                "inference_id",
                                "model_id",
                            )
                        )
                    )
                if source == "DATABASE":
                    self.assertIsNone(error["model_id"])
                    self.assertIsNone(error["inference_id"])

    def test_partitioned_serialization_reloads_ingestion_dates(self):
        completed_at = self.generation_end - timedelta(days=60)
        records = [
            build_feedback_event(
                f"completion-{index}",
                f"user-{index % 10}",
                completed_at + timedelta(days=index % 20),
                "model_006",
                700,
                self.generation_end,
            )
            for index in range(1_000)
        ]
        with tempfile.TemporaryDirectory() as output_root:
            with PartitionedNDJSONWriter(
                output_root,
                "feedback",
            ) as writer:
                for record in records:
                    writer.write(record, record["ingested_at"].date())
            reloaded = list(
                iter_local_partitioned_records("feedback", output_root)
            )
            self.assertEqual(len(reloaded), len(records))
            self.assertTrue(
                all(
                    physical_date == record["ingested_at"].date()
                    for record, physical_date in reloaded
                )
            )
            self.assertGreater(len({date_value for _, date_value in reloaded}), 10)

    def test_duplicate_feedback_ids_are_rejected(self):
        connection = sqlite3.connect(":memory:")
        connection.executescript(
            """
            CREATE TABLE users (user_id TEXT PRIMARY KEY, signup_us INTEGER);
            CREATE TABLE conversations (
                conversation_id TEXT PRIMARY KEY, user_id TEXT, created_us INTEGER
            );
            CREATE TABLE messages (
                message_id TEXT PRIMARY KEY, conversation_id TEXT,
                user_id TEXT, created_us INTEGER
            );
            CREATE TABLE completions (
                completion_id TEXT PRIMARY KEY, message_id TEXT,
                conversation_id TEXT, user_id TEXT, status TEXT,
                requested_us INTEGER, completed_us INTEGER
            );
            CREATE TABLE inferences (
                inference_id TEXT PRIMARY KEY, completion_id TEXT,
                user_id TEXT, model_id TEXT, request_us INTEGER,
                response_us INTEGER, latency_ms INTEGER, status TEXT
            );
            """
        )
        feedback = build_feedback_event(
            "completion-1",
            "user-1",
            self.generation_end - timedelta(days=1),
            "model_006",
            700,
            self.generation_end,
        )
        duplicate = dict(feedback)
        with self.assertRaisesRegex(ValueError, "uniqueness"):
            validate_serialized_quality_events(
                connection,
                self.models,
                iter(((feedback, feedback["ingested_at"].date()),
                      (duplicate, duplicate["ingested_at"].date()))),
                iter(()),
                self.generation_end,
                production_scale=False,
            )
        connection.close()


if __name__ == "__main__":
    unittest.main()
