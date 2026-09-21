import tempfile
import unittest
import uuid
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

from src.data_generator.daily_generation import (
    _object_key,
    assert_target_available,
    daily_seed,
    generate_daily_records,
    validate_daily_records,
    write_daily_locally,
)
from src.data_generator.main import parse_args


class DailyGenerationTests(unittest.TestCase):
    @patch("sys.argv", ["generator", "--daily-date", "2026-08-21"])
    def test_daily_date_argument_parsing(self):
        self.assertEqual(parse_args().daily_date, date(2026, 8, 21))

    def test_daily_seed_is_date_and_seed_deterministic(self):
        day = date(2026, 8, 21)
        self.assertEqual(daily_seed(day, 42), daily_seed(day, 42))
        self.assertNotEqual(daily_seed(day, 42), daily_seed(day, 43))
        self.assertNotEqual(daily_seed(day, 42), daily_seed(day + timedelta(days=1), 42))

    def test_object_keys_are_target_date_scoped(self):
        key = _object_key("raw", "messages", date(2026, 8, 21), 42)
        self.assertEqual(
            key,
            "raw/messages/dt=2026-08-21/daily-2026-08-21-seed-42.json",
        )
        self.assertIn("bronze/messages/dt=2026-08-21/", _object_key(
            "bronze", "messages", date(2026, 8, 21), 42
        ))

    def test_existing_partition_is_rejected(self):
        client = MagicMock()
        client.list_objects_v2.side_effect = lambda **kwargs: {
            "KeyCount": 1 if kwargs["Prefix"].startswith("raw/messages/") else 0
        }
        with self.assertRaises(FileExistsError):
            assert_target_available(
                client, "bucket", date(2026, 8, 21), date(2026, 8, 20)
            )

    def test_overwrite_allows_only_the_target_date(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"KeyCount": 1}
        assert_target_available(
            client,
            "bucket",
            date(2026, 8, 21),
            date(2026, 8, 20),
            overwrite=True,
        )

    def test_non_next_date_is_rejected(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"KeyCount": 0}
        with self.assertRaises(ValueError):
            assert_target_available(
                client, "bucket", date(2026, 8, 22), date(2026, 8, 20)
            )

    def test_writer_never_mutates_historical_partition(self):
        timestamp = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
        records = {"messages": [{"message_id": "m1", "ingested_at": timestamp}]}
        bronze = {"messages": list(records["messages"])}
        with tempfile.TemporaryDirectory() as root:
            historical = Path(root) / "raw/messages/dt=2026-08-20/history.json"
            historical.parent.mkdir(parents=True)
            historical.write_text('{"message_id":"old"}\n')
            before = historical.read_bytes()
            paths = write_daily_locally(
                root, date(2026, 8, 21), 42, records, bronze, []
            )
            self.assertEqual(historical.read_bytes(), before)
            self.assertTrue(paths["raw"]["messages"].exists())
            with self.assertRaises(FileExistsError):
                write_daily_locally(
                    root, date(2026, 8, 21), 42, records, bronze, []
                )

    def test_generated_records_reuse_existing_users_and_conversations(self):
        day = date(2026, 8, 21)
        prior = day - timedelta(days=1)
        user = {
            "user_id": "u1", "email": "u@example.com", "name": "U",
            "country_code": "US", "account_status": "ACTIVE",
            "signup_source": "ORGANIC",
            "signup_at": datetime(2025, 8, 21, tzinfo=timezone.utc),
            "ingested_at": datetime(2025, 8, 21, tzinfo=timezone.utc),
        }
        conversation = {
            "conversation_id": "c-old", "user_id": "u1",
            "created_at": datetime(2026, 8, 19, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 8, 19, tzinfo=timezone.utc),
        }
        message = {
            "message_id": "m-old", "conversation_id": "c-old", "user_id": "u1",
            "device_id": "device_001", "sequence_number": 1,
            "message_text": "old", "created_at": datetime(2026, 8, 20, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 8, 20, tzinfo=timezone.utc),
        }
        model = {
            "model_id": "model_003", "model_name": "GPT", "model_version": "3",
            "provider": "OPENAI", "release_date": date(2025, 1, 1),
            "active_flag": True,
        }
        device = {"device_id": "device_001", "device_type": "DESKTOP",
                  "operating_system": "macOS", "browser": "Chrome",
                  "app_platform": "WEB"}
        state = {
            "users": [user], "updates": [],
            "user_state": {"u1": {key: user[key] for key in (
                "email", "name", "country_code", "account_status"
            )}},
            "models": [model], "devices": [device], "plans": [],
            "subscriptions": [],
            "recent": {"conversations": [conversation], "messages": [message],
                       "completions": [], "model_inferences": [], "feedback": [],
                       "errors": [], "purchases": [], "payments": []},
            "counts": {entity: Counter({prior: 1}) for entity in (
                "user_updates", "conversations", "messages", "completions",
                "model_inferences", "feedback", "errors", "subscriptions",
                "purchases", "payments"
            )},
        }
        first = generate_daily_records(state, day, 42)
        second = generate_daily_records(state, day, 42)
        self.assertEqual(first, second)
        self.assertEqual(
            validate_daily_records(first, state, day),
            {"referential_violations": 0, "causal_violations": 0},
        )
        self.assertTrue(all(row["user_id"] == "u1" for row in first["messages"]))
        self.assertIn("c-old", first["_metadata"]["continued_conversation_ids"])
        generated_ids = [
            row[id_field]
            for entity, id_field in (
                ("conversations", "conversation_id"),
                ("messages", "message_id"),
                ("completions", "completion_id"),
                ("model_inferences", "inference_id"),
                ("errors", "error_id"),
            )
            for row in first[entity]
        ]
        self.assertEqual(len(generated_ids), len(set(generated_ids)))
        for generated_id in generated_ids:
            self.assertEqual(str(uuid.UUID(generated_id)), generated_id)


if __name__ == "__main__":
    unittest.main()
