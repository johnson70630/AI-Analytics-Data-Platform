import unittest
from collections import Counter
from datetime import date

from src.data_generator.activity_data import (
    CONVERSATION_MAX_COUNT,
    CONVERSATION_MIN_COUNT,
    MESSAGE_MAX_COUNT,
    MESSAGE_MIN_COUNT,
    generate_conversations,
    generate_messages,
    summarize_activity,
    validate_conversations,
    validate_messages,
)
from src.data_generator.reference_data import generate_devices
from src.data_generator.user_data import generate_user_updates, generate_users
from src.data_generator.utils import initialize_randomness


class ActivityDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.partition_date = date(2026, 8, 20)
        fake = initialize_randomness(42)
        cls.users = generate_users(fake, cls.partition_date)
        cls.user_updates = generate_user_updates(
            cls.users,
            fake,
            cls.partition_date,
        )
        cls.devices = generate_devices()
        initialize_randomness(42)
        cls.conversations = generate_conversations(
            cls.users,
            cls.user_updates,
            cls.partition_date,
        )
        cls.messages = generate_messages(
            cls.users,
            cls.user_updates,
            cls.conversations,
            cls.devices,
            cls.partition_date,
        )

    def test_expected_count_ranges(self):
        self.assertTrue(
            CONVERSATION_MIN_COUNT
            <= len(self.conversations)
            <= CONVERSATION_MAX_COUNT
        )
        self.assertTrue(
            MESSAGE_MIN_COUNT <= len(self.messages) <= MESSAGE_MAX_COUNT
        )

    def test_conversations_and_messages_pass_validation(self):
        validate_conversations(
            self.users,
            self.user_updates,
            self.conversations,
            self.partition_date,
        )
        validate_messages(
            self.users,
            self.user_updates,
            self.conversations,
            self.messages,
            self.devices,
            self.partition_date,
        )

    def test_activity_is_long_tailed(self):
        counts = Counter(
            conversation["user_id"] for conversation in self.conversations
        )
        self.assertGreater(len(self.users) - len(counts), 1_500)
        self.assertGreater(max(counts.values()), 30)
        self.assertGreater(len(set(counts.values())), 10)

    def test_message_sequences_and_timestamps(self):
        messages_by_conversation = {}
        for message in self.messages:
            messages_by_conversation.setdefault(
                message["conversation_id"], []
            ).append(message)
        for messages in messages_by_conversation.values():
            self.assertEqual(
                [message["sequence_number"] for message in messages],
                list(range(1, len(messages) + 1)),
            )
            self.assertTrue(
                all(
                    current["created_at"] < following["created_at"]
                    for current, following in zip(messages, messages[1:])
                )
            )

    def test_multi_day_and_device_missingness(self):
        summary = summarize_activity(
            self.users,
            self.conversations,
            self.messages,
            self.devices,
        )
        self.assertGreater(summary["multi_day_conversations"], 0)
        null_rate = (
            summary["platform_distribution"]["NULL"] / len(self.messages)
        )
        self.assertGreaterEqual(null_rate, 0.005)
        self.assertLessEqual(null_rate, 0.01)

    def test_messages_reference_conversation_owner(self):
        owners = {
            conversation["conversation_id"]: conversation["user_id"]
            for conversation in self.conversations
        }
        self.assertTrue(
            all(
                message["user_id"] == owners[message["conversation_id"]]
                for message in self.messages
            )
        )


if __name__ == "__main__":
    unittest.main()
