import unittest
from collections import Counter
from datetime import date

from src.data_generator.user_data import (
    ACCOUNT_STATUSES,
    USER_COUNT,
    USER_FIELDS,
    USER_UPDATE_COUNT,
    USER_UPDATE_FIELDS,
    VALID_ACCOUNT_TRANSITIONS,
    generate_user_updates,
    generate_users,
    validate_user_updates,
    validate_users,
)
from src.data_generator.utils import initialize_randomness


class UserDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.partition_date = date(2026, 8, 20)
        fake = initialize_randomness(42)
        cls.users = generate_users(fake, cls.partition_date)
        cls.updates = generate_user_updates(
            cls.users,
            fake,
            cls.partition_date,
        )

    def test_expected_counts_and_schemas(self):
        self.assertEqual(len(self.users), USER_COUNT)
        self.assertEqual(len(self.updates), USER_UPDATE_COUNT)
        self.assertTrue(all(set(user) == USER_FIELDS for user in self.users))
        self.assertTrue(
            all(set(update) == USER_UPDATE_FIELDS for update in self.updates)
        )

    def test_users_and_updates_pass_validation(self):
        validate_users(self.users, self.partition_date)
        validate_user_updates(self.users, self.updates)

    def test_initial_user_emails_and_ids_are_unique(self):
        self.assertEqual(
            len({user["user_id"] for user in self.users}),
            USER_COUNT,
        )
        self.assertEqual(
            len({user["email"] for user in self.users}),
            USER_COUNT,
        )

    def test_update_counts_are_long_tailed(self):
        counts = Counter(update["user_id"] for update in self.updates)
        self.assertEqual(len(self.users) - len(counts), 8_400)
        self.assertEqual(sum(count == 1 for count in counts.values()), 1_450)
        self.assertEqual(sum(count >= 2 for count in counts.values()), 150)

    def test_account_status_transitions_are_valid(self):
        for update in self.updates:
            if update["field_name"] == "account_status":
                self.assertIn(
                    (update["old_value"], update["new_value"]),
                    VALID_ACCOUNT_TRANSITIONS,
                )

    def test_temporal_and_state_chain_integrity(self):
        users_by_id = {user["user_id"]: user for user in self.users}
        updates_by_user = {}
        for update in self.updates:
            updates_by_user.setdefault(update["user_id"], []).append(update)

        for user_id, updates in updates_by_user.items():
            user = users_by_id[user_id]
            state = {
                "country_code": user["country_code"],
                "account_status": user["account_status"],
                "email": user["email"],
                "name": user["name"],
            }
            previous_timestamp = user["signup_at"]
            for update in updates:
                self.assertGreater(update["updated_at"], previous_timestamp)
                self.assertEqual(update["old_value"], state[update["field_name"]])
                self.assertNotEqual(update["old_value"], update["new_value"])
                state[update["field_name"]] = update["new_value"]
                previous_timestamp = update["updated_at"]

    def test_weighted_distributions_are_reasonable(self):
        countries = Counter(user["country_code"] for user in self.users)
        self.assertGreater(countries["US"], 5_000)
        statuses = Counter(user["account_status"] for user in self.users)
        self.assertGreater(statuses["ACTIVE"], 9_100)
        self.assertEqual(set(statuses), set(ACCOUNT_STATUSES))

    def test_seed_reproduces_users_and_updates(self):
        fake = initialize_randomness(42)
        users = generate_users(fake, self.partition_date)
        updates = generate_user_updates(users, fake, self.partition_date)
        self.assertEqual(users, self.users)
        self.assertEqual(updates, self.updates)

    def test_validation_rejects_broken_state_chain(self):
        broken_updates = list(self.updates)
        broken_updates[0] = dict(broken_updates[0])
        broken_updates[0]["old_value"] = "not-the-prior-value"
        with self.assertRaisesRegex(ValueError, "old_value"):
            validate_user_updates(self.users, broken_updates)


if __name__ == "__main__":
    unittest.main()
