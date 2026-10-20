import unittest
from datetime import date, datetime, timezone

from databricks.ingestion.state_updater import (
    COUNT_ENTITIES,
    SOURCE_SCHEMA,
    _latest_subscriptions,
    calculate_clean_counts,
    collapse_user_updates,
    validate_date_sequence,
    validate_update_fields,
    window_start,
)


class StateUpdaterPureLogicTests(unittest.TestCase):
    def test_default_schema_and_count_entities(self):
        self.assertEqual(SOURCE_SCHEMA, "ai_analytics.source")
        self.assertEqual(
            COUNT_ENTITIES,
            ("user_updates", "conversations", "messages", "errors"),
        )

    def test_30_day_window_is_inclusive(self):
        self.assertEqual(window_start(date(2026, 8, 23)), date(2026, 7, 25))

    def test_clean_counts_use_only_supported_count_entities(self):
        records = {
            "user_updates": [{}, {}],
            "conversations": [{}],
            "messages": [{}, {}, {}],
            "errors": [],
            "subscriptions": [{}, {}, {}, {}],
        }
        self.assertEqual(
            calculate_clean_counts(records),
            {
                "user_updates": 2,
                "conversations": 1,
                "messages": 3,
                "errors": 0,
            },
        )

    def test_user_update_field_validation(self):
        validate_update_fields([{"field_name": "email"}])
        with self.assertRaisesRegex(ValueError, "unsupported field_name"):
            validate_update_fields([{"field_name": "signup_at"}])

    def test_date_sequence_accepts_next_day(self):
        latest = {entity: date(2026, 8, 22) for entity in COUNT_ENTITIES}
        self.assertEqual(
            validate_date_sequence(latest, date(2026, 8, 23)),
            {"previous_date": date(2026, 8, 22), "mode": "new_day"},
        )

    def test_date_sequence_accepts_same_day_retry(self):
        latest = {entity: date(2026, 8, 23) for entity in COUNT_ENTITIES}
        self.assertEqual(
            validate_date_sequence(latest, date(2026, 8, 23))["mode"],
            "retry",
        )

    def test_date_sequence_rejects_gap_and_stale_dates(self):
        latest = {entity: date(2026, 8, 22) for entity in COUNT_ENTITIES}
        with self.assertRaisesRegex(ValueError, "gap"):
            validate_date_sequence(latest, date(2026, 8, 24))
        with self.assertRaisesRegex(ValueError, "stale"):
            validate_date_sequence(latest, date(2026, 8, 21))

    def test_date_sequence_rejects_inconsistent_entity_dates(self):
        latest = {entity: date(2026, 8, 22) for entity in COUNT_ENTITIES}
        latest["errors"] = date(2026, 8, 21)
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            validate_date_sequence(latest, date(2026, 8, 23))

    def test_user_updates_are_ordered_by_timestamp_then_update_id(self):
        timestamp = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
        base = {
            "user_id": "user-1",
            "old_value": "old",
            "updated_at": timestamp,
            "ingested_at": timestamp,
        }
        updates = [
            {
                **base,
                "update_id": "b",
                "field_name": "email",
                "new_value": "second@example.com",
            },
            {
                **base,
                "update_id": "a",
                "field_name": "email",
                "new_value": "first@example.com",
            },
            {
                **base,
                "update_id": "c",
                "field_name": "country_code",
                "new_value": "CA",
            },
        ]
        patch = collapse_user_updates(updates)[0]
        self.assertEqual(patch["email"], "second@example.com")
        self.assertEqual(patch["country_code"], "CA")
        self.assertEqual(patch["changed_fields"], ["email", "country_code"])

    def test_subscription_collapse_keeps_newest_lifecycle_record(self):
        old = {
            "subscription_id": "sub-1",
            "updated_at": datetime(2026, 8, 22, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 8, 22, tzinfo=timezone.utc),
            "subscription_status": "ACTIVE",
        }
        new = {
            **old,
            "updated_at": datetime(2026, 8, 23, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 8, 23, tzinfo=timezone.utc),
            "subscription_status": "CANCELLED",
        }
        self.assertEqual(_latest_subscriptions([new, old]), [new])

    def test_empty_subscriptions_are_supported(self):
        self.assertEqual(_latest_subscriptions([]), [])


if __name__ == "__main__":
    unittest.main()
