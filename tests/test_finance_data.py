import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

from src.data_generator.finance_data import (
    PAYMENT_METHODS,
    PLAN_VERSION_CUTOVER,
    add_month,
    generate_purchases_and_payments,
    generate_subscriptions,
    validate_finance_records,
)
from src.data_generator.ingestion import iter_local_partitioned_records
from src.data_generator.reference_data import generate_subscription_plans
from src.data_generator.utils import initialize_randomness
from src.data_generator.writers import PartitionedNDJSONWriter


class FinanceDataTests(unittest.TestCase):
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
        signup_base = datetime(2025, 8, 21, tzinfo=timezone.utc)
        cls.users = [
            {
                "user_id": f"user-{index:04d}",
                "signup_at": signup_base + timedelta(days=index % 240),
                "account_status": "ACTIVE",
                "country_code": ("US", "CA", "GB", "IN")[index % 4],
            }
            for index in range(1_200)
        ]
        cls.closed_user_id = cls.users[0]["user_id"]
        cls.closed_at = cls.users[0]["signup_at"] + timedelta(days=120)
        cls.updates = [
            {
                "user_id": cls.closed_user_id,
                "field_name": "account_status",
                "new_value": "CLOSED",
                "updated_at": cls.closed_at,
            }
        ]
        cls.plans = generate_subscription_plans()
        cls.plans_by_id = {plan["plan_id"]: plan for plan in cls.plans}
        conversation_counts = Counter(
            {
                user["user_id"]: (index * 17) % 55
                for index, user in enumerate(cls.users)
            }
        )
        cls.subscriptions = generate_subscriptions(
            cls.users,
            cls.updates,
            cls.plans,
            conversation_counts,
            cls.generation_end,
        )
        cls.purchases, cls.payments = generate_purchases_and_payments(
            cls.users,
            cls.updates,
            cls.plans,
            cls.subscriptions,
            cls.generation_end,
        )

    @staticmethod
    def _dated(records):
        return [(record, record["ingested_at"].date()) for record in records]

    def test_subscription_lineage_periods_prices_versions_and_boundaries(self):
        users_by_id = {user["user_id"]: user for user in self.users}
        by_user = defaultdict(list)
        for subscription in self.subscriptions:
            user = users_by_id[subscription["user_id"]]
            plan = self.plans_by_id[subscription["plan_id"]]
            self.assertGreaterEqual(subscription["started_at"], user["signup_at"])
            if subscription["ended_at"] is not None:
                self.assertGreaterEqual(
                    subscription["ended_at"], subscription["started_at"]
                )
            if plan["plan_name"] == "FREE":
                self.assertEqual(subscription["actual_monthly_price"], 0)
            else:
                self.assertGreater(subscription["actual_monthly_price"], 0)
                expected_version = (
                    "v1"
                    if subscription["started_at"] < PLAN_VERSION_CUTOVER
                    else "v2"
                )
                self.assertTrue(subscription["plan_id"].endswith(expected_version))
            by_user[subscription["user_id"]].append(subscription)

        self.assertEqual(set(by_user), set(users_by_id))
        for user_id, periods in by_user.items():
            ordered = sorted(periods, key=lambda row: row["started_at"])
            self.assertEqual(
                self.plans_by_id[ordered[0]["plan_id"]]["plan_name"], "FREE"
            )
            self.assertLessEqual(
                sum(row["subscription_status"] == "ACTIVE" for row in ordered),
                1,
            )
            for previous, current in zip(ordered, ordered[1:]):
                self.assertIsNotNone(previous["ended_at"])
                self.assertLessEqual(previous["ended_at"], current["started_at"])
            if user_id == self.closed_user_id:
                self.assertTrue(
                    all(row["updated_at"] <= self.closed_at for row in ordered)
                )

    def test_purchase_and_payment_causality_money_retries_and_refunds(self):
        subscriptions_by_id = {
            row["subscription_id"]: row for row in self.subscriptions
        }
        purchases_by_id = {
            row["purchase_id"]: row for row in self.purchases
        }
        purchases_by_subscription = defaultdict(list)
        for purchase in self.purchases:
            subscription = subscriptions_by_id[purchase["subscription_id"]]
            plan = self.plans_by_id[purchase["plan_id"]]
            self.assertEqual(purchase["user_id"], subscription["user_id"])
            self.assertEqual(purchase["plan_id"], subscription["plan_id"])
            self.assertNotEqual(plan["plan_name"], "FREE")
            self.assertAlmostEqual(
                purchase["total_amount"],
                purchase["subtotal_amount"]
                - purchase["discount_amount"]
                + purchase["tax_amount"],
                places=2,
            )
            purchases_by_subscription[purchase["subscription_id"]].append(purchase)
        for purchases in purchases_by_subscription.values():
            ordered = sorted(purchases, key=lambda row: row["purchase_created_at"])
            for previous, current in zip(ordered, ordered[1:]):
                self.assertEqual(
                    current["purchase_created_at"],
                    add_month(previous["purchase_created_at"]),
                )

        attempts = defaultdict(list)
        for payment in self.payments:
            purchase = purchases_by_id[payment["purchase_id"]]
            self.assertEqual(payment["user_id"], purchase["user_id"])
            self.assertIn(payment["payment_method"], PAYMENT_METHODS)
            self.assertEqual(payment["payment_amount"], purchase["total_amount"])
            attempts[payment["purchase_id"]].append(payment)
        self.assertEqual(set(attempts), set(purchases_by_id))
        self.assertTrue(any(len(rows) > 1 for rows in attempts.values()))
        self.assertTrue(
            any(
                row["payment_status"] == "REFUNDED"
                for row in self.payments
            )
        )
        for purchase_id, rows in attempts.items():
            ordered = sorted(rows, key=lambda row: row["processed_at"])
            self.assertEqual(ordered, rows)
            self.assertTrue(
                all(
                    first["processed_at"] < second["processed_at"]
                    for first, second in zip(ordered, ordered[1:])
                )
            )
            statuses = {row["payment_status"] for row in ordered}
            expected = (
                "REFUNDED"
                if "REFUNDED" in statuses
                else "COMPLETED"
                if "SUCCESS" in statuses
                else "FAILED"
            )
            self.assertEqual(purchases_by_id[purchase_id]["purchase_status"], expected)

    def test_serialized_reload_ingestion_late_arrivals_and_full_validation(self):
        with tempfile.TemporaryDirectory() as output_root:
            for entity_name, records in (
                ("subscriptions", self.subscriptions),
                ("purchases", self.purchases),
                ("payments", self.payments),
            ):
                with PartitionedNDJSONWriter(output_root, entity_name) as writer:
                    for record in records:
                        writer.write(record, record["ingested_at"].date())
            reloaded = {
                entity_name: list(
                    iter_local_partitioned_records(entity_name, output_root)
                )
                for entity_name in ("subscriptions", "purchases", "payments")
            }
            self.assertTrue(
                all(
                    physical_date == record["ingested_at"].date()
                    for rows in reloaded.values()
                    for record, physical_date in rows
                )
            )
            self.assertTrue(
                any(
                    (record["ingested_at"] - record["updated_at"]).total_seconds()
                    > 86_400
                    for rows in reloaded.values()
                    for record, _ in rows
                )
            )
            summary = validate_finance_records(
                self.users,
                self.updates,
                self.plans,
                iter(reloaded["subscriptions"]),
                iter(reloaded["purchases"]),
                iter(reloaded["payments"]),
                self.generation_end,
                production_scale=False,
            )
            self.assertEqual(summary["subscriptions"], len(self.subscriptions))
            self.assertEqual(summary["purchases"], len(self.purchases))
            self.assertEqual(summary["payments"], len(self.payments))
            self.assertEqual(summary["duplicate_fk_lineage"], "PASS")
            self.assertEqual(summary["monetary_reconciliation"], "PASS")

    def test_duplicate_subscription_id_is_rejected(self):
        duplicate = dict(self.subscriptions[0])
        dated_subscriptions = self._dated(self.subscriptions) + [
            (duplicate, duplicate["ingested_at"].date())
        ]
        with self.assertRaisesRegex(ValueError, "Duplicate subscription_id"):
            validate_finance_records(
                self.users,
                self.updates,
                self.plans,
                iter(dated_subscriptions),
                iter(self._dated(self.purchases)),
                iter(self._dated(self.payments)),
                self.generation_end,
                production_scale=False,
            )


if __name__ == "__main__":
    unittest.main()
