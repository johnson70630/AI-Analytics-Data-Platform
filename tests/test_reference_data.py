import unittest
from copy import deepcopy

from src.data_generator.reference_data import (
    generate_devices,
    generate_models,
    generate_subscription_plans,
    validate_devices,
    validate_models,
    validate_subscription_plans,
)
from src.data_generator.utils import initialize_randomness


class ReferenceDataTests(unittest.TestCase):
    def test_catalog_counts_and_stable_ids(self):
        self.assertEqual(
            [model["model_id"] for model in generate_models()],
            [f"model_{number:03d}" for number in range(1, 9)],
        )
        self.assertEqual(
            [device["device_id"] for device in generate_devices()],
            [f"device_{number:03d}" for number in range(1, 13)],
        )
        self.assertEqual(
            [plan["plan_id"] for plan in generate_subscription_plans()],
            [
                "plan_free_v1",
                "plan_plus_v1",
                "plan_plus_v2",
                "plan_pro_v1",
                "plan_pro_v2",
            ],
        )

    def test_reference_data_is_independent_of_seed(self):
        initialize_randomness(1)
        first = (
            generate_models(),
            generate_devices(),
            generate_subscription_plans(),
        )
        initialize_randomness(999)
        second = (
            generate_models(),
            generate_devices(),
            generate_subscription_plans(),
        )
        self.assertEqual(first, second)

    def test_model_validation_rejects_duplicate_ids(self):
        models = generate_models()
        models[1]["model_id"] = models[0]["model_id"]
        with self.assertRaisesRegex(ValueError, "duplicate.*model_id"):
            validate_models(models)

    def test_model_validation_rejects_unordered_release_dates(self):
        models = generate_models()
        models[0]["release_date"], models[1]["release_date"] = (
            models[1]["release_date"],
            models[0]["release_date"],
        )
        with self.assertRaisesRegex(ValueError, "release dates"):
            validate_models(models)

    def test_device_validation_rejects_inconsistent_combination(self):
        devices = deepcopy(generate_devices())
        devices[3]["operating_system"] = "Android"
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            validate_devices(devices)

    def test_plan_validation_rejects_negative_price(self):
        plans = generate_subscription_plans()
        plans[0]["monthly_price"] = -1
        with self.assertRaisesRegex(ValueError, "monthly_price"):
            validate_subscription_plans(plans)

    def test_plan_validation_requires_usd(self):
        plans = generate_subscription_plans()
        plans[0]["currency"] = "EUR"
        with self.assertRaisesRegex(ValueError, "currency must be USD"):
            validate_subscription_plans(plans)


if __name__ == "__main__":
    unittest.main()
