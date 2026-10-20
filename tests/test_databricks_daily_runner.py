import unittest
from collections import Counter
from datetime import date
from unittest.mock import MagicMock, call, patch

from databricks.ingestion import delta_state
from databricks.jobs import run_daily_generator as runner


SIMULATION_DATE = date(2026, 8, 23)


def _state(latest_date=date(2026, 8, 22)):
    return {
        "counts": {
            entity: Counter({latest_date: 1})
            for entity in runner.state_updater.COUNT_ENTITIES
        }
    }


def _clean_records():
    records = {
        entity: [] for entity in runner.daily_generation.DAILY_ENTITIES
    }
    records["user_updates"] = [{"id": "update-1"}]
    records["conversations"] = [{"id": "conversation-1"}]
    records["messages"] = [{"id": "message-1"}, {"id": "message-2"}]
    records["errors"] = [{"id": "error-1"}]
    records["_metadata"] = {}
    return records


def _bronze_records():
    records = {
        entity: [] for entity in runner.bronze_writer.SUPPORTED_DAILY_ENTITIES
    }
    records["messages"] = [
        {"id": "message-1"},
        {"id": "message-1-replay"},
        {"id": "message-2"},
    ]
    return records


class DailyRunnerTests(unittest.TestCase):
    def setUp(self):
        self.spark = object()
        self.state = _state()
        self.clean = _clean_records()
        self.bronze = _bronze_records()
        self.manifest = [
            {
                "issue_type": "DUPLICATE_RECORD",
                "violation_type": "REPLAY_SAME_DAY",
            },
            {
                "issue_type": "MISSING_FIELD",
                "violation_type": "NULL_REQUIRED_FIELD",
            },
            {
                "issue_type": "DUPLICATE_RECORD",
                "violation_type": "REPLAY_SAME_DAY",
            },
        ]

    def _patch_pipeline(self):
        return (
            patch.object(
                runner.delta_state,
                "load_generator_state",
                return_value=self.state,
            ),
            patch.object(
                runner.daily_generation,
                "generate_daily_records",
                return_value=self.clean,
            ),
            patch.object(
                runner.daily_generation,
                "validate_daily_records",
                return_value={"clean": "PASS"},
            ),
            patch.object(
                runner.daily_generation,
                "_bronze_records",
                return_value=(self.bronze, self.manifest),
            ),
            patch.object(
                runner.bronze_writer,
                "validate_bronze_day",
                return_value=[{"status": "VALIDATED"}],
            ),
            patch.object(
                runner.state_updater,
                "validate_state_update",
                return_value={"state": "VALIDATED"},
            ),
            patch.object(
                runner.bronze_writer,
                "append_bronze_day",
                return_value=[{"status": "SUBMITTED"}],
            ),
            patch.object(
                runner.state_updater,
                "update_generator_state",
                return_value={"completed": True},
            ),
            patch.object(
                runner,
                "_post_write_validation",
                return_value={"status": "PASS"},
            ),
        )

    def test_dry_run_generates_and_validates_without_writes(self):
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])

        result = runner.run_daily_generator(
            self.spark,
            simulation_date=SIMULATION_DATE,
        )

        self.assertEqual(result["mode"], "DRY_RUN")
        self.assertTrue(result["completed"])
        self.assertFalse(result["bronze_write_attempted"])
        self.assertFalse(result["state_update_attempted"])
        mocks[1].assert_called_once()
        mocks[2].assert_called_once()
        mocks[4].assert_called_once()
        mocks[5].assert_called_once()
        mocks[6].assert_not_called()
        mocks[7].assert_not_called()
        mocks[8].assert_not_called()

    def test_write_mode_appends_bronze_before_state_update(self):
        order = []
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])
        mocks[6].side_effect = lambda *args, **kwargs: order.append("bronze") or []
        mocks[7].side_effect = lambda *args, **kwargs: order.append("state") or {}

        result = runner.run_daily_generator(
            self.spark,
            simulation_date=SIMULATION_DATE,
            dry_run=False,
        )

        self.assertEqual(order, ["bronze", "state"])
        self.assertEqual(result["mode"], "WRITE")
        self.assertTrue(result["completed"])
        mocks[8].assert_called_once()

    def test_bronze_write_failure_prevents_state_update(self):
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])
        mocks[6].side_effect = RuntimeError("Bronze failed")

        with self.assertRaisesRegex(RuntimeError, "Bronze failed"):
            runner.run_daily_generator(
                self.spark,
                simulation_date=SIMULATION_DATE,
                dry_run=False,
            )
        mocks[7].assert_not_called()
        mocks[8].assert_not_called()

    def test_bronze_validation_failure_prevents_all_mutations(self):
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])
        mocks[4].side_effect = ValueError("invalid Bronze")

        with self.assertRaisesRegex(ValueError, "invalid Bronze"):
            runner.run_daily_generator(
                self.spark,
                simulation_date=SIMULATION_DATE,
                dry_run=False,
            )
        mocks[6].assert_not_called()
        mocks[7].assert_not_called()

    def test_state_validation_failure_prevents_all_mutations(self):
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])
        mocks[5].side_effect = ValueError("invalid state")

        with self.assertRaisesRegex(ValueError, "invalid state"):
            runner.run_daily_generator(
                self.spark,
                simulation_date=SIMULATION_DATE,
                dry_run=False,
            )
        mocks[6].assert_not_called()
        mocks[7].assert_not_called()

    def test_clean_validation_failure_prevents_all_mutations(self):
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])
        mocks[2].side_effect = ValueError("invalid clean records")

        with self.assertRaisesRegex(ValueError, "invalid clean records"):
            runner.run_daily_generator(
                self.spark,
                simulation_date=SIMULATION_DATE,
                dry_run=False,
            )
        mocks[3].assert_not_called()
        mocks[4].assert_not_called()
        mocks[5].assert_not_called()
        mocks[6].assert_not_called()
        mocks[7].assert_not_called()

    def test_automatic_simulation_date_is_latest_plus_one_day(self):
        patchers = self._patch_pipeline()
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])

        result = runner.run_daily_generator(self.spark)

        self.assertEqual(result["simulation_date"], SIMULATION_DATE)
        self.assertEqual(mocks[1].call_args.args[1], SIMULATION_DATE)

    def test_inconsistent_daily_count_dates_are_rejected(self):
        state = _state()
        state["counts"]["errors"] = Counter({date(2026, 8, 21): 1})
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            runner.infer_simulation_date(state)

    def test_clean_and_physical_bronze_counts_remain_distinct(self):
        patchers = self._patch_pipeline()
        [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])

        result = runner.run_daily_generator(
            self.spark,
            simulation_date=SIMULATION_DATE,
        )

        self.assertEqual(result["clean_counts"]["messages"], 2)
        self.assertEqual(result["bronze_physical_counts"]["messages"], 3)

    def test_manifest_summary_is_deterministic_and_aggregated(self):
        summary = runner.summarize_injection_manifest(list(reversed(self.manifest)))
        self.assertEqual(summary["issue_count"], 3)
        self.assertEqual(
            summary["issue_counts_by_type"],
            {"DUPLICATE_RECORD": 2, "MISSING_FIELD": 1},
        )
        self.assertEqual(
            summary["violation_counts_by_type"],
            {"NULL_REQUIRED_FIELD": 1, "REPLAY_SAME_DAY": 2},
        )


class DeltaStateSchemaTests(unittest.TestCase):
    @patch.object(delta_state, "_rows_to_dicts", return_value=[])
    def test_custom_source_schema_is_used_for_every_table(self, _rows):
        spark = MagicMock()

        delta_state.load_generator_state(
            spark,
            source_schema="ai_analytics.state_updater_test",
        )

        expected_suffixes = (
            "generator_users",
            "generator_user_state",
            "generator_models",
            "generator_devices",
            "generator_subscription_plans",
            "generator_recent_conversations",
            "generator_recent_messages",
            "generator_subscriptions",
            "generator_daily_counts",
        )
        self.assertEqual(
            spark.table.call_args_list,
            [
                call(f"ai_analytics.state_updater_test.{suffix}")
                for suffix in expected_suffixes
            ],
        )
        spark.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")

    def test_invalid_source_schema_is_rejected_before_spark_access(self):
        spark = MagicMock()
        with self.assertRaisesRegex(ValueError, "invalid source_schema"):
            delta_state.load_generator_state(
                spark,
                source_schema="ai_analytics.source; DROP TABLE users",
            )
        spark.table.assert_not_called()


if __name__ == "__main__":
    unittest.main()
