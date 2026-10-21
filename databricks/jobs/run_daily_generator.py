"""Orchestrate one deterministic daily generator run against managed Delta."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

def _parse_bundle_root(argv: list[str]) -> Path:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--bundle-root", required=True)
    arguments, _ = parser.parse_known_args(argv)
    return Path(arguments.bundle_root)


# A serverless Python-script task can execute through a wrapper that does not
# define __file__. Use the bundle root supplied by the job resource instead.
if globals().get("__package__") in (None, ""):
    _BUNDLE_ROOT = _parse_bundle_root(sys.argv[1:])
    for _import_path in (_BUNDLE_ROOT, _BUNDLE_ROOT / "databricks"):
        if str(_import_path) not in sys.path:
            sys.path.insert(0, str(_import_path))
    from ingestion import bronze_writer, delta_state, state_updater
else:
    from databricks.ingestion import bronze_writer, delta_state, state_updater

from src.data_generator import daily_generation


DEFAULT_SEED = 42


def _latest_generator_dates(state: Mapping[str, Any]) -> dict[str, date]:
    counts = state.get("counts")
    if not isinstance(counts, Mapping):
        raise ValueError("generator state has no daily counts mapping")

    latest = {}
    for entity in state_updater.COUNT_ENTITIES:
        entity_counts = counts.get(entity)
        if not entity_counts:
            raise ValueError(f"generator state has no daily counts for {entity}")
        latest[entity] = max(entity_counts)
    return latest


def current_generator_date(state: Mapping[str, Any]) -> date:
    """Return the consistent completion-marker date across all count streams."""
    latest = _latest_generator_dates(state)
    current_dates = set(latest.values())
    if len(current_dates) != 1:
        raise ValueError(
            "generator daily-count latest dates are inconsistent: "
            f"{dict(sorted(latest.items()))}"
        )
    return next(iter(current_dates))


def infer_simulation_date(state: Mapping[str, Any]) -> date:
    """Infer the next date only when all generator count streams agree."""
    return current_generator_date(state) + timedelta(days=1)


def _resolve_simulation_date(
    state: Mapping[str, Any], requested_date: date | None
) -> tuple[date, str, date]:
    current_date = current_generator_date(state)
    if requested_date is None:
        return current_date + timedelta(days=1), "RUN", current_date
    if not isinstance(requested_date, date) or isinstance(requested_date, datetime):
        raise TypeError("simulation_date must be a datetime.date or None")
    if requested_date == current_date:
        return requested_date, "ALREADY_COMPLETED", current_date
    state_updater.validate_date_sequence(
        _latest_generator_dates(state), requested_date
    )
    return requested_date, "RUN", current_date


def _already_completed_summary(
    simulation_date: date,
    current_date: date,
    seed: int,
    source_schema: str,
    bronze_schema: str,
) -> dict[str, Any]:
    return {
        "simulation_date": simulation_date,
        "seed": seed,
        "source_schema": source_schema,
        "bronze_schema": bronze_schema,
        "mode": "ALREADY_COMPLETED",
        "current_generator_date": current_date,
        "reason": "generator state already completed through requested date",
        "clean_counts": None,
        "bronze_physical_counts": None,
        "injection_issue_count": None,
        "injection_issue_counts_by_type": None,
        "injection_violation_counts_by_type": None,
        "clean_validation": None,
        "bronze_validation": None,
        "state_validation": None,
        "bronze_write_attempted": False,
        "bronze_write_summary": None,
        "state_update_attempted": False,
        "state_update_summary": None,
        "post_write_validation": None,
        "completed": True,
    }


def _record_counts(
    records: Mapping[str, Any], entities: tuple[str, ...]
) -> dict[str, int]:
    return {entity: len(records.get(entity, ())) for entity in entities}


def summarize_injection_manifest(manifest: list[dict[str, Any]]) -> dict[str, Any]:
    """Return deterministic aggregate counts without exposing manifest rows."""
    issue_types = Counter(row["issue_type"] for row in manifest)
    violation_types = Counter(
        row["violation_type"]
        for row in manifest
        if row.get("violation_type") is not None
    )
    return {
        "issue_count": len(manifest),
        "issue_counts_by_type": dict(sorted(issue_types.items())),
        "violation_counts_by_type": dict(sorted(violation_types.items())),
    }


def _post_write_validation(
    spark: Any,
    clean_records: Mapping[str, Any],
    bronze_records: Mapping[str, Any],
    simulation_date: date,
    source_schema: str,
    bronze_schema: str,
) -> dict[str, Any]:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError("PySpark is required for post-write validation") from exc

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    bronze_counts = {}
    for entity in bronze_writer.SUPPORTED_DAILY_ENTITIES:
        target = bronze_writer.target_table_name(entity, bronze_schema)
        actual = (
            spark.table(target)
            .filter(functions.col("dt") == functions.lit(simulation_date))
            .count()
        )
        expected = len(bronze_records.get(entity, ()))
        if actual != expected:
            raise RuntimeError(
                f"Bronze post-write count mismatch for {entity}: "
                f"expected={expected}, actual={actual}"
            )
        bronze_counts[entity] = actual

    count_table = f"{source_schema}.generator_daily_counts"
    rows = (
        spark.table(count_table)
        .filter(functions.col("entity").isin(*state_updater.COUNT_ENTITIES))
        .groupBy("entity")
        .agg(functions.max("activity_date").alias("latest_date"))
        .collect()
    )
    latest_dates = {row["entity"]: row["latest_date"] for row in rows}
    sequence = state_updater.validate_date_sequence(latest_dates, simulation_date)
    if sequence["mode"] != "retry":
        raise RuntimeError(
            "generator daily counts did not advance through "
            f"{simulation_date}: {latest_dates}"
        )

    expected_clean_counts = {
        entity: len(clean_records[entity])
        for entity in state_updater.COUNT_ENTITIES
    }
    daily_rows = (
        spark.table(count_table)
        .filter(
            (functions.col("activity_date") == functions.lit(simulation_date))
            & functions.col("entity").isin(*state_updater.COUNT_ENTITIES)
        )
        .select("entity", "row_count")
        .collect()
    )
    actual_clean_counts = {row["entity"]: row["row_count"] for row in daily_rows}
    if actual_clean_counts != expected_clean_counts:
        raise RuntimeError(
            "generator daily clean counts do not match generated records: "
            f"expected={expected_clean_counts}, actual={actual_clean_counts}"
        )

    return {
        "bronze_counts": bronze_counts,
        "generator_latest_dates": latest_dates,
        "generator_clean_counts": actual_clean_counts,
        "status": "PASS",
    }


def run_daily_generator(
    spark: Any,
    simulation_date: date | None = None,
    seed: int = DEFAULT_SEED,
    source_schema: str = delta_state.SOURCE_SCHEMA,
    bronze_schema: str = bronze_writer.TARGET_SCHEMA,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Generate, validate, and optionally persist one deterministic day."""
    state = delta_state.load_generator_state(
        spark,
        source_schema=source_schema,
    )
    resolved_date, disposition, current_date = _resolve_simulation_date(
        state, simulation_date
    )
    if disposition == "ALREADY_COMPLETED":
        return _already_completed_summary(
            resolved_date,
            current_date,
            seed,
            source_schema,
            bronze_schema,
        )

    clean_records = daily_generation.generate_daily_records(
        state,
        resolved_date,
        seed,
    )
    clean_validation = daily_generation.validate_daily_records(
        clean_records,
        state,
        resolved_date,
    )
    bronze_records, injection_manifest = daily_generation._bronze_records(
        clean_records,
        seed,
        resolved_date,
        state,
    )

    bronze_validation = bronze_writer.validate_bronze_day(
        spark,
        bronze_records,
        resolved_date,
        target_schema=bronze_schema,
    )
    state_validation = state_updater.validate_state_update(
        spark,
        clean_records,
        resolved_date,
        source_schema=source_schema,
    )

    clean_counts = _record_counts(clean_records, daily_generation.DAILY_ENTITIES)
    bronze_counts = _record_counts(
        bronze_records,
        bronze_writer.SUPPORTED_DAILY_ENTITIES,
    )
    manifest_summary = summarize_injection_manifest(injection_manifest)
    summary = {
        "simulation_date": resolved_date,
        "seed": seed,
        "source_schema": source_schema,
        "bronze_schema": bronze_schema,
        "mode": "DRY_RUN" if dry_run else "WRITE",
        "clean_counts": clean_counts,
        "bronze_physical_counts": bronze_counts,
        "injection_issue_count": manifest_summary["issue_count"],
        "injection_issue_counts_by_type": manifest_summary[
            "issue_counts_by_type"
        ],
        "injection_violation_counts_by_type": manifest_summary[
            "violation_counts_by_type"
        ],
        "clean_validation": clean_validation,
        "bronze_validation": bronze_validation,
        "state_validation": state_validation,
        "bronze_write_attempted": False,
        "bronze_write_summary": None,
        "state_update_attempted": False,
        "state_update_summary": None,
        "post_write_validation": None,
        "completed": False,
    }

    if dry_run:
        summary["completed"] = True
        return summary

    summary["bronze_write_attempted"] = True
    summary["bronze_write_summary"] = bronze_writer.append_bronze_day(
        spark,
        bronze_records,
        resolved_date,
        target_schema=bronze_schema,
    )

    summary["state_update_attempted"] = True
    summary["state_update_summary"] = state_updater.update_generator_state(
        spark,
        clean_records,
        resolved_date,
        source_schema=source_schema,
    )
    summary["post_write_validation"] = _post_write_validation(
        spark,
        clean_records,
        bronze_records,
        resolved_date,
        source_schema,
        bronze_schema,
    )
    summary["completed"] = True
    return summary


def _get_spark_session() -> Any:
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def main() -> dict[str, Any]:
    """Run the production next-day pipeline as a Databricks Python task."""
    summary = run_daily_generator(
        _get_spark_session(),
        simulation_date=None,
        seed=42,
        source_schema="ai_analytics.source",
        bronze_schema="ai_analytics.bronze",
        dry_run=False,
    )
    print(json.dumps(summary, default=str, sort_keys=True, separators=(",", ":")))
    return summary


if __name__ == "__main__":
    main()
