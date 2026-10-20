"""Idempotent updates for the managed Delta synthetic-generator state."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta
from typing import Any

from src.data_generator.writers import serialize_record


SOURCE_SCHEMA = "ai_analytics.source"

COUNT_ENTITIES = (
    "user_updates",
    "conversations",
    "messages",
    "errors",
)
MUTABLE_TABLES = (
    "generator_user_state",
    "generator_recent_conversations",
    "generator_recent_messages",
    "generator_subscriptions",
    "generator_daily_counts",
)
REQUIRED_CLEAN_ENTITIES = (*COUNT_ENTITIES, "subscriptions")
USER_STATE_FIELDS = ("email", "name", "country_code", "account_status")

EVENT_FIELDS = {
    "user_updates": {
        "update_id",
        "user_id",
        "field_name",
        "old_value",
        "new_value",
        "updated_at",
        "ingested_at",
    },
    "conversations": {
        "conversation_id",
        "user_id",
        "created_at",
        "ingested_at",
    },
    "messages": {
        "message_id",
        "conversation_id",
        "user_id",
        "device_id",
        "sequence_number",
        "message_text",
        "created_at",
        "ingested_at",
    },
    "subscriptions": {
        "subscription_id",
        "user_id",
        "plan_id",
        "started_at",
        "ended_at",
        "subscription_status",
        "actual_monthly_price",
        "updated_at",
        "ingested_at",
    },
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def window_start(simulation_date: date) -> date:
    """Return the inclusive first date in a 30-day state window."""
    _validate_date(simulation_date)
    return simulation_date - timedelta(days=29)


def calculate_clean_counts(
    clean_records: Mapping[str, Iterable[Any]],
) -> dict[str, int]:
    """Return clean daily counts for only the four distribution inputs."""
    return {
        entity: len(_materialize_records(clean_records, entity))
        for entity in COUNT_ENTITIES
    }


def validate_update_fields(updates: Iterable[Mapping[str, Any]]) -> None:
    """Reject user update events outside the generator's mutable field set."""
    for index, update in enumerate(updates):
        if not isinstance(update, Mapping):
            raise TypeError(f"user_updates record {index} must be a mapping")
        field = update.get("field_name")
        if field not in USER_STATE_FIELDS:
            raise ValueError(
                f"user_updates record {index} has unsupported field_name: {field!r}"
            )


def collapse_user_updates(
    updates: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse ordered change events into one deterministic patch per user."""
    rows = [dict(row) for row in updates]
    validate_update_fields(rows)
    for index, row in enumerate(rows):
        _require_fields("user_updates", index, row, EVENT_FIELDS["user_updates"])

    patches: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=_user_update_sort_key):
        user_id = row["user_id"]
        patch = patches.setdefault(
            user_id,
            {"user_id": user_id, "changed_fields": []},
        )
        field = row["field_name"]
        patch[field] = row["new_value"]
        if field not in patch["changed_fields"]:
            patch["changed_fields"].append(field)

    return [patches[user_id] for user_id in sorted(patches)]


def validate_date_sequence(
    latest_dates: Mapping[str, date], simulation_date: date
) -> dict[str, Any]:
    """Allow only the next day or an exact same-day retry."""
    _validate_date(simulation_date)
    missing = set(COUNT_ENTITIES).difference(latest_dates)
    unexpected = set(latest_dates).difference(COUNT_ENTITIES)
    if missing or unexpected:
        raise ValueError(
            "daily-count entities do not match the required set; "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )
    current_dates = set(latest_dates.values())
    if None in current_dates or len(current_dates) != 1:
        raise ValueError(
            "daily-count latest dates are inconsistent: "
            f"{dict(sorted(latest_dates.items()))}"
        )
    previous_date = next(iter(current_dates))
    if simulation_date == previous_date:
        mode = "retry"
    elif simulation_date == previous_date + timedelta(days=1):
        mode = "new_day"
    elif simulation_date < previous_date:
        raise ValueError(
            f"stale simulation date {simulation_date}; current date is {previous_date}"
        )
    else:
        raise ValueError(
            f"simulation date gap: expected {previous_date + timedelta(days=1)} "
            f"or retry {previous_date}, got {simulation_date}"
        )
    return {"previous_date": previous_date, "mode": mode}


def _validate_date(value: Any) -> None:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError("simulation_date must be a datetime.date")


def _materialize_records(
    clean_records: Mapping[str, Iterable[Any]], entity: str
) -> list[Any]:
    if entity not in clean_records:
        raise ValueError(f"clean records are missing required entity: {entity}")
    value = clean_records[entity]
    if isinstance(value, (str, bytes, Mapping)):
        raise TypeError(f"clean_records[{entity!r}] must be an iterable of records")
    return list(value)


def _materialize_clean_records(
    clean_records: Mapping[str, Iterable[Any]],
) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(clean_records, Mapping):
        raise TypeError("clean_records must be a mapping")
    materialized = {}
    for entity in REQUIRED_CLEAN_ENTITIES:
        rows = _materialize_records(clean_records, entity)
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise TypeError(f"{entity} record {index} must be a mapping")
        materialized[entity] = [dict(row) for row in rows]
    return materialized


def _require_fields(
    entity: str,
    index: int,
    row: Mapping[str, Any],
    expected: set[str],
) -> None:
    missing = expected.difference(row)
    unexpected = set(row).difference(expected)
    if missing or unexpected:
        raise ValueError(
            f"{entity} record {index} does not match the clean source contract; "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )


def _validate_clean_event_shapes(records: Mapping[str, list[dict[str, Any]]]) -> None:
    for entity, expected in EVENT_FIELDS.items():
        for index, row in enumerate(records[entity]):
            _require_fields(entity, index, row, expected)
    validate_update_fields(records["user_updates"])


def _ordered_value(value: Any) -> tuple[str, str]:
    if isinstance(value, datetime):
        return ("datetime", value.isoformat())
    return (type(value).__name__, str(value))


def _user_update_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        _ordered_value(row.get("updated_at")),
        str(row.get("update_id", "")),
        str(row.get("user_id", "")),
    )


def _latest_subscriptions(
    records: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Choose one latest lifecycle record per subscription deterministically."""
    latest: dict[str, dict[str, Any]] = {}
    latest_key: dict[str, tuple[Any, ...]] = {}
    for row in records:
        copied = dict(row)
        key = (
            _ordered_value(copied.get("updated_at")),
            _ordered_value(copied.get("ingested_at")),
            json.dumps(copied, default=str, sort_keys=True),
        )
        subscription_id = copied["subscription_id"]
        if subscription_id not in latest or key > latest_key[subscription_id]:
            latest[subscription_id] = copied
            latest_key[subscription_id] = key
    return [latest[key] for key in sorted(latest)]


def _validate_schema_name(source_schema: str) -> str:
    if not isinstance(source_schema, str) or not source_schema.strip():
        raise ValueError("source_schema must be a non-empty string")
    parts = source_schema.split(".")
    if len(parts) not in (1, 2) or not all(
        _IDENTIFIER.fullmatch(part) for part in parts
    ):
        raise ValueError(f"invalid source_schema: {source_schema!r}")
    return source_schema


def _table_name(source_schema: str, table: str) -> str:
    _validate_schema_name(source_schema)
    if table not in MUTABLE_TABLES and table != "generator_users":
        raise ValueError(f"unsupported generator state table: {table}")
    return f"{source_schema}.{table}"


def _target_struct_type(spark: Any, table_name: str) -> Any:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required generator state table does not exist: {table_name}"
        )
    return spark.table(table_name).schema


def _prepare_target_dataframe(
    spark: Any,
    table_name: str,
    records: Iterable[Mapping[str, Any]],
) -> Any:
    """Parse records into exactly the existing target table StructType."""
    target_schema = _target_struct_type(spark, table_name)
    target_columns = tuple(field.name for field in target_schema.fields)
    rows = []
    for index, record in enumerate(records):
        unexpected = set(record).difference(target_columns)
        missing = set(target_columns).difference(record)
        if missing or unexpected:
            raise ValueError(
                f"record {index} does not match {table_name}; "
                f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )
        rows.append({column: record[column] for column in target_columns})

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not rows:
        return spark.createDataFrame([], target_schema)

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError(
            "PySpark is required to update Delta generator state"
        ) from exc

    json_rows = [(serialize_record(row).rstrip("\n"),) for row in rows]
    raw = spark.createDataFrame(json_rows, ["_state_json"])
    return (
        raw.select(
            functions.from_json(
                functions.col("_state_json"),
                target_schema,
                {"mode": "FAILFAST"},
            ).alias("_record")
        )
        .select("_record.*")
        .select(*target_columns)
    )


def _latest_count_dates(spark: Any, source_schema: str) -> dict[str, date]:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError(
            "PySpark is required to inspect Delta generator state"
        ) from exc

    table = _table_name(source_schema, "generator_daily_counts")
    rows = (
        spark.table(table)
        .filter(functions.col("entity").isin(*COUNT_ENTITIES))
        .groupBy("entity")
        .agg(functions.max("activity_date").alias("latest_date"))
        .collect()
    )
    return {row["entity"]: row["latest_date"] for row in rows}


def _assert_unique_ids(
    records: Iterable[Mapping[str, Any]], entity: str, key: str
) -> None:
    seen = set()
    for index, row in enumerate(records):
        value = row.get(key)
        if value is None:
            raise ValueError(f"{entity} record {index} has null {key}")
        if value in seen:
            raise ValueError(f"{entity} clean batch contains duplicate {key}: {value}")
        seen.add(value)


def _prepare_user_state_changes(
    spark: Any,
    updates: list[dict[str, Any]],
    source_schema: str,
) -> Any | None:
    patches = collapse_user_updates(updates)
    if not patches:
        _target_struct_type(spark, _table_name(source_schema, "generator_user_state"))
        return None

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError(
            "PySpark is required to update Delta generator state"
        ) from exc

    target_table = _table_name(source_schema, "generator_user_state")
    target_schema = _target_struct_type(spark, target_table)
    target_columns = tuple(field.name for field in target_schema.fields)
    expected_columns = {"user_id", *USER_STATE_FIELDS}
    if set(target_columns) != expected_columns:
        raise ValueError(
            f"unexpected schema for {target_table}: {list(target_columns)}"
        )

    encoded = []
    for patch in patches:
        state_patch = {
            column: patch.get(column)
            for column in target_columns
        }
        encoded.append(
            (serialize_record(state_patch).rstrip("\n"), patch["changed_fields"])
        )
    raw = spark.createDataFrame(encoded, ["_state_json", "changed_fields"])
    parsed = raw.select(
        functions.from_json(
            functions.col("_state_json"),
            target_schema,
            {"mode": "FAILFAST"},
        ).alias("patch"),
        "changed_fields",
    ).select("patch.*", "changed_fields")

    users_table = _table_name(source_schema, "generator_users")
    _target_struct_type(spark, users_table)
    unknown = (
        parsed.select("user_id")
        .join(spark.table(users_table).select("user_id"), "user_id", "left_anti")
        .limit(1)
        .collect()
    )
    if unknown:
        raise ValueError(
            f"user update references unknown user_id: {unknown[0]['user_id']}"
        )

    current = spark.table(target_table).alias("current")
    patch = parsed.alias("patch")
    expressions = [functions.col("patch.user_id").alias("user_id")]
    for field in USER_STATE_FIELDS:
        expressions.append(
            functions.when(
                functions.array_contains(functions.col("patch.changed_fields"), field),
                functions.col(f"patch.{field}"),
            )
            .otherwise(functions.col(f"current.{field}"))
            .alias(field)
        )
    return patch.join(current, "user_id", "inner").select(*expressions)


def _build_plan(
    spark: Any,
    clean_records: Mapping[str, Iterable[Any]],
    simulation_date: date,
    source_schema: str,
) -> dict[str, Any]:
    _validate_date(simulation_date)
    _validate_schema_name(source_schema)
    records = _materialize_clean_records(clean_records)
    _validate_clean_event_shapes(records)
    _assert_unique_ids(records["conversations"], "conversations", "conversation_id")
    _assert_unique_ids(records["messages"], "messages", "message_id")

    latest_dates = _latest_count_dates(spark, source_schema)
    sequence = validate_date_sequence(latest_dates, simulation_date)
    counts = {entity: len(records[entity]) for entity in COUNT_ENTITIES}
    input_counts = {
        entity: len(records[entity]) for entity in REQUIRED_CLEAN_ENTITIES
    }

    user_state = _prepare_user_state_changes(
        spark, records["user_updates"], source_schema
    )
    conversations = _prepare_target_dataframe(
        spark,
        _table_name(source_schema, "generator_recent_conversations"),
        records["conversations"],
    )
    messages = _prepare_target_dataframe(
        spark,
        _table_name(source_schema, "generator_recent_messages"),
        records["messages"],
    )
    subscriptions = _prepare_target_dataframe(
        spark,
        _table_name(source_schema, "generator_subscriptions"),
        _latest_subscriptions(records["subscriptions"]),
    )
    count_rows = [
        {
            "entity": entity,
            "activity_date": simulation_date,
            "row_count": row_count,
        }
        for entity, row_count in counts.items()
    ]
    daily_counts = _prepare_target_dataframe(
        spark,
        _table_name(source_schema, "generator_daily_counts"),
        count_rows,
    )
    prepared_counts = {
        "user_state": 0 if user_state is None else user_state.count(),
        "conversations": conversations.count(),
        "messages": messages.count(),
        "subscriptions": subscriptions.count(),
        "daily_counts": daily_counts.count(),
    }
    expected_prepared_counts = {
        "user_state": len(collapse_user_updates(records["user_updates"])),
        "conversations": input_counts["conversations"],
        "messages": input_counts["messages"],
        "subscriptions": len(_latest_subscriptions(records["subscriptions"])),
        "daily_counts": len(COUNT_ENTITIES),
    }
    if prepared_counts != expected_prepared_counts:
        raise RuntimeError(
            "prepared state row counts do not match clean input: "
            f"expected={expected_prepared_counts}, actual={prepared_counts}"
        )
    return {
        "records": records,
        "counts": counts,
        "input_counts": input_counts,
        "prepared_counts": prepared_counts,
        "sequence": sequence,
        "user_state": user_state,
        "conversations": conversations,
        "messages": messages,
        "subscriptions": subscriptions,
        "daily_counts": daily_counts,
    }


def _validation_summary(
    plan: Mapping[str, Any], simulation_date: date, source_schema: str
) -> dict[str, Any]:
    return {
        "simulation_date": simulation_date,
        "previous_generator_date": plan["sequence"]["previous_date"],
        "mode": plan["sequence"]["mode"],
        "clean_input_counts": dict(plan["input_counts"]),
        "daily_count_values": dict(plan["counts"]),
        "prepared_row_counts": dict(plan["prepared_counts"]),
        "source_schema": source_schema,
        "window_start": window_start(simulation_date),
        "mutation_attempted": False,
        "completed": False,
    }


def validate_state_update(
    spark: Any,
    clean_records: Mapping[str, Iterable[Any]],
    simulation_date: date,
    source_schema: str = SOURCE_SCHEMA,
) -> dict[str, Any]:
    """Prepare and validate all state changes without mutating Delta tables."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    plan = _build_plan(spark, clean_records, simulation_date, source_schema)
    return _validation_summary(plan, simulation_date, source_schema)


def _delta_table(spark: Any, table_name: str) -> Any:
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError(
            "Delta Lake is required to update generator state"
        ) from exc
    return DeltaTable.forName(spark, table_name)


def _merge_all(spark: Any, table_name: str, source: Any, key: str) -> None:
    (
        _delta_table(spark, table_name)
        .alias("target")
        .merge(source.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_existing_only(spark: Any, table_name: str, source: Any, key: str) -> None:
    (
        _delta_table(spark, table_name)
        .alias("target")
        .merge(source.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdateAll()
        .execute()
    )


def _merge_subscriptions(spark: Any, table_name: str, source: Any) -> None:
    (
        _delta_table(spark, table_name)
        .alias("target")
        .merge(
            source.alias("source"),
            "target.subscription_id = source.subscription_id",
        )
        .whenMatchedUpdateAll(condition="source.updated_at > target.updated_at")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_daily_counts(spark: Any, table_name: str, source: Any) -> None:
    (
        _delta_table(spark, table_name)
        .alias("target")
        .merge(
            source.alias("source"),
            "target.entity = source.entity "
            "AND target.activity_date = source.activity_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _trim_before(
    spark: Any, table_name: str, date_column: str, first_date: date
) -> None:
    _delta_table(spark, table_name).delete(
        f"CAST({date_column} AS DATE) < DATE '{first_date.isoformat()}'"
    )


def _assert_state_invariants(
    spark: Any, simulation_date: date, source_schema: str
) -> dict[str, int]:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError(
            "PySpark is required to validate generator state"
        ) from exc

    first_date = window_start(simulation_date)
    user_state_table = _table_name(source_schema, "generator_user_state")
    users_table = _table_name(source_schema, "generator_users")
    user_state = spark.table(user_state_table)
    users = spark.table(users_table)
    state_count = user_state.count()
    user_count = users.count()
    distinct_state_count = user_state.select("user_id").distinct().count()
    if state_count != user_count or distinct_state_count != state_count:
        raise RuntimeError(
            "generator_user_state must contain exactly one row per generator user"
        )

    result = {}
    for table_suffix, key in (
        ("generator_recent_conversations", "conversation_id"),
        ("generator_recent_messages", "message_id"),
        ("generator_subscriptions", "subscription_id"),
    ):
        frame = spark.table(_table_name(source_schema, table_suffix))
        count = frame.count()
        if frame.select(key).distinct().count() != count:
            raise RuntimeError(f"{table_suffix} contains duplicate {key} values")
        if table_suffix.startswith("generator_recent_"):
            outside = frame.filter(
                (functions.to_date("ingested_at") < functions.lit(first_date))
                | (functions.to_date("ingested_at") > functions.lit(simulation_date))
            ).limit(1).count()
            if outside:
                raise RuntimeError(
                    f"{table_suffix} contains rows outside the 30-day window"
                )
        result[table_suffix.removeprefix("generator_") + "_count"] = count
    return result


def _daily_count_summary(
    spark: Any, simulation_date: date, source_schema: str
) -> dict[str, Any]:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - Databricks integration only
        raise RuntimeError(
            "PySpark is required to validate generator state"
        ) from exc

    rows = (
        spark.table(_table_name(source_schema, "generator_daily_counts"))
        .filter(functions.col("entity").isin(*COUNT_ENTITIES))
        .groupBy("entity")
        .agg(
            functions.min("activity_date").alias("first_date"),
            functions.max("activity_date").alias("last_date"),
            functions.countDistinct("activity_date").alias("days"),
            functions.count("*").alias("rows"),
        )
        .collect()
    )
    summary = {row["entity"]: row.asDict(recursive=True) for row in rows}
    expected_first = window_start(simulation_date)
    for entity in COUNT_ENTITIES:
        item = summary.get(entity)
        if (
            item is None
            or item["first_date"] != expected_first
            or item["last_date"] != simulation_date
            or item["days"] != 30
            or item["rows"] != 30
        ):
            raise RuntimeError(
                f"generator_daily_counts does not contain one complete 30-day "
                f"window for {entity}: {item}"
            )
    return summary


def update_generator_state(
    spark: Any,
    clean_records: Mapping[str, Iterable[Any]],
    simulation_date: date,
    source_schema: str = SOURCE_SCHEMA,
) -> dict[str, Any]:
    """Apply one clean generator day, writing daily counts last as its marker."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    plan = _build_plan(spark, clean_records, simulation_date, source_schema)
    summary = _validation_summary(plan, simulation_date, source_schema)
    summary["mutation_attempted"] = True

    if plan["user_state"] is not None:
        _merge_existing_only(
            spark,
            _table_name(source_schema, "generator_user_state"),
            plan["user_state"],
            "user_id",
        )

    conversation_table = _table_name(source_schema, "generator_recent_conversations")
    message_table = _table_name(source_schema, "generator_recent_messages")
    if plan["records"]["conversations"]:
        _merge_all(
            spark, conversation_table, plan["conversations"], "conversation_id"
        )
    _trim_before(
        spark, conversation_table, "ingested_at", window_start(simulation_date)
    )

    if plan["records"]["messages"]:
        _merge_all(spark, message_table, plan["messages"], "message_id")
    _trim_before(spark, message_table, "ingested_at", window_start(simulation_date))

    if plan["records"]["subscriptions"]:
        _merge_subscriptions(
            spark,
            _table_name(source_schema, "generator_subscriptions"),
            plan["subscriptions"],
        )

    summary.update(_assert_state_invariants(spark, simulation_date, source_schema))

    count_table = _table_name(source_schema, "generator_daily_counts")
    _trim_before(spark, count_table, "activity_date", window_start(simulation_date))
    _merge_daily_counts(spark, count_table, plan["daily_counts"])
    summary["daily_count_date_range"] = _daily_count_summary(
        spark, simulation_date, source_schema
    )
    summary["completed"] = True
    return summary
