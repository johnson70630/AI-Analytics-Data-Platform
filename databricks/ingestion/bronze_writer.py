"""Schema-safe, retry-idempotent appends to managed Delta Bronze tables."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from typing import Any

from src.data_generator.writers import serialize_record


TARGET_SCHEMA = "ai_analytics.bronze"

SUPPORTED_DAILY_ENTITIES = (
    "user_updates",
    "conversations",
    "messages",
    "completions",
    "model_inferences",
    "feedback",
    "errors",
    "subscriptions",
    "purchases",
    "payments",
)


def validate_entity(entity: str) -> str:
    """Return a supported daily entity or reject it before any Spark work."""
    if entity not in SUPPORTED_DAILY_ENTITIES:
        raise ValueError(f"Unsupported daily Bronze entity: {entity!r}")
    return entity


def transaction_app_id(entity: str) -> str:
    """Return the stable Delta transaction application ID for one entity."""
    return f"ai-analytics-bronze-{validate_entity(entity)}"


def transaction_version(simulation_date: date) -> int:
    """Convert a simulation date into its deterministic YYYYMMDD version."""
    if not isinstance(simulation_date, date):
        raise TypeError("simulation_date must be a datetime.date")
    return int(simulation_date.strftime("%Y%m%d"))


def target_table_name(
    entity: str,
    target_schema: str = TARGET_SCHEMA,
) -> str:
    """Return the fully qualified managed Bronze table name."""
    validate_entity(entity)
    if not isinstance(target_schema, str) or not target_schema.strip():
        raise ValueError("target_schema must be a non-empty string")
    return f"{target_schema}.{entity}"


def _summary(
    entity: str,
    simulation_date: date,
    input_count: int,
    *,
    target_schema: str,
    write_attempted: bool,
    status: str,
    validated_count: int | None = None,
) -> dict[str, Any]:
    return {
        "entity": entity,
        "input_physical_row_count": input_count,
        "validated_physical_row_count": validated_count,
        "write_attempted": write_attempted,
        "submitted_physical_row_count": input_count if write_attempted else 0,
        "target_table": target_table_name(entity, target_schema),
        "transaction_app_id": transaction_app_id(entity),
        "transaction_version": transaction_version(simulation_date),
        "status": status,
    }


def _target_struct_type(spark: Any, target_table: str) -> Any:
    if not spark.catalog.tableExists(target_table):
        raise RuntimeError(f"Required Delta Bronze table does not exist: {target_table}")
    target_struct_type = spark.table(target_table).schema
    target_columns = tuple(field.name for field in target_struct_type.fields)
    if "dt" not in target_columns:
        raise ValueError(f"Target Bronze table has no dt column: {target_table}")
    return target_struct_type


def _align_records(
    entity: str,
    records: Iterable[Mapping[str, Any]],
    simulation_date: date,
    target_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Copy records, add dt, reject extras, and retain physical row order."""
    aligned = []
    allowed = set(target_columns)
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise TypeError(
                f"{entity} record {index} must be a mapping, "
                f"not {type(record).__name__}"
            )
        row = dict(record)
        row["dt"] = simulation_date
        unexpected = set(row).difference(allowed)
        if unexpected:
            raise ValueError(
                f"{entity} record {index} contains fields absent from the "
                f"target schema: {sorted(unexpected)}"
            )
        aligned.append({column: row.get(column) for column in target_columns})
    return aligned


def prepare_bronze_dataframe(
    spark: Any,
    entity: str,
    records: Iterable[Mapping[str, Any]],
    simulation_date: date,
    target_schema: str = TARGET_SCHEMA,
) -> Any:
    """Build a DataFrame with exactly the live target table's StructType.

    Missing dirty-record fields are parsed as null. Unexpected source fields
    are rejected before Spark execution. FAILFAST parsing makes incompatible
    non-null values fail instead of silently becoming null.
    """
    validate_entity(entity)
    transaction_version(simulation_date)
    target_table = target_table_name(entity, target_schema)
    target_struct_type = _target_struct_type(spark, target_table)
    target_columns = tuple(field.name for field in target_struct_type.fields)
    aligned = _align_records(
        entity,
        records,
        simulation_date,
        target_columns,
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not aligned:
        return spark.createDataFrame([], target_struct_type)

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - exercised in Databricks
        raise RuntimeError(
            "PySpark is required to prepare Delta Bronze DataFrames"
        ) from exc

    json_rows = [
        (serialize_record(row).rstrip("\n"),)
        for row in aligned
    ]
    raw = spark.createDataFrame(json_rows, ["_bronze_json"])
    prepared = (
        raw.select(
            functions.from_json(
                functions.col("_bronze_json"),
                target_struct_type,
                {"mode": "FAILFAST"},
            ).alias("_record")
        )
        .select("_record.*")
        .select(*target_columns)
    )
    return prepared


def append_bronze_entity(
    spark: Any,
    entity: str,
    records: Iterable[Mapping[str, Any]],
    simulation_date: date,
    target_schema: str = TARGET_SCHEMA,
) -> dict[str, Any]:
    """Validate and append one physical entity batch with Delta txn options."""
    validate_entity(entity)
    version = transaction_version(simulation_date)
    materialized = list(records)
    if not materialized:
        return _summary(
            entity,
            simulation_date,
            0,
            target_schema=target_schema,
            write_attempted=False,
            status="SKIPPED_EMPTY",
            validated_count=0,
        )

    prepared = prepare_bronze_dataframe(
        spark,
        entity,
        materialized,
        simulation_date,
        target_schema,
    )
    validated_count = prepared.count()
    if validated_count != len(materialized):
        raise ValueError(
            f"{entity} physical row count changed during schema alignment: "
            f"input={len(materialized)}, prepared={validated_count}"
        )

    target_table = target_table_name(entity, target_schema)
    (
        prepared.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .option("txnAppId", transaction_app_id(entity))
        .option("txnVersion", version)
        .saveAsTable(target_table)
    )
    return _summary(
        entity,
        simulation_date,
        len(materialized),
        target_schema=target_schema,
        write_attempted=True,
        status="SUBMITTED",
        validated_count=validated_count,
    )


def _validate_day_entities(bronze_records: Mapping[str, Any]) -> None:
    unexpected = set(bronze_records).difference(SUPPORTED_DAILY_ENTITIES)
    if unexpected:
        raise ValueError(
            "Bronze day contains unsupported entities: "
            f"{sorted(unexpected)}"
        )


def append_bronze_day(
    spark: Any,
    bronze_records: Mapping[str, Iterable[Mapping[str, Any]]],
    simulation_date: date,
    target_schema: str = TARGET_SCHEMA,
) -> list[dict[str, Any]]:
    """Append all supported daily entities in a stable order."""
    _validate_day_entities(bronze_records)
    return [
        append_bronze_entity(
            spark,
            entity,
            bronze_records.get(entity, ()),
            simulation_date,
            target_schema,
        )
        for entity in SUPPORTED_DAILY_ENTITIES
    ]


def validate_bronze_day(
    spark: Any,
    bronze_records: Mapping[str, Iterable[Mapping[str, Any]]],
    simulation_date: date,
    target_schema: str = TARGET_SCHEMA,
) -> list[dict[str, Any]]:
    """Prepare and execute schema validation without writing Delta data."""
    _validate_day_entities(bronze_records)
    summaries = []
    for entity in SUPPORTED_DAILY_ENTITIES:
        materialized = list(bronze_records.get(entity, ()))
        prepared = prepare_bronze_dataframe(
            spark,
            entity,
            materialized,
            simulation_date,
            target_schema,
        )
        validated_count = prepared.count()
        if validated_count != len(materialized):
            raise ValueError(
                f"{entity} physical row count changed during schema alignment: "
                f"input={len(materialized)}, prepared={validated_count}"
            )
        summaries.append(
            _summary(
                entity,
                simulation_date,
                len(materialized),
                target_schema=target_schema,
                write_attempted=False,
                status="VALIDATED",
                validated_count=validated_count,
            )
        )
    return summaries
