from collections import Counter
from datetime import datetime, timezone
import re

SOURCE_SCHEMA = "ai_analytics.source"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_source_schema(source_schema):
    if not isinstance(source_schema, str) or not source_schema.strip():
        raise ValueError("source_schema must be a non-empty string")

    parts = source_schema.split(".")

    if len(parts) not in (1, 2) or not all(
        _IDENTIFIER.fullmatch(part) for part in parts
    ):
        raise ValueError(f"invalid source_schema: {source_schema!r}")

    return source_schema


def _to_utc(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    return value


def _rows_to_dicts(df, *order_columns):
    if order_columns:
        df = df.orderBy(*order_columns)

    rows = []

    for row in df.collect():
        record = row.asDict(recursive=True)

        rows.append({
            key: _to_utc(value)
            for key, value in record.items()
        })

    return rows


def load_generator_state(spark, source_schema=SOURCE_SCHEMA) -> dict:
    source_schema = _validate_source_schema(source_schema)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    users = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_users"),
        "user_id",
    )

    user_state_rows = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_user_state"),
        "user_id",
    )

    user_state = {
        row["user_id"]: {
            "email": row["email"],
            "name": row["name"],
            "country_code": row["country_code"],
            "account_status": row["account_status"],
        }
        for row in user_state_rows
    }

    models = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_models"),
        "model_id",
    )

    devices = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_devices"),
        "device_id",
    )

    plans = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_subscription_plans"),
        "plan_id",
    )

    recent_conversations = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_recent_conversations"),
        "created_at",
        "conversation_id",
    )

    recent_messages = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_recent_messages"),
        "created_at",
        "message_id",
    )

    subscriptions = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_subscriptions"),
        "started_at",
        "subscription_id",
    )

    count_rows = _rows_to_dicts(
        spark.table(f"{source_schema}.generator_daily_counts"),
        "entity",
        "activity_date",
    )

    counts = {
        "user_updates": Counter(),
        "conversations": Counter(),
        "messages": Counter(),
        "errors": Counter(),
    }

    for row in count_rows:
        counts[row["entity"]][row["activity_date"]] = row["row_count"]

    return {
        "users": users,
        "user_state": user_state,
        "models": models,
        "devices": devices,
        "plans": plans,
        "recent": {
            "conversations": recent_conversations,
            "messages": recent_messages,
        },
        "subscriptions": subscriptions,
        "counts": counts,
    }
