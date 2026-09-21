"""Append-only daily source and Bronze generation from existing S3 state."""

from __future__ import annotations

import hashlib
import json
import random
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as day_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from faker import Faker

from .activity_data import (
    DEVICE_WEIGHTS,
    HOUR_WEIGHTS,
    PLATFORMS,
    PLATFORM_WEIGHTS,
    _prompt_text,
)
from .finance_data import add_month, build_purchase_and_payments
from .inference_data import generate_inference_records
from .ingestion import EVENT_ID_FIELDS, ingestion_timestamp
from .messy_data import BRONZE_ENTITIES, inject_record
from .quality_events import (
    ERROR_SOURCES,
    ERROR_SOURCE_WEIGHTS,
    build_independent_error,
    build_linked_model_error,
    build_feedback_event,
    should_generate_feedback,
)
from .reference_data import (
    validate_devices,
    validate_models,
    validate_subscription_plans,
)
from .user_data import (
    UPDATE_FIELDS,
    _choose_update_field,
    _new_account_status,
    _new_country,
    _new_email,
    _new_name,
)
from .utils import generate_uuid, initialize_randomness
from .writers import serialize_ndjson


DAILY_ENTITIES = (
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
REFERENCE_ENTITIES = ("models", "devices", "subscription_plans")
COPY_ONLY_ENTITIES = {"user_updates", "conversations", "errors", "subscriptions"}
ID_ENTITIES = {
    "user_updates": "update_id",
    "conversations": "conversation_id",
    "messages": "message_id",
    "completions": "completion_id",
    "model_inferences": "inference_id",
    "feedback": "feedback_id",
    "errors": "error_id",
    "subscriptions": "subscription_id",
    "purchases": "purchase_id",
    "payments": "payment_id",
}
TIMESTAMP_FIELDS = {
    "users": ("signup_at", "ingested_at"),
    "user_updates": ("updated_at", "ingested_at"),
    "conversations": ("created_at", "ingested_at"),
    "messages": ("created_at", "ingested_at"),
    "completions": ("requested_at", "completed_at", "ingested_at"),
    "model_inferences": ("request_at", "response_at", "ingested_at"),
    "feedback": ("created_at", "ingested_at"),
    "errors": ("occurred_at", "ingested_at"),
    "subscriptions": ("started_at", "ended_at", "updated_at", "ingested_at"),
    "purchases": ("purchase_created_at", "updated_at", "ingested_at"),
    "payments": ("processed_at", "refunded_at", "updated_at", "ingested_at"),
}


def daily_seed(simulation_date: date, seed: int) -> int:
    """Derive one stable RNG seed from the requested day and base seed."""
    digest = hashlib.sha256(f"{simulation_date.isoformat()}:{seed}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _parse_datetime(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_record(entity: str, row: dict[str, Any]) -> dict[str, Any]:
    for field in TIMESTAMP_FIELDS.get(entity, ()):
        if row.get(field) is not None:
            row[field] = _parse_datetime(row[field])
    if entity == "models" and isinstance(row.get("release_date"), str):
        row["release_date"] = date.fromisoformat(row["release_date"])
    return row


def _partition_date(key: str) -> date:
    return date.fromisoformat(key.split("/dt=", 1)[1].split("/", 1)[0])


def list_entity_objects(client: Any, bucket: str, layer: str, entity: str) -> list[dict]:
    objects = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{layer}/{entity}/dt="
    ):
        objects.extend(
            item for item in page.get("Contents", []) if item["Key"].endswith(".json")
        )
    return sorted(objects, key=lambda item: item["Key"])


def _load_keys(client: Any, bucket: str, entity: str, keys: Iterable[str]) -> list[dict]:
    def load(key: str) -> list[dict]:
        response = client.get_object(Bucket=bucket, Key=key)
        return [
            _parse_record(entity, json.loads(line))
            for line in response["Body"].read().decode("utf-8").splitlines()
            if line
        ]

    key_list = list(keys)
    with ThreadPoolExecutor(max_workers=8) as executor:
        chunks = executor.map(load, key_list)
        rows = [row for chunk in chunks for row in chunk]
    return rows


def _keys_between(objects: list[dict], first_date: date, last_date: date) -> list[str]:
    return [
        item["Key"]
        for item in objects
        if first_date <= _partition_date(item["Key"]) <= last_date
    ]


def _latest_keys(objects: list[dict]) -> list[str]:
    newest = max(_partition_date(item["Key"]) for item in objects)
    return [item["Key"] for item in objects if _partition_date(item["Key"]) == newest]


def _average(counts: Counter, end: date, days: int) -> float:
    values = [counts[end - timedelta(days=offset)] for offset in range(days)]
    return sum(values) / days


def discover_historical_max(client: Any, bucket: str) -> date:
    """Return the latest Raw partition across evolving source entities."""
    dates = []
    for entity in DAILY_ENTITIES:
        dates.extend(
            _partition_date(item["Key"])
            for item in list_entity_objects(client, bucket, "raw", entity)
        )
    if not dates:
        raise RuntimeError("No historical Raw daily partitions found")
    return max(dates)


def assert_target_available(
    client: Any,
    bucket: str,
    simulation_date: date,
    historical_max: date,
    *,
    overwrite: bool = False,
) -> None:
    found = []
    for layer in ("raw", "bronze"):
        for entity in (*DAILY_ENTITIES, *REFERENCE_ENTITIES):
            prefix = f"{layer}/{entity}/dt={simulation_date.isoformat()}/"
            if client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get(
                "KeyCount", 0
            ):
                found.append(prefix)
    quality_prefix = f"quality/injection_manifest/dt={simulation_date.isoformat()}/"
    if client.list_objects_v2(
        Bucket=bucket, Prefix=quality_prefix, MaxKeys=1
    ).get("KeyCount", 0):
        found.append(quality_prefix)
    if found and not overwrite:
        raise FileExistsError(
            "daily partition already exists; refusing to overwrite: " + ", ".join(found)
        )
    expected = historical_max + timedelta(days=1)
    if simulation_date != expected:
        raise ValueError(
            f"daily date must be the next date after {historical_max}: {expected}"
        )


def capture_historical_inventory(client: Any, bucket: str, through: date) -> dict:
    """Capture key/size/ETag metadata for all historical Raw and Bronze objects."""
    inventory = {}
    for layer in ("raw", "bronze"):
        for entity in (*DAILY_ENTITIES, *REFERENCE_ENTITIES, "users"):
            for item in list_entity_objects(client, bucket, layer, entity):
                if _partition_date(item["Key"]) <= through:
                    inventory[item["Key"]] = (
                        int(item.get("Size", 0)),
                        str(item.get("ETag", "")),
                    )
    for item in list_entity_objects(client, bucket, "quality", "injection_manifest"):
        if _partition_date(item["Key"]) <= through:
            inventory[item["Key"]] = (int(item.get("Size", 0)), str(item.get("ETag", "")))
    return dict(sorted(inventory.items()))


def _latest_user_state(users: list[dict], updates: list[dict]) -> dict[str, dict]:
    state = {
        row["user_id"]: {
            "email": row["email"],
            "name": row["name"],
            "country_code": row["country_code"],
            "account_status": row["account_status"],
        }
        for row in users
    }
    for update in sorted(updates, key=lambda row: row["updated_at"]):
        state[update["user_id"]][update["field_name"]] = update["new_value"]
    return state


def load_daily_state(client: Any, bucket: str, historical_max: date) -> dict:
    """Load compact source state and recent distributions directly from S3 Raw."""
    inventories = {
        entity: list_entity_objects(client, bucket, "raw", entity)
        for entity in ("users", *DAILY_ENTITIES, *REFERENCE_ENTITIES)
    }
    recent_start = historical_max - timedelta(days=29)
    first_source_date = date.min
    users = _load_keys(
        client,
        bucket,
        "users",
        _keys_between(inventories["users"], first_source_date, historical_max),
    )
    updates = _load_keys(
        client,
        bucket,
        "user_updates",
        _keys_between(
            inventories["user_updates"], first_source_date, historical_max
        ),
    )
    models = _load_keys(client, bucket, "models", _latest_keys(inventories["models"]))
    devices = _load_keys(client, bucket, "devices", _latest_keys(inventories["devices"]))
    plans = _load_keys(
        client,
        bucket,
        "subscription_plans",
        _latest_keys(inventories["subscription_plans"]),
    )
    validate_models(models)
    validate_devices(devices)
    validate_subscription_plans(plans)
    recent = {
        entity: _load_keys(
            client,
            bucket,
            entity,
            _keys_between(inventories[entity], recent_start, historical_max),
        )
        for entity in (
            "conversations",
            "messages",
            "completions",
            "model_inferences",
            "feedback",
            "errors",
            "purchases",
            "payments",
        )
    }
    subscriptions = _load_keys(
        client,
        bucket,
        "subscriptions",
        _keys_between(
            inventories["subscriptions"], first_source_date, historical_max
        ),
    )
    recent_rows = {
        **recent,
        "user_updates": [row for row in updates if row["ingested_at"].date() >= recent_start],
        "subscriptions": [
            row for row in subscriptions if row["ingested_at"].date() >= recent_start
        ],
    }
    counts = {
        entity: Counter(row["ingested_at"].date() for row in recent_rows[entity])
        for entity in DAILY_ENTITIES
    }
    return {
        "inventories": inventories,
        "users": users,
        "updates": updates,
        "user_state": _latest_user_state(users, updates),
        "models": models,
        "devices": devices,
        "plans": plans,
        "recent": recent,
        "subscriptions": subscriptions,
        "counts": counts,
    }


def _at_on_day(simulation_date: date, *, earliest_second: int = 0) -> datetime:
    second = random.randint(earliest_second, 86_398)
    return datetime.combine(simulation_date, day_time.min, timezone.utc) + timedelta(
        seconds=second
    )


def _device_id(devices: list[dict]) -> str | None:
    if random.random() < 0.0075:
        return None
    platform = random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS, k=1)[0]
    candidates = [row for row in devices if row["app_platform"] == platform]
    return random.choices(
        [row["device_id"] for row in candidates],
        weights=[DEVICE_WEIGHTS[row["device_id"]] for row in candidates],
        k=1,
    )[0]


def _generate_updates(
    users_by_id: dict[str, dict], state: dict[str, dict], active_ids: list[str],
    target: int, simulation_date: date, fake: Faker, generation_end: datetime,
    last_activity_by_user: dict[str, datetime],
) -> list[dict]:
    eligible = [user_id for user_id in active_ids if state[user_id]["account_status"] != "CLOSED"]
    selected = random.sample(eligible, min(target, len(eligible)))
    rows = []
    for user_id in selected:
        current = state[user_id]
        field = _choose_update_field(current)
        old = current[field]
        if field == "country_code":
            new = _new_country(old)
        elif field == "account_status":
            new = _new_account_status(old)
        elif field == "name":
            new = _new_name(fake, old)
        else:
            new = _new_email(fake, old)
        updated_at = _at_on_day(simulation_date)
        if field == "account_status" and new == "CLOSED":
            updated_at = max(
                updated_at,
                last_activity_by_user.get(user_id, updated_at) + timedelta(seconds=1),
            )
            if updated_at > generation_end:
                updated_at = generation_end
        rows.append(
            {
                "update_id": generate_uuid(),
                "user_id": user_id,
                "field_name": field,
                "old_value": old,
                "new_value": new,
                "updated_at": updated_at,
                "ingested_at": ingestion_timestamp(updated_at, generation_end),
            }
        )
        current[field] = new
    return rows


def _generate_activity(
    state: dict, simulation_date: date, generation_end: datetime
) -> tuple[list[dict], list[dict], list[str], set[str]]:
    users = state["users"]
    user_state = state["user_state"]
    recent_messages = state["recent"]["messages"]
    counts = state["counts"]
    historical_max = simulation_date - timedelta(days=1)
    daily_active = defaultdict(set)
    for row in recent_messages:
        daily_active[row["ingested_at"].date()].add(row["user_id"])
    active_target = max(
        1,
        round(
            sum(
                len(daily_active[historical_max - timedelta(days=offset)])
                for offset in range(7)
            )
            / 7
        ),
    )
    eligible = [
        row["user_id"]
        for row in users
        if user_state[row["user_id"]]["account_status"] != "CLOSED"
    ]
    active_ids = random.sample(eligible, min(active_target, len(eligible)))
    new_count = max(1, round(_average(counts["conversations"], historical_max, 7)))
    continued_count = max(1, round(new_count * 0.12))
    message_target = max(
        new_count + continued_count,
        round(_average(counts["messages"], historical_max, 7)),
    )

    recent_conversations = {
        row["conversation_id"]: row for row in state["recent"]["conversations"]
    }
    latest_message = {}
    for row in recent_messages:
        current = latest_message.get(row["conversation_id"])
        if current is None or (row["sequence_number"], row["created_at"]) > (
            current["sequence_number"], current["created_at"]
        ):
            latest_message[row["conversation_id"]] = row
    candidates = [
        (conversation_id, recent_conversations[conversation_id], message)
        for conversation_id, message in latest_message.items()
        if conversation_id in recent_conversations
        and user_state[message["user_id"]]["account_status"] != "CLOSED"
    ]
    selected_continued = random.sample(candidates, min(continued_count, len(candidates)))

    conversations = []
    sessions = []
    for _ in range(new_count):
        user_id = random.choice(active_ids)
        created_at = _at_on_day(simulation_date)
        conversation = {
            "conversation_id": generate_uuid(),
            "user_id": user_id,
            "created_at": created_at,
            "ingested_at": ingestion_timestamp(created_at, generation_end),
        }
        conversations.append(conversation)
        sessions.append((conversation, 0, created_at, None))
    continued_ids = []
    for conversation_id, conversation, last_message in selected_continued:
        continued_ids.append(conversation_id)
        sessions.append(
            (
                conversation,
                int(last_message["sequence_number"]),
                datetime.combine(simulation_date, day_time.min, timezone.utc),
                last_message.get("device_id"),
            )
        )

    allocations = [1] * len(sessions)
    for _ in range(message_target - len(sessions)):
        allocations[random.randrange(len(sessions))] += 1
    messages = []
    for session, count in zip(sessions, allocations):
        conversation, prior_sequence, lower_bound, previous_device = session
        available = max(1, int((generation_end - lower_bound).total_seconds()))
        offsets = sorted(random.sample(range(1, available + 1), min(count, available)))
        for index, offset in enumerate(offsets, start=1):
            if previous_device is None or random.random() < 0.12:
                previous_device = _device_id(state["devices"])
            created_at = lower_bound + timedelta(seconds=offset)
            messages.append(
                {
                    "message_id": generate_uuid(),
                    "conversation_id": conversation["conversation_id"],
                    "user_id": conversation["user_id"],
                    "device_id": previous_device,
                    "sequence_number": prior_sequence + index,
                    "message_text": _prompt_text(),
                    "created_at": created_at,
                    "ingested_at": ingestion_timestamp(created_at, generation_end),
                }
            )
    return conversations, messages, continued_ids, set(active_ids)


def _generate_quality(
    messages: list[dict], completions: list[dict], inferences: list[dict],
    models: list[dict], error_target: int, generation_end: datetime
) -> tuple[list[dict], list[dict]]:
    completion_by_id = {row["completion_id"]: row for row in completions}
    message_by_id = {row["message_id"]: row for row in messages}
    feedback = []
    propensities = {}
    inference_contexts = []
    failed = []
    for inference in inferences:
        completion = completion_by_id[inference["completion_id"]]
        message = message_by_id[completion["message_id"]]
        context = (
            inference["inference_id"],
            inference["completion_id"],
            inference["user_id"],
            inference["model_id"],
            round(inference["request_at"].timestamp() * 1_000_000),
            None
            if inference["response_at"] is None
            else round(inference["response_at"].timestamp() * 1_000_000),
            message["message_id"],
            message["conversation_id"],
            round(completion["completed_at"].timestamp() * 1_000_000),
        )
        inference_contexts.append(context)
        if inference["inference_status"] == "FAILED":
            failed.append(
                (
                    context[0], context[1], context[2], context[3], context[4],
                    context[6], context[7], context[8],
                )
            )
        if (
            inference["inference_status"] == "SUCCESS"
            and should_generate_feedback(
                inference["user_id"], inference["latency_ms"], propensities
            )
        ):
            feedback.append(
                build_feedback_event(
                    inference["completion_id"],
                    inference["user_id"],
                    completion["completed_at"],
                    inference["model_id"],
                    inference["latency_ms"],
                    generation_end,
                )
            )
    message_contexts = [
        (
            row["message_id"], row["conversation_id"], row["user_id"],
            round(row["created_at"].timestamp() * 1_000_000),
        )
        for row in messages
    ]
    sources = random.choices(ERROR_SOURCES, weights=ERROR_SOURCE_WEIGHTS, k=error_target)
    linked_target = min(len(failed), round(sources.count("MODEL") * 0.82))
    random.shuffle(failed)
    errors = []
    linked_index = 0
    for source in sources:
        if source == "MODEL" and linked_index < linked_target:
            errors.append(build_linked_model_error(failed[linked_index], generation_end))
            linked_index += 1
        else:
            errors.append(
                build_independent_error(
                    source, message_contexts, inference_contexts, models, generation_end
                )
            )
    return feedback, errors


def _generate_finance(state: dict, simulation_date: date, generation_end: datetime) -> tuple:
    plans = {row["plan_id"]: row for row in state["plans"]}
    users = {
        row["user_id"]: {**row, **state["user_state"][row["user_id"]]}
        for row in state["users"]
    }
    purchases = []
    payments = []
    for subscription in state["subscriptions"]:
        if subscription["subscription_status"] != "ACTIVE":
            continue
        plan = plans[subscription["plan_id"]]
        if plan["plan_name"] == "FREE":
            continue
        billing_at = add_month(subscription["started_at"])
        while billing_at.date() < simulation_date:
            billing_at = add_month(billing_at)
        if billing_at.date() != simulation_date:
            continue
        purchase, attempts = build_purchase_and_payments(
            users[subscription["user_id"]],
            subscription,
            plan,
            "RENEWAL",
            billing_at,
            generation_end,
            generation_end,
        )
        purchases.append(purchase)
        payments.extend(attempts)
    return [], purchases, payments


def generate_daily_records(state: dict, simulation_date: date, seed: int) -> dict:
    """Generate one deterministic logical day from the same prior source state."""
    fake = initialize_randomness(daily_seed(simulation_date, seed))
    generation_end = datetime.combine(
        simulation_date, day_time(23, 59, 59), timezone.utc
    )
    conversations, messages, continued_ids, active_ids = _generate_activity(
        state, simulation_date, generation_end
    )
    users_by_id = {row["user_id"]: row for row in state["users"]}
    update_target = max(
        1,
        round(_average(state["counts"]["user_updates"], simulation_date - timedelta(days=1), 7)),
    )
    last_activity_by_user = {}
    for message in messages:
        last_activity_by_user[message["user_id"]] = max(
            message["created_at"],
            last_activity_by_user.get(message["user_id"], message["created_at"]),
        )
    updates = _generate_updates(
        users_by_id,
        {key: dict(value) for key, value in state["user_state"].items()},
        sorted(active_ids),
        update_target,
        simulation_date,
        fake,
        generation_end,
        last_activity_by_user,
    )
    completions, inferences = generate_inference_records(
        messages, state["models"], generation_end
    )
    error_target = max(
        1,
        round(_average(state["counts"]["errors"], simulation_date - timedelta(days=1), 7)),
    )
    feedback, errors = _generate_quality(
        messages, completions, inferences, state["models"], error_target, generation_end
    )
    subscriptions, purchases, payments = _generate_finance(
        state, simulation_date, generation_end
    )
    records = {
        "user_updates": updates,
        "conversations": conversations,
        "messages": messages,
        "completions": completions,
        "model_inferences": inferences,
        "feedback": feedback,
        "errors": errors,
        "subscriptions": subscriptions,
        "purchases": purchases,
        "payments": payments,
    }
    records["_metadata"] = {
        "active_user_ids": sorted({row["user_id"] for row in messages}),
        "continued_conversation_ids": continued_ids,
    }
    return records


def _bronze_records(records: dict, seed: int, simulation_date: date, state: dict) -> tuple:
    generation_end = datetime.combine(
        simulation_date, day_time(23, 59, 59), timezone.utc
    )
    historical_conversations = {
        row["conversation_id"]: row["created_at"]
        for row in state["recent"]["conversations"]
    }
    contexts = {
        "conversations": {
            **historical_conversations,
            **{row["conversation_id"]: row["created_at"] for row in records["conversations"]},
        },
        "purchases": {
            row["purchase_id"]: row["purchase_created_at"]
            for row in records["purchases"]
        },
    }
    bronze = {}
    manifest = []
    for entity in DAILY_ENTITIES:
        source_rows = records[entity]
        if not source_rows:
            continue
        if entity in COPY_ONLY_ENTITIES:
            bronze[entity] = [dict(row) for row in source_rows]
            continue
        output = []
        for row in source_rows:
            dirty, duplicate, issue = inject_record(
                row, entity, seed, generation_end, contexts
            )
            output.append(dirty)
            if duplicate is not None:
                output.append(duplicate)
            if issue is not None:
                manifest.append(issue)
        bronze[entity] = output
    return bronze, manifest


def validate_daily_records(records: dict, state: dict, simulation_date: date) -> dict:
    """Validate new-day relationships and clean Raw temporal causality."""
    violations = []
    users = {row["user_id"] for row in state["users"]}
    historical_conversations = {
        row["conversation_id"]: row for row in state["recent"]["conversations"]
    }
    conversations = {
        **historical_conversations,
        **{row["conversation_id"]: row for row in records["conversations"]},
    }
    messages = {row["message_id"]: row for row in records["messages"]}
    completions = {row["completion_id"]: row for row in records["completions"]}
    models = {row["model_id"]: row for row in state["models"]}
    subscriptions = {
        row["subscription_id"]: row for row in state["subscriptions"]
    }
    subscriptions.update({row["subscription_id"]: row for row in records["subscriptions"]})
    purchases = {row["purchase_id"]: row for row in records["purchases"]}
    plans = {row["plan_id"] for row in state["plans"]}
    for row in records["user_updates"]:
        if row["user_id"] not in users:
            violations.append("user_updates.user_id")
        if row["updated_at"].date() != simulation_date:
            violations.append("user_updates.updated_at")
        if row["field_name"] == "account_status" and row["new_value"] == "CLOSED":
            if any(
                message["user_id"] == row["user_id"]
                and message["created_at"] >= row["updated_at"]
                for message in records["messages"]
            ):
                violations.append("user_updates.closed_before_activity_end")
    for row in records["messages"]:
        parent = conversations.get(row["conversation_id"])
        if row["user_id"] not in users or parent is None or parent["user_id"] != row["user_id"]:
            violations.append("messages.reference")
        elif row["created_at"] < parent["created_at"]:
            violations.append("messages.causality")
    for row in records["completions"]:
        message = messages.get(row["message_id"])
        if message is None or row["user_id"] != message["user_id"]:
            violations.append("completions.reference")
        elif not message["created_at"] <= row["requested_at"] <= row["completed_at"]:
            violations.append("completions.causality")
    for row in records["model_inferences"]:
        completion = completions.get(row["completion_id"])
        model = models.get(row["model_id"])
        if completion is None or model is None or model["release_date"] > row["request_at"].date():
            violations.append("model_inferences.reference")
        if row["response_at"] is not None and row["response_at"] < row["request_at"]:
            violations.append("model_inferences.causality")
    for row in records["feedback"]:
        if row["completion_id"] not in completions or row["user_id"] not in users:
            violations.append("feedback.reference")
    for row in records["subscriptions"]:
        if row["user_id"] not in users or row["plan_id"] not in plans:
            violations.append("subscriptions.reference")
    for row in records["purchases"]:
        subscription = subscriptions.get(row["subscription_id"])
        if subscription is None or row["user_id"] != subscription["user_id"]:
            violations.append("purchases.reference")
    for row in records["payments"]:
        purchase = purchases.get(row["purchase_id"])
        if purchase is None or row["user_id"] != purchase["user_id"]:
            violations.append("payments.reference")
        elif row["processed_at"] < purchase["purchase_created_at"]:
            violations.append("payments.causality")
    for entity, rows in records.items():
        if entity.startswith("_"):
            continue
        ids = [row[ID_ENTITIES[entity]] for row in rows]
        if len(ids) != len(set(ids)):
            violations.append(f"{entity}.duplicate_new_id")
        for row in rows:
            if row["ingested_at"].date() != simulation_date:
                violations.append(f"{entity}.partition")
    if violations:
        raise ValueError("daily validation failed: " + ", ".join(violations[:20]))
    return {"referential_violations": 0, "causal_violations": 0}


def validate_global_ids(
    client: Any, bucket: str, records: dict, through_date: date | None = None
) -> int:
    """Stream historical IDs one entity at a time and reject collisions."""
    collisions = 0
    for entity, id_field in ID_ENTITIES.items():
        new_ids = {row[id_field] for row in records[entity]}
        if not new_ids:
            continue
        def count_collisions(item: dict) -> int:
            response = client.get_object(Bucket=bucket, Key=item["Key"])
            return sum(
                1
                for raw_line in response["Body"].iter_lines()
                if raw_line and json.loads(raw_line).get(id_field) in new_ids
            )

        objects = [
            item
            for item in list_entity_objects(client, bucket, "raw", entity)
            if through_date is None or _partition_date(item["Key"]) <= through_date
        ]
        with ThreadPoolExecutor(max_workers=8) as executor:
            collisions += sum(executor.map(count_collisions, objects))
    if collisions:
        raise ValueError(f"global ID collision validation failed: {collisions}")
    return collisions


def _logical_digest(records: dict) -> str:
    payload = {
        entity: records[entity]
        for entity in DAILY_ENTITIES
    }
    return hashlib.sha256(serialize_ndjson([payload]).encode()).hexdigest()


def _object_key(layer: str, entity: str, simulation_date: date, seed: int) -> str:
    """Build the arrival partition key.

    This centralized decision is the extension seam for a future bounded
    late-arrival mode. M15.1 always uses the simulation date and never writes
    a prior partition.
    """
    return (
        f"{layer}/{entity}/dt={simulation_date.isoformat()}/"
        f"daily-{simulation_date.isoformat()}-seed-{seed}.json"
    )


def write_daily_locally(
    root: str | Path, simulation_date: date, seed: int, records: dict,
    bronze: dict, manifest: list[dict], *, overwrite: bool = False
) -> dict:
    paths = {"raw": {}, "bronze": {}, "quality": {}}
    root = Path(root)
    for layer, datasets in (("raw", records), ("bronze", bronze)):
        for entity in DAILY_ENTITIES:
            rows = datasets.get(entity, [])
            if not rows:
                continue
            path = root / _object_key(layer, entity, simulation_date, seed)
            if path.exists() and not overwrite:
                raise FileExistsError(f"local daily output already exists: {path}")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(serialize_ndjson(rows), encoding="utf-8")
            paths[layer][entity] = path
    if manifest:
        key = _object_key("quality", "injection_manifest", simulation_date, seed)
        path = root / key
        if path.exists() and not overwrite:
            raise FileExistsError(f"local daily output already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(serialize_ndjson(manifest), encoding="utf-8")
        paths["quality"]["injection_manifest"] = path
    return paths


def upload_daily_files(
    client: Any,
    bucket: str,
    root: str | Path,
    paths: dict,
    *,
    overwrite: bool = False,
) -> dict:
    uploaded = {"raw": {}, "bronze": {}, "quality": {}}
    root = Path(root)
    started = time.perf_counter()
    for layer in ("raw", "bronze", "quality"):
        for entity, path in paths[layer].items():
            key = path.relative_to(root).as_posix()
            request = dict(
                Bucket=bucket,
                Key=key,
                Body=path.read_bytes(),
                ContentType="application/x-ndjson",
            )
            if not overwrite:
                request["IfNoneMatch"] = "*"
            client.put_object(**request)
            uploaded[layer][entity] = key
    return {"keys": uploaded, "seconds": time.perf_counter() - started}


def volume_report(state: dict, records: dict, historical_max: date) -> dict:
    report = {}
    for entity in DAILY_ENTITIES:
        report[entity] = {
            "new": len(records[entity]),
            "avg_7d": _average(state["counts"][entity], historical_max, 7),
            "avg_30d": _average(state["counts"][entity], historical_max, 30),
        }
    return report


def run_daily_generation(
    client: Any,
    bucket: str,
    simulation_date: date,
    seed: int,
    output_root: str | Path,
    *,
    upload: bool,
    overwrite: bool = False,
) -> dict:
    """Generate, validate, write, and optionally upload one append-only day."""
    total_started = time.perf_counter()
    discovered_max = discover_historical_max(client, bucket)
    if overwrite:
        if discovered_max > simulation_date:
            raise ValueError("overwrite cannot target a date before the S3 maximum")
        historical_max = simulation_date - timedelta(days=1)
    else:
        historical_max = discovered_max
    assert_target_available(
        client,
        bucket,
        simulation_date,
        historical_max,
        overwrite=overwrite,
    )
    historical_before = capture_historical_inventory(client, bucket, historical_max)

    state_started = time.perf_counter()
    state = load_daily_state(client, bucket, historical_max)
    state_seconds = time.perf_counter() - state_started

    generation_started = time.perf_counter()
    records = generate_daily_records(state, simulation_date, seed)
    first_digest = _logical_digest(records)
    replay = generate_daily_records(state, simulation_date, seed)
    if _logical_digest(replay) != first_digest:
        raise ValueError("daily determinism validation failed")
    generation_seconds = time.perf_counter() - generation_started
    validation = validate_daily_records(records, state, simulation_date)
    collisions = validate_global_ids(client, bucket, records, historical_max)
    bronze_started = time.perf_counter()
    bronze, manifest = _bronze_records(records, seed, simulation_date, state)
    bronze_seconds = time.perf_counter() - bronze_started

    write_started = time.perf_counter()
    paths = write_daily_locally(
        output_root,
        simulation_date,
        seed,
        records,
        bronze,
        manifest,
        overwrite=overwrite,
    )
    write_seconds = time.perf_counter() - write_started
    uploaded = {"keys": {"raw": {}, "bronze": {}, "quality": {}}, "seconds": 0.0}
    if upload:
        if overwrite:
            expected_keys = {
                path.relative_to(Path(output_root)).as_posix()
                for layer_paths in paths.values()
                for path in layer_paths.values()
            }
            existing_keys = set()
            for layer in ("raw", "bronze"):
                for entity in (*DAILY_ENTITIES, *REFERENCE_ENTITIES):
                    existing_keys.update(
                        item["Key"]
                        for item in list_entity_objects(client, bucket, layer, entity)
                        if _partition_date(item["Key"]) == simulation_date
                    )
            existing_keys.update(
                item["Key"]
                for item in list_entity_objects(
                    client, bucket, "quality", "injection_manifest"
                )
                if _partition_date(item["Key"]) == simulation_date
            )
            if existing_keys != expected_keys:
                raise ValueError(
                    "overwrite target inventory does not exactly match deterministic output"
                )
        uploaded = upload_daily_files(
            client, bucket, output_root, paths, overwrite=overwrite
        )
    historical_after = capture_historical_inventory(client, bucket, historical_max)
    if historical_before != historical_after:
        raise ValueError("historical S3 inventory changed during daily generation")

    issue_counts = Counter(row["issue_type"] for row in manifest)
    return {
        "historical_max": historical_max,
        "simulation_date": simulation_date,
        "state": state,
        "records": records,
        "bronze": bronze,
        "manifest": manifest,
        "issue_counts": dict(issue_counts),
        "validation": validation,
        "global_id_collisions": collisions,
        "determinism_digest": first_digest,
        "historical_inventory_objects": len(historical_before),
        "historical_immutability": "PASS",
        "paths": paths,
        "uploaded": uploaded,
        "volumes": volume_report(state, records, historical_max),
        "timings": {
            "state_loading": state_seconds,
            "generation_and_determinism": generation_seconds,
            "bronze_creation": bronze_seconds,
            "raw_bronze_writing": write_seconds,
            "s3_upload": uploaded["seconds"],
            "total": time.perf_counter() - total_started,
        },
    }


def print_daily_report(result: dict) -> None:
    records = result["records"]
    print("\nM15.1 daily generation: PASS")
    print(f"Historical maximum: {result['historical_max']}")
    print(f"Generated date: {result['simulation_date']}")
    print(f"Active users: {len(records['_metadata']['active_user_ids']):,}")
    print(f"Continued conversations: {len(records['_metadata']['continued_conversation_ids']):,}")
    for entity in DAILY_ENTITIES:
        volume = result["volumes"][entity]
        print(
            f"{entity}: {volume['new']:,} new / "
            f"7d avg {volume['avg_7d']:.1f} / 30d avg {volume['avg_30d']:.1f}"
        )
    print(f"Bronze issues: {result['issue_counts']}")
    print(f"Historical objects protected: {result['historical_inventory_objects']:,}")
    print(f"Determinism digest: {result['determinism_digest']}")
    print(f"Global ID collisions: {result['global_id_collisions']}")
    for name, seconds in result["timings"].items():
        print(f"{name} seconds: {seconds:.3f}")
