"""Conversation and user-message source generation for persistent chat threads."""

import json
import random
import statistics
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .reference_data import generate_devices
from .user_data import validate_user_updates, validate_users
from .utils import generate_uuid


CONVERSATION_MIN_COUNT = 45_000
CONVERSATION_MAX_COUNT = 55_000
MESSAGE_MIN_COUNT = 250_000
MESSAGE_MAX_COUNT = 350_000

CONVERSATION_FIELDS = {"conversation_id", "user_id", "created_at"}
MESSAGE_FIELDS = {
    "message_id",
    "conversation_id",
    "user_id",
    "device_id",
    "sequence_number",
    "message_text",
    "created_at",
}
WAREHOUSE_FIELDS = {
    "user_key",
    "date_key",
    "conversation_key",
    "message_key",
}

ACTIVITY_SEGMENTS = ("INACTIVE", "LOW", "NORMAL", "ACTIVE", "POWER")
ACTIVITY_SEGMENT_WEIGHTS = (22, 33, 34, 9, 2)
HOUR_WEIGHTS = (
    1, 1, 1, 1, 1, 2, 4, 7, 9, 10, 10, 9,
    8, 8, 9, 10, 10, 11, 12, 12, 10, 8, 5, 3,
)

PLATFORMS = ("WEB", "IOS", "ANDROID")
PLATFORM_WEIGHTS = (55, 30, 15)
DEVICE_WEIGHTS = {
    "device_001": 20,
    "device_002": 20,
    "device_003": 10,
    "device_004": 90,
    "device_005": 100,
    "device_006": 10,
    "device_007": 10,
    "device_008": 8,
    "device_009": 12,
    "device_010": 12,
    "device_011": 4,
    "device_012": 4,
}

PROMPT_TOPICS = (
    "SQL joins",
    "star schemas",
    "Python decorators",
    "data quality checks",
    "AWS S3 partitioning",
    "machine learning evaluation",
    "REST API design",
    "database indexing",
    "ETL testing",
    "Docker networking",
    "time-series forecasting",
    "customer churn analysis",
    "Git branching strategies",
    "cloud cost optimization",
    "dimensional modeling",
    "stream processing",
    "query performance",
    "feature engineering",
    "data privacy",
    "monitoring pipelines",
)


def _generation_end(partition_date: date) -> datetime:
    return datetime.combine(partition_date, time(23, 59, 59), timezone.utc)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _latest_source_path(
    entity_name: str,
    partition_date: date,
    source_root: str | Path,
) -> Path:
    source_directory = (
        Path(source_root)
        / "raw"
        / entity_name
        / f"dt={partition_date.isoformat()}"
    )
    candidates = list(source_directory.glob("*.json"))
    if not candidates:
        raise RuntimeError(
            f"No existing {entity_name} source file found in {source_directory}"
        )
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    try:
        return [
            json.loads(line)
            for line in path.read_text().splitlines()
            if line
        ]
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to read source NDJSON from {path}: {exc}") from exc


def load_existing_user_data(
    partition_date: date,
    source_root: str | Path = "data",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], tuple[Path, Path]]:
    """Load and validate the existing Milestone 3 user source extracts."""
    users_path = _latest_source_path("users", partition_date, source_root)
    updates_path = _latest_source_path("user_updates", partition_date, source_root)
    users = _read_ndjson(users_path)
    updates = _read_ndjson(updates_path)
    for user in users:
        user["signup_at"] = _parse_utc(user["signup_at"])
    for update in updates:
        update["updated_at"] = _parse_utc(update["updated_at"])
    validate_users(users, partition_date)
    validate_user_updates(users, updates)
    return users, updates, (users_path, updates_path)


def _terminal_closed_times(
    users: list[dict[str, Any]],
    user_updates: list[dict[str, Any]],
) -> dict[str, datetime]:
    closed_times = {
        user["user_id"]: user["signup_at"]
        for user in users
        if user["account_status"] == "CLOSED"
    }
    for update in user_updates:
        if (
            update["field_name"] == "account_status"
            and update["new_value"] == "CLOSED"
        ):
            closed_times.setdefault(update["user_id"], update["updated_at"])
    return closed_times


def _conversation_count() -> int:
    segment = random.choices(
        ACTIVITY_SEGMENTS,
        weights=ACTIVITY_SEGMENT_WEIGHTS,
        k=1,
    )[0]
    if segment == "INACTIVE":
        return 0
    if segment == "LOW":
        return random.choices((1, 2), weights=(65, 35), k=1)[0]
    if segment == "NORMAL":
        return random.choices(
            tuple(range(3, 11)),
            weights=(15, 18, 18, 16, 13, 10, 6, 4),
            k=1,
        )[0]
    if segment == "ACTIVE":
        values = tuple(range(11, 31))
        return random.choices(values, weights=tuple(range(30, 10, -1)), k=1)[0]
    return random.randint(31, 90)


def _activity_timestamp(start_at: datetime, end_at: datetime) -> datetime:
    if end_at <= start_at:
        return start_at
    total_days = (end_at.date() - start_at.date()).days
    for _ in range(30):
        day_offset = int(total_days * (random.random() ** 0.85))
        hour = random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
        candidate = datetime.combine(
            start_at.date() + timedelta(days=day_offset),
            time(hour, random.randrange(60), random.randrange(60)),
            timezone.utc,
        )
        if start_at <= candidate <= end_at:
            return candidate
    available_seconds = int((end_at - start_at).total_seconds())
    return start_at + timedelta(seconds=random.randint(0, available_seconds))


def generate_conversations(
    users: list[dict[str, Any]],
    user_updates: list[dict[str, Any]],
    partition_date: date,
) -> list[dict[str, Any]]:
    """Generate long-tail persistent conversation threads for existing users."""
    generation_end = _generation_end(partition_date)
    closed_times = _terminal_closed_times(users, user_updates)
    conversations = []
    for user in users:
        user_id = user["user_id"]
        if user_id in closed_times and closed_times[user_id] <= user["signup_at"]:
            continue
        activity_end = generation_end
        if user_id in closed_times:
            activity_end = min(
                activity_end,
                closed_times[user_id] - timedelta(seconds=1),
            )
        if activity_end < user["signup_at"]:
            continue

        for _ in range(_conversation_count()):
            conversations.append(
                {
                    "conversation_id": generate_uuid(),
                    "user_id": user_id,
                    "created_at": _activity_timestamp(
                        user["signup_at"],
                        activity_end,
                    ),
                }
            )

    validate_conversations(users, user_updates, conversations, partition_date)
    return conversations


def _message_count() -> int:
    length_group = random.choices(
        ("SHORT", "TYPICAL", "LONG", "VERY_LONG"),
        weights=(38, 44, 16, 2),
        k=1,
    )[0]
    if length_group == "SHORT":
        return random.choices((1, 2, 3), weights=(35, 40, 25), k=1)[0]
    if length_group == "TYPICAL":
        return random.choices(
            (4, 5, 6, 7, 8),
            weights=(15, 25, 25, 20, 15),
            k=1,
        )[0]
    if length_group == "LONG":
        return random.randint(9, 20)
    return random.randint(21, 35)


def _message_gap_seconds() -> int:
    gap_group = random.choices(
        ("SHORT", "SAME_DAY", "MULTI_DAY", "LONG_RETURN"),
        weights=(88, 8, 3.5, 0.5),
        k=1,
    )[0]
    if gap_group == "SHORT":
        return random.randint(30, 1_800)
    if gap_group == "SAME_DAY":
        return random.randint(1_801, 21_600)
    if gap_group == "MULTI_DAY":
        return random.randint(86_400, 7 * 86_400)
    return random.randint(8 * 86_400, 30 * 86_400)


def _message_timestamps(
    conversation_start: datetime,
    activity_end: datetime,
    requested_count: int,
) -> list[datetime]:
    available_seconds = max(
        0,
        int((activity_end - conversation_start).total_seconds()),
    )
    message_count = min(requested_count, available_seconds + 1)
    timestamps = []
    current = conversation_start
    for sequence_index in range(message_count):
        remaining_messages = message_count - sequence_index - 1
        latest_timestamp = activity_end - timedelta(seconds=remaining_messages)
        max_delay = max(0, int((latest_timestamp - current).total_seconds()))
        if sequence_index == 0:
            delay = min(random.randint(0, 300), max_delay)
        else:
            delay = min(_message_gap_seconds(), max_delay)
            delay = max(1, delay)
        current += timedelta(seconds=delay)
        timestamps.append(current)
    return timestamps


def _choose_device(
    devices_by_platform: dict[str, list[dict[str, Any]]],
) -> str:
    platform = random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS, k=1)[0]
    candidates = devices_by_platform[platform]
    return random.choices(
        [device["device_id"] for device in candidates],
        weights=[DEVICE_WEIGHTS[device["device_id"]] for device in candidates],
        k=1,
    )[0]


def _prompt_text() -> str:
    topic = random.choice(PROMPT_TOPICS)
    second_topic = random.choice(PROMPT_TOPICS)
    prompt_group = random.choices(
        ("SHORT", "MEDIUM", "LONG"),
        weights=(30, 60, 10),
        k=1,
    )[0]
    if prompt_group == "SHORT":
        template = random.choice(
            (
                "Explain {topic}.",
                "Give me an example of {topic}.",
                "What are the basics of {topic}?",
                "Help me understand {topic}.",
            )
        )
        return template.format(topic=topic)
    if prompt_group == "MEDIUM":
        template = random.choice(
            (
                "Can you explain {topic} and show a practical example?",
                "Help me compare {topic} with {second_topic} for a production project.",
                "What are the main tradeoffs when using {topic} in a real system?",
                "Create a step-by-step approach for learning {topic} effectively.",
            )
        )
        return template.format(topic=topic, second_topic=second_topic)
    return (
        f"I'm working on a project involving {topic}. Explain how it relates to "
        f"{second_topic}, include a practical implementation approach, and point "
        "out common mistakes or operational risks I should consider."
    )


def generate_messages(
    users: list[dict[str, Any]],
    user_updates: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
    devices: list[dict[str, Any]],
    partition_date: date,
) -> list[dict[str, Any]]:
    """Generate ordered user prompts with device persistence and thread returns."""
    generation_end = _generation_end(partition_date)
    closed_times = _terminal_closed_times(users, user_updates)
    devices_by_platform = {
        platform: [
            device for device in devices if device["app_platform"] == platform
        ]
        for platform in PLATFORMS
    }

    messages = []
    for conversation in conversations:
        activity_end = generation_end
        closed_at = closed_times.get(conversation["user_id"])
        if closed_at is not None:
            activity_end = min(activity_end, closed_at - timedelta(seconds=1))
        timestamps = _message_timestamps(
            conversation["created_at"],
            activity_end,
            _message_count(),
        )
        previous_device_id = _choose_device(devices_by_platform)
        for sequence_number, created_at in enumerate(timestamps, start=1):
            if sequence_number > 1 and random.random() < 0.12:
                previous_device_id = _choose_device(devices_by_platform)
            device_id = (
                None if random.random() < 0.0075 else previous_device_id
            )
            messages.append(
                {
                    "message_id": generate_uuid(),
                    "conversation_id": conversation["conversation_id"],
                    "user_id": conversation["user_id"],
                    "device_id": device_id,
                    "sequence_number": sequence_number,
                    "message_text": _prompt_text(),
                    "created_at": created_at,
                }
            )

    validate_messages(
        users,
        user_updates,
        conversations,
        messages,
        devices,
        partition_date,
    )
    return messages


def validate_conversations(
    users: list[dict[str, Any]],
    user_updates: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
    partition_date: date,
) -> None:
    """Validate conversation schema, references, timing, and long-tail activity."""
    if not CONVERSATION_MIN_COUNT <= len(conversations) <= CONVERSATION_MAX_COUNT:
        raise ValueError(
            "conversations validation failed: expected 45,000-55,000 records, "
            f"received {len(conversations)}"
        )
    users_by_id = {user["user_id"]: user for user in users}
    closed_times = _terminal_closed_times(users, user_updates)
    conversation_ids = set()
    generation_end = _generation_end(partition_date)
    for conversation in conversations:
        if (
            set(conversation) != CONVERSATION_FIELDS
            or WAREHOUSE_FIELDS.intersection(conversation)
        ):
            raise ValueError("conversations validation failed: invalid schema")
        conversation_id = conversation.get("conversation_id")
        if not conversation_id or conversation_id in conversation_ids:
            raise ValueError(
                "conversations validation failed: duplicate conversation_id "
                f"{conversation_id}"
            )
        conversation_ids.add(conversation_id)
        user = users_by_id.get(conversation.get("user_id"))
        if user is None:
            raise ValueError(
                "conversations validation failed: unknown user_id for "
                f"{conversation_id}"
            )
        created_at = conversation.get("created_at")
        if (
            not isinstance(created_at, datetime)
            or created_at.tzinfo is None
            or not user["signup_at"] <= created_at <= generation_end
        ):
            raise ValueError(
                "conversations validation failed: invalid created_at for "
                f"{conversation_id}"
            )
        closed_at = closed_times.get(user["user_id"])
        if closed_at is not None and created_at >= closed_at:
            raise ValueError(
                "conversations validation failed: conversation after CLOSED for "
                f"{conversation_id}"
            )

    counts = Counter(conversation["user_id"] for conversation in conversations)
    zero_count = len(users) - len(counts)
    sorted_counts = sorted(counts.values(), reverse=True)
    top_five_percent_count = max(1, int(len(users) * 0.05))
    top_share = sum(sorted_counts[:top_five_percent_count]) / len(conversations)
    if zero_count < len(users) * 0.15 or max(sorted_counts) < 31 or top_share < 0.20:
        raise ValueError(
            "conversations validation failed: user activity is not sufficiently "
            "long-tailed"
        )


def validate_messages(
    users: list[dict[str, Any]],
    user_updates: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    devices: list[dict[str, Any]],
    partition_date: date,
) -> None:
    """Validate message integrity, ordering, timing, text, and distributions."""
    if not MESSAGE_MIN_COUNT <= len(messages) <= MESSAGE_MAX_COUNT:
        raise ValueError(
            "messages validation failed: expected 250,000-350,000 records, "
            f"received {len(messages)}"
        )
    conversations_by_id = {
        conversation["conversation_id"]: conversation
        for conversation in conversations
    }
    valid_device_ids = {device["device_id"] for device in devices}
    device_platforms = {
        device["device_id"]: device["app_platform"] for device in devices
    }
    closed_times = _terminal_closed_times(users, user_updates)
    generation_end = _generation_end(partition_date)
    message_ids = set()
    messages_by_conversation: dict[str, list[dict[str, Any]]] = {}
    platform_counts = Counter()

    for message in messages:
        if set(message) != MESSAGE_FIELDS or WAREHOUSE_FIELDS.intersection(message):
            raise ValueError("messages validation failed: invalid schema")
        message_id = message.get("message_id")
        if not message_id or message_id in message_ids:
            raise ValueError(
                f"messages validation failed: duplicate message_id {message_id}"
            )
        message_ids.add(message_id)
        conversation = conversations_by_id.get(message.get("conversation_id"))
        if conversation is None:
            raise ValueError(
                f"messages validation failed: unknown conversation for {message_id}"
            )
        if message.get("user_id") != conversation["user_id"]:
            raise ValueError(
                f"messages validation failed: user mismatch for {message_id}"
            )
        device_id = message.get("device_id")
        if device_id is not None and device_id not in valid_device_ids:
            raise ValueError(
                f"messages validation failed: invalid device_id for {message_id}"
            )
        platform_counts[
            "NULL" if device_id is None else device_platforms[device_id]
        ] += 1
        message_text = message.get("message_text")
        if not isinstance(message_text, str) or not message_text.strip():
            raise ValueError(
                f"messages validation failed: empty message_text for {message_id}"
            )
        created_at = message.get("created_at")
        closed_at = closed_times.get(message["user_id"])
        if (
            not isinstance(created_at, datetime)
            or created_at.tzinfo is None
            or created_at < conversation["created_at"]
            or created_at > generation_end
            or (closed_at is not None and created_at >= closed_at)
        ):
            raise ValueError(
                f"messages validation failed: invalid created_at for {message_id}"
            )
        messages_by_conversation.setdefault(
            conversation["conversation_id"], []
        ).append(message)

    multi_day_count = 0
    for conversation_id, conversation_messages in messages_by_conversation.items():
        expected_sequence = 1
        previous_timestamp = None
        calendar_days = set()
        for message in conversation_messages:
            if message["sequence_number"] != expected_sequence:
                raise ValueError(
                    "messages validation failed: non-contiguous sequence for "
                    f"{conversation_id}"
                )
            if (
                previous_timestamp is not None
                and message["created_at"] <= previous_timestamp
            ):
                raise ValueError(
                    "messages validation failed: timestamps not increasing for "
                    f"{conversation_id}"
                )
            calendar_days.add(message["created_at"].date())
            previous_timestamp = message["created_at"]
            expected_sequence += 1
        if len(calendar_days) > 1:
            multi_day_count += 1

    if len(messages_by_conversation) != len(conversations):
        raise ValueError(
            "messages validation failed: every conversation must have a message"
        )
    if multi_day_count == 0:
        raise ValueError(
            "messages validation failed: no conversations span multiple days"
        )

    total = len(messages)
    null_rate = platform_counts["NULL"] / total
    if not 0.005 <= null_rate <= 0.01:
        raise ValueError(
            f"messages validation failed: NULL device rate {null_rate:.3f} "
            "outside expected range"
        )
    tracked_total = total - platform_counts["NULL"]
    for platform, bounds in {
        "WEB": (0.50, 0.60),
        "IOS": (0.25, 0.35),
        "ANDROID": (0.10, 0.20),
    }.items():
        ratio = platform_counts[platform] / tracked_total
        if not bounds[0] <= ratio <= bounds[1]:
            raise ValueError(
                "messages validation failed: platform distribution outside "
                f"expected range for {platform}: {ratio:.3f}"
            )


def summarize_activity(
    users: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    devices: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return concise conversation/message distribution metrics."""
    conversations_per_user = Counter(
        conversation["user_id"] for conversation in conversations
    )
    user_buckets = {
        "0": len(users) - len(conversations_per_user),
        "1-2": sum(1 <= count <= 2 for count in conversations_per_user.values()),
        "3-10": sum(3 <= count <= 10 for count in conversations_per_user.values()),
        "11-30": sum(11 <= count <= 30 for count in conversations_per_user.values()),
        "30+": sum(count > 30 for count in conversations_per_user.values()),
    }

    messages_per_conversation = Counter(
        message["conversation_id"] for message in messages
    )
    message_counts = sorted(messages_per_conversation.values())
    p95_index = max(0, int(0.95 * len(message_counts)) - 1)
    conversation_dates: dict[str, set[date]] = {}
    device_platforms = {
        device["device_id"]: device["app_platform"] for device in devices
    }
    platform_distribution = Counter()
    for message in messages:
        conversation_dates.setdefault(message["conversation_id"], set()).add(
            message["created_at"].date()
        )
        platform_distribution[
            "NULL"
            if message["device_id"] is None
            else device_platforms[message["device_id"]]
        ] += 1

    return {
        "users_available": len(users),
        "user_conversation_buckets": user_buckets,
        "conversation_count": len(conversations),
        "message_count": len(messages),
        "average_messages": statistics.mean(message_counts),
        "median_messages": statistics.median(message_counts),
        "p95_messages": message_counts[p95_index],
        "max_messages": max(message_counts),
        "multi_day_conversations": sum(
            len(calendar_days) > 1
            for calendar_days in conversation_dates.values()
        ),
        "platform_distribution": platform_distribution,
    }
