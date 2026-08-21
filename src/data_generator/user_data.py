"""Realistic operational users and user change-event generation."""

import random
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from faker import Faker

from .utils import generate_uuid


USER_COUNT = 10_000
USER_UPDATE_COUNT = 1_750
UPDATED_USER_COUNT = 1_600
MULTI_UPDATE_USER_COUNT = 150

PRIMARY_COUNTRIES = ("US", "CA", "GB", "IN", "DE", "AU", "JP", "OTHER")
COUNTRY_WEIGHTS = (55, 8, 7, 7, 5, 4, 4, 10)
OTHER_COUNTRIES = ("BR", "FR", "MX", "NL", "SG", "KR", "ES", "IT")
VALID_COUNTRIES = set(PRIMARY_COUNTRIES[:-1]) | set(OTHER_COUNTRIES)

ACCOUNT_STATUSES = ("ACTIVE", "SUSPENDED", "CLOSED")
ACCOUNT_STATUS_WEIGHTS = (94, 4, 2)
SIGNUP_SOURCES = ("ORGANIC", "PAID_SEARCH", "REFERRAL", "SOCIAL", "OTHER")
SIGNUP_SOURCE_WEIGHTS = (45, 25, 15, 10, 5)
UPDATE_FIELDS = ("country_code", "account_status", "name", "email")
UPDATE_FIELD_WEIGHTS = {
    "country_code": 35,
    "account_status": 30,
    "name": 20,
    "email": 15,
}
VALID_ACCOUNT_TRANSITIONS = {
    ("ACTIVE", "SUSPENDED"),
    ("ACTIVE", "CLOSED"),
    ("SUSPENDED", "ACTIVE"),
    ("SUSPENDED", "CLOSED"),
}

USER_FIELDS = {
    "user_id",
    "email",
    "name",
    "country_code",
    "account_status",
    "signup_source",
    "signup_at",
}
USER_UPDATE_FIELDS = {
    "update_id",
    "user_id",
    "field_name",
    "old_value",
    "new_value",
    "updated_at",
}
WAREHOUSE_FIELDS = {
    "user_key",
    "date_key",
    "effective_start_date",
    "effective_end_date",
    "current_flag",
}
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
COUNTRY_PATTERN = re.compile(r"^[A-Z]{2}$")


def _generation_end(partition_date: date) -> datetime:
    return datetime.combine(partition_date, time(23, 59, 59), timezone.utc)


def _weighted_country() -> str:
    country = random.choices(PRIMARY_COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
    if country == "OTHER":
        return random.choice(OTHER_COUNTRIES)
    return country


def _signup_timestamp(partition_date: date) -> datetime:
    end_at = _generation_end(partition_date)
    window_seconds = 365 * 24 * 60 * 60
    # This curve modestly favors recent signups without imposing rigid seasonality.
    seconds_ago = int(window_seconds * (random.random() ** 1.3))
    return end_at - timedelta(seconds=seconds_ago)


def generate_users(fake: Faker, partition_date: date) -> list[dict[str, Any]]:
    """Generate exactly 10,000 deterministic, source-style initial user records."""
    users = []
    for _ in range(USER_COUNT):
        users.append(
            {
                "user_id": generate_uuid(),
                "email": fake.unique.email(),
                "name": None if random.random() < 0.04 else fake.name(),
                "country_code": _weighted_country(),
                "account_status": random.choices(
                    ACCOUNT_STATUSES,
                    weights=ACCOUNT_STATUS_WEIGHTS,
                    k=1,
                )[0],
                "signup_source": (
                    None
                    if random.random() < 0.01
                    else random.choices(
                        SIGNUP_SOURCES,
                        weights=SIGNUP_SOURCE_WEIGHTS,
                        k=1,
                    )[0]
                ),
                "signup_at": _signup_timestamp(partition_date),
            }
        )

    validate_users(users, partition_date)
    return users


def _choose_update_field(state: dict[str, Any]) -> str:
    available_fields = [
        field
        for field in UPDATE_FIELDS
        if not (field == "account_status" and state[field] == "CLOSED")
    ]
    return random.choices(
        available_fields,
        weights=[UPDATE_FIELD_WEIGHTS[field] for field in available_fields],
        k=1,
    )[0]


def _new_country(old_value: str) -> str:
    while True:
        new_value = _weighted_country()
        if new_value != old_value:
            return new_value


def _new_account_status(old_value: str) -> str:
    transitions = {
        "ACTIVE": (("SUSPENDED", "CLOSED"), (85, 15)),
        "SUSPENDED": (("ACTIVE", "CLOSED"), (90, 10)),
    }
    values, weights = transitions[old_value]
    return random.choices(values, weights=weights, k=1)[0]


def _new_name(fake: Faker, old_value: str | None) -> str:
    while True:
        new_value = fake.name()
        if new_value != old_value:
            return new_value


def _new_email(fake: Faker, old_value: str) -> str:
    while True:
        new_value = fake.unique.email()
        if new_value != old_value:
            return new_value


def generate_user_updates(
    users: list[dict[str, Any]],
    fake: Faker,
    partition_date: date,
) -> list[dict[str, Any]]:
    """Generate chronological source changes while maintaining evolving state."""
    end_at = _generation_end(partition_date)
    eligible_users = [
        user
        for user in users
        if int((end_at - user["signup_at"]).total_seconds()) >= 2
    ]
    if len(eligible_users) < UPDATED_USER_COUNT:
        raise ValueError(
            "user_updates generation failed: insufficient users with time "
            "available after signup"
        )

    selected_users = random.sample(eligible_users, UPDATED_USER_COUNT)
    state_by_user = {
        user["user_id"]: {
            "country_code": user["country_code"],
            "account_status": user["account_status"],
            "email": user["email"],
            "name": user["name"],
        }
        for user in selected_users
    }

    updates = []
    for user_number, user in enumerate(selected_users):
        event_count = 2 if user_number < MULTI_UPDATE_USER_COUNT else 1
        available_seconds = int((end_at - user["signup_at"]).total_seconds())
        offsets = sorted(
            random.sample(range(1, available_seconds + 1), event_count)
        )
        state = state_by_user[user["user_id"]]

        for offset in offsets:
            field_name = _choose_update_field(state)
            old_value = state[field_name]
            if field_name == "country_code":
                new_value = _new_country(old_value)
            elif field_name == "account_status":
                new_value = _new_account_status(old_value)
            elif field_name == "name":
                new_value = _new_name(fake, old_value)
            else:
                new_value = _new_email(fake, old_value)

            updates.append(
                {
                    "update_id": generate_uuid(),
                    "user_id": user["user_id"],
                    "field_name": field_name,
                    "old_value": old_value,
                    "new_value": new_value,
                    "updated_at": user["signup_at"] + timedelta(seconds=offset),
                }
            )
            state[field_name] = new_value

    if len(updates) != USER_UPDATE_COUNT:
        raise ValueError(
            "user_updates generation failed: expected "
            f"{USER_UPDATE_COUNT} events, generated {len(updates)}"
        )
    validate_user_updates(users, updates)
    return updates


def _ratio(counts: Counter, value: Any, total: int) -> float:
    return counts[value] / total


def _validate_ratio(
    dataset_name: str,
    attribute_name: str,
    value: Any,
    ratio: float,
    minimum: float,
    maximum: float,
) -> None:
    if not minimum <= ratio <= maximum:
        raise ValueError(
            f"{dataset_name} validation failed: {attribute_name}={value} "
            f"ratio {ratio:.3f} outside expected range "
            f"{minimum:.3f}-{maximum:.3f}"
        )


def validate_users(users: list[dict[str, Any]], partition_date: date) -> None:
    """Validate user schema, required values, timestamps, and distributions."""
    if len(users) != USER_COUNT:
        raise ValueError(
            f"users validation failed: expected {USER_COUNT}, received {len(users)}"
        )

    user_ids = set()
    emails = set()
    end_at = _generation_end(partition_date)
    start_at = end_at - timedelta(days=365)
    for user in users:
        if set(user) != USER_FIELDS or WAREHOUSE_FIELDS.intersection(user):
            raise ValueError(
                f"users validation failed: invalid schema for {user.get('user_id')}"
            )
        user_id = user.get("user_id")
        if not user_id or user_id in user_ids:
            raise ValueError(f"users validation failed: duplicate user_id {user_id}")
        user_ids.add(user_id)

        email = user.get("email")
        if not isinstance(email, str) or not EMAIL_PATTERN.fullmatch(email):
            raise ValueError(f"users validation failed: invalid email for {user_id}")
        if email in emails:
            raise ValueError(f"users validation failed: duplicate email {email}")
        emails.add(email)

        country_code = user.get("country_code")
        if (
            not isinstance(country_code, str)
            or not COUNTRY_PATTERN.fullmatch(country_code)
            or country_code not in VALID_COUNTRIES
        ):
            raise ValueError(
                f"users validation failed: invalid country_code for {user_id}"
            )
        if user.get("account_status") not in ACCOUNT_STATUSES:
            raise ValueError(
                f"users validation failed: invalid account_status for {user_id}"
            )
        if user.get("signup_source") not in {*SIGNUP_SOURCES, None}:
            raise ValueError(
                f"users validation failed: invalid signup_source for {user_id}"
            )
        signup_at = user.get("signup_at")
        if (
            not isinstance(signup_at, datetime)
            or signup_at.tzinfo is None
            or not start_at <= signup_at <= end_at
        ):
            raise ValueError(
                f"users validation failed: invalid signup_at for {user_id}"
            )

    countries = Counter(user["country_code"] for user in users)
    country_groups = Counter(
        country if country in PRIMARY_COUNTRIES[:-1] else "OTHER"
        for country in countries.elements()
    )
    country_ranges = {
        "US": (0.50, 0.60),
        "CA": (0.05, 0.11),
        "GB": (0.04, 0.10),
        "IN": (0.04, 0.10),
        "DE": (0.02, 0.08),
        "AU": (0.015, 0.07),
        "JP": (0.015, 0.07),
        "OTHER": (0.07, 0.13),
    }
    for value, (minimum, maximum) in country_ranges.items():
        _validate_ratio(
            "users",
            "country_code",
            value,
            _ratio(country_groups, value, len(users)),
            minimum,
            maximum,
        )

    statuses = Counter(user["account_status"] for user in users)
    for value, bounds in {
        "ACTIVE": (0.91, 0.97),
        "SUSPENDED": (0.02, 0.06),
        "CLOSED": (0.005, 0.04),
    }.items():
        _validate_ratio(
            "users",
            "account_status",
            value,
            _ratio(statuses, value, len(users)),
            *bounds,
        )

    signup_sources = Counter(user["signup_source"] for user in users)
    for value, bounds in {
        "ORGANIC": (0.40, 0.50),
        "PAID_SEARCH": (0.20, 0.30),
        "REFERRAL": (0.11, 0.19),
        "SOCIAL": (0.07, 0.13),
        "OTHER": (0.03, 0.08),
        None: (0.003, 0.02),
    }.items():
        _validate_ratio(
            "users",
            "signup_source",
            value,
            _ratio(signup_sources, value, len(users)),
            *bounds,
        )

    name_null_ratio = sum(user["name"] is None for user in users) / len(users)
    _validate_ratio("users", "name", None, name_null_ratio, 0.02, 0.06)

    recent_cutoff = end_at - timedelta(days=182, hours=12)
    recent_ratio = sum(
        user["signup_at"] >= recent_cutoff for user in users
    ) / len(users)
    _validate_ratio(
        "users",
        "signup_at",
        "recent_half_of_window",
        recent_ratio,
        0.54,
        0.65,
    )


def validate_user_updates(
    users: list[dict[str, Any]],
    updates: list[dict[str, Any]],
) -> None:
    """Validate update referential, temporal, transition, and state integrity."""
    if not 1_500 <= len(updates) <= 2_000:
        raise ValueError(
            "user_updates validation failed: expected 1,500-2,000 events, "
            f"received {len(updates)}"
        )

    users_by_id = {user["user_id"]: user for user in users}
    update_ids = set()
    updates_by_user: dict[str, list[dict[str, Any]]] = {}
    for update in updates:
        if set(update) != USER_UPDATE_FIELDS or WAREHOUSE_FIELDS.intersection(update):
            raise ValueError(
                "user_updates validation failed: invalid schema for "
                f"{update.get('update_id')}"
            )
        update_id = update.get("update_id")
        if not update_id or update_id in update_ids:
            raise ValueError(
                f"user_updates validation failed: duplicate update_id {update_id}"
            )
        update_ids.add(update_id)
        user_id = update.get("user_id")
        if user_id not in users_by_id:
            raise ValueError(
                f"user_updates validation failed: unknown user_id {user_id}"
            )
        if update.get("field_name") not in UPDATE_FIELDS:
            raise ValueError(
                "user_updates validation failed: invalid field_name "
                f"for {update_id}"
            )
        if update.get("old_value") == update.get("new_value"):
            raise ValueError(
                f"user_updates validation failed: unchanged value for {update_id}"
            )
        updates_by_user.setdefault(user_id, []).append(update)

    for user_id, user_updates in updates_by_user.items():
        user = users_by_id[user_id]
        state = {field: user[field] for field in UPDATE_FIELDS}
        previous_timestamp = user["signup_at"]
        for update in user_updates:
            updated_at = update.get("updated_at")
            if (
                not isinstance(updated_at, datetime)
                or updated_at.tzinfo is None
                or updated_at <= previous_timestamp
            ):
                raise ValueError(
                    "user_updates validation failed: timestamps not strictly "
                    f"ordered for user {user_id}"
                )
            field_name = update["field_name"]
            if update["old_value"] != state[field_name]:
                raise ValueError(
                    "user_updates validation failed: old_value does not match "
                    f"prior state for {update['update_id']}"
                )

            old_value = update["old_value"]
            new_value = update["new_value"]
            if field_name == "account_status" and (
                old_value,
                new_value,
            ) not in VALID_ACCOUNT_TRANSITIONS:
                raise ValueError(
                    "user_updates validation failed: invalid account_status "
                    f"transition for {update['update_id']}"
            )
            if field_name == "country_code" and (
                not isinstance(new_value, str)
                or new_value not in VALID_COUNTRIES
                or not COUNTRY_PATTERN.fullmatch(new_value)
            ):
                raise ValueError(
                    "user_updates validation failed: invalid new country for "
                    f"{update['update_id']}"
                )
            if field_name == "email" and (
                not isinstance(new_value, str)
                or not EMAIL_PATTERN.fullmatch(new_value)
            ):
                raise ValueError(
                    "user_updates validation failed: invalid new email for "
                    f"{update['update_id']}"
                )

            state[field_name] = new_value
            previous_timestamp = updated_at

    field_counts = Counter(update["field_name"] for update in updates)
    for value, bounds in {
        "country_code": (0.28, 0.42),
        "account_status": (0.23, 0.37),
        "name": (0.14, 0.26),
        "email": (0.10, 0.21),
    }.items():
        _validate_ratio(
            "user_updates",
            "field_name",
            value,
            _ratio(field_counts, value, len(updates)),
            *bounds,
        )

    updates_per_user = Counter(update["user_id"] for update in updates)
    if len(updates_per_user) >= len(users) or max(updates_per_user.values()) < 2:
        raise ValueError(
            "user_updates validation failed: expected most users to have no "
            "updates and some users to have multiple updates"
        )


def summarize_user_data(
    users: list[dict[str, Any]],
    updates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return concise generation metrics without exposing record contents."""
    country_counts = Counter(user["country_code"] for user in users)
    country_distribution = Counter(
        country if country in PRIMARY_COUNTRIES[:-1] else "OTHER"
        for country in country_counts.elements()
    )
    account_status_distribution = Counter(
        user["account_status"] for user in users
    )
    signup_source_distribution = Counter(user["signup_source"] for user in users)
    update_type_distribution = Counter(
        update["field_name"] for update in updates
    )
    updates_per_user = Counter(update["user_id"] for update in updates)
    return {
        "country_distribution": country_distribution,
        "account_status_distribution": account_status_distribution,
        "signup_source_distribution": signup_source_distribution,
        "update_type_distribution": update_type_distribution,
        "users_with_0_updates": len(users) - len(updates_per_user),
        "users_with_1_update": sum(
            count == 1 for count in updates_per_user.values()
        ),
        "users_with_2_plus_updates": sum(
            count >= 2 for count in updates_per_user.values()
        ),
    }
