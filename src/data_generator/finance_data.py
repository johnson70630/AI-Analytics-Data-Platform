"""Causal subscription, purchase, and payment source-event generation."""

import calendar
import json
import random
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from .inference_data import MetricHistogram
from .ingestion import (
    ingestion_timestamp,
    iter_local_partitioned_records,
    iter_s3_partitioned_records,
    remove_local_event_outputs,
)
from .reference_data import validate_subscription_plans
from .utils import generate_uuid
from .writers import PartitionedNDJSONWriter


PLAN_VERSION_CUTOVER = datetime(2026, 2, 1, tzinfo=timezone.utc)
SUBSCRIPTION_STATUSES = ("ACTIVE", "CANCELLED", "EXPIRED")
PURCHASE_TYPES = ("NEW_SUBSCRIPTION", "RENEWAL", "UPGRADE")
PURCHASE_STATUSES = ("COMPLETED", "FAILED", "REFUNDED", "PENDING")
PAYMENT_STATUSES = ("SUCCESS", "FAILED", "REFUNDED")
PAYMENT_METHODS = ("CARD", "PAYPAL", "APPLE_PAY", "GOOGLE_PAY")
PAYMENT_METHOD_WEIGHTS = (76, 12, 7, 5)

SUBSCRIPTION_FIELDS = {
    "subscription_id",
    "user_id",
    "plan_id",
    "started_at",
    "ended_at",
    "subscription_status",
    "actual_monthly_price",
    "updated_at",
    "ingested_at",
}
PURCHASE_FIELDS = {
    "purchase_id",
    "user_id",
    "subscription_id",
    "plan_id",
    "purchase_type",
    "purchase_status",
    "subtotal_amount",
    "discount_amount",
    "tax_amount",
    "total_amount",
    "purchase_created_at",
    "updated_at",
    "ingested_at",
}
PAYMENT_FIELDS = {
    "payment_id",
    "purchase_id",
    "user_id",
    "payment_status",
    "payment_method",
    "payment_amount",
    "refund_amount",
    "processed_at",
    "refunded_at",
    "updated_at",
    "ingested_at",
}

TAX_RATES = {
    "US": Decimal("0.07"),
    "CA": Decimal("0.05"),
    "GB": Decimal("0.08"),
    "IN": Decimal("0.06"),
    "DE": Decimal("0.08"),
    "AU": Decimal("0.07"),
    "JP": Decimal("0.06"),
}


def _generation_end(partition_date: date) -> datetime:
    return datetime.combine(partition_date, time(23, 59, 59), timezone.utc)


def _money(value: Decimal | float | int | str) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _money_float(value: Decimal | float | int | str) -> float:
    return float(_money(value))


def add_month(timestamp: datetime) -> datetime:
    """Add one calendar month while preserving UTC time and clamping the day."""
    year = timestamp.year + (timestamp.month // 12)
    month = timestamp.month % 12 + 1
    day = min(timestamp.day, calendar.monthrange(year, month)[1])
    return timestamp.replace(year=year, month=month, day=day)


def load_existing_plans(
    source_root: str | Path,
) -> tuple[list[dict[str, Any]], Path]:
    """Load the newest valid five-row plan snapshot without regenerating it."""
    paths = sorted(
        (Path(source_root) / "raw" / "subscription_plans").glob(
            "dt=*/*.json"
        ),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    if not paths:
        raise RuntimeError("No existing subscription plan data found")
    failures = []
    for path in paths:
        try:
            plans = [
                json.loads(line)
                for line in path.read_text().splitlines()
                if line
            ]
            validate_subscription_plans(plans)
            return plans, path
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            failures.append(f"{path}: {exc}")
    raise RuntimeError("No valid subscription plan snapshot: " + "; ".join(failures))


def load_finance_sources(
    source_root: str | Path,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    Counter[str],
]:
    users = [
        row for row, _ in iter_local_partitioned_records("users", source_root)
    ]
    updates = [
        row
        for row, _ in iter_local_partitioned_records("user_updates", source_root)
    ]
    plans, _ = load_existing_plans(source_root)
    conversation_counts = Counter(
        row["user_id"]
        for row, _ in iter_local_partitioned_records(
            "conversations",
            source_root,
        )
    )
    return users, updates, plans, conversation_counts


def terminal_closed_times(
    users: list[dict[str, Any]],
    updates: list[dict[str, Any]],
) -> dict[str, datetime]:
    closed = {
        user["user_id"]: user["signup_at"]
        for user in users
        if user["account_status"] == "CLOSED"
    }
    for update in updates:
        if (
            update["field_name"] == "account_status"
            and update["new_value"] == "CLOSED"
        ):
            closed.setdefault(update["user_id"], update["updated_at"])
    return closed


def _activity_modifier(conversation_count: int) -> float:
    if conversation_count == 0:
        return 0.72
    if conversation_count <= 3:
        return 0.85
    if conversation_count <= 10:
        return 0.98
    if conversation_count <= 30:
        return 1.10
    return 1.25


def _paid_duration_days() -> int:
    group = random.choices(
        ("SHORT", "MEDIUM", "LONG"),
        weights=(15, 55, 30),
        k=1,
    )[0]
    if group == "SHORT":
        return random.randint(28, 75)
    if group == "MEDIUM":
        return random.randint(90, 240)
    return random.randint(241, 420)


def _plan_sequence(
    conversation_count: int,
) -> list[str]:
    modifier = _activity_modifier(conversation_count)
    ever_plus = random.random() < min(0.30, 0.232 * modifier)
    ever_pro = random.random() < min(0.08, 0.045 * modifier)
    if ever_plus and ever_pro:
        return (
            ["PLUS", "PRO"]
            if random.random() < 0.88
            else ["PRO", "PLUS"]
        )
    if ever_pro:
        return ["PRO"]
    if ever_plus:
        return ["PLUS"]
    return []


def _plan_for_start(
    plan_name: str,
    started_at: datetime,
    plans_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if plan_name == "FREE":
        return plans_by_id["plan_free_v1"]
    version = "v1" if started_at < PLAN_VERSION_CUTOVER else "v2"
    return plans_by_id[f"plan_{plan_name.lower()}_{version}"]


def _actual_price(plan: dict[str, Any]) -> float:
    list_price = _money(plan["monthly_price"])
    if plan["plan_name"] == "FREE":
        return 0.0
    if random.random() < 0.15:
        discount_rate = random.choice(
            (Decimal("0.10"), Decimal("0.15"), Decimal("0.20"))
        )
        return _money_float(list_price * (Decimal("1") - discount_rate))
    return float(list_price)


def _new_subscription(
    user_id: str,
    plan: dict[str, Any],
    started_at: datetime,
    ended_at: datetime | None,
    status: str,
    generation_end: datetime,
) -> dict[str, Any]:
    updated_at = ended_at or started_at
    return {
        "subscription_id": generate_uuid(),
        "user_id": user_id,
        "plan_id": plan["plan_id"],
        "started_at": started_at,
        "ended_at": ended_at,
        "subscription_status": status,
        "actual_monthly_price": _actual_price(plan),
        "updated_at": updated_at,
        "ingested_at": ingestion_timestamp(updated_at, generation_end),
    }


def _split_paid_period(
    user_id: str,
    plan_name: str,
    started_at: datetime,
    ended_at: datetime | None,
    final_status: str,
    generation_end: datetime,
    plans_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    actual_end = ended_at or generation_end
    if started_at < PLAN_VERSION_CUTOVER < actual_end:
        old_plan = _plan_for_start(plan_name, started_at, plans_by_id)
        new_plan = _plan_for_start(
            plan_name,
            PLAN_VERSION_CUTOVER,
            plans_by_id,
        )
        return [
            _new_subscription(
                user_id,
                old_plan,
                started_at,
                PLAN_VERSION_CUTOVER,
                "EXPIRED",
                generation_end,
            ),
            _new_subscription(
                user_id,
                new_plan,
                PLAN_VERSION_CUTOVER,
                ended_at,
                final_status,
                generation_end,
            ),
        ]
    plan = _plan_for_start(plan_name, started_at, plans_by_id)
    return [
        _new_subscription(
            user_id,
            plan,
            started_at,
            ended_at,
            final_status,
            generation_end,
        )
    ]


def generate_subscriptions(
    users: list[dict[str, Any]],
    updates: list[dict[str, Any]],
    plans: list[dict[str, Any]],
    conversation_counts: Counter[str],
    generation_end: datetime,
) -> list[dict[str, Any]]:
    """Generate non-overlapping user subscription lifecycles."""
    plans_by_id = {plan["plan_id"]: plan for plan in plans}
    closed_times = terminal_closed_times(users, updates)
    subscriptions = []
    for user in users:
        user_id = user["user_id"]
        signup_at = user["signup_at"]
        cutoff = min(closed_times.get(user_id, generation_end), generation_end)
        if cutoff <= signup_at:
            subscriptions.append(
                _new_subscription(
                    user_id,
                    plans_by_id["plan_free_v1"],
                    signup_at,
                    signup_at,
                    "CANCELLED",
                    generation_end,
                )
            )
            continue

        available_days = (cutoff - signup_at).days
        sequence = _plan_sequence(conversation_counts[user_id])
        if available_days < 35:
            sequence = []
        if not sequence:
            ended_at = cutoff if user_id in closed_times else None
            subscriptions.append(
                _new_subscription(
                    user_id,
                    plans_by_id["plan_free_v1"],
                    signup_at,
                    ended_at,
                    "CANCELLED" if ended_at else "ACTIVE",
                    generation_end,
                )
            )
            continue

        maximum_delay = min(35, max(7, available_days - 28))
        paid_start = signup_at + timedelta(
            days=random.randint(7, maximum_delay),
            seconds=random.randint(0, 86_399),
        )
        if paid_start >= cutoff:
            subscriptions.append(
                _new_subscription(
                    user_id,
                    plans_by_id["plan_free_v1"],
                    signup_at,
                    cutoff if user_id in closed_times else None,
                    "CANCELLED" if user_id in closed_times else "ACTIVE",
                    generation_end,
                )
            )
            continue

        subscriptions.append(
            _new_subscription(
                user_id,
                plans_by_id["plan_free_v1"],
                signup_at,
                paid_start,
                "EXPIRED",
                generation_end,
            )
        )
        current_start = paid_start
        for index, plan_name in enumerate(sequence):
            is_last = index == len(sequence) - 1
            remaining_days = max(0, (cutoff - current_start).days)
            if not is_last:
                duration_days = min(
                    random.randint(35, 155),
                    max(1, remaining_days - 14),
                )
                period_end = min(
                    cutoff,
                    current_start + timedelta(days=duration_days),
                )
                status = "EXPIRED"
            else:
                stays_active = (
                    user_id not in closed_times and random.random() < 0.80
                )
                if stays_active:
                    period_end = None
                    status = "ACTIVE"
                else:
                    period_end = min(
                        cutoff,
                        current_start + timedelta(days=_paid_duration_days()),
                    )
                    status = "CANCELLED"
                    if period_end >= generation_end and user_id not in closed_times:
                        period_end = None
                        status = "ACTIVE"
            subscriptions.extend(
                _split_paid_period(
                    user_id,
                    plan_name,
                    current_start,
                    period_end,
                    status,
                    generation_end,
                    plans_by_id,
                )
            )
            if period_end is None:
                current_start = generation_end
                break
            current_start = period_end

        if current_start < cutoff:
            free_end = cutoff if user_id in closed_times else None
            subscriptions.append(
                _new_subscription(
                    user_id,
                    plans_by_id["plan_free_v1"],
                    current_start,
                    free_end,
                    "CANCELLED" if free_end else "ACTIVE",
                    generation_end,
                )
            )
    return subscriptions


def _attempt_statuses() -> list[str]:
    outcome = random.random()
    if outcome < 0.93:
        return ["SUCCESS"]
    if outcome < 0.98:
        return ["FAILED", "SUCCESS"]
    if random.random() < 0.50:
        return ["FAILED", "FAILED", "SUCCESS"]
    return ["FAILED", "FAILED"]


def _retry_delay_seconds(attempt_number: int) -> int:
    if attempt_number == 1:
        return random.randint(300, 7_200)
    return random.choices(
        (random.randint(3_600, 21_600), random.randint(21_601, 86_400)),
        weights=(80, 20),
        k=1,
    )[0]


def _purchase_amounts(
    subscription: dict[str, Any],
    plan: dict[str, Any],
    country_code: str,
) -> tuple[float, float, float, float]:
    subtotal = _money(plan["monthly_price"])
    actual = _money(subscription["actual_monthly_price"])
    discount = max(Decimal("0.00"), subtotal - actual)
    taxable = subtotal - discount
    tax_rate = TAX_RATES.get(country_code, Decimal("0.04"))
    tax = (
        Decimal("0.00")
        if random.random() < 0.35
        else _money(taxable * tax_rate)
    )
    total = _money(taxable + tax)
    return tuple(float(value) for value in (subtotal, discount, tax, total))


def build_purchase_and_payments(
    user: dict[str, Any],
    subscription: dict[str, Any],
    plan: dict[str, Any],
    purchase_type: str,
    purchase_created_at: datetime,
    user_cutoff: datetime,
    generation_end: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Create one purchase and its ordered payment-attempt lifecycle."""
    purchase_id = generate_uuid()
    subtotal, discount, tax, total = _purchase_amounts(
        subscription,
        plan,
        user["country_code"],
    )
    method = random.choices(
        PAYMENT_METHODS,
        weights=PAYMENT_METHOD_WEIGHTS,
        k=1,
    )[0]
    cutoff = min(user_cutoff, generation_end)
    statuses = _attempt_statuses()
    processed_times = []
    first_processed = purchase_created_at + timedelta(
        seconds=min(
            random.randint(1, 120),
            max(0, int((cutoff - purchase_created_at).total_seconds())),
        )
    )
    processed_times.append(first_processed)
    for attempt_number in range(1, len(statuses)):
        candidate = processed_times[-1] + timedelta(
            seconds=_retry_delay_seconds(attempt_number)
        )
        if candidate > cutoff:
            statuses = statuses[:attempt_number]
            break
        processed_times.append(candidate)

    successful_index = next(
        (index for index, status in enumerate(statuses) if status == "SUCCESS"),
        None,
    )
    refunded_at = None
    if successful_index is not None and random.random() < 0.02:
        candidate = processed_times[successful_index] + timedelta(
            days=random.randint(1, 14),
            seconds=random.randint(1, 3_600),
        )
        if candidate <= cutoff:
            refunded_at = candidate
            statuses[successful_index] = "REFUNDED"

    payments = []
    for status, processed_at in zip(statuses, processed_times):
        is_refunded = status == "REFUNDED"
        payment_updated_at = refunded_at if is_refunded else processed_at
        payments.append(
            {
                "payment_id": generate_uuid(),
                "purchase_id": purchase_id,
                "user_id": user["user_id"],
                "payment_status": status,
                "payment_method": method,
                "payment_amount": total,
                "refund_amount": total if is_refunded else 0.0,
                "processed_at": processed_at,
                "refunded_at": refunded_at if is_refunded else None,
                "updated_at": payment_updated_at,
                "ingested_at": ingestion_timestamp(
                    payment_updated_at,
                    generation_end,
                ),
            }
        )

    if refunded_at is not None:
        purchase_status = "REFUNDED"
        purchase_updated_at = refunded_at
    elif successful_index is not None:
        purchase_status = "COMPLETED"
        purchase_updated_at = processed_times[successful_index]
    else:
        purchase_status = "FAILED"
        purchase_updated_at = processed_times[-1]
    purchase = {
        "purchase_id": purchase_id,
        "user_id": user["user_id"],
        "subscription_id": subscription["subscription_id"],
        "plan_id": subscription["plan_id"],
        "purchase_type": purchase_type,
        "purchase_status": purchase_status,
        "subtotal_amount": subtotal,
        "discount_amount": discount,
        "tax_amount": tax,
        "total_amount": total,
        "purchase_created_at": purchase_created_at,
        "updated_at": purchase_updated_at,
        "ingested_at": ingestion_timestamp(
            purchase_updated_at,
            generation_end,
        ),
    }
    return purchase, payments


def generate_purchases_and_payments(
    users: list[dict[str, Any]],
    updates: list[dict[str, Any]],
    plans: list[dict[str, Any]],
    subscriptions: list[dict[str, Any]],
    generation_end: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate paid charges and one-or-more attempts from subscriptions."""
    users_by_id = {user["user_id"]: user for user in users}
    plans_by_id = {plan["plan_id"]: plan for plan in plans}
    closed_times = terminal_closed_times(users, updates)
    subscriptions_by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for subscription in subscriptions:
        subscriptions_by_user[subscription["user_id"]].append(subscription)

    purchases = []
    payments = []
    for user_id, user_subscriptions in subscriptions_by_user.items():
        user_subscriptions.sort(key=lambda row: row["started_at"])
        previous_plan_name = None
        for subscription in user_subscriptions:
            plan = plans_by_id[subscription["plan_id"]]
            plan_name = plan["plan_name"]
            if plan_name == "FREE":
                previous_plan_name = "FREE"
                continue
            if previous_plan_name == plan_name:
                first_purchase_type = "RENEWAL"
            elif previous_plan_name == "PLUS" and plan_name == "PRO":
                first_purchase_type = "UPGRADE"
            else:
                first_purchase_type = "NEW_SUBSCRIPTION"
            billing_at = subscription["started_at"]
            subscription_end = subscription["ended_at"] or generation_end
            billing_number = 0
            while billing_at < subscription_end and billing_at <= generation_end:
                purchase_type = (
                    first_purchase_type if billing_number == 0 else "RENEWAL"
                )
                purchase, attempts = build_purchase_and_payments(
                    users_by_id[user_id],
                    subscription,
                    plan,
                    purchase_type,
                    billing_at,
                    closed_times.get(user_id, generation_end),
                    generation_end,
                )
                purchases.append(purchase)
                payments.extend(attempts)
                billing_number += 1
                billing_at = add_month(billing_at)
            previous_plan_name = plan_name
    return purchases, payments


class FinanceIngestionMetrics:
    def __init__(self) -> None:
        self.delays = MetricHistogram()
        self.categories: Counter[str] = Counter()
        self.partitions = {
            "subscriptions": set(),
            "purchases": set(),
            "payments": set(),
        }

    def add(
        self,
        entity_name: str,
        event_at: datetime,
        ingested_at: Any,
        physical_date: date,
        generation_end: datetime,
    ) -> None:
        if (
            not isinstance(ingested_at, datetime)
            or ingested_at < event_at
            or ingested_at > generation_end
            or physical_date != ingested_at.date()
        ):
            raise ValueError(f"{entity_name} has invalid ingestion metadata")
        delay = (ingested_at - event_at).total_seconds()
        self.delays.add(delay)
        self.partitions[entity_name].add(physical_date)
        if delay < 60:
            self.categories["same_minute"] += 1
        if ingested_at.date() == event_at.date():
            self.categories["same_day"] += 1
        elif ingested_at.date() == event_at.date() + timedelta(days=1):
            self.categories["next_day"] += 1
        if delay > 86_400:
            self.categories["over_one_day"] += 1


def _percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, int(len(ordered) * probability) - 1)]


def validate_finance_records(
    users: list[dict[str, Any]],
    updates: list[dict[str, Any]],
    plans: list[dict[str, Any]],
    subscriptions_with_dates: Iterable[tuple[dict[str, Any], date]],
    purchases_with_dates: Iterable[tuple[dict[str, Any], date]],
    payments_with_dates: Iterable[tuple[dict[str, Any], date]],
    generation_end: datetime,
    *,
    production_scale: bool = True,
) -> dict[str, Any]:
    """Validate serialized finance data and return operational summaries."""
    users_by_id = {user["user_id"]: user for user in users}
    plans_by_id = {plan["plan_id"]: plan for plan in plans}
    closed_times = terminal_closed_times(users, updates)
    ingestion = FinanceIngestionMetrics()

    subscriptions = []
    subscription_ids = set()
    subscriptions_by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
    status_counts: Counter[str] = Counter()
    for subscription, physical_date in subscriptions_with_dates:
        if set(subscription) != SUBSCRIPTION_FIELDS:
            raise ValueError("Subscription schema is invalid")
        subscription_id = subscription.get("subscription_id")
        if not subscription_id or subscription_id in subscription_ids:
            raise ValueError(f"Duplicate subscription_id {subscription_id}")
        subscription_ids.add(subscription_id)
        user = users_by_id.get(subscription.get("user_id"))
        plan = plans_by_id.get(subscription.get("plan_id"))
        if user is None or plan is None:
            raise ValueError(f"Subscription {subscription_id} has invalid FK")
        started_at = subscription.get("started_at")
        ended_at = subscription.get("ended_at")
        updated_at = subscription.get("updated_at")
        status = subscription.get("subscription_status")
        if (
            not isinstance(started_at, datetime)
            or not isinstance(updated_at, datetime)
            or started_at < user["signup_at"]
            or updated_at < started_at
            or updated_at > generation_end
            or (ended_at is not None and ended_at < started_at)
            or status not in SUBSCRIPTION_STATUSES
        ):
            raise ValueError(f"Subscription {subscription_id} has invalid timing")
        if (status == "ACTIVE") != (ended_at is None):
            raise ValueError(f"Subscription {subscription_id} has invalid status/end")
        actual_price = _money(subscription.get("actual_monthly_price"))
        if (
            actual_price < 0
            or (plan["plan_name"] == "FREE" and actual_price != 0)
            or (plan["plan_name"] != "FREE" and actual_price <= 0)
            or actual_price > _money(plan["monthly_price"])
        ):
            raise ValueError(f"Subscription {subscription_id} has invalid price")
        if plan["plan_name"] != "FREE":
            expected_version = (
                "v1" if started_at < PLAN_VERSION_CUTOVER else "v2"
            )
            if not plan["plan_id"].endswith(expected_version):
                raise ValueError(
                    f"Subscription {subscription_id} violates plan cutover"
                )
        closed_at = closed_times.get(user["user_id"])
        if closed_at is not None and (
            started_at > closed_at
            or updated_at > closed_at
            or (ended_at is not None and ended_at > closed_at)
        ):
            raise ValueError(f"Subscription {subscription_id} exceeds CLOSED time")
        ingestion.add(
            "subscriptions",
            updated_at,
            subscription.get("ingested_at"),
            physical_date,
            generation_end,
        )
        subscriptions.append(subscription)
        subscriptions_by_user[user["user_id"]].append(subscription)
        status_counts[status] += 1

    if set(subscriptions_by_user) != set(users_by_id):
        raise ValueError("Every user must have a subscription lifecycle")
    for user_id, user_subscriptions in subscriptions_by_user.items():
        ordered = sorted(user_subscriptions, key=lambda row: row["started_at"])
        first_plan = plans_by_id[ordered[0]["plan_id"]]
        if (
            first_plan["plan_name"] != "FREE"
            or ordered[0]["started_at"] != users_by_id[user_id]["signup_at"]
        ):
            raise ValueError(f"User {user_id} does not start on FREE")
        active_count = sum(row["subscription_status"] == "ACTIVE" for row in ordered)
        if active_count > 1:
            raise ValueError(f"User {user_id} has multiple ACTIVE subscriptions")
        for previous, current in zip(ordered, ordered[1:]):
            if (
                previous["ended_at"] is None
                or previous["ended_at"] > current["started_at"]
            ):
                raise ValueError(f"User {user_id} has overlapping subscriptions")

    if production_scale and not 11_000 <= len(subscriptions) <= 15_000:
        raise ValueError(
            f"Subscription count {len(subscriptions)} is outside 11K-15K"
        )

    subscriptions_by_id = {
        subscription["subscription_id"]: subscription
        for subscription in subscriptions
    }
    purchases = []
    purchase_ids = set()
    purchase_status_counts: Counter[str] = Counter()
    purchase_type_counts: Counter[str] = Counter()
    for purchase, physical_date in purchases_with_dates:
        if set(purchase) != PURCHASE_FIELDS:
            raise ValueError("Purchase schema is invalid")
        purchase_id = purchase.get("purchase_id")
        if not purchase_id or purchase_id in purchase_ids:
            raise ValueError(f"Duplicate purchase_id {purchase_id}")
        purchase_ids.add(purchase_id)
        subscription = subscriptions_by_id.get(purchase.get("subscription_id"))
        if subscription is None:
            raise ValueError(f"Purchase {purchase_id} has invalid subscription")
        if (
            purchase.get("user_id") != subscription["user_id"]
            or purchase.get("plan_id") != subscription["plan_id"]
        ):
            raise ValueError(f"Purchase {purchase_id} has cross-lineage FK")
        plan = plans_by_id[subscription["plan_id"]]
        created_at = purchase.get("purchase_created_at")
        updated_at = purchase.get("updated_at")
        subscription_end = subscription["ended_at"] or generation_end
        if (
            plan["plan_name"] == "FREE"
            or purchase.get("purchase_type") not in PURCHASE_TYPES
            or purchase.get("purchase_status") not in PURCHASE_STATUSES
            or not isinstance(created_at, datetime)
            or not isinstance(updated_at, datetime)
            or not subscription["started_at"] <= created_at < subscription_end
            or updated_at < created_at
            or updated_at > generation_end
        ):
            raise ValueError(f"Purchase {purchase_id} has invalid timing/type")
        subtotal = _money(purchase.get("subtotal_amount"))
        discount = _money(purchase.get("discount_amount"))
        tax = _money(purchase.get("tax_amount"))
        total = _money(purchase.get("total_amount"))
        if (
            subtotal < 0
            or discount < 0
            or tax < 0
            or discount > subtotal
            or total < 0
            or total != _money(subtotal - discount + tax)
        ):
            raise ValueError(f"Purchase {purchase_id} has invalid money")
        closed_at = closed_times.get(purchase["user_id"])
        if closed_at is not None and (
            created_at > closed_at or updated_at > closed_at
        ):
            raise ValueError(f"Purchase {purchase_id} exceeds CLOSED time")
        ingestion.add(
            "purchases",
            updated_at,
            purchase.get("ingested_at"),
            physical_date,
            generation_end,
        )
        purchases.append(purchase)
        purchase_status_counts[purchase["purchase_status"]] += 1
        purchase_type_counts[purchase["purchase_type"]] += 1

    if production_scale and not 11_500 <= len(purchases) <= 25_000:
        raise ValueError(
            f"Purchase count {len(purchases)} is not roughly within 12K-25K"
        )

    purchases_by_id = {purchase["purchase_id"]: purchase for purchase in purchases}
    payments = []
    payment_ids = set()
    payment_status_counts: Counter[str] = Counter()
    payment_method_counts: Counter[str] = Counter()
    payments_by_purchase: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for payment, physical_date in payments_with_dates:
        if set(payment) != PAYMENT_FIELDS:
            raise ValueError("Payment schema is invalid")
        payment_id = payment.get("payment_id")
        if not payment_id or payment_id in payment_ids:
            raise ValueError(f"Duplicate payment_id {payment_id}")
        payment_ids.add(payment_id)
        purchase = purchases_by_id.get(payment.get("purchase_id"))
        if purchase is None or payment.get("user_id") != purchase["user_id"]:
            raise ValueError(f"Payment {payment_id} has invalid purchase/user")
        processed_at = payment.get("processed_at")
        refunded_at = payment.get("refunded_at")
        updated_at = payment.get("updated_at")
        status = payment.get("payment_status")
        if (
            status not in PAYMENT_STATUSES
            or payment.get("payment_method") not in PAYMENT_METHODS
            or not isinstance(processed_at, datetime)
            or not isinstance(updated_at, datetime)
            or processed_at < purchase["purchase_created_at"]
            or updated_at < processed_at
            or updated_at > generation_end
            or _money(payment.get("payment_amount"))
            != _money(purchase["total_amount"])
        ):
            raise ValueError(f"Payment {payment_id} has invalid fields/timing")
        refund_amount = _money(payment.get("refund_amount"))
        if status == "REFUNDED":
            if (
                not isinstance(refunded_at, datetime)
                or refunded_at <= processed_at
                or updated_at < refunded_at
                or refund_amount <= 0
                or refund_amount > _money(payment["payment_amount"])
            ):
                raise ValueError(f"Payment {payment_id} has invalid refund")
        elif refunded_at is not None or refund_amount != 0:
            raise ValueError(f"Payment {payment_id} has unexpected refund")
        closed_at = closed_times.get(payment["user_id"])
        if closed_at is not None and updated_at > closed_at:
            raise ValueError(f"Payment {payment_id} exceeds CLOSED time")
        ingestion.add(
            "payments",
            updated_at,
            payment.get("ingested_at"),
            physical_date,
            generation_end,
        )
        payments.append(payment)
        payments_by_purchase[purchase["purchase_id"]].append(payment)
        payment_status_counts[status] += 1
        payment_method_counts[payment["payment_method"]] += 1

    if set(payments_by_purchase) != set(purchases_by_id):
        raise ValueError("Every purchase must have at least one payment attempt")
    attempt_buckets = Counter()
    first_success = successful_after_retry = all_failed = 0
    for purchase_id, attempts in payments_by_purchase.items():
        ordered = sorted(attempts, key=lambda row: row["processed_at"])
        if any(
            current["processed_at"] >= following["processed_at"]
            for current, following in zip(ordered, ordered[1:])
        ):
            raise ValueError(f"Purchase {purchase_id} has unordered retries")
        attempt_count = len(ordered)
        attempt_bucket = (
            "1" if attempt_count == 1 else "2" if attempt_count == 2 else "3+"
        )
        attempt_buckets[attempt_bucket] += 1
        has_success = any(
            row["payment_status"] in {"SUCCESS", "REFUNDED"} for row in ordered
        )
        has_refund = any(row["payment_status"] == "REFUNDED" for row in ordered)
        purchase_status = purchases_by_id[purchase_id]["purchase_status"]
        expected_status = (
            "REFUNDED"
            if has_refund
            else "COMPLETED"
            if has_success
            else "FAILED"
        )
        if purchase_status != expected_status:
            raise ValueError(f"Purchase {purchase_id} has inconsistent final status")
        if ordered[0]["payment_status"] in {"SUCCESS", "REFUNDED"}:
            first_success += 1
        elif has_success:
            successful_after_retry += 1
        else:
            all_failed += 1

    plan_names = {
        plan_id: plan["plan_name"] for plan_id, plan in plans_by_id.items()
    }
    ever_plus = {
        row["user_id"]
        for row in subscriptions
        if plan_names[row["plan_id"]] == "PLUS"
    }
    ever_pro = {
        row["user_id"]
        for row in subscriptions
        if plan_names[row["plan_id"]] == "PRO"
    }
    ever_paid = ever_plus | ever_pro
    if production_scale:
        plus_rate = len(ever_plus) / len(users)
        pro_rate = len(ever_pro) / len(users)
        if not 0.18 <= plus_rate <= 0.24:
            raise ValueError(
                f"PLUS lifetime adoption {plus_rate:.2%} is outside 18%-24%"
            )
        if not 0.03 <= pro_rate <= 0.06:
            raise ValueError(
                f"PRO lifetime adoption {pro_rate:.2%} is outside 3%-6%"
            )
    current_plans = Counter(
        plan_names[row["plan_id"]]
        for row in subscriptions
        if row["subscription_status"] == "ACTIVE"
    )
    transitions = Counter()
    for user_subscriptions in subscriptions_by_user.values():
        ordered = sorted(user_subscriptions, key=lambda row: row["started_at"])
        names = [plan_names[row["plan_id"]] for row in ordered]
        for previous, current in zip(names, names[1:]):
            if previous == current:
                continue
            if previous == "FREE" and current == "PLUS":
                transitions["FREE -> PLUS"] += 1
            elif previous == "FREE" and current == "PRO":
                transitions["FREE -> PRO"] += 1
            elif previous == "PLUS" and current == "PRO":
                transitions["PLUS -> PRO"] += 1
            elif previous in {"PLUS", "PRO"} and current == "FREE":
                transitions["paid -> FREE"] += 1
            else:
                transitions[f"{previous} -> {current}"] += 1

    paid_durations = [
        ((row["ended_at"] or generation_end) - row["started_at"]).total_seconds()
        / 86_400
        for row in subscriptions
        if plan_names[row["plan_id"]] != "FREE"
    ]
    purchase_totals = [float(_money(row["total_amount"])) for row in purchases]
    discounted = sum(_money(row["discount_amount"]) > 0 for row in purchases)
    successful_purchase_amount = sum(
        _money(row["total_amount"])
        for row in purchases
        if row["purchase_status"] in {"COMPLETED", "REFUNDED"}
    )
    refunds = sum(_money(row["refund_amount"]) for row in payments)
    financials = {
        "gross": sum(_money(row["subtotal_amount"]) for row in purchases),
        "discounts": sum(_money(row["discount_amount"]) for row in purchases),
        "tax": sum(_money(row["tax_amount"]) for row in purchases),
        "completed": successful_purchase_amount,
        "refunds": refunds,
        "net_collected": successful_purchase_amount - refunds,
    }
    partition_report = {
        entity_name: {
            "count": len(partition_dates),
            "earliest": min(partition_dates),
            "latest": max(partition_dates),
        }
        for entity_name, partition_dates in ingestion.partitions.items()
    }
    if any(report["count"] < 2 for report in partition_report.values()):
        raise ValueError("Finance outputs do not span multiple partitions")
    if production_scale and ingestion.categories["over_one_day"] == 0:
        raise ValueError("Finance outputs have no genuinely late arrivals")

    return {
        "users": len(users),
        "subscriptions": len(subscriptions),
        "ever_paid": len(ever_paid),
        "never_paid": len(users) - len(ever_paid),
        "ever_plus": len(ever_plus),
        "ever_pro": len(ever_pro),
        "current_plans": current_plans,
        "subscription_statuses": status_counts,
        "transitions": transitions,
        "paid_duration": {
            "average": statistics.mean(paid_durations),
            "median": statistics.median(paid_durations),
            "p95": _percentile(paid_durations, 0.95),
        },
        "purchases": len(purchases),
        "purchase_types": purchase_type_counts,
        "purchase_statuses": purchase_status_counts,
        "purchase_amounts": {
            "average": statistics.mean(purchase_totals),
            "median": statistics.median(purchase_totals),
            "p95": _percentile(purchase_totals, 0.95),
            "max": max(purchase_totals),
        },
        "discounted_percentage": discounted / len(purchases) * 100,
        "payments": len(payments),
        "payment_statuses": payment_status_counts,
        "payment_methods": payment_method_counts,
        "attempt_buckets": attempt_buckets,
        "first_attempt_success_percentage": first_success / len(purchases) * 100,
        "successful_after_retry": successful_after_retry,
        "all_failed": all_failed,
        "refunded_purchases": purchase_status_counts["REFUNDED"],
        "refund_rate": purchase_status_counts["REFUNDED"] / len(purchases) * 100,
        "financials": financials,
        "partitions": partition_report,
        "ingestion_delay": ingestion.delays.summary(),
        "arrival_categories": ingestion.categories,
        "duplicate_fk_lineage": "PASS",
        "monetary_reconciliation": "PASS",
    }


def _write_finance_entity(
    entity_name: str,
    records: list[dict[str, Any]],
    output_root: str | Path,
) -> list[Path]:
    with PartitionedNDJSONWriter(output_root, entity_name) as writer:
        for record in records:
            writer.write(record, record["ingested_at"].date())
    paths = sorted(writer.paths.values())
    print(
        f"Wrote {len(records):,} {entity_name} records across "
        f"{len(paths)} local daily partitions"
    )
    return paths


def generate_finance_data_locally(
    partition_date: date,
    output_root: str | Path,
) -> dict[str, int]:
    """Generate only finance events from existing serialized source data."""
    users, updates, plans, conversation_counts = load_finance_sources(
        output_root
    )
    generation_end = _generation_end(partition_date)
    subscriptions = generate_subscriptions(
        users,
        updates,
        plans,
        conversation_counts,
        generation_end,
    )
    purchases, payments = generate_purchases_and_payments(
        users,
        updates,
        plans,
        subscriptions,
        generation_end,
    )
    removed = remove_local_event_outputs(
        ("subscriptions", "purchases", "payments"),
        output_root,
    )
    print(f"Removed {len(removed)} obsolete local finance files")
    for entity_name, records in (
        ("subscriptions", subscriptions),
        ("purchases", purchases),
        ("payments", payments),
    ):
        _write_finance_entity(entity_name, records, output_root)
    return {
        "subscriptions": len(subscriptions),
        "purchases": len(purchases),
        "payments": len(payments),
    }


def validate_local_finance_data(
    partition_date: date,
    source_root: str | Path,
) -> dict[str, Any]:
    """Reload and validate all local serialized finance NDJSON."""
    users, updates, plans, _ = load_finance_sources(source_root)
    return validate_finance_records(
        users,
        updates,
        plans,
        iter_local_partitioned_records("subscriptions", source_root),
        iter_local_partitioned_records("purchases", source_root),
        iter_local_partitioned_records("payments", source_root),
        _generation_end(partition_date),
    )


def validate_s3_finance_data(
    client: Any,
    bucket: str,
    partition_date: date,
    source_root: str | Path,
) -> dict[str, Any]:
    """Stream finance NDJSON from S3 and run complete validation."""
    users, updates, plans, _ = load_finance_sources(source_root)
    return validate_finance_records(
        users,
        updates,
        plans,
        iter_s3_partitioned_records(client, bucket, "subscriptions"),
        iter_s3_partitioned_records(client, bucket, "purchases"),
        iter_s3_partitioned_records(client, bucket, "payments"),
        _generation_end(partition_date),
    )


def print_finance_summary(summary: dict[str, Any]) -> None:
    """Print the Milestone 7 operational validation report."""
    users = summary["users"]
    print("\nUsers:")
    for label, key in (
        ("total", "users"),
        ("ever paid", "ever_paid"),
        ("never paid", "never_paid"),
        ("ever PLUS", "ever_plus"),
        ("ever PRO", "ever_pro"),
    ):
        print(f"{label}: {summary[key]:,}")

    print("\nCurrent plan distribution:")
    for plan_name in ("FREE", "PLUS", "PRO"):
        count = summary["current_plans"][plan_name]
        print(f"{plan_name}: {count:,} ({count / users:.2%})")

    print(f"\nSubscription periods: {summary['subscriptions']:,}")
    for status in SUBSCRIPTION_STATUSES:
        print(f"{status}: {summary['subscription_statuses'][status]:,}")
    print("\nTransitions:")
    for transition in (
        "FREE -> PLUS",
        "FREE -> PRO",
        "PLUS -> PRO",
        "PRO -> PLUS",
        "paid -> FREE",
    ):
        print(f"{transition}: {summary['transitions'][transition]:,}")
    duration = summary["paid_duration"]
    print("\nPaid duration (days):")
    print(f"average: {duration['average']:.2f}")
    print(f"median: {duration['median']:.2f}")
    print(f"p95: {duration['p95']:.2f}")

    print(f"\nPurchases: {summary['purchases']:,}")
    for purchase_type in PURCHASE_TYPES:
        print(f"{purchase_type}: {summary['purchase_types'][purchase_type]:,}")
    print("Purchase statuses:")
    for status in PURCHASE_STATUSES:
        print(f"{status}: {summary['purchase_statuses'][status]:,}")
    amounts = summary["purchase_amounts"]
    print("Purchase amounts:")
    for metric in ("average", "median", "p95", "max"):
        print(f"{metric}: ${amounts[metric]:,.2f}")
    print(f"discounted purchases: {summary['discounted_percentage']:.2f}%")

    print(f"\nPayment attempts: {summary['payments']:,}")
    for status in PAYMENT_STATUSES:
        print(f"{status}: {summary['payment_statuses'][status]:,}")
    print("Payment methods:")
    for method in PAYMENT_METHODS:
        count = summary["payment_methods"][method]
        print(f"{method}: {count:,} ({count / summary['payments']:.2%})")
    print("Retry behavior:")
    for bucket in ("1", "2", "3+"):
        print(f"{bucket} attempt(s): {summary['attempt_buckets'][bucket]:,}")
    print(
        "first-attempt success: "
        f"{summary['first_attempt_success_percentage']:.2f}%"
    )
    print(f"successful after retry: {summary['successful_after_retry']:,}")
    print(f"all attempts failed: {summary['all_failed']:,}")
    print(f"refunded purchases: {summary['refunded_purchases']:,}")
    print(f"refund rate: {summary['refund_rate']:.2f}%")

    print("\nFinancial reconciliation:")
    for label, key in (
        ("gross purchase amount", "gross"),
        ("discounts", "discounts"),
        ("tax", "tax"),
        ("completed amount", "completed"),
        ("refunds", "refunds"),
        ("net collected", "net_collected"),
    ):
        print(f"{label}: ${summary['financials'][key]:,.2f}")

    print("\nDaily ingestion partitions:")
    for entity_name, report in summary["partitions"].items():
        print(
            f"{entity_name}: {report['count']} "
            f"({report['earliest']} to {report['latest']})"
        )
    print("Combined ingestion delays (seconds):")
    for label, metric in (
        ("median", "median"),
        ("p95", "p95"),
        ("p99", "p99"),
        ("maximum", "max"),
    ):
        print(f"{label}: {summary['ingestion_delay'][metric]:,.0f}")
    arrivals = summary["arrival_categories"]
    print("Arrival categories:")
    print(f"same-minute: {arrivals['same_minute']:,}")
    print(f"same-day: {arrivals['same_day']:,}")
    print(f"next-day: {arrivals['next_day']:,}")
    print(f">1 day: {arrivals['over_one_day']:,}")
