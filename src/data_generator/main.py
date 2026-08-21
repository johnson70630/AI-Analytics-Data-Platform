"""CLI entry point for generating operational source data."""

import argparse
from datetime import date, datetime, timezone

from faker import Faker

from .activity_data import (
    generate_conversations,
    generate_messages,
    load_existing_user_data,
    summarize_activity,
)
from .config import (
    DEFAULT_AWS_REGION,
    DEFAULT_LOCAL_ROOT,
    DEFAULT_OUTPUT_MODE,
    DEFAULT_RANDOM_SEED,
    load_config,
)
from .reference_data import (
    generate_devices,
    generate_models,
    generate_subscription_plans,
)
from .utils import add_random_delay, generate_uuid, initialize_randomness
from .user_data import (
    ACCOUNT_STATUSES,
    PRIMARY_COUNTRIES,
    SIGNUP_SOURCES,
    UPDATE_FIELDS,
    generate_user_updates,
    generate_users,
    summarize_user_data,
)
from .writers import write_records_locally, write_records_to_s3


TEST_ENTITY = "generator_test"
TEST_RECORD_COUNT = 5


def generate_test_records(fake: Faker, partition_date: date) -> list[dict]:
    """Generate temporary infrastructure-validation records only."""
    start = datetime.combine(partition_date, datetime.min.time(), timezone.utc)
    return [
        {
            "test_id": generate_uuid(),
            "generated_at": add_random_delay(start, 0, 86_399),
            "value": fake.word(),
        }
        for _ in range(TEST_RECORD_COUNT)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Bronze-layer operational source datasets."
    )
    parser.add_argument(
        "--dataset",
        choices=("reference", "users", "activity"),
        default="reference",
        help="Dataset group to generate (default: reference).",
    )
    parser.add_argument(
        "--output",
        choices=("local", "s3"),
        default=DEFAULT_OUTPUT_MODE,
        help="Output destination (default: local).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_RANDOM_SEED,
        help="Seed for Python random and Faker (default: 42).",
    )
    parser.add_argument(
        "--partition-date",
        type=date.fromisoformat,
        default=date.today(),
        metavar="YYYY-MM-DD",
        help="Bronze partition date (default: today).",
    )
    return parser.parse_args()


def _print_distribution(title: str, values: tuple, counts: dict) -> None:
    print(f"\n{title}:")
    for value in values:
        print(f"{value if value is not None else 'NULL'}: {counts[value]}")


def _generate_user_datasets(fake: Faker, partition_date: date) -> tuple:
    users = generate_users(fake, partition_date)
    updates = generate_user_updates(users, fake, partition_date)
    summary = summarize_user_data(users, updates)

    print(f"Users generated: {len(users):,}")
    print(f"User updates generated: {len(updates):,}")
    _print_distribution(
        "Country distribution",
        PRIMARY_COUNTRIES,
        summary["country_distribution"],
    )
    _print_distribution(
        "Account status distribution",
        ACCOUNT_STATUSES,
        summary["account_status_distribution"],
    )
    _print_distribution(
        "Signup source distribution",
        (*SIGNUP_SOURCES, None),
        summary["signup_source_distribution"],
    )
    _print_distribution(
        "Update type distribution",
        UPDATE_FIELDS,
        summary["update_type_distribution"],
    )
    print("\nUsers with:")
    print(f"0 updates: {summary['users_with_0_updates']}")
    print(f"1 update: {summary['users_with_1_update']}")
    print(f"2+ updates: {summary['users_with_2_plus_updates']}")

    return (
        ("users", "users", users),
        ("user_updates", "user updates", updates),
    )


def _generate_activity_datasets(partition_date: date) -> tuple:
    users, user_updates, source_paths = load_existing_user_data(
        partition_date,
        DEFAULT_LOCAL_ROOT,
    )
    devices = generate_devices()
    conversations = generate_conversations(users, user_updates, partition_date)
    messages = generate_messages(
        users,
        user_updates,
        conversations,
        devices,
        partition_date,
    )
    summary = summarize_activity(users, conversations, messages, devices)

    print(f"Loaded users from {source_paths[0]}")
    print(f"Loaded user updates from {source_paths[1]}")
    print(f"Users available: {summary['users_available']:,}")
    print("\nUsers with:")
    for bucket, count in summary["user_conversation_buckets"].items():
        print(f"{bucket} conversations: {count:,}")
    print(f"\nTotal conversations: {summary['conversation_count']:,}")
    print("\nMessages:")
    print(f"total: {summary['message_count']:,}")
    print(f"average per conversation: {summary['average_messages']:.2f}")
    print(f"median per conversation: {summary['median_messages']:.1f}")
    print(f"p95 per conversation: {summary['p95_messages']}")
    print(f"maximum: {summary['max_messages']}")
    print(
        "\nConversations spanning >1 calendar day: "
        f"{summary['multi_day_conversations']:,}"
    )
    print("\nPlatform/device distribution:")
    for platform in ("WEB", "IOS", "ANDROID", "NULL"):
        print(f"{platform}: {summary['platform_distribution'][platform]:,}")

    return (
        ("conversations", "conversations", conversations),
        ("messages", "messages", messages),
    )


def main() -> None:
    args = parse_args()
    config = load_config(require_s3=args.output == "s3")
    fake = initialize_randomness(args.seed)
    if args.dataset == "reference":
        datasets = (
            ("models", "models", generate_models()),
            ("devices", "devices", generate_devices()),
            (
                "subscription_plans",
                "subscription plans",
                generate_subscription_plans(),
            ),
        )
    elif args.dataset == "users":
        datasets = _generate_user_datasets(fake, args.partition_date)
    else:
        datasets = _generate_activity_datasets(args.partition_date)

    for entity_name, display_name, records in datasets:
        print(f"Generated {len(records)} {display_name}")
        if args.output == "local":
            write_records_locally(
                records,
                entity_name,
                args.partition_date,
                output_root=DEFAULT_LOCAL_ROOT,
            )
        else:
            write_records_to_s3(
                records,
                entity_name,
                args.partition_date,
                bucket=config["s3_bucket"],
                region=config["aws_default_region"] or DEFAULT_AWS_REGION,
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
            )


if __name__ == "__main__":
    main()
