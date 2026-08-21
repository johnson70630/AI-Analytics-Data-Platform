"""CLI entry point for generating operational source data."""

import argparse
from datetime import date, datetime, timezone

import boto3
from faker import Faker

from .activity_data import (
    generate_conversations,
    generate_messages,
    load_existing_user_data,
    summarize_activity,
    validate_conversations,
    validate_messages,
)
from .config import (
    DEFAULT_AWS_REGION,
    DEFAULT_LOCAL_ROOT,
    DEFAULT_OUTPUT_MODE,
    DEFAULT_RANDOM_SEED,
    load_config,
)
from .ingestion import (
    S3_REPLACEMENT_KEYS,
    add_ingestion_timestamps,
    ingestion_report,
    load_local_partitioned_records,
    remove_local_event_outputs,
    upload_local_partitioned_files,
    validate_ingestion_records,
    write_partitioned_records_locally,
    write_partitioned_records_to_s3,
)
from .finance_data import (
    generate_finance_data_locally,
    print_finance_summary,
    validate_local_finance_data,
    validate_s3_finance_data,
)
from .inference_data import (
    generate_inference_data_locally,
    print_inference_summary,
    validate_local_inference_data,
    validate_s3_inference_data,
)
from .quality_events import (
    generate_quality_events_locally,
    print_quality_summary,
    validate_local_quality_events,
    validate_s3_quality_events,
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
    validate_user_updates,
    validate_users,
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
        choices=(
            "reference",
            "users",
            "activity",
            "inference",
            "feedback_errors",
            "finance",
        ),
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
    generation_end = datetime.combine(
        partition_date,
        datetime.max.time().replace(microsecond=0),
        timezone.utc,
    )
    datasets_by_entity = {
        "users": add_ingestion_timestamps(users, "signup_at", generation_end),
        "user_updates": add_ingestion_timestamps(
            user_updates,
            "updated_at",
            generation_end,
        ),
        "conversations": add_ingestion_timestamps(
            conversations,
            "created_at",
            generation_end,
        ),
        "messages": add_ingestion_timestamps(
            messages,
            "created_at",
            generation_end,
        ),
    }
    for entity_name, records in datasets_by_entity.items():
        validate_ingestion_records(entity_name, records, generation_end)
    delay_report = ingestion_report(datasets_by_entity)

    print(f"Loaded users from {len(source_paths[0])} source file(s)")
    print(f"Loaded user updates from {len(source_paths[1])} source file(s)")
    print(f"Users preserved: {len(users):,}")
    print(f"User updates preserved: {len(user_updates):,}")
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
    print(f"p99 per conversation: {summary['p99_messages']}")
    print(f"maximum: {summary['max_messages']}")
    print(
        "\nConversations spanning >1 calendar day: "
        f"{summary['multi_day_conversations']:,}"
    )
    print("\nPlatform/device distribution:")
    for platform in ("WEB", "IOS", "ANDROID", "NULL"):
        print(f"{platform}: {summary['platform_distribution'][platform]:,}")

    print("\nIngestion delays (seconds):")
    for metric in ("median", "p95", "p99", "maximum"):
        print(f"{metric}: {delay_report[metric]:,.0f}")
    print("\nDaily ingestion partitions:")
    for entity_name, partition_summary in delay_report["partitions"].items():
        print(
            f"{entity_name}: {partition_summary['count']} "
            f"({partition_summary['earliest']} to {partition_summary['latest']})"
        )

    return tuple(
        (entity_name, entity_name.replace("_", " "), records)
        for entity_name, records in datasets_by_entity.items()
    )


def _load_partitioned_activity_datasets(partition_date: date) -> tuple:
    generation_end = datetime.combine(
        partition_date,
        datetime.max.time().replace(microsecond=0),
        timezone.utc,
    )
    loaded = {}
    for entity_name in ("users", "user_updates", "conversations", "messages"):
        records, physical_dates, paths = load_local_partitioned_records(
            entity_name,
            DEFAULT_LOCAL_ROOT,
        )
        validate_ingestion_records(
            entity_name,
            records,
            generation_end,
            physical_dates,
        )
        loaded[entity_name] = records
        print(
            f"Reloaded {len(records):,} {entity_name} records from "
            f"{len(paths)} validated daily files"
        )
    validate_users(loaded["users"], partition_date)
    validate_user_updates(loaded["users"], loaded["user_updates"])
    validate_conversations(
        loaded["users"],
        loaded["user_updates"],
        loaded["conversations"],
        partition_date,
    )
    validate_messages(
        loaded["users"],
        loaded["user_updates"],
        loaded["conversations"],
        loaded["messages"],
        generate_devices(),
        partition_date,
    )
    return tuple(
        (entity_name, entity_name.replace("_", " "), records)
        for entity_name, records in loaded.items()
    )


def main() -> None:
    args = parse_args()
    config = load_config(require_s3=args.output == "s3")
    fake = initialize_randomness(args.seed)
    if args.dataset == "finance":
        if args.output == "local":
            generate_finance_data_locally(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            summary = validate_local_finance_data(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            print("\nSerialized local Milestone 7 validation: PASS")
            print_finance_summary(summary)
        else:
            local_summary = validate_local_finance_data(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            print("Validated local Milestone 7 data before S3 upload: PASS")
            for entity_name in ("subscriptions", "purchases", "payments"):
                upload_local_partitioned_files(
                    entity_name,
                    DEFAULT_LOCAL_ROOT,
                    bucket=config["s3_bucket"],
                    region=config["aws_default_region"] or DEFAULT_AWS_REGION,
                    aws_access_key_id=config["aws_access_key_id"],
                    aws_secret_access_key=config["aws_secret_access_key"],
                )
            client = boto3.client(
                "s3",
                region_name=config["aws_default_region"] or DEFAULT_AWS_REGION,
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
            )
            s3_summary = validate_s3_finance_data(
                client,
                config["s3_bucket"],
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            if any(
                s3_summary[entity_name] != local_summary[entity_name]
                for entity_name in ("subscriptions", "purchases", "payments")
            ):
                raise ValueError("S3 Milestone 7 totals do not match local data")
            print("\nSerialized S3 Milestone 7 validation: PASS")
            print_finance_summary(s3_summary)
        return

    if args.dataset == "feedback_errors":
        if args.output == "local":
            generate_quality_events_locally(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            summary = validate_local_quality_events(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            print("\nSerialized local Milestone 6 validation: PASS")
            print_quality_summary(summary)
        else:
            local_summary = validate_local_quality_events(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            print("Validated local Milestone 6 data before S3 upload: PASS")
            for entity_name in ("feedback", "errors"):
                upload_local_partitioned_files(
                    entity_name,
                    DEFAULT_LOCAL_ROOT,
                    bucket=config["s3_bucket"],
                    region=config["aws_default_region"] or DEFAULT_AWS_REGION,
                    aws_access_key_id=config["aws_access_key_id"],
                    aws_secret_access_key=config["aws_secret_access_key"],
                )
            client = boto3.client(
                "s3",
                region_name=config["aws_default_region"] or DEFAULT_AWS_REGION,
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
            )
            s3_summary = validate_s3_quality_events(
                client,
                config["s3_bucket"],
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            if (
                s3_summary["feedback"] != local_summary["feedback"]
                or s3_summary["errors"] != local_summary["errors"]
            ):
                raise ValueError("S3 Milestone 6 totals do not match local data")
            print("\nSerialized S3 Milestone 6 validation: PASS")
            print_quality_summary(s3_summary)
        return

    if args.dataset == "inference":
        if args.output == "local":
            generate_inference_data_locally(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            summary = validate_local_inference_data(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            print("\nSerialized local Milestone 5 validation: PASS")
            print_inference_summary(summary)
        else:
            local_summary = validate_local_inference_data(
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            print("Validated local Milestone 5 data before S3 upload: PASS")
            for entity_name in ("completions", "model_inferences"):
                upload_local_partitioned_files(
                    entity_name,
                    DEFAULT_LOCAL_ROOT,
                    bucket=config["s3_bucket"],
                    region=config["aws_default_region"] or DEFAULT_AWS_REGION,
                    aws_access_key_id=config["aws_access_key_id"],
                    aws_secret_access_key=config["aws_secret_access_key"],
                )
            client = boto3.client(
                "s3",
                region_name=config["aws_default_region"] or DEFAULT_AWS_REGION,
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
            )
            s3_summary = validate_s3_inference_data(
                client,
                config["s3_bucket"],
                args.partition_date,
                DEFAULT_LOCAL_ROOT,
            )
            if (
                s3_summary["completions"] != local_summary["completions"]
                or s3_summary["model_inferences"]
                != local_summary["model_inferences"]
            ):
                raise ValueError("S3 Milestone 5 totals do not match local data")
            print("\nSerialized S3 Milestone 5 validation: PASS")
            print_inference_summary(s3_summary)
        return

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
        datasets = (
            _load_partitioned_activity_datasets(args.partition_date)
            if args.output == "s3"
            else _generate_activity_datasets(args.partition_date)
        )

    if args.dataset == "activity":
        entity_names = tuple(entity_name for entity_name, _, _ in datasets)
        if args.output == "local":
            removed = remove_local_event_outputs(
                entity_names,
                DEFAULT_LOCAL_ROOT,
            )
            print(f"Removed {len(removed)} obsolete local event files")
            for entity_name, _, records in datasets:
                write_partitioned_records_locally(
                    records,
                    entity_name,
                    DEFAULT_LOCAL_ROOT,
                )
        else:
            for entity_name, _, records in datasets:
                write_partitioned_records_to_s3(
                    records,
                    entity_name,
                    bucket=config["s3_bucket"],
                    region=config["aws_default_region"] or DEFAULT_AWS_REGION,
                    aws_access_key_id=config["aws_access_key_id"],
                    aws_secret_access_key=config["aws_secret_access_key"],
                    replacement_key=S3_REPLACEMENT_KEYS[entity_name],
                )
        return

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
