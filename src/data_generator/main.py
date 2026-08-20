"""CLI entry point for generating operational source data."""

import argparse
from datetime import date, datetime, timezone

from faker import Faker

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
        description="Generate Bronze-layer operational reference datasets."
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


def main() -> None:
    args = parse_args()
    config = load_config(require_s3=args.output == "s3")
    initialize_randomness(args.seed)
    datasets = (
        ("models", "models", generate_models()),
        ("devices", "devices", generate_devices()),
        (
            "subscription_plans",
            "subscription plans",
            generate_subscription_plans(),
        ),
    )

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
