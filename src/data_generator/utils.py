"""Reusable deterministic ID and UTC timestamp helpers."""

import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker


def initialize_randomness(seed: int) -> Faker:
    """Seed Python and Faker, then return a seeded Faker instance."""
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)
    return fake


def generate_uuid() -> str:
    """Return a UUID4-shaped value derived from seeded Python randomness."""
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def ensure_utc(timestamp: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC.

    Naive inputs are interpreted as UTC. Generated raw event timestamps use UTC
    consistently so future causal event chains can be compared safely.
    """
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def add_random_delay(
    timestamp: datetime,
    min_seconds: int = 0,
    max_seconds: int = 0,
) -> datetime:
    """Return a UTC timestamp at or after the supplied event timestamp."""
    if min_seconds < 0 or max_seconds < min_seconds:
        raise ValueError(
            "Delay bounds must satisfy 0 <= min_seconds <= max_seconds"
        )
    return ensure_utc(timestamp) + timedelta(
        seconds=random.randint(min_seconds, max_seconds)
    )
