"""Reusable deterministic ID and UTC timestamp helpers."""

import random as _stdlib_random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker


rng = _stdlib_random.Random()


def initialize_randomness(seed: int) -> Faker:
    """Seed the generator-owned RNG and a new Faker instance."""
    rng.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)
    return fake


def generate_uuid() -> str:
    """Return a UUID4-shaped value derived from seeded Python randomness."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


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
        seconds=rng.randint(min_seconds, max_seconds)
    )
