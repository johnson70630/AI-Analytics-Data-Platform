"""Environment configuration for fake source-data output."""

import os
from pathlib import Path

from dotenv import load_dotenv


DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_OUTPUT_MODE = "local"
DEFAULT_RANDOM_SEED = 42
DEFAULT_LOCAL_ROOT = Path("data")
REQUIRED_S3_SETTINGS = (
    "S3_BUCKET",
)


def load_config(require_s3: bool = False) -> dict[str, str | None]:
    """Load environment settings, requiring AWS values only for S3 output."""
    load_dotenv()

    config = {
        "aws_default_region": os.getenv(
            "AWS_DEFAULT_REGION", DEFAULT_AWS_REGION
        ),
        "s3_bucket": os.getenv("S3_BUCKET"),
    }

    if require_s3:
        missing = [
            name
            for name in REQUIRED_S3_SETTINGS
            if not os.getenv(name)
        ]
        if missing:
            raise RuntimeError(
                "Missing required S3 configuration: " + ", ".join(missing)
            )

    return config
