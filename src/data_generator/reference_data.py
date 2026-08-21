"""Deterministic operational reference-data catalogs and validation."""

from datetime import date
from typing import Any


VALID_DEVICE_TYPES = {"DESKTOP", "MOBILE", "TABLET"}
VALID_APP_PLATFORMS = {"WEB", "IOS", "ANDROID"}
EXPECTED_MODEL_COUNT = 8
EXPECTED_DEVICE_COUNT = 12
EXPECTED_SUBSCRIPTION_PLAN_COUNT = 5


def _validate_unique(
    records: list[dict[str, Any]],
    fields: tuple[str, ...],
    dataset_name: str,
) -> None:
    seen: set[tuple[Any, ...]] = set()
    for record in records:
        value = tuple(record.get(field) for field in fields)
        if value in seen:
            field_names = ", ".join(fields)
            raise ValueError(
                f"{dataset_name} validation failed: duplicate "
                f"({field_names}) value {value}"
            )
        seen.add(value)


def validate_models(models: list[dict[str, Any]]) -> None:
    """Validate model IDs, versions, release dates, and active flags."""
    if len(models) != EXPECTED_MODEL_COUNT:
        raise ValueError(
            "models validation failed: expected "
            f"{EXPECTED_MODEL_COUNT} records, received {len(models)}"
        )
    _validate_unique(models, ("model_id",), "models")
    _validate_unique(
        models,
        ("model_name", "model_version", "provider"),
        "models",
    )

    releases_by_model: dict[tuple[str, str], list[tuple[tuple[int, ...], date]]] = {}
    for model in models:
        if not isinstance(model.get("release_date"), date):
            raise ValueError(
                "models validation failed: release_date must be present "
                f"for {model.get('model_id')}"
            )
        if not isinstance(model.get("active_flag"), bool):
            raise ValueError(
                "models validation failed: active_flag must be boolean "
                f"for {model.get('model_id')}"
            )
        try:
            version = tuple(
                int(part) for part in model["model_version"].split(".")
            )
        except (AttributeError, KeyError, ValueError) as exc:
            raise ValueError(
                "models validation failed: model_version must be numeric "
                f"for {model.get('model_id')}"
            ) from exc
        catalog_key = (model.get("model_name"), model.get("provider"))
        releases_by_model.setdefault(catalog_key, []).append(
            (version, model["release_date"])
        )

    for catalog_key, releases in releases_by_model.items():
        ordered_releases = sorted(releases)
        release_dates = [release_date for _, release_date in ordered_releases]
        if release_dates != sorted(release_dates):
            raise ValueError(
                "models validation failed: release dates are out of version "
                f"order for {catalog_key[0]} from {catalog_key[1]}"
            )


def generate_models() -> list[dict[str, Any]]:
    """Return the stable operational model catalog."""
    models = [
        {
            "model_id": "model_001",
            "model_name": "Nova",
            "model_version": "1.0",
            "provider": "OpenAI",
            "release_date": date(2024, 6, 18),
            "active_flag": False,
        },
        {
            "model_id": "model_002",
            "model_name": "Nova",
            "model_version": "1.1",
            "provider": "OpenAI",
            "release_date": date(2025, 1, 15),
            "active_flag": True,
        },
        {
            "model_id": "model_003",
            "model_name": "Orion",
            "model_version": "2.0",
            "provider": "Anthropic",
            "release_date": date(2025, 3, 20),
            "active_flag": True,
        },
        {
            "model_id": "model_004",
            "model_name": "Atlas",
            "model_version": "1.0",
            "provider": "Google",
            "release_date": date(2025, 9, 5),
            "active_flag": False,
        },
        {
            "model_id": "model_005",
            "model_name": "Orion",
            "model_version": "1.0",
            "provider": "Anthropic",
            "release_date": date(2024, 4, 8),
            "active_flag": False,
        },
        {
            "model_id": "model_006",
            "model_name": "Atlas",
            "model_version": "1.1",
            "provider": "Google",
            "release_date": date(2026, 2, 12),
            "active_flag": True,
        },
        {
            "model_id": "model_007",
            "model_name": "Helix",
            "model_version": "1.0",
            "provider": "Cohere",
            "release_date": date(2024, 11, 4),
            "active_flag": False,
        },
        {
            "model_id": "model_008",
            "model_name": "Helix",
            "model_version": "1.1",
            "provider": "Cohere",
            "release_date": date(2025, 8, 19),
            "active_flag": True,
        },
    ]
    validate_models(models)
    return models


def validate_devices(devices: list[dict[str, Any]]) -> None:
    """Validate device IDs, categories, and OS/platform combinations."""
    if len(devices) != EXPECTED_DEVICE_COUNT:
        raise ValueError(
            "devices validation failed: expected "
            f"{EXPECTED_DEVICE_COUNT} records, received {len(devices)}"
        )
    _validate_unique(devices, ("device_id",), "devices")

    for device in devices:
        device_id = device.get("device_id")
        device_type = device.get("device_type")
        operating_system = device.get("operating_system")
        browser = device.get("browser")
        app_platform = device.get("app_platform")

        if device_type not in VALID_DEVICE_TYPES:
            raise ValueError(
                "devices validation failed: invalid device_type "
                f"for {device_id}"
            )
        if app_platform not in VALID_APP_PLATFORMS:
            raise ValueError(
                "devices validation failed: invalid app_platform "
                f"for {device_id}"
            )

        valid_combination = (
            app_platform == "WEB"
            and (
                (
                    device_type == "DESKTOP"
                    and (
                        (
                            operating_system == "macOS"
                            and browser in {"Chrome", "Safari", "Firefox"}
                        )
                        or (
                            operating_system == "Windows"
                            and browser in {"Chrome", "Edge", "Firefox"}
                        )
                        or (
                            operating_system == "Linux"
                            and browser in {"Chrome", "Firefox"}
                        )
                    )
                )
                or (
                    device_type in {"MOBILE", "TABLET"}
                    and (
                        (operating_system == "iOS" and browser == "Safari")
                        or (operating_system == "Android" and browser == "Chrome")
                    )
                )
            )
        ) or (
            app_platform == "IOS"
            and device_type in {"MOBILE", "TABLET"}
            and operating_system == "iOS"
            and browser is None
        ) or (
            app_platform == "ANDROID"
            and device_type in {"MOBILE", "TABLET"}
            and operating_system == "Android"
            and browser is None
        )
        if not valid_combination:
            raise ValueError(
                "devices validation failed: inconsistent device combination "
                f"for {device_id}"
            )


def generate_devices() -> list[dict[str, Any]]:
    """Return the stable operational device catalog."""
    devices = [
        {
            "device_id": "device_001",
            "device_type": "DESKTOP",
            "operating_system": "macOS",
            "browser": "Chrome",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_002",
            "device_type": "DESKTOP",
            "operating_system": "Windows",
            "browser": "Chrome",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_003",
            "device_type": "DESKTOP",
            "operating_system": "Windows",
            "browser": "Edge",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_004",
            "device_type": "MOBILE",
            "operating_system": "iOS",
            "browser": None,
            "app_platform": "IOS",
        },
        {
            "device_id": "device_005",
            "device_type": "MOBILE",
            "operating_system": "Android",
            "browser": None,
            "app_platform": "ANDROID",
        },
        {
            "device_id": "device_006",
            "device_type": "TABLET",
            "operating_system": "iOS",
            "browser": None,
            "app_platform": "IOS",
        },
        {
            "device_id": "device_007",
            "device_type": "DESKTOP",
            "operating_system": "macOS",
            "browser": "Safari",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_008",
            "device_type": "DESKTOP",
            "operating_system": "Linux",
            "browser": "Firefox",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_009",
            "device_type": "MOBILE",
            "operating_system": "iOS",
            "browser": "Safari",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_010",
            "device_type": "MOBILE",
            "operating_system": "Android",
            "browser": "Chrome",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_011",
            "device_type": "TABLET",
            "operating_system": "iOS",
            "browser": "Safari",
            "app_platform": "WEB",
        },
        {
            "device_id": "device_012",
            "device_type": "TABLET",
            "operating_system": "Android",
            "browser": "Chrome",
            "app_platform": "WEB",
        },
    ]
    validate_devices(devices)
    return devices


def validate_subscription_plans(plans: list[dict[str, Any]]) -> None:
    """Validate subscription plan IDs, prices, currencies, and active flags."""
    if len(plans) != EXPECTED_SUBSCRIPTION_PLAN_COUNT:
        raise ValueError(
            "subscription_plans validation failed: expected "
            f"{EXPECTED_SUBSCRIPTION_PLAN_COUNT} records, received {len(plans)}"
        )
    _validate_unique(plans, ("plan_id",), "subscription_plans")

    for plan in plans:
        plan_id = plan.get("plan_id")
        monthly_price = plan.get("monthly_price")
        currency = plan.get("currency")
        if (
            isinstance(monthly_price, bool)
            or not isinstance(monthly_price, (int, float))
            or monthly_price < 0
        ):
            raise ValueError(
                "subscription_plans validation failed: monthly_price must "
                f"be non-negative for {plan_id}"
            )
        if currency != "USD":
            raise ValueError(
                "subscription_plans validation failed: currency must be USD "
                f"for {plan_id}"
            )
        if not isinstance(plan.get("active_flag"), bool):
            raise ValueError(
                "subscription_plans validation failed: active_flag must be "
                f"boolean for {plan_id}"
            )


def generate_subscription_plans() -> list[dict[str, Any]]:
    """Return the stable, version-ready operational subscription plan catalog."""
    plans = [
        {
            "plan_id": "plan_free_v1",
            "plan_name": "FREE",
            "monthly_price": 0.00,
            "currency": "USD",
            "active_flag": True,
        },
        {
            "plan_id": "plan_plus_v1",
            "plan_name": "PLUS",
            "monthly_price": 15.00,
            "currency": "USD",
            "active_flag": False,
        },
        {
            "plan_id": "plan_plus_v2",
            "plan_name": "PLUS",
            "monthly_price": 20.00,
            "currency": "USD",
            "active_flag": True,
        },
        {
            "plan_id": "plan_pro_v1",
            "plan_name": "PRO",
            "monthly_price": 40.00,
            "currency": "USD",
            "active_flag": False,
        },
        {
            "plan_id": "plan_pro_v2",
            "plan_name": "PRO",
            "monthly_price": 50.00,
            "currency": "USD",
            "active_flag": True,
        },
    ]
    validate_subscription_plans(plans)
    return plans
