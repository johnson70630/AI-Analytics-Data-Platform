from __future__ import annotations

import os
from typing import Any

from databricks.sdk import WorkspaceClient
from langchain_core.tools import tool


WAREHOUSE_ID_ENV = "DATABRICKS_SQL_WAREHOUSE_ID"

CATALOG = "ai_analytics"
SCHEMA = "observability"


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None

    if hasattr(value, "value"):
        return str(value.value)

    return str(value)


def _workspace_client() -> WorkspaceClient:
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE")

    if profile:
        return WorkspaceClient(profile=profile)

    return WorkspaceClient()


def _resolve_warehouse_id(
    warehouse_id: str | None = None,
) -> str:
    if warehouse_id and warehouse_id.strip():
        return warehouse_id.strip()

    value = os.getenv(WAREHOUSE_ID_ENV)

    if not value or not value.strip():
        raise ValueError(
            f"{WAREHOUSE_ID_ENV} is not configured"
        )

    return value.strip()


def _execute_select(
    *,
    client: WorkspaceClient,
    warehouse_id: str,
    statement: str,
) -> list[dict[str, Any]]:
    """
    Execute one fixed read-only observability query and return
    rows as dictionaries.
    """
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=CATALOG,
        schema=SCHEMA,
        statement=statement,
        wait_timeout="30s",
        row_limit=500,
    )

    state = _enum_value(
        getattr(response.status, "state", None)
    )

    if state != "SUCCEEDED":
        error = getattr(response.status, "error", None)
        message = getattr(error, "message", None)

        raise RuntimeError(
            f"Databricks SQL statement failed: "
            f"state={state}, message={message}"
        )

    if (
        response.manifest is None
        or response.manifest.schema is None
    ):
        return []

    columns = [
        column.name
        for column in response.manifest.schema.columns or []
    ]

    if response.result is None:
        return []

    data = response.result.data_array or []

    return [
        dict(zip(columns, row))
        for row in data
    ]


def _non_pass(
    rows: list[dict[str, Any]],
    status_column: str,
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get(status_column) != "PASS"
    ]


def fetch_latest_observability_summary(
    *,
    client: WorkspaceClient | None = None,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    """
    Read the latest Data Quality / observability state.

    Strictly read-only. All SQL statements are fixed SELECT queries.
    """
    resolved_warehouse_id = _resolve_warehouse_id(
        warehouse_id
    )
    workspace = client or _workspace_client()

    freshness = _execute_select(
        client=workspace,
        warehouse_id=resolved_warehouse_id,
        statement="""
            SELECT
                entity_name,
                latest_event_at,
                latest_ingested_at,
                latest_partition_date,
                monitoring_reference_at,
                monitoring_run_at,
                freshness_delay_hours,
                freshness_warning_threshold_hours,
                freshness_status
            FROM ai_analytics.observability.dq_entity_freshness
            WHERE monitoring_run_at = (
                SELECT MAX(monitoring_run_at)
                FROM ai_analytics.observability.dq_entity_freshness
            )
            ORDER BY entity_name
        """,
    )

    row_counts = _execute_select(
        client=workspace,
        warehouse_id=resolved_warehouse_id,
        statement="""
            SELECT
                entity_name,
                date_key,
                activity_date,
                row_count,
                previous_day_row_count,
                rolling_7_day_average,
                previous_day_change_pct,
                rolling_7_day_change_pct,
                volume_warning_threshold_pct,
                volume_status
            FROM ai_analytics.observability.dq_row_count_daily
            WHERE activity_date = (
                SELECT MAX(activity_date)
                FROM ai_analytics.observability.dq_row_count_daily
            )
            ORDER BY entity_name
        """,
    )

    quality = _execute_select(
        client=workspace,
        warehouse_id=resolved_warehouse_id,
        statement="""
            SELECT
                date_key,
                metric_date,
                entity_name,
                metric_name,
                numerator,
                denominator,
                metric_rate,
                previous_rate,
                rolling_7_day_rate,
                quality_warning_delta,
                quality_status
            FROM ai_analytics.observability.dq_quality_metrics_daily
            WHERE metric_date = (
                SELECT MAX(metric_date)
                FROM ai_analytics.observability.dq_quality_metrics_daily
            )
            ORDER BY entity_name, metric_name
        """,
    )

    # This table represents the current reconciliation snapshot
    # and does not contain a monitoring date column.
    reconciliation = _execute_select(
        client=workspace,
        warehouse_id=resolved_warehouse_id,
        statement="""
            SELECT
                reconciliation_name,
                source_layer,
                target_layer,
                source_count,
                target_count,
                difference,
                difference_pct,
                status
            FROM ai_analytics.observability.dq_pipeline_reconciliation
            ORDER BY reconciliation_name
        """,
    )

    freshness_issues = _non_pass(
        freshness,
        "freshness_status",
    )

    volume_issues = _non_pass(
        row_counts,
        "volume_status",
    )

    quality_issues = _non_pass(
        quality,
        "quality_status",
    )

    reconciliation_issues = _non_pass(
        reconciliation,
        "status",
    )

    return {
        "freshness": {
            "check_count": len(freshness),
            "issue_count": len(freshness_issues),
            "issues": freshness_issues,
            "rows": freshness,
        },
        "row_counts": {
            "check_count": len(row_counts),
            "issue_count": len(volume_issues),
            "issues": volume_issues,
            "rows": row_counts,
        },
        "quality": {
            "check_count": len(quality),
            "issue_count": len(quality_issues),
            "issues": quality_issues,
            "rows": quality,
        },
        "reconciliation": {
            "check_count": len(reconciliation),
            "issue_count": len(reconciliation_issues),
            "issues": reconciliation_issues,
            "rows": reconciliation,
        },
        "total_issue_count": (
            len(freshness_issues)
            + len(volume_issues)
            + len(quality_issues)
            + len(reconciliation_issues)
        ),
    }


@tool
def get_latest_observability_summary() -> dict[str, Any]:
    """
    Inspect the latest data observability state for the AI Analytics
    platform.

    Use this tool to investigate stale data, volume anomalies,
    data-quality anomalies, and cross-layer reconciliation failures.

    Returns the latest checks, non-PASS issues, and issue counts
    for each observability category.

    This tool is strictly read-only and cannot modify data.
    """
    return fetch_latest_observability_summary()
