"""Orchestrate the AI analytics dbt warehouse as distinct Airflow tasks."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def dbt_command(command: str) -> str:
    """Run a dbt command from the mounted project directory."""
    return f"cd {DBT_PROJECT_DIR} && dbt {command}"


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="ai_analytics_dbt_pipeline",
    description=(
        "Orchestrates dbt snapshot, staging, Silver, Gold, observability, "
        "and validation for the AI analytics warehouse"
    ),
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "airflow", "ai-analytics", "warehouse"],
) as dag:
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        bash_command=dbt_command("parse"),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_command("snapshot"),
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt_command("run --select path:models/silver/staging"),
    )

    dbt_silver_dimensions = BashOperator(
        task_id="dbt_silver_dimensions",
        bash_command=dbt_command(
            "run --select dim_date dim_user dim_model dim_device "
            "dim_subscription_plan"
        ),
    )

    dbt_silver_facts = BashOperator(
        task_id="dbt_silver_facts",
        bash_command=dbt_command(
            "run --select fact_conversation fact_message fact_completion "
            "fact_model_inference fact_feedback fact_error fact_subscription "
            "fact_purchase fact_payment"
        ),
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=dbt_command("run --select path:models/gold"),
    )

    dbt_observability = BashOperator(
        task_id="dbt_observability",
        bash_command=dbt_command("run --select path:models/observability"),
    )

    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=dbt_command(
            "test --select path:models/silver path:models/gold "
            "path:models/observability "
            "--exclude path:models/silver/staging "
            "assert_fact_source_row_counts "
            "assert_user_history_coverage_and_current "
            "assert_user_history_type1_latest "
            "assert_user_history_type2_version_counts "
            "--indirect-selection cautious"
        ),
    )

    (
        dbt_parse
        >> dbt_snapshot
        >> dbt_staging
        >> dbt_silver_dimensions
        >> dbt_silver_facts
        >> dbt_gold
        >> dbt_observability
        >> dbt_tests
    )
