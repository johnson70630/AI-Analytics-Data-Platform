"""Orchestrate the AI analytics dbt warehouse as distinct Airflow tasks."""

import logging
import smtplib
from datetime import timedelta
from email.mime.text import MIMEText

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_TARGET = "postgres_dev"
MAILPIT_SMTP_HOST = "mailpit"
MAILPIT_SMTP_PORT = 1025
ALERT_FROM_EMAIL = "airflow@ai-analytics.local"
ALERT_TO_EMAIL = "data-eng@ai-analytics.local"
LOCAL_TZ = pendulum.timezone("America/Los_Angeles")

LOGGER = logging.getLogger(__name__)


def dbt_command(command: str) -> str:
    """Run a dbt command against the PostgreSQL warehouse target."""
    return f"cd {DBT_PROJECT_DIR} && dbt {command} --target {DBT_TARGET}"


def notify_on_failure(context):
    """Send dbt_tests failure context to the local Mailpit inbox."""
    task_instance = context["task_instance"]
    logical_date = context.get("logical_date") or context.get("execution_date")
    exception = context.get("exception")

    message = MIMEText(
        "\n".join(
            (
                f"DAG: {task_instance.dag_id}",
                f"Task: {task_instance.task_id}",
                f"Run ID: {context.get('run_id') or task_instance.run_id}",
                f"Execution/logical date: {logical_date or 'Unavailable'}",
                f"Try number: {task_instance.try_number}",
                f"Exception: {exception or 'Unavailable'}",
                f"Log URL: {task_instance.log_url}",
            )
        ),
        "plain",
        "utf-8",
    )
    message["Subject"] = (
        f"[Airflow] {task_instance.dag_id}.{task_instance.task_id} failed"
    )
    message["From"] = ALERT_FROM_EMAIL
    message["To"] = ALERT_TO_EMAIL

    try:
        with smtplib.SMTP(MAILPIT_SMTP_HOST, MAILPIT_SMTP_PORT) as smtp:
            smtp.send_message(message)
    except (OSError, smtplib.SMTPException):
        LOGGER.exception(
            "Failed to send the Airflow failure alert to Mailpit for %s.%s",
            task_instance.dag_id,
            task_instance.task_id,
        )
        raise


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
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "airflow", "ai-analytics", "warehouse"],
) as dag:
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        bash_command=dbt_command("parse"),
    )

    extract_load = BashOperator(
        task_id="extract_load",
        bash_command=(
            "cd /opt/airflow && "
            "python -m src.data_generator.postgres_landing "
            "--incremental --lookback-days 2"
        ),
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt_command("run --select path:models/silver/staging"),
    )

    dbt_user_state_prerequisites = BashOperator(
        task_id="dbt_user_state_prerequisites",
        bash_command=dbt_command(
            "run --select user_state_history current_user_state"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_command("snapshot"),
        retries=3,
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
            "--indirect-selection cautious"
        ),
        retries=0,
        on_failure_callback=notify_on_failure,
    )

    (
        dbt_parse
        >> extract_load
        >> dbt_staging
        >> dbt_user_state_prerequisites
        >> dbt_snapshot
        >> dbt_silver_dimensions
        >> dbt_silver_facts
        >> dbt_gold
        >> dbt_observability
        >> dbt_tests
    )
