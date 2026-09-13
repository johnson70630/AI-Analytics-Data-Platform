FROM apache/airflow:2.9.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "dbt-core==1.12.3" \
    "dbt-duckdb==1.11.0" \
    "dbt-postgres==1.11.0"
