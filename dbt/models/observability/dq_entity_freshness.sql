{{ config(materialized='table', schema='observability') }}

{% set freshness_warn_hours = var('dq_freshness_warn_hours', 72) %}

with entity_observations as (
    select
        'messages' as entity_name,
        max(created_at) as latest_event_at,
        max(ingested_at) as latest_ingested_at,
        max(source_partition_date) as latest_partition_date
    from {{ ref('fact_message') }}

    union all

    select
        'conversations', max(created_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_conversation') }}

    union all

    select
        'completions', max(requested_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_completion') }}

    union all

    select
        'model_inferences', max(request_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_model_inference') }}

    union all

    select
        'feedback', max(created_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_feedback') }}

    union all

    select
        'errors', max(occurred_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_error') }}

    union all

    select
        'subscriptions', max(started_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_subscription') }}

    union all

    select
        'purchases', max(purchase_created_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_purchase') }}

    union all

    select
        'payments', max(processed_at), max(ingested_at),
        max(source_partition_date)
    from {{ ref('fact_payment') }}
),

project_reference as (
    select max(latest_event_at) as project_max_activity_at
    from entity_observations
)

select
    observations.entity_name,
    observations.latest_event_at,
    observations.latest_ingested_at,
    observations.latest_partition_date,
    reference.project_max_activity_at as monitoring_reference_at,
    current_timestamp as monitoring_run_at,
    date_diff(
        'second',
        observations.latest_event_at,
        reference.project_max_activity_at
    ) / 3600.0 as freshness_delay_hours,
    {{ freshness_warn_hours }}::double as freshness_warning_threshold_hours,
    case
        when observations.latest_event_at is null then 'FAIL'
        when date_diff(
            'second',
            observations.latest_event_at,
            reference.project_max_activity_at
        ) / 3600.0 > {{ freshness_warn_hours }} then 'WARN'
        else 'PASS'
    end as freshness_status
from entity_observations as observations
cross join project_reference as reference
