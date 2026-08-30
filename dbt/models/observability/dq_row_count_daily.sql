{{ config(materialized='table', schema='observability') }}

{% set volume_warning_pct = var('dq_volume_warning_pct', 0.50) %}

with entity_daily_counts as (
    select 'messages' as entity_name, date_key, count(*) as row_count
    from {{ ref('fact_message') }} group by date_key
    union all
    select 'conversations', date_key, count(*)
    from {{ ref('fact_conversation') }} group by date_key
    union all
    select 'completions', date_key, count(*)
    from {{ ref('fact_completion') }} group by date_key
    union all
    select 'model_inferences', date_key, count(*)
    from {{ ref('fact_model_inference') }} group by date_key
    union all
    select 'feedback', date_key, count(*)
    from {{ ref('fact_feedback') }} group by date_key
    union all
    select 'errors', date_key, count(*)
    from {{ ref('fact_error') }} group by date_key
    union all
    select 'subscriptions', date_key, count(*)
    from {{ ref('fact_subscription') }} group by date_key
    union all
    select 'purchases', date_key, count(*)
    from {{ ref('fact_purchase') }} group by date_key
    union all
    select 'payments', date_key, count(*)
    from {{ ref('fact_payment') }} group by date_key
),

entity_bounds as (
    select
        entity_name,
        min(date_key) as minimum_date_key,
        max(date_key) as maximum_date_key
    from entity_daily_counts
    where date_key is not null
    group by entity_name
),

complete_daily_spine as (
    select
        bounds.entity_name,
        dates.date_key,
        dates.full_date as activity_date,
        coalesce(counts.row_count, 0) as row_count
    from entity_bounds as bounds
    inner join {{ ref('dim_date') }} as dates
        on dates.date_key between bounds.minimum_date_key and bounds.maximum_date_key
    left join entity_daily_counts as counts
        on bounds.entity_name = counts.entity_name
       and dates.date_key = counts.date_key
),

daily_baselines as (
    select
        *,
        lag(row_count) over (
            partition by entity_name order by activity_date
        ) as previous_day_row_count,
        avg(row_count) over (
            partition by entity_name
            order by activity_date
            rows between 7 preceding and 1 preceding
        ) as rolling_7_day_average
    from complete_daily_spine
),

daily_changes as (
    select
        *,
        (row_count - previous_day_row_count)::double
            / nullif(previous_day_row_count, 0) as previous_day_change_pct,
        (row_count - rolling_7_day_average)::double
            / nullif(rolling_7_day_average, 0) as rolling_7_day_change_pct
    from daily_baselines
)

select
    entity_name,
    date_key,
    activity_date,
    row_count,
    previous_day_row_count,
    rolling_7_day_average,
    previous_day_change_pct,
    rolling_7_day_change_pct,
    {{ volume_warning_pct }}::double as volume_warning_threshold_pct,
    case
        when rolling_7_day_average is null then 'PASS'
        when rolling_7_day_average = 0 and row_count > 0 then 'WARN'
        when rolling_7_day_average > 0
         and abs(rolling_7_day_change_pct) > {{ volume_warning_pct }} then 'WARN'
        else 'PASS'
    end as volume_status
from daily_changes
