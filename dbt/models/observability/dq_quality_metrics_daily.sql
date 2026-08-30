{{ config(materialized='table', schema='observability') }}

{% set quality_warning_delta = var('dq_quality_rate_warning_delta', 0.005) %}

with quality_counts as (
    select
        date_key,
        'messages' as entity_name,
        'missing_message_text_rate' as metric_name,
        count_if(is_message_text_missing) as numerator,
        count(*) as denominator
    from {{ ref('fact_message') }} group by date_key

    union all

    select date_key, 'messages', 'invalid_message_timestamp_rate',
        count_if(is_timestamp_order_valid = false), count(*)
    from {{ ref('fact_message') }} group by date_key

    union all

    select date_key, 'messages', 'unresolved_message_user_key_rate',
        count_if(user_id is not null and user_key is null), count(*)
    from {{ ref('fact_message') }} group by date_key

    union all

    select date_key, 'messages', 'unresolved_message_device_key_rate',
        count_if(device_id is not null and device_key is null), count(*)
    from {{ ref('fact_message') }} group by date_key

    union all

    select date_key, 'completions', 'invalid_completion_timestamp_rate',
        count_if(is_timestamp_order_valid = false), count(*)
    from {{ ref('fact_completion') }} group by date_key

    union all

    select date_key, 'completions', 'recovered_completion_user_id_rate',
        count_if(is_user_id_recovered), count(*)
    from {{ ref('fact_completion') }} group by date_key

    union all

    select date_key, 'completions', 'failed_completion_rate',
        count_if(completion_status = 'FAILED'), count(*)
    from {{ ref('fact_completion') }} group by date_key

    union all

    select date_key, 'model_inferences', 'missing_inference_model_rate',
        count_if(is_model_id_missing), count(*)
    from {{ ref('fact_model_inference') }} group by date_key

    union all

    select date_key, 'model_inferences', 'invalid_inference_timestamp_rate',
        count_if(is_timestamp_order_valid = false), count(*)
    from {{ ref('fact_model_inference') }} group by date_key

    union all

    select date_key, 'model_inferences', 'failed_inference_rate',
        count_if(inference_status = 'FAILED'), count(*)
    from {{ ref('fact_model_inference') }} group by date_key

    union all

    select date_key, 'feedback', 'missing_feedback_type_rate',
        count_if(is_feedback_type_missing), count(*)
    from {{ ref('fact_feedback') }} group by date_key

    union all

    select date_key, 'purchases', 'missing_purchase_type_rate',
        count_if(is_purchase_type_missing), count(*)
    from {{ ref('fact_purchase') }} group by date_key

    union all

    select date_key, 'payments', 'missing_payment_method_rate',
        count_if(is_payment_method_missing), count(*)
    from {{ ref('fact_payment') }} group by date_key

    union all

    select date_key, 'payments', 'invalid_payment_timestamp_rate',
        count_if(is_timestamp_order_valid = false), count(*)
    from {{ ref('fact_payment') }} group by date_key

    union all

    select date_key, 'payments', 'failed_payment_rate',
        count_if(payment_status = 'FAILED'), count(*)
    from {{ ref('fact_payment') }} group by date_key
),

quality_rates as (
    select
        counts.date_key,
        dates.full_date as metric_date,
        counts.entity_name,
        counts.metric_name,
        counts.numerator,
        counts.denominator,
        counts.numerator::double / nullif(counts.denominator, 0) as metric_rate
    from quality_counts as counts
    inner join {{ ref('dim_date') }} as dates using (date_key)
),

quality_baselines as (
    select
        *,
        lag(metric_rate) over (
            partition by entity_name, metric_name order by metric_date
        ) as previous_rate,
        avg(metric_rate) over (
            partition by entity_name, metric_name
            order by metric_date
            rows between 7 preceding and 1 preceding
        ) as rolling_7_day_rate
    from quality_rates
)

select
    date_key,
    metric_date,
    entity_name,
    metric_name,
    numerator,
    denominator,
    metric_rate,
    previous_rate,
    rolling_7_day_rate,
    {{ quality_warning_delta }}::double as quality_warning_delta,
    case
        when metric_rate is null or rolling_7_day_rate is null then 'PASS'
        when metric_rate - rolling_7_day_rate > {{ quality_warning_delta }}
            then 'WARN'
        else 'PASS'
    end as quality_status
from quality_baselines
