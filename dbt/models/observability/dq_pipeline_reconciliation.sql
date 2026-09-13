{{ config(materialized='table', schema='observability') }}

with reconciliation_counts as (
    select 'staging_to_fact_conversations' as reconciliation_name,
        'staging' as source_layer, 'silver' as target_layer,
        (select count(*) from {{ ref('stg_conversations') }}) as source_count,
        (select count(*) from {{ ref('fact_conversation') }}) as target_count
    union all
    select 'staging_to_fact_messages', 'staging', 'silver',
        (select count(*) from {{ ref('stg_messages') }}),
        (select count(*) from {{ ref('fact_message') }})
    union all
    select 'staging_to_fact_completions', 'staging', 'silver',
        (select count(*) from {{ ref('stg_completions') }}),
        (select count(*) from {{ ref('fact_completion') }})
    union all
    select 'staging_to_fact_model_inferences', 'staging', 'silver',
        (select count(*) from {{ ref('stg_model_inferences') }}),
        (select count(*) from {{ ref('fact_model_inference') }})
    union all
    select 'staging_to_fact_feedback', 'staging', 'silver',
        (select count(*) from {{ ref('stg_feedback') }}),
        (select count(*) from {{ ref('fact_feedback') }})
    union all
    select 'staging_to_fact_errors', 'staging', 'silver',
        (select count(*) from {{ ref('stg_errors') }}),
        (select count(*) from {{ ref('fact_error') }})
    union all
    select 'staging_to_fact_subscriptions', 'staging', 'silver',
        (select count(*) from {{ ref('stg_subscriptions') }}),
        (select count(*) from {{ ref('fact_subscription') }})
    union all
    select 'staging_to_fact_purchases', 'staging', 'silver',
        (select count(*) from {{ ref('stg_purchases') }}),
        (select count(*) from {{ ref('fact_purchase') }})
    union all
    select 'staging_to_fact_payments', 'staging', 'silver',
        (select count(*) from {{ ref('stg_payments') }}),
        (select count(*) from {{ ref('fact_payment') }})
    union all
    select 'silver_to_gold_messages', 'silver', 'gold',
        (select count(*) from {{ ref('fact_message') }}),
        (select sum(message_count) from {{ ref('gold_product_metrics_daily') }})
    union all
    select 'silver_to_gold_conversations', 'silver', 'gold',
        (select count(*) from {{ ref('fact_conversation') }}),
        (select sum(conversation_count) from {{ ref('gold_product_metrics_daily') }})
    union all
    select 'silver_to_gold_completions', 'silver', 'gold',
        (select count(*) from {{ ref('fact_completion') }}),
        (select sum(completion_count) from {{ ref('gold_product_metrics_daily') }})
    union all
    select 'silver_to_gold_feedback', 'silver', 'gold',
        (select count(*) from {{ ref('fact_feedback') }}),
        (select sum(feedback_count) from {{ ref('gold_product_metrics_daily') }})
    union all
    select 'silver_to_gold_model_inferences', 'silver', 'gold',
        (select count(*) from {{ ref('fact_model_inference') }}),
        (select sum(inference_count) from {{ ref('gold_ml_metrics_daily') }})
    union all
    select 'silver_to_gold_purchases', 'silver', 'gold',
        (select count(*) from {{ ref('fact_purchase') }}),
        (select sum(purchase_count) from {{ ref('gold_finance_metrics_daily') }})
    union all
    select 'silver_to_gold_payments', 'silver', 'gold',
        (select count(*) from {{ ref('fact_payment') }}),
        (select sum(payment_attempt_count) from {{ ref('gold_finance_metrics_daily') }})
)

select
    reconciliation_name,
    source_layer,
    target_layer,
    source_count,
    target_count,
    target_count - source_count as difference,
    {{ as_double('(target_count - source_count)') }} / nullif(source_count, 0)
        as difference_pct,
    case when target_count = source_count then 'PASS' else 'FAIL' end as status
from reconciliation_counts
