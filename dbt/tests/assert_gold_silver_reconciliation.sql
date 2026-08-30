with reconciliations as (
    select
        'product_messages' as metric_name,
        (select sum(message_count) from {{ ref('gold_product_metrics_daily') }})
            as gold_value,
        (select count(*) from {{ ref('fact_message') }}) as silver_value

    union all

    select
        'product_conversations',
        (select sum(conversation_count) from {{ ref('gold_product_metrics_daily') }}),
        (select count(*) from {{ ref('fact_conversation') }})

    union all

    select
        'product_completions',
        (select sum(completion_count) from {{ ref('gold_product_metrics_daily') }}),
        (select count(*) from {{ ref('fact_completion') }})

    union all

    select
        'product_feedback',
        (select sum(feedback_count) from {{ ref('gold_product_metrics_daily') }}),
        (select count(*) from {{ ref('fact_feedback') }})

    union all

    select
        'ml_inferences',
        (select sum(inference_count) from {{ ref('gold_ml_metrics_daily') }}),
        (select count(*) from {{ ref('fact_model_inference') }})

    union all

    select
        'finance_purchases',
        (select sum(purchase_count) from {{ ref('gold_finance_metrics_daily') }}),
        (select count(*) from {{ ref('fact_purchase') }})

    union all

    select
        'finance_payments',
        (select sum(payment_attempt_count) from {{ ref('gold_finance_metrics_daily') }}),
        (select count(*) from {{ ref('fact_payment') }})

    union all

    select
        'user_messages',
        (select sum(message_count) from {{ ref('gold_user_activity_daily') }}),
        (select count(*) from {{ ref('fact_message') }} where user_id is not null)

    union all

    select
        'user_conversations',
        (select sum(conversation_count) from {{ ref('gold_user_activity_daily') }}),
        (select count(*) from {{ ref('fact_conversation') }} where user_id is not null)

    union all

    select
        'user_completions',
        (select sum(completion_count) from {{ ref('gold_user_activity_daily') }}),
        (select count(*) from {{ ref('fact_completion') }} where user_id is not null)

    union all

    select
        'user_feedback',
        (select sum(feedback_count) from {{ ref('gold_user_activity_daily') }}),
        (select count(*) from {{ ref('fact_feedback') }} where user_id is not null)

    union all

    select
        'user_inferences',
        (select sum(inference_count) from {{ ref('gold_user_activity_daily') }}),
        (select count(*) from {{ ref('fact_model_inference') }} where user_id is not null)
)

select *
from reconciliations
where gold_value is distinct from silver_value
