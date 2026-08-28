with reconciliations as (
    select
        'conversation' as fact_name,
        (select count(*) from {{ ref('stg_conversations') }}) as source_rows,
        (select count(*) from {{ ref('fact_conversation') }}) as fact_rows

    union all

    select
        'message',
        (select count(*) from {{ ref('stg_messages') }}),
        (select count(*) from {{ ref('fact_message') }})

    union all

    select
        'completion',
        (select count(*) from {{ ref('stg_completions') }}),
        (select count(*) from {{ ref('fact_completion') }})

    union all

    select
        'model_inference',
        (select count(*) from {{ ref('stg_model_inferences') }}),
        (select count(*) from {{ ref('fact_model_inference') }})

    union all

    select
        'feedback',
        (select count(*) from {{ ref('stg_feedback') }}),
        (select count(*) from {{ ref('fact_feedback') }})

    union all

    select
        'error',
        (select count(*) from {{ ref('stg_errors') }}),
        (select count(*) from {{ ref('fact_error') }})

    union all

    select
        'subscription',
        (select count(*) from {{ ref('stg_subscriptions') }}),
        (select count(*) from {{ ref('fact_subscription') }})

    union all

    select
        'purchase',
        (select count(*) from {{ ref('stg_purchases') }}),
        (select count(*) from {{ ref('fact_purchase') }})

    union all

    select
        'payment',
        (select count(*) from {{ ref('stg_payments') }}),
        (select count(*) from {{ ref('fact_payment') }})
)

select *
from reconciliations
where source_rows != fact_rows
