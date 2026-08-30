select fact_name || '.user_key' as relationship_name, fact_id
from (
    select 'conversation' as fact_name, conversation_id as fact_id, user_key
    from {{ ref('fact_conversation') }}
    union all
    select 'message', message_id, user_key from {{ ref('fact_message') }}
    union all
    select 'completion', completion_id, user_key from {{ ref('fact_completion') }}
    union all
    select 'model_inference', inference_id, user_key from {{ ref('fact_model_inference') }}
    union all
    select 'feedback', feedback_id, user_key from {{ ref('fact_feedback') }}
    union all
    select 'error', error_id, user_key from {{ ref('fact_error') }}
    union all
    select 'subscription', subscription_id, user_key from {{ ref('fact_subscription') }}
    union all
    select 'purchase', purchase_id, user_key from {{ ref('fact_purchase') }}
    union all
    select 'payment', payment_id, user_key from {{ ref('fact_payment') }}
) as fact
left join {{ ref('dim_user') }} as dimension using (user_key)
where fact.user_key is not null and dimension.user_key is null

union all

select 'message.device_key', message_id
from {{ ref('fact_message') }} as fact
left join {{ ref('dim_device') }} as dimension using (device_key)
where fact.device_key is not null and dimension.device_key is null

union all

select 'model_inference.model_key', inference_id
from {{ ref('fact_model_inference') }} as fact
left join {{ ref('dim_model') }} as dimension using (model_key)
where fact.model_key is not null and dimension.model_key is null

union all

select 'error.model_key', error_id
from {{ ref('fact_error') }} as fact
left join {{ ref('dim_model') }} as dimension using (model_key)
where fact.model_key is not null and dimension.model_key is null

union all

select 'subscription.subscription_plan_key', subscription_id
from {{ ref('fact_subscription') }} as fact
left join {{ ref('dim_subscription_plan') }} as dimension
    using (subscription_plan_key)
where fact.subscription_plan_key is not null
  and dimension.subscription_plan_key is null

union all

select 'purchase.subscription_plan_key', purchase_id
from {{ ref('fact_purchase') }} as fact
left join {{ ref('dim_subscription_plan') }} as dimension
    using (subscription_plan_key)
where fact.subscription_plan_key is not null
  and dimension.subscription_plan_key is null

union all

select fact_name || '.date_key', fact_id
from (
    select 'conversation' as fact_name, conversation_id as fact_id, date_key
    from {{ ref('fact_conversation') }}
    union all
    select 'message', message_id, date_key from {{ ref('fact_message') }}
    union all
    select 'completion', completion_id, date_key from {{ ref('fact_completion') }}
    union all
    select 'model_inference', inference_id, date_key from {{ ref('fact_model_inference') }}
    union all
    select 'feedback', feedback_id, date_key from {{ ref('fact_feedback') }}
    union all
    select 'error', error_id, date_key from {{ ref('fact_error') }}
    union all
    select 'subscription', subscription_id, date_key from {{ ref('fact_subscription') }}
    union all
    select 'purchase', purchase_id, date_key from {{ ref('fact_purchase') }}
    union all
    select 'payment', payment_id, date_key from {{ ref('fact_payment') }}
) as fact
left join {{ ref('dim_date') }} as dimension using (date_key)
where fact.date_key is not null and dimension.date_key is null
