select 'conversation' as fact_name, conversation_id as fact_id
from {{ ref('fact_conversation') }}
where user_key_match_count > 1

union all

select 'message', message_id
from {{ ref('fact_message') }}
where user_key_match_count > 1

union all

select 'completion', completion_id
from {{ ref('fact_completion') }}
where user_key_match_count > 1

union all

select 'model_inference', inference_id
from {{ ref('fact_model_inference') }}
where user_key_match_count > 1

union all

select 'feedback', feedback_id
from {{ ref('fact_feedback') }}
where user_key_match_count > 1

union all

select 'error', error_id
from {{ ref('fact_error') }}
where user_key_match_count > 1

union all

select 'subscription', subscription_id
from {{ ref('fact_subscription') }}
where user_key_match_count > 1

union all

select 'purchase', purchase_id
from {{ ref('fact_purchase') }}
where user_key_match_count > 1

union all

select 'payment', payment_id
from {{ ref('fact_payment') }}
where user_key_match_count > 1
