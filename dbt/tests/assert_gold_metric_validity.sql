select 'product' as mart_name, cast(date_key as varchar) as grain_key
from {{ ref('gold_product_metrics_daily') }}
where active_users < 0
   or conversation_count < 0
   or message_count < 0
   or completion_count < 0
   or successful_completion_count < 0
   or failed_completion_count < 0
   or cancelled_completion_count < 0
   or feedback_count < 0
   or positive_feedback_count < 0
   or negative_feedback_count < 0
   or completion_success_rate not between 0 and 1
   or feedback_rate not between 0 and 1
   or positive_feedback_rate not between 0 and 1

union all

select 'finance', cast(date_key as varchar)
from {{ ref('gold_finance_metrics_daily') }}
where subscription_start_count < 0
   or subscription_end_count < 0
   or purchase_count < 0
   or payment_attempt_count < 0
   or purchases_with_payment_attempts < 0
   or purchases_with_multiple_payment_attempts < 0
   or retry_attempt_count < 0
   or purchases_with_multiple_payment_attempts > purchases_with_payment_attempts
   or payment_success_rate not between 0 and 1
   or retry_purchase_rate not between 0 and 1
   or net_revenue is distinct from gross_collected_amount - refund_amount

union all

select
    'ml',
    cast(date_key as varchar) || '|'
        || coalesce(model_key, model_id, '__UNRESOLVED__')
from {{ ref('gold_ml_metrics_daily') }}
where inference_count < 0
   or successful_inference_count < 0
   or failed_inference_count < 0
   or inference_success_rate not between 0 and 1
   or average_latency_ms < 0
   or p50_latency_ms < 0
   or p95_latency_ms < 0
   or p99_latency_ms < 0
   or p50_latency_ms > p95_latency_ms
   or p95_latency_ms > p99_latency_ms
   or total_input_tokens < 0
   or total_output_tokens < 0
   or total_tokens < 0

union all

select 'user_activity', cast(date_key as varchar) || '|' || user_id
from {{ ref('gold_user_activity_daily') }}
where message_count < 0
   or conversation_count < 0
   or completion_count < 0
   or successful_completion_count < 0
   or feedback_count < 0
   or positive_feedback_count < 0
   or inference_count < 0
   or input_tokens < 0
   or output_tokens < 0
   or total_tokens < 0
   or active_flag is distinct from (message_count > 0)
