select
    payments.payment_id,
    payments.purchase_id,
    users.user_key,
    count(users.user_key) over (
        partition by payments.payment_id
    ) as user_key_match_count,
    dates.date_key,
    payments.user_id,
    payments.payment_status,
    payments.payment_method,
    payments.payment_amount,
    payments.refund_amount,
    payments.processed_at,
    payments.refunded_at,
    payments.updated_at,
    payments.ingested_at,
    payments.physical_partition_date as source_partition_date,
    payments.is_payment_method_missing,
    payments.is_timestamp_order_valid
from {{ ref('stg_payments') }} as payments
left join {{ ref('dim_user') }} as users
    on payments.user_id = users.user_id
   and payments.processed_at >= users.effective_start_at
   and (
       payments.processed_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_date') }} as dates
    on cast(payments.processed_at as date) = dates.full_date
