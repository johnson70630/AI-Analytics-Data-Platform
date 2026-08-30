with source_data as (
    select
        cast(payment_id as varchar) as payment_id,
        cast(purchase_id as varchar) as purchase_id,
        cast(user_id as varchar) as user_id,
        cast(payment_status as varchar) as payment_status,
        cast(payment_method as varchar) as payment_method,
        try_cast(payment_amount as decimal(18, 2)) as payment_amount,
        try_cast(refund_amount as decimal(18, 2)) as refund_amount,
        try_cast(processed_at as timestamp) as processed_at,
        try_cast(refunded_at as timestamp) as refunded_at,
        try_cast(updated_at as timestamp) as updated_at,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'payments') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by payment_id
            order by ingested_at asc, physical_partition_date asc
        ) as replay_rank
    from source_data
),

deduplicated as (
    select
        payment_id,
        purchase_id,
        user_id,
        payment_status,
        payment_method,
        payment_amount,
        refund_amount,
        processed_at,
        refunded_at,
        updated_at,
        ingested_at,
        physical_partition_date
    from ranked
    where replay_rank = 1
)

select
    payments.payment_id,
    payments.purchase_id,
    payments.user_id,
    payments.payment_status,
    payments.payment_method,
    payments.payment_amount,
    payments.refund_amount,
    payments.processed_at,
    payments.refunded_at,
    payments.updated_at,
    payments.ingested_at,
    payments.physical_partition_date,
    payments.payment_method is null as is_payment_method_missing,
    case
        when purchases.purchase_id is null then null
        else payments.processed_at >= purchases.purchase_created_at
    end as is_timestamp_order_valid
from deduplicated as payments
left join {{ ref('stg_purchases') }} as purchases
    on payments.purchase_id = purchases.purchase_id
