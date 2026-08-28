select
    purchases.purchase_id,
    purchases.subscription_id,
    users.user_key,
    count(users.user_key) over (
        partition by purchases.purchase_id
    ) as user_key_match_count,
    plans.subscription_plan_key,
    dates.date_key,
    purchases.user_id,
    purchases.plan_id,
    purchases.purchase_type,
    purchases.purchase_status,
    purchases.subtotal_amount,
    purchases.discount_amount,
    purchases.tax_amount,
    purchases.total_amount,
    purchases.purchase_created_at,
    purchases.updated_at,
    purchases.ingested_at,
    purchases.physical_partition_date as source_partition_date,
    purchases.is_purchase_type_missing
from {{ ref('stg_purchases') }} as purchases
left join {{ ref('dim_user') }} as users
    on purchases.user_id = users.user_id
   and purchases.purchase_created_at >= users.effective_start_at
   and (
       purchases.purchase_created_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_subscription_plan') }} as plans
    on purchases.plan_id = plans.plan_id
left join {{ ref('dim_date') }} as dates
    on cast(purchases.purchase_created_at as date) = dates.full_date
