select
    subscriptions.subscription_id,
    users.user_key,
    count(users.user_key) over (
        partition by subscriptions.subscription_id
    ) as user_key_match_count,
    plans.subscription_plan_key,
    dates.date_key,
    subscriptions.user_id,
    subscriptions.plan_id,
    subscriptions.subscription_status,
    subscriptions.started_at,
    subscriptions.ended_at,
    subscriptions.actual_monthly_price,
    subscriptions.updated_at,
    subscriptions.ingested_at,
    subscriptions.physical_partition_date as source_partition_date
from {{ ref('stg_subscriptions') }} as subscriptions
left join {{ ref('dim_user') }} as users
    on subscriptions.user_id = users.user_id
   and subscriptions.started_at >= users.effective_start_at
   and (
       subscriptions.started_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_subscription_plan') }} as plans
    on subscriptions.plan_id = plans.plan_id
left join {{ ref('dim_date') }} as dates
    on cast(subscriptions.started_at as date) = dates.full_date
