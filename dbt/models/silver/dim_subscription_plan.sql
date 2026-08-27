select
    md5(plan_id) as subscription_plan_key,
    plan_id,
    plan_name,
    monthly_price,
    currency,
    active_flag,
    physical_partition_date as source_partition_date
from {{ ref('stg_subscription_plans') }}
