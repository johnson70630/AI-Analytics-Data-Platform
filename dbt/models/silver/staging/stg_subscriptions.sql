with source_data as (
    select
        cast(subscription_id as varchar) as subscription_id,
        cast(user_id as varchar) as user_id,
        cast(plan_id as varchar) as plan_id,
        {{ staging_cast('started_at', 'timestamp') }} as started_at,
        {{ staging_cast('ended_at', 'timestamp') }} as ended_at,
        cast(subscription_status as varchar) as subscription_status,
        {{ staging_cast('actual_monthly_price', 'decimal(18, 2)') }}
            as actual_monthly_price,
        {{ staging_cast('updated_at', 'timestamp') }} as updated_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'subscriptions') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by subscription_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    subscription_id,
    user_id,
    plan_id,
    started_at,
    ended_at,
    subscription_status,
    actual_monthly_price,
    updated_at,
    ingested_at,
    physical_partition_date
from ranked
where replay_rank = 1
