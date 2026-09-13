with source_data as (
    select
        cast(plan_id as varchar) as plan_id,
        cast(plan_name as varchar) as plan_name,
        {{ staging_cast('monthly_price', 'decimal(18, 2)') }} as monthly_price,
        cast(currency as varchar) as currency,
        {{ staging_cast('active_flag', 'boolean') }} as active_flag,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'subscription_plans') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by plan_id
            order by physical_partition_date desc nulls last
        ) as snapshot_rank
    from source_data
)

select
    plan_id,
    plan_name,
    monthly_price,
    currency,
    active_flag,
    physical_partition_date
from ranked
where snapshot_rank = 1
