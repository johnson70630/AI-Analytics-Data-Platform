with source_data as (
    select
        cast(purchase_id as varchar) as purchase_id,
        cast(user_id as varchar) as user_id,
        cast(subscription_id as varchar) as subscription_id,
        cast(plan_id as varchar) as plan_id,
        cast(purchase_type as varchar) as purchase_type,
        cast(purchase_status as varchar) as purchase_status,
        {{ staging_cast('subtotal_amount', 'decimal(18, 2)') }}
            as subtotal_amount,
        {{ staging_cast('discount_amount', 'decimal(18, 2)') }}
            as discount_amount,
        {{ staging_cast('tax_amount', 'decimal(18, 2)') }} as tax_amount,
        {{ staging_cast('total_amount', 'decimal(18, 2)') }} as total_amount,
        {{ staging_cast('purchase_created_at', 'timestamp') }}
            as purchase_created_at,
        {{ staging_cast('updated_at', 'timestamp') }} as updated_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'purchases') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by purchase_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    purchase_id,
    user_id,
    subscription_id,
    plan_id,
    purchase_type,
    purchase_status,
    subtotal_amount,
    discount_amount,
    tax_amount,
    total_amount,
    purchase_created_at,
    updated_at,
    ingested_at,
    physical_partition_date,
    purchase_type is null as is_purchase_type_missing
from ranked
where replay_rank = 1
