with source_data as (
    select
        {{ staging_cast('purchase_id', 'varchar') }} as purchase_id,
        {{ staging_cast('user_id', 'varchar') }} as user_id,
        {{ staging_cast('subscription_id', 'varchar') }} as subscription_id,
        {{ staging_cast('plan_id', 'varchar') }} as plan_id,
        {{ staging_cast('purchase_type', 'varchar') }} as purchase_type,
        {{ staging_cast('purchase_status', 'varchar') }} as purchase_status,
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
